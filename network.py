import logging as log
import selectors
import socket
import threading
import types

import miniupnpc

from connection import Connection
from message import Message
import util


class NetworkThread(threading.Thread):
    TIMEOUT = 1
    DEFAULT_PORT = 63000

    def __init__(self, queue):
        threading.Thread.__init__(self)

        # cli thread communication
        self.queue = queue

        # upnp port forwarding config
        self.port = self.DEFAULT_PORT
        self.upnp = miniupnpc.UPnP()
        self.upnp.discoverdelay = 10
        self.register_forwarding()

        # create selector object, listening socket
        self.lsock = None
        try_again = True  # until we find a free port
        while try_again:
            print('Start listening on (%s, %s)' % (self.host, self.port))
            try:
                self.sel = selectors.DefaultSelector()
                self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.lsock.bind((self.host, self.port))
                self.lsock.listen()
                self.lsock.setblocking(False)
                try_again = False
            except Exception as e:
                print('Could not open listening socket:', e)
                self.port += 1

        log.debug('listening on (%s, %s)' % (self.host, self.port))

        # register listening socket and comm. queue in select
        self.sel.register(self.lsock, selectors.EVENT_READ, data=None)
        self.sel.register(self.queue, selectors.EVENT_READ)

        # mapping ident -> Connection object
        self.connections = {}

        # flags
        self.running = True
        self.done = False

    def run(self):
        while self.running:
            events = self.sel.select(timeout=self.TIMEOUT)
            for key, mask in events:
                if key.fileobj is self.queue:
                    self.handle_queue()
                elif key.fileobj is self.lsock:
                    self.accept_wrapper()
                else:
                    self.service_connection(key.fileobj, key.data, mask)
        print('Shutting down this peer...')
        self.reset()

    def handle_queue(self):
        cmd, payload = self.queue.get()
        if cmd == 'connect':
            host = payload['host']
            port = payload['port']
            print('Try to connect to (%s, %s)' % (host, port))
            data = types.SimpleNamespace(addr=(host, port), inb=b'', outb=b'')
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect((host, port))
                events = selectors.EVENT_READ | selectors.EVENT_WRITE
                self.sel.register(sock, events, data=data)

                ident = util.get_key(*sock.getpeername())
                self.connections[ident] = (sock, Connection(self.queue, self.connections, ident, host, port, known=True))
                self.get_connection(ident).send(Message({
                    'do': 'hello',
                    'content': {
                        'lhost': self.host,
                        'lport': self.port,
                    },
                }))
            except (ConnectionRefusedError, ConnectionAbortedError, TimeoutError) as e:
                print('Could not establish connection to (%s, %s):' % (host, port), e)
        elif cmd == 'send_msg':
            sleep(1)
        elif cmd == 'broadcast':
            for _, conn in self.connections.values():
                conn.send(Message(payload))
        else:
            print('Unknown command:', cmd)

    def reset(self):
        self.lsock.close()
        self.sel.close()
        for sock, _ in self.connections.values():
            sock.close()
        self.remove_forwarding()
        self.connections = {}
        self.lsock = None
        self.sel = None
        self.done = True

    def is_done(self):
        return self.done

    def stop(self):
        self.running = False

    def get_connection(self, ident):
        return self.connections[ident][1]

    def get_socket(self, ident):
        return self.connections[ident][0]

    def accept_wrapper(self):
        sock, addr = self.lsock.accept()  # Should be ready to read
        log.debug('accepted connection from %s' % str(addr))
        sock.setblocking(False)
        data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.sel.register(sock, events, data=data)

        ident = util.get_key(*sock.getpeername())
        self.connections[ident] = (sock, Connection(self.queue, self.connections, ident, addr[0], addr[1]))

    def service_connection(self, sock, data, mask):
        ident = util.get_key(*sock.getpeername())
        connection = self.get_connection(ident)
        if mask & selectors.EVENT_READ:
            try:
                recv_data = sock.recv(1024)  # Should be ready to read
                if recv_data:
                    connection.handle_data(recv_data)
                else:  # connection closed
                    log.debug('closing connection to %s' % str(data.addr))
                    self.sel.unregister(sock)
                    sock.close()
                    del self.connections[ident]
            except (ConnectionResetError, ConnectionAbortedError):
                print('Connection reset/aborted:', ident)
                self.sel.unregister(sock)
                sock.close()
                del self.connections[ident]
        if mask & selectors.EVENT_WRITE:
            if connection.has_data():
                log.debug('echoing %s from buffer to socket %s' % (repr(connection.out), data.addr))
                sent = sock.send(connection.out)  # Should be ready to write
                connection.out = connection.out[sent:]

    def register_forwarding(self):
        self.upnp.discover()
        self.upnp.selectigd()
        # addportmapping(external-port, protocol, internal-host, internal-port, description, remote-host)
        self.upnp.addportmapping(self.port, 'TCP', self.upnp.lanaddr, self.port, 'Naxos', '')
        self.host = self.upnp.lanaddr

    def remove_forwarding(self):
        # deleteportmapping(external-port, protocol, description)
        self.upnp.deleteportmapping(self.port, 'TCP', 'Naxos')
