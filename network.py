import selectors
import socket
import threading
import types
import logging as log

import util
from message import Message
from connection import Connection


class NetworkThread(threading.Thread):
    TIMEOUT = 1
    DEFAULT_HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
    DEFAULT_PORT = 65432        # Port to listen on (non-privileged ports are > 1023)

    def __init__(self, queue, host=DEFAULT_HOST, port=DEFAULT_PORT):
        threading.Thread.__init__(self)

        # gui thread communication
        self.queue = queue

        # listening socket
        self.host = host
        self.port = port
        self.lsock = None

        # select wrapper class
        self.sel = None

        # mapping ident -> Connection object
        self.connections = {}

        # flags
        self.running = True
        self.done = False

    def run(self):
        print('Starting server on (%s, %s)' % (self.host, self.port))

        # create selector object, listening socket
        self.sel = selectors.DefaultSelector()
        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lsock.bind((self.host, self.port))
        self.lsock.listen()
        self.lsock.setblocking(False)

        # register listening socket and comm. queue in select
        self.sel.register(self.lsock, selectors.EVENT_READ, data=None)
        self.sel.register(self.queue, selectors.EVENT_READ)

        log.debug('listening on (%s, %s)' % (self.host, self.port))
        while self.running:
            events = self.sel.select(timeout=self.TIMEOUT)
            for key, mask in events:
                if key.fileobj is self.queue:
                    #  handle gui messages here
                    self.handle_queue()
                    continue

                if key.fileobj is self.lsock:
                    self.accept_wrapper(key.fileobj)
                else:
                    self.service_connection(key, mask)
        print('Stopping server')
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
            except ConnectionRefusedError:
                print('Could not establish connection to (%s, %s)' % (host, port))
        elif cmd == 'send_msg':
            sleep(1)
        elif cmd == 'broadcast':
            for conn in self.connections.values():
                conn[1].send(Message(payload))
        else:
            print('Unknown command %s' % cmd)

    def reset(self):
        self.lsock.close()
        self.sel.close()
        for _, (sock, _) in self.connections.items():  # close all sockets
            sock.close()
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

    def accept_wrapper(self, sock):
        conn, addr = sock.accept()  # Should be ready to read
        log.debug('accepted connection from %s' % str(addr))
        conn.setblocking(False)
        data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.sel.register(conn, events, data=data)

        ident = util.get_key(*conn.getpeername())
        self.connections[ident] = (conn, Connection(self.queue, self.connections, ident, addr[0], addr[1]))

    def service_connection(self, key, mask):
        sock = key.fileobj
        data = key.data
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
                print('Connection reset/aborted: %s' % ident)
                self.sel.unregister(sock)
                sock.close()
                del self.connections[ident]
        if mask & selectors.EVENT_WRITE:
            if connection.has_data():
                log.debug('echoing %s to %s' % (repr(connection.out), data.addr))
                sent = sock.send(connection.out)  # Should be ready to write
                connection.out = connection.out[sent:]
