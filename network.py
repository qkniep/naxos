import selectors
import socket
import threading
import types
import logging as log

import util
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

    def set_connection_pair(self, host, port):
        self.host = host
        self.port = port

    def run(self):
        print("Starting server on (%s, %s)" % (self.host, self.port))

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
                    continue

                if key.data is None:
                    self.accept_wrapper(key.fileobj)
                else:
                    self.service_connection(key, mask)
        print("Stopping server")
        self.reset()

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

        ident = util.get_key(conn)
        self.connections[ident] = (conn, Connection(self.connections, ident))

    def service_connection(self, key, mask):
        sock = key.fileobj
        data = key.data
        ident = util.get_key(sock)
        connection = self.get_connection(ident)
        if mask & selectors.EVENT_READ:
            recv_data = sock.recv(1024)  # Should be ready to read
            if recv_data:
                connection.handle_data(recv_data)
            else:  # connection closed
                log.debug('closing connection to %s' % str(data.addr))
                self.sel.unregister(sock)
                sock.close()
                del self.connections[ident]
        if mask & selectors.EVENT_WRITE:
            if connection.has_data():
                log.debug('echoing %s to %s' % (repr(connection.out), data.addr))
                sent = sock.send(connection.out)  # Should be ready to write
                connection.out = connection.out[sent:]