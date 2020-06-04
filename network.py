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

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT):
        threading.Thread.__init__(self)
        self.running = True
        self.done = False
        self.host = host
        self.port = port
        self.lsock = None
        self.sel = None
        self.connections = {}

    def set_connection_pair(self, host, port):
        self.host = host
        self.port = port

    def run(self):
        print("Starting server on (%s, %s)" % (self.host, self.port))
        self.sel = selectors.DefaultSelector()
        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lsock.bind((self.host, self.port))
        self.lsock.listen()
        self.lsock.setblocking(False)
        self.sel.register(self.lsock, selectors.EVENT_READ, data=None)
        log.debug('listening on (%s, %s)' % (self.host, self.port))

        while self.running:
            events = self.sel.select(timeout=self.TIMEOUT)
            for key, mask in events:
                if key.data is None:
                    self.accept_wrapper(key.fileobj)
                else:
                    self.service_connection(key, mask)
        self.lsock.close()
        self.sel.close()
        self.reset()
        print("Stopping server")

    def reset(self):
        self.lsock = None
        self.sel = None
        for key, (sock, _) in self.connections.items():
            sock.close()
        self.connections = {}
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
        # connection = self.connections[ident]
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