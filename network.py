import logging as log
import random, selectors, socket, types

import miniupnpc

from connection import Connection
from message import Message


# Wrapper for socket API and uPnP:
# - listening for incoming connectionst
# - establishing connections to peers
class NetworkNode:

    DEFAULT_PORT = 63002
    RECV_BUFFER = 2048

    def __init__(self, selector):
        # upnp port forwarding config
        self.upnp = miniupnpc.UPnP()
        self.upnp.discoverdelay = 10
        self.upnp.discover()
        self.upnp.selectigd()
        host = self.upnp.lanaddr  # TODO: register port forwarding on local address but give peers global address
        port = self.DEFAULT_PORT
        self.register_forwarding(host, port)
        self.listen_sock = create_listening_socket(host, port)

        self.selector = selector
        self.selector.register(self.listen_sock, selectors.EVENT_READ)

        self.connections = {}  # map address -> Connection object
        self.done = False

    def accept_incoming_connection(self):
        sock, addr = self.listen_sock.accept()
        log.debug('accepted connection from %s' % str(addr))
        sock.setblocking(False)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.selector.register(sock, events)
        self.connections[addr] = Connection(sock)

    def service_connection(self, sock, mask):
        addr = sock.getpeername()
        conn = self.connections[addr]
        if mask & selectors.EVENT_READ:
            try:
                recv_data = sock.recv(self.RECV_BUFFER)
                if recv_data:
                    yield from conn.handle_data(recv_data)
                else:  # connection closed
                    log.debug('closing connection to %s' % str(addr))
                    self.close_connection(addr)
            except (ConnectionResetError, ConnectionAbortedError):
                print('Connection reset/aborted:', addr)
                self.close_connection(addr)
        if mask & selectors.EVENT_WRITE:
            conn.flush_out_buffer()

    def synchronize_peer(self, peer_addr, peer_listen_addr):
        conn = self.connections[peer_addr]
        conn.remote_listen_addr = peer_listen_addr
        # ignore self and non-open connections -> connections to those will be made when they are opened themselves
        neighbors = filter(lambda c: c != self and c.is_synchronized(), self.connections.values())
        conn.send(Message({
            'do': 'connect_to',
            'hosts': [c.remote_listen_addr for c in neighbors]
        }))
        conn._is_synchronized = True

    def reset(self):
        self.selector.close()
        self.listen_sock.close()
        for conn in self.connections.values():
            conn.sock.close()
        self.remove_forwarding()
        self.connections = {}
        self.listen_sock = None
        self.selector = None
        self.done = True

    def connect_to_node(self, addr):
        print('Trying to connect:', addr)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(addr)
            events = selectors.EVENT_READ | selectors.EVENT_WRITE
            self.selector.register(sock, events)

            conn = Connection(sock, known=True)
            self.connections[addr] = conn
            conn.send(Message({
                'do': 'hello',
                'listen_addr': self.listen_sock.getsockname(),
            }))
        except (ConnectionRefusedError, ConnectionAbortedError, TimeoutError) as e:
            print('Could not establish connection to (%s, %s):' % addr, e)

    def close_connection(self, addr):
        self.selector.unregister(self.get_socket(addr))
        self.get_socket(addr).close()
        del self.connections[addr]

    def send(self, addr, payload):
        self.connections[addr].send(Message(payload))

    def broadcast(self, payload):
        print('NUMBER OF CONNECTIONS: ', len(self.connections))
        for conn in self.connections.values():
            conn.send(Message(payload))

    def register_forwarding(self, host, port):
        # addportmapping(external-port, protocol, internal-host, internal-port, description, remote-host)
        self.upnp.addportmapping(port, 'TCP', host, port, 'Naxos', '')

    def remove_forwarding(self):
        # deleteportmapping(external-port, protocol, description)
        self.upnp.deleteportmapping(self.port, 'TCP', 'Naxos')

    def is_done(self):
        return self.done

    def get_socket(self, addr):
        return self.connections[addr].sock

    def get_random_addr(self):
        return random.choice(list(self.connections.keys()))


def create_listening_socket(host, port):
    print('Start listening:', (host, port))
    log.debug('listening on (%s, %s)' % (host, port))
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((host, port))
        sock.listen()
        sock.setblocking(False)
        return sock
    except Exception as e:
        print('Could not open listening socket:', e)
