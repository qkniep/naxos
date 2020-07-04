# -*- coding: utf-8 -*-
"""Network abstraction."""

import logging as log
import random
import selectors
import socket
import struct

import miniupnpc

from connection import Connection
from message import Message


class NetworkNode:
    """Wrapper for socket API and uPnP.

    Listen for incoming connections, establish connections to other network nodes.
    """

    DEFAULT_PORT = 63000
    RECV_BUFFER = 2048

    def __init__(self, selector, cache):
        """Create new network node with uPnP forwarding and a listening socket.
        All opened sockets are registered with selector (select wrapper).
        """
        # upnp port forwarding config
        self.upnp = miniupnpc.UPnP()
        self.upnp.discoverdelay = 10
        self.upnp.discover()
        self.upnp.selectigd()

        host = self.upnp.lanaddr
        self.listen_sock, port = create_listening_socket(host, self.DEFAULT_PORT)
        self.listen_addr = (self.upnp.externalipaddress(), port)
        print('This peer is listening for incoming connections on:', self.listen_addr)

        self.register_forwarding(host, port)
        self.port = port

        self.selector = selector
        self.selector.register(self.listen_sock, selectors.EVENT_READ)

        self.cache = cache
        self.address_pool = set()

        self.connections = {}  # map address -> Connection object
        self.done = False

    def accept_incoming_connection(self):
        """Accept an incoming connection on the listeing socket,
        register it with the selector and add it to the connections map.
        """
        sock, addr = self.listen_sock.accept()
        log.debug('accepted connection from %s', str(addr))
        sock.setblocking(False)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.selector.register(sock, events)
        self.connections[addr] = Connection(sock)

    def service_connection(self, sock, mask):
        """Handle a single connection represented by the socket sock.
        Close the connection if it failed or our peer terminated it.
        Yield messages arriving on the socket.
        """
        addr = sock.getpeername()
        conn = self.connections[addr]
        if mask & selectors.EVENT_READ:
            try:
                recv_data = sock.recv(self.RECV_BUFFER)
                if recv_data:
                    yield from conn.handle_data(recv_data)
                else:  # connection closed by peer
                    log.debug('closing connection to %s', str(addr))
                    self.close_connection(addr)
            except (ConnectionResetError, ConnectionAbortedError):
                print('Connection reset/aborted:', addr)
                self.close_connection(addr)
        if mask & selectors.EVENT_WRITE:
            conn.flush_out_buffer()

    def reset(self):
        """Reset everything to the state after the node was created."""
        self.selector.close()
        self.listen_sock.close()
        for conn in self.connections.values():
            conn.sock.close()
        self.remove_forwarding()
        self.connections = {}
        self.listen_sock = None
        self.selector = None
        self.done = True

    def connect_to_node(self, addr, first_message='hello'):
        """Try to connect to another network node under the address addr.
        Send a Message of type first_message after successfully connecting.
        This first message also contains this node's listening address.

        Returns:
            Remote address of the new socket on success, None on failure.
        """
        print('Trying to connect:', addr)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(addr)
            events = selectors.EVENT_READ | selectors.EVENT_WRITE
            self.selector.register(sock, events)

            conn = Connection(sock, known=True)
            self.connections[addr] = conn
            self.send(addr, {
                'do': first_message,
                'listen_addr': self.listen_addr,
            })
            return addr
        except (ConnectionRefusedError, ConnectionAbortedError, TimeoutError) as error:
            print('Could not establish connection to %s: %s' % (addr, error))
            return None

    def close_connection(self, addr):
        """Closes a connection to another network node."""
        self.selector.unregister(self.get_socket(addr))
        self.get_socket(addr).close()
        del self.connections[addr]

    def send(self, to, payload):
        """Sends a message containing payload to the paxos peer."""
        sock = self.cache.route(to)
        if sock == 'broadcast':  # no route found for this naxos id
            self.broadcast(payload)
            return
        
        if 'from' not in payload:
            payload['from'] = self.unique_id_from_own_addr()
        if 'to' not in payload:
            payload['to'] = to
        addr = sock.getpeername()

        try:
            self.connections[tuple(addr)].send(Message(payload))
        except (ConnectionRefusedError, ConnectionAbortedError, TimeoutError) as error:
            print("Connection to %s aborted: %s. Fallback to broadcasting..." % (addr, error))
            self.broadcast(payload)

    def broadcast(self, payload, sock=None):
        """Sends a message containing payload to all other PEERS (non-client connections). If sock is not None, do not send it to that connection."""
        connections = [x for x in self.connections.values() if (not x.is_client() and x.sock != sock)]  # filter client and connection the message came from (alway true if sock is None)

        if 'from' not in payload:
            payload['from'] = self.unique_id_from_own_addr()
        if 'to' not in payload:
            payload['to'] = 'broadcast'

        for conn in connections:
            conn.send(Message(payload))

    def register_forwarding(self, host, port):
        """Adds a new port forwarding rule for the specified host name and port."""
        self.upnp.addportmapping(port, 'TCP', host, port, 'Naxos', '')

    def remove_forwarding(self):
        """Deletes the port forwarding rule for our port."""
        self.upnp.deleteportmapping(self.port, 'TCP', 'Naxos')

    # XXX deprecated?
    def is_done(self):
        return self.done

    def set_remote_listen_addr(self, sock, listen_addr):
        """Sets the remote_listen_addr of the remote paxos peer connected via sock."""
        conn = self.connections[sock.getpeername()]
        conn.remote_listen_addr = listen_addr

    def get_remote_listen_addr(self, sock):
        """Returns the remote_listen_addr of the remote paxos peer conencted via sock."""
        return self.connections[sock.getpeername()].remote_listen_addr

    def set_http_addr(self, sock, http_addr):
        """Sets the address where a client's HTTP server runs."""
        self.connections[sock.getpeername()].set_client(http_addr)

    def get_http_addr(self, sock):
        """Returns the address where a client's HTTP server runs."""
        conn = self.connections[sock.getpeername()]
        if not conn.is_client():
            raise Exception("Tried to get the HTTP-server address for a paxos peer.")
        return conn.http_addr

    def get_socket(self, addr):
        """Returns the socket corresponding to the connection based on the remote peer's address."""
        return self.connections[addr].sock

    def get_random_listen_addr(self):
        """Pick a network node from this node's connections and return their listen address."""
        return random.choice(list(self.connections.values())).remote_listen_addr

    def unique_id_from_own_addr(self):
        """Deterministically generates a single integer ID from this network node's address."""
        host, port = self.listen_addr
        ip_int = struct.unpack("!I", socket.inet_aton(host))[0]
        return ip_int * 65536 + port

    def is_connected(self, addr):
        return addr in self.connections

def create_listening_socket(host, port=0):
    """Create a new listening socket on this node.
    Select a free port if port is not explicitly set or already taken.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((host, port))
        sock.listen()
        sock.setblocking(False)
        _port = sock.getsockname()[1]
        # print('This peer is listening for incoming connections on:', (host, _port))
        log.debug('listening on (%s, %s)', host, _port)

        return sock, _port
    except Exception as exception:
        log.debug('could not open listening socket: %s', exception)
        return create_listening_socket(host)
