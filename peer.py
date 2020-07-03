#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Overlay P2P network of peers running paxos to maintain a consistent index."""

import logging as log
import selectors
import sys
import time
from threading import Thread

from index import Index
from message import Message
from network import NetworkNode
from paxos import PaxosNode


class Peer(Thread):
    """Overlay network peer maintaining a network node, paxos instances, and an index.
    Serves as an index server to the clients.
    Coordinates with peers through paxos to maintain a consistent index.
    """

    SELECT_TIMEOUT = 1
    VERSION = '0.3.0'

    def __init__(self, first=True, addr=None):
        """"""
        super().__init__()  # Thread constructor

        self.selector = selectors.DefaultSelector()
        self.network = NetworkNode(self.selector)
        if first:
            self.paxos = PaxosNode(self.network)
        else:
            self.paxos = None
            self.connect_to_paxos(addr)
        self.index = Index()
        self._running = True

    def run(self):  # called by Thread.start()
        """Main loop: Handles incoming messages from other peers and commands from main thread."""
        print('Running Naxos v' + self.VERSION)
        try:
            while self._running:
                events = self.selector.select(timeout=self.SELECT_TIMEOUT)
                for key, mask in events:
                    if key.fileobj is self.network.listen_sock:
                        self.network.accept_incoming_connection()
                    else:
                        for msg in self.network.service_connection(key.fileobj, mask):
                            self.handle_message(key.fileobj, msg)
        finally:
            print('Shutting down this peer...')
            self.network.reset()

    def handle_message(self, sock, msg):
        """Handles the Message msg, which arrived at the socket sock.
        The message might have been sent by another paxos peer or a client.
        """
        print('[IN]:\t%s' % msg)

        cmd = msg['do']
        if cmd == 'paxos_join_confirm':
            self.paxos = PaxosNode(self.network, msg['group_size'], msg['leader'])
            self.paxos.add_peer_addr(msg['my_node_id'], sock.getpeername())
            self.index.from_json(msg['index'])
            peers = [tuple(p) for p in msg['peers']]
            for node_id, listen_addr in peers:
                addr = self.network.connect_to_node(tuple(listen_addr))
                self.paxos.add_peer_addr(node_id, addr)

        elif self.paxos is None:  # do not handle other Messages if not yet part of Paxos
            if self.network.connections:
                self.network.send(sock.getpeername(), Message({
                    'do': 'try_other_peer',
                    'addr': self.network.get_random_listen_addr(),
                }))

        elif cmd == 'hello':
            self.network.set_remote_listen_addr(sock, tuple(msg['listen_addr']))
        elif cmd == 'client_hello':
            self.network.set_http_addr(sock, tuple(msg['http_addr']))

        elif cmd == 'paxos_join_request':
            self.network.set_remote_listen_addr(sock, tuple(msg['listen_addr']))
            if self.paxos.group_sizes[self.paxos.next_index] == 1:
                self.paxos.group_sizes[self.paxos.next_index] += 1
                self.send_paxos_join_confirmation(sock.getpeername())
            else:
                self.run_paxos({
                    'change': 'join',
                    'respond_addr': sock.getpeername(),
                    'listen_addr': tuple(msg['listen_addr']),
                })
        elif cmd == 'paxos_relay':
            self.paxos.start_paxos_round(msg['value'])
        elif cmd == 'paxos_prepare':
            self.paxos.handle_prepare(sock.getpeername(), tuple(msg['id']))
        elif cmd == 'paxos_promise':
            self.paxos.handle_promise(tuple(msg['id']), msg['accepted'])
        elif cmd == 'paxos_propose':
            self.paxos.handle_propose(sock.getpeername(), tuple(msg['id']), msg['index'], msg['value'])
        elif cmd == 'paxos_accept':
            index, chosen_value = self.paxos.handle_accept(tuple(msg['id']), msg['index'])
            if chosen_value is not None:
                self.apply_chosen_value(index, chosen_value)
        elif cmd == 'paxos_learn':
            self.apply_chosen_value(msg['index'], msg['value'])
            self.paxos.handle_learn(tuple(msg['id']), msg['index'], msg['value'])

        elif cmd == 'index_search':
            self.network.send(sock.getpeername(), {
                'do': 'index_search_result',
                'query': msg['filename'],  # in case of multiple searches, out of order...
                'addr': self.index.search_entry(msg['filename']),
            })
        elif cmd == 'index_add':
            addr = self.network.get_http_addr(sock)
            if self.paxos.group_sizes[self.paxos.next_index] == 1:
                self.index.add_entry(msg['filename'], addr)
            else:
                self.run_paxos({
                    'change': 'add',
                    'entry': msg['filename'],
                    'addr': addr,
                })
        elif cmd == 'index_remove':
            if self.paxos.group_sizes[self.paxos.next_index] == 1:
                self.index.remove_entry(msg['filename'])
            else:
                self.run_paxos({
                    'change': 'remove',
                    'entry': msg['filename'],
                })

    def stop(self):
        """Stop this Peer's thread from running, initiating clean shutdown."""
        self._running = False

    def connect_to_paxos(self, addr):
        """."""
        self.network.connect_to_node(addr, 'paxos_join_request')

    def run_paxos(self, value):
        """."""
        if self.paxos is not None:
            self.paxos.start_paxos_round(value)

    def apply_chosen_value(self, index, value):
        """Applies the changes needed after selecting value through paxos.
        Might handle these changes differently based on whether we started the paxos round.
        """
        if len(self.paxos.group_sizes) > index:
            self.paxos.group_sizes.append(self.paxos.group_sizes[index] + 1)
            while len(self.paxos.accepted_values) > index and self.paxos.accepted_values[index] is not None:
                if self.paxos.accepted_values[index]['change'] == 'join':
                    self.paxos.group_sizes.append(self.paxos.group_sizes[index+1] + 1)
                else:
                    self.paxos.group_sizes.append(self.paxos.group_sizes[index+1])
                index += 1

        if value['change'] == 'join':
            conn_addresses = [c.sock.getpeername() for c in self.network.connections.values()]
            if tuple(value['respond_addr']) in conn_addresses:
                print(self.network.listen_addr)
                self.send_paxos_join_confirmation(tuple(value['respond_addr']))
        elif value['change'] == 'add':
            self.index.add_entry(value['entry'], value['addr'])
        elif value['change'] == 'remove':
            self.index.remove_entry(value['entry'])

    def send_paxos_join_confirmation(self, addr):
        """Sends a confirmation for joining the paxos overlay network to addr.
        All necessary information about the curent state of paxos is included.
        """
        # TODO: maybe remove 'is not None'
        peers = [(n, self.network.connections[a].remote_listen_addr)
                 for n, a in self.paxos.peer_addresses.items()
                 if a != addr and self.network.connections[a].remote_listen_addr is not None]
        self.network.send(addr, {
            'do': 'paxos_join_confirm',
            'my_node_id': self.paxos.node_id(),
            'group_size': self.paxos.group_sizes[self.paxos.next_index],  # TODO: maybe remove (unnecessary)
            'leader': self.paxos.current_leader,
            'index': self.index.to_json(),
            'peers': peers,
        })


if __name__ == '__main__':
    NUM_ARGS = len(sys.argv)
    if NUM_ARGS not in [1, 3]:
        sys.exit('Usage: python peer.py (ip port)')

    log.basicConfig(level=log.DEBUG, filename='debug.log')
    if NUM_ARGS == 1:
        peer = Peer()
    elif NUM_ARGS == 3:
        peer = Peer(False, (sys.argv[1], int(sys.argv[2])))
    peer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        peer.stop()
