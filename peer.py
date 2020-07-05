#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Overlay P2P network of peers running paxos to maintain a consistent index."""

import copy
import logging as log
import random
import selectors
import sys
import time
from threading import Thread

from cache import Cache
from index import Index
from message import Message
from network import NetworkNode
from paxos import PaxosNode
from periodic_runner import PeriodicRunner
import util


def watch_address_pool(context: dict):
    log.debug("Checking address pool...")
    net = context['network']
    pool = net.address_pool
    old = context['old']
    queue = context['queue']

    diff = pool - old
    if diff == set():
        picked = [addr for addr in random.sample(pool, min(len(pool), 2)) if not net.is_connected(addr)]
        queue.put(('connect_to_sampled', {
            'picked': picked
        }))
    else:
        context['old'] = copy.copy(pool)


class Peer(Thread):
    """Overlay network peer maintaining a network node, paxos instances, and an index.
    Serves as an index server to the clients.
    Coordinates with peers through paxos to maintain a consistent index.
    """

    SELECT_TIMEOUT = 1
    VERSION = '0.2.0'

    def __init__(self, first=False):
        """"""
        super().__init__()  # Thread constructor

        self.queue = util.PollableQueue()
        self.selector = selectors.DefaultSelector()
        self.cache = Cache()
        self.index = Index()
        self.network = NetworkNode(self.selector, self.cache)
        
        if first:
            self.paxos = PaxosNode(self.network)
            paxos_header = '- This peer is now operating Paxos node %s' % self.paxos.node_id()
        else:
            self.paxos = None
            paxos_header = '- This peer is not yet operating as a Paxos node.'
            
        self.periodic_runner = PeriodicRunner()
        self.periodic_runner.start()
        
        naxos_header = '- Running Naxos v%s' % self.VERSION
        net_header = '- This peer is listening for incoming connections on: %s:%s' % self.network.listen_addr
        mid = '='*max(len(naxos_header), len(net_header), len(paxos_header))

        print('\n'.join((mid, naxos_header, net_header, paxos_header, mid)))
        
        self.running = True

        self.selector.register(self.queue, selectors.EVENT_READ)

    def run(self):  # called by Thread.start()
        """Main loop: Handles incoming messages and commands sent from main thread."""
        try:
            while self.running:
                events = self.selector.select(timeout=self.SELECT_TIMEOUT)
                for key, mask in events:
                    if key.fileobj is self.queue:
                        self.handle_queue()
                    elif key.fileobj is self.network.listen_sock:
                        self.network.accept_incoming_connection()
                    else:
                        for msg in self.network.service_connection(key.fileobj, mask):
                            self.cache.update_routing(key.fileobj.getpeername(), msg)
                            self.handle_message(key.fileobj, msg)
                            self.cache.processed(key.fileobj.getpeername(), msg)
        finally:
            log.info('Shutting down this peer...')
            self.periodic_runner.stop()
            self.network.reset()

    def handle_queue(self):
        """Handles the next incoming message on the thread-safe queue.
        The message was sent either by this thread or the CLI (stdin) thread.
        """
        cmd, payload = self.queue.get()
        if cmd == 'connect':
            self.network.connect_to_node(payload['addr'], 'paxos_join_request')
            # TODO: add to self.paxos.peer_addresses
        elif cmd == 'first_connection':
            self.network.connect_to_node(payload['addr'])
            self.network.broadcast({
                'do': 'ping',
            })
            self.periodic_runner.register(watch_address_pool, {
                'queue': self.queue,
                'network': self.network,
                'old': set(),
            }, 3)
        elif cmd == 'connect_to_sampled':
            picked = payload['picked']
            log.debug("picked: %s", picked)
            for addr in picked:
                self.network.connect_to_node(addr)
            self.periodic_runner.unregister(watch_address_pool)

        elif cmd == 'start_paxos':
            if self.paxos is not None:
                self.paxos.start_paxos_round(payload['value'])
        else:
            raise ValueError('Unknown command: %s' % cmd)

    def handle_message(self, sock, msg):
        """Handles the Message msg, which arrived at the socket sock.
        The message might have been sent by another paxos peer or a client.
        """
        log.info('[IN]:\t%s' % msg)
        if self.cache.seen(msg):
            print("Skip handling message since it has already been handled.")
            return

        cmd = msg['do']
        to = msg['to']


        # forward messages that are not for this peer.
        # if it is a broadcast, cmd handling should call broadcast.
        if to not in (self.network.unique_id_from_own_addr(), 'broadcast'):
            log.info("Forward message")
            self.network.send(to, msg)
            return

        # ping broadcast to discover peers in the network
        if cmd == 'ping':
            self.network.send(msg['from'], {
                'do': 'ping_response',
                'addr': self.network.listen_addr,
            })
            self.network.broadcast(msg, sock)
        
        # response to a ping broadcast to the one who initially sent the ping, delivering own addr
        elif cmd == 'ping_response':
            # has to be for this peer, since it would have been forwardet otherwise.
            self.network.address_pool.add(tuple(msg['addr']))  # remember this addr
        
        # broadcast to analyze the overlay structure
        elif cmd == 'discover_overlay':
            self.network.send(msg['from'], {
                'do': 'discover_overlay_response',
                'neighbours': [conn.get_identifier() for conn in self.network.connections.values() if not conn.is_client()]
            })
            self.network.broadcast(msg, sock)

        if cmd == 'paxos_join_confirm':
            self.paxos = PaxosNode(self.network, msg['group_size'], msg['leader'])
            self.paxos.add_peer_addr(msg['my_node_id'], sock.getpeername())
            self.index.from_json(msg['index'])
            peers = [tuple(p) for p in msg['peers']]
            for node_id, listen_addr in peers:
                addr = self.network.connect_to_node(tuple(listen_addr))
                self.paxos.add_peer_addr(node_id, addr)

        # elif self.paxos is None:  # do not handle other Messages if not yet part of Paxos
        #     if self.network.connections:
        #         self.network.send(sock.getpeername(), {
        #             'do': 'try_other_peer',
        #             'addr': self.network.get_random_listen_addr(),
        #         })

        elif cmd == 'hello':
            self.network.set_remote_listen_addr(sock, tuple(msg['listen_addr']))
        elif cmd == 'client_hello':
            self.network.set_http_addr(sock, tuple(msg['http_addr']))
            # self.network.set_remote_listen_addr(sock, tuple(msg['http_addr']))

        elif cmd == 'paxos_join_request':
            self.network.set_remote_listen_addr(sock, tuple(msg['listen_addr']))
            if self.paxos.group_size == 1:
                self.paxos.group_size += 1
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
            self.paxos.handle_propose(sock.getpeername(), tuple(msg['id']), msg['value'])
        elif cmd == 'paxos_accept':
            chosen_value = self.paxos.handle_accept(tuple(msg['id']))
            if chosen_value is not None:
                self.apply_chosen_value(chosen_value, self_started_round=True)
        elif cmd == 'paxos_learn':
            self.apply_chosen_value(msg['value'])
            self.paxos.handle_learn(tuple(msg['id']), msg['value'])

        elif cmd == 'index_search':
            self.network.send(sock.getpeername(), {
                'do': 'index_search_result',
                'query': msg['filename'],  # in case of multiple searches, out of order...
                'addr': self.index.search_entry(msg['filename']),
            })
        elif cmd == 'index_add':
            addr = self.network.get_http_addr(sock)
            if self.paxos.group_size == 1:
                self.index.add_entry(msg['filename'], addr)
            else:
                self.run_paxos({
                    'change': 'add',
                    'entry': msg['filename'],
                    'addr': addr,
                })
        elif cmd == 'index_remove':
            if self.paxos.group_size == 1:
                self.index.remove_entry(msg['filename'])
            else:
                self.run_paxos({
                    'change': 'remove',
                    'entry': msg['filename'],
                })

    def stop(self):
        self.running = False

    def connect_to_paxos(self, addr):
        """Adds a 'connect' command to the command queue."""
        self.queue.put(('connect', {
            'addr': addr,
        }))

    def run_paxos(self, value):
        """Adds a 'start_paxos' command to the command queue."""
        self.queue.put(('start_paxos', {
            'value': value,
        }))

    def apply_chosen_value(self, value, self_started_round=False):
        """Applies the changes needed after selecting value through paxos.
        Might handle these changes differently based on whether we started the paxos round.
        """
        if value['change'] == 'join':
            self.paxos.group_size += 1
            conn_addresses = [c.sock.getpeername() for c in self.network.connections.values()]
            if tuple(value['respond_addr']) in conn_addresses:
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
            'group_size': self.paxos.group_size,
            'leader': self.paxos.current_leader,
            'index': self.index.to_json(),
            'peers': peers,
        })

    def connect(self, addr):
        self.queue.put(('first_connection', {
            'addr': addr,
        }))


if __name__ == '__main__':
    NUM_ARGS = len(sys.argv)
    if NUM_ARGS not in [1, 3]:
        sys.exit('Usage: python peer.py (ip port)')

    log.basicConfig(level=log.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename='debug.log',
                        filemode='w')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = log.StreamHandler()
    console.setLevel(log.INFO)
    # set a format which is simpler for console use
    formatter = log.Formatter('%(asctime)s %(message)s\n', '%H:%M')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    log.getLogger('').addHandler(console)

    peer = Peer(NUM_ARGS == 1)
    peer.start()
    if NUM_ARGS == 3:
        addr = (sys.argv[1], int(sys.argv[2]))
        peer.connect(addr)
        # peer.connect_to_paxos((sys.argv[1], int(sys.argv[2])))
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        peer.stop()
