#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Overlay P2P network of peers running paxos to maintain a consistent index."""

import copy
import ipaddress
import logging as log
import random
import re
import selectors
import sys
import time
from threading import Thread

from cache import Cache
from index import Index
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
    KEEPALIVE_TIMEOUT = 5
    VERSION = '0.3.0'

    def __init__(self, first=True, addr=None):
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
            self.connect_to_paxos(addr)
            self.queue.put(('first_connection', {}))
            paxos_header = '- This peer is not yet operating as a Paxos node.'
        self.last_keepalive = time.time()
        self.peer_keepalives = {}
        self._running = True
        self.periodic_runner = PeriodicRunner()
        self.periodic_runner.start()

        naxos_header = '- Running Naxos v%s' % self.VERSION
        net_header = '- This peer is listening for incoming connections on: %s:%s' % self.network.listen_addr
        mid = '='*max(len(naxos_header), len(net_header), len(paxos_header))

        print('\n'.join((mid, naxos_header, net_header, paxos_header, mid)))
        self.selector.register(self.queue, selectors.EVENT_READ)

    def run(self):  # called by Thread.start()
        """Main loop: Handles incoming messages and commands sent from main thread."""
        try:
            while self._running:
                events = self.selector.select(timeout=self.SELECT_TIMEOUT)
                if self.paxos is not None:
                    self.check_keepalive_timeouts()
                for key, mask in events:
                    if key.fileobj is self.queue:
                        self.handle_queue()
                    elif key.fileobj is self.network.listen_sock:
                        self.network.accept_incoming_connection()
                    else:
                        for msg in self.network.service_connection(key.fileobj, mask):
                            self.cache.update_routing(key.fileobj.getpeername(), msg)
                            self.handle_message(self.network.get_connection(key.fileobj.getpeername()), msg)
                            self.cache.processed(key.fileobj.getpeername(), msg)
        finally:
            log.info('Shutting down this peer...')
            self.periodic_runner.stop()
            self.network.reset()

    def check_keepalive_timeouts(self):
        """Check if any peer timed out long enough to assume they are offline."""
        current_time = time.time()
        leader_id = self.paxos.current_leader
        if self.paxos.is_leader():
            for (node_id, last_keepalive) in self.peer_keepalives.items():
                if current_time - last_keepalive > 3 * self.KEEPALIVE_TIMEOUT:
                    self.paxos.start_paxos_round({
                        'change': 'leave',
                        'node_id': node_id,
                    })
                    self.peer_keepalives[node_id] = current_time - self.KEEPALIVE_TIMEOUT
        elif leader_id in self.peer_keepalives:
            last_leader_keepalive = self.peer_keepalives[leader_id]
            if current_time - last_leader_keepalive > 3 * self.KEEPALIVE_TIMEOUT:  # TODO: randomize timeout
                self.paxos.start_election()
                self.peer_keepalives[leader_id] = current_time - self.KEEPALIVE_TIMEOUT

        if current_time - self.last_keepalive > self.KEEPALIVE_TIMEOUT:
            if self.paxos.is_leader():
                self.network.broadcast({'do': 'keepalive', 'node_id': self.paxos.node_id()})
            else:
                self.network.send(leader_id,
                                  {'do': 'keepalive', 'node_id': self.paxos.node_id()})
            self.last_keepalive = current_time

    def handle_queue(self):
        """Handles the next incoming message on the thread-safe queue.
        The message was sent either by this thread or the CLI (stdin) thread.
        """
        cmd, payload = self.queue.get()
        if cmd == 'connect':
            self.network.connect_to_node(payload['addr'], 'paxos_join_request')
            # TODO: add to self.paxos.peer_addresses
        elif cmd == 'first_connection':
            # self.network.connect_to_node(payload['addr'])
            self.network.broadcast({
                'do': 'ping',
            })
            self.periodic_runner.register(watch_address_pool, {
                'queue': self.queue,
                'network': self.network,
                'old': set(),
            }, 0.5)
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

    def handle_message(self, conn, msg):
        """Handles the Message msg, which arrived at the socket sock.
        The message might have been sent by another paxos peer or a client.
        """
        log.info('[IN]:\t%s' % msg)
        if self.cache.seen(msg):
            log.info("Skip handling message since it has already been handled.")
            return

        sock = conn.sock
        cmd = msg['do']
        to = msg['to']
        src = msg['from']

        # forward messages that are not for this peer.
        # if it is a broadcast, cmd handling should call broadcast.
        if to not in (self.network.unique_id_from_own_addr(), 'broadcast'):
            log.info("Forward message")
            self.network.send(to, msg)
            return

        # ping broadcast to discover peers in the network
        if cmd == 'ping':
            self.network.send(src, {
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
            self.network.send(src, {
                'do': 'discover_overlay_response',
                'neighbours': [conn.get_identifier() for conn in self.network.connections.values()
                               if not conn.is_client()]
            })
            self.network.broadcast(msg, sock)

        if cmd == 'paxos_join_confirm':
            self.paxos = PaxosNode(self.network, msg['leader'])
            self.network.send(self.paxos.current_leader,
                              {'do': 'keepalive', 'node_id': self.paxos.node_id()})

        elif self.paxos is None:  # do not handle other Messages if not yet part of paxos
            if self.network.connections:
                self.network.send(src, {
                    'do': 'try_other_peer',
                    'addr': self.network.get_random_listen_addr(),
                })

        elif cmd == 'hello':
            self.network.set_remote_listen_addr(sock, tuple(msg['listen_addr']))
        elif cmd == 'client_hello':
            self.network.set_http_addr(sock, tuple(msg['http_addr']))
        elif cmd == 'keepalive':
            self.peer_keepalives[msg['node_id']] = time.time()
            log.debug(self.peer_keepalives)

        elif cmd == 'paxos_join_request':
            self.network.set_remote_listen_addr(sock, tuple(msg['listen_addr']))
            index = self.paxos.get_last_applied_value_index()
            if self.paxos.group_sizes[index] == 1:
                self.paxos.group_sizes[index] += 1
                self.send_paxos_join_confirmation(src)
            else:
                self.run_paxos({
                    'change': 'join',
                    'node_id': src,
                    'listen_addr': tuple(msg['listen_addr']),
                })
        elif cmd == 'paxos_relay':
            self.paxos.start_paxos_round(msg['value'])
        elif cmd == 'paxos_prepare':
            self.paxos.handle_prepare(src, tuple(msg['proposal_id']))
        elif cmd == 'paxos_promise':
            self.paxos.handle_promise(tuple(msg['proposal_id']), msg['acc_id'],
                                      msg['accepted'], msg['majority'])
        elif cmd == 'paxos_propose':
            self.paxos.handle_propose(src, tuple(msg['proposal_id']), msg['index'], msg['value'])
        elif cmd == 'paxos_accept':
            index, chosen_value = self.paxos.handle_accept(tuple(msg['proposal_id']), msg['index'])
            if chosen_value is not None:
                self.update_paxos_log_and_apply_value(index)
        elif cmd == 'paxos_learn':  # TODO: handle duplicate learns
            if len(self.paxos.chosen) <= msg['index'] or not self.paxos.chosen[msg['index']]:
                self.paxos.handle_learn(msg['index'], msg['value'])
                self.update_paxos_log_and_apply_value(msg['index'])
        elif cmd == 'paxos_fill_log_hole':
            self.network.send(src, {
                'do': 'paxos_learn',
                'index': msg['index'],
                'value': self.paxos.log[msg['index']],
            })

        elif cmd == 'index_search':
            self.network.send(src, {
                'do': 'index_search_result',
                'query': msg['filename'],  # in case of multiple searches, out of order...
                'addr': self.index.search_entry(msg['filename']),
            })
        elif cmd == 'index_add':
            addr = self.network.get_http_addr(sock)
            log.debug(self.paxos.log)
            # if self.paxos.group_sizes[len(self.paxos.log)] == 1:
            if self.paxos.group_sizes[self.paxos.get_last_applied_value_index()] == 1:
                self.index.add_entry(msg['filename'], addr)
            else:
                self.run_paxos({
                    'change': 'add',
                    'entry': msg['filename'],
                    'addr': addr,
                })
        elif cmd == 'index_remove':
            if self.paxos.group_sizes[self.paxos.get_last_applied_value_index()] == 1:
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

    def update_paxos_log_and_apply_value(self, index):
        """."""
        if self.paxos.fix_log_holes(index):
            while len(self.paxos.chosen) > index and self.paxos.chosen[index]:
                self.apply_chosen_value(index, self.paxos.log[index])
                index += 1

    def apply_chosen_value(self, index, value):
        """Applies the changes needed after selecting value through paxos.
        Might handle these changes differently based on whether we started the paxos round.
        """
        assert len(self.paxos.group_sizes) > index
        if value['change'] == 'join':
            self.paxos.group_sizes.append(self.paxos.group_sizes[index] + 1)
        elif value['change'] == 'leave':
            self.paxos.group_sizes.append(self.paxos.group_sizes[index] - 1)
        else:
            self.paxos.group_sizes.append(self.paxos.group_sizes[index])

        if value['change'] == 'join':
            if self.paxos.is_leader():
                self.send_paxos_join_confirmation(value['node_id'])
        elif value['change'] == 'leave':
            if self.paxos.is_leader():
                self.peer_keepalives.pop(value['node_id'], None)
        elif value['change'] == 'add':
            self.index.add_entry(value['entry'], value['addr'])
        elif value['change'] == 'remove':
            self.index.remove_entry(value['entry'])

    def send_paxos_join_confirmation(self, node_id):
        """Sends a confirmation for joining the paxos overlay network to addr.
        All necessary information about the curent state of paxos is included.
        """
        self.network.send(node_id, {
            'do': 'paxos_join_confirm',
            'my_node_id': self.paxos.node_id(),
            'leader': self.paxos.current_leader,
        })

    def connect(self, addr):
        self.queue.put(('first_connection', {
            'addr': addr,
        }))


if __name__ == '__main__':
    NUM_ARGS = len(sys.argv)
    if NUM_ARGS not in [1, 2]:
        sys.exit('Usage: python peer.py (ip:port)')

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

    if NUM_ARGS == 1:
        peer = Peer()
    elif NUM_ARGS == 2:
        try:
            host, port = sys.argv[1].split(':')
            pattern = r'^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$'  # ipv4
            ret = util.is_public_ip(host)  # returns True or str
            if not ret is True:
                sys.exit(ret)
        except (IndexError, ValueError):
            sys.exit('Usage: python peer.py (ip:port)')
        peer = Peer(False, (host, int(port)))
    peer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        peer.stop()
