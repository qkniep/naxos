import logging as log
import random
import selectors
import sys
import socket
from threading import Thread

from index import Index
from message import Message
from network import NetworkNode
from paxos import PaxosNode
import util


class Peer(Thread):

    SELECT_TIMEOUT = 1
    VERSION = '0.2.0'

    def __init__(self, first=False):
        super().__init__()

        self.queue = util.PollableQueue()
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.queue, selectors.EVENT_READ)
        self.network = NetworkNode(self.selector)
        # TODO: use address (IP+Port) instead of PRNG to derive unique node_id
        if first:
            self.paxos = PaxosNode(self, self.network, random.getrandbits(128), 1)
        else:
            self.paxos = None
        self.index = Index()
        self.running = True

    def run(self):
        print('Running Naxos v' + self.VERSION)
        while self.running:
            events = self.selector.select(timeout=self.SELECT_TIMEOUT)
            for key, mask in events:
                if key.fileobj is self.queue:
                    self.handle_queue()
                elif key.fileobj is self.network.listen_sock:
                    self.network.accept_incoming_connection()
                else:
                    for msg in self.network.service_connection(key.fileobj, mask):
                        self.handle_message(key.fileobj, msg)
        print('Shutting down this peer...')
        self.network.reset()

    def handle_queue(self):
        cmd, payload = self.queue.get()
        if cmd == 'connect':
            self.network.connect_to_node(payload['addr'], 'paxos_join_request')
        elif cmd == 'start_paxos':
            if self.paxos is not None:
                self.paxos.start_paxos_round(payload['value'])
        else:
            print('Unknown command:', cmd)

    def handle_message(self, sock, msg):
        print('[IN]:\t%s' % msg)

        cmd = msg['do']
        if cmd == 'paxos_join_confirm':
            self.paxos = PaxosNode(self, self.network, random.getrandbits(128), msg['group_size'])
            self.index.fromJSON(msg['index'])
            peers = [tuple(p) for p in msg['peers']]
            for addr in peers:
                self.network.connect_to_node(addr)

        elif self.paxos is None:  # do not handle other Messages if not yet part of Paxos
            if self.network.connections:
                self.network.send(sock.getpeername(), Message({
                    'do': 'try_other_peer',
                    'addr': self.network.get_random_listen_addr(),
                }))

        elif cmd == 'hello':
            self.network.set_remote_listen_addr(sock, tuple(msg['listen_addr']))
        elif cmd == 'client_hello':
            self.network.set_remote_listen_addr(sock, tuple(msg['http_addr']))

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

        elif cmd == 'paxos_prepare':
            self.paxos.handle_prepare(sock.getpeername(), tuple(msg['id']))
        elif cmd == 'paxos_promise':
            self.paxos.handle_promise(tuple(msg['id']), msg['accepted'])
        elif cmd == 'paxos_propose':
            self.paxos.handle_propose(sock.getpeername(), tuple(msg['id']), msg['value'])
        elif cmd == 'paxos_accept':
            self.paxos.handle_accept(tuple(msg['id']))
        elif cmd == 'paxos_learn':
            self.apply_chosen_value(msg['value'])  # TODO: change broadcast to include localhost
            self.paxos.handle_learn(tuple(msg['id']), msg['value'])

        elif cmd == 'index_search':
            self.network.send(sock.getpeername(), {
                'do': 'index_search_result',
                'query': msg['filename'],  # in case of multiple searches, out of order...
                'addr': self.index.search_entry(msg['filename']),
            })
        elif cmd == 'index_add':
            addr = self.network.get_remote_listen_addr(sock)
            self.run_paxos({
                'change': 'add',
                'entry': msg['filename'],
                'addr': addr,
            })
        elif cmd == 'index_remove':
            self.run_paxos({
                'change': 'remove',
                'entry': msg['filename'],
            })

    def stop(self):
        if self.network:
            self.network.stop()

    def connect_to_paxos(self, addr):
        self.queue.put(('connect', {
            'addr': addr,
        }))

    def run_paxos(self, value):
        self.queue.put(('start_paxos', {
            'value': value,
        }))

    def on_close(self):
        self.running = False  # TODO: is this thread safe? (we read this variable in run)
        while not self.network.is_done():
            pass

    def apply_chosen_value(self, value, selfStartedRound=False):
        if value['change'] == 'join':
            self.paxos.group_size += 1
            if selfStartedRound:
                self.send_paxos_join_confirmation(tuple(value['respond_addr']))
        elif value['change'] == 'add':
            self.index.add_entry(value['entry'], value['addr'])
        elif value['change'] == 'remove':
            self.index.remove_entry(value['entry'])

    def send_paxos_join_confirmation(self, addr):
        peers = list(filter(lambda c: c.sock.getpeername() != addr, self.network.connections.values()))
        peers = [c.remote_listen_addr for c in peers]
        self.network.send(addr, {
            'do': 'paxos_join_confirm',
            'group_size': self.paxos.group_size,
            'index': self.index.toJSON(),
            'peers': peers
        })


if __name__ == '__main__':
    if not len(sys.argv) in range(2,4):
        sys.exit('Usage: python peer.py (server|ip port)')

    log.basicConfig(level=log.DEBUG, filename='debug.log')
    peer = Peer(sys.argv[1] == 'server')
    peer.start()
    if sys.argv[1] != 'server':
        peer.connect_to_paxos((sys.argv[1], int(sys.argv[2])))
