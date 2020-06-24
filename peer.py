import logging as log
import random
import selectors
import sys
import socket
from threading import Thread

from index import Index
from network import NetworkNode
from paxos import PaxosNode
import util


class Peer(Thread):

    SELECT_TIMEOUT = 1
    VERSION = '0.2.0'

    def __init__(self):
        super.__init__()

        self.queue = util.PollableQueue()
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.queue, selectors.EVENT_READ)
        self.network = NetworkNode(self.selector)
        # TODO: use address (IP+Port) instead of PRNG to derive unique node_id
        self.paxos = PaxosNode(self.network, random.getrandbits(128), len(self.network.connections)+1)
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
            self.network.connect_to_node(payload['addr'])
        if cmd == 'start_paxos':
            self.paxos.start_paxos_round(payload['value'])
        else:
            print('Unknown command:', cmd)

    def handle_message(self, sock, msg):
        print('[IN]:\t%s' % msg)

        cmd = msg['do']
        if cmd == 'connect_to':
            hosts = [tuple(h) for h in msg['hosts']]
            for addr in filter(lambda a: a not in self.network.connections, hosts):
                self.connect_to_peer(*addr)
        elif cmd == 'hello':
            # know listening host/port now -> connection is considered open
            self.network.synchronize_peer(sock.getpeername(), msg['listen_addr'])
            if self.paxos.group_size == 1:
                self.send_paxos_join_confirmation(sock.getpeername())
            else:
                self.run_paxos({
                    'change': 'join',
                    'entry': sock.getpeername(),
                })
        elif cmd == 'paxos_join':
            self.paxos.group_size = msg['group_size']
            self.index.fromJSON(msg['index'])
        elif cmd == 'paxos_prepare':
            self.paxos.handle_prepare(sock.getpeername(), tuple(msg['id']))
        elif cmd == 'paxos_promise':
            self.paxos.handle_promise(tuple(msg['id']), msg['accepted'])
        elif cmd == 'paxos_propose':
            self.paxos.handle_propose(sock.getpeername(), tuple(msg['id']), msg['value'])
        elif cmd == 'paxos_accept':
            self.paxos.handle_accept(tuple(msg['id']))
        elif cmd == 'paxos_learn':
            v = msg['value']
            if v['change'] == 'join':
                self.send_paxos_join_confirmation(v['entry'])
            elif v['change'] == 'add':
                self.index.add_entry(v['entry'], v['addr'])
            elif v['change'] == 'remove':
                self.index.remove_entry(v['entry'])
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

    def connect_to_peer(self, ip, port):
        self.queue.put(('connect', {
            'addr': (ip, int(port)),
        }))

    def run_paxos(self, value):
        self.queue.put(('start_paxos', {
            'value': value,
        }))

    def on_close(self):
        self.running = False  # TODO: is this thread safe? (we read this variable in run)
        while not self.network.is_done():
            pass

    def send_paxos_join_confirmation(self, addr):
        self.paxos.group_size += 1
        self.network.send(addr, {
            'do': 'paxos_join',
            'group_size': self.paxos.group_size,
            'index': self.index.toJSON(),
        })


if __name__ == '__main__':
    if not len(sys.argv) in range(2,4):
        sys.exit('Usage: python peer.py (server|ip port)')

    log.basicConfig(level=log.DEBUG, filename='debug.log')
    peer = Peer()
    peer.start()
    if sys.argv[1] != 'server':
        peer.connect_to_peer(sys.argv[1], sys.argv[2])
