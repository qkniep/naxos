import logging as log
import random, selectors, sys, socket, threading

from network import NetworkNode
from paxos import PaxosNode
import util


class Peer(threading.Thread):

    SELECT_TIMEOUT = 1
    VERSION = '0.2.0'

    def __init__(self):
        threading.Thread.__init__(self)

        self.queue = util.PollableQueue()
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.queue, selectors.EVENT_READ)
        self.network = NetworkNode(self.selector)
        self.paxos = PaxosNode(self.network, random.getrandbits(128), len(self.network.connections)+1)
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
        elif cmd == 'paxos_prepare':
            self.paxos.handle_prepare(sock.getpeername(), tuple(msg['id']))
        elif cmd == 'paxos_promise':
            self.paxos.handle_promise(tuple(msg['id']), msg['accepted'])
        elif cmd == 'paxos_propose':
            self.paxos.handle_propose(sock.getpeername(), tuple(msg['id']), msg['value'])
        elif cmd == 'paxos_accept':
            self.paxos.handle_accept(tuple(msg['id']))
        elif cmd == 'paxos_learn':
            self.paxos.handle_learn(tuple(msg['id']), msg['value'])

    def stop(self):
        if self.network:
            self.network.stop()

    def connect_to_peer(self, ip, port):
        self.queue.put(('connect', {
            'addr': (ip, int(port)),
        }))
        self.paxos.group_size += 1

    def run_paxos(self):
        self.queue.put(('start_paxos', {
            'value': self.network.get_random_addr(),
        }))

    def on_close(self):
        self.running = False
        while not self.network.is_done():
            pass


if __name__ == '__main__':

    log.basicConfig(level=log.DEBUG, filename='debug.log')
    peer = Peer()
    peer.start()
    if sys.argv[1] != 'server':
        peer.connect_to_peer(sys.argv[1], sys.argv[2])
    if input() == 'p':
        peer.run_paxos()
