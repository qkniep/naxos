import logging as log
import random
import sys
import socket
import threading

from network import NetworkNode
from paxos import PaxosNode
import util


class Peer(threading.Thread):

    VERSION = 0.1

    def __init__(self):
        threading.Thread.__init__(self)

        self.queue = util.PollableQueue()
        self.network = NetworkNode(self.queue)
        self.paxos = PaxosNode(self.network, random.getrandbits(128), len(self.network.connections)+1)

    def run(self):
        self.network.run()

    def stop(self):
        if self.network:
            self.network.stop()

    def connect_to_peer(self, ip, port):
        self.queue.put(('connect', {
            'host': ip,
            'port': int(port),
        }))

    def on_close(self):
        if self.network is not None:
            self.network.stop()
            while not self.network.is_done():  # blocking wait
                pass


if __name__ == '__main__':  # called as script, not as module

    log.basicConfig(level=log.DEBUG, filename='debug.log')
    peer = Peer()
    peer.start()
    if sys.argv[1] != 'server':
        peer.connect_to_peer(sys.argv[1], sys.argv[2])
