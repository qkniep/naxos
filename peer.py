import logging as log
import sys
import socket
import threading

from network import NetworkThread
import paxos
import util


class Peer:
    VERSION = 0.1

    def __init__(self):
        self.queue = util.PollableQueue()
        self.nt = NetworkThread(self.queue)
        self.nt.start()

    def stop(self):
        if self.nt is not None:
            self.nt.stop()

    def connect(self, ip, port):
        self.queue.put(('connect', {
            'host': ip,
            'port': int(port),
        }))

    def on_close(self):
        if self.nt is not None:
            self.nt.stop()
            while not self.nt.is_done():  # blocking wait
                pass


if __name__ == '__main__':  # called as script, not as module

    log.basicConfig(level=log.DEBUG, filename='debug.log')
    peer = Peer()
    if sys.argv[1] != 'server':
        peer.connect(sys.argv[1], sys.argv[2])
        start_paxos_round('hello')
