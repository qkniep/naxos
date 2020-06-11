import threading
import logging as log
import sys
import socket

from network import NetworkThread
import util


VERSION = 0.1
log.basicConfig(level=log.DEBUG, filename='debug.log')

if __name__ == '__main__':  # called as script, not as module

    def start(ip, port):
        global nt, queue
        queue = util.PollableQueue()
        nt = NetworkThread(queue, ip, int(port))
        nt.start()

    def stop():
        global nt
        if nt is not None:
            nt.stop()

    def connect(ip, port):
        global queue
        queue.put(('connect', {
            'host': ip,
            'port': int(port),
        }))

    def on_close():
        if nt is not None:
            nt.stop()
            while not nt.is_done():  # blocking wait
                pass
        root.destroy()

    if sys.argv[1] != 'server':
        start(sys.argv[1], sys.argv[2])
        connect(sys.argv[3], sys.argv[4])
    elif len(sys.argv) == 2:
        start(NetworkThread.DEFAULT_HOST, NetworkThread.DEFAULT_PORT)
    elif len(sys.argv) == 4:
        start(sys.argv[2], sys.argv[3])
