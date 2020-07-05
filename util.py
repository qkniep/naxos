# -*- coding: utf-8 -*-
"""Utility functions for working with Base64 encoding and providing non-POSIX system support."""

import base64
import os
import queue
import socket
import struct

DELIMITER = b'|'


def encode_data(data):
    """Base64-encode data returning the base64 string."""
    return base64.b64encode(data.encode('utf-8'))


def decode_data(data):
    """Base64-decode data returning the original string."""
    return base64.b64decode(data).decode('utf-8')


def identifier(host, port):
    """Deterministically generates a single integer ID from address."""
    ip_int = struct.unpack("!I", socket.inet_aton(host))[0]
    return ip_int * 65536 + port


class PollableQueue(queue.Queue):
    """Source: https://stackoverflow.com/a/38002212
    Wrapper around Python's thread-safe Queue.
    Problem with Windows: select only works on sockets,
    so if we want to include the queue in a select, we have to 'emulate' a socket
    -> write a byte into it on every put, read one on every get
    ...kinda sad but it's windows.
    """

    def __init__(self):
        """Create the queue and a pair of connected sockets."""
        super().__init__()
        if os.name == 'posix':
            self._putsocket, self._getsocket = socket.socketpair()
        else:
            # Compatibility on non-POSIX systems
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.bind(('127.0.0.1', 0))
            server.listen(1)
            self._putsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._putsocket.connect(server.getsockname())
            self._getsocket, _ = server.accept()
            server.close()

    def fileno(self):
        """Return the fileno of the socket, so windows is happy."""
        return self._getsocket.fileno()

    def put(self, item):
        """Overrides put method of queue.Queue to use the socket."""
        super().put(item)
        self._putsocket.send(b'x')

    def get(self):
        """Overrides get method of queue.Queue to use the socket."""
        self._getsocket.recv(1)
        return super().get()
