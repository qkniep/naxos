"""Abstractions for working with connections between network nodes."""

import logging as log

from message import Message
import util


class Connection:
    """Wrapper around socket.
    Represents a single connection between two network nodes.
    """

    def __init__(self, sock, known=False):
        log.info('Connection created: %s', sock.getsockname())
        self.sock = sock
        self.in_buf = b''
        self.out_buf = b''

        self._is_client = False
        self.http_addr = None

        if known:
            self.remote_listen_addr = sock.getpeername()
        else:
            self.remote_listen_addr = None

    def get_identifier(self):
        return util.identifier(*self.remote_listen_addr) if self.remote_listen_addr is not None else -1

    def is_client(self):
        """Returns true if the remote node is a Naxos client, false otherwise."""
        return self._is_client

    def set_client(self, http_addr):
        """Specifies that the remote node is a Naxos client, listening to on http_addr."""
        self._is_client = True
        self.http_addr = http_addr

    def handle_data(self, data):
        """Append new incoming bytes data to the buffer in_buf.
        Split by the message delimiter and yield parsed Message objects.
        """
        splitted = data.split(util.DELIMITER)  # multiple messages in one packet
        for i, msg_chunk in enumerate(splitted):
            self.in_buf += msg_chunk
            if i == len(splitted)-1:
                break  # last one is either incomplete or empty string, so no parsing in this case
            yield from self.parse_in_buffer()

    def parse_in_buffer(self):
        """Decodes raw transmitted bytes returning a Message object."""
        message = util.decode_data(self.in_buf)
        yield Message.deserialize(message)
        self.in_buf = b''

    def send(self, msg):
        """Prepare Message msg to be sent over this connection."""
        log.info('[OUT]:   %s over %s' % (msg.serialize(), self.sock.getpeername()))
        self.out_buf += util.encode_data(msg.serialize()) + util.DELIMITER

    def flush_out_buffer(self):
        """Write everything currently in out_buf to the socket."""
        if self.out_buf:
            sent = self.sock.send(self.out_buf)
            log.debug('echo %s from buffer to socket %s',
                      repr(self.out_buf[:sent]), self.sock.getsockname())
            self.out_buf = self.out_buf[sent:]
