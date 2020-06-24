import logging as log

from message import Message
import util


class Connection:

    def __init__(self, sock, known = False):
        print('Connection created: ', sock.getsockname())
        self.sock = sock
        self.in_buf = b''
        self.out_buf = b''

        # need peer's listening address, before that we don't consider the connection complete
        if not known:
            self._is_synchronized = False
            self.remote_listen_addr = None
        else:
            self._is_synchronized = True
            self.remote_listen_addr = sock.getpeername()

    def synchronize_peer(self):
        pass

    def handle_data(self, data):
        splitted = data.split(util.DELIMITER)  # it could happen that we receive multiple messages in one chunk
        for i, msg_chunk in enumerate(splitted):
            self.in_buf += msg_chunk
            if i == len(splitted)-1:  # last one is either incomplete or empty string, so no parsing in this case
                break
            yield from self.parse_in_buffer()

    def parse_in_buffer(self):
        message = util.decode_data(self.in_buf)
        yield Message.deserialize(message)
        self.in_buf = b''

    def send(self, msg):
        log.debug('[OUT]:\t%s' % msg.serialize())
        self.out_buf += util.encode_data(msg.serialize()) + util.DELIMITER

    def flush_out_buffer(self):
        if self.out_buf:
            sent = self.sock.send(self.out_buf)
            log.debug('echo %s from buffer to socket %s' % (repr(self.out_buf[:sent]), self.sock.getsockname()))
            self.out_buf = self.out_buf[sent:]

    def is_synchronized(self):
        return self._is_synchronized
