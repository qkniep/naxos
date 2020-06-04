import base64
import queue
import socket
import os

DELIMITER = b"|"

def encode_data(data):
    return base64.b64encode(data.encode("utf-8"))

def decode_data(data):
    return base64.b64decode(data).decode("utf-8")

def get_key(socket):
    return "%s|%s" % socket.getpeername()

#  Source: https://stackoverflow.com/a/38002212
#  problem with windows: select only works on sockets, so if we want to include the queue in a select, we have to "emulate" a socket
#  -> write a byte into it on every put, read one on every get
#  ...kinda sad but it's windows.
class PollableQueue(queue.Queue):
    def __init__(self):
        super().__init__()
        # Create a pair of connected sockets
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

    def fileno(self):  # return the fileno of the socket, so windows is happy
        return self._getsocket.fileno()

    def put(self, item):
        super().put(item)
        self._putsocket.send(b'x')  # write a byte to the socket, so the select can report a read

    def get(self):
        self._getsocket.recv(1)  # read a byte from the socket
        return super().get()
