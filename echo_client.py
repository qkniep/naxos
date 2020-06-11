import socket
import time
import util
import string

from message import Message


HOST = '127.0.0.1'  # The server's hostname or IP address
PORT = 65432        # The port used by the server

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    i = 0
    while True:
        msg = Message({
            "content": string.ascii_letters[i]
        })
        # s.sendall(util.encode_data(string.ascii_letters[i]) + util.DELIMITER)
        s.sendall(util.encode_data(msg.serialize()) + util.DELIMITER)
        data = s.recv(1024)
        splitted = data.split(util.DELIMITER)
        print(splitted)
        for msg in splitted:
            if msg == b'':
                continue
            print('Received msg: %s' % Message.deserialize(util.decode_data(msg)))
        time.sleep(0.5)
        i = (i+1) % len(string.ascii_letters)
