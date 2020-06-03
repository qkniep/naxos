import socket
import time
import util

import string

HOST = '127.0.0.1'  # The server's hostname or IP address
PORT = 65432        # The port used by the server

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    i = 0
    while True:
        s.sendall(util.encode_data(string.ascii_letters[i]) + util.DELIMITER)
        data = s.recv(1024)
        splitted = data.split(util.DELIMITER)
        for msg in splitted:
            print('Received msg: %s' % util.decode_data(msg))
        time.sleep(0.5)
        i = (i+1) % len(string.ascii_letters)