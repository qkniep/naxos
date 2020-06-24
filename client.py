import configparser
import logging as log
import os
import re
import selectors
import shlex
import socket
import sys
import threading
import time

from pathlib import Path

import util
from connection import Connection
from message import Message


SELECT_TIMEOUT = 2
RECV_BUFFER = 1024

class InputThread(threading.Thread):

    def __init__(self, queue):
        super().__init__()

        self.queue = queue
        self.running = True

    def run(self):
        while self.running:
            try:
                line = input('> ')
            except EOFError:
                print('Encountered EOF in stdin ಠ_ಠ')
                exit(0)
            if line == '':
                continue
            cmd, *args = shlex.split(line)
            
            if cmd == 'search' or cmd == 's':
                if len(args) == 0:
                    print('Usage: > search <filename1> <filename2> ...')
                    continue
                
                self.queue.put(('search', {
                    'files': args
                }))
            elif cmd == 'download' or cmd == 'd':
                if len(args) == 0:
                    print('Usage: > download <filename1> <filename2> ...')
                    continue

                self.queue.put(('download', {
                    'files': args
                }))
            elif cmd == 'quit':
                self.queue.put(('quit', {}))
            else:
                print('Usage: > (search|download) <filename1> <filename2> ...')

    def stop(self):
        self.running = False


class DirectoryObserver(threading.Thread):
    CHECK_INTERVAL = 1

    def __init__(self, path, queue):
        super().__init__()

        self.queue = queue
        self.path = path
        self.running = True

    def run(self):
        scan = lambda p: set([f.name for f in p.glob('*') if f.is_file()])  # list (non-recursive) all files in directory
        files = scan(self.path)
        old = files
        self.queue.put(('insert', {
            'files': list(files)
        }))
        try:
            while self.running:
                new = files - old
                removed = old - files
                if new:  # new files detected
                    print('new: %s' % new)
                    self.queue.put(('insert', {
                        'files': list(new)
                    }))
                if removed:  # removed files detected
                    print('delete: %s' % removed)
                    self.queue.put(('delete', {
                        'files': list(removed)
                    }))
                old = files
                files = scan(self.path)
                time.sleep(self.CHECK_INTERVAL)
        finally:
            print('shutting down observer. :<')

    def stop(self):
        self.running = False


def parse_config(argv):
    config_path = Path.cwd() / 'naxos.ini'
    host = None
    port = None
    naxos_path = None
    
    if len(argv) == 1 and config_path.is_file():  # try to read the provided config file
        try:
            config = configparser.ConfigParser()
            config.read(config_path)
            host = config['naxos']['host_ip']
            port = config['naxos']['host_port']
            naxos_path = Path(config['naxos']['naxos_directory'])
        except:
            print('Malformatted ini file.')
            exit(0)
    else:  # try to read the necessary parameters from args
        try:
            host, port = argv[1].split(':')
            naxos_path = Path(argv[2])
        except:
            print('Usage: python client.py <naxos peer host>:<naxos peer port> <"path to naxos directory">')
            print('Alternative: python client.py with a naxos.ini file in the same directory')
            exit(0)

    # sanity check
    pattern = r'^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$'  # ipv4
    if not re.match(pattern, host):
        print('host ip is not a well formed ip address.')
        exit(0)
    if not naxos_path.is_dir():
        print('provided naxos directory is not a directory.')
        exit(0)
    if not port.isdigit():
        print('provided port is not a valid positive number.')
        exit(0)
    elif not (1024 <= int(port) <= 65535):
        print('provided port is not a valid port.')
        exit(0)
    port = int(port)
    addr = (host, port)

    return addr, naxos_path


def handle_queue(queue, sock, conn):
    cmd, payload = queue.get()
    if cmd == 'quit':
        dir_observer.stop()
        input_thread.stop()
        sock.close()
        exit(0)

    elif cmd == 'insert':
        for f in payload['files']:
            conn.send(Message({
                'do': 'index_add',
                'filename': f
            }))
    
    elif cmd == 'delete':
        for f in payload['files']:
            conn.send(Message({
                'do': 'index_remove',
                'filename': f
            }))

    elif cmd == 'search':
        for f in payload['files']:
            conn.send(Message({
                'do': 'index_search',
                'filename': f
            }))
        
    elif cmd == 'download':  # TODO: this makes no sense, download should search and use the answer to make an http request to the provided ip
        print('TODO')
        # conn.send(Message({
        #     'do': 'client_download',
        #     'files': payload['files']
        # }))

    else:
        raise ValueError('Unexpected command.')


def handle_data(data):
    global BUFFER
    splitted = data.split(util.DELIMITER)  # it could happen that we receive multiple messages in one chunk
    for i, msg_chunk in enumerate(splitted):
        BUFFER += msg_chunk
        if i == len(splitted)-1:  # last one is either incomplete or empty string, so no parsing in this case
            break
        yield Message.deserialize(util.decode_data(BUFFER))
        BUFFER = b''


def handle_response(message):
    cmd = msg['do']
    
    if cmd == 'index_search_result':
        if msg['addr'] is not None:
            print('Found results for query: %s' % msg['query'])
        else:
            print('No results found for query: %s' % msg['query'])



BUFFER = b''

if __name__ == '__main__':
    log.basicConfig(level=log.DEBUG, filename='client_debug.log')

    addr, naxos_path = parse_config(sys.argv)

    selector = selectors.DefaultSelector()

    queue = util.PollableQueue()
    selector.register(queue, selectors.EVENT_READ)

    input_thread = InputThread(queue)
    input_thread.start()
    dir_observer = DirectoryObserver(naxos_path, queue)
    dir_observer.start()

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(addr)
        selector.register(sock, selectors.EVENT_READ | selectors.EVENT_WRITE)
    except (ConnectionRefusedError, ConnectionAbortedError, TimeoutError) as e:
        print('Could not establish connection to (%s, %s):' % addr, e)
        dir_observer.stop()
        input_thread.stop()
        exit(0)
    conn = Connection(sock, known=True)
    conn.send(Message({
        'do': 'client_hello'
    }))

    try:
        while True:
            events = selector.select(timeout=SELECT_TIMEOUT)
            for key, mask in events:
                if key.fileobj is queue:
                    handle_queue(queue, sock, conn)
                elif key.fileobj is sock:
                    if mask & selectors.EVENT_READ:
                        try:
                            recv_data = sock.recv(RECV_BUFFER)
                            if recv_data:
                                for msg in handle_data(recv_data):
                                    log.debug(msg)
                                    handle_response(msg)
                                    
                            else:  # connection closed
                                selector.unregister(sock)
                                sock.close()
                                exit(0)
                        except (ConnectionResetError, ConnectionAbortedError):
                            print('Connection reset/aborted:', addr)
                            selector.unregister(sock)
                            sock.close()
                            exit(0)
                    if mask & selectors.EVENT_WRITE:
                        conn.flush_out_buffer()
    finally:
        dir_observer.stop()
        input_thread.stop()
        print('Exiting...')
