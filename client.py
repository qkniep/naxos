# -*- coding: utf-8 -*-
"""Naxos client CLI."""

import configparser
import functools
import http.server
import logging as log
import random
import re
import selectors
import shlex
import shutil
import socket
import socketserver
import sys
import threading
import time
import urllib.request

from pathlib import Path

import miniupnpc

import util
from connection import Connection
from message import Message


SELECT_TIMEOUT = 2
RECV_BUFFER = 1024

class InputThread(threading.Thread):
    """Thread for handling the user input (stdin)."""

    def __init__(self, queue):
        """"""
        super().__init__()  # Thread constructor

        self.queue = queue
        self.running = True

    def run(self):  # called by Thread.start()
        while self.running:
            try:
                line = input('> ')
            except EOFError:
                sys.exit(0)
            if line == '':
                continue
            cmd, *args = shlex.split(line)

            if cmd in ('search', 's'):
                if len(args) == 0:
                    print('Usage: > search <filename1> <filename2> ...')
                    continue

                self.queue.put(('search', {
                    'files': args
                }))
            elif cmd in ('download', 'd'):
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
        self.running = False  # TODO: is this thread safe? (we read this variable in run)


class DirectoryObserver(threading.Thread):
    """Thread for handling changes in the naxos directory.
    Upon start, adds all files in the directory to the index.
    Upon adding/removing of file to the directory, adds/removes it from the index.
    """

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
        self.running = False  # TODO: is this thread safe? (we read this variable in run)


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
            sys.exit('Malformatted ini file.')
    else:  # try to read the necessary parameters from args
        try:
            host, port = argv[1].split(':')
            naxos_path = Path(argv[2])
        except:
            print('Usage: python client.py <naxos peer host>:<naxos peer port> <"path to naxos directory">')
            print('Alternative: python client.py with a naxos.ini file in the same directory')
            sys.exit(0)

    # sanity check
    pattern = r'^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$'  # ipv4
    if not re.match(pattern, host):
        sys.exit('host ip is not a well formed ip address.')
    if not naxos_path.is_dir():
        sys.exit('provided naxos directory is not a directory.')
    if not port.isdigit():
        sys.exit('provided port is not a valid positive number.')
    elif not 1024 <= int(port) <= 65535:
        sys.exit('provided port is not a valid port.')
    port = int(port)
    addr = (host, port)

    return addr, naxos_path


def handle_queue(queue, naxos_path, sock, conn):
    cmd, payload = queue.get()
    if cmd == 'quit':
        dir_observer.stop()
        input_thread.stop()
        sock.close()
        sys.exit(0)

    elif cmd == 'insert':
        for file in payload['files']:
            conn.send(Message({
                'do': 'index_add',
                'filename': file
            }))

    elif cmd == 'delete':
        for file in payload['files']:
            conn.send(Message({
                'do': 'index_remove',
                'filename': file
            }))

    elif cmd == 'search':
        for file in payload['files']:
            conn.send(Message({
                'do': 'index_search',
                'filename': file
            }))

    elif cmd == 'download':
        for f in payload['files']:
            addr = RESULTS.get(f)
            if addr is not None:
                print('Using cached address (%s:%s) for %s' % (*addr, f))
                download(naxos_path, f, addr)
            else:
                print('You have to search for the file first.')  # TODO: Auto-search
    else:
        raise ValueError('Unexpected command.')


def download(path, filename, addr):
    host, port = addr
    url = 'http://%s:%s/%s' % (host, port, filename)

    if (path / filename).exists:  # check if filename already used
        i = 0
        while (path / ('%s_%s' % (filename, i))).exists:  # find one that is unused
            i += 1
        filename = '%s_%s' % (filename, i)
        print('Saving file as %s.' % filename)

    try:  # open url, write response to file
        with urllib.request.urlopen(url) as response, open(path / filename, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
    except Exception as error:
        print('Error while downloading file %s: %s' % (filename, error))


def handle_data(data):  # XXX DRY
    global BUFFER
    splitted = data.split(util.DELIMITER)  # works with multiple messages in one chunk
    for i, msg_chunk in enumerate(splitted):
        BUFFER += msg_chunk
        if i == len(splitted)-1:
            break  # last one is either incomplete or empty string, so no parsing in this case
        yield Message.deserialize(util.decode_data(BUFFER))
        BUFFER = b''


def handle_response(msg):
    cmd = msg['do']

    if cmd == 'index_search_result':
        addr = msg['addr']
        query = msg['query']
        if addr is not None:
            print('Found results for query: %s' % query)
            RESULTS[query] = addr
        else:
            print('No results found for query: %s' % msg['query'])
    else:
        print(msg)


def get_httpd(path):
    Handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=path)

    upnp = miniupnpc.UPnP()
    upnp.discoverdelay = 10
    upnp.discover()
    upnp.selectigd()

    (host, port), remove_mapping = determine_http_addr(upnp)

    return socketserver.TCPServer((host, port), Handler), remove_mapping, (host, port)


def determine_http_addr(upnp):
    host = upnp.lanaddr
    while True:
        port = random.randint(1024, 65535)  # random port
        try:
            upnp.addportmapping(port, 'TCP', host, port, 'Naxos HTTP Server', '')
            break  # no exception, so mapping worked
        except:
            pass
    remove_mapping = lambda: upnp.deleteportmapping(port, 'TCP', 'Naxos')
    return (host, port), remove_mapping


RESULTS = {}
BUFFER = b''

if __name__ == '__main__':
    log.basicConfig(level=log.DEBUG, filename='client_debug.log')

    address, naxos_path = parse_config(sys.argv)

    selector = selectors.DefaultSelector()

    queue = util.PollableQueue()
    selector.register(queue, selectors.EVENT_READ)

    input_thread = InputThread(queue)
    input_thread.start()
    dir_observer = DirectoryObserver(naxos_path, queue)
    dir_observer.start()
    _httpd, remove_mapping, http_addr = get_httpd(str(naxos_path))
    httpd = threading.Thread(target=_httpd.serve_forever)
    httpd.setDaemon(True)
    httpd.start()

    try:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(address)
            selector.register(sock, selectors.EVENT_READ | selectors.EVENT_WRITE)
        except (ConnectionRefusedError, ConnectionAbortedError, TimeoutError) as exception:
            print('Could not establish connection to (%s, %s):' % address, exception)
            raise Exception()
        conn = Connection(sock, known=True)
        conn.send(Message({
            'do': 'client_hello',
            'http_addr': http_addr
        }))

        while True:
            events = selector.select(timeout=SELECT_TIMEOUT)
            for key, mask in events:
                if key.fileobj is queue:
                    handle_queue(queue, naxos_path, sock, conn)
                elif key.fileobj is sock:
                    if mask & selectors.EVENT_READ:
                        try:
                            recv_data = sock.recv(RECV_BUFFER)
                            if recv_data:
                                for message in handle_data(recv_data):
                                    log.debug(message)
                                    handle_response(message)
                            else:  # connection closed
                                selector.unregister(sock)
                                sock.close()
                                sys.exit(0)
                        except (ConnectionResetError, ConnectionAbortedError):
                            print('Connection reset/aborted:', address)
                            selector.unregister(sock)
                            sock.close()
                            sys.exit(0)
                    if mask & selectors.EVENT_WRITE:
                        conn.flush_out_buffer()
    finally:
        dir_observer.stop()
        input_thread.stop()

        # stop http server and remove upnp mapping
        _httpd.shutdown()
        remove_mapping()
        print('Exiting...')
