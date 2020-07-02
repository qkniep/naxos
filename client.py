#!/usr/bin/python
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
import urllib.error
import urllib.request

from pathlib import Path

import miniupnpc

import util
from connection import Connection
from message import Message


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
        """Terminates this thread."""
        self.running = False  # TODO: is this thread safe? (we read this variable in run)


class Client:
    """"""

    SELECT_TIMEOUT = 2
    RECV_BUFFER = 1024

    def __init__(self, queue, address, naxos_path, http_addr):
        self.address = address
        self.naxos_path = naxos_path
        self.http_addr = http_addr

        self.selector = selectors.DefaultSelector()
        self.queue = queue
        self.selector.register(queue, selectors.EVENT_READ)

        self.results = {}

        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect(self.address)
            self.selector.register(self.sock, selectors.EVENT_READ | selectors.EVENT_WRITE)
        except (ConnectionRefusedError, ConnectionAbortedError, TimeoutError) as exception:
            sys.exit('Could not establish connection to (%s, %s):' % self.address, exception)
        self.conn = Connection(self.sock, known=True)
        self.conn.send(Message({
            'do': 'client_hello',
            'http_addr': self.http_addr
        }))

    def handle_connections(self):
        while True:
            events = self.selector.select(timeout=self.SELECT_TIMEOUT)
            for key, mask in events:
                if key.fileobj is self.queue:
                    self.handle_queue()
                elif key.fileobj is self.sock:
                    if mask & selectors.EVENT_READ:
                        try:
                            recv_data = self.sock.recv(self.RECV_BUFFER)
                            if recv_data:
                                for message in self.conn.handle_data(recv_data):
                                    log.debug(message)
                                    self.handle_response(message)
                            else:  # connection closed
                                self.reset()
                        except (ConnectionResetError, ConnectionAbortedError):
                            print('Connection reset/aborted:', address)
                            self.reset()
                    if mask & selectors.EVENT_WRITE:
                        self.conn.flush_out_buffer()

    def handle_queue(self):
        """Handles a user command from the thread-safe queue.
        This command probably came from the CLI (stdin) thread.
        """
        cmd, payload = self.queue.get()
        if cmd == 'quit':
            self.reset()

        elif cmd == 'insert':
            for file in payload['files']:
                self.conn.send(Message({
                    'do': 'index_add',
                    'filename': file
                }))

        elif cmd == 'delete':
            for file in payload['files']:
                self.conn.send(Message({
                    'do': 'index_remove',
                    'filename': file
                }))

        elif cmd == 'search':
            for file in payload['files']:
                self.conn.send(Message({
                    'do': 'index_search',
                    'filename': file
                }))

        elif cmd == 'download':
            for file in payload['files']:
                addr = self.results.get(file)
                if addr is not None:
                    print('Using cached address (%s:%s) for %s' % (*addr, file))
                    download(self.naxos_path, file, addr)
                else:
                    print('You have to search for the file first.')  # TODO: Auto-search
        else:
            raise ValueError('Unexpected command.')

    def handle_response(self, msg):
        """Handles a response message we got from an index server."""
        cmd = msg['do']

        if cmd == 'index_search_result':
            addr = msg['addr']
            query = msg['query']
            if addr is not None:
                print('Found results for query: %s' % query)
                self.results[query] = addr
            else:
                print('No results found for query: %s' % msg['query'])
        else:
            print(msg)

    def reset(self):
        """Cleanup deregister FDs at selector and close sockets, finally exit."""
        self.selector.unregister(self.sock)
        self.sock.close()
        sys.exit(0)


def scan(path):
    """Lists (non-recursively) all files in directory."""
    return {f.name for f in path.glob('*') if f.is_file()}


def parse_config(argv):
    """Parses the Naxos configuration file."""
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
        except KeyError:
            sys.exit('Malformatted naxos.ini file.')
    else:  # try to read the necessary parameters from args
        try:
            host, port = argv[1].split(':')
            naxos_path = Path(argv[2])
        except (IndexError, ValueError):
            print('Usage: python client.py\
                  <naxos peer host>:<naxos peer port>\
                  <"path to naxos directory">')
            print('Alternative: python client.py with a naxos.ini file in the same directory')
            sys.exit(0)

    # sanity checks
    pattern = r'^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$'  # ipv4
    if not re.match(pattern, host):
        sys.exit('host ip is not a well formed IPv4 address.')
    if not naxos_path.is_dir():
        sys.exit('provided naxos directory is not a directory.')
    if not port.isdigit():
        sys.exit('provided port is not a valid positive number.')
    elif not 1024 <= int(port) <= 65535:
        sys.exit('provided port is not a valid port.')
    port = int(port)
    addr = (host, port)

    return addr, naxos_path


def download(path, filename, addr):
    """Download a file from a peer via HTTP and save it to disk.

    An HTTP GET is sent to: http://addr/filename.
    The result is downloaded into: path/filename.
    """
    host, port = addr
    url = 'http://%s:%s/%s' % (host, port, filename)

    if (path / filename).exists():  # check if filename already used
        i = 0
        while (path / ('%s_%s' % (filename, i))).exists:  # find one that is unused
            i += 1
        filename = '%s_%s' % (filename, i)
    print('Saving file as %s.' % filename)

    try:  # open url, write response to file
        with urllib.request.urlopen(url) as response, open(path / filename, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
    except (IOError, urllib.error.URLError, shutil.Error) as error:
        print('Error while downloading file %s: %s' % (filename, error))


def get_httpd(path):
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=path)

    upnp = miniupnpc.UPnP()
    upnp.discoverdelay = 10
    upnp.discover()
    upnp.selectigd()

    host = upnp.lanaddr
    while True:
        port = random.randint(1024, 65535)  # random port
        try:
            upnp.addportmapping(port, 'TCP', host, port, 'Naxos HTTP Server', '')
            break  # no exception, so mapping worked
        except miniupnpc.ConflictInMappingEntry:
            pass
    remove_mapping = lambda: upnp.deleteportmapping(port, 'TCP', 'Naxos')

    public_host = upnp.externalipaddress()
    return socketserver.TCPServer((host, port), handler), remove_mapping, (public_host, port)


if __name__ == '__main__':
    log.basicConfig(level=log.DEBUG, filename='client_debug.log')

    address, naxos_path = parse_config(sys.argv)

    queue = util.PollableQueue()
    selector = selectors.DefaultSelector()
    selector.register(queue, selectors.EVENT_READ)

    input_thread = InputThread(queue)
    input_thread.start()
    dir_observer = DirectoryObserver(naxos_path, queue)
    dir_observer.start()
    httpd, remove_mapping, http_addr = get_httpd(str(naxos_path))
    httpd_thread = threading.Thread(target=httpd.serve_forever)
    httpd_thread.daemon = True
    httpd_thread.start()

    try:
        client = Client(queue, address, naxos_path, http_addr)
        client.handle_connections()
    finally:
        dir_observer.stop()
        input_thread.stop()

        # stop http server and remove upnp mapping
        httpd.shutdown()
        remove_mapping()
        print('Exiting...')
