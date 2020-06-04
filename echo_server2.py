import selectors
import socket
import types
import logging as log
import tkinter as tk
import threading

from util import *
from connection import Connection


VERSION = 0.1
log.basicConfig(level=log.DEBUG, filename='debug.log')

TIMEOUT = 5.0
DEFAULT_HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
DEFAULT_PORT = 65432        # Port to listen on (non-privileged ports are > 1023)
RUNNING = True
CONN = {}  # Host|Port -> Connection

def accept_wrapper(sel, sock):
    conn, addr = sock.accept()  # Should be ready to read
    log.debug('accepted connection from %s' % str(addr))
    conn.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)

    ident = get_key(conn)
    CONN[ident] = Connection(CONN, ident, conn)

def service_connection(sel, key, mask):
    sock = key.fileobj
    data = key.data
    ident = get_key(sock)
    connection = CONN[ident]
    if mask & selectors.EVENT_READ:
        recv_data = sock.recv(1024)  # Should be ready to read
        if recv_data:
            connection.handle_data(recv_data)
        else:  # connection closed
            log.debug('closing connection to %s' % str(data.addr))
            sel.unregister(sock)
            sock.close()
            del CONN[ident]
    if mask & selectors.EVENT_WRITE:
        if connection.has_data():
            log.debug('echoing %s to %s' % (repr(connection.out), data.addr))
            sent = sock.send(connection.out)  # Should be ready to write
            connection.out = connection.out[sent:]

def networking(host, port):
    print("Starting server on (%s, %s)" % (host, port))
    sel = selectors.DefaultSelector()
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.bind((host, port))
    lsock.listen()
    log.debug('listening on (%s, %s)' % (host, port))
    lsock.setblocking(False)
    sel.register(lsock, selectors.EVENT_READ, data=None)
    while RUNNING:
        events = sel.select(timeout=TIMEOUT)
        for key, mask in events:
            if key.data is None:
                accept_wrapper(sel, key.fileobj)
            else:
                service_connection(sel, key, mask)
    lsock.close()
    print("Stopping server")

def start_network(host, port):
    global RUNNING
    RUNNING = True
    thread = threading.Thread(target = networking, args=[host, port])
    thread.deamon = True
    thread.start()

def stop_network():
    global RUNNING
    RUNNING = False

if __name__ == "__main__":
    root=tk.Tk()

    frame=tk.Frame(root,width=500,height=450)

    hostname_label=tk.Label(frame, text="Hostname")
    port_label=tk.Label(frame, text="Port")

    hostname_label.grid(row=0,sticky=tk.E)
    port_label.grid(row=1,sticky=tk.E)

    hostname_entry=tk.Entry(frame)
    hostname_entry.insert(tk.END, DEFAULT_HOST)
    port_entry=tk.Entry(frame)
    port_entry.insert(tk.END, DEFAULT_PORT)

    hostname_entry.grid(row=0,column=1)
    port_entry.grid(row=1,column=1)

    button1=tk.Button(frame,text="Start Server", command=lambda: start_network(hostname_entry.get(), int(port_entry.get())))        
    button1.grid(row=2,column=0)
    button3=tk.Button(frame,text="Stop Server", command=stop_network)
    button3.grid(row=2,column=1)
    frame.grid()

    root.mainloop()