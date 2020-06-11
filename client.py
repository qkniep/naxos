import threading
import logging as log
import tkinter as tk

from network import NetworkThread
import util

VERSION = 0.1
log.basicConfig(level=log.DEBUG, filename='debug.log')

if __name__ == "__main__":  # called as script, not as module
    
    def start():
        global nt, queue
        queue = util.PollableQueue()
        nt = NetworkThread(queue, hostname_entry.get(), int(port_entry.get()))
        nt.start()
    
    def stop():
        global nt
        if nt is not None:
            nt.stop()
    
    def connect():
        global queue
        queue.put(("connect", {
            "host": hostname_connect_entry.get(),
            "port": int(port_connect_entry.get())
        }))

    def on_close():
        if nt is not None:
            nt.stop()
            while not nt.is_done():  # blocking wait
                pass
        root.destroy()
    
    root=tk.Tk()
    root.title("Naxos %s" % VERSION)

    root.protocol("WM_DELETE_WINDOW", on_close)
    nt = None
    queue = None

    frame=tk.Frame(root,width=500,height=450)
    frame2 = tk.Frame(root, width=500, height=450)


    #  Frame 1, own listening port
    hostname_label=tk.Label(frame, text="Hostname")
    port_label=tk.Label(frame, text="Port")

    hostname_label.grid(row=0,sticky=tk.E)
    port_label.grid(row=1,sticky=tk.E)

    hostname_entry=tk.Entry(frame)
    hostname_entry.insert(tk.END, NetworkThread.DEFAULT_HOST)
    port_entry=tk.Entry(frame)
    port_entry.insert(tk.END, NetworkThread.DEFAULT_PORT)

    hostname_entry.grid(row=0,column=1)
    port_entry.grid(row=1,column=1)

    button1=tk.Button(frame,text="Start Listening", command=start)
    button1.grid(row=2,column=0)
    button3=tk.Button(frame,text="Stop Listening", command=stop)
    button3.grid(row=2,column=1)


    #  Frame 2, connect to other peer
    host_connect_label=tk.Label(frame2, text="Hostname")
    port_connect_label=tk.Label(frame2, text="Port")

    host_connect_label.grid(row=0,sticky=tk.E)
    port_connect_label.grid(row=1,sticky=tk.E)

    hostname_connect_entry=tk.Entry(frame2)
    hostname_connect_entry.insert(tk.END, NetworkThread.DEFAULT_HOST)
    port_connect_entry=tk.Entry(frame2)
    port_connect_entry.insert(tk.END, NetworkThread.DEFAULT_PORT)

    hostname_connect_entry.grid(row=0,column=1)
    port_connect_entry.grid(row=1,column=1)

    button1=tk.Button(frame2,text="Connect", command=connect)
    button1.grid(row=2,column=1)



    frame.grid(row=0,column=0)
    frame2.grid(row=0,column=1)

    root.mainloop()