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

    def on_close():
        if nt is not None:
            nt.stop()
            while not nt.is_done():  # blocking wait
                pass
        root.destroy()
    
    root=tk.Tk()
    root.protocol("WM_DELETE_WINDOW", on_close)
    nt = None
    queue = None

    frame=tk.Frame(root,width=500,height=450)

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

    button1=tk.Button(frame,text="Start Server", command=start)
    button1.grid(row=2,column=0)
    button3=tk.Button(frame,text="Stop Server", command=stop)
    button3.grid(row=2,column=1)
    frame.grid()

    root.mainloop()