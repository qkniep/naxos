import util
from message import Message

class Connection:
    
    def __init__(self, queue, connections,
                 identifier,
                 host, port):
        print("Connection %s created." % identifier)
        self.id = identifier
        self.host = host
        self.port = port
        self.connections = connections
        self.queue = queue
        self.buf = b''
        self.out = b''

        # need listening host/port pair, before we don't consider the connection complete
        self._is_synchronized = False
        self.lhost = None
        self.lport = None

    def synchronize_peer(self):
        msg = Message({
            "do": "connect_to",
            "hosts": [],
        })
        for _, (_, conn) in self.connections.items():
            # ignore self and non-open connections -> connections to those will be made when they are opened themselves
            if conn is self or not conn.is_synchronized():
                continue
            msg["hosts"].append(conn.get_laddr())
        print(msg.serialize())
        self.send(msg)


    def handle_data(self, data):
        if util.DELIMITER in data:  # message ends here
            splitted = data.split(util.DELIMITER)  # it could happen that we receive multiple messages in one chunk
            for i, msg in enumerate(splitted):
                self.buf += msg
                if i == len(splitted)-1:  # last one is either incomplete or empty string, so no parsing in this case
                    break
                self.parse_buffer()
                self.reset_buffer()
        else:  # no message end in this chunk, so no parsing
            self.buf += data
    
    def get_addr(self):
        return (self.host, self.port)
    
    def get_laddr(self):
        return (self.lhost, self.lport)

    def parse_buffer(self):
        message = util.decode_data(self.buf)
        self.handle_message(Message.deserialize(message))
    
    def reset_buffer(self):
        self.buf = b''

    def handle_message(self, msg):
        print("--- Handle message: ---")
        print(msg)
        cmd = msg["do"]

        if cmd == "connect_to":
            for (host, port) in msg["hosts"]:
                self.queue.put(("connect", {
                    "host": host,
                    "port": port
                }))

        elif cmd == "hello":
            # know listening host/port now -> connection is considered open
            self.lhost = msg["content"]["lhost"]
            self.lport = msg["content"]["lport"]
            self._is_synchronized = True
            self.synchronize_peer()
        # self.send(msg)

    def send(self, msg):
        self.out += util.encode_data(msg.serialize()) + util.DELIMITER

    def has_data(self):
        return self.out != b''

    def is_synchronized(self):
        return self._is_synchronized

    def send_to(self, conn, msg):
        self.connections[conn].send(msg)