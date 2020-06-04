import util
from message import Message

class Connection:
    
    def __init__(self, connections, identifier):
        print("Connection %s created." % identifier)
        self.id = identifier
        self.buf = b''
        self.out = b''
        self.connections = connections

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

    def parse_buffer(self):
        message = util.decode_data(self.buf)
        self.handle_message(Message.deserialize(message))
    
    def reset_buffer(self):
        self.buf = b''

    def handle_message(self, msg):
        print("--- Handle message: ---")
        print(msg)
        self.send(msg)

    def send(self, msg):
        self.out += util.encode_data(msg.serialize()) + util.DELIMITER

    def has_data(self):
        return self.out != b''

    def send_to(self, conn, msg):
        self.connections[conn].send(msg)