import base64

DELIMITER = b"|"

def encode_data(data):
    return base64.b64encode(data.encode("utf-8"))

def decode_data(data):
    return base64.b64decode(data).decode("utf-8")

def get_key(socket):
    return "%s|%s" % socket.getpeername()
