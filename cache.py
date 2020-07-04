import copy
from socket import socket

from message import Message


class Cache:

    def __init__(self):
        self.message_cache = {}
        self.routing_cache = {}

    def process(self, origin: socket, msg: Message):  # origin is a socket, msg a Message object
        self.message_cache[msg['id']] = (origin, msg)
        self.routing_cache[msg['from']] = origin
        
    def route(self, dest: str):  # returns a socket or 'broadcast'
        return self.routing_cache.get(dest, 'broadcast')

    def seen(self, msg: Message):  # msg is a Message object, returns a boolean
        return msg['id'] in self.message_cache
