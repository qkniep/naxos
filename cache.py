import copy
from socket import socket

from connection import Connection
from message import Message


class Cache:

    def __init__(self):
        self.message_cache = {}
        self.routing_cache = {}

    def update_routing(self, origin: tuple, msg: Message):
        print("Update routing info: %s over %s" % (msg['from'], origin))
        self.routing_cache[msg['from']] = origin
    
    def processed(self, origin: tuple, msg: Message):
        self.message_cache[msg['id']] = (origin, msg)

    def route(self, dest: str):
        return self.routing_cache.get(dest, 'broadcast')

    def seen(self, msg: Message):
        return msg['id'] in self.message_cache
