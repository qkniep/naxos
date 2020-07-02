import copy


class Cache:

    def __init__(self):
        self.message_cache = {}
        self.routing_cache = {}

    def process(self, origin, msg):
        self.message_cache[msg['id']] = origin
        self.routing_cache[msg['from']] = origin
        
    def route(self, dest):
        return self.routing_cache.get(dest, 'broadcast')

    def seen(self, msg):
        return msg['id'] in self.message_cache
