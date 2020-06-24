import copy
import json


class Message(dict):

    def __init__(self, init={}):
        for key,val in init.items():
            self[key] = copy.deepcopy(val)

    def serialize(self):
        return json.dumps(self)

    @staticmethod
    def deserialize(msg):
        return Message(json.loads(msg))
