import json

class Message:

    def __init__(self, init={}):
        self.dict = init

    def serialize(self):
        return json.dumps(self.dict)

    @staticmethod
    def deserialize(msg):
        return Message(json.loads(msg))

    def __str__(self):
        return str(self.dict)
