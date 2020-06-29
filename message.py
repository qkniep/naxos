# -*- coding: utf-8 -*-
"""The message module provides abstractions for reasoning about messages."""

import copy
import json


class Message(dict):
    """Message objects are JSON-serializable collections of key/value pairs."""

    def __init__(self, init):
        """Create a new Message by deepcopying the init dictionary."""
        super().__init__(copy.deepcopy(init))

    def serialize(self):
        """Serialize this Message object as JSON."""
        return json.dumps(self)

    @staticmethod
    def deserialize(msg):
        """Deserialize a json string returning a Message object."""
        return Message(json.loads(msg))
