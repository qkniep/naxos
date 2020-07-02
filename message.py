# -*- coding: utf-8 -*-
"""The message module provides abstractions for reasoning about messages."""

import copy
import json
import uuid


class Message(dict):
    """Message objects are JSON-serializable collections of key/value pairs."""

    def __init__(self, init={}, paxos_id=None):
        """Create a new Message by deepcopying the init dictionary."""
        super().__init__(copy.deepcopy(init))
        if 'id' not in init:  # random message id if not set
            self['id'] = str(uuid.uuid4())

        if 'from' not in init:
            if paxos_id is None:
                raise ValueError("paxos_id must be provided.")
            self['from'] = paxos_id

    def serialize(self):
        """Serialize this Message object as JSON."""
        return json.dumps(self)

    @staticmethod
    def deserialize(msg):
        """Deserialize a json string returning a Message object."""
        return Message(json.loads(msg))
