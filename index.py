# -*- coding: utf-8 -*-
""""""

import json


class Index():
    """Maintains """

    def __init__(self):
        """Creates a new empty index with no entries."""
        self.index = {}          # map filename -> client addr
        # TODO: self.reverse_index = {}  # map client addr -> filenames

    def search_entry(self, filename):
        """Searches the index for an entry exactly matching a filename.

        Returns:
            The address (ip, port) of a client holding the searched file.
            Or None if no such peer exists in the index.
        """
        return self.index.get(filename)

    def add_entry(self, filename, addr):
        """Add an entry to the index."""
        self.index[filename] = addr

    def remove_entry(self, filename):
        """Remove a file from the index."""
        del self.index[filename]

    def to_json(self):
        """Serializes this index to JSON for sending it over the wire."""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    def from_json(self, index):
        """Deserializes index from JSON such that:
        i2.from_json(i1.to_json()) leads to i2 having the same state as i1.
        """
        self.index = json.loads(index)['index']
