import json


class Index():

    def __init__(self):
        self.index = {}
        # TODO: self.reverse_index = {}  // map client addr -> filename

    def search_entry(self, filename):
        return self.index.get(filename)

    def add_entry(self, filename, addr):
        self.index[filename] = addr

    def remove_entry(self, filename):
        del self.index[filename]

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    def fromJSON(self, index):
        self.index = json.loads(index)['index']
