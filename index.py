class Index():

    def __init__(self):
        self.index = {}
        # TODO: self.reverse_index = {}  // map client addr -> filename
        self.change_log = []

    def search_entry(self, filename):
        return self.index[filename]

    def add_entry(self, filename, addr):
        self.index[filename] = addr

    def remove_entry(self, filename):
        del self.index[filename]


