class PaxosNode:

    def __init__(self, network_node, node_id, num_peers):
        self.network_node = network_node
        self.majority = (num_peers + 1) // 2
        self.current_id = (0, node_id)
        self.highest_promised = (0, 0)

    def start_paxos_round(self, value):
        self.current_id = (self.current_id[0] + 1, self.current_id[1])
        self.node.broadcast({
            'command': 'prepare',
            'id': current_id,
        })
        self.accepted_value = value
        self.promises = 0

    def handle_prepare(self, proposal_id):
        if proposal_id < self.highest_promised:
            return
        self.highest_promised = proposal_id
        self.node.send(src, {
            'command': 'promise',
            'id': proposal_id,
            'accepted': accepted_value,
        })

    def handle_promise(self, proposal_id, value):
        if proposal_id != self.current_id:
            return
        self.promises += 1
        if value != None:
            accepted_value = value  #???
        if self.promises >= self.majority:
            self.node.broadcast({
                'command': 'propose',
                'id': proposal_id,
                'value': accepted_value,
            })

    def handle_propose(proposal_id, value):
        if proposal_id < self.highest_promised:
            return
        self.accepted_value = value
        #self.accepted_id = proposal_id  #???
        self.acceptances = 0
        self.node.send(src, {
            'command': 'accepted',
            'id': proposal_id
        })

    def handle_accepted():
        if id != self.current_id:
            return
        self.acceptances += 1
        if self.acceptances >= self.majority:
            self.node.broadcast({
                'command': 'learn',
                'id': proposal_id,
                'value': self.accepted_value,
            })
            self.chosen = true
