class PaxosNode:

    def __init__(self, network_node, node_id, num_peers):
        print('This peer is now operating Paxos node %i in a group of size %i' % (node_id, num_peers))
        self.network_node = network_node
        self.group_size = num_peers
        self.current_id = (0, node_id)
        self.highest_promised = (0, 0)
        self.accepted_value = None
        self.chosen = False

    def start_paxos_round(self, value):
        self.current_id = (self.current_id[0] + 1, self.current_id[1])
        self.network_node.broadcast({
            'do': 'paxos_prepare',
            'id': self.current_id,
        })
        self.accepted_value = value
        self.promises = 1

    def handle_prepare(self, src, proposal_id):
        if proposal_id < self.highest_promised:
            print('Rejecting Prepare: proposal_id < highest_promised')
            return
        self.highest_promised = proposal_id
        self.network_node.send(src, {
            'do': 'paxos_promise',
            'id': proposal_id,
            'accepted': self.accepted_value,
        })

    def handle_promise(self, proposal_id, value):
        if proposal_id != self.current_id:
            print('Rejecting Promise: proposal_id != current_id')
            return
        self.promises += 1
        if value is not None:
            self.accepted_value = value  #???
        if self.promises == self.majority():
            self.acceptances = 1
            self.network_node.broadcast({
                'do': 'paxos_propose',
                'id': proposal_id,
                'value': self.accepted_value,
            })

    def handle_propose(self, src, proposal_id, value):
        if proposal_id < self.highest_promised:
            print('Rejecting Proposal: proposal_id < highest_promised')
            return
        self.accepted_value = value
        #self.accepted_id = proposal_id  #???
        self.network_node.send(src, {
            'do': 'paxos_accept',
            'id': proposal_id
        })

    def handle_accept(self, proposal_id):
        if proposal_id != self.current_id:
            print('Rejecting Accept: proposal_id != current_id')
            return
        self.acceptances += 1
        if self.acceptances == self.majority():
            self.network_node.broadcast({
                'do': 'paxos_learn',
                'id': proposal_id,
                'value': self.accepted_value,
            })
            self.chosen = True

    def handle_learn(self, proposal_id, value):
        self.accepted_value = value
        self.chosen = True

    def majority(self):
        return (self.group_size + 1) // 2
