# -*- coding: utf-8 -*-
"""Paxos distributed consensus protocol, currently only Single-Paxos w/o leader."""


class PaxosNode:
    """Maintains paxos state.
    """

    def __init__(self, network_node, num_peers=1, leader=None):
        """Initializes a new paxos node."""
        node_id = network_node.unique_id_from_own_addr()
        print('This peer is now operating Paxos node', node_id)
        print('Paxos group size is', num_peers)
        self.network_node = network_node
        self.peer_addresses = {}  # map node_id -> remote socket addr
        if num_peers == 1:
            self.group_sizes = [1]
        else:
            self.group_sizes = [2]
        if leader is None:
            self.current_leader = (node_id, self.network_node.listen_addr)
        else:
            self.current_leader = leader
        self.current_id = (0, node_id)
        self.highest_promised = (0, 0)
        self.promises = 1
        self.acceptances = []
        self.accepted_values = []
        self.chosen = []
        self.next_index = 0

    def start_paxos_round(self, value):
        """Starts a paxos round.

        Relays the value to the leader or sends the prepare if we are leader.
        Ultimately we try to get value chosen.
        """
        if self.current_leader[0] == self.node_id():
            # self.current_id = (self.current_id[0] + 1, self.current_id[1])
            self.network_node.broadcast({
                'do': 'paxos_propose',
                'id': self.current_id,
                'index': self.next_index,
                'value': value
            })
            self.accepted_values.append(value)  # XXX
            self.acceptances.append(1)
            self.next_index += 1
        else:
            self.network_node.send(self.peer_addresses[self.current_leader[0]], {
                'do': 'paxos_relay',
                'value': value,
            })

    def handle_prepare(self, src, proposal_id):
        """Handles a paxos prepare message."""
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
        """Handle a paxos promise message."""
        if proposal_id != self.current_id:
            print('Rejecting Promise: proposal_id != current_id')
            return
        self.promises += 1
        if value is not None:
            self.accepted_value = value  # XXX only accept value with highest ID???
        if self.promises == self.majority(0):  # TODO: self.majority(index)
            self.acceptances = 1
            self.network_node.broadcast({
                'do': 'paxos_propose',
                'id': proposal_id,
                'value': self.accepted_value,
            })

    def handle_propose(self, src, proposal_id, index, value):
        """Handles a paxos propose message."""
        if proposal_id < self.highest_promised:
            print('Rejecting Proposal: proposal_id < highest_promised')  # TODO: implement NACK
            return
        while len(self.accepted_values) <= index:
            self.accepted_values.append(None)
        self.accepted_values[index] = value
        # self.accepted_id = proposal_id  #???
        self.network_node.send(src, {
            'do': 'paxos_accept',
            'id': proposal_id,
            'index': index,
        })

    def handle_accept(self, proposal_id, index):
        """Handles a paxos accept message."""
        if proposal_id != self.current_id:
            print('Rejecting Accept: proposal_id != current_id')
            return -1, None
        self.acceptances[index] += 1
        if self.acceptances[index] == self.majority(index):
            self.network_node.broadcast({
                'do': 'paxos_learn',
                'id': proposal_id,
                'index': index,
                'value': self.accepted_values[index],
            })
            while len(self.chosen) <= index:
                self.chosen.append(False)
            self.chosen[index] = True
            return index, self.accepted_values[index]
        return -1, None

    def handle_learn(self, _proposal_id, index, value):
        """Handles a paxos learn message."""
        while len(self.accepted_values) <= index:
            self.accepted_values.append(None)
        self.accepted_values[index] = value
        while len(self.chosen) <= index:
            self.chosen.append(False)
        self.chosen[index] = True

    def add_peer_addr(self, node_id, addr):
        """Adds a mapping (node ID -> address) to this peer's list of such mappings."""
        self.peer_addresses[node_id] = addr

    def majority(self, index):
        """Returns the number of peers needed for a majority (strictly more than 50%)."""
        return (self.group_sizes[index]) // 2 + 1

    def node_id(self):
        """Returns this peer's paxos node ID."""
        return self.current_id[1]
