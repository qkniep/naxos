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
        self.group_size = num_peers
        if leader is None:
            self.current_leader = (node_id, self.network_node.listen_addr)
        else:
            self.current_leader = leader
        self.current_id = (0, node_id)
        self.highest_promised = (0, 0)
        self.promises = 1
        self.acceptances = 1
        self.accepted_value = None
        self.chosen = False

    def start_paxos_round(self, value):
        """Starts a paxos round.

        Relays the value to the leader or sends the prepare if we are leader.
        Ultimately we try to get value chosen.
        """
        if self.current_leader[0] == self.node_id():
            self.current_id = (self.current_id[0] + 1, self.current_id[1])
            self.network_node.broadcast({
                'do': 'paxos_prepare',
                'id': self.current_id,
            })
            self.accepted_value = value  # XXX
            self.promises = 1
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
            self.accepted_value = value  # XXX
        print(self.majority())
        if self.promises == self.majority():
            self.acceptances = 1
            self.network_node.broadcast({
                'do': 'paxos_propose',
                'id': proposal_id,
                'value': self.accepted_value,
            })

    def handle_propose(self, src, proposal_id, value):
        """Handles a paxos propose message."""
        if proposal_id < self.highest_promised:
            print('Rejecting Proposal: proposal_id < highest_promised')
            return
        self.accepted_value = value
        # self.accepted_id = proposal_id  #???
        self.network_node.send(src, {
            'do': 'paxos_accept',
            'id': proposal_id
        })

    def handle_accept(self, proposal_id):
        """Handles a paxos accept message."""
        if proposal_id != self.current_id:
            print('Rejecting Accept: proposal_id != current_id')
            return None
        self.acceptances += 1
        if self.acceptances == self.majority():
            self.network_node.broadcast({
                'do': 'paxos_learn',
                'id': proposal_id,
                'value': self.accepted_value,
            })
            self.chosen = True
            return self.accepted_value
        return None

    def handle_learn(self, _proposal_id, value):
        """Handles a paxos learn message."""
        self.accepted_value = value
        self.chosen = True

    def add_peer_addr(self, node_id, addr):
        """Adds a mapping (node ID -> address) to this peer's list of such mappings."""
        self.peer_addresses[node_id] = addr

    def majority(self):
        """Returns the number of peers needed for a majority (strictly more than 50%)."""
        return (self.group_size) // 2 + 1

    def node_id(self):
        """Returns this peer's paxos node ID."""
        return self.current_id[1]
