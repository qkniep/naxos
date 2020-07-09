# -*- coding: utf-8 -*-
"""Paxos distributed consensus protocol, currently only Single-Paxos w/o leader."""

import logging as log


class PaxosNode:
    """Maintains paxos state.
    """

    def __init__(self, network_node, leader=None):
        """Initializes a new paxos node."""
        node_id = network_node.unique_id_from_own_addr()
        # log.debug('Paxos group size is %s', num_peers)
        self.network_node = network_node
        if leader is None:
            self.group_sizes = [1]
        else:
            self.group_sizes = [2]
        if leader is None:
            self.current_leader = node_id
        else:
            self.current_leader = leader
        self.current_id = (0, node_id)
        self.highest_promised = (0, 0)
        self.highest_numbered_proposal = None
        self.promises = 1
        self.acceptances = []
        self.log = []
        self.chosen = []

    def start_election(self):
        """Tries to become the new leader through Prepare/Promise."""
        self.current_id = (self.current_id[0] + 1, self.node_id())
        self.promises = 1
        self.network_node.broadcast({
            'do': 'paxos_prepare',
            'proposal_id': self.current_id,
        })
        self.highest_promised = self.current_id

    def start_paxos_round(self, value):
        """Starts a paxos round.

        Relays the value to the leader or sends the prepare if we are leader.
        Ultimately we try to get value chosen.
        """
        if self.current_leader == self.node_id():
            # self.current_id = (self.current_id[0] + 1, self.current_id[1])
            self.network_node.broadcast({
                'do': 'paxos_propose',
                'proposal_id': self.current_id,
                'index': len(self.log),
                'value': value
            })
            self.log.append(value)
            self.acceptances.append(1)
        else:
            self.network_node.send(self.current_leader, {
                'do': 'paxos_relay',
                'value': value,
            })

    def handle_prepare(self, src, proposal_id):
        """Handles a paxos prepare message."""
        if proposal_id < self.highest_promised:
            log.info('Rejecting Prepare: proposal_id < highest_promised')
            return
        self.highest_promised = proposal_id
        majority = self.majority(self.get_last_applied_value_index())
        self.network_node.send(src, {
            'do': 'paxos_promise',
            'proposal_id': proposal_id,
            'acc_id': self.current_id,
            'accepted': self.log,
            'majority': majority,
        })
        self.current_leader = proposal_id[1]

    def handle_promise(self, proposal_id, acc_id, value, majority):
        """Handle a paxos promise message."""
        if proposal_id != self.current_id:
            log.info('Rejecting Promise: proposal_id != current_id')
            return
        self.promises += 1
        if self.highest_numbered_proposal is None or acc_id > self.highest_numbered_proposal:
            self.log = value
            self.highest_numbered_proposal = acc_id
        if self.promises == majority:
            self.current_leader = self.node_id()

    def handle_propose(self, src, proposal_id, index, value):
        """Handles a paxos propose message."""
        if proposal_id < self.highest_promised:
            log.info('Rejecting Proposal: proposal_id < highest_promised')
            return
        while len(self.log) <= index:
            self.log.append(None)
        self.log[index] = value
        self.network_node.send(src, {
            'do': 'paxos_accept',
            'proposal_id': proposal_id,
            'index': index,
        })

    def handle_accept(self, proposal_id, index):
        """Handles a paxos accept message."""
        if proposal_id != self.current_id:
            log.info('Rejecting Accept: proposal_id != current_id')
            return -1, None
        self.acceptances[index] += 1
        if self.acceptances[index] == self.majority(index):
            self.network_node.broadcast({
                'do': 'paxos_learn',
                'index': index,
                'value': self.log[index],
            })
            while len(self.chosen) <= index:
                self.chosen.append(False)
            self.chosen[index] = True
            return index, self.log[index]
        return -1, None

    def handle_learn(self, index, value):
        """Handles a paxos learn message."""
        while len(self.log) <= index:
            self.log.append(None)
        self.log[index] = value
        while len(self.chosen) <= index:
            self.chosen.append(False)
        self.chosen[index] = True

    def fix_log_holes(self, index):
        """."""
        up_to_date = True
        for i in range(index+1):
            if not self.chosen[i]:
                up_to_date = False
                self.network_node.send(self.current_leader, {
                    'do': 'paxos_fill_log_hole',
                    'index': i,
                })
        return up_to_date

    def is_leader(self):
        """Returns whether this paxos node thinks itself to be the leader."""
        return self.current_leader == self.node_id()

    def get_last_applied_value_index(self):
        try:
            return self.chosen.index(False)
        except ValueError:
            return -1

    def majority(self, index):
        """Returns the number of peers needed for a majority (strictly more than 50%)."""
        while index >= len(self.group_sizes):
            if self.log[index-1]['change'] not in ['join', 'leave']:
                index -= 1
        return (self.group_sizes[index]) // 2 + 1

    def node_id(self):
        """Returns this peer's paxos node ID."""
        return self.current_id[1]
