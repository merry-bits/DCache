# -*- coding: utf-8 -*-
from bisect import bisect_left
from datetime import datetime, timedelta
from hashlib import md5
from logging import getLogger
from operator import itemgetter
from sys import byteorder


_LOG = getLogger(__name__)


def _index_for_node(node_id, replica):
    m = md5()
    m.update("{}_{}".format(node_id, replica).encode('utf-8'))
    return (
        int.from_bytes(m.digest(), byteorder=byteorder, signed=False) /
        pow(2, m.digest_size * 8))


class Nodes():
    """List of currently available nodes.

    Determines where a key should be saved. Each node has several places (owns
    some part of the key space) on the distribution circle. For each redundancy
    there is a separate circle.
    """

    TIMEOUT = timedelta(seconds=10)
    REPLICAS = 5
    REDUNDANCY = 3

    def __init__(self, node_id, req_address, pub_address):
        """Save information about the local node first.

        Adds the local node to the distribution map.
        """
        self.id = node_id
        self.req_address = req_address
        self.pub_address = pub_address
        self._nodes = {}
        self._distributions = [  # [(id, index)] times REDUNDANCY
            [] for _ in range(self.REDUNDANCY)]
        self._add_to_distribution(self.id)

    @property
    def nodes(self):
        # Add own node to the list, before returning it.
        all_nodes = {
            self.id: (self.req_address, self.pub_address, datetime.now())}
        all_nodes.update(self._nodes)
        return all_nodes

    def _add_to_distribution(self, node_id):
        """Put the new node_id on the distribution circles.
        """
        nodes = [self.id]
        nodes.extend(self._nodes.keys())
        nodes.sort()
        index = nodes.index(node_id)
        for redundancy, distribution in enumerate(self._distributions):
            shifted_node_id = nodes[(index + redundancy) % len(nodes)]
            distribution.extend(
                (node_id, _index_for_node(shifted_node_id, j))
                for j in range(self.REPLICAS))
            distribution.sort(key=itemgetter(1))

    def _remove_from_distribution(self, remove_id):
        """Filter the distribution map, leaving the node to be removed out.
        """
        self._distributions = [
            [(node_id, index)
                for node_id, index in distribution if node_id != remove_id]
            for distribution in self._distributions]

    def add_node(self, node_id, req_addr, pub_addr, last_update=None):
        """Add anode to the list and update the distribution. """
        self._nodes[node_id] = [
            req_addr, pub_addr,
            datetime.now() if last_update is None else last_update]
        self._add_to_distribution(node_id)

    def update_nodes(self, nodes):
        """Update the node last seen time stamp or add the node to the list.

        :return: list of newly added nodes
        :rtype: [(node_id, req_addr, pub_addr), ]
        """
        new_nodes = []
        for node_id, (req_addr, pub_addr, last_update) in nodes.items():
            if node_id != self.id:
                node = self._nodes.get(node_id)
                if node is None:
                    new_nodes.append((node_id, req_addr, pub_addr))
                    self.add_node(node_id, req_addr, pub_addr, last_update)
                elif last_update > self._nodes[node_id][-1]:
                    self._nodes[node_id][-1] = last_update
        return new_nodes

    def remove_dead_nodes(self):
        """Look for nods that have not been heard of in a long time and remove
        them.

        :return: [node_id]
        """
        now = datetime.now()
        # Filter out dead nodes.
        removed_nodes = [
            node_id
            for node_id, (_, _, last_update) in self._nodes.items()
            if now - last_update > self.TIMEOUT]
        # Remove them.
        for node_id in removed_nodes:
            self._nodes.pop(node_id, None)
            self._remove_from_distribution(node_id)
        return removed_nodes

    def get_nodes_for_index(self, index):
        """Check to which node the index belongs, for each redundancy.

        :rtype: set(node_id)
        """
        nodes = set()
        for distribution in self._distributions:
            distribution_index = 0  # index is bigger than last entry
            if index <= distribution[-1][1]:
                distribution_index = bisect_left(
                    [d[1] for d in distribution], index)
            nodes.add(distribution[distribution_index][0])
        return nodes
