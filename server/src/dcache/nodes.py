from bisect import bisect_left
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from hashlib import md5
from logging import getLogger
from sys import byteorder
from uuid import uuid1

from zmq import Socket

_LOG = getLogger(__name__)

_HASH_ALGORITHM = md5

# digest_size is in bytes, so calculating the max value is a matter of
# calculating the right 2^x value:
_MAX_HASH_VALUE = pow(2, _HASH_ALGORITHM().digest_size * 8)


def index_for(key):
    """Calculate the index on the distribution circle of the hashed key value.

    :type key: str
    :return: The index, between 0 and 1.
    :rtype: float
    """
    hash_object = _HASH_ALGORITHM()
    hash_object.update(key.encode('utf-8'))
    hash_bytes = hash_object.digest()
    hash_value = int.from_bytes(hash_bytes, byteorder=byteorder, signed=False)
    return hash_value / _MAX_HASH_VALUE


def _index_for(node_id, redundancy, replica):
    """Given the node_id and the context calculate the unique position on the
    distribution circle.

    :type node_id: str
    :param redundancy: The index in the context of redundancy.
        0 is the original. 1 the first copy and so one.
    :type redundancy: int
    :param replica: The replica index of the node node_id on the distribution
        circle.
    :type replica: int
    :return: The index, between 0 and 1.
    :rtype: float
    """
    return index_for(f"{node_id}_{redundancy}_{replica}")


def _node_for_index(distribution_circle, index):
    """Find the responsible node for the hash index given hash indices for all
    available nodes.

    :param distribution_circle: Tuples of index (between 0 and 1) and
        corresponding node node_id.
    :type distribution_circle: [(float, str)]
    :param index: float
    :return: The node node_id of the node responsible for index.
    :rtype: str
    """
    assert distribution_circle
    assert index >= 0.0
    assert index <= 1.0
    indices = [index for index, _ in distribution_circle]
    # If the index is bigger than the last circle entry it belongs to is the
    # first entry (wrap around, anti clock wise).
    last_index = indices[-1]
    circle_index = 0 if index > last_index else bisect_left(indices, index)
    return distribution_circle[circle_index][1]


class Nodes:
    """Currently available cache server nodes.

    Determines where a key should be saved. Each node has several places (owns
    some part of the key space) on the distribution circle. For each redundancy
    there is a separate circle.
    """

    TIMEOUT = timedelta(seconds=12)

    REPLICAS = 5

    REDUNDANCY = 3

    @dataclass
    class Node:

        req_address: str

        pub_address: str

        last_seen: datetime

        req_socket: Socket

    def __init__(self, request_address, publish_address):
        # Server (local) node:
        self._request_address = request_address
        self._publish_address = publish_address
        self.node_id = uuid1().hex  # unique node node_id, based on current time
        # Other nodes:
        self.nodes = {}  # {str: Node}
        # One distribution circle per redundancy level:
        # Define as many circles as REDUNDANCY, but at most as many nodes there
        # are, which is one, for now.
        self._distribution_circles = [
            []  # [(index, node_id)]
        ]
        self._add_to_circles(self.node_id)
        self._nodes_count_on_most_recent_redistribute = 1

    def _add_to_circles(self, node_id, circles=None, recalculate_indices=False):
        """Put the new node_id on the distribution circles.
        """
        circles = self._distribution_circles if circles is None else circles
        # Add the new node to each redundant circle of hash indexes.
        # Reminder: one circle (from 0 to 1) has times REPLICAS indices per
        # node.
        for redundancy_index, circle in enumerate(circles):
            # Calculate and add all indices for one circle. Keep track what
            # hash index belongs to which node!
            if recalculate_indices:
                circle.clear()
                node_ids = list(self.nodes.keys())  # add all nodes again
                node_ids.append(self.node_id)
            else:
                node_ids = [node_id]  # only add given node
            for node_id in node_ids:
                circle.extend(
                    (_index_for(node_id, redundancy_index, replica_index),
                        node_id)
                    for replica_index in range(self.REPLICAS))
            # Put all hash indices in order again.
            circle.sort()

    def _remove_from_circles(self, remove_id, circles=None):
        """Filter the distribution map, leaving the node to be removed out.
        """
        circles = self._distribution_circles if circles is None else circles
        new_circles = [
            # entry: (index, node_id)
            [entry for entry in circle if entry[1] != remove_id]
            for circle in circles]
        circles.clear()
        circles.extend(new_circles)

    def _add_node(self, node_id, node):
        """Add Node to storage and distribution circle.

        The local node (self.node_id) is only added to the circle!

        :type node_id: str
        :type node: Nodes.Node
        """
        assert node_id not in self.nodes
        recalculate_indices = False
        if node_id != self.node_id:
            self.nodes[node_id] = node
            circles_len = len(self._distribution_circles)
            nodes_len = len(self.nodes) + 1
            # Increase redundancy? Now that one more node is available.
            if circles_len < nodes_len and circles_len < self.REDUNDANCY:
                self._distribution_circles.append([])
                recalculate_indices = True
        self._add_to_circles(node_id, recalculate_indices=recalculate_indices)

    def _remove_node(self, node_id):
        """Remove node.

        :type node_id: str
        :return: The removed node. Socket are still open!
        :rtype: Nodes.Node
        """
        assert node_id in self.nodes
        nodes_count = len(self.nodes) + 1
        # More circles than nodes are not necessary!
        if nodes_count - 1 < len(self._distribution_circles):
            self._distribution_circles.pop(-1)
        self._remove_from_circles(node_id)
        return self.nodes.pop(node_id)

    def redistribute(self, keys, *node_ids):
        """Given a list of nodes that changed (where added or removed) and a
        list of keys (key and hash index tuples) calculate the new node ids
        for each key.

        :param node_ids:
        :param keys: Tuples of key and hash index of the key.
        :type keys: iterable
        :return: Two dictionaries. The first contains the the nodes where a key
            needs to be send to and the second tells if the local node should
            keep a key or not.
        :rtype: tuple
        """
        if not node_ids:
            return
        # Calculate what the circles where, before self.nodes was changed.
        old_circles = [
            []
            for i, _ in enumerate(self._distribution_circles)
            if i < self._nodes_count_on_most_recent_redistribute]
        self._nodes_count_on_most_recent_redistribute = len(self.nodes) + 1
        self._add_to_circles(self.node_id, old_circles)
        for node_id in self.nodes.keys():
            self._add_to_circles(node_id, old_circles)
        for node_id in node_ids:
            if node_id in self.nodes:  # was added
                self._remove_from_circles(node_id, old_circles)
            else:
                self._add_to_circles(node_id, old_circles)  # was there before
        # Now calculate ownerships.
        send_list = defaultdict(set)
        keep = defaultdict(lambda: False)
        me_set = frozenset(self.node_id)
        # Check what to do with each key.
        for key, key_index in keys:
            old_nodes = self.get_nodes_for_index(key_index, old_circles)
            old_nodes_set = frozenset(old_nodes.keys())
            new_nodes = self.get_nodes_for_index(key_index)
            new_nodes_set = frozenset(new_nodes.keys())
            # If local node did own the key, where should it go?
            if self.node_id in old_nodes:
                # Which nodes need the key?
                send_list[key] |= new_nodes_set - old_nodes_set - me_set
            # What to keep? Other nodes might have sent it already!
            if self.node_id in new_nodes_set:
                keep[key] = True
        return send_list, keep

    def update_nodes(self, nodes, create_node):
        """Update the node last seen time stamp or add the node to the list.

        :param nodes: Dictionary of node ids with request, publish and last seen
            tuples.
        :type nodes: dict
        :param create_node: Function that takes a request, publish and last seen
            tuple and returns a Nodes.Node object.
        :return: Added node ids.
        :rtype: list
        """
        # Create sets with unique socket addresses.
        request_addresses = {
            node.req_address for node in self.nodes.values()}
        request_addresses.add(self._request_address)
        publish_addresses = {
            node.pub_address for node in self.nodes.values()}
        publish_addresses.add(self._publish_address)
        # See what can be added or updated.
        added_nodes = []
        for node_id, (request, publish, last_seen) in nodes.items():
            if node_id != self.node_id:
                stored_node = self.nodes.get(node_id)
                # Request address and publish address can not be reused. Block
                # new nodes with existing addresses.
                if (stored_node is None and request not in request_addresses and
                        publish not in publish_addresses):
                    node = create_node(request, publish, last_seen)
                    _LOG.debug("Adding node: %s", node_id)
                    self._add_node(node_id, node)
                    added_nodes.append(node_id)
                elif (stored_node is not None and
                      last_seen > stored_node.last_seen):
                    stored_node.last_seen = last_seen
        return added_nodes

    def remove_dead_nodes(self):
        """Look for nods that have not been heard of in a long time and remove
        them.

        :return: List of removed nod id and node tuples.
        :rtype: list
        """
        now = datetime.utcnow()
        # Filter out dead nodes.
        to_remove = [
            node_id
            for node_id, node in self.nodes.items()
            if now - node.last_seen > self.TIMEOUT]
        # Remove them.
        for node_id in to_remove:
            last_seen = self.nodes[node_id].last_seen
            _LOG.debug("Node %s is stale: %s %s", node_id, now, last_seen)
        removed_nodes = (
            (node_id, self._remove_node(node_id)) for node_id in to_remove)
        return [
            (node_id, node)
            for node_id, node in removed_nodes if node is not None]

    def get_nodes_for(self, key, circles=None):
        """Check to which node the key belongs, for each redundancy.

        :return: Dictionary with node ids and their Nodes.Node objects.
        :rtype: dict
        """
        return self.get_nodes_for_index(index_for(key), circles)

    def get_nodes_for_index(self, key_index, circles=None):
        """Check to which node the key belongs, for each redundancy.

        :return: Dictionary with node ids and their Nodes.Node objects.
        :rtype: dict
        """
        circles = self._distribution_circles if circles is None else circles
        assert circles
        # What are the responsible nodes?
        nodes = (_node_for_index(circle, key_index) for circle in circles)
        return {node_id: self.nodes.get(node_id) for node_id in nodes}
