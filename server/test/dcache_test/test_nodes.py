from datetime import datetime
from itertools import chain
from unittest import TestCase

# noinspection PyProtectedMember
from dcache.nodes import (
    _index_for,
    Nodes,
    _node_for_index,
)
from given_when_then import (
    given_when_then_test,
    GWT,
)


class TestNodesFunctions(TestCase):

    # noinspection PyPep8Naming,PyAttributeOutsideInit
    @given_when_then_test
    class test_hash_differs_when_using_indices(GWT):
        """Given on node is str try different indices and see that each hash is
        unique. This is of course not a very strong test, just trying out the
        basics.
        """

        def given(self):
            self.redundancy = 3
            self.replicas = 2
            self.node_id = "abc"

        def when(self):
            self.indices_on_hash_circles = [
                [_index_for(self.node_id, redundancy_index, replica_index)
                    for replica_index in range(self.replicas)]
                for redundancy_index in range(self.redundancy)
            ]

        def then(self):
            indices = set(chain.from_iterable(self.indices_on_hash_circles))
            assert len(indices) == self.redundancy * self.replicas

    # noinspection PyPep8Naming,PyAttributeOutsideInit
    @given_when_then_test
    class test_right_node_when_asking(GWT):
        """Define a distribution circle with two nodes, two replicas each, and
        try to get the (right) ids for a bunch of indices.
        """

        NODE_1_ID = "a"

        NODE_2_ID = "b"

        def given(self):
            # What the test circle looks like:
            self.distribution_circle = [
                (0.1, self.NODE_1_ID),
                (0.2, self.NODE_2_ID),
                (0.5, self.NODE_1_ID),
                (0.9, self.NODE_2_ID),
            ]
            # What we expect for each index (always go anti clock wise):
            self.indices_to_test = [
                (0.00, self.NODE_1_ID),  # goes to first entry, node 1
                (0.11, self.NODE_2_ID),  # something for node 2
                (0.19, self.NODE_2_ID),  # something still for node 2
                (0.21, self.NODE_1_ID),  # something for node 1
                (0.90, self.NODE_2_ID),  # exact match
                (0.91, self.NODE_1_ID),  # wraps around
            ]

        def when(self):
            # Run each index through the algorithm and safe the result.
            self.nodes = [
                _node_for_index(self.distribution_circle, index)
                for index, _ in self.indices_to_test
            ]

        def then(self):
            # Compare what we got with what was expected.
            to_compare = zip(self.nodes, self.indices_to_test)
            for node, (index, should_be) in to_compare:
                assert node == should_be, f"index: {index}"


class TestNodes(TestCase):

    def setUp(self):
        self.nodes = Nodes("", "")
        self.node_id = self.nodes.node_id  # shortcut

    # noinspection PyPep8Naming,PyAttributeOutsideInit
    @given_when_then_test
    class test_distribution_circles_populated_when_new_node_object(GWT):
        """Check the right entries exist in a new nodes object.
        """

        def given(self):
            node_id = self.test_case.node_id
            # Calculate each distribution circle.
            self.circles = [
                sorted(
                    _index_for(node_id, 0, replica_index)  # redundancy of one
                    for replica_index in range(Nodes.REPLICAS)
                )
            ]

        def when(self):
            self.test_case.nodes.get_nodes_for("")  # populates circles

        def then(self):
            circles = self.test_case.nodes._distribution_circles  # shortcut
            # Compare with pre calculated values.
            assert len(circles) == len(self.circles)
            for circle, expected_indices in zip(circles, self.circles):
                assert all(
                    node_id == self.test_case.node_id for _, node_id in circle)
                indices = [index for index, _ in circle]
                for index, expected_index in zip(indices, expected_indices):
                    assert index == expected_index

    # noinspection PyPep8Naming,PyAttributeOutsideInit
    @given_when_then_test
    class test_removed_when_remove(GWT):
        """Check node information gets removed properly, ones the timeout
        passes.
        """

        # noinspection PyUnusedLocal
        @staticmethod
        def create_node(request, publish, last_seen):
            # noinspection PyCallByClass,PyTypeChecker
            return Nodes.Node(request, publish, last_seen, None)

        def given(self):
            self.other_node_id = "b"
            other_node = ("", "", datetime.utcnow() - Nodes.TIMEOUT * 2)
            self.test_case.nodes.update_nodes(
                {self.other_node_id: other_node}, self.create_node)

        def when(self):
            self.test_case.nodes.remove_dead_nodes()

        def then(self):
            # Check all information is gone.
            nodes = self.test_case.nodes.nodes
            assert len(nodes) == 0
            for circle in self.test_case.nodes._distribution_circles:
                assert all(
                    node_id == self.test_case.node_id for _, node_id in circle)

    # noinspection PyPep8Naming,PyAttributeOutsideInit
    @given_when_then_test
    class test_node_present_when_update(GWT):
        """Add a node, check all information is present.
        """

        # noinspection PyUnusedLocal
        @staticmethod
        def create_node(request, publish, last_seen):
            # noinspection PyTypeChecker
            return Nodes.Node(None, None, None, last_seen)

        def given(self):
            self.other_node_id = "b"

        def when(self):
            self.test_case.nodes.update_nodes(
                {self.other_node_id: ("1", "1", datetime.utcnow())},
                self.create_node)

        def then(self):
            # Look for other node id:
            node_ids = self.test_case.nodes.nodes.keys()
            assert self.other_node_id in node_ids
            # Check circle contains new id, too:
            for circle in self.test_case.nodes._distribution_circles:
                assert any(
                    node_id == self.other_node_id for _, node_id in circle)
