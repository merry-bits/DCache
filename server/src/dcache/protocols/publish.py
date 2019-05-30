from datetime import datetime
from itertools import chain
from logging import getLogger

from dcache.zmq import ENCODING
from dcache.zmq import datetime_to_str
from dcache.zmq import str_to_datetime

_LOG = getLogger(__name__)


class PublishServer:

    def update_nodes(self, nodes):
        """

        :param nodes: Dictionary of node key and node information: request
            address, publish address and last seen time stamp.
        :type nodes: {node_id: (str, str, datetime.datetime)}
        :return: List of nodes ids that where added.
        :rtype: [str]
        """
        raise NotImplementedError()

    def redistribute(self, *node_ids):
        """

        :type node_ids: str
        """
        raise NotImplementedError()

    def get_other_nodes(self):
        """

        :return: Dictionary with node id and node object.
        :rtype: {node_id: dcache.nodes.Nodes.Node}
        """
        raise NotImplementedError()

    def get_server_node_info(self):
        """Get the node id, request address and publish address from the servers
        own node.

        :rtype: (str, str, str)
        """
        raise NotImplementedError()


class PublishProtocol:

    TOPIC = b"n"  # publish nodes

    server: PublishServer = None

    @staticmethod
    def handle(data):
        """

        :type data: [bytes]
        :rtype: APIProtocol
        """
        assert data[0] == PublishProtocol.TOPIC
        server = PublishProtocol.server  # shortcut
        assert server is not None
        nodes = {}
        data = (part.decode(ENCODING) for part in data[1:])
        node_id = None
        req_address = None
        pub_address = None
        for i, part in enumerate(data):
            i4 = i % 4
            if i4 == 0:
                node_id = part
            elif i4 == 1:
                req_address = part
            elif i4 == 2:
                pub_address = part
            elif i4 == 3:
                last_seen = part
                last_seen_date = str_to_datetime(last_seen)
                nodes[node_id] = (req_address, pub_address, last_seen_date)
        # _LOG.debug("Got node list: %s", nodes)
        added_nodes = server.update_nodes(nodes)
        if added_nodes:
            _LOG.debug("%s node(s) where added.", len(added_nodes))
            server.redistribute(*added_nodes)

    @staticmethod
    def build_publish():
        """

        :return: List of bytes to publish.
        :rtype: iterable
        """
        server = PublishProtocol.server  # shortcut
        assert server is not None
        nodes = [(PublishProtocol.TOPIC.decode(ENCODING),)]  # topic frame
        nodes_list = [
            (node_id, node.req_address, node.pub_address,
                datetime_to_str(node.last_seen))
            for node_id, node in server.get_other_nodes().items()
        ]
        nodes.extend(nodes_list)
        # Add local node.
        now = datetime.utcnow()
        now_str = datetime_to_str(now)
        nodes.append((*server.get_server_node_info(), now_str))
        # _LOG.debug("Publishing %s: %s nodes.", self._pub_address, nodes)
        return [p.encode(ENCODING) for p in chain.from_iterable(nodes)]
