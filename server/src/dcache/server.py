from datetime import datetime
from datetime import timedelta
from itertools import chain
from logging import getLogger
from uuid import uuid4

# noinspection PyUnresolvedReferences
from zmq import DEALER
# noinspection PyUnresolvedReferences
from zmq import POLLIN
# noinspection PyUnresolvedReferences
from zmq import PUB
# noinspection PyUnresolvedReferences
from zmq import Poller
# noinspection PyUnresolvedReferences
from zmq import RCVTIMEO
# noinspection PyUnresolvedReferences
from zmq import ROUTER
# noinspection PyUnresolvedReferences
from zmq import SNDTIMEO
# noinspection PyUnresolvedReferences
from zmq import SUB
# noinspection PyUnresolvedReferences
from zmq import SUBSCRIBE

from dcache.protocols.api import APIProtocol
from dcache.protocols.publish import PublishProtocol
from dcache.protocols.publish import PublishServer
from .cache import Cache
from .nodes import Nodes
from .nodes import index_for
from .protocols.api import APIServer
from .protocols.request import RequestProtocol
from .protocols.request import RequestServer
from .zmq import ENCODING

_LOG = getLogger(__name__)


class Server(RequestServer, APIServer, PublishServer):
    """A node server.

    Each node has a request URL where request are accepted and a service URL
    where the node periodically publishes its list of known nodes and the time
    of the last contact (last publication from that node on the service URL).

    The request commands are:
    -new: register a new node
    -get: get a key
    -set: set a key

    Each node owns a Nodes object which tells to which node a key belongs and a
    Cache object to store key/value pairs.

    When keys need to be moved (node list did change) then the old and new
    owners are compared for each key and the key gets sent or deleted
    accordingly.
    """

    PUB_INTERVAL = timedelta(seconds=5)

    IO_TIMEOUT = 5 * 1000  # milliseconds

    def __init__(
            self, context, req_address, pub_address, api_address,
            register_address=None):
        """

        :type context: zmq.Context
        :type req_address: str
        :type pub_address: str
        :type api_address: str
        :type register_address: str
        """
        self._last_published = datetime(1970, 1, 1)  # never
        self._register_address = register_address
        self._context = context
        self._req_address = req_address
        self._pub_address = pub_address
        self._api_address = api_address
        self._req_socket = None
        self._pub_socket = None
        self._api_socket = None
        self._sub_socket = None
        self._register_socket = None  # connect to cluster, one time use
        # noinspection SpellCheckingInspection
        self._poller = Poller()
        self._pending_requests = {}  # id: (f(id, error, data), timeout_time)
        self._cache = Cache()
        self._nodes = Nodes(req_address, pub_address)
        # Inject server.
        RequestProtocol.server = self
        APIProtocol.server = self
        PublishProtocol.server = self
        _LOG.info("node node_id: %s", self._nodes.node_id)

    def _prepare(self, socket, register_to_poller=POLLIN):
        """

        :type socket: zmq.Socket
        :type register_to_poller: int
        """
        socket.setsockopt(RCVTIMEO, self.IO_TIMEOUT)
        socket.setsockopt(SNDTIMEO, self.IO_TIMEOUT)
        if register_to_poller is not None:
            self._poller.register(socket, register_to_poller)

    def _create_node(self, req_address, pub_address, last_seen):
        """Create request socket for node, register id and subscribe to the
        publication address.

        :type req_address: str
        :type pub_address: str
        :type last_seen: datetime.datetime
        """
        request_socket = self._context.socket(DEALER)
        self._prepare(request_socket)
        request_socket.connect(req_address)
        self._sub_socket.connect(pub_address)
        _LOG.debug("Subscribed to %s", pub_address)
        # noinspection PyCallByClass
        return Nodes.Node(req_address, pub_address, last_seen, request_socket)

    def _destroy_node(self, node):
        """Clean up.

        Unsubscribe from pub address and remove request socket from poller.

        :type node: Nodes.Node
        :return:
        """
        self._sub_socket.disconnect(node.pub_address)
        _LOG.debug("Unsubscribed from %s", node.pub_address)
        self._poller.unregister(node.req_socket)
        _LOG.debug("Removed node: %s %s", node.req_address, node.pub_address)

    def _handle_connect_response(self, request_id, error, response):
        """

        :type request_id: str
        :type error: RequestProtocol.Error
        :type response: [bytes]
        :rtype: [str]
        """
        # Close register socket, not needed anymore.
        self._poller.unregister(self._register_socket)
        self._register_socket = None
        if error != RequestProtocol.Error.NO_ERROR:
            raise RuntimeError("Could not register node, error: %s", error)
        node_id, req_address, pub_address = (
            part.decode(ENCODING) for part in response)
        now = datetime.utcnow()
        # Register remote node.
        added_nodes = self._nodes.update_nodes(
            {node_id: (req_address, pub_address, now)}, self._create_node)
        if not added_nodes or added_nodes[0] != node_id:
            raise RuntimeError(
                "Could not accept cluster node %s", self._register_address)
        self.redistribute(node_id)
        return [request_id]

    def _connect(self):
        """Register and connect to an existing cluster.

        Use a one time socket to send a CMD_NEW request to the other server.
        Add the other server as a new node, once the response is available.

        """
        assert self._register_socket is None
        if not self._register_address:
            return
        # Create socket.
        _LOG.debug("Registering to %s", self._register_address)
        self._register_socket = self._context.socket(DEALER)
        self._prepare(self._register_socket)  # timeout, poller
        self._register_socket.connect(self._register_address)
        # Make request.
        request_id = uuid4().bytes
        build_request = (
            lambda rid:
                RequestProtocol.build_connect_to_cluster_request(
                    rid, self._nodes.node_id, self._req_address,
                    self._pub_address))
        self.send_requests(
            [request_id], [self._register_socket], build_request,
            self._handle_connect_response)

    def _step_handle_request_answers(self, socket, node_id):
        """

        :type socket: zmq.Socket
        :type node_id: str
        """
        # See if a message on one of the nodes arrived.
        data = socket.recv_multipart()
        request_id, error, data = RequestProtocol.read_answer(data)
        if not request_id:
            _LOG.debug("Ignoring response with no request id.")
            return
        # Handle the answer if possible.
        handler, _ = self._pending_requests.get(request_id, (None, None))
        if handler is None:
            _LOG.debug("Ignoring response %s from %s", request_id, node_id)
            return
        # noinspection PyCallingNonCallable
        completed_requests = handler(request_id, error, data)
        _LOG.debug("Handled response from %s, %s", node_id, completed_requests)
        # Remove pending requests.
        for request_id in completed_requests:
            self._pending_requests.pop(request_id, None)

    def _step_handle_timeouts(self, now):
        """

        :type now: datetime.datetime
        """
        # Handle timeouts on pending requests.
        completed_requests = [
            handler(request_id, None, None)
            for request_id, (handler, timeout) in self._pending_requests.items()
            if timeout < now
        ]
        # Clean up completed_requests timeouts.
        for request_id in chain.from_iterable(completed_requests):
            self._pending_requests.pop(request_id, None)

    def _step_remove_dead_nodes(self):
        removed_nodes = []
        for node_id, node in self._nodes.remove_dead_nodes():
            self._destroy_node(node)
            removed_nodes.append(node_id)
        self.redistribute(*removed_nodes)

    def _step(self, sockets):
        """

        :param sockets: Result from poller, dictionary of socket: event.
        """
        now = datetime.utcnow()
        # Handle API requests.
        if self._api_socket in sockets:
            data = self._api_socket.recv_multipart()
            if not APIProtocol.handler_for(data).handle_request():
                _LOG.error("API request not handled!")
                assert True
        # Incoming requests?
        if self._req_socket in sockets:
            data = self._req_socket.recv_multipart()
            if not RequestProtocol.handler_for(data).handle_request():
                _LOG.error("Request not handled!")
                assert True
        # Handle pending requests.
        if self._register_socket and self._register_socket in sockets:
            self._step_handle_request_answers(self._register_socket, "-")
        for node_id, node in self._nodes.nodes.items():
            if node.req_socket in sockets:
                self._step_handle_request_answers(node.req_socket, node_id)
        # Handle pending request which did not get an answer in time.
        self._step_handle_timeouts(now)
        # Handle incoming node updates (subscriptions).
        if self._sub_socket in sockets:
            PublishProtocol.handle(self._sub_socket.recv_multipart())
        # Remove dead nodes.
        self._step_remove_dead_nodes()
        # Publish nodes?
        if now - self._last_published > self.PUB_INTERVAL:
            self._last_published = now
            self._pub_socket.send_multipart(PublishProtocol.build_publish())

    def loop(self):
        """Event loop, listen to all sockets and handle_request all messages.

        Expected to be called only once!
        """
        assert self._req_address
        assert self._req_socket is None
        assert self._pub_address
        assert self._pub_socket is None
        assert self._api_address
        assert self._api_socket is None
        assert self._sub_socket is None
        # Create publish and request sockets.
        self._pub_socket = self._context.socket(PUB)
        self._prepare(self._pub_socket, register_to_poller=False)
        self._pub_socket.bind(self._pub_address)
        _LOG.debug("Publishing on %s", self._pub_address)
        self._req_socket = self._context.socket(ROUTER)
        self._prepare(self._req_socket)  # timeout, poller
        self._req_socket.bind(self._req_address)
        _LOG.debug("Waiting for requests on %s", self._req_address)
        # API socket.
        self._api_socket = self._context.socket(ROUTER)
        self._prepare(self._api_socket)  # timeout, poller
        self._api_socket.bind(self._api_address)
        _LOG.debug("Waiting for API requests on %s", self._api_address)
        # Subscriber.
        self._sub_socket = self._context.socket(SUB)
        self._sub_socket.setsockopt(SUBSCRIBE, PublishProtocol.TOPIC)
        self._poller.register(self._sub_socket, POLLIN)
        # Send connect to cluster request?
        self._connect()
        # Enter ZMQ loop.
        timeout = self.PUB_INTERVAL.seconds * 1000
        stop = False
        while not stop:
            try:
                # Wait for messages, but not too long!
                sockets = dict(self._poller.poll(timeout))
            except KeyboardInterrupt:
                stop = True
            else:
                self._step(sockets)

    # === RequestServer begin, shared functions

    def send_multipart(self, data):
        return self._req_socket.send_multipart(data)

    def get_cache_value(self, key):
        return self._cache.get(key)

    def set_cache_value(self, key, value, timestamp, key_index=None):
        key_index = index_for(key) if key_index is None else key_index
        return self._cache.set(key, value, timestamp, key_index)

    def does_node_id_exist(self, node_id):
        if node_id == self._nodes.node_id:
            return True
        return node_id in self._nodes.nodes

    def add_node(self, node_id, request_address, publish_address):
        now = datetime.utcnow()
        added_nodes = self._nodes.update_nodes(
            {node_id: (request_address, publish_address, now)},
            self._create_node)
        return added_nodes and added_nodes[0] == node_id

    def get_other_node_ids(self):
        return set(self._nodes.nodes.keys())

    def send_multipart_to(self, node_id, data):
        node = self._nodes.nodes.get(node_id)
        if node:
            return node.req_socket.send_multipart(data)
        return None

    def redistribute(self, *node_ids):
        if not node_ids:
            return
        keys = ((key, entry.hash_index) for key, entry in self._cache.items())
        send_list, keep = self._nodes.redistribute(keys, *node_ids)
        for key, node_ids in send_list.items():
            if node_ids:
                entry = self._cache.get(key)
                if entry is not None:
                    last_update, value = entry
                    # Request id gets ignored for the purpose of handling the
                    # responses. Reuse the same for all requests.
                    data = RequestProtocol.build_set_request(
                        uuid4().bytes, key, value, last_update)
                    for node_id in node_ids:
                        socket = self._nodes.nodes[node_id].req_socket
                        socket.send_multipart(data)
                        _LOG.debug("Redistributing '%s' to %s", key, node_id)
        for key, should_keep in keep.items():
            if not should_keep:
                self._cache.set(key, None, None, None)
                _LOG.debug("Removed key '%s', no longer needed.")

    def get_server_node_info(self):
        return self._nodes.node_id, self._req_address, self._pub_address

    # === RequestServer end

    # === APIServer begin, additional functions

    def send_api_multipart(self, data):
        return self._api_socket.send_multipart(data)

    def get_server_node_id(self):
        return self._nodes.node_id

    def send_requests(
            self, request_ids, sockets, build_request, handle_response):
        if not request_ids:
            return
        # Send request and register ids as pending.
        now = datetime.utcnow()
        timeout = now + timedelta(milliseconds=Server.IO_TIMEOUT)
        for request_id, request_socket in zip(request_ids, sockets):
            data = build_request(request_id)
            request_socket.send_multipart(data)
            self._pending_requests[request_id] = (handle_response, timeout)
            _LOG.debug("Added pending request %s", request_id)

    def get_nodes_for_index(self, key_index):
        return self._nodes.get_nodes_for_index(key_index)

    def get_distribution_circles(self):
        # noinspection PyProtectedMember
        return self._nodes._distribution_circles

    def get_cached_keys_count(self):
        # noinspection PyProtectedMember
        return len(self._cache._cache)

    def get_cache_space_usage(self):
        return self._cache.space_usage

    # === APIServer end

    # === PublishServer begin

    def update_nodes(self, nodes):
        return self._nodes.update_nodes(nodes, self._create_node)

    def get_other_nodes(self):
        return self._nodes.nodes

    # === PublishServer end
