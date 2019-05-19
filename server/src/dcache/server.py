from datetime import (
    datetime,
    timedelta,
)
from itertools import chain
from logging import getLogger
from uuid import uuid4

# noinspection PyUnresolvedReferences
from zmq import (
    Poller,
    ROUTER,
    DEALER,
    REP,
    REQ,
    PUB,
    SUB,
    PULL,
    POLLIN,
    SUBSCRIBE,
    RCVTIMEO,
    SNDTIMEO,
)

from .cache import Cache
from .nodes import Nodes
from .nodes import index_for
from .request_protocol import RequestProtocol
from .request_protocol import RequestServer
from .zmq import ENCODING
from .zmq import datetime_to_str
from .zmq import split_header
from .zmq import str_to_datetime

_LOG = getLogger(__name__)


class Server(RequestServer):
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

    ENCODING = "utf-8"

    PUB_INTERVAL = timedelta(seconds=5)

    IO_TIMEOUT = 5 * 1000  # milliseconds

    API_VERSION = "1"

    API_GET = "get"

    API_SET = "set"

    NODE_TOPIC = "n"

    # noinspection SpellCheckingInspection
    @classmethod
    def recv_multipart_str(cls, zmq_socket, *args, **kwargs):
        """Wrapper for recv_multipart.

        :type zmq_socket: zmq.Socket
        :return: Header and data. Header is a list of bytes, data a list of
            strings.
        :rtype: (list, list)
        """
        parts = zmq_socket.recv_multipart(*args, **kwargs)
        header, data = split_header(parts)
        return header, [part.decode(cls.ENCODING) for part in data]

    @classmethod
    def send_multipart_str(cls, zmq_socket, header, parts, *args, **kwargs):
        """Wrapper for send_multipart.

        :type zmq_socket: zmq.Socket
        :param header: List of bytes (frames) to prepend to parts.
        :type header: list
        :param parts: List of strings (frames) to send.
        :type parts: iterable
        :return: Result from send_multipart call.
        """
        parts_bytes = [part.encode(cls.ENCODING) for part in parts]
        if header:
            parts_bytes = header + parts_bytes
        return zmq_socket.send_multipart(parts_bytes, *args, **kwargs)

    def __init__(self, context, req_address, pub_address, api_address):
        """

        :param context: zmq.Context
        :param req_address:  str
        :param pub_address:  str
        """
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
        self._pending_requests = {}  # id: (function(id, data), timeout_time)
        self._cache = Cache()
        self._nodes = Nodes(req_address, pub_address)
        RequestProtocol.server = self
        _LOG.info("node node_id: %s", self._nodes.node_id)

    # === RequestServer begin

    def send_multipart(self, data):
        return self._req_socket.send_multipart(data)

    def get_cache_value(self, key):
        return self._cache.get(key)

    def set_cache_value(self, key, value, timestamp):
        return self._cache.set(key, value, timestamp, index_for(key))

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
            return node.node.req_socket.send_multipart(data)
        return None

    def redistribute(self, node_id):
        self._redistribute(node_id)

    def get_server_node_info(self):
        return self._nodes.node_id, self._req_address, self._pub_address

    # === RequestServer end

    def _send_requests(
            self, request_ids, sockets, build_request, handle_response):
        """Using the ids send requests to nodes and register them as pending.

        The error and remaining response frames will be passed to the
        handle_request function. If a timeout occurs instead, None will be
        passed in as the error.

        :param request_ids: List of strings.
        :type request_ids: iterable
        :param sockets: Where to send the request to. zmq.Socket objects.
        :type sockets: iterable
        :param build_request: Function to construct the request frames
        :type build_request: function(request_id, *data) -> [bytes]
        :param handle_response: Function for handling any response. The first
            argument for the function is the request id which was used to make
            the request, followed by the error. The last argument are the the
            remaining response frames. The error is None in case a timeout
            occurred.
        :type handle_response: function(bytes, RequestProtocol.Error, [bytes])
        :return:
        """
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

    def _create_node(self, req_address, pub_address, last_seen):
        """Create request socket for node, register id and subscribe to the
        publication address.

        :type req_address: str
        :type pub_address: str
        :type last_seen: datetime.datetime
        """
        request_socket = self._context.socket(DEALER)
        request_socket.setsockopt(RCVTIMEO, self.IO_TIMEOUT)
        request_socket.setsockopt(SNDTIMEO, self.IO_TIMEOUT)
        request_socket.connect(req_address)
        self._sub_socket.connect(pub_address)
        _LOG.debug("Subscribed to %s", pub_address)
        self._poller.register(request_socket, POLLIN)
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

    def _publish(self):
        """Publish all known nodes, including the local node.
        """
        nodes = [(Server.NODE_TOPIC, )]  # topic frame
        nodes_list = [
            (node_id, node.req_address, node.pub_address,
                datetime_to_str(node.last_seen))
            for node_id, node in self._nodes.nodes.items()
        ]
        nodes.extend(nodes_list)
        # Add local node.
        now = datetime.utcnow()
        now_str = datetime_to_str(now)
        nodes.append(
            (self._nodes.node_id, self._req_address, self._pub_address,
                now_str))
        # _LOG.debug("Publishing %s: %s nodes.", self._pub_address, nodes)
        Server.send_multipart_str(
            self._pub_socket, [], chain.from_iterable(nodes))

    def _handle_publication(self, data):
        """Update the nodes information with the new list.

        Redistribute keys, if necessary.

        :param data:
        :return:
        """
        nodes = {}
        assert data[0] == Server.NODE_TOPIC
        data = data[1:]
        while data:
            node_id, req_address, pub_address, last_seen = data[:4]
            last_seen_date = str_to_datetime(last_seen)
            nodes[node_id] = (req_address, pub_address, last_seen_date)
            data = data[4:]
        # _LOG.debug("Got node list: %s", nodes)
        added_nodes = self._nodes.update_nodes(nodes, self._create_node)
        if added_nodes:
            _LOG.debug("%s nodes where added.", len(added_nodes))
            self._redistribute(*added_nodes)

    def _redistribute(self, *node_ids):
        """A node was added or removed, redistribute the affected keys.

        :param node_ids: List of node id strings that where added or removed.
        :type node_ids: str
        """
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

    def _handle_api_get(self, header, key):
        """Handle an API get request.

        Always return the first response.
        """
        _, value = self._cache.get(key)
        contact_others = False
        nodes = self._nodes.get_nodes_for(key)
        if value is None:
            # Check if we should have it:
            contact_others = self._nodes.node_id not in nodes
        if not contact_others:
            Server.send_multipart_str(self._api_socket, header, [value or ""])
        else:
            request_ids = [uuid4().bytes for _ in nodes]

            # Generate functions to handle_request the response.
            # noinspection PyUnusedLocal,PyShadowingNames,PyDefaultArgument
            def handle_response(
                    request_id, error, data, header=header,
                    request_ids=request_ids):
                """Handle get response, send the value back and cancel all
                pending requests.

                Send back "" if a timeout occurred.

                :type request_id: bytes
                :type error: RequestProtocol.Error or None
                :param data: List of bytes, the result.
                :type data: list
                :type header: list
                :param request_ids: List of all requests done for this api get
                    request.
                :return: All request ids that are now resolved.
                """
                if error != RequestProtocol.Error.NO_ERROR:
                    data = ""
                else:
                    data = data[0].decode(ENCODING)  # data[1] = timestamp
                Server.send_multipart_str(self._api_socket, header, [data])
                return request_ids

            build_request = (
                lambda rid: RequestProtocol.build_get_request(rid, key))
            sockets = (node.req_socket for node in nodes.values())
            self._send_requests(
                request_ids, sockets, build_request, handle_response)

    def _handle_api_set(self, header, key, value):
        """Handle an API set request.

        Set the value on all nodes and return "0" if all nodes report "0" else
        return "-1". If a timeout happened return "-2".

        :param header: List of bytes, header for sending back the response.
        :type key: str
        :type value: str
        """
        key_index = index_for(key)
        server_node_id = self._nodes.node_id  # shortcut
        nodes = self._nodes.get_nodes_for_index(key_index)
        other_node_ids = [
            node_id for node_id in nodes if node_id != server_node_id]
        # Prepare requests ids for other nodes.
        request_ids = [uuid4().bytes for _ in other_node_ids]

        # Generate functions to handle_request the responses from other nodes.
        # Answer to client once all nodes reported back or a timeout happens.
        # noinspection PyShadowingNames,PyDefaultArgument
        def handle_response(
                request_id, error, data, header=header, request_ids=request_ids,
                responses=[], responses_count=len(nodes)):
            """Keep track of what nodes respond.

            Once all results are in or a timeout happened send the right result
            back to the api set request.

            :type request_id: bytes
            :type error: RequestProtocol.Error or None
            :param data: List of strings, the result.
            :type data: list
            :type header: list
            :param request_ids: List of all requests done for this api get
                request.
            :param responses: List for keeping track of what nodes responded so
                far.
            :param responses_count:
            :return: All request ids that are now resolved.
            """
            # noinspection PyPep8Naming
            Errors = Server.Errors  # shortcut
            _LOG.debug(
                "Set %s %s %s %s, %s", responses_count, responses, request_ids,
                error, data)
            result = None
            if error is None:
                result = Errors.TIMEOUT
            else:
                responses.append(error)
                if len(responses) == responses_count:
                    # Done, this was the last node responding. Send result.
                    no_error = all(
                        r == RequestProtocol.Error.NO_ERROR for r in responses)
                    result = Errors.NO_ERROR if no_error else Errors.TOO_BIG
            if result is not None:
                Server.send_multipart_str(
                    self._api_socket, header, [result.value])
                _LOG.debug("Set response: %s", result)
            # On timeout cancel all pending request, otherwise only the current
            # one.
            return request_ids if error is None else [request_id]

        now = datetime.utcnow()
        if request_ids:
            build_request = (
                lambda rid:
                    RequestProtocol.build_set_request(rid, key, value, now))
            sockets = (
                node.req_socket
                for node_id, node in nodes.items() if node_id != server_node_id)
            _LOG.debug("Set key %s on nodes %s.", key, other_node_ids)
            self._send_requests(
                request_ids, sockets, build_request, handle_response)
        if self._nodes.node_id in nodes:
            error = self._cache.set(key, value, now, key_index)
            # noinspection PyPep8Naming
            TOO_BIG = Cache.Error.TOO_BIG  # shortcut
            # noinspection PyPep8Naming
            Error = RequestProtocol.Error  # shortcut
            error = Error.TOO_BIG if error == TOO_BIG else Error.NO_ERROR
            handle_response(b"", error, [])

    def _step_handle_request_answers(self, socket, node_id):
        # See if a message on one of the nodes arrived.
        completed_requests = []
        data = socket.recv_multipart()
        request_id, error, data = RequestProtocol.read_answer(data)
        if request_id:
            handler = self._pending_requests.get(request_id)
            if handler:
                handled_ids = handler[0](request_id, error, data)
                completed_requests.extend(handled_ids)
                _LOG.debug(
                    "Handled response from %s, completed %s", node_id,
                    handled_ids)
            else:
                _LOG.debug("Ignoring response %s from %s", request_id, node_id)
        else:
            _LOG.debug("Ignoring response with no request id.")
        # Clean up pending requests.
        for request_id in completed_requests:
            self._pending_requests.pop(request_id, None)

    def _step_handle_timeouts(self, now):
        # Handle timeouts on pending requests.
        completed_requests = []
        for request_id, (handler, timeout) in self._pending_requests.items():
            if timeout < now:  # deadline passed
                _LOG.debug(
                    "Timeout %s: %.3f", request_id,
                    now.timestamp() - timeout.timestamp())
                handled_ids = handler(request_id, None, None)
                completed_requests.extend(handled_ids)
        # Clean up completed_requests timeouts.
        for request_id in completed_requests:
            self._pending_requests.pop(request_id, None)

    def _step_remove_dead_nodes(self):
        removed_nodes = []
        for node_id, node in self._nodes.remove_dead_nodes():
            self._destroy_node(node)
            removed_nodes.append(node_id)
        self._redistribute(*removed_nodes)

    def _step(self, last_published):
        stop = False
        try:
            # Wait for messages, but not too long!
            sockets = dict(
                self._poller.poll(self.PUB_INTERVAL.seconds * 1000))
        except KeyboardInterrupt:
            stop = True
        else:
            now = datetime.utcnow()
            # Handle API requests.
            if self._api_socket in sockets:
                header, data = Server.recv_multipart_str(self._api_socket)
                action = data[0]
                data = data[1:]
                if action == Server.API_GET:
                    self._handle_api_get(header, *data)
                elif action == Server.API_SET:
                    self._handle_api_set(header, *data)
                elif action == "status":
                    cache_items = (
                        f"{k}={v.value}" for k, v in self._cache.items())
                    # noinspection PyProtectedMember
                    data = [
                        f"{self._nodes.node_id}",
                        f"{','.join(self._nodes.nodes.keys())}",
                        f"{self._nodes._distribution_circles[0]}",
                        f"{len(self._cache)}",
                        f"{self._cache.space_usage}",
                        f"{','.join(cache_items)}",
                    ]
                    Server.send_multipart_str(self._api_socket, header, data)
                else:
                    _LOG.debug("Unknown API request %s.", action)
            # Incoming requests?
            if self._req_socket in sockets:
                data = self._req_socket.recv_multipart()
                handler = RequestProtocol.handler_for(data)
                handled = handler.handle_request()
                if not handled:
                    _LOG.error("Request not handled!")
                    assert True
            # Handle pending requests.
            if self._register_socket and self._register_socket in sockets:
                self._step_handle_request_answers(self._register_socket, None)
            for node_id, node in self._nodes.nodes.items():
                if node.req_socket in sockets:
                    self._step_handle_request_answers(node.req_socket, node_id)
            # Handle pending request which did not get an answer in time.
            self._step_handle_timeouts(now)
            # Handle incoming node updates (subscriptions).
            if self._sub_socket in sockets:
                _, data = Server.recv_multipart_str(self._sub_socket)
                self._handle_publication(data)
            # Remove dead nodes.
            self._step_remove_dead_nodes()
            # Publish nodes?
            if (last_published is None or
                    now - last_published > self.PUB_INTERVAL):
                last_published = now
                self._publish()
        return stop, last_published

    def _register(self, register_address):
        """Register and connect to an existing cluster.

        Use a one time socket to send a CMD_NEW request to the other server.
        Add the other server as a new node, once the response is available.

        :param register_address:
        :return:
        """
        assert register_address
        assert self._register_socket is None
        # Create socket.
        _LOG.debug("Registering to %s", register_address)
        self._register_socket = self._context.socket(DEALER)
        self._register_socket.setsockopt(RCVTIMEO, self.IO_TIMEOUT)
        self._register_socket.setsockopt(SNDTIMEO, self.IO_TIMEOUT)
        self._register_socket.connect(register_address)
        self._poller.register(self._register_socket, POLLIN)
        # Prepare request.
        request_id = uuid4()

        # noinspection PyUnusedLocal
        def handle_response(response_request_id, error, response):
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
                    "Could not accept cluster node %s", register_address)
            self._redistribute(node_id)
            return [response_request_id]

        # Make request.
        build_request = (
            lambda rid:
                RequestProtocol.build_connect_to_cluster_request(
                    rid, self._nodes.node_id, self._req_address,
                    self._pub_address))
        self._send_requests(
            [request_id.bytes], [self._register_socket], build_request,
            handle_response)

    def loop(self, register_address):
        """Event loop, listen to all sockets and handle_request all messages.

        If a register_address of an existing node is given the this node will
        register itself there, before entering the loop.
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
        self._pub_socket.setsockopt(RCVTIMEO, self.IO_TIMEOUT)
        self._pub_socket.setsockopt(SNDTIMEO, self.IO_TIMEOUT)
        _LOG.debug("Publishing on %s", self._pub_address)
        self._pub_socket.bind(self._pub_address)
        self._req_socket = self._context.socket(ROUTER)
        self._req_socket.setsockopt(RCVTIMEO, self.IO_TIMEOUT)
        self._req_socket.setsockopt(SNDTIMEO, self.IO_TIMEOUT)
        _LOG.debug("Waiting for requests on %s", self._req_address)
        self._req_socket.bind(self._req_address)
        self._poller.register(self._req_socket, POLLIN)
        # API socket.
        self._api_socket = self._context.socket(ROUTER)
        self._api_socket.setsockopt(RCVTIMEO, self.IO_TIMEOUT)
        self._api_socket.setsockopt(SNDTIMEO, self.IO_TIMEOUT)
        _LOG.debug("Waiting for API requests on %s", self._api_address)
        self._api_socket.bind(self._api_address)
        self._poller.register(self._api_socket, POLLIN)
        # Subscriber.
        self._sub_socket = self._context.socket(SUB)
        self._sub_socket.setsockopt(
            SUBSCRIBE, Server.NODE_TOPIC.encode(Server.ENCODING))
        self._poller.register(self._sub_socket, POLLIN)
        # Connect to cluster?
        if register_address:
            self._register(register_address)
        # Enter ZMQ loop.
        stop = False
        last_published = None
        while not stop:
            stop, last_published = self._step(last_published)
