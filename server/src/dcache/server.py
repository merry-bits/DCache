# -*- coding: utf-8 -*-
from collections import defaultdict
from datetime import datetime, timedelta
from logging import getLogger
from random import choice
from threading import Thread
from uuid import uuid1
from wsgiref import simple_server

from zmq import Poller
from zmq import REP, REQ, PUB, SUB, PULL, POLLIN  # @UnresolvedImport
from zmq import SUBSCRIBE, RCVTIMEO, SNDTIMEO  # @UnresolvedImport

from .api import app, set_config, SUB_ENDPOINT, PUSH_ENDPOINT
from .cache import Cache, key_index
from .nodes import Nodes


_LOG = getLogger(__name__)


def _dt_to_str(dt):
    return "{}:{}:{}:{}:{}:{}".format(
        dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)


def _str_to_dt(s):
    return datetime(*map(int, s.split(":")))


class Server():
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

    PUB_INTERVALL = timedelta(seconds=5)

    IO_TIMEOUT = 5 * 1000  # milliseconds

    CMD_NEW = "new"

    CMD_GET = "get"

    CMD_SET = "set"

    def __init__(self, context, req_address, pub_address):
        self._context = context
        self._poller = Poller()
        self._nodes_sockets = {}  # requests and service sockets
        # FIFO request queue for nodes: [in_progress, data, callback(response)]
        self._nodes_requests = defaultdict(list)  # requests queues for nodes
        self._cache = Cache()  # the cache
        node_id = uuid1().hex  # unique node id, based on current time
        _LOG.info("node id: %s", node_id)
        self._nodes = Nodes(node_id, req_address, pub_address)  # other nodes

    def _open_req_socket(self, addr):
        req_socket = self._context.socket(REQ)
        req_socket.setsockopt(RCVTIMEO, self.IO_TIMEOUT)
        req_socket.connect(addr)
        self._poller.register(req_socket, POLLIN)
        return req_socket

    def _open_sub_socket(self, addr):
        sub_socket = self._context.socket(SUB)
        sub_socket.setsockopt_string(SUBSCRIBE, "")
        sub_socket.connect(addr)
        self._poller.register(sub_socket, POLLIN)
        return sub_socket

    def _add_request(self, node_id, data, callback):
        """Adds a new request for a particular node.

        Each request in the queue consists of:
        - in progress flag
        - data to be send
        - function (with one argument, the response) to be called when the
            response is ready
        """
        self._nodes_requests[node_id].append([False, data, callback])

    def _hanlde_subscriptions(self, poll_sockets):
        """Read the nodes list from each other node and merge the information
        into the local node list.

        :return: true if nodes where added
        """
        added = []
        for node_id, (_, pub_socket) in self._nodes_sockets.items():
            if pub_socket in poll_sockets:
                node_id, nodes = pub_socket.recv_json()
                # Convert string to datetime.
                nodes = {
                    i: (req_addr, pub_addr, _str_to_dt(last))
                    for i, (req_addr, pub_addr, last) in nodes.items()}
                # Merge nodes list.
                new_nodes = self._nodes.update_nodes(nodes)
                # Create sockets for new nodes.
                for node_id, req_addr, pub_addr in new_nodes:
                    _LOG.debug("adding node: %s", node_id)
                    req_socket = self._open_req_socket(req_addr)
                    sub_socket = self._open_sub_socket(pub_addr)
                    added.append((node_id, (req_socket, sub_socket)))
        self._nodes_sockets.update(dict(added))  # save sockets
        return len(added) > 0  # nodes list got changed?

    def _add_node(self, node_id, req_addr, pub_addr, req_socket=None):
        """Add one node to the list and create the sockets for the request
        and service URLs (subscribe to the node).
        """
        _LOG.debug("adding node: %s", node_id)
        # Add node to the nodes list.
        self._nodes.add_node(node_id, req_addr, pub_addr)
        if req_socket is None:  # does that not yet exist?
            req_socket = self._open_req_socket(req_addr)
        sub_socket = self._open_sub_socket(pub_addr)
        # Remember sockets.
        self._nodes_sockets[node_id] = (req_socket, sub_socket)

    def _remove_nodes(self, removed_nodes):
        """Unregister and remove a node, remove all pending request, too.
        """
        for node_id in removed_nodes:
            _LOG.debug("removing node: %s", node_id)
            # Unregister sockets from poller.
            req_socket, pub_socket = self._nodes_sockets.get(
                node_id, (None, None))
            if req_socket is not None:
                self._poller.unregister(req_socket)
            if pub_socket is not None:
                self._poller.unregister(pub_socket)
            # Forget the sockets and erase request queue.
            self._nodes_sockets.pop(node_id, None)
            self._nodes_requests.pop(node_id, None)
        return len(removed_nodes) > 0  # nodes list got changed?

    def _handle_responses(self, poll_sockets):
        """When a request from the queue gets an answer call the function for
        that request, if possible.
        """
        for node_id, (req_socket, _) in self._nodes_sockets.items():
            # Some node did replay?
            if req_socket in poll_sockets:
                # Get the answer and call the callback function with it, if
                # there was one.
                response = req_socket.recv_json()
                requests = self._nodes_requests[node_id]
                if requests:
                    in_progress, _, callback = requests[0]
                    if in_progress:
                        callback(response)
                        # Request done, remove it from the queue.
                        self._nodes_requests[node_id] = requests[1:]

    def _handle_request(self, node_changes, action, data):
        # Handle the requested action.
        _LOG.debug("Request %s: %s", action, str(data))
        if action == self.CMD_NEW:  # new node, register it
            node_id, req_addr, pub_addr = data
            self._add_node(node_id, req_addr, pub_addr)
            node_changes[0] = True  # a node was added!
            return (self._nodes.id, self._nodes.pub_address)
        if action == self.CMD_SET:  # set a key and value
            key, timestamp, value = data
            timestamp = _str_to_dt(timestamp)
            return self._cache.set(
                key, value, timestamp,
                self._nodes.get_nodes_for_index(key_index(key)))
        if action == self.CMD_GET:  # return value for a key
            in_cache, timestamp, value = self._cache.get(data)
            return (in_cache, _dt_to_str(timestamp), value)

    def _rebalance(self):
        """ Check if key should be on some other nodes now, move/delete as
        necessary.
        """
        send_entries = defaultdict(list)
        remove_entries = []
        # Loop through all keys and compare on which nodes they should be.
        for key, ts, nodes, value, index in self._cache.key_indices:
            new_nodes = self._nodes.get_nodes_for_index(index)
            if new_nodes != nodes:  # node list for that key changed?
                if self._nodes.id not in new_nodes:
                    remove_entries.append(key)  # no longer local
                for new_node in new_nodes - nodes:  # where should they be?
                    send_entries[new_node].append((key, ts, value))
                self._cache.set(key, value, ts, new_nodes)  # save new nodelist
        # Queue requests to set the keys on the new nodes.
        moves = 0
        for new_node, entries in send_entries.items():
            if new_node != self._nodes.id:
                for key, ts, value in entries:
                    self._add_request(
                        new_node, (self.CMD_SET, (key, _dt_to_str(ts), value)),
                        lambda response: None)
                    moves += 1
        # Remove dead nodes from cache.
        for key in remove_entries:
            self._cache.delete_key(key)
        _LOG.debug(
            "adjusted distribution, %d moves, %d deletes", moves,
            len(remove_entries))

    def _handle_api_get(self, api_pub_socket, req_id, key):
        """Handle a API get request from the local API.

        If the key is not local, send a request to one of the responsible
        nodes. Upon arrival of the response publish the response for the API,
        using the request id, which only the right listens for.
        """
        req_id = req_id.encode("utf-8")

        # Publish the result.
        def send_response(resp, req_id=req_id, api_pub_socket=api_pub_socket):
            found, _, value = resp
            found = b"1" if found else b"0"
            value = value.encode("utf-8") if value else b""
            api_pub_socket.send_multipart([req_id, found, value])

        # Where is the key stored?
        nodes = self._nodes.get_nodes_for_index(key_index(key))
        if nodes:
            if self._nodes.id in nodes:  # local answer directly
                in_cache, _, value = self._cache.get(key)
                send_response((in_cache, None, value))
            else:  # remote, make request
                node_id = choice(list(nodes))
                self._add_request(node_id, (self.CMD_GET, key), send_response)
        else:  # can not be stored in cache, for whatever strange reason
            send_response((False, None, None))

    def _handle_api_set(self, api_pub_socket, req_id, key, value):
        """Handle a API set request from the local API.

        Check where the key should be stored and make the appropriate requests.
        The publish all results to the API, alltough the API only cares about
        the fastest response.
        """
        req_id = req_id.encode("utf-8")

        def send_response(resp, req_id=req_id, api_pub_socket=api_pub_socket):
            resp = str(resp).encode("utf-8")  # "0", "-1", ...
            api_pub_socket.send_multipart([req_id, resp])

        # Where should the key go?
        nodes = self._nodes.get_nodes_for_index(key_index(key))
        if nodes:
            timestamp = datetime.now()
            if self._nodes.id in nodes:  # save locally and publish response
                resp = self._cache.set(key, value, timestamp, nodes)
                send_response(resp)
                nodes.remove(self._nodes.id)  # done with local node!
            for node_id in nodes:  # make requests to all remote nodes
                self._add_request(
                    node_id,
                    (self.CMD_SET, (key, _dt_to_str(timestamp), value)),
                    send_response)
        else:
            send_response(-2)  # no nodes

    def loop(self, api_port, req_addr):
        """Event loop, listen to all sockets and handle all messages.

        If a req_address of an existing node is given the this node will
        register itself there, before entering the loop.

        The request on the api_port are handled by a Python WSGI instance
        running the Flask API app.
        """
        # Register to existing node?
        if req_addr is not None:
            _LOG.debug("Contacting %s", req_addr)
            req_socket = self._open_req_socket(req_addr)
            req_socket.send_json(
                (self.CMD_NEW,
                    (self._nodes.id, self._nodes.req_address,
                        self._nodes.pub_address)))
            node_id, pub_addr = req_socket.recv_json()
            _LOG.debug("Received: %s %s", str(node_id), pub_addr)
            self._add_node(node_id, req_addr, pub_addr, req_socket)
            self._rebalance()

        # Create request and service sockets.
        nodes_publisher = self._context.socket(PUB)
        nodes_publisher.setsockopt(RCVTIMEO, self.IO_TIMEOUT)
        nodes_publisher.setsockopt(SNDTIMEO, self.IO_TIMEOUT)
        _LOG.debug("Publishing on %s", self._nodes.pub_address)
        nodes_publisher.bind(self._nodes.pub_address)
        req_socket = self._context.socket(REP)
        req_socket.setsockopt(RCVTIMEO, self.IO_TIMEOUT)
        req_socket.setsockopt(SNDTIMEO, self.IO_TIMEOUT)
        _LOG.debug("waiting for requests on %s", self._nodes.req_address)
        req_socket.bind(self._nodes.req_address)
        self._poller.register(req_socket, POLLIN)

        # Create in-process sockets to the API app.
        api_pull_socket = self._context.socket(PULL)
        api_pull_socket.bind(PUSH_ENDPOINT)
        self._poller.register(api_pull_socket, POLLIN)
        api_pub_socket = self._context.socket(PUB)
        api_pub_socket.bind(SUB_ENDPOINT)

        _LOG.info("Entering server loop")
        # Start web server.
        set_config(self._context)
        httpd = simple_server.make_server(
            '0.0.0.0', int(api_port), app)
        Thread(target=httpd.serve_forever).start()
        # Enter ZMQ loop.
        stop = False
        last_published = None
        try:
            while not stop:
                try:
                    # Wait for messages, but not too long!
                    sockets = dict(
                        self._poller.poll(self.PUB_INTERVALL.seconds * 1000))
                except KeyboardInterrupt:
                    stop = True
                else:
                    changes = [False]
                    # Handle incoming node updates (subscriptions).
                    if self._hanlde_subscriptions(sockets):
                        changes[0] = True
                    # Handle incoming responses.
                    self._handle_responses(sockets)
                    # Incoming requests?
                    if req_socket in sockets:
                        req_socket.send_json(
                            self._handle_request(
                                changes, *req_socket.recv_json()))
                    # Remove dead nodes?
                    if self._remove_nodes(self._nodes.remove_dead_nodes()):
                        changes[0] = True
                    # Did nodes change?
                    if changes[0]:
                        self._rebalance()
                    # Request something?
                    for node_id, requests in self._nodes_requests.items():
                        if requests and not requests[0][0]:
                            request = requests[0]
                            request[0] = True  # in progress
                            self._nodes_sockets[node_id][0].send_json(
                                request[1])
                    # Publish something?
                    now = datetime.now()
                    if (last_published is None or
                            now - last_published > self.PUB_INTERVALL):
                        # Get nodes, convert last datetime to string
                        nodes = {
                            i: (req_addr, pub_addr, _dt_to_str(last))
                            for i, (req_addr, pub_addr, last)
                            in self._nodes.nodes.items()}
                        _LOG.debug(
                            "publishing:\n%s", "\n".join(
                                "{}: {}".format(i, str(n))
                                for i, n in nodes.items()))
                        nodes_publisher.send_json((self._nodes.id, nodes))
                        last_published = now
                    # Handle API get requests
                    if api_pull_socket in sockets:
                        req_id, action, key, value = (
                            api_pull_socket.recv_json())
                        if action == "get":
                            self._handle_api_get(api_pub_socket, req_id, key)
                        if action == "set":
                            self._handle_api_set(
                                api_pub_socket, req_id, key, value)
        finally:
            httpd.shutdown()
