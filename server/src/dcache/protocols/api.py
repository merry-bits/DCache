from datetime import datetime
from enum import Enum
from enum import unique
from logging import getLogger
from uuid import uuid4

from dcache.cache import Cache
from dcache.nodes import index_for
from dcache.zmq import ENCODING
from dcache.zmq import split_header
from .request import RequestProtocol

_LOG = getLogger(__name__)


class APIServer:

    def send_api_multipart(self, data):
        """Send byte frames, using the servers api socket.

        :type data: [bytes]
        """
        raise NotImplementedError()

    def get_cache_value(self, key):
        """

        :type key: str
        :return: Timestamp of las modify and value
        :rtype: (datetime.datetime, str)
        """
        raise NotImplementedError()

    def set_cache_value(self, key, value, timestamp, key_index):
        """

        :type key: str
        :type value: str
        :type timestamp: datetime.datetime
        :type key_index: float
        :rtype: dcache.cache.Cache.Error
        """
        raise NotImplementedError()

    def get_server_node_id(self):
        """

        :rtype: str
        """
        raise NotImplementedError()

    def send_requests(
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
        raise NotImplementedError()

    def get_nodes_for_index(self, key_index):
        """Check to which node the key belongs, for each redundancy.

        :type key_index: float
        :return: Dictionary with node ids and their Nodes.Node objects.
        :rtype: dict
        """
        raise NotImplementedError()

    def get_other_node_ids(self):
        """Ids of all other node ids, not including the servers node.

        :rtype: iterable
        """
        raise NotImplementedError()

    def get_distribution_circles(self):
        """

        :rtype: list
        """
        raise NotImplementedError()

    def get_cached_keys_count(self):
        """

        :rtype: int
        """
        raise NotImplementedError()

    def get_cache_space_usage(self):
        """

        :rtype: float
        """
        raise NotImplementedError()


class APIProtocol:

    _VERSION = b"1"

    @unique
    class _Type(Enum):

        GET = b"get"

        SET = b"set"

        STATUS = b"status"

    @unique
    class Error(Enum):

        NO_ERROR = b"0"

        TOO_BIG = b"1"

        TIMEOUT = b"2"

        UNKNOWN_REQUEST = b"998"

        VERSION_NOT_SUPPORTED = b"999"

    _handlers = {  # _Type: APIProtocol
    }

    @staticmethod
    def handler_for(data):
        """

        :type data: [bytes]
        :rtype: APIProtocol
        """
        if not data:
            return APIProtocol(None, None, None)
        # Read headers (includes empty frame).
        headers, data = split_header(data)
        # Read the version.
        if not data:
            return APIProtocol(None, None, None)
        version = data[0]
        data = data[1:]
        # Read the type.
        if not data:
            return APIProtocol(headers, None, None)
        try:
            request_type = APIProtocol._Type(data[0])
        except ValueError:
            return APIProtocol(headers, None, None)
        data = data[1:]
        command = APIProtocol._handlers.get(request_type)
        if command is not None:
            return command(headers, version, data)
        return APIProtocol(headers, None, None)

    def __init__(self, headers, version, data):
        self._headers = headers
        self._version = version
        self._data = data

    server: APIServer = None

    def _send(self, error=None, response=None):
        """

        :type error: RequestProtocol.Error
        :type response: [bytes]
        """
        assert APIProtocol.server is not None
        server = APIProtocol.server  # shortcut
        error = APIProtocol.Error.NO_ERROR if error is None else error
        response = [] if response is None else response
        data = [
            *self._headers,
            error.value,
            *response
        ]
        return server.send_api_multipart(data)

    def handle_request(self):
        """

        :return: True if request is dealt with.
        """
        if self._headers is None:
            _LOG.warning("Handled request without headers. No response send.")
            return True  # done nothing to send
        if self._version is None:
            _LOG.info("Handled unknown API request.")
            self._send(APIProtocol.Error.UNKNOWN_REQUEST)
            return True
        if self._version != APIProtocol._VERSION:
            _LOG.info("Handled API request with mismatching version.")
            self._send(APIProtocol.Error.VERSION_NOT_SUPPORTED)
            return True
        return False


class _GetRequest(APIProtocol):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._request_ids = None

    # noinspection PyUnusedLocal
    def _handle_response_from_other_node(self, request_id, error, data):
        """Handle get response, send the value back and cancel all
        pending requests.

        Send back "" if a timeout occurred.

        :type request_id: bytes
        :type error: RequestProtocol.Error or None
        :param data: List of bytes, the result.
        :type data: list
        :return: All request ids that are now resolved.
        """
        assert self._request_ids is not None
        assert self._headers is not None
        if error is None:
            self._send(APIProtocol.Error.TIMEOUT)
        else:
            if error != RequestProtocol.Error.NO_ERROR:
                value = b""
                _LOG.warning("Error on get request to other node: %s", error)
            else:
                value = data[0]  # data[1] = timestamp
            self._send(response=[value])
        return self._request_ids

    def handle_request(self):
        assert APIProtocol.server is not None
        server = APIProtocol.server  # shortcut
        # Get key.
        if self._data is None:
            self._send(APIProtocol.Error.UNKNOWN_REQUEST)
            return True
        key = self._data[0].decode(ENCODING)
        # Get value from cache.
        _, value = server.get_cache_value(key)
        # Or get value from somewhere else?
        contact_others = False
        nodes = server.get_nodes_for_index(index_for(key))
        if value is None:
            # Check if we should have it:
            contact_others = server.get_server_node_id() not in nodes
        if not contact_others:
            value = value or ""
            self._send(response=[value.encode(ENCODING)])
        else:
            build_request = (
                lambda rid: RequestProtocol.build_get_request(rid, key))
            sockets = (node.req_socket for node in nodes.values())
            self._request_ids = [uuid4().bytes for _ in nodes]
            server.send_requests(
                self._request_ids, sockets, build_request,
                self._handle_response_from_other_node)
        return True


# noinspection PyProtectedMember
APIProtocol._handlers[APIProtocol._Type.GET] = _GetRequest


class _SetRequest(APIProtocol):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._request_ids = None
        self._responses = None
        self._responses_count = None

    # noinspection PyShadowingNames,PyDefaultArgument
    def _handle_response_from_other_node(self, request_id, error, data):
        """Keep track of what nodes respond.

        Once all results are in or a timeout happened send the right result
        back to the api set request.

        :type request_id: bytes
        :type error: RequestProtocol.Error or None
        :param data: List of strings, the result.
        :type data: list
        :return: All request ids that are now resolved.
        """
        assert self._request_ids is not None
        assert self._headers is not None
        assert self._responses is not None
        assert self._responses_count is not None
        # noinspection PyPep8Naming
        Error = APIProtocol.Error  # shortcut
        _LOG.debug(
            "Set %s %s %s %s, %s", self._responses_count, self._responses,
            self._request_ids, error, data)
        result = None
        if error is None:
            result = Error.TIMEOUT
        else:
            self._responses.append(error)
            if len(self._responses) == self._responses_count:
                # Done, this was the last node responding. Send result.
                no_error = all(
                    r == RequestProtocol.Error.NO_ERROR
                    for r in self._responses)
                result = Error.NO_ERROR if no_error else Error.TOO_BIG
        if result is not None:
            self._send(error=result)
            _LOG.debug("Set response: %s", result)
        # On timeout cancel all pending request, otherwise only the current
        # one.
        return self._request_ids if error is None else [request_id]

    def handle_request(self):
        assert APIProtocol.server is not None
        server = APIProtocol.server  # shortcut
        self._responses = []
        # Get key and value
        if len(self._data) != 2:
            self._send(APIProtocol.Error.UNKNOWN_REQUEST)
            return True
        key, value = (part.decode(ENCODING) for part in self._data)
        # Where to sent the key to?
        key_index = index_for(key)
        server_node_id = server.get_server_node_id()
        nodes = server.get_nodes_for_index(key_index)
        self._responses_count = len(nodes)
        other_node_ids = [
            node_id for node_id in nodes if node_id != server_node_id]
        # Prepare requests ids for other nodes.
        self._request_ids = [uuid4().bytes for _ in other_node_ids]
        now = datetime.utcnow()
        if self._request_ids:
            build_request = (
                lambda rid:
                    RequestProtocol.build_set_request(rid, key, value, now))
            sockets = (
                node.req_socket
                for node_id, node in nodes.items() if node_id != server_node_id)
            _LOG.debug("Set key %s on nodes %s.", key, other_node_ids)
            server.send_requests(
                self._request_ids, sockets, build_request,
                self._handle_response_from_other_node)
        # Handle setting the value on the local cache and register the result.
        if server_node_id in nodes:
            error = server.set_cache_value(key, value, now, key_index)
            # noinspection PyPep8Naming
            TOO_BIG = Cache.Error.TOO_BIG  # shortcut
            # noinspection PyPep8Naming
            Error = RequestProtocol.Error  # shortcut
            error = Error.TOO_BIG if error == TOO_BIG else Error.NO_ERROR
            self._handle_response_from_other_node(b"", error, [])
        return True


# noinspection PyProtectedMember
APIProtocol._handlers[APIProtocol._Type.SET] = _SetRequest


class _StatusRequest(APIProtocol):

    def handle_request(self):
        assert APIProtocol.server is not None
        server = APIProtocol.server  # shortcut
        # noinspection PyProtectedMember
        data = [
            f"{server.get_server_node_id}",
            f"{','.join(server.get_other_node_ids())}",
            f"{server.get_distribution_circles()}",
            f"{server.get_cached_keys_count()}",
            f"{server.get_cache_space_usage()}",
        ]
        self._send(response=(part.encode(ENCODING) for part in data))
        return True


# noinspection PyProtectedMember
APIProtocol._handlers[APIProtocol._Type.STATUS] = _StatusRequest
