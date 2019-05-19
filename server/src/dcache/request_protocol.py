"""Defines requests between nodes.

See "Request protocol" in README.
"""
from enum import (
    unique,
    Enum,
)
from logging import getLogger

from dcache.zmq import str_to_datetime
from .cache import Cache
from .zmq import ENCODING
from .zmq import datetime_to_str
from .zmq import split_header

_LOG = getLogger(__name__)


class RequestServer:

    def send_multipart(self, data):
        """Send byte frames, using the servers request socket.

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

    def set_cache_value(self, key, value, timestamp):
        """

        :type key: str
        :type value: str
        :type timestamp: datetime.datetime
        :rtype: dcache.cache.Cache.Error
        """
        raise NotImplementedError()

    def does_node_id_exist(self, node_id):
        """

        :type node_id: str
        :rtype: bool
        """
        raise NotImplementedError()

    def add_node(self, node_id, request_address, publish_address):
        """

        :type node_id: str
        :type request_address: str
        :type publish_address: str
        :rtype: bool
        """
        raise NotImplementedError()

    def get_other_node_ids(self):
        """

        :return: Iterable over existing node ids, excluding the server node.
        """
        raise NotImplementedError()

    def send_multipart_to(self, node_id, data):
        """Send data frames to request address of the specified node.

        :type node_id: str
        :type data: [bytes]
        """
        raise NotImplementedError()

    def redistribute(self, node_id):
        """The given node id was added, update distribution circle etc.

        :type node_id: str
        """
        raise NotImplementedError()

    def get_server_node_info(self):
        """Get the node id, request address and publish address from the servers
        own node.

        :rtype: (str, str, str)
        """
        raise NotImplementedError()


class RequestProtocol:

    _VERSION = b"1"

    @unique
    class _Type(Enum):

        GET = b"get"

        SET = b"set"

        CONNECT_TO_CLUSTER = b"connect"

    @unique
    class Error(Enum):

        NO_ERROR = b"0"

        TOO_BIG = b"1"

        NODE_ID_TAKEN = b"997"

        UNKNOWN_REQUEST = b"998"

        VERSION_NOT_SUPPORTED = b"999"

    _handlers = {  # _Type: RequestProtocol
    }

    @staticmethod
    def build_set_request(request_id, key, value, timestamp):
        """

        :type request_id: bytes
        :type key: str
        :type value: str
        :type timestamp: datetime.datetime
        :rtype: [bytes]
        """
        timestamp_str = datetime_to_str(timestamp)
        return [
            request_id,
            b"",
            RequestProtocol._VERSION,
            RequestProtocol._Type.SET.value,
            key.encode(ENCODING),
            value.encode(ENCODING),
            timestamp_str.encode(ENCODING),
        ]

    @staticmethod
    def build_get_request(request_id, key):
        """

        :type request_id: bytes
        :type key: str
        :rtype: [bytes]
        """
        return [
            request_id,
            b"",
            RequestProtocol._VERSION,
            RequestProtocol._Type.GET.value,
            key.encode(ENCODING),
        ]

    @staticmethod
    def build_connect_to_cluster_request(
            request_id, node_id, request_address, publish_address):
        """

        :type request_id: bytes
        :type node_id: str
        :type request_address: str
        :type publish_address: str
        :rtype: [bytes]
        """
        return [
            request_id,
            b"",
            RequestProtocol._VERSION,
            RequestProtocol._Type.CONNECT_TO_CLUSTER.value,
            node_id.encode(ENCODING),
            request_address.encode(ENCODING),
            publish_address.encode(ENCODING),
        ]

    @staticmethod
    def handler_for(data):
        if not data:
            return RequestProtocol(None, None, None)
        # Read headers (includes empty frame).
        headers, data = split_header(data)
        # Read the version.
        if not data:
            return RequestProtocol(None, None, None)
        version = data[0]
        data = data[1:]
        # Read the type.
        if not data:
            return RequestProtocol(headers, None, None)
        try:
            request_type = RequestProtocol._Type(data[0])
        except ValueError:
            return RequestProtocol(headers, None, None)
        data = data[1:]
        command = RequestProtocol._handlers.get(request_type)
        if command is not None:
            return command(headers, version, data)
        return RequestProtocol(headers, None, None)

    @staticmethod
    def read_answer(data):
        """Read an answer.

        :type data: [bytes]
        :return: The request id, error and data. If malformed all is None. If an
            error is present data is an empty list. The request id might be
            None.
        :rtype: (bytes, RequestProtocol.Error, [bytes])
        """
        malformed_answer = (None, None, None)
        # Read request id.
        if not data:
            return malformed_answer
        headers, data = split_header(data)
        if len(headers) == 2:
            request_id = headers[0]
        else:
            return malformed_answer
        # Read the error code.
        if not data:
            return malformed_answer
        try:
            error = RequestProtocol.Error(data[0])
        except ValueError:
            return malformed_answer
        data = data[1:]
        return request_id, error, data

    server: RequestServer = None

    def __init__(self, headers, version, params):
        self._headers = headers
        self._version = version
        self._params = params

    def _send(self, error=None, response=None):
        """

        :type error: RequestProtocol.Error
        :type response: [bytes]
        """
        assert RequestProtocol.server is not None
        server = RequestProtocol.server  # shortcut
        error = RequestProtocol.Error.NO_ERROR if error is None else error
        response = [] if response is None else response
        data = [
            *self._headers,
            error.value,
            *response
        ]
        return server.send_multipart(data)

    def handle_request(self):
        """

        :return: True if request is dealt with.
        """
        if self._headers is None:
            _LOG.warning("Handled request without headers. No response send.")
            return True  # done nothing to send
        if self._version is None:
            _LOG.info("Handled unknown request.")
            self._send(RequestProtocol.Error.UNKNOWN_REQUEST)
            return True
        if self._version != RequestProtocol._VERSION:
            _LOG.info("Handled request with mismatching version.")
            self._send(RequestProtocol.Error.VERSION_NOT_SUPPORTED)
            return True
        return False


class _GetRequest(RequestProtocol):

    def handle_request(self):
        assert RequestProtocol.server is not None
        server = RequestProtocol.server  # shortcut
        done = super().handle_request()
        if done:
            return True
        if not self._params:
            self._send(RequestProtocol.Error.UNKNOWN_REQUEST)
            return True
        key = self._params[0].decode(ENCODING)
        timestamp, value = server.get_cache_value(key)
        timestamp_str = datetime_to_str(timestamp) if timestamp else ""
        value = "" if value is None else value
        response = (value, timestamp_str)
        response = (part.encode(ENCODING) for part in response)
        self._send(response=response)
        return True


# noinspection PyProtectedMember
RequestProtocol._handlers[RequestProtocol._Type.GET] = _GetRequest


class _SetRequest(RequestProtocol):

    def handle_request(self):
        assert RequestProtocol.server is not None
        server = RequestProtocol.server  # shortcut
        done = super().handle_request()
        if done:
            return True
        if len(self._params) < 3:
            self._send(RequestProtocol.Error.UNKNOWN_REQUEST)
            return True
        params = (part.decode(ENCODING) for part in self._params[:3])
        key, value, timestamp = params
        timestamp_datetime = str_to_datetime(timestamp)
        error = server.set_cache_value(key, value, timestamp_datetime)
        if error == Cache.Error.TOO_BIG:
            error = RequestProtocol.Error.TOO_BIG
        else:
            error = RequestProtocol.Error.NO_ERROR
        self._send(error)
        return True


# noinspection PyProtectedMember
RequestProtocol._handlers[RequestProtocol._Type.SET] = _SetRequest


class _ConnectRequest(RequestProtocol):

    def handle_request(self):
        assert RequestProtocol.server is not None
        server = RequestProtocol.server  # shortcut
        done = super().handle_request()
        if done:
            return True
        if len(self._params) < 3:
            self._send(RequestProtocol.Error.UNKNOWN_REQUEST)
            return True
        params = (part.decode(ENCODING) for part in self._params[:3])
        node_id, req_address, pub_address = params
        exists = server.does_node_id_exist(node_id)
        if exists:
            self._send(RequestProtocol.Error.NODE_ID_TAKEN)
            return True
        # Add to nodes.
        other_node_ids = server.get_other_node_ids()
        added_node = server.add_node(node_id, req_address, pub_address)
        if added_node:
            # Forward request.
            new_node = (node_id, req_address, pub_address)
            new_node = (part.encode(ENCODING) for part in new_node)
            new_node = [
                b"", RequestProtocol._Type.CONNECT_TO_CLUSTER.value, *new_node]
            for cluster_node_id in other_node_ids:
                server.send_multipart_to(cluster_node_id, new_node)
            # Move data.
            server.redistribute(node_id)
            # Send response.
            response = server.get_server_node_info()
            response = (part.encode(ENCODING) for part in response)
            self._send(response=response)
        else:
            self._send(error=RequestProtocol.Error.NODE_ID_TAKEN)
        return True


# noinspection PyProtectedMember
RequestProtocol._handlers[RequestProtocol._Type.CONNECT_TO_CLUSTER] = (
    _ConnectRequest)
