from enum import (
    Enum,
    unique,
)
from logging import getLogger

# noinspection PyUnresolvedReferences
from zmq import Context, REQ, RCVTIMEO, SNDTIMEO


_LOG = getLogger(__name__)


class Cache:

    API_GET = "get"

    API_SET = "set"

    ENCODING = "utf-8"

    IO_TIMEOUT = 5 * 1000  # milliseconds

    @unique
    class Errors(Enum):

        NO_ERROR = "0"

        TOO_BIG = "-1"

        TIMEOUT = "-2"

        NODE_ID_TAKE = "-998"

        VERSION_NOT_SUPPORTED = "-999"

    def __init__(self, server_api_address, context=None):
        if context is None:
            context = Context()
            _LOG.debug("Created new context.")
        self._context = context
        self._api_socket = self._context.socket(REQ)
        self._api_socket.setsockopt(RCVTIMEO, self.IO_TIMEOUT)
        self._api_socket.setsockopt(SNDTIMEO, self.IO_TIMEOUT)
        self._api_socket.connect(server_api_address)
        _LOG.debug(f"Connected to: {server_api_address}")

    def _make_request(self, *data):
        parts = [part.encode(Cache.ENCODING) for part in data]
        self._api_socket.send_multipart(parts)
        response = self._api_socket.recv_multipart()
        return [part.decode(Cache.ENCODING) for part in response]

    def get(self, key):
        response = self._make_request(Cache.API_GET, key)
        return response[0] if response else None

    def set(self, key, value):
        error = self._make_request(Cache.API_SET, key, value or "")
        return Cache.Errors(error[0]) if error else None
