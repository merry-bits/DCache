from enum import Enum
from enum import unique
from logging import getLogger

# noinspection PyUnresolvedReferences
from zmq import Context, REQ, RCVTIMEO, SNDTIMEO


_LOG = getLogger(__name__)


class Cache:

    _VERSION = b"1"

    _GET = b"get"

    _SET = b"set"

    ENCODING = "utf-8"

    IO_TIMEOUT = 5 * 1000  # milliseconds

    @unique
    class Error(Enum):

        NO_ERROR = b"0"

        TOO_BIG = b"1"

        TIMEOUT = b"2"

        UNKNOWN_REQUEST = b"998"

        VERSION_NOT_SUPPORTED = b"999"

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
        self._api_socket.send_multipart(data)
        response = self._api_socket.recv_multipart()
        return response

    def get(self, key):
        """

        :type key: str
        :rtype: str
        """
        key = key.encode(Cache.ENCODING)
        response = self._make_request(Cache._VERSION, Cache._GET, key)
        error = Cache.Error(response[0])
        value = response[1] if error == Cache.Error.NO_ERROR else b""
        value = value.decode(Cache.ENCODING)
        return value

    def set(self, key, value):
        """

        :type key: str
        :type value: str
        :rtype: Cache.Error
        """
        key = key.encode(Cache.ENCODING)
        value = (value or "").encode(Cache.ENCODING)
        error = self._make_request(Cache._VERSION, Cache._SET, key, value)
        return Cache.Error(error[0])
