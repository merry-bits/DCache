# -*- coding: utf-8 -*-
from json import loads, dumps
from urllib.parse import urlencode
from urllib.request import urlopen, Request


class DCache():
    """Interface for a DCache node. """

    def __init__(self, url):
        self._url = url  # to bytes

    def set(self, key, value):
        """Set a key.

        Setting an empty key removes the key from the cache.

        :return: 0 if no error occurred
        """
        data = dumps({"key": key, "value": value}).encode("utf-8")
        req = Request(self._url)
        req.add_header('Content-Type', 'application/json')
        with urlopen(req, data) as response:
            data = loads(response.read().decode("utf-8"))
        return int(data["error"])

    def get(self, key):
        """Look up the value of a key.

        :return: (found, value) tuple, if found is False, then value is None
        """
        data = urlencode({"key": key})
        with urlopen(self._url + "?" + data) as response:
            data = loads(response.read().decode("utf-8"))
        return (data["found"], data["value"])
