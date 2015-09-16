# -*- coding: utf-8 -*-
from hashlib import md5
from logging import getLogger
from sys import byteorder


_LOG = getLogger(__name__)


def key_index(key):
    """

    :return: index of this key as a value between 0 and 1
    """
    m = md5()
    m.update(key.encode("utf-8"))
    return (
        int.from_bytes(m.digest(), byteorder=byteorder, signed=False) /
        pow(2, m.digest_size * 8))


class Cache():
    """The Cache for the node with a limited size and a last set first forget
    strategy.

    Each key has the following information stored:
    - time stamp of when the key was set
    - list of nodes (ids) where the key should be stored
    - value of the key
    """

    MAX_SIZE = 1024 * 1024  # time stamps are ignored for measuring the size!

    def __init__(self):
        #: :type self._cache: dict(str, (datetime.datetime, [long], str))
        self._cache = {}
        self._size = 0  # bytes

    @property
    def key_indices(self):
        return (
            (k, ts, nodes, value, key_index(k))
            for k, (ts, nodes, value) in self._cache.items())

    def delete_key(self, key):
        ts, _, value = self._cache.pop(key, (None, None, ""))
        if ts is not None:  # key was in cache, reduce size
            self._size -= len(key) + len(value)

    def _removed_oldest_key(self, except_key):
        """Look for the key with the oldest time stamp and remove it.
        """
        oldest = None
        key_to_delete = None
        for k, (t, _, _) in self._cache:
            if k != except_key:
                if oldest is None or t < oldest:
                    oldest = t
                    key_to_delete = k
        if key_to_delete is not None:
            self.delete_key(key_to_delete)

    def set(self, key, value, timestamp, nodes):
        """Set the value for the key.

        If the key does not fit in the available cache size, return an error.
        If there is not enough space: delete old keys until there is.
        """
        error = 0
        if not value:  # delete
            self.delete_key(key)
        else:
            # Calculate key and value size. Only key and value are considered.
            current_size = 0
            current_time_stamp = None
            key_len = len(key)
            size = key_len + len(value)
            if size > self.MAX_SIZE:
                error = -1  # to big
            else:
                # Is there an entry to update?
                if key in self._cache:
                    current_size += key_len
                    current_time_stamp, _, current_value = self._cache[key]
                    current_size += len(current_value)
                # Not in cache or update needed?
                # Update happens if the timestamps are the same, too.
                if (current_time_stamp is None or
                        current_time_stamp <= timestamp):
                    size_diff = size - current_size
                    while self._size + size_diff > self.MAX_SIZE:
                        self._removed_oldest_key()
                    self._cache[key] = (timestamp, nodes, value)
                    self._size += size_diff
            _LOG.debug(
                "Did set key %s, usage: %f%%", key,
                100 * self._size / self.MAX_SIZE)
        return error

    def get(self, key):
        """Return the value and when the key was set.

        :rtype: (bool, datetime.datetime, str)
        """
        in_cache = False
        timestamp = None
        value = None
        try:
            timestamp, _, value = self._cache[key]
            in_cache = True
        except KeyError:
            pass  # leaves in_cache to False
        return (in_cache, timestamp, value)
