from dataclasses import dataclass
from datetime import datetime
from enum import (
    unique,
    Enum,
)
from logging import getLogger


_LOG = getLogger(__name__)


class Cache:
    """The Cache for the node with a limited size and a last set first forget
    strategy.

    Each key has the following information stored:
    - time stamp of when the key was set
#    - list of nodes (ids) where the key should be stored
    - value of the key
    """

    # Max amount of characters in the cache (len values of keys and values).
    MAX_SIZE = 1024 * 1024  # time stamps are ignored for measuring the size!

    @unique
    class Error(Enum):

        NO_ERROR = "0"

        TOO_BIG = "-1"

    @dataclass
    class _Entry:

        value: str

        last_update: datetime

        hash_index: float

    def __init__(self):
        self._cache = {}  # key: _Entry
        self._size = 0  # characters count

    def __len__(self):
        return len(self._cache)

    def items(self):
        return self._cache.items()

    @property
    def space_usage(self):
        """How much of then available cache size is in use, in percentages.

        :rtype: float
        """
        return 100 * self._size / self.MAX_SIZE

    def _removed_oldest_key(self, except_key):
        """Look for the key with the oldest time stamp and remove it.

        :type except_key: str
        """
        oldest = None
        key_to_delete = None
        for k, entry in self._cache.items():
            if k != except_key:
                if oldest is None or entry.last_update < oldest:
                    oldest = entry.last_update
                    key_to_delete = k
        if key_to_delete is not None:
            self._delete_key(key_to_delete)

    def _delete_key(self, key):
        """Remove from cache and adjust size.

        :type key: str
        """
        entry = self._cache.pop(key, None)
        # Update size, if necessary.
        if entry is not None:
            self._size -= len(key) + len(entry.value)

    def _set_entry(self, key, entry, size_change):
        """Write one entry to the cache, make space first, if necessary.

        :type key: value
        :type entry: Cache._Entry
        :type size_change: int
        """
        assert size_change <= self.MAX_SIZE
        # Make space, if necessary.
        while self._size + size_change > self.MAX_SIZE:
            self._removed_oldest_key(key)
        # Update entry and size count
        self._cache[key] = entry
        self._size += size_change
        _LOG.debug("Did set key %s, usage: %f%%", key, self.space_usage)

    def set(self, key, value, timestamp, hash_index):
        """Set the value for the key.

        If there is not enough space: delete oldest keys until there is.

        :type key: str
        :type value: str or None
        :type timestamp: datetime.datetime
        :type hash_index: float or None
        :return:
        :rtype: Cache.Error
        """
        if not value:  # delete when None or empty!
            self._delete_key(key)
            return Cache.Error.NO_ERROR
        # Check size.
        key_len = len(key)
        size = key_len + len(value or "")
        if size > self.MAX_SIZE:
            return Cache.Error.TOO_BIG
        # Add or modify entry.
        new_size = None  # nothing to do
        entry = self._cache.get(key, None)
        if entry is None:
            # New entry needed.
            new_size = size
            # noinspection PyCallByClass
            entry = Cache._Entry(value, timestamp, hash_index)
        elif entry.last_update <= timestamp:
            # Update existing entry.
            current_size = key_len + len(entry.value)
            new_size = size - current_size
            entry.value = value  # technically changes cache size...
            entry.last_update = timestamp
        if new_size is not None:  # set entry necessary?
            self._set_entry(key, entry, new_size)
        return Cache.Error.NO_ERROR

    def get(self, key):
        """Return the value and when the key was set.

        :rtype: (datetime.datetime, str)
        """
        entry = self._cache.get(key)
        exists = entry is not None
        last_update = entry.last_update if exists else None
        value = entry.value if exists else None
        return last_update, value

    def index_for(self, key):
        entry = self._cache.get(key)
        return entry.hash_index if entry is not None else None
