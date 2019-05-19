from datetime import (
    datetime,
    timedelta,
)
from unittest import TestCase
from unittest.mock import patch

from dcache.cache import Cache
from given_when_then import (
    given_when_then_test,
    GWT,
)


class TestCache(TestCase):

    def setUp(self):
        self.cache = Cache()

    # noinspection PyPep8Naming,PyAttributeOutsideInit
    @patch.object(Cache, "MAX_SIZE", 2)
    @given_when_then_test
    class test_error_when_size_to_big_for_one_key(GWT):

        def given(self):
            # Total size of 3, exceeds size maximum.
            self.key = "a"
            self.value = "12"
            self.timestamp = datetime.utcfromtimestamp(0)

        def when(self):
            self.error = self.test_case.cache.set(
                self.key, self.value, self.timestamp, 0.0)

        def then(self):
            assert self.error == Cache.Error.TOO_BIG

    # noinspection PyPep8Naming,PyAttributeOutsideInit
    @given_when_then_test
    class test_get_when_set(GWT):

        def given(self):
            self.key1 = "a"
            self.value1 = "1"
            self.timestamp1 = datetime.utcfromtimestamp(0)
            self.key2 = "b"
            self.value2 = "2"
            self.timestamp2 = datetime.utcfromtimestamp(1)
            self.key3 = "c"  # not set

        def when(self):
            cache = self.test_case.cache  # shortcut
            cache.set(self.key1, self.value1, self.timestamp1, 0)
            cache.set(self.key2, self.value2, self.timestamp2, 0)

        def then(self):
            cache = self.test_case.cache  # shortcut
            last_update, value = cache.get(self.key1)
            assert last_update == self.timestamp1
            assert value == self.value1
            last_update, value = cache.get(self.key2)
            assert last_update == self.timestamp2
            assert value == self.value2
            last_update, value = cache.get(self.key3)
            assert last_update is None
            assert value is None

    # noinspection PyPep8Naming,PyAttributeOutsideInit
    @given_when_then_test
    class test_removed_when_set_empty(GWT):

        def given(self):
            self.key = "a"
            self.test_case.cache.set(
                self.key, "1", datetime.utcfromtimestamp(1), 0)

        def when(self):
            self.test_case.cache.set(self.key, "", None, None)

        def then(self):
            _, value = self.test_case.cache.get(self.key)
            assert value is None

    # noinspection PyPep8Naming,PyAttributeOutsideInit
    @given_when_then_test
    class test_removed_when_set_none(GWT):

        def given(self):
            self.key = "a"
            self.test_case.cache.set(
                self.key, "1", datetime.utcfromtimestamp(1), 0)

        def when(self):
            self.test_case.cache.set(self.key, None, None, None)

        def then(self):
            last_update, value = self.test_case.cache.get(self.key)
            assert last_update is None
            assert value is None

    # noinspection PyPep8Naming,PyAttributeOutsideInit
    @given_when_then_test
    class test_value_updated_when_setting_new(GWT):
        """Set new values once with new and once with same time stamp and check
        for the updated value.
        """

        def given(self):
            cache = self.test_case.cache  # shortcut
            self.key1 = "a"
            self.key2 = "b"
            self.timestamp = datetime.utcfromtimestamp(0)
            cache.set(self.key1, "1", self.timestamp, 0)
            cache.set(self.key2, "2", self.timestamp, 0)
            self.timestamp_new = self.timestamp + timedelta(seconds=1)
            self.new_value1 = "3"
            self.new_value2 = "4"

        def when(self):
            cache = self.test_case.cache  # shortcut
            cache.set(self.key1, self.new_value1, self.timestamp_new, 0)
            cache.set(self.key2, self.new_value2, self.timestamp, 0)

        def then(self):
            cache = self.test_case.cache  # shortcut
            # New time and new value:
            last_update, value = cache.get(self.key1)
            assert last_update == self.timestamp_new
            assert value == self.new_value1
            # Same time, but new value:
            last_update, value = cache.get(self.key2)
            assert last_update == self.timestamp
            assert value == self.new_value2

    # noinspection PyPep8Naming,PyAttributeOutsideInit
    @given_when_then_test
    class test_not_updated_when_timestamp_is_old(GWT):

        def given(self):
            self.key = "a"
            self.value = "1"
            self.old_timestamp = datetime.utcfromtimestamp(0)
            self.new_timestamp = self.old_timestamp + timedelta(seconds=1)
            self.test_case.cache.set(
                self.key, self.value, self.new_timestamp, 0)
            self.new_value = "2"

        def when(self):
            self.test_case.cache.set(
                self.key, self.new_value, self.old_timestamp, 0)

        def then(self):
            last_update, value = self.test_case.cache.get(self.key)
            assert last_update == self.new_timestamp
            assert value == self.value

    # noinspection PyPep8Naming,PyAttributeOutsideInit
    @patch.object(Cache, "MAX_SIZE", 2)
    @given_when_then_test
    class test_key_removed_when_not_enough_size(GWT):
        """Reduce cache size so that only one entry fits, then see if an entry
        gets removed when setting a new one.
        """

        def given(self):
            cache = self.test_case.cache  # shortcut
            self.key1 = "a"
            self.timestamp = datetime.utcfromtimestamp(0)
            cache.set(self.key1, "1", self.timestamp, 0)
            self.key2 = "b"

        def when(self):
            self.test_case.cache.set(self.key2, "2", self.timestamp, 0)

        def then(self):
            cache = self.test_case.cache  # shortcut
            _, value = cache.get(self.key1)
            assert value is None
            _, value = cache.get(self.key2)
            assert value is not None
