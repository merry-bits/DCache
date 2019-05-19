"""
The simulated clients request keys.

Sometimes those keys where put in the cache bey themselves, in which case the
values get verified.
"""
import time
from random import choice
from uuid import uuid4

# noinspection PyPackageRequirements
from locust import Locust, TaskSet, task, events
from zmq import Context

from dcache_client.zmq_client import Cache


def _timeit(f):
    start_time = time.time()
    result = f()
    elapsed = int((time.time() - start_time) * 1000)
    return elapsed, result


def _request_failure(name, response_time, e):
    events.request_failure.fire(
        request_type="cache", name=name, response_time=response_time,
        exception=e)


def _request_success(name, response_time, length):
    events.request_success.fire(
        request_type="cache", name=name, response_time=response_time,
        response_length=length)


class CacheClient(Cache):
    """
    Define helper function on top of the cache client to register access times
    and faults with Locust.
    """

    def test_set(self, key, value):
        super_set = super().set
        response_time, error = _timeit(lambda: super_set(key, value))
        try:
            if error != Cache.Errors.NO_ERROR:
                raise ValueError(f"error: {error}")
        except Exception as e:
            _request_failure("set", response_time, e)
        else:
            _request_success("set", response_time, 1)
        return error

    def test_get(self, key, expected):
        super_get = super().get
        response_time, value = _timeit(lambda: super_get(key))
        try:
            if value != expected:
                raise AttributeError(f"value mismatch: {value} != {expected}")
        except Exception as e:
            _request_failure("get", response_time, e)
        else:
            _request_success("get", response_time, len(value))


class CacheLocust(Locust):

    def __init__(self, *args, **kwargs):
        # noinspection PyArgumentList
        super().__init__(*args, **kwargs)
        # noinspection PyUnresolvedReferences
        addresses = self.server_api_address.split(",")
        # noinspection PyUnresolvedReferences
        self.client = CacheClient(choice(addresses), self.zmq_context)


class ClientTasks(TaskSet):

    def __init__(self, *args, **kwargs):
        super(ClientTasks, self).__init__(*args, **kwargs)
        self._key = None
        self._value = None

    @task(10)
    def api_get_key(self):
        if self._key is not None:
            self.client.test_get(self._key, self._value)
        else:
            self.client.test_get(uuid4().hex, "")

    @task
    def api_set_key(self):
        # Set a new key.
        self._key = uuid4().hex
        self._value = uuid4().hex
        self.client.test_set(self._key, self._value)


class Clients(CacheLocust):

    server_api_address = (
        "tcp://127.0.0.1:8001,tcp://127.0.0.1:8002,tcp://127.0.0.1:8003")

    zmq_context = Context()

    task_set = ClientTasks

    min_wait = 100  # minimum milliseconds between tasks

    max_wait = 500  # maximum milliseconds between tasks
