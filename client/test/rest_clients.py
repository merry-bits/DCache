from json import loads
from uuid import uuid4

from locust import HttpLocust, TaskSet, task  # @UnresolvedImport


class ClientTasks(TaskSet):

    def __init__(self, *args, **kwds):
        super(ClientTasks, self).__init__(*args, **kwds)
        self._key = None
        self._value = None

    @task
    def api_get_key(self):
        if self._key is not None:
            resp = self.client.get(
                "/dcache?key=%s" % self._key, name="/dcache?key=[key]")
            content = loads(resp.content)
            if content["value"] != self._value:
                raise ValueError()
        else:
            resp = self.client.get(
                "/dcache?key=%s" % str(uuid4), name="/dcache?key=")

    @task
    def api_set_key(self):
        self._key = str(uuid4())
        self._value = str(uuid4())
        resp = self.client.post(
            "/dcache", json={"key": self._key, "value": self._value})
        content = loads(resp.content)
        if content["error"] != "0":
            raise RuntimeError()


class WebsiteUser(HttpLocust):

    task_set = ClientTasks

    min_wait = 1000

    max_wait = 5000
