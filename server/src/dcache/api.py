# -*- coding: utf-8 -*-
from uuid import uuid4

from flask import current_app, request, jsonify
from flask.app import Flask
from werkzeug.exceptions import BadRequest, InternalServerError
from zmq import PUSH, SUB, POLLIN  # @UnresolvedImport
from zmq import Poller
from zmq import SNDTIMEO, SUBSCRIBE  # @UnresolvedImport


_TIMEOUT = 5 * 1000  # milliseconds

SUB_ENDPOINT = "inproc://api_response"

PUSH_ENDPOINT = "inproc://api_request"

app = Flask(__name__)


def set_config(context):
    app.config["context"] = context


def _send_request(req_id, action, key, value=None):
    """ Generate a request ID, push the request to the node and wait for the
    result, filtering by the ID on the subscription.

    This should be a request/response call to the node, but since many must be
    possible at the same time, a PUSH/PULL AND PUB/SUB with an unique ID for
    each request is used instead.

    :return: the node response
    :rtype: []
    """
    # Create and connect the sockets.
    context = current_app.config["context"]
    subscriber = context.socket(SUB)
    subscriber.setsockopt_string(SUBSCRIBE, req_id)
    subscriber.connect(SUB_ENDPOINT)
    request = context.socket(PUSH)
    request.setsockopt(SNDTIMEO, _TIMEOUT)
    request.connect(PUSH_ENDPOINT)
    # Push the request.
    request.send_json([req_id, action, key, value])
    # Wait for the response from the publisher.
    poller = Poller()
    poller.register(subscriber, POLLIN)
    sockets = dict(poller.poll(_TIMEOUT))
    if subscriber not in sockets:  # no response, time out
        raise InternalServerError()
    # Return the response, without the unique request ID
    return subscriber.recv_multipart()[1:]


@app.route("/dcache", methods=["GET"])
def cache_get():
    """ Read a cache key.

    Expect the key as a GET parameter.

    Returns a JSON object:
        { "found": true, "value": "xxxxx" }
    """
    key = request.args.get("key")
    if key is None:
        raise BadRequest()
    req_id = str(uuid4())
    found, value = _send_request(req_id, "get", key)
    return jsonify(found=(found == b"1"), value=value.decode("utf-8"))


@app.route("/dcache", methods=["POST"])
def cache_set():
    """ Set a cache key.

    Expects the key and value as a JSON object in the POST body:
    { "key": "my_key", "value": "xxxxx" }

    Returns the error state (0 equals no error) as a JSON object:
    { "error": "0" }
    """
    req_id = str(uuid4())
    set_request = request.get_json()
    if "key" not in set_request or "value" not in set_request:
        raise BadRequest()
    error = _send_request(
        req_id, "set", set_request["key"], set_request["value"])
    return jsonify(error=error[0].decode("utf-8"))
