#! /usr/bin/env python3
# -*- coding: utf-8 -*-
from argparse import ArgumentParser
from logging import basicConfig
from sys import stdout

from zmq import Context

from dcache.server import Server

if __name__ == "__main__":
    basicConfig(stream=stdout, level="INFO")
    parser = ArgumentParser(description="Distributed cache node.")
    parser.add_argument(
        "request",
        help="Server request socket. Handles internal requests. "
        "Like tcp://127.0.0.1:9001")
    parser.add_argument(
        "publish",
        help="Server publishes known nodes through this socket. "
        "Like tcp://127.0.0.1:9101")
    parser.add_argument(
        "api",
        help="Socket for responding to cache requests. This is the interface "
        "to the distributed cache (cluster). Like tcp://*:8001")
    parser.add_argument(
        "--node",
        help=""
        "Request address of an other node in the cluster tho which this server "
        "should be connected to. Like tcp://127.0.0.1:9000")
    args = parser.parse_args()
    server = Server(Context(), args.request, args.publish, args.api, args.node)
    server.loop()
