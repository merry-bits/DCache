#! /usr/bin/env python3
# -*- coding: utf-8 -*-
from argparse import ArgumentParser
from logging import basicConfig
from sys import stdout

from zmq import Context

from dcache.server import Server


if __name__ == "__main__":
    basicConfig(stream=stdout, level="DEBUG")
    parser = ArgumentParser(description="Distributed cache node.")
    parser.add_argument("request", help="Like tcp://127.0.0.1:9001")
    parser.add_argument("publish", help="Like tcp://127.0.0.1:9101")
    parser.add_argument("api", help="Like tcp://*:8001")
    parser.add_argument(
        "--node",
        help=""
        "Request address of an other node in the cluster. "
        "Like tcp://127.0.0.1:9000")
    args = parser.parse_args()
    server = Server(Context(), args.request, args.publish, args.api)
    server.loop(args.node)
