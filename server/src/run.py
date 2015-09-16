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
    parser.add_argument("request", help="")
    parser.add_argument("service", help="")
    parser.add_argument("api", type=int, help="")
    parser.add_argument("--node", help="")
    args = parser.parse_args()
    server = Server(Context(), args.request, args.service)
    try:
        server.loop(args.api, args.node)
    except KeyboardInterrupt:
        pass
