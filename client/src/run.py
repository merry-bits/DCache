#! /usr/bin/env python3
# -*- coding: utf-8 -*-
from argparse import ArgumentParser
from logging import basicConfig
from sys import stdout

from dcache_client import Cache

if __name__ == "__main__":
    basicConfig(stream=stdout, level="DEBUG")
    parser = ArgumentParser(description="Distributed cache client.")
    parser.add_argument("api", help="Like tcp://*:8001")
    parser.add_argument("key", help="The key to get or set.")
    parser.add_argument("--set", help="New value for the key.")
    args = parser.parse_args()
    client = Cache(args.api)
    args_key = args.key
    if args.key == "status":
        # noinspection PyProtectedMember
        print(client._make_request(client._VERSION, b"status"))
    elif args.set is None:
        get_value = client.get(args_key)
        print(f"Value: {get_value}")
    else:
        set_error = client.set(args_key, args.set)
        print(f"Tried to set '{args_key}', error: {set_error}")
