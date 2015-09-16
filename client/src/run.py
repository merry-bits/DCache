#! /usr/bin/env python3
# -*- coding: utf-8 -*-
from argparse import ArgumentParser

from dcache_client.api_client import DCache


if __name__ == "__main__":
    parser = ArgumentParser(description="Set or get a cache value by key.")
    parser.add_argument(
        "url", help="The API url, something like http://localhost:9001/dcache")
    parser.add_argument("key", help="The key to get or set")
    parser.add_argument(
        "-v", "--value", help="If provided, set the key with the given value")
    args = parser.parse_args()
    cache = DCache(args.url)
    if args.value is not None:
        print(cache.set(args.key, args.value))
    else:
        print(cache.get(args.key))
