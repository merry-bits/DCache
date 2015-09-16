#! /usr/bin/env python3
# -*- coding: utf-8 -*-
from argparse import ArgumentParser

from dcache_client.api_client import DCache
from uuid import uuid4
from sys import stdin, stdout


def _set_keys(cache, count):
    keys = {}
    try:
        for _ in range(count):
            key = str(uuid4())
            value = str(uuid4())
            error = cache.set(key, value)
            if error != 0:
                print("E", end="")
                stdout.flush()
            else:
                print(".", end="")
                keys[key] = value
                stdout.flush()
    finally:
        print("")  # print new line
    return keys


def _check_keys(cache, keys):
    errors = 0
    try:
        for k, v in keys.items():
            found, value = cache.get(k)
            if not found or value != v:
                errors += 1
                print("E", end="")
                stdout.flush()
            else:
                print(".", end="")
                stdout.flush()
    finally:
        print("")  # print new line
    return errors


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Test if keys are lost when one node goes down.")
    parser.add_argument(
        "url", help="The API url, something like http://localhost:9001/dcache")
    parser.add_argument(
        "--count", type=int, default=1000, help="Count of keys to test.")
    args = parser.parse_args()
    cache = DCache(args.url)
    print("Setting keys...")
    keys = _set_keys(cache, args.count)
    print("Disable one node, then press <enter>.")
    stdin.readline()
    print("Checking keys...")
    errors = _check_keys(cache, keys)
    print("Encountered {} cache misses.".format(errors))
