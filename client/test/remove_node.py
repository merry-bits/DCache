#! /usr/bin/env python3
"""
When a node goes down keys may be lost, too.

This script can be used to demonstrate this effect.

Keys are lost, nice the distribution across redundancy levels is random. There
is no guaranty that n copies of a key end up on n nodes.
"""
from argparse import ArgumentParser

from dcache_client.zmq_client import Cache
from uuid import uuid4
from sys import stdin, stdout


def _set_keys(cache, count):
    keys = {}
    try:
        for _ in range(count):
            key = str(uuid4())
            value = str(uuid4())
            error = cache.set(key, value)
            if error != Cache.Errors.NO_ERROR:
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
            value = cache.get(k)
            if not value or value != v:
                errors += 1
                print("E", end="")
                stdout.flush()
            else:
                print(".", end="")
                stdout.flush()
    finally:
        print("")  # print new line
    return errors


def _unset_keys(cache, keys):
    for key in keys.keys():
        cache.set(key, None)


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Test how many keys are lost when one node goes down.")
    parser.add_argument(
        "url", help="The API url, something like tcp://127.0.0.1:8001")
    parser.add_argument(
        "--count", type=int, default=1000, help="Count of keys to test.")
    args = parser.parse_args()
    cache_client = Cache(args.url)
    print("Setting keys...")
    keys_set = _set_keys(cache_client, args.count)
    print("Disable one node, wait until it disappeared, then press <enter>.")
    stdin.readline()
    print("Checking keys...")
    misses = _check_keys(cache_client, keys_set)
    print(f"Encountered {misses} ({100 * misses / args.count}%)cache misses.")
    print("Deleting keys...")
    _unset_keys(cache_client, keys_set)
