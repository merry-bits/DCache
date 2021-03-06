# Distributed redundant cache

A distributed cache written in Python using ZeroMQ. It enables to either set or
retrieve a value by key. Both value and key are strings. An empty value can not
be stored. Storing an empty value deletes the entry instead.

The cache consists of server nodes which communicate with each other and
distribute the cache entries between them. When a node leaves the cache cluster
not all entries from the leaving node are lost. Each node can answer any query
and will forward a request to other nodes as necessary.

Stored or deleted values only become consistent eventually. The first node that
answers a get request determines the value a key has for the request made. Where
and on how many nodes an entry will be stored depends on the key and the number
of nodes in the cluster (not mentioning the nodes configuration).


## Installation

Create a Python 3 virtual environment or use an existing one and in the project
folder run the following command to install the required python packages:
```bash
    $ pip install -r requirements.txt
```

Once the requirements are fulfilled the server and client scripts can be used.

No setup or package support exists at this time.


## Run

Both the server and the command line client need a Python 3 environment and use
their set of parameters. Both accept `--help` and will show what is available
or needed.

To change the cache size (measured in characters) change the `MAX_SIZE` value in
[cache.py](server/src/dbcache/cache.py) before the server instance gets created.


### Server node

Run a node with:
```bash
    $ cd server/src
    $ ./run.py <REQUEST ADDRESS> <PUBLISH ADDRESS> <API ADDRESS>
```

Each node needs three ZeroMQ socket address descriptors (the likes of
`tcp://127.0.0.1:8000`):
- `request`: get/set cache commands and connect a new node to existing ones 
- `publication`: nodes checking who is still running/reachable
- `api`: get/set client requests

If a new node needs to be connected to an existing cluster it needs a ZeroMQ
socket address with the `--node` parameter pointing to the request socket of
another running node.


### Configuration must match!

Each node can calculate where a key should be stored by the key, a nodes id and
the replication and redundancy settings. If the number of replicas and the level
of redundancy are not the same for all nodes in a cluster some nodes will
mistakenly expect keys to be where they are not!


## Client

Set or get a key with:
```bash
    $ cd client/src
    $ ./run.py
```

Alternatively import the `Cache` class from dcache_client and use the provided
functions.  


# Implementation

- the distribution of the keys is based on the blog entry
[Consistent hashing](http://michaelnielsen.org/blog/consistent-hashing/).
- it is possible to add or remove one node at a time during runtime


## Server detection

Each server publishes all known servers regularly to all subscribers on the
publish URL. In turn the server registers to all other servers and merges their
list with its own. In this list each server has a last-seen date. If a node is
not seen by at least one node within a defined amount of time each node starts
to remove that node from their list.


### Publish protocol

```
publish = S:(node-topic *node)
node-topic = "n"
node = node-id request-address publication-address last-seen
last-seen = YEAR : MONTH : DAY : HOUR : MINUTES : SECONDS ; UTC, Unix time
```


## Key distribution

According to the blog entry each node owns several indices (between 0 and 1),
based on the node ID. This indices are calculated by each node for all other
nodes and in that way each node knows to which node a key belongs. How often
each node is present on the distribution circle is configured by `REPLICAS`.
It is possible to use more than one circle so that the same key may be stored on
different nodes. The amount of circles is configured with `REDUNDANCY`. (both
to be found in [nodes.py](server/src/dcache/nodes.py))


### Request protocol

```
request = *(set / get / connect-to-cluster)
set = \
    C:(ids "" version "set" KEY VALUE timestamp)
    S:(ids "" (version-not-supported / unknown-request / too-big / no-error))
get = \
    C:(ids "" version "get" KEY)
    S:(ids "" \
        (version-not-supported / unknown-request / (no-error VALUE timestamp))
connect-to-cluster = \
    C:(ids "" version "connect" NODE-ID REQUEST-ADDRESS PUBLISH-ADDRESS) \
    S:(ids "" \
        (version-not-supported / unknown-request / node-id-taken / \
            (no-error SERVER-NODE-ID SERVER-REQUEST-ADDRESS \
                SERVER-PUBLISH-ADDRESS
            )
        )
    )
ids = *ID ; zero or more ids, identifying a client request
version = "1"
no-error = "0" ; operation was successful
too-big = "1" ; the key and value pair is bigger than the cache size
node-id-taken = "997"
unknown-request = "998"
version-not-supported = "999"
timestamp = YEAR : MONTH : DAY : HOUR : MINUTES : SECONDS ; UTC, Unix time
```


#### To be added:

The node to join should reuse the redundancy and replica values of the cluster
and keep the own values until then at 1 only. Transmitting those values during
the `connect-to-cluster` command should do the trick.

This way a calculation for where a key should be store will always yield the
exact same result, no matter which node makes the calculation.


### API protocol

```
api = *(set / get)
set = \
    C:(ids "" version "set" KEY VALUE) \
    S:(ids "" \
        (no-error / version-not-supported / unknown-request / too-big / \
            timeout))
get = \
    C:(ids "" version "get" KEY) \
    S:(ids "" (version-not-supported / unknown-request) / (no-error VALUE))
ids = *ID ; zero or more ids, identifying a client request
version = "1"
no-error = "0" ; operation was successful
too-big = "1" ; the key and value pair is bigger than the cache size
timeout = "2" ; at least one copy of the key was not stored as intended
unknown-request = "998"
version-not-supported = "999"
```
