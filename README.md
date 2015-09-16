# Distributed redundant cache

A distributed cache with a REST API, written in Python using ZeroMQ.
The cache consists of server nodes which communicate with each other and
distribute the cache keys between them. The system has redundancy, meaning each
key is stored on more than one server (default 3). A Python client for the
API is provided as well.


## Installation

Create a Python 3 virtual environment or use an existing one and in the project
folder run:
```bash
    $ pip install -r requirements.txt
```


## For running a load test with Locust

Create a virtual or prepare an existing Python 2 environment and install
[Locust](http://locust.io/):
```bash
    $ pip install locustio
```


## Run

Both the node and the command line client need a Python 3 environment and use
their set of parameters. Both accept `--help` and will show what is available
or needed.

The test run uses locustio to simulate client requests. This needs a Python 2
environment.


### Node

Run a node with:
```
    $ cd server/src
    $ ./run.py
```

Each node needs two ZeroMQ socket address descriptors (the likes of
`tcp://127.0.0.1:8000`):
- `request`: get/set cache commands and connect a new node to existing ones 
- `service`: nodes making sure who is still running/reachable
In addition each node runs a HTTP service on `0.0.0.0` at the given API port.

If a new node needs to be connected to an existing cluster it needs a ZeroMQ
socket address in the `--node` parameter pointing to the request socket of
another running node from that cluster.


## Client

Set or get a key with:
```
    $ cd client/src
    $ ./run.py
```

## Load test

Start at least one node, remember the selected API port, then in the Python 2
environment run:
```
    $ locust -f client/test/rest_clients.py --host http://localhost:<API port>
```
Now a locust test page is running at `http://localhost:8089` where you can
start and stop clients setting and reading random keys.


## Keys test

Start at least one node, then:
```
    $ cd client/test
    $ PYTHONPATH="../src" ./broken_node.py <URL>
```
The URL parameter should point to the API service of a running node. Something
like `http://localhost:<API port>/dcache`.

In theory this should add 1000 random keys, read and compare them, but at
present it fails randomly if more than one node run as a cluster.


# Solution

- the distribution of the keys is based on the blog entry
[Consistent hashing](http://michaelnielsen.org/blog/consistent-hashing/)
- it is possible to add or remove one node at a time during runtime
- on start up a new node needs to know only one other node to join
- ZeroMQ is used for communication
- each node provides the same API to the outside world


## Server detection

Each server publishes all known servers regularly to all subscribers on the
service URL. In turn the server registers to all other servers and merges their
list with it's own. In this list each server has a last-seen date. If a node is
not seen by at least one node within a certain amount of time each node starts
to remove that node from the list.


## Key distribution

According to the blog entry each node owns several indices (between 0 and 1),
based on the nodes ID. This indices are calculated by each node for all other
nodes and in that way each node knows to which node a key belongs.


## Redundancy

Each key is stored on three different nodes. This is achieved by making the IDs
of the nodes sortable and giving each node the keys for the next two following
nodes to store, as well as their own keys.


## Adding/removing a node

When a node is started up it registers itself to the existing node, subscribes
to thats node service URL and shortly receives the list of all other nodes, too. 

When one node is not seen for a certain amount of time each other node will
remove that node from the list of known nodes.


## Limitations

- the nodes are assumed to be in the same time zone
- a winter/summer time change would cause the cache to misbehave 
- the system is not bug free:
    - timeouts when getting/setting keys (broken node test)
    - not all keys are copied to their redundancy nodes (rest clients test)


# Files

- client/src: the Python client to the cache
- client/test: integration tests
- server/src: the code for a cache node
- requirements.txt: PIP requirements for the server and client (tests have
their own requirements!)
- README.md: this file
