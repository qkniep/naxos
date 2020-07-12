# Naxos


## Requirements:

* Python >= 3.7
* miniupnpc == 2.1 [^1]

## Starting a Naxos Overlay

> :warning: **Since 2 peers is a weird corner case (and nothing interesting happens with just one peer), we suggest to construct a network of at least 3 peers.**

To start a new Naxos overlay network as the first index server:
```
python peer.py
```
To join an existing Naxos network as an index server, where `<ip>:<port>` is the address of your entry node:
```
python peer.py <ip> <port>
```

## Client Usage

To start the client type:
```
python client.py <ip>:<port> "<path to naxos directory>"
```
where `<ip>:<port>` is the address of a peer in the naxos overlay and `<path to naxos directory>` is the path to the directory where downloads should be saved and whose content should be indexed.
If you provide a `naxos.ini` file following the template below in the same directory as the script you can start the client like this:
```
python client.py
```
`naxos.ini` template:
```
[naxos]
host_ip=<ip>
host_port=<port>
naxos_directory=<path>
```

The client supports the following commands:
* `> search <filename>`: Search the index for a filename, cache the result
* `> download <filename>`: Download a file from the cached address. *Note: Due to the implementation of the client it is not trivial to automatically search for a file before downloading, so you need to search for a file, before you can download it.*
* `> overlay`: Ping into the network and construct a visualization of the overlay. Returns a shortened link.
* `> quit|exit`: Exit the client.


[^1]: Should installation via pip fail (happened to us on Windows), use the .whl from [here](https://ci.appveyor.com/project/miniupnp/miniupnp) for your environment.