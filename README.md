# Naxos


## Requirements:

* Python >= 3.7
* miniupnpc == 2.1 [^1]

## Getting Started

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


[^1]: Should installation via pip fail (happened to us on Windows), use the .whl from [here](https://ci.appveyor.com/project/miniupnp/miniupnp) for your environment.