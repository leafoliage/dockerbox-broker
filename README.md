# Dockerbox-Broker

Dockerbox broker currently maintains port forwarding from localhost to remote TCP docker host for ports binded on the remote host.

## Prerequisites

A remote docker host listening on a TCP port. See [freebsd-dockerbox](https://github.com/leafoliage/freebsd-dockerbox).

## Quickstart

Install dockerbox broker.

```sh
make install
```

Configure `/usr/local/etc/dockerbox-broker/dockerbox-broker.env`.

```
DOCKER_BASE=http://10.0.0.1:2375             # The port remote host is listening on
LOG_FILE_PATH=/var/log/dockerbox-broker.log  # Set log file path
MAX_RETRIES=1                                # Connect Retry
KEEPALIVE_IDLE=30                            # Connection keepalive idle timeout
KEEPALIVE_INTERVAL=5                         # Connection keepalive probe interval
KEEPALIVE_COUNT=1                            # Connection keepalive probe count
CONNECT_TIMEOUT=1
UDP_IDLE_TIMEOUT=90                          # Seconds before an idle UDP session is dropped
```

Start dockerbox broker daemon.

```sh
service dockerbox-broker start
```

Check status.

```sh
service dockerbox-broker status
```

Stop daemon.

```sh
service dockerbox-broker stop
```

## Binding to a specific host address

By default a forward listens on every local interface. The `dockerbox.bind` label picks a specific one instead:

```sh
docker run -p 8080:80 -l dockerbox.bind=192.168.88.220 nginx
```

The broker then listens on `192.168.88.220:8080` and forwards to `10.0.0.1:8080` as usual.

Publish on the wildcard inside the dockerbox and let the label choose the address here. The two are not interchangeable: `-p` names an address of the dockerbox, the label names an address of this machine. A port published on a specific address inside the dockerbox stays reachable only at that address, while the broker's dial goes to `DOCKER_BASE` — so the forward accepts your connection and then has nowhere to send it.

### Label syntax

Entries are comma-separated, and each one is an address, an address and host port, or an address and host port and protocol:

| Entry | Applies to |
| --- | --- |
| `192.168.88.220` | every published port |
| `192.168.88.220:8080` | host port 8080, either protocol |
| `10.0.0.5:53/udp` | host port 53, udp only |

The most specific entry wins, so a default and its exceptions can sit side by side:

```sh
docker run -p 80:80 -p 443:443 -p 53:53/udp \
  -l dockerbox.bind='192.168.88.220,127.0.0.1:53/udp' \
  mycontainer
```

That serves 80 and 443 on `192.168.88.220` and keeps DNS to this host's loopback. Loopback is fine in a label — only the listen side is local, and the dial still goes to the dockerbox.

An IPv6 address takes brackets when a port follows it, since `2001:db8::5:8080` is otherwise a valid address in its own right:

```
[2001:db8::5]:8080
```

A malformed entry is logged and skipped; the container's other ports are unaffected. Ports with no matching entry keep the default behaviour, so adding the label to one container changes nothing for the rest.

### Bindings published on a specific address in the dockerbox

`-p <IP>:8080:80` is honoured — the broker mirrors it and listens on `<IP>:8080` — but the dial still goes to `10.0.0.1:8080`, so it only works if the port is reachable there too, e.g. via a DNAT rule inside the dockerbox. Without one, prefer the label.

Every address is mirrored the same way, loopback included: the dial target is always `DOCKER_BASE`, never the bound address, so a listener on `127.0.0.1` forwards to the dockerbox rather than back into the broker. The one exception is `::`, which Docker reports alongside `0.0.0.0` for a wildcard publish — honouring both would have the second forwarder collide with the dual-stack listener the first one opened.

## Known Issues

* Containers see the broker's address as the source of forwarded traffic, not the original client's — the same as with Docker's own `docker-proxy`. Per-client IP allowlists or logging inside a container will not work as they would if the container ran locally.
* "host" network mode is not supported.
* Direct routing is not supported.
