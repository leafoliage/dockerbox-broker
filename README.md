# Dockerbox-Broker

Dockerbox broker maintains port forwarding from localhost to a remote TCP Docker host for ports bound on the remote host.

## Prerequisites

A remote docker host listening on a TCP port. See [freebsd-dockerbox](https://github.com/leafoliage/freebsd-dockerbox).

## Quickstart

Install dockerbox broker.

```sh
make install
```

Configure `/usr/local/etc/dockerbox-broker/dockerbox-broker.env`.

```
DOCKER_BASE=http://10.0.0.1:2375              # Remote Docker API endpoint (forward target host is derived from this)
LOG_FILE_PATH=/var/log/dockerbox-broker.log   # Log file path
SOCKET_PATH=/var/run/dockerbox-broker.sock    # Unix socket used by the status command
MAX_RETRIES=1                                 # Consecutive connect failures before the daemon exits
KEEPALIVE_IDLE=30                             # Seconds of silence before first keepalive probe
KEEPALIVE_INTERVAL=5                          # Seconds between keepalive probes
KEEPALIVE_COUNT=1                             # Unanswered probes before the connection is considered dead
CONNECT_TIMEOUT=1                             # Docker API connect/request timeout in seconds
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
