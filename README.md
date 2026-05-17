# Dockerbox-Broker

Dockerbox broker currently maintains port forwarding from localhost to remote TCP docker host for ports binded on the remote host.

## Prerequisites

A remote docker host listening on a TCP port.

## Quickstart

Install dockerbox broker.

```sh
make install
```

Configure `/usr/local/etc/dockerbox-broker/dockerbox-broker.env`.

```
DOCKER_BASE=http://10.0.0.1:2375             # The port remote host is listening on
LOG_FILE_PATH=/var/log/dockerbox-broker.log  # Set log file path
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
