# Docker-Broker

Docker broker currently maintains port forwarding from localhost to remote TCP docker host for ports binded on the remote host.

## Prerequisites

A remote docker host listening on a TCP port.

## Quickstart

Configure `.env`

```
DOCKER_BASE=http://10.0.0.1:2375    # The port remote host is listening on
LOG_FILE_PATH=./docker-observe.log  # Set log file path
```

Start docker broker daemon.

```sh
./daemon.sh build
./daemon.sh start
```

Check status

```sh
./daemon.sh status
```

Stop daemon.

```sh
./daemon.sh stop
```
