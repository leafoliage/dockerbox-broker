#!/bin/sh
name=docker-broker

get_daemon_pid(){
  pgrep -fx "daemon: ${name}\[[0-9]*\]"
}

[ -z "$1" ] && exit 1

case "$1" in
  "build")
    go build -o ${name} .
    ;;
  "start")
    [ ! -f ./${name} ] && echo "Please $0 build" && exit 1
    [ -z "$(get_daemon_pid)" ] && daemon -r -t ${name} ./${name} || echo "${name} is running as pid ${pid}"
    ;;
  "stop")
    ppid=$(pgrep -fx "daemon: ${name}\[[0-9]*\]")
    pkill -9 -P ${ppid}
    kill -9 ${ppid}
    ;;
  "status")
    [ -z "$(get_daemon_pid)" ] && echo "${name} is not running" || echo "${name} is running as pid ${pid}"
    ;;
esac
