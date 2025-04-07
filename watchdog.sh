#!/bin/sh

apk add --no-cache redis docker-compose bash

while true; do
  echo "$(date '+%Y-%m-%d %H:%M:%S') Running redis-cli"
  result=$(redis-cli --raw -h sentinel-1 -p 26379 SENTINEL get-master-addr-by-name eventmaster)
  echo "$(date '+%Y-%m-%d %H:%M:%S') see: $result"

  ip=$(echo "$result" | sed -n 1p)
  port=$(echo "$result" | sed -n 2p)
  echo "Master IP: $ip, Port: $port"

  if [ "$port" = "6386" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') Port is 6386. Sleeping 10s to let system recover and shutting down slave..."
    sleep 10
    docker-compose -p dds25-12 -f /root/docker-compose.yml stop event-bus-slave
  else
    echo "$(date '+%Y-%m-%d %H:%M:%S') Port is not 6386. Doing nothing."
  fi

  sleep 10
done
