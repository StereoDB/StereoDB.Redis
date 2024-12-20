# StereoDB.Redis

Redis server for StereoDB

## API Protocols

- Don't know.

## Commands

Basic commands like SET/GET/ECHO and some technical information.
These commands supported over terminal too.

## Perf testing

```
docker run -it --rm redis redis-benchmark -h 172.30.112.1 -n 10000 -c 100  -t "ping,incr,set,get"
```