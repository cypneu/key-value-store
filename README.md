# Key-Value Store

A **Redis-compatible in-memory key-value store** built from scratch in Zig. Features master-replica replication, transactions, streams, blocking commands, and RDB persistence.

> Built as a learning project following [CodeCrafters "Build Your Own Redis"](https://codecrafters.io/challenges/redis) challenge, then extended with additional features.

## ✨ Features

| Category | Features |
|----------|----------|
| **Data Types** | Strings, Lists, Streams |
| **Persistence** | RDB snapshots (load & save) |
| **Replication** | Master-replica with PSYNC, real-time propagation |
| **Transactions** | MULTI/EXEC with command queuing |
| **Blocking** | BLPOP, XREAD with timeouts |
| **Protocol** | Full RESP2 implementation |

## 🧠 Data Structures

Each store uses purpose-built data structures optimized for its access patterns:

| Store | Data Structure | Complexity | Notes |
|-------|---------------|------------|-------|
| **StringStore** | `StringHashMap` | O(1) get/set | Lazy expiration - expired keys deleted on access |
| **ListStore** | Ring-buffer Deque | O(1) push/pop | Circular buffer with dynamic resizing, O(r) range where r = result size |
| **StreamStore** | Radix Tree | O(k) insert/lookup | Compressed trie keyed by 128-bit entry IDs, efficient range scans |

## 🚀 Quick Start

### Option 1: Native Zig (Linux only)

**Prerequisites:** [Zig 0.14+](https://ziglang.org/download/)

```bash
# Build
zig build

# Run server (default port 6379)
./zig-out/bin/main

# Run with custom port
./zig-out/bin/main --port 6380

# Run as replica
./zig-out/bin/main --port 6380 --replicaof "127.0.0.1 6379"
```

### Option 2: Docker

```bash
docker build -t zig-redis .
docker run -it -v $(pwd):/app -p 6379:6379 zig-redis bash -c "zig build && ./zig-out/bin/main"

# Run as replica (connects to master on host machine)
docker run -it -v $(pwd):/app -p 6380:6380 zig-redis bash -c "zig build && ./zig-out/bin/main --port 6380 --replicaof 'host.docker.internal 6379'"
```

### Connect with redis-cli

The server speaks the RESP2 protocol, so any standard Redis client works out of the box. The easiest way to test is with `redis-cli`:

```bash
# Install redis-cli (macOS)
brew install redis

# Connect to the running server
redis-cli -p 6379

127.0.0.1:6379> SET hello world
OK
127.0.0.1:6379> GET hello
"world"
127.0.0.1:6379> LPUSH mylist a b c
(integer) 3
127.0.0.1:6379> LRANGE mylist 0 -1
1) "c"
2) "b"
3) "a"
```

## 📋 Supported Commands

### Strings

`SET`, `GET`, `INCR`, `TYPE`, `KEYS`, `ECHO`, `PING`

### Lists

`LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `BLPOP`

### Streams

`XADD`, `XRANGE`, `XREAD`, `XLEN`, `TYPE`

### Server

`INFO`, `CONFIG GET`, `SAVE`, `WAIT`

### Transactions

`MULTI`, `EXEC`, `DISCARD`

### Replication

`REPLCONF`, `PSYNC`

## 📊 Benchmarks

Compare throughput against reference Redis using `redis-benchmark`. The script spins up both servers in Docker containers and runs identical workloads:

- Strings: SET/GET, INCR
- Lists: LPUSH/LPOP/RPUSH, LRANGE
- Streams: XADD, XRANGE, XREAD
- Concurrency: 50 parallel clients
- Pipelining: batches of 16 commands

```bash
./benchmarks/run.sh
```
