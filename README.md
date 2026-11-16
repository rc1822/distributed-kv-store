# Distributed Key-Value Store

A portfolio-focused distributed systems project that demonstrates synchronous replication, durable storage, concurrent request handling, failure behavior, and automatic read fallback between a primary and backup node.

**Author:** Roman Chaidiouk

## Why this project exists

The goal is to make core distributed-systems behavior visible in a small codebase that is easy to run, test, and explain. The system uses only Python's standard library and SQLite.

## Architecture

```text
                        write: PUT / DELETE
                 +--------------------------------+
                 |                                v
+--------+    TCP/JSON    +-----------+      TCP/JSON      +----------+
| Client | -------------> |  Primary  | -----------------> |  Backup  |
+--------+                 +-----------+    synchronous     +----------+
    |                           |            replication         |
    |                           |                                |
    |                    SQLite storage                    SQLite storage
    |                                                            |
    +---------------- read fallback if primary is down ----------+
```

### Consistency model

Writes are acknowledged only after the primary receives confirmation from the backup. If replication fails, the primary rolls back its local mutation and reports `REPLICATION_FAILED`.

This prioritizes consistency over write availability when the backup is offline.

## Features

- Primary/backup architecture
- Newline-delimited JSON RPC over TCP sockets
- Multithreaded request handling
- Synchronous write replication
- Rollback when replication fails
- Durable SQLite storage on both nodes
- Idempotent replication using unique operation IDs
- Per-key locking instead of one global write lock
- Health/status requests
- Automatic client fallback to the backup for reads
- Structured errors and file logging
- Docker Compose setup
- Automated integration tests
- No external Python dependencies

## Project structure

```text
distributed-kv-store/
├── kvstore/
│   ├── common.py
│   ├── locking.py
│   ├── server.py
│   └── storage.py
├── tests/
│   └── test_integration.py
├── backup_server.py
├── client.py
├── primary_server.py
├── config.json
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## Quick start — local Python

Requires Python 3.10+.

### 1. Start the backup

```bash
python backup_server.py
```

### 2. Start the primary

In a second terminal:

```bash
python primary_server.py
```

### 3. Use the client

In a third terminal:

```bash
python client.py put city Toronto
python client.py get city
python client.py keys
python client.py delete city
```

Values can also be JSON:

```bash
python client.py put user '{"name":"Roman","active":true}'
```

On Windows PowerShell, quote JSON according to your shell's escaping rules.

## Read fallback

The client uses `--server auto` by default. If the primary is unavailable, `get`, `keys`, and `ping` automatically try the backup.

```bash
python client.py get city
```

You can target a node directly:

```bash
python client.py --server primary ping
python client.py --server backup ping
```

The backup intentionally rejects direct writes.

## Docker

Start both nodes:

```bash
docker compose up --build
```

Then use the normal local client commands because ports 5000 and 5001 are exposed to the host.

```bash
python client.py put city Toronto
python client.py get city
```

Stop the services:

```bash
docker compose down
```

Use `docker compose down -v` if you also want to remove the persistent Docker volumes.

## Run the tests

```bash
python -m unittest discover -s tests -v
```

The integration suite checks:

- replicated writes
- reads from both nodes
- replicated deletes
- backup read-only behavior
- rollback when the backup is unavailable
- backup persistence across restart
- health/status responses

## Protocol examples

Put:

```json
{"op":"put","key":"city","value":"Toronto"}
```

Get:

```json
{"op":"get","key":"city"}
```

Delete:

```json
{"op":"delete","key":"city"}
```

Health check:

```json
{"op":"ping"}
```

Successful response:

```json
{"ok":true,"value":"Toronto","served_by":"primary"}
```

Error response:

```json
{"ok":false,"error":"NOT_FOUND","message":"key 'city' does not exist"}
```

## Failure behavior

### Backup goes down

Reads from the primary continue to work. New writes fail because the system will not acknowledge a write that was not replicated. The primary restores its previous local state before returning the error.

### Primary goes down

The backup remains available for reads. The client automatically falls back to it for read-only commands.

### Process restart

Data is stored in SQLite, so restarting either process does not erase its local copy.

## Current design limits

This project deliberately keeps the architecture understandable rather than pretending to be a production database. It does not yet implement:

- automatic backup promotion for writes
- leader election or consensus
- network-partition-safe failover
- automatic state reconciliation after a node misses updates
- authentication or TLS
- sharding

Those limitations are useful discussion points because automatic failover without consensus can create split-brain behavior.

## Technical decisions worth discussing in an interview

**Why synchronous replication?**  
It prevents the primary from telling a client that a write succeeded before the backup has the same update.

**Why rollback on replication failure?**  
The primary keeps its acknowledged state aligned with the backup rather than silently accepting an unreplicated write.

**Why per-key locks?**  
Writes to different keys do not need to block one another. Operations on the same key are still serialized.

**Why operation IDs?**  
If a replication request is retried, the backup can recognize that it already applied that mutation instead of applying it twice.

**Why SQLite?**  
It provides durable local storage while keeping the project dependency-free and easy to inspect.

## License

MIT
