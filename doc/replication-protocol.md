# Valkey Replication Protocol

All replication logic lives in `src/replication.c` (~5,700 lines).

## 1. Protocol Overview

Two protocols:
- **SYNC** (legacy): Always does a full resync, no offset tracking.
- **PSYNC** (modern): Supports both full and **partial** resync. Replica sends `PSYNC <replid> <offset>`.

## 2. Replica State Machine

Triggered by `REPLICAOF <host> <port>` → `replicationSetPrimary()` → `connectWithPrimary()`.

The replica steps through these states in `syncWithPrimary()`:

```
REPL_STATE_CONNECTING
  → RECEIVE_PING_REPLY
  → SEND_HANDSHAKE        (sends REPLCONF listening-port, ip-address, capa, version)
  → RECEIVE_AUTH_REPLY
  → RECEIVE_PORT_REPLY
  → RECEIVE_IP_REPLY
  → RECEIVE_CAPA_REPLY
  → RECEIVE_VERSION_REPLY
  → SEND_PSYNC
  → RECEIVE_PSYNC_REPLY
  → TRANSFER              (receiving RDB)
  → CONNECTED             (steady-state streaming)
```

## 3. PSYNC: Partial vs Full Resync

### Replica sends:
- If it has a cached previous connection: `PSYNC <old_replid> <last_offset+1>` (attempt partial resync)
- If fresh: `PSYNC ? -1` (request full resync)

### Primary decides in `primaryTryPartialResynchronization()`:

**Partial resync** succeeds only if:
1. Replica's `replid` matches `server.replid` or `server.replid2`
2. Replication backlog exists
3. Requested offset falls within the backlog range

- **Success**: Primary sends `+CONTINUE <replid>` and streams buffered data from the requested offset. No RDB needed.
- **Failure**: Falls through to full resync.

## 4. Full Resync: RDB Transfer

Primary sends `+FULLRESYNC <replid> <offset>` then starts a BGSAVE:

| Mode | Mechanism |
|------|-----------|
| **Disk-based** | `rdbSaveBackground()` writes RDB to file; `sendBulkToReplica()` streams `$<size>\r\n<data>` |
| **Diskless** | `rdbSaveToReplicasSockets()` streams RDB directly over the socket |

Replica receives the RDB:
- **Disk mode**: BIO thread writes to a temp file → main thread loads it via `rdbLoad()`
- **Memory/diskless mode**: Loads directly from socket into DB (optionally swapping with a temp DB atomically)

After RDB load, `replicaAfterLoadPrimaryRDB()` creates the `server.primary` client and transitions to `REPL_STATE_CONNECTED`.

### Diskless load modes (`repl-diskless-load` config)

| Mode | Double memory? | Serves old data during load? |
|------|---------------|------------------------------|
| `when-db-empty` (default) | No | N/A (DB was already empty) |
| `swapdb` | **Yes** — loads RDB into temp DB while old data stays live, then atomically swaps | Yes |
| `flush-before-load` | **No** — flushes existing DB before loading | No |

The `swapdb` mode passes `RDBFLAGS_EMPTY_DATA` only after the swap; `when-db-empty` and `flush-before-load` pass it before loading begins (`replication.c:2449`).

## 5. Dual-Channel Replication (Valkey-specific)

A Valkey optimization using **two TCP connections** during full resync:
- **RDB channel**: Downloads the snapshot
- **Main channel**: Accumulates the incremental stream while RDB loads (buffered in `server.pending_repl_data`)

Once RDB finishes, buffered commands are replayed, then the main channel sends a regular `PSYNC` to resume streaming. Avoids the main channel being blocked waiting for RDB download.

RDB channel state machine:
```
REPL_DUAL_CHANNEL_STATE_NONE
REPL_DUAL_CHANNEL_SEND_HANDSHAKE
REPL_DUAL_CHANNEL_RECEIVE_AUTH_REPLY
REPL_DUAL_CHANNEL_RECEIVE_REPLCONF_REPLY
REPL_DUAL_CHANNEL_RECEIVE_ENDOFF
REPL_DUAL_CHANNEL_RDB_LOAD
REPL_DUAL_CHANNEL_RDB_LOADED
```

## 6. Replication Backlog

Enables partial resync after reconnection.

**Key insight**: Replicas and the backlog share the same `server.repl_buffer_blocks` linked list — no data is duplicated. Each `replBufBlock` has a `refcount`. Blocks are freed only when `refcount == 0` (no replica or backlog references them). A slow replica implicitly extends the effective backlog.

```c
typedef struct replBufBlock {
    int refcount;          // # of replicas or backlog referencing this block
    long long id;          // unique monotonic id
    long long repl_offset; // replication offset at start of this block
    size_t size, used;
    char buf[];
} replBufBlock;

typedef struct replBacklog {
    listNode *ref_repl_buf_node; // first block the backlog holds a reference to
    size_t unindexed_count;
    rax *blocks_index;           // radix tree index for fast offset lookup
    long long histlen;           // total bytes in backlog
    long long offset;            // repl offset of first byte in backlog
} replBacklog;
```

Data flows in via `feedReplicationBuffer()` (`replication.c:449`), which:
1. Appends to tail block or creates a new one
2. Updates `primary_repl_offset` and backlog `histlen`
3. Maintains a radix tree index (`blocks_index`) for fast offset lookup during partial resync

Trimming: blocks are trimmed from the head only when `histlen > repl_backlog_size` AND the head block has `refcount == 1` (only the backlog references it, no replica does).

## 7. Steady-State Command Streaming

After initial sync:
1. Every write command → `replicationFeedReplicas()` → `feedReplicationBuffer()` → appended to shared `repl_buffer_blocks`
2. Each online replica's write handler drains its portion of the shared buffer
3. Replica applies commands via the normal `readQueryFromClient` handler — it treats the primary like a regular client

For **cascade replication** (replica-of-replica), raw bytes from the primary are forwarded verbatim via `replicationFeedStreamFromPrimaryStream()`, preserving identical offsets throughout the chain.

## 8. Replication Offset

### Primary side: `server.primary_repl_offset`

A **byte counter** of how much data has been written into the replication stream. Incremented inside `feedReplicationBuffer()` by exactly the number of bytes appended. Counts raw RESP-encoded bytes — not a command counter.

Each new `replBufBlock` records `tail->repl_offset = server.primary_repl_offset + 1` — the offset of its first byte — enabling fast seek during partial resync.

### Replica side: two offsets on `server.primary->repl_data`

| Field | Meaning |
|-------|---------|
| `read_reploff` | Bytes **read** from the socket so far |
| `reploff` | Bytes **applied** (fully processed commands) |

`read_reploff` is updated in `networking.c:3016` as bytes arrive:
```c
c->repl_data->read_reploff += c->nread;
```

`reploff` is updated after `processInputBuffer()` — after a complete command is parsed and executed (`networking.c:3771`):
```c
c->repl_data->reploff = c->repl_data->read_reploff - sdslen(c->querybuf) + c->qb_pos;
```

The gap `read_reploff - reploff` is data received but not yet fully parsed (e.g., a partially-received command).

### Heartbeat: REPLCONF ACK

Every second, the replica sends the **applied** offset to the primary (`replicationSendAck()`, `replication.c:4680`):
```
REPLCONF ACK <reploff> [FACK <aof_fsync_offset>]
```

The primary uses this to:
- Track replica lag (for `WAIT` command and `min-replicas-to-write`)
- Detect timed-out replicas (disconnects if no ACK within `repl_timeout`)
- Know when to start command streaming after diskless RDB

## 9. Reconnection

On disconnect, `replicationCachePrimary()` saves the primary client struct (preserving replid/offset) instead of freeing it. On reconnect:
- If the primary accepts `+CONTINUE` → `replicationResurrectCachedPrimary()` restores it seamlessly (partial resync)
- Otherwise → cached client is discarded, full resync begins

## 10. Flow Summary

### Full Resync (disk-based)

```
Replica                              Primary
  |                                    |
  |-- PING --------------------------> |
  |-- REPLCONF listening-port -------> |
  |-- REPLCONF capa psync2 ---------> |
  |-- PSYNC ? -1 -------------------> |
  |                                    |-- rdbSaveBackground()
  |<-- +FULLRESYNC <replid> <offset> --|
  |<-- $<size>\r\n<RDB bytes> ---------|  (sendBulkToReplica)
  |  [BIO thread saves to temp file]   |
  |  [load RDB into memory]            |
  |-- REPLCONF ACK <offset> ---------> |  (now ONLINE)
  |<-- [write commands stream] --------|
  |-- REPLCONF ACK <offset> ---------> |  (every ~1s)
```

### Partial Resync After Reconnect

```
Disconnect → replicationCachePrimary() saves state → immediate reconnect
Handshake → PSYNC <old_replid> <last_offset+1>
Primary: checks replid + backlog range → +CONTINUE
replicationResurrectCachedPrimary() → steady state resumes, no data loss
```
