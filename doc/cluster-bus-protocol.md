# Valkey Cluster Bus Protocol

All cluster logic lives in `src/cluster_legacy.c` (~8,384 lines), with structs and message type definitions in `src/cluster_legacy.h`.

## 1. Transport & Wire Format

All cluster communication happens over a dedicated TCP port (default: client port + 10000). Every message uses the `clusterMsg` struct with a fixed header (`cluster_legacy.h:265`):

```
sig[4] = "RCmb"   — magic bytes
type              — message type
currentEpoch      — sender's logical clock
configEpoch       — sender's config version
sender[40]        — sender's node ID
myslots[2048]     — 16384-bit slot bitmap
replicaof[40]     — primary's node ID (zeros = sender is a primary)
flags             — PFAIL, FAIL, PRIMARY, REPLICA, etc.
data              — gossip sections / fail payload / update payload
```

A lighter `clusterMsgLight` header is used for `PUBLISH` and `MODULE` messages between nodes that advertise support, stripping cluster-metadata fields to reduce overhead.

## 2. Message Types

| Type | Value | Purpose |
|------|-------|---------|
| `PING` | 0 | Heartbeat, carries gossip sections |
| `PONG` | 1 | Reply to PING |
| `MEET` | 2 | Force receiver to join the cluster |
| `FAIL` | 3 | Broadcast "this node is definitely dead" |
| `PUBLISH` | 4 | Pub/Sub channel propagation |
| `FAILOVER_AUTH_REQUEST` | 5 | Replica requests votes from primaries |
| `FAILOVER_AUTH_ACK` | 6 | Primary grants vote to a replica |
| `UPDATE` | 7 | Notify a node of newer slot config |
| `MFSTART` | 8 | Manual failover initiation |
| `MODULE` | 9 | Module cluster API message |
| `PUBLISHSHARD` | 10 | Sharded Pub/Sub propagation |

## 3. Gossip

Each PING/PONG carries gossip sections (`clusterMsgDataGossip`) — each section is 34 bytes describing a third node: its ID, last ping/pong timestamps, IP/port, and **the sender's view of its flags** (including PFAIL/FAIL).

`clusterSendPing()` (`cluster_legacy.c:4769`) selects `max(3, N * gossip_perc / 100)` random nodes as gossip candidates, then **appends all currently-PFAIL nodes** at the end (lines 4890-4908) to ensure failure information propagates quickly.

`clusterCron()` (`cluster_legacy.c:6049`) runs ~10x/second and:
- Pings the node with the oldest `pong_received` among 5 random candidates
- Pings any node not heard from in `cluster_node_timeout / 2`
- Checks for PFAIL/FAIL conditions
- Runs `clusterHandleReplicaFailover()` if this node is a replica

Optional **extension messages** (`clusterMsgPingExt`) can be appended after gossip sections in PING/PONG/MEET, carrying auxiliary data: hostname, human-readable name, shard ID, client IPs, availability zone, and forgotten-node TTLs.

## 4. Failure Detection

### Step 1: PFAIL (local suspicion)

In `clusterCron` (line 6185):
```c
node_delay = min(now - node->ping_sent, now - node->data_received);
if (node_delay > cluster_node_timeout)
    node->flags |= CLUSTER_NODE_PFAIL;
```

`data_received` is updated on **any** cluster bus data from a node — active traffic prevents false positives.

PFAIL is cleared immediately when a PONG arrives from the suspected node (line 4124).

### Step 2: FAIL (cluster-wide consensus)

When a gossip section mentions a node as PFAIL/FAIL, `clusterProcessGossipSection()` (line 2751) adds a **failure report** from the sender — but only if the sender is a **voting primary** (serves at least one slot). Replicas cannot influence quorum.

Failure reports expire after `2 * cluster_node_timeout` (`CLUSTER_FAIL_REPORT_VALIDITY_MULT = 2`).

`markNodeAsFailingIfNeeded()` (line 2567) runs the quorum check:
```c
needed_quorum = (cluster->size / 2) + 1;  // majority of voting primaries
failures = clusterNodeFailureReportsCount(node);
if (nodeIsVotingPrimary(myself)) failures++;  // count self

if (nodeTimedOut(node) && failures >= needed_quorum)
    markNodeAsFailing(node);
```

Two conditions must both hold:
1. The local node must itself have flagged the target PFAIL (cannot reach it directly)
2. A majority of voting primaries must have also reported it PFAIL/FAIL via gossip

### Step 3: FAIL broadcast

`markNodeAsFailing()` (line 2529) sets `CLUSTER_NODE_FAIL` and immediately calls `clusterSendFail()` (line 5017), broadcasting `CLUSTERMSG_TYPE_FAIL` to all connected nodes. Receivers mark the node FAIL directly — no quorum check needed on receipt since the sender already validated it.

### FAIL Reversal (`clearNodeFailureIfNeeded`, line 2595)

- **Replicas**: FAIL is cleared **immediately** when the node reconnects.
- **Voting primaries**: FAIL is only cleared after `2 * cluster_node_timeout` has elapsed AND it still owns its slots (i.e., no failover succeeded in the meantime).

## 5. Failed PRIMARY Node: Failover Election

`clusterHandleReplicaFailover()` (`cluster_legacy.c:5528`) runs every 100ms from `clusterCron`.

### Eligibility checks

- `myself` is a replica, its primary is in FAIL state
- `cluster_replica_no_failover` is not set
- Replica data is fresh enough:
  ```c
  data_age = now - last_interaction_with_primary;
  if (data_age > repl_ping_period + node_timeout * validity_factor)
      return;  // too stale, don't attempt failover
  ```

### Election delay (staggered to pick the best replica)

```c
failover_auth_time = now
    + base_delay                        // min(timeout/30, 500ms)
    + random() % base_delay             // jitter to avoid ties
    + replica_rank * (base_delay * 2)   // rank 0 = best offset = wins first
    + failed_primary_rank * base_delay  // staggers across multiple simultaneous failures
```

**Replica rank** (`clusterGetReplicaRank`, line 5348): rank 0 = highest replication offset. The most up-to-date replica fires first and is most likely to win before others even try.

**Failed primary rank** (`clusterGetFailedPrimaryRank`, line 5382): when multiple primaries fail simultaneously, staggers their replicas' elections using lexicographic shard ID comparison to reduce vote conflicts.

### Vote request

```c
server.cluster->currentEpoch++;
clusterRequestFailoverAuth();  // broadcasts FAILOVER_AUTH_REQUEST to all nodes
```

### How primaries decide to vote (`clusterSendFailoverAuthIfNeeded`, line 5234)

A primary grants a vote only if **all** hold:
1. Voter is itself a voting primary
2. Request epoch ≥ currentEpoch
3. Not already voted in this epoch (`lastVoteEpoch != currentEpoch`)
4. Requester's primary is in FAIL state (or `FORCEACK` for manual failover)
5. None of the slots the candidate claims have a higher `configEpoch` at another node (otherwise sends `UPDATE` to correct the stale candidate)

If granted: `lastVoteEpoch = currentEpoch`, sends `FAILOVER_AUTH_ACK`.

### Promotion (`clusterFailoverReplaceYourPrimary`, line 5476)

When `failover_auth_count >= (cluster_size / 2) + 1`:
1. Removes REPLICA flag, sets PRIMARY flag (`clusterSetNodeAsPrimary`)
2. Reassigns every slot from the old primary to itself
3. Broadcasts PONG to all nodes with new config
4. `verifyClusterConfigWithData()` deletes data for slots no longer owned

## 6. Failed REPLICA Node

Replicas hold no slots and have no quorum role, so their failure handling is simpler.

**Detection**: same PFAIL → FAIL path via gossip and quorum.

**FAIL clearing**: FAIL is cleared immediately when the node reconnects (no delay, unlike primaries).

### Replica Migration (`clusterHandleReplicaMigration`, line 5744)

If a primary ends up with zero live replicas (orphaned), and another primary has ≥ 2 replicas, one excess replica automatically migrates:
1. Identifies the orphaned primary with slots
2. Among replicas of the most-replicated primary, selects the candidate with the lexicographically smallest node ID
3. Waits `CLUSTER_REPLICA_MIGRATION_DELAY` (5000ms) before migrating (grace period for natural failover)
4. Calls `clusterSetPrimary()` — full sync required (different shard, different replication history)

Controlled by `cluster_migration_barrier` (minimum replicas a primary must retain before one migrates).

## 7. Cluster State

`clusterUpdateState()` (line 6498) recomputes overall health:
- **FAIL** if `cluster-require-full-coverage` is set and any slot is uncovered or owned by a FAIL node
- **FAIL** if reachable voting primaries < `(size / 2) + 1` (minority partition)
- A restarted primary waits a rejoin delay before re-entering OK state, to receive config updates first

## 8. Key Configuration Parameters

| Parameter | Default | Role |
|-----------|---------|------|
| `cluster_node_timeout` | 15000ms | Master timeout: PFAIL detection, FAIL report validity (2x), failover delays |
| `cluster_replica_validity_factor` | 10 | Max allowed data age = `repl_ping_period + timeout * factor` |
| `cluster_migration_barrier` | 1 | Min replicas a primary must retain before one migrates to an orphan |
| `cluster_replica_no_failover` | 0 | If set, replicas never auto-failover |
| `cluster_require_full_coverage` | 1 | If set, cluster enters FAIL if any slot is uncovered |
| `cluster_message_gossip_perc` | 10 | Percentage of known nodes included in each gossip packet |

## 9. Flow Summary

### Primary Failure → Failover

```
Node A unreachable for > cluster_node_timeout
         |
         v
Local node sets A → PFAIL
         |
         v
Gossip propagates A's PFAIL flag to other nodes
         |
         v
markNodeAsFailingIfNeeded():
  local PFAIL + majority of voting primaries reported PFAIL?
         |
         v (yes)
markNodeAsFailing(): A → FAIL + broadcast CLUSTERMSG_TYPE_FAIL
         |
         v
A's replica detects FAIL, starts election timer (rank-based delay)
         |
         v
Sends FAILOVER_AUTH_REQUEST with incremented epoch
         |
         v
Majority of voting primaries send FAILOVER_AUTH_ACK
         |
         v
Replica promotes itself, claims A's slots, broadcasts new config
```

### Replica Failure

```
Replica R unreachable for > cluster_node_timeout
         |
         v
PFAIL → FAIL (same gossip/quorum path as primary)
         |
         v
If primary has no other replicas → orphaned primary
         |
         v
clusterHandleReplicaMigration(): another primary's excess replica
migrates to the orphaned primary after 5s grace period
```
