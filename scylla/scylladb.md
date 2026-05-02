# ScyllaDB Study Notes

## What is ScyllaDB?

ScyllaDB is a high-performance, Apache Cassandra-compatible NoSQL database written in C++. It is a drop-in replacement for Cassandra, offering dramatically higher throughput and lower latency by eliminating the JVM and using a shard-per-core architecture built on the Seastar framework.

- **Data model:** Wide-column store (keyspaces → tables → rows identified by partition key + clustering columns)
- **Query language:** CQL (Cassandra Query Language) — fully compatible with Cassandra drivers
- **CAP classification:** AP system — prioritizes Availability and Partition Tolerance; consistency is tunable
- **Replication:** Leaderless, peer-to-peer (like Cassandra)
- **Written in:** C++ (Cassandra is Java; ScyllaDB avoids JVM GC pauses)

---

## Architecture

### Shard-per-Core (Seastar Framework)
- Each CPU core is assigned a fixed subset of data (a "shard") and handles its own I/O, networking, and memory
- Eliminates inter-thread locks and context switching
- Results in near-linear scalability with core count

#### How It Works

The core problem with traditional thread-per-request systems (like Cassandra): multiple threads share memory, caches, and I/O queues and need **locks** to coordinate. At high concurrency this causes lock contention, cache line bouncing, context switching overhead, and JVM GC pauses.

ScyllaDB's solution — **shared-nothing per core**:

```
Core 0 → owns Token Range [0, 25%)    → its own memory, cache, I/O queues, network queues
Core 1 → owns Token Range [25%, 50%)  → its own memory, cache, I/O queues, network queues
Core 2 → owns Token Range [50%, 75%)  → its own memory, cache, I/O queues, network queues
Core 3 → owns Token Range [75%, 100%) → its own memory, cache, I/O queues, network queues
```

- No shared mutable state between cores → **zero locks**
- Each core has its own row cache, memtable, and SSTable file handles
- NIC steers incoming packets directly to the correct core's queue via RSS (Receive Side Scaling)

#### Seastar: The Engine

Seastar replaces OS threads with:
1. **Async I/O (io_uring/aio):** I/O never blocks; it submits a request and resumes via callback
2. **Futures & Promises (continuations):** code chains `.then()` callbacks instead of blocking
3. **Per-core event loop:** each core runs its own busy-polling loop — no interrupts, no sleeping

```
Core N event loop:
  while true:
    poll network queue    → handle packets
    poll I/O completions  → resume waiting continuations
    run ready tasks
    (never sleeps, never context-switches)
```

#### Request Flow

```
Client request arrives
      │
      ▼
NIC steers packet → Core 2's network queue  (based on connection hash)
      │
      ▼
Core 2 checks: does this partition key hash to my shard?
      │
   YES → handle locally, zero cross-core communication
      │
   NO  → forward to owning core via lock-free SPSC queue
      │
      ▼
Owning core reads from its own memtable / row cache / SSTable
      │
      ▼
Response sent back
```

#### Traditional vs Shard-per-Core

| | Cassandra (thread-per-request) | ScyllaDB (shard-per-core) |
|---|---|---|
| Shared memory | Yes → locks required | No → zero locks |
| CPU cache | Bounces between cores | Each core's L1/L2 stays hot |
| Context switching | Frequent (OS scheduler) | None — one event loop per core |
| GC pauses | Yes (JVM) | None (C++) |
| I/O model | Blocking threads | Async (never blocks) |
| Throughput scaling | Sub-linear | Near-linear with core count |

#### CPU Pinning Gotcha

ScyllaDB pins each shard to a physical core at startup. In containers, it detects available cores and creates exactly that many shards. Always allocate **whole cores** — fractional CPU limits (e.g., 1.5 cores) waste a shard slot.

### Token Ring & Consistent Hashing
- Data is distributed across nodes using a token ring (same as Cassandra)
- Each partition key is hashed to a token; nodes own token ranges
- **Tablets** (introduced in ScyllaDB 6.x): a new sub-partition distribution unit replacing vnodes for more elastic and fine-grained data movement during scaling

### Vnodes vs Tablets

#### Vnodes (original mechanism)

Each physical node owns many small token ranges (virtual nodes) scattered around the ring. Typical config: 256 vnodes per node.

```
Token Ring — 3 nodes, 4 vnodes each (scattered):
  Node 1: ranges [10-30], [80-100], [140-160], [210-230]
  Node 2: ranges [30-50], [100-120], [160-180], [230-250]
  Node 3: ranges [50-80], [120-140], [180-210], [250-10]
```

Problems with vnodes:
- **Rebalancing is slow:** adding a node requires streaming from ALL existing nodes simultaneously → cluster-wide I/O storm
- **Repair is expensive:** thousands of ranges to reconcile → slow `nodetool repair`
- **Coarse granularity:** unit of movement is a whole vnode range, can't move partial ranges
- **No table-awareness:** all tables share the same token ranges regardless of their data size
- **Hotspot handling:** a hot partition is stuck in one range on one node — no relief possible

#### Tablets (ScyllaDB 6.x+)

Each **table** is independently split into many small tablets. Each tablet has its own token sub-range, memtable, and SSTables, and can be moved between nodes/shards individually.

```
Table "users" — tablet map (stored in system table):
  Tablet 1: pk hash [0,     8192)  → Node 1, Shard 2
  Tablet 2: pk hash [8192,  16384) → Node 2, Shard 0
  Tablet 3: pk hash [16384, 24576) → Node 3, Shard 1
  ...
```

How rebalancing works with tablets:
```
Add Node 4:
  Migrate Tablet 7  → Node 4  (only Node 1 streams, controlled)
  Migrate Tablet 15 → Node 4  (only Node 2 streams, controlled)
  Migrate Tablet 22 → Node 4  (only Node 3 streams, controlled)
  Each migration is independent — no cluster-wide I/O storm
```

Tablets can also be **split** (tablet grows too large) or **merged** (tablet too small), adapting to data distribution dynamically.

#### Comparison

| | Vnodes | Tablets |
|---|---|---|
| Unit of distribution | Fixed token range per node | Independent tablet per table |
| Assignment | Consistent hash (implicit, static) | System table (explicit, dynamic) |
| Rebalancing | Cluster-wide I/O storm | Incremental, one tablet at a time |
| Granularity | Coarse (whole vnode) | Fine (split/merge individual tablets) |
| Table-awareness | No — all tables share ranges | Yes — each table has its own tablet map |
| Hotspot handling | Poor — stuck in one range | Good — split hot tablet, distribute halves |
| Repair | Slow (thousands of ranges) | Faster (per-tablet) |
| Cassandra support | Yes | No — ScyllaDB only |

**Backwards compatibility:** tables created before ScyllaDB 6.x continue using vnodes; new tables default to tablets. Migration requires recreating tables.

### Replication
- Data is replicated across N nodes (Replication Factor = RF)
- Replication strategy: `SimpleStrategy` (single DC) or `NetworkTopologyStrategy` (multi-DC)
- Reads/writes are sent to multiple replicas; consistency level determines how many must respond

### Consistency Levels

A consistency level (CL) is set **per operation** — it tells the coordinator how many replicas must respond before the operation succeeds. The remaining replicas still receive the write; they just don't block the acknowledgment.

#### Single-DC Levels

| CL | Replicas required | Notes |
|---|---|---|
| `ANY` | 1 (or just a hint) | Writes only; weakest possible |
| `ONE` | Any 1 replica | Fast, stale reads possible |
| `TWO` | Any 2 replicas | Rarely used directly |
| `THREE` | Any 3 replicas | Rarely used directly |
| `QUORUM` | `floor(RF/2)+1` across all DCs | Strong consistency |
| `ALL` | Every replica | Strongest; any node down = failure |
| `SERIAL` | Quorum via Paxos | Lightweight transactions (CAS) |
| `LOCAL_SERIAL` | Local quorum via Paxos | LWT in local DC only |

#### Multi-DC Levels

| CL | Replicas required | Notes |
|---|---|---|
| `LOCAL_ONE` | 1 in local DC | Prefer over `ONE` in multi-DC |
| `LOCAL_QUORUM` | Majority in local DC only | Most common production choice |
| `EACH_QUORUM` | Majority in every DC independently | Strongest write durability; writes only |

#### With RF=3, 2 DCs (6 total replicas):
```
ONE          → 1 of 6
LOCAL_ONE    → 1 of 3 in local DC
QUORUM       → 4 of 6  (floor(6/2)+1)
LOCAL_QUORUM → 2 of 3 in local DC
EACH_QUORUM  → 2 of 3 in us-east-1 AND 2 of 3 in eu-west-1
ALL          → all 6
```

#### When to Use Each

- **`LOCAL_ONE`:** time-series, IoT, logs — throughput over consistency; stale reads acceptable
- **`LOCAL_QUORUM`:** most multi-DC apps — strong within a region, no cross-region RTT
- **`QUORUM`:** single-DC apps or when global cross-DC consistency is needed (pays cross-region latency)
- **`EACH_QUORUM`:** financial records, audit logs — every DC durably committed before ack; write fails if any DC unreachable
- **`ALL`:** admin operations, bulk backfills — never in normal traffic paths
- **`ANY`:** best-effort fire-and-forget writes; success means only a hint was stored
- **`SERIAL` / `LOCAL_SERIAL`:** compare-and-set (CAS) via Paxos — `INSERT IF NOT EXISTS`, `UPDATE IF col = val`; ~4x slower than regular writes; use sparingly

#### Strong Consistency Rule

Reads always see the latest write when: **write replicas + read replicas > RF**

```
RF=3, Write QUORUM(2) + Read QUORUM(2): 2+2=4 > 3 ✓ strong
RF=3, Write ONE(1)   + Read ONE(1):    1+1=2 < 3 ✗ may be stale
```

#### Common Production Patterns

| Use Case | Write CL | Read CL |
|---|---|---|
| Multi-DC app (most cases) | `LOCAL_QUORUM` | `LOCAL_QUORUM` |
| Time-series / IoT / logs | `LOCAL_ONE` | `LOCAL_ONE` |
| Global financial records | `EACH_QUORUM` | `LOCAL_QUORUM` |
| Unique constraint (CAS) | `LOCAL_SERIAL` | `LOCAL_SERIAL` |
| Single-DC app | `QUORUM` | `QUORUM` |

### Latency Expectations per Consistency Level

Latency = coordinator overhead + network RTT to farthest required replica + replica processing + wait for slowest required response.

#### Single-DC (same region)

| CL | p50 | p99 | Notes |
|---|---|---|---|
| `ONE` | 0.1–0.5 ms | 1–2 ms | Single replica; p99 hits slow outliers |
| `QUORUM` (RF=3) | 0.3–1 ms | 2–5 ms | Waits for 2nd of 3 — hedges against slowest |
| `ALL` (RF=3) | 1–3 ms | 5–20 ms | Waits for slowest replica |
| `SERIAL` (LWT) | 5–15 ms | 20–50 ms | 4 Paxos round trips |

#### Multi-DC (cross-region)

| CL | p50 | p99 | Notes |
|---|---|---|---|
| `LOCAL_ONE` / `LOCAL_QUORUM` | 0.1–1 ms | 2–5 ms | Same as single-DC — stays local |
| `QUORUM` / `EACH_QUORUM` | 60–160 ms | 200–400 ms | Blocked on cross-region RTT |
| `LOCAL_SERIAL` | 5–15 ms | 20–50 ms | Paxos within local DC only |
| `SERIAL` | 120–320 ms | 400–800 ms | Paxos across all DCs |

#### Inter-region RTT reference

| Pair | RTT |
|---|---|
| us-east-1 ↔ us-west-2 | ~65 ms |
| us-east-1 ↔ eu-west-1 | ~80 ms |
| us-east-1 ↔ ap-southeast-1 | ~180 ms |
| eu-west-1 ↔ ap-southeast-1 | ~170 ms |

#### Why QUORUM p99 is often lower than ONE p99

With `ONE`, the coordinator picks one replica and waits — if it's slow, the client waits the full slow time. With `QUORUM`, the coordinator sends to all 3 replicas simultaneously and uses the 2nd response, discarding the slowest. This **hedged request** effect means QUORUM eliminates tail outliers that ONE cannot. At p99 and beyond, QUORUM frequently outperforms ONE.

#### Speculative Retry

If a replica hasn't responded within a threshold, the driver sends the request to another replica without waiting for the first to fail — reduces p99 without changing CL semantics:

```cql
ALTER TABLE myapp.events
  WITH speculative_retry = '99percentile';  -- or '10ms', 'always', 'none'
```

#### Latency Budget Quick Reference

```
< 1ms    → LOCAL_ONE  (stale reads acceptable)
1–5ms    → LOCAL_QUORUM  (strong, best all-around)
5–20ms   → LOCAL_SERIAL  (CAS within DC)
60–200ms → QUORUM / EACH_QUORUM  (cross-region consistency)
Avoid    → ALL in hot paths; SERIAL cross-DC
```

---

## Multi-Region Replication

### NetworkTopologyStrategy

Multi-region deployments use `NetworkTopologyStrategy`, specifying replicas per datacenter (DC = one region):

```cql
CREATE KEYSPACE myapp
  WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'us-east-1':      3,
    'eu-west-1':      3,
    'ap-southeast-1': 3
  };
-- Every row is stored on 3 nodes in each region = 9 total replicas
```

### Topology Hierarchy

```
Cluster
  └── Datacenter (DC) — one per region
        └── Rack — one per availability zone
              └── Nodes
```

`NetworkTopologyStrategy` places replicas on **different racks within each DC** — losing one AZ never loses a replica.

### Write Path Across Regions

A coordinator node (any node the client connects to) sends the write to **all replicas in all DCs simultaneously**, but only waits for acknowledgment from the number required by the consistency level:

```
Client writes pk=alice (consistency=LOCAL_QUORUM)
        │
        ▼
Coordinator (us-east-1)
        ├──▶ Replica A (us-east-1, AZ-a) ─┐ waits for 2 of 3
        ├──▶ Replica B (us-east-1, AZ-b) ─┘ → ack to client
        ├──▶ Replica C (us-east-1, AZ-c)
        │
        ├──▶ Replica D (eu-west-1)  ─┐ fire-and-forget
        ├──▶ Replica E (eu-west-1)   │ (replicates async)
        └──▶ Replica F (eu-west-1)  ─┘
```

### Consistency Level Trade-offs

| Level | Latency | Behaviour |
|---|---|---|
| `LOCAL_ONE` | Lowest | 1 local replica — stale reads possible |
| `LOCAL_QUORUM` | Low | Majority in local DC — strongly consistent locally |
| `QUORUM` | High (cross-region RTT) | Majority across ALL DCs — globally consistent |
| `EACH_QUORUM` | High | Majority in EVERY DC independently |
| `ALL` | Highest | Every replica everywhere — any node down = failure |

**Most common production pattern:** write `LOCAL_QUORUM` + read `LOCAL_QUORUM` — strong consistency within a region, eventual consistency across regions.

### Hinted Handoff

When a replica is temporarily unreachable at write time, the coordinator stores a **hint** on its local disk and replays it to the replica when it recovers.

#### How it works

```
Write pk=alice (LOCAL_QUORUM, RF=3):
  Replica 1 ✓, Replica 2 ✓  ← quorum met, client acked
  Replica 3 ✗ (down)        ← coordinator stores hint on disk

Hint: { target: Replica 3, pk: alice, mutation: ..., timestamp: T }

Replica 3 recovers (detected via gossip in ~10–30s):
  Coordinator reads hint from disk
  Replays mutation to Replica 3 (rate-limited)
  Deletes hint on success
```

#### The hint window

Hints are kept for **3 hours by default** (`max_hint_window_in_ms`). If the node is down longer:
- Hints older than 3h are discarded
- A data gap exists on the recovered node
- `nodetool repair` is required to reconcile the missing data

#### Key properties

- Only **writes and deletes** are hinted — reads never are
- Hints are stored **per shard** (consistent with shared-nothing design)
- Hints survive coordinator restarts (stored on disk)
- If the coordinator itself is permanently lost, its hints are lost — repair covers this
- Hinted handoff is **best-effort**, not a consistency guarantee; the primary guarantee comes from RF + quorum

#### Configuration

```yaml
max_hint_window_in_ms: 10800000   # 3 hours — how long to keep hints
max_hints_file_size_in_mb: 128    # per target node per shard
hinted_handoff_enabled: true
```

#### Hinted Handoff vs Repair

| | Hinted Handoff | Repair |
|---|---|---|
| Triggered by | Missed write during outage | Manual / scheduled |
| Covers | Writes within hint window | Any divergence — missed writes, corruption |
| Speed | Fast — replays known mutations | Slow — Merkle tree comparison |
| When needed | Short outages (< hint window) | Long outages, periodic maintenance |

### Repair (Anti-Entropy)

`nodetool repair` runs Merkle tree comparisons between replicas to find and fix diverged data. Must be run per-DC in multi-region setups. Typically scheduled weekly or after node recovery.

### Anti-Entropy Repair (Deep Dive)

Repair detects and fixes replica divergence using **Merkle trees** — hash trees that fingerprint all data in a token range.

#### How Merkle trees work

```
Build tree for token range on each replica:
  hash each row → combine hashes bottom-up into a tree

Compare root hashes:
  Match → no divergence → done (zero data transferred)
  Mismatch → drill down to find exactly which rows differ

Only divergent rows are streamed — cost proportional to divergence, not total data size
```

#### Types of repair

```bash
nodetool repair mykeyspace users                          # full repair
nodetool repair --incremental mykeyspace users            # only changed SSTables (faster)
nodetool repair -dc us-east-1 mykeyspace users            # scope to one DC (avoid cross-region transfer)
nodetool repair -st 0 -et 100000 mykeyspace users         # specific token range
```

#### When to run repair

1. **After node down > 3 hours** — hint window expired, gap exists → repair fills it
2. **After replacing or bootstrapping a node** — validates streamed data is complete
3. **Scheduled weekly (minimum)** — prevents silent replica drift; must run within `gc_grace_seconds` (default 10 days)
4. **After topology changes** — adding/removing nodes
5. **Before decommissioning a node** — confirms data is on other replicas

#### The gc_grace_seconds / zombie resurrection risk

Tombstones (delete markers) are physically purged after `gc_grace_seconds` (default 10 days). If a replica missed a delete and repair does not run before the tombstone is purged:

```
t=0:    Delete pk=alice → tombstone on Replicas 1, 2; Replica 3 was down, missed it
t=10d:  Tombstone purged from Replicas 1, 2 (gc_grace_seconds passed)
t=10d+: Replica 3 recovers with pk=alice still alive
        No repair → Replica 3 streams "alive" version back → pk=alice resurrects (zombie)
```

Running repair within every gc_grace_seconds window prevents this.

#### Repair vs other consistency mechanisms

| Mechanism | Automatic? | Covers | Limitation |
|---|---|---|---|
| Replication | Yes | Normal writes | Requires replica to be up |
| Hinted handoff | Yes | Outages < 3h | Hints expire; coordinator loss = hint loss |
| Read repair | Yes (per read) | Data touched by reads | Cold data never repaired |
| Anti-entropy repair | No — must schedule | All divergence | Expensive; I/O intensive |

#### Read repair

An opportunistic lighter repair: when a QUORUM read fetches from multiple replicas and detects a mismatch, the coordinator silently pushes the correct value to the stale replica. Automatic but only covers data that is actively read. Not a substitute for full repair.

#### Cost and mitigation

Full repair is I/O and CPU intensive — it reads all SSTables to build Merkle trees. Mitigations:
- Use **incremental repair** — only examines SSTables changed since last repair
- Scope to **one DC at a time** — no cross-region traffic
- Run during **low-traffic windows**
- Use **ScyllaDB Manager** to throttle bandwidth and schedule intelligently

### Measuring Unreplicated Data

Under-replication manifests in five forms: pending hints, expired-hint gaps, replica divergence, node down (active under-replication), and streaming lag. Each has its own signal.

#### 1. Pending hints (Prometheus — primary signal)

```bash
curl -s http://<node>:9180/metrics | grep hints_manager
```

| Metric | Meaning | Healthy value |
|---|---|---|
| `scylla_hints_manager_hints_in_progress` | Writes queued, not yet on target replica | 0 |
| `scylla_hints_manager_errors` | Hints that failed to replay permanently | 0 (alert if > 0) |
| `rate(scylla_hints_manager_written[5m])` | Rate of new hint creation | Low / 0 at steady state |

#### 2. Node health (live under-replication)

```bash
nodetool status
# DN = Down Normal → all writes to its ranges are being hinted
# Any DN node = active under-replication
```

```bash
# Size of hint files on disk = volume of undelivered writes
du -sh /var/lib/scylla/hints/
ls /var/lib/scylla/hints/    # one directory per target node UUID
```

#### 3. Repair validation (divergence between replicas)

```bash
nodetool repair --validate mykeyspace users
# Compares Merkle trees without streaming — reports which token ranges have diverged replicas
```

#### 4. Streaming progress

```bash
nodetool netstats
# Shows active data streaming — data in-flight is temporarily under-replicated
```

#### 5. Consistency-level divergence test (manual canary)

Write `LOCAL_QUORUM`, read `ALL` — if `ALL` fails or times out, a replica is behind:

```python
session.execute(stmt_local_quorum, [pk, val])        # write
try:
    session.execute(stmt_all, [pk])                   # read ALL
except Unavailable as e:
    print(f"under-replicated: {e.alive_replicas}/{e.required_replicas} replicas alive")
```

#### Health checklist

| Signal | Tool | Healthy |
|---|---|---|
| Any node DN? | `nodetool status` | All UN |
| Hints pending? | Prometheus | `hints_in_progress = 0` |
| Hint errors? | Prometheus | `hints_manager_errors = 0` |
| Hint files on disk? | `du /var/lib/scylla/hints` | Empty |
| Active streaming? | `nodetool netstats` | Not sending/receiving |
| Repair run recently? | ScyllaDB Manager | Within 10 days |

#### When unreplicated data is found

| Finding | Action |
|---|---|
| `hints_in_progress` high, node UP | Node slow; check I/O/CPU; hints drain automatically |
| `hints_manager_errors > 0` | Run `nodetool repair` on affected node/table |
| Node DN < 3h | Wait for recovery; hints replay automatically |
| Node DN > 3h | On recovery: `nodetool repair -dc <dc> <keyspace>` |
| Large hint directory | Node was down a long time; repair after recovery |
| Repair validation finds mismatches | Run full repair on affected table/range |

### Active-Active Multi-Region

ScyllaDB supports **active-active** — any region can accept reads and writes simultaneously. No primary/secondary distinction.

- **Conflict resolution:** last-write-wins (LWW) by timestamp — the write with the higher timestamp survives after convergence
- **Risk:** concurrent writes to the same key from two regions → one silently wins; ensure NTP clock sync to minimise skew

### Regional Failure Handling

```
eu-west-1 goes dark (RF=3 per DC, consistency=LOCAL_QUORUM):
  us-east-1: 3 replicas intact → LOCAL_QUORUM = 2 → still serves traffic
  ap-southeast-1: same → still serves traffic
  eu-west-1 clients must failover to another region via driver config
```

### Topology-Aware Drivers

Client drivers prefer local DC nodes for coordinator selection — avoids cross-region round trips to reach a coordinator:

```python
cluster = Cluster(
    contact_points=['node1.us-east-1'],
    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='us-east-1')
)
```

### Key Design Decisions

| Decision | Recommendation |
|---|---|
| RF per DC | Always 3 — survives one node/AZ failure |
| Write consistency | `LOCAL_QUORUM` for low latency; `EACH_QUORUM` for strict per-DC guarantees |
| Read consistency | `LOCAL_QUORUM` unless cross-DC linearizability is required |
| Conflict resolution | Ensure NTP sync; avoid concurrent writes to same key if ordering matters |
| Repair cadence | Weekly minimum; always after node recovery |

### What Happens When a Node Dies

#### Detection (~0–30 seconds)

ScyllaDB uses the **phi accrual failure detector** via gossip. Each node exchanges heartbeats with random peers every second. When heartbeats stop, the detector computes a failure probability (phi). When phi exceeds the threshold, the node is marked DOWN across the cluster. Detection adapts to network jitter — nodes in slow AZs get more tolerance.

#### Impact on reads/writes (RF=3, LOCAL_QUORUM)

```
1 node down → 2 remaining → LOCAL_QUORUM (needs 2) still met → no client impact
2 nodes down → 1 remaining → cannot meet LOCAL_QUORUM → writes/reads fail
```

Coordinators immediately stop routing to the DOWN node (gossip state is shared). Writes to the dead node's token ranges are hinted on the coordinator's disk.

#### Recovery path

```
Node down < 3h:
  CommitLog replays pre-crash state on restart
  Gossip detects UP → hint replay fills missed writes
  Fully automatic — no manual steps

Node down > 3h:
  Hints for the gap period expired → data missing
  On recovery: nodetool repair -dc <dc> <keyspace>

Node permanently lost:
  scylla --replace-address <dead-node-ip>  ← new node streams data from replicas
  Run nodetool repair after bootstrap
```

#### Impact summary

| Scenario | Writes | Reads | Recovery |
|---|---|---|---|
| 1 node down, RF=3, LOCAL_QUORUM | Unaffected | Unaffected | Auto (< 3h) or repair (> 3h) |
| 1 node down, RF=3, CL=ALL | Fail for dead node's ranges | Fail | Bring node back |
| 2 nodes down, RF=3, LOCAL_QUORUM | Fail | Fail | Restore ≥1 node |
| Node permanently lost | Unaffected | Unaffected | `--replace-address` + repair |

### Active-Active Multi-Region

In active-active, every region simultaneously accepts reads and writes — no primary/secondary distinction.

```
Active-Active:                        Active-Passive:
  US clients ──▶ us-east-1  ──┐        US clients ──▶ us-east-1 (primary, writes)
                               ├──▶                        │
  EU clients ──▶ eu-west-1  ──┘        EU clients ──▶ us-east-1 (cross-region, slow)
  both regions accept writes                eu-west-1 (reads only)
```

#### Conflict Resolution: Last-Write-Wins (LWW)

Every write carries a microsecond-precision timestamp. When two regions write the same key concurrently, the higher timestamp wins after replication converges:

```
eu-west-1: pk=user:123, val="london",   timestamp=1000
us-east-1: pk=user:123, val="new york", timestamp=1001
→ both regions converge to "new york" — "london" is silently dropped
```

**Clock skew is critical:** if a node's clock is ahead, its writes always win regardless of actual order. Mitigate with NTP/PTP; some teams use hybrid logical clocks.

#### When LWW Is Safe vs Dangerous

Safe:
- Last writer really should win (e.g., user updates their profile)
- Partition keys are region-scoped (same key rarely written from two regions)
- Idempotent data (e.g., sensor readings)

Dangerous:
- Both writes must be visible (e.g., two users appending to a shared list — one append is lost)
- Counters — concurrent increments from two regions lose one increment
- Unreliable clocks in virtualised environments

#### Partition Key Design for Active-Active

Avoid conflicts by scoping partition keys to a region:

```
Good:  pk = (region, user_id) → EU writes always go to eu-west-1, no conflict
Bad:   pk = user_id           → any region can write → conflict risk
```

#### Failover

No promotion needed — other regions are already active. Failover is just a client routing change via the driver. On recovery, hinted handoff replays missed writes (within the 3-hour window); beyond that, `nodetool repair` reconciles.

#### Active-Active vs Active-Passive

| | Active-Active | Active-Passive |
|---|---|---|
| Writes | Any region | Primary only |
| Write latency | Local to user | Cross-region for remote users |
| Conflict handling | LWW — losing write silently dropped | No conflicts (single writer) |
| Failover | Reroute clients only | Promote replica to primary |
| Best for | Globally distributed, low-latency writes | Simpler apps, one dominant write region |

### Compaction
ScyllaDB writes data to immutable SSTables (Sorted String Tables). Compaction merges SSTables to reclaim space and remove tombstones (deleted data markers).

- **Size-Tiered Compaction (STCS):** Default; good for write-heavy workloads
- **Leveled Compaction (LCS):** Better for read-heavy workloads; organizes SSTables into levels
- **Incremental Compaction (ICS):** ScyllaDB-specific; reduces space amplification vs STCS
- **Time-Window Compaction (TWCS):** For time-series data; compacts within time windows

---

## Data Model

```
Keyspace (like a database/schema)
  └── Table (like an RDBMS table, but schema is denormalized)
        └── Row = Partition Key + Clustering Columns + Regular Columns
```

### Partition Key
- Determines which node(s) store the row
- All rows with the same partition key are co-located on disk (efficient range scans within a partition)
- Choose carefully — hot partitions cause uneven load

### Clustering Columns
- Define the sort order of rows within a partition
- Enable efficient range queries within a partition: `WHERE pk = X AND ck >= Y AND ck <= Z`

### Primary Key
```cql
PRIMARY KEY ((partition_key), clustering_col1, clustering_col2)
```

---

## Storage Engine: LSM Tree (CommitLog → Memtable → SSTable)

ScyllaDB uses a **Log-Structured Merge Tree (LSM)** storage engine. Writes are always sequential (never in-place updates), making them extremely fast.

### Write Path

```
Client Write
     │
     ▼
1. CommitLog  ──── appended first (durability guarantee)
     │
     ▼
2. Memtable   ──── written to in-memory sorted structure (per shard)
     │
     │  (memtable full or flush triggered)
     ▼
3. SSTable    ──── flushed to disk as immutable file
                        │
                        │  (background)
                        ▼
                   Compaction ──── merges SSTables, removes tombstones
```

### 1. CommitLog

**Purpose:** Durability — ensures no data is lost if the node crashes before the memtable is flushed.

- Every write is appended to the CommitLog **before** the memtable is updated
- Sequential append-only log — very fast (no random I/O)
- Divided into fixed-size segments (default 32 MB)
- A segment is **deleted** once all its data has been flushed to SSTables
- On crash recovery, ScyllaDB replays the CommitLog to rebuild unflushed memtable data
- Each shard has its own CommitLog segment — no cross-core coordination

```
CommitLog on disk:
  commitlog-1.log  [fully flushed → safe to delete]
  commitlog-2.log  [partially flushed → kept]
  commitlog-3.log  [current → being written]
```

### 2. Memtable

**Purpose:** In-memory write buffer. Absorbs writes so disk I/O happens in large sequential batches.

- Lives entirely in RAM (per shard, per table)
- Data stored in a **sorted structure** by partition key + clustering columns
- Reads check the memtable first — cache hit means no disk I/O
- Flushed to a new SSTable when memory threshold is exceeded, on a timer, or via `nodetool flush`
- In ScyllaDB: memtables are **off-heap** (ScyllaDB's own allocator, not OS heap) — avoids fragmentation and enables precise per-shard memory accounting

```
Memtable (RAM):
  ┌─────────────────────────────┐
  │  pk=alice  ck=2024  val=A   │  ← sorted
  │  pk=bob    ck=2023  val=B   │
  │  pk=carol  ck=2025  val=C   │
  └─────────────────────────────┘
        │  flush when full
        ▼
     SSTable on disk
```

### 3. SSTable (Sorted String Table)

**Purpose:** Immutable on-disk storage. Once written, an SSTable is never modified.

An SSTable is a **collection of files** written together:

| File | Purpose |
|------|---------|
| `Data.db` | Actual row data, sorted by partition key + clustering columns |
| `Index.db` | Partition key → byte offset in Data.db (sparse index) |
| `Summary.db` | Coarser index of Index.db — fits in memory for fast lookup |
| `Filter.db` | Bloom filter — probabilistically answers "does this partition exist?" |
| `Statistics.db` | Metadata: min/max timestamps, tombstone counts, compression info |
| `CompressionInfo.db` | Maps compressed chunks to uncompressed offsets |

#### Read Path Through an SSTable

```
Read pk=alice
     │
     ▼
1. Bloom Filter   → "definitely not here" → skip SSTable entirely
     │               "maybe here" → continue
     ▼
2. Summary.db    → find approximate position in Index.db
     │
     ▼
3. Index.db      → find exact byte offset in Data.db
     │
     ▼
4. Data.db       → read the actual row
```

### Read Path: Full Picture

```
Client Read
     │
     ├──▶ Row Cache (if enabled) ──▶ HIT → return immediately
     │
     ├──▶ Memtable ──▶ check for most recent in-memory version
     │
     └──▶ SSTables (newest to oldest)
               │
               ├── Bloom Filter → skip if "definitely not here"
               ├── Summary → Index → Data
               └── merge all versions (highest timestamp wins)
                         │
                         ▼
                    Return merged result
```

### LSM vs B-Tree Trade-offs

| | LSM (ScyllaDB) | B-Tree (MySQL/Postgres) |
|---|---|---|
| Write speed | Very fast (sequential append) | Slower (random in-place update) |
| Read speed | Slower (may check multiple SSTables) | Faster (single tree lookup) |
| Space amplification | Higher (multiple versions until compaction) | Lower |
| Write amplification | Lower initially, higher during compaction | Higher on write |
| Best for | Write-heavy workloads | Read-heavy / transactional |

---

## Bloom Filters and Row Cache

### Bloom Filter

**Purpose:** Avoid opening SSTables that don't contain a queried partition key — entirely in-memory check, zero disk I/O.

A bloom filter is a probabilistic bit array + k hash functions, built at SSTable flush time:

```
Insert "alice" into filter:
  hash1("alice")=2, hash2("alice")=5, hash3("alice")=9 → set bits 2,5,9

Query "dave":
  hash1("dave")=2 → bit=1 ✓
  hash2("dave")=4 → bit=0 ✗  → DEFINITELY NOT HERE → skip SSTable

Query "alice":
  hash1=2 ✓, hash2=5 ✓, hash3=9 ✓ → MAYBE HERE → proceed to disk lookup
```

- **False negatives: impossible** — if a key exists, its bits are always set
- **False positives: possible** — another key's bits happen to cover this key's positions → unnecessary disk read
- False positive rate tunable via `bloom_filter_fp_chance` (default ~1%):
  - Lower → larger filter in RAM → fewer false positives
  - Higher → smaller filter → more unnecessary disk reads
- `Filter.db` is loaded into memory when the SSTable is opened — the check itself is pure RAM

### Row Cache

**Purpose:** Store fully assembled, merged rows in memory so repeated reads of hot data never touch disk.

```
Read pk=alice
     │
  Row Cache HIT ───────────────────▶ return immediately (no disk I/O)
     │
  MISS
     │
     ▼
Memtable + SSTables (bloom filter → summary → index → data, merge)
     │
     ▼
Populate row cache → return to client
```

#### Row Cache vs OS Page Cache

ScyllaDB uses **Direct I/O** (bypasses OS page cache) and manages its own row cache:

| | OS Page Cache | ScyllaDB Row Cache |
|---|---|---|
| What is cached | Raw disk pages (SSTable bytes) | Fully deserialized, merged rows |
| Eviction control | Kernel (LRU-ish) | ScyllaDB — precise per-shard control |
| Shard-aware | No | Yes |
| Granularity | 4 KB pages | Whole rows |
| Decompression | Per page access | Done once, cached uncompressed |

#### Cache Invalidation

ScyllaDB uses **invalidation on write**, not write-through:
- Write to a cached partition → cache entry for that partition is **invalidated**
- Next read re-fetches from memtable/disk and repopulates the cache
- Write to an uncached partition → cache unaffected

#### When to Enable Row Cache

- **Good fit:** read-heavy workloads with hot rows (small % of rows get most reads — Zipf distribution)
- **Bad fit:** write-heavy workloads — cache invalidated constantly, wastes memory
- Configured in `scylla.yaml` via `row_cache_size_in_mb` (default: 0, disabled)
- Per-table control via CQL:

```cql
CREATE TABLE hot_data (
  id UUID PRIMARY KEY,
  val TEXT
) WITH caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'};
```

#### Bloom Filter + Row Cache Together

```
Read pk=alice
     │
     ▼
Row Cache? ── HIT ──────────────────────────────▶ done (fastest path)
     │
    MISS
     │
     ▼
For each SSTable (newest → oldest):
  Bloom Filter → "definitely not here"? → skip entirely
              → "maybe here"? → Summary → Index → Data.db
     │
     ▼
Merge memtable + SSTable results by timestamp
     │
     ▼
Populate row cache → return to client
```

- **Row cache** eliminates repeated disk reads for hot data
- **Bloom filter** eliminates unnecessary SSTable opens for cold/absent data
- They operate at different layers and complement each other

---

## CQL Basics

```cql
-- Create keyspace
CREATE KEYSPACE mykeyspace
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};

-- Create table
CREATE TABLE mykeyspace.users (
  user_id   UUID,
  username  TEXT,
  email     TEXT,
  created   TIMESTAMP,
  PRIMARY KEY (user_id)
);

-- Insert
INSERT INTO mykeyspace.users (user_id, username, email, created)
  VALUES (uuid(), 'alice', 'alice@example.com', toTimestamp(now()));

-- Read
SELECT * FROM mykeyspace.users WHERE user_id = <uuid>;

-- Update
UPDATE mykeyspace.users SET email = 'new@example.com' WHERE user_id = <uuid>;

-- Delete
DELETE FROM mykeyspace.users WHERE user_id = <uuid>;

-- TTL (auto-expire data)
INSERT INTO mykeyspace.sessions (session_id, data)
  VALUES ('abc', 'xyz') USING TTL 3600;
```

---

## RPO and RTO

**RPO (Recovery Point Objective):** how much data can be lost — measured as time (e.g., "at most 5 minutes of writes").
**RTO (Recovery Time Objective):** how long the system can be unavailable — measured as time (e.g., "back up within 1 minute").

```
─────────────────────────────────────────────────────▶ time
Last good state    Failure    Recovery complete
      │               │               │
      │←── RPO ───────│               │
                      │←─── RTO ──────│
```

### RPO by Failure Scope

| Failure | CL = LOCAL_QUORUM | CL = EACH_QUORUM | Backup only |
|---|---|---|---|
| Single node | 0 | 0 | 0 |
| Single AZ | 0 | 0 | 0 |
| Entire DC/region | Milliseconds of async lag | 0 | N/A |
| Data corruption / accidental delete | N/A — replication propagates deletes | N/A | Time since last snapshot |

### RTO by Failure Scope

| Failure | Multi-DC active-active | Single DC | Full restore from backup |
|---|---|---|---|
| Single node | ~10s (gossip detects, auto-reroute) | ~10s | Hours |
| Single AZ | ~10s | ~10s | Hours |
| Entire DC/region | Seconds–1 min (driver failover) | Hours–days | Hours–days |
| Data corruption | Hours (restore snapshot) | Hours (restore snapshot) | Hours |

### Concrete Scenarios

**Node dies (RF=3, LOCAL_QUORUM):**
- RPO = 0 — quorum writes were on ≥2 nodes before ack
- RTO = ~10s — gossip detects failure, coordinator reroutes automatically
- Action needed: none immediately; replace node at convenience

**AZ goes down (RF=3 across 3 AZs, LOCAL_QUORUM):**
- RPO = 0 — 2 surviving AZs hold every row
- RTO = ~10s — automatic
- Action needed: none; surviving AZs absorb traffic

**Region goes down (multi-DC active-active, LOCAL_QUORUM):**
- RPO = milliseconds of async replication lag
- RTO = seconds–1 minute — driver reconnects to surviving DC
- Action needed: verify driver failover config; monitor surviving DC load

**Region goes down (single-DC):**
- RPO = hours — time since last snapshot
- RTO = hours to days — restore backup to new cluster
- Action needed: restore, rebuild, redirect traffic

**Accidental TRUNCATE / corruption (any setup):**
- RPO = time since last snapshot — replication faithfully propagated the delete
- RTO = time to restore + validate
- Action needed: restore pre-corruption snapshot; reapply any writes since snapshot if available

### The Levers

| Lever | Effect |
|---|---|
| Higher RF (3 vs 1) | Tolerates more node failures at RPO=0 |
| `EACH_QUORUM` writes | RPO=0 across DCs; higher write latency |
| Multi-DC deployment | Reduces regional failure RTO from hours to seconds |
| Regular snapshots | Controls RPO for corruption/deletion; upload to S3 |
| `nodetool repair` | Prevents replica divergence from silently increasing effective RPO |
| ScyllaDB Manager | Automates scheduled backups, incremental backups, repair |

### Key Gotcha: Repair and RPO Drift

Without regular repair, replicas can silently diverge over time (missed hints, network blips). A replica that was down for 4 hours may have missed writes beyond the hint window. If it later becomes the sole survivor of other failures, the effective RPO increases even with RF=3. **Scheduled weekly repair is not optional** — it is what keeps the RPO guarantee real.

---

## Key Differences: ScyllaDB vs Cassandra

| Feature | ScyllaDB | Cassandra |
|---------|----------|-----------|
| Language | C++ | Java |
| GC pauses | None | Yes (JVM GC) |
| Architecture | Shard-per-core (Seastar) | Thread-per-request |
| Throughput | ~10x higher (benchmarked) | Baseline |
| Latency (p99) | Sub-millisecond typical | Higher, GC-impacted |
| Auto-tuning | Yes (cache, compaction, I/O) | Manual tuning needed |
| Tablets | Yes (v6+) | No (uses vnodes only) |
| CQL compatibility | Full | Native |
| Licensing | AGPL (open source) + Enterprise | Apache 2.0 |

---

## ScyllaDB vs Other Databases

| | ScyllaDB | DynamoDB | Redis | MySQL |
|---|---|---|---|---|
| Model | Wide-column | Key-value/document | In-memory KV | Relational |
| Consistency | Tunable (AP) | Tunable (eventual/strong) | Single-node strong | Strong (ACID) |
| Joins | No | No | No | Yes |
| Managed cloud | ScyllaDB Cloud | AWS native | ElastiCache | RDS/Aurora |
| Best for | High-throughput time-series, IoT, messaging | Serverless/AWS-native apps | Caching, sessions | Transactional workloads |

---

## Versions

- **ScyllaDB Open Source 6.x** (2024+): Introduced tablets for elastic scaling
- **ScyllaDB Enterprise**: Commercial version with additional features (encryption, audit logging, advanced backup)
- **ScyllaDB Cloud**: Fully managed DBaaS

---

## Common Interview / Study Questions

**Q: Why is ScyllaDB faster than Cassandra?**
ScyllaDB avoids JVM garbage collection pauses by being written in C++. Its shard-per-core architecture (via Seastar) eliminates inter-thread lock contention — each core owns its data independently, enabling near-linear throughput scaling with cores.

**Q: What is a partition key and why does it matter?**
The partition key determines which node(s) store the data via consistent hashing. A poorly chosen partition key leads to hot spots (uneven load). It should have high cardinality and distribute writes evenly.

**Q: What is a tombstone?**
A marker written on delete to signal that data is logically deleted. Physical deletion happens during compaction. Accumulating too many tombstones can degrade read performance.

**Q: What is the difference between QUORUM and LOCAL_QUORUM?**
QUORUM requires a majority across all replicas in all datacenters. LOCAL_QUORUM requires a majority only within the local datacenter — lower latency for multi-DC deployments since it avoids cross-DC round trips.

**Q: How does ScyllaDB handle schema changes?**
Schema changes are distributed across the cluster via gossip. ScyllaDB supports online schema changes (adding/dropping columns) without downtime, but operations like changing the primary key require table recreation.

**Q: What are tablets in ScyllaDB 6.x?**
Tablets are fine-grained data distribution units that replace vnodes for new tables. They allow ScyllaDB to move smaller pieces of data when adding/removing nodes, making cluster rebalancing faster and less disruptive.

**Q: What is compaction and when does it happen?**
Compaction merges multiple SSTables into fewer, larger ones. It reclaims space from deleted data (tombstones) and outdated versions of rows. It runs automatically in the background based on the compaction strategy configured per table.

**Q: Can ScyllaDB do transactions?**
Not traditional ACID transactions. It supports lightweight transactions (LWT) via Paxos for compare-and-set operations (e.g., `INSERT IF NOT EXISTS`), but these are expensive and should be used sparingly.

**Q: What is the Seastar framework?**
Seastar is an open-source C++ framework for building high-performance async I/O applications. ScyllaDB is built on it. Seastar uses a shared-nothing, continuation-based (futures/promises) programming model where each CPU core runs independently.

**Q: How does the shard-per-core architecture work?**
Each CPU core owns an exclusive token range, its own memory, row cache, memtable, SSTable handles, and I/O queues. The NIC steers packets to the correct core via RSS. Cross-core communication uses lock-free SPSC queues. Each core runs a Seastar async event loop — busy-polling with no OS context switches and no blocking I/O. This eliminates locks, keeps CPU caches hot, and scales throughput near-linearly with core count.

**Q: What are the storage engine components (CommitLog, Memtable, SSTable)?**
ScyllaDB uses an LSM tree engine. On write: data is appended to the CommitLog first (durability), then written to the in-memory Memtable (per shard, sorted). When the memtable is full it is flushed to an immutable SSTable on disk. On read: ScyllaDB checks the row cache, then memtable, then SSTables (using bloom filter → summary → index → data), merging versions by timestamp. Compaction periodically merges SSTables to reduce read amplification and reclaim space from tombstones.

**Q: How do bloom filters and row cache work?**
A bloom filter is a probabilistic in-memory bit array built at SSTable flush time. On read it answers "definitely not here" (skip the SSTable, zero disk I/O) or "maybe here" (proceed to disk). False negatives are impossible; false positives (~1% default) cause occasional unnecessary disk reads. The row cache stores fully assembled merged rows in RAM — a cache hit returns data with no disk I/O at all. ScyllaDB uses Direct I/O and bypasses the OS page cache, managing its own shard-aware row cache with precise eviction control. On write, cached entries are invalidated (not updated). Row cache suits read-heavy, hot-row workloads; bloom filters help all workloads by skipping irrelevant SSTables.

**Q: What are tablets and how do they differ from vnodes?**
Vnodes split the token ring into many small fixed ranges per node (256 by default). Adding a node requires streaming from all existing nodes simultaneously — a cluster-wide I/O storm. Tablets (ScyllaDB 6.x) replace this: each table gets its own tablet map stored in a system table. Each tablet is an independent unit (own memtable + SSTables) covering a token sub-range, and can be migrated, split, or merged individually. Rebalancing moves one tablet at a time — no cluster-wide disruption. Tablets are also table-aware (large tables get more tablets) and enable hot partition splitting by redistributing tablet halves to different nodes.

**Q: How does multi-region replication work?**
Use `NetworkTopologyStrategy` with a replica count per DC (region). Every write goes to all DCs simultaneously — the coordinator only waits for the consistency level to be met locally (e.g., `LOCAL_QUORUM`) before acking the client; remote replicas catch up asynchronously. ScyllaDB is active-active — any region accepts reads and writes; conflicts are resolved by last-write-wins (LWW) on timestamp. If a remote node is down, the coordinator stores a hint and replays it on recovery. Topology-aware drivers route to local DC coordinators to avoid cross-region round trips. Regional failures are tolerated as long as each surviving DC can meet its local quorum independently.

**Q: What is active-active multi-region?**
Active-active means every region simultaneously accepts both reads and writes — no primary/secondary distinction. Writes land locally (low latency) and replicate asynchronously to other regions. Conflicts (same key written from two regions concurrently) are resolved by last-write-wins: the write with the higher microsecond timestamp survives; the other is silently dropped. This makes clock sync (NTP/PTP) critical. Failover requires only a client routing change since all regions are already active. The main design principle: scope partition keys to a region so the same key is rarely written from two places simultaneously.

**Q: What are consistency levels and how to use each?**
A consistency level (CL) is set per operation and tells the coordinator how many replicas must respond before acking the client. Key levels: `LOCAL_ONE` (1 local replica — fastest, stale reads possible; use for logs/IoT), `LOCAL_QUORUM` (majority in local DC — most common production choice; strong locally, eventual cross-DC), `QUORUM` (majority across all DCs — global consistency but pays cross-region RTT), `EACH_QUORUM` (majority in every DC — strongest write durability; write fails if any DC unreachable), `ALL` (every replica — admin use only), `SERIAL`/`LOCAL_SERIAL` (Paxos CAS — for `IF NOT EXISTS` / `IF col=val`; ~4x slower). Strong consistency holds when write replicas + read replicas > RF (e.g., QUORUM+QUORUM with RF=3: 2+2=4>3 ✓).

**Q: What latency can we expect with different consistency levels?**
Single-DC: `ONE`/`LOCAL_ONE` ~0.1–0.5ms p50, 1–2ms p99; `QUORUM`/`LOCAL_QUORUM` ~0.3–1ms p50, 2–5ms p99; `ALL` ~1–3ms p50, 5–20ms p99; `SERIAL` ~5–15ms p50. Cross-region: `LOCAL_QUORUM` stays local (same as single-DC); `QUORUM`/`EACH_QUORUM` blocked on inter-region RTT (60–200ms typical). Counterintuitively, QUORUM p99 is often lower than ONE p99 because QUORUM sends to all 3 replicas and uses the 2nd response — hedging against slow replica outliers. Speculative retry (`WITH speculative_retry = '99percentile'`) further reduces p99 by re-issuing requests to another replica if one is slow.

**Q: What is RPO and RTO in ScyllaDB context?**
RPO (Recovery Point Objective) = how much data can be lost; RTO (Recovery Time Objective) = how long can the system be down. For node/AZ failure with RF=3 and `LOCAL_QUORUM`: RPO=0 (quorum was on ≥2 nodes), RTO=~10 seconds (gossip detection, automatic rerouting). For DC/region failure with multi-DC active-active: RPO=milliseconds of async replication lag (or 0 with `EACH_QUORUM`), RTO=seconds (driver failover). For full cluster restore from backup: RPO=time since last snapshot, RTO=hours. Accidental deletion/corruption is not protected by replication (deletes propagate to all DCs) — only snapshots help. Regular `nodetool repair` prevents replica divergence from silently increasing effective RPO.

**Q: How does hinted handoff work?**
When a write's coordinator cannot reach a replica, it stores a hint (target node + mutation + timestamp) to its local disk, acks the client once quorum is met from other replicas, then replays the hint to the recovered node when gossip signals it is back up (~10–30s). Hints are kept for 3 hours by default — outages longer than the hint window leave a data gap that requires `nodetool repair`. Only writes/deletes are hinted (never reads). Hints are stored per shard, survive coordinator restarts, but are lost if the coordinator itself is permanently destroyed. Hinted handoff is best-effort; the real consistency guarantee is RF + quorum.

**Q: What is anti-entropy repair and when to use it?**
Repair uses Merkle trees (hash trees) to compare replicas: each replica hashes its rows bottom-up into a tree; coordinators exchange root hashes, drill down on mismatches, and stream only divergent rows — cost proportional to divergence, not total data. Run repair: after a node is down > 3 hours (hint window expired), after replacing/bootstrapping a node, on a weekly schedule (must run within gc_grace_seconds = 10 days to prevent zombie resurrection of deleted rows), and after topology changes. Scope repair per-DC to avoid cross-region transfers. Use incremental repair (`--incremental`) to only examine changed SSTables. Repair is expensive (I/O and CPU intensive) — run during low-traffic windows or use ScyllaDB Manager to throttle and schedule automatically.

**Q: How to measure unreplicated data?**
Under-replication has five forms: pending hints, expired-hint gaps, replica divergence, node down, streaming lag. Primary signal: `scylla_hints_manager_hints_in_progress` (Prometheus) — should be 0; `hints_manager_errors > 0` means data permanently missed, repair needed. Check `nodetool status` for `DN` nodes (any DN = active under-replication). Measure hint file volume with `du /var/lib/scylla/hints/`. Use `nodetool repair --validate` to compare Merkle trees and identify diverged ranges without streaming. Use `nodetool netstats` for active streaming lag. Manual canary: write at `LOCAL_QUORUM`, read at `ALL` — `Unavailable` error means a replica is behind.

**Q: What happens when a node dies?**
Detection takes 10–30 seconds via the phi accrual failure detector (gossip-based). Once marked DOWN, coordinators stop routing to it and begin hinting writes to its token ranges. With RF=3 and LOCAL_QUORUM, reads and writes continue unaffected on surviving replicas — clients see no errors. If the node recovers within 3 hours: CommitLog replays pre-crash state, gossip detects UP, hints replay automatically — fully self-healing. If down > 3h: hints expired, a data gap exists — run `nodetool repair` after recovery. If permanently lost: bootstrap a replacement with `--replace-address` which streams the dead node's data from surviving replicas, then run repair to validate.

---

## Questions Asked in This Study Session

1. What is ScyllaDB and what is the latest version?
2. How does the shard-per-core architecture work?
3. What are the storage engine components (CommitLog, Memtable, SSTable)?
4. How do bloom filters and row cache work?
5. What are tablets and how do they differ from vnodes?
6. How does multi-region replication work?
7. What is active-active multi-region?
8. What are consistency levels and how to use each?
9. What latency can we expect with different consistency levels?
10. What is RPO and RTO in ScyllaDB context?
11. How does hinted handoff work?
12. What is anti-entropy repair and when to use it?
13. How to measure unreplicated data?
14. What happens when a node dies?
