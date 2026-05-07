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

### How Shards Work with Replication and Multiple Nodes

Replication and sharding are orthogonal — replication operates between nodes, sharding operates inside each node.

```
Token ring  → decides which NODES own replicas  (cluster level)
Replication → copies data across those nodes    (node level)
Sharding    → within each node, which CORE      (node-internal)
              handles the data
```

**Write path (RF=3, 3 nodes, 4 shards each):**
```
pk=alice → token 10% → primary: Node 1, replicas: Node 2, Node 3

Client → coordinator (Node 2, any shard that receives the connection)
  Coordinator sends write to all 3 nodes in parallel:
    → Node 1: network → routed to Shard that owns token 10%
    → Node 2: SPSC queue → routed to Shard that owns token 10%
    → Node 3: network → routed to Shard that owns token 10%
  Each shard: CommitLog → memtable
  Coordinator waits for CL (LOCAL_QUORUM = 2 of 3) → acks client
```

**Shard assignment is per-node, not cluster-wide:**
Nodes independently divide their token range across their shards. Different nodes can have different core counts — the same token might be Shard 10 on Node 1 (32 cores) but Shard 2 on Node 2 (16 cores). Only the token range matters for routing, not the shard number.

**Read path (LOCAL_QUORUM):**
Coordinator sends read to all RF replicas, waits for quorum responses, merges by timestamp, returns to client. If replicas disagree, coordinator pushes correct version back to stale shard (read repair).

**Cross-shard vs cross-node:**
```
Cross-shard (same node): SPSC lock-free queue → ~100ns
Cross-node (replication): TCP over NIC         → ~0.1–1ms same AZ
```

**Streaming is shard-aware:**
When a new node joins, each shard streams only its own token range data directly to the corresponding shard on the new node — no cross-shard coordination needed.

**With tablets (v6+):** the system table records exactly which node AND which shard owns each tablet. The load balancer can target a specific shard on a specific node, balancing across both dimensions independently.

| Concept | Scope | Determines |
|---|---|---|
| Token ring | Cluster | Which nodes store replicas |
| Replication Factor | Cluster | How many node copies |
| Vnode / Tablet | Node | How token ranges are assigned |
| Shard | Node-internal | Which core handles a token range |

### How Different Tables with Different Partition Keys End Up on the Same Shard

A shard owns a **token range**, not a table or a key. Every partition key from every table is hashed to a token — if two keys from different tables produce tokens that fall in the same shard's range, they both live on that shard.

```
Table: users,  pk = "alice"    → murmur3("alice")    → token 1,234,567 → Shard 2
Table: orders, pk = "order-99" → murmur3("order-99") → token 1,300,000 → Shard 2
Table: events, pk = "2024-01"  → murmur3("2024-01")  → token 1,280,000 → Shard 2
```

The token ring is cluster-level, not per-table. A shard's range applies to all tables simultaneously. Within the shard, each table is kept separate (own memtable, own SSTables):

```
Shard 2:
  ├── memtable  → users table
  ├── memtable  → orders table
  ├── SSTables/ → users/
  └── SSTables/ → orders/
```

**With vnodes:** 256 vnodes per node divided evenly among shards. For every range a shard owns, ALL tables' partitions hashing into that range live there. Hot shard problem: if two tables both have hot partitions hashing to the same shard, they compound — both burden the same shard with no relief.

**With tablets (v6+):** each table has its own independent tablet map. Same token range can map to different shards for different tables. The load balancer can move each table's tablets independently — a hot `users` tablet doesn't affect `orders` placement.

| | Vnodes | Tablets |
|---|---|---|
| Token-to-shard mapping | Global (all tables share same ring) | Per-table |
| Two tables, same token range | Always same shard | Can be different shards |
| Hot shard from multiple tables | Compounds | Independent per table |

### What is a Shard?

A shard is **one CPU core's exclusive domain** — every resource that core needs is assigned to it at startup and never shared with other cores.

```
Shard (= 1 physical CPU core)
  ├── Token range slice     — subset of the hash ring it owns
  ├── Memtable              — its own in-memory write buffer
  ├── SSTable file handles  — its own open file descriptors
  ├── Row cache             — its own cache of hot rows
  ├── CommitLog segment     — its own durability log
  ├── Network queues        — NIC steers packets here via RSS
  ├── I/O queues            — its own io_uring rings
  └── Event loop            — single-threaded, runs forever, never sleeps
```

No two shards share any of these — no locks anywhere.

**How data maps to a shard:**
Every partition key is hashed to a token (0 to 2^64). The ring is divided among nodes, then each node's range is subdivided among its shards:
```
Node 1 owns: [0, 2^64/3)
  └── Shard 0: [0,        2^64/12)
  └── Shard 1: [2^64/12,  2^64/6)
  └── Shard 2: [2^64/6,   2^64/4)
  └── Shard 3: [2^64/4,   2^64/3)
```
A write for `pk=alice` → hash → token T → maps to Node 1 → maps to Shard 2 → Shard 2 handles it entirely.

**Cross-shard hops:** if a packet lands on the wrong core (NIC steering miss), the core forwards it to the owning shard via a lock-free SPSC queue. Rare by design; drivers pin connections to minimize this.

**Shard count = physical cores seen at startup.** Always allocate whole cores — fractional CPU (e.g., 3.5 cores) wastes a shard slot.

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

#### Async I/O: io_uring and Linux AIO

**The problem with blocking I/O:**
Traditional blocking I/O requires one thread per concurrent I/O operation — each blocked thread holds ~8 MB stack and causes OS context switches. This is the core bottleneck in thread-per-request systems like Cassandra.

**Linux AIO (libaio) — older API:**
```c
io_submit(ctx, nr, iocbs);    // submit — returns immediately
io_getevents(ctx, ...);       // later: check completions
```
Limitations: only works with `O_DIRECT`, can still block on metadata operations, file I/O only.

**io_uring — modern API (Linux 5.1+, 2019):**
Two shared ring buffers (mmap'd between user space and kernel) — no syscalls needed in polling mode:
```
User space                        Kernel space
┌──────────────────┐             ┌──────────────────┐
│ Submission Queue │ ──writes──▶ │  Process SQEs    │
│ (SQ Ring)        │             │  issue to device │
└──────────────────┘             └──────────────────┘
┌──────────────────┐             ┌──────────────────┐
│ Completion Queue │ ◀──writes── │  Write CQEs on   │
│ (CQ Ring)        │             │  completion      │
└──────────────────┘             └──────────────────┘
```

| | Linux AIO | io_uring |
|---|---|---|
| Buffered I/O | No (O_DIRECT only) | Yes |
| Network I/O | No | Yes |
| Truly non-blocking | No (can block on metadata) | Yes |
| Syscall overhead | Per submission | Zero in polling mode |

**How Seastar uses it:**
Each core runs a single event loop that never blocks. io_uring is the mechanism that makes this possible:
```
Core N — Seastar event loop:
  while true:
    write pending SQEs → io_uring submission ring
    read completed CQEs → fire continuations for each
    poll NIC queues
    run ready futures/coroutines
    (never sleeps, never context-switches)
```

One core handles thousands of concurrent I/O operations. While one coroutine waits on disk, the event loop runs others:
```cpp
seastar::future<> handle_request() {
    auto data = co_await read_file(path);   // suspends — core does other work
    auto result = co_await process(data);   // resumes when I/O completes
    co_await send_response(result);
}
```

**Direct I/O (O_DIRECT):**
ScyllaDB bypasses the OS page cache entirely — unpredictable page cache eviction causes latency spikes. ScyllaDB manages its own shard-aware row cache instead. Direct I/O requires 4096-byte aligned buffers; Seastar handles this internally.

**I/O Scheduler (per shard):**
Prioritises foreground over background to prevent compaction from starving reads/writes:
```
commitlog writes  ← highest
memtable flushes  ← high
reads             ← high
compaction writes ← low
repair streaming  ← lowest
```

**I/O calibration (`scylla_io_setup` / `iotune`):**
Measures actual disk capabilities at install time (sequential throughput, random IOPS, latency) and configures queue depth and rate limits for that specific hardware.

**io_uring vs libaio in ScyllaDB:**
Seastar uses io_uring on kernels ≥ 5.1, falls back to libaio on older kernels.

**Why this eliminates the threading problem:**
```
Cassandra: 1000 concurrent reads → 1000 threads → 1000 context switches, lock contention
ScyllaDB:  1000 concurrent reads → 1 event loop → 1000 SQEs in ring
             → kernel processes, writes CQEs → continuations fire on completion
             → zero context switches, zero blocking, zero locks
```

#### How the I/O Scheduler Prioritizes Reads Over Compaction

ScyllaDB implements a **userspace fair queue** in Seastar that sits between ScyllaDB code and the NVMe device. Prioritization happens before requests reach the device — the NVMe only sees pre-ordered requests.

```
ScyllaDB code
     │
     ▼
Seastar I/O Scheduler  ← prioritization happens here (userspace)
     │
     ▼
io_uring submission ring
     │
     ▼
NVMe device (sees pre-prioritized requests)
```

**Mechanism: Weighted Fair Queue (shares-based)**

Each I/O class has a shares value. The scheduler assigns a virtual finish time (vft) to every request:
```
vft = virtual_start_time + (request_cost / class_shares)
```
Lowest vft is dispatched first.

```
I/O Class           Shares   Effect
commitlog           1000     highest priority (durability)
memtable flush       400     high
reads (queries)      200     preempts compaction
compaction           100     lowest — scheduled last
repair streaming      50     background only
```

Read vft = cost/200 (smaller) → scheduled before compaction vft = cost/100 (larger). Even with 100 pending compaction requests, a single arriving read is dispatched first.

**Capacity model (iotune)**

`scylla_io_setup` measures disk capabilities at install time (random IOPS, sequential bandwidth, optimal queue depth) and writes to `/etc/scylla.d/io.conf`. The scheduler uses these to set total I/O budget and max queue depth. Requests beyond the queue depth wait in Seastar's userspace queue — giving ScyllaDB full ordering control before anything reaches the NVMe.

**Adaptive latency targeting**

If read latency exceeds its target, the scheduler dynamically reduces compaction's shares until reads recover. When the disk is idle, compaction gets the full remaining budget — no waste.

**Compaction bandwidth cap (hard limit)**
```yaml
# scylla.yaml
compaction_throughput_mb_per_sec: 100   # 0 = unlimited
```
A ceiling on compaction I/O regardless of spare capacity. Lower in production if p99 spikes during compaction.

**Why `none` I/O scheduler for NVMe?**
OS schedulers (mq-deadline, cfq) were designed for HDDs to minimise seek time. NVMe has no rotational latency — setting `none` removes OS interference and hands full ordering control to Seastar.
```bash
echo none > /sys/block/nvme0n1/queue/scheduler
```

#### CPU Pinning Gotcha

ScyllaDB pins each shard to a physical core at startup. In containers, it detects available cores and creates exactly that many shards. Always allocate **whole cores** — fractional CPU limits (e.g., 1.5 cores) waste a shard slot.

### How the Token Range is Divided Across a Cluster

ScyllaDB uses the **Murmur3 partitioner**. Every partition key is hashed to a 64-bit signed integer:
```
Token space: -2^63  ──────────────────────────────  2^63 - 1
Total size: 2^64 values (~18.4 quintillion)
```
The space is a ring — `2^63 - 1` wraps around to `-2^63`.

**Step 1 — Division across nodes (vnodes):**

Without vnodes (simple): ring divided into N equal contiguous slices. Problem: adding a node requires streaming half of one node's data.

With vnodes (default 256 per node): ring divided into `N × 256` equally spaced slots, distributed in an interleaved (scattered) pattern:
```
3 nodes × 256 vnodes = 768 slots
Step = 2^64 / 768

Slot 0 → Node 1,  Slot 1 → Node 2,  Slot 2 → Node 3
Slot 3 → Node 1,  Slot 4 → Node 2,  Slot 5 → Node 3  ...
```
Node 1 gets 256 scattered token positions. Scattering ensures that when Node 1 fails, its ranges are spread across all other nodes — no single node absorbs all the load.

**Step 2 — Division within a node (shards):**

Within each node, every token maps to a shard via:
```
shard_id = floor((token + 2^63) × num_shards / 2^64)
```
This divides the full token space into `num_shards` equal slices. A node doesn't own all tokens in a shard's slice — only the ones its vnodes cover — but the formula applies uniformly across the full space.

**Combined formula:**
```
pk → token:   murmur3(serialized_partition_key)   range: [-2^63, 2^63)
token → node: vnode lookup (or tablet map)
token → shard: floor((token + 2^63) × num_shards / 2^64)
```

**Adding a node — vnodes vs tablets:**
```
Vnodes: new node gets 256 evenly interleaved positions
  → each existing node gives up ~1/N of its vnodes
  → all existing nodes stream simultaneously → cluster-wide I/O storm

Tablets: load balancer moves one tablet at a time
  → one source node streams one tablet → sequential, no I/O storm
```

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

### Automatic Rebalancing

#### Vnodes: semi-automatic (operator-triggered)

ScyllaDB streams data automatically when a topology change is initiated, but does **not** proactively rebalance a live cluster without a trigger:

```
nodetool addnode        → auto-streams token ranges to new node ✓
nodetool decommission   → auto-streams its data to remaining nodes ✓
Live load imbalance     → nothing automatic ✗
Hot partition           → nothing automatic ✗
```

After adding a node with vnodes, old nodes still hold data belonging to the new node — run cleanup to free it:

```bash
nodetool cleanup mykeyspace   # deletes data no longer owned by this node
```

#### Tablets: fully automatic (load balancer driven)

The **tablet load balancer** runs continuously, monitoring distribution across nodes and shards:

- **On node add:** migrates tablets one at a time to the new node — no cluster-wide I/O storm
- **Live imbalance:** automatically migrates tablets from overloaded to underloaded nodes/shards
- **Hot tablet:** auto-splits into two smaller tablets and places the halves on different nodes
- **Tiny tablets:** auto-merges adjacent small tablets after data deletion
- **Cleanup:** old tablet copies deleted automatically after migration

No `nodetool cleanup` needed. Operator involvement is minimal — add the node, the balancer does the rest.

#### Key difference

With vnodes, the operator says "go rebalance" and ScyllaDB does it. With tablets, ScyllaDB continuously maintains balance on its own — the operator just adds/removes nodes and monitors.

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

### Seed Nodes and Gossip Protocol

#### Seed Nodes

A seed node is a **well-known entry point** — a stable node whose address is hardcoded in `scylla.yaml` so new nodes have somewhere to call on first boot.

```yaml
seeds: "10.0.1.1,10.0.1.2,10.0.1.3"
```

Seeds have no elevated role in a running cluster — they are just bootstrap contacts. Existing nodes run fine even if all seeds are down. After a node joins, it gossips with random peers and never needs seeds again.

Best practice: 2–3 seeds, one per AZ, on stable long-running nodes (not spot/ephemeral). Every node should have the same seed list.

#### Gossip Protocol

Gossip is ScyllaDB's **peer-to-peer epidemic protocol** — every node continuously exchanges state with a few random peers; information spreads like a rumor.

**One gossip round (every 1 second):**
```
Node A picks 1–3 random peers

SYN:  A → B: digest of all nodes A knows { node_id, generation, max_version }
ACK:  B compares digest → sends newer state A is missing; requests state B needs
ACK2: A sends state B requested

Both nodes now have each other's latest knowledge.
```

**What gossip carries:**

| State | Content |
|---|---|
| `STATUS` | `NORMAL`, `JOINING`, `LEAVING`, `DECOMMISSIONED` |
| `TOKENS` | Token ranges this node owns |
| `SCHEMA` | Hash of schema — mismatch triggers schema sync |
| `DC` / `RACK` | Topology for NetworkTopologyStrategy |
| `LOAD` | Approximate data size |
| `RELEASE_VERSION` | ScyllaDB version |

**Heartbeat (generation + version):**
```
generation = Unix timestamp when node last started (jumps on restart)
version    = increments every second (the heartbeat tick)
```
Other nodes see a generation jump and know the node restarted vs just being slow.

**Phi Accrual Failure Detector:**
Tracks inter-arrival times of heartbeats, computes phi (probability score for "is this node dead?"):
```
phi < 8  → node UP
phi = 8  → threshold → node marked DOWN  (typically 10–30s after last heartbeat)
phi > 8  → node definitely dead
```
Adapts to network jitter — high-latency AZs get more tolerance before being declared dead.

**Convergence speed:**
```
Rounds to full convergence ≈ log₃(N)
  N=100 nodes:  ~5 seconds
  N=1000 nodes: ~7 seconds
```
O(log N) — gossip scales gracefully with cluster size.

**Bootstrap flow:**
```
New node → contacts seed → receives full cluster state
         → announces JOINING via gossip
         → existing nodes stream token range data
         → announces NORMAL via gossip
         → gossips with random peers (seeds no longer needed)
```

**Gossip vs centralised coordination:**

| | Gossip | ZooKeeper / etcd |
|---|---|---|
| SPOF | None | Coordinator quorum |
| Scalability | O(log N) | Limited by coordinator |
| Consistency | Eventual (seconds) | Strong |
| Failure detection | Built-in (phi accrual) | Requires separate heartbeat |

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

## Secondary Indexes

Without a secondary index, any query on a non-partition-key column requires a full cluster scan (`ALLOW FILTERING`) — catastrophically slow. Secondary indexes solve this without schema redesign.

### Three Options

| | Local Index | Global Index | Materialized View |
|---|---|---|---|
| Created by | `CREATE INDEX` (local) | `CREATE INDEX` (default) | `CREATE MATERIALIZED VIEW` |
| Storage location | Same node as base data | Any node (hash of indexed col) | Any node (hash of view PK) |
| Partition key required in query | Yes | No | No |
| Extra node hop on read | 0 | 1 | 0 (view is a real table) |
| Extra node hop on write | 0 | 1 | 1 |
| Consistency with base | Atomic (same shard) | Eventually consistent | Eventually consistent |

### Global Secondary Index

ScyllaDB creates a hidden MV internally. Indexed column becomes the view's partition key:

```cql
CREATE INDEX ON users(email);
-- Hidden MV: partition key = email, clustering key = user_id
-- Stored across ALL nodes based on murmur3(email)
```

**Write:** base table write + index node write (2 nodes, not atomic)
**Read:**
```
WHERE email = 'alice@example.com'
  1. Coordinator → index node (hash of email) → returns user_id
  2. Coordinator → base node (hash of user_id) → returns full row
  → 2 internal round trips (~2× latency)
```

### Local Secondary Index (ScyllaDB-specific)

Index co-located with base data — same node, same shard:

```cql
CREATE INDEX ON users((user_id), email);
-- (( )) means: index partition key = base table partition key
```

**Write:** base write + local index write on SAME shard → zero extra network hop
**Read:**
```
WHERE user_id = 'alice' AND email = 'alice@example.com'
  1. Coordinator → owning node → local index lookup → returns row
  → 1 round trip (same as base table read)
```

**Constraint:** partition key MUST be in the WHERE clause. Cannot answer "find all users with email X" cluster-wide — only "within partition X, find rows where email = Y".

### Materialized View (user-defined)

Full denormalized copy with user-controlled schema:

```cql
CREATE MATERIALIZED VIEW users_by_email AS
  SELECT user_id, email, name
  FROM users
  WHERE email IS NOT NULL AND user_id IS NOT NULL
  PRIMARY KEY (email, user_id);

-- Query like a regular table (1 hop — no base table lookup needed)
SELECT * FROM users_by_email WHERE email = 'alice@example.com';
```

MV vs global index: global index is a fixed-schema MV (indexed col + base PK only). MV is flexible — control which columns are stored, add clustering columns, filter rows in the definition.

### Write Consistency Challenge

Global index and MV writes are **not atomic** with the base table:
```
Write base table: success ✓
Write index/MV node: node down ✗
→ base and index diverge → stale index (query misses recently written row)
```
ScyllaDB retries failed MV writes via a view update backlog, but there is a window of inconsistency. Local indexes don't have this — the index write is on the same shard, handled together with the base write.

### Read/Write Path Summary

```
Global index read:  client → coordinator → index node → base node → client  (2 hops)
Local index read:   client → coordinator → owning node → client              (1 hop)
MV read:            client → coordinator → view node → client                (1 hop)

Global index write: base node + index node  (2 nodes)
Local index write:  base node only          (0 extra hops)
MV write:           base node + view node   (2 nodes)
```

### Performance Impacts and Best Practices for Secondary Indexes

#### Write Performance Impact

**Local SI** — negligible: +1 local memtable write per replica (same shard, zero network).

**Global Index / MV** — significant:
```
Per replica (RF=3): +1 local read + cross-node MV write
Table with 3 MVs:   3 base + 3 reads + up to 27 MV writes per client write
```
Never MV-index a frequently updated column:
```cql
-- catastrophic: every page load updates last_seen → read + delete + insert in MV
CREATE MATERIALIZED VIEW users_by_last_seen ...  PRIMARY KEY (last_seen, user_id)
```

#### Read Performance Impact

Selectivity determines everything for global index:
```
High cardinality (email, UUID): 2 hops → fast
Low cardinality (status='active', 80% of rows): phase 2 fans out to all nodes → O(total data)
Rule: global index column should have thousands+ of distinct values
```

Tombstone accumulation on indexed column updates: each email change leaves a tombstone in the old MV partition → degrades reads over time.

#### Storage Impact
```
Global index:   ~50 bytes/row (indexed col + base PK)      small, distributed
Local SI:       ~50 bytes/row                               tiny, co-located
MV SELECT *:    full row copy                               doubles storage
MV projected:   only selected columns                       much smaller
```
Always project only needed columns in MVs — never `SELECT *` unless required.

#### Compaction Impact

Each index/MV is an independent SSTable set with its own compaction. Table with 3 MVs + 2 local SIs = 6 concurrent compaction workloads on the same NVMe. Rule of thumb: **max 2–3 MVs per write-heavy table**.

#### MV Lag

MV writes are async — under high write load the view falls behind. Monitor:
```bash
scylla_view_update_backlog_size   # should stay near 0
```

#### Best Practices

1. **Prefer Local SI when partition key is known** — almost free, atomic, 1 hop
2. **Test selectivity before adding global index** — `SELECT COUNT(DISTINCT col) FROM table`; < ~1000 distinct values → avoid
3. **Project only needed columns in MVs** — `SELECT user_id, email, role` not `SELECT *`
4. **Filter rows in MV definition** — `WHERE is_active = true` excludes rows never queried → saves storage and write overhead
5. **Never index frequently changing columns in MVs** — email/phone (set once) good; last_seen/status/score (changes often) bad
6. **Limit MVs per table** — 1–2 MVs: fine; 3 MVs: monitor carefully; 4+: reconsider schema
7. **Prefer schema design over indexing** — a dedicated lookup table (write to both on insert) is faster and consistent vs a MV with eventual consistency gap
8. **Repair includes MVs** — `nodetool repair` on base table also repairs its MVs

#### Performance Cost Matrix

| | Local SI | Global Index | MV (projected) |
|---|---|---|---|
| Write overhead | ~0 | 1 read + cross-node write | 1 read + cross-node write |
| Read overhead | 0 extra hops | +1 hop (high selectivity) | 0 extra hops |
| Storage | Tiny, co-located | Small, distributed | Partial copy |
| Consistency | Atomic | Eventually | Eventually |
| Tombstone risk | Low | Medium | High (on updates) |
| Max per table | ~5 fine | 2–3 max | 2–3 max |

### When to Use Local SI vs Materialized View

**Decision framework — work through in order:**

1. **Do you always know the partition key at query time?**
   - YES → Local SI is viable (and preferred if it fits)
   - NO → must use MV or Global Index

2. **Does the query cross partition boundaries?**
   - Filter within one partition ("orders for customer X with status=pending") → Local SI
   - Unknown partition ("find user by email") → MV

3. **How write-heavy is the workload?**
   - Very write-heavy / latency-sensitive → Local SI (zero cross-node write overhead)
   - Moderate writes → MV acceptable

4. **Can you tolerate brief stale reads?**
   - NO → Local SI only (atomic with base)
   - YES → MV or Global Index

5. **Do you need custom view structure?**
   - Different clustering order, column projection, row filtering → MV
   - Just find rows by a column → Global Index (simpler)

**Use Local SI when:** always querying with partition key + filtering within it.
```cql
-- Orders for customer X filtered by status (customer_id always known)
CREATE INDEX ON orders((customer_id), status);
SELECT * FROM orders WHERE customer_id = X AND status = 'pending';
```
Zero write overhead, 1 hop, atomically consistent, low storage.

**Use MV when:** need to query by a column that is not the base partition key, or need a fully different access shape.
```cql
-- Login: look up user by email (don't know user_id at login time)
CREATE MATERIALIZED VIEW users_by_email AS
  SELECT user_id, email, hashed_password
  FROM users
  WHERE email IS NOT NULL AND user_id IS NOT NULL
  PRIMARY KEY (email, user_id);
-- 1 hop — MV is a real table, no extra base lookup needed

-- Order tracking: customer has tracking number, not order_id
CREATE MATERIALIZED VIEW orders_by_tracking AS
  SELECT tracking_no, customer_id, order_id, status
  FROM orders
  WHERE tracking_no IS NOT NULL AND customer_id IS NOT NULL AND order_id IS NOT NULL
  PRIMARY KEY (tracking_no, customer_id, order_id);
```
Use MV over Global Index when: need full row in 1 hop, custom clustering order, column projection, or row filtering in view definition.

**Anti-patterns:**
```
❌ MV on low-cardinality column (status, country) → fan-out on reads
❌ MV on frequently updated rows → read-before-write on every update
❌ MV with SELECT * on wide rows → doubles storage
❌ Local SI when partition key not always known at query time
❌ Local SI to search across all partitions (only works within one partition)
```

**Decision tree:**
```
Always know partition key? → YES → Local SI (filter within partition)
                          → NO  → Need full row in 1 hop?   → MV
                                   Custom clustering/filter? → MV
                                   Simple high-cardinality?  → Global Index
                                   Low cardinality column?   → Redesign schema
```

### Read Path: Scatter-Gather vs Direct Lookup

**ALLOW FILTERING (no index) — scatter-gather:**
Coordinator fans out to ALL nodes, each scans all SSTables. Cost = O(total cluster data). Never use in production on large tables.

**Global index (high selectivity) — 2-phase direct lookup:**
```
Phase 1: hash(indexed_value) → index node → returns base PK(s)
Phase 2: hash(base_pk) → base node → returns full row
Total: 2 internal round trips (~2× regular read latency)
```

**Global index (low selectivity) — 2-phase fan-out:**
Phase 1 returns millions of base PKs → Phase 2 fans out to all nodes for each → approaches scatter-gather performance. Worse than ALLOW FILTERING in some cases (random lookups vs sequential scan).

```
High selectivity (email, UUID): 1 base PK → 1 extra hop → fast
Low selectivity (country, status): millions of PKs → fan-out to all nodes → slow
Rule: avoid global index on columns with < ~1000 distinct values
```

**Global index on range query:**
Each distinct indexed value is a separate index partition on potentially different nodes. A 31-day date range = up to 31 separate index lookups → near scatter-gather. Use clustering columns for range queries, not global indexes.

**Local index — direct lookup, 1 hop:**
```
hash(partition_key) → owning node
  local index lookup + base table lookup (same shard, zero network)
Total: 1 round trip — same as regular base table read
```

**Consistency level cost:**
Global index at LOCAL_QUORUM pays LOCAL_QUORUM twice (once per phase). Local index pays it once.

**Summary:**

| Query type | Pattern | Hops | Cost |
|---|---|---|---|
| ALLOW FILTERING | Scatter-gather | N | O(total data) |
| Global index, high selectivity | 2-phase direct | 2 | O(1) |
| Global index, low selectivity | 2-phase + fan-out | 2+N | ~O(total data) |
| Global index, range query | Multi-index + fan-out | M+N | expensive |
| Local index | Direct lookup | 1 | O(1) |

### Write Path: Local SI vs MV

**Local SI write (atomic, zero extra hops):**
```
Base Replica Node (Shard 2):
  1. CommitLog append — covers base + index in one atomic append
  2. Base memtable write
  3. Local index memtable write (same shard)
  4. Ack coordinator
```

**MV / Global Index write (async, cross-node):**
```
Base Replica Node:
  1. Read existing row (local) → get old indexed column value
  2. Compute MV delta:
       DELETE old MV entry (old indexed value → base PK)
       INSERT new MV entry (new indexed value → base PK)
  3. CommitLog + memtable for base write
  4. Send MV mutations to MV replica nodes (ASYNC — different nodes)
  5. Ack coordinator (WITHOUT waiting for MV write to complete)

MV Replica Node (async):
  CommitLog + memtable for MV entry
```

**Read-before-write:** every MV write that changes an indexed column requires reading the old value to delete the old MV entry. Local SI never needs this.

**Consistency gap:** client ack is based on base CL only — MV write is async → window where index is stale. ScyllaDB retries via a per-shard **view update backlog**. Local SI has no gap — CommitLog covers both atomically.

**Write amplification (RF=3, one MV):**
```
Local SI:  3 base writes + 3 local index writes = 3 nodes, 0 extra hops
MV/Global: 3 base writes + 3 local reads + up to 3×RF cross-node MV writes
```

| | Local SI | MV / Global Index |
|---|---|---|
| Read-before-write | No | Yes (updates/deletes) |
| Index write location | Same shard | Cross-node (async) |
| CommitLog | Single atomic append | Separate appends |
| Consistency | Atomic with base | Eventually consistent |
| Client ack waits for | Base + index | Base only |
| Failure handling | N/A | View update backlog |

### When to Use Each

| Scenario | Use |
|---|---|
| Query by non-PK column, don't know partition key | Global index or MV |
| Query by non-PK column, always know partition key | Local index |
| Need full row without 2-hop read overhead | MV |
| Write-heavy, can't afford cross-node index write | Local index |
| Need filtered view (only some rows in view) | MV (supports WHERE in definition) |

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

## Production Hardware Requirements

ScyllaDB is designed to saturate hardware — it uses every CPU core, all available RAM, and pushes disk I/O to its limits. Hardware is the capacity plan: near-linear throughput scaling with cores means the spec sheet directly determines what the cluster can do.

### CPU

- **Minimum:** 4 physical cores
- **Production:** 16–64 physical cores per node (32 is the sweet spot)
- Allocate **whole cores only** — fractional CPU in containers wastes shard slots
- High clock speed matters — ScyllaDB is CPU-bound on low-latency workloads
- Hyperthreading: usable but each HT thread is half a physical core; disable for predictable latency
- **Cloud:** AWS `i3en` family (includes local NVMe), GCP `n2-highmem`, Azure `Lsv3`

### Memory

- **Minimum:** 8 GB; **Production:** 64–256 GB per node
- Rule of thumb: **2 GB per core minimum, 4–8 GB per core preferred**
- ScyllaDB allocates memory at startup (own allocator, not OS heap) — prevents GC-like pauses
- Leave 20–30% headroom for compaction spikes
- **Disable swap** — a swapped ScyllaDB node is worse than a crashed one (catastrophic latency)

### Storage

Storage is the most critical dimension.

| Type | Random IOPS | Latency | Verdict |
|---|---|---|---|
| Local NVMe SSD | 500k–1M+ | 20–100 μs | **Strongly preferred** |
| SATA SSD | 50k–100k | 100–500 μs | Acceptable |
| HDD | 100–200 | 5–10 ms | Avoid |
| Network-attached (EBS, NFS) | Variable | ms-level spikes | Avoid for data |

```
Disk layout (recommended):
  /var/lib/scylla/data      → NVMe SSD  (data directory)
  /var/lib/scylla/commitlog → separate NVMe (prevents compaction I/O competing with CommitLog)
  /var/lib/scylla/hints     → same disk as data is fine
```

**Sizing:** `disk = (data per node) × 2.5` (accounts for space amplification during compaction)

**RAID:** use RAID 0 (striping) for more IOPS across multiple NVMe drives. RAID 1/5/6 not needed — ScyllaDB replication is your redundancy, not RAID.

**Filesystem:** XFS strongly recommended (`noatime,nodiratime`). Avoid btrfs/ZFS — copy-on-write semantics conflict with ScyllaDB's I/O model.

### Network

- **Minimum:** 1 GbE; **Production:** 10 GbE; **High throughput:** 25 GbE+
- Replication bandwidth: `write throughput × (RF - 1)` — at 500 MB/s writes with RF=3, replication alone needs 1 GB/s
- In large clusters: separate NICs for client traffic vs internal cluster traffic
- Required ports: `9042` (CQL), `7000` (inter-node), `7001` (inter-node TLS), `9180` (Prometheus), `10000` (REST API)

### OS Tuning (applied by `scylla_setup`)

```bash
# CPU: performance governor (no frequency scaling)
echo performance > /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Memory: disable transparent huge pages (causes allocator latency spikes)
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag

# Swap: must be off
swapoff -a

# I/O scheduler: 'none' for NVMe (has its own queue), 'mq-deadline' for SATA SSD
echo none > /sys/block/nvme0n1/queue/scheduler

# I/O calibration (auto-tunes ScyllaDB's I/O scheduler for your specific disk)
scylla_io_setup
```

### Reference Specs

| Tier | CPU | RAM | Storage | Network |
|---|---|---|---|---|
| Minimum production | 16 cores | 64 GB | 2× NVMe 1–2 TB + separate commitlog | 10 GbE |
| High performance | 64 cores | 256–512 GB | 4× NVMe RAID 0 6–8 TB | 25 GbE |

### Why Memory Matters Even With NVMe

Even with NVMe (20–100 μs latency), RAM is ~1000x faster:

```
RAM access:   ~100 nanoseconds
NVMe access:  ~20,000–100,000 nanoseconds (20–100 μs)
```

| Memory role | What happens without enough |
|---|---|
| **Row cache** | Every read hits NVMe instead of RAM — 1000x slower per access |
| **Memtable** | Frequent small flushes → more SSTables → higher read amplification → more NVMe reads per query |
| **Bloom filters + Index/Summary** | Evicted from RAM → every read starts touching NVMe unnecessarily |
| **Compaction** | Low memory → small flushes → SSTables pile up → compaction I/O competes with reads/writes on NVMe |
| **Per-shard budget** | Each shard gets a RAM slice — too small → constant flushing even on fast NVMe |

**Mental model:** NVMe sets the *floor* (makes disk fast when you must hit it). Memory sets the *ceiling* (determines how often you hit disk at all). A well-sized cluster serves 90%+ of reads from cache — NVMe speed barely matters for those reads.

This is why ScyllaDB recommends 4–8 GB per core — it's about keeping hot data off disk entirely, not about OS overhead.

### Things That Kill ScyllaDB Performance

```
❌ Network-attached storage for data (EBS, NFS) — ms-level latency spikes
❌ Transparent huge pages enabled — allocator latency spikes
❌ Swap enabled — catastrophic latency; disable entirely
❌ CPU frequency scaling (powersave governor) — inconsistent shard timing
❌ Fractional CPU in containers — wasted shard slots
❌ Burstable cloud instances (t3/t4g) — CPU throttling destroys p99
❌ HDD storage — random IOPS ~200 vs NVMe 500k+
❌ Shared noisy-neighbour workloads on same node
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

**Q: Does ScyllaDB automatically rebalance data?**
With vnodes (pre-6.x): semi-automatic — ScyllaDB streams data automatically when a topology change is operator-triggered (`nodetool addnode`, `decommission`), but does nothing about live load imbalance or hot partitions without intervention. Must also run `nodetool cleanup` after adding nodes to free stale data from old owners. With tablets (ScyllaDB 6.x+): fully automatic — the tablet load balancer continuously monitors distribution and migrates tablets from overloaded to underloaded nodes/shards, auto-splits hot/large tablets, auto-merges small tablets, and cleans up old copies after migration. Adding a node with tablets requires no manual steps beyond initiating the join.

**Q: What are the production hardware requirements?**
ScyllaDB saturates hardware — specs directly determine capacity. CPU: 16–64 physical whole cores (32 is sweet spot); near-linear throughput scaling with cores. RAM: 64–256 GB; 2 GB/core minimum, 4–8 GB/core preferred; disable swap entirely. Storage: local NVMe SSD strongly preferred (500k+ random IOPS, 20–100μs latency) — network-attached storage (EBS/NFS) causes ms-level latency spikes that destroy p99; size at `data × 2.5` for space amplification; separate disk for CommitLog; XFS filesystem; RAID 0 for striping across multiple NVMe drives. Network: 10 GbE minimum (25 GbE for high throughput). OS tuning via `scylla_setup`: performance CPU governor, disable THP, disable swap, calibrate I/O scheduler with `scylla_io_setup`. Key killers: THP, swap, network-attached storage, fractional CPU, burstable instances, CPU frequency scaling.

---

## Recommended AWS Instance Types

ScyllaDB's AWS instance selection is driven by three hard requirements: **local NVMe storage** (no EBS), **whole physical cores**, and **no burstable CPU**. The instance family determines all three.

### Primary: i3en (ScyllaDB's top recommendation)

Intel-based, local NVMe, high memory-to-core ratio. The workhorse family for production ScyllaDB.

| Instance | vCPU | RAM | Local NVMe | Network |
|---|---|---|---|---|
| i3en.xlarge | 4 | 32 GB | 1.25 TB (1×) | Up to 25 Gbps |
| i3en.2xlarge | 8 | 64 GB | 2.5 TB (1×) | Up to 25 Gbps |
| i3en.3xlarge | 12 | 96 GB | 7.5 TB (2×) | Up to 25 Gbps |
| i3en.6xlarge | 24 | 192 GB | 15 TB (2×) | 25 Gbps |
| i3en.12xlarge | 48 | 384 GB | 30 TB (2×) | 50 Gbps |
| i3en.24xlarge | 96 | 768 GB | 60 TB (2×) | 100 Gbps |

**Sweet spot:** `i3en.6xlarge` or `i3en.12xlarge` — 24–48 vCPUs, enough RAM, two NVMe drives to separate data and CommitLog.

**Memory ratio:** i3en gives 8 GB/vCPU — exceeds the 4–8 GB/core recommendation.

### Secondary: i4i (newer generation, higher IOPS)

Intel Ice Lake with NVMe, up to 40% better price/performance than i3.

| Instance | vCPU | RAM | Local NVMe | NVMe IOPS |
|---|---|---|---|---|
| i4i.xlarge | 4 | 32 GB | 937 GB (1×) | 200k read / 100k write |
| i4i.2xlarge | 8 | 64 GB | 1.875 TB (1×) | 400k / 200k |
| i4i.4xlarge | 16 | 128 GB | 3.75 TB (1×) | 800k / 400k |
| i4i.8xlarge | 32 | 256 GB | 7.5 TB (2×) | 1.6M / 800k |
| i4i.16xlarge | 64 | 512 GB | 15 TB (4×) | 3.2M / 1.6M |
| i4i.32xlarge | 128 | 1024 GB | 30 TB (8×) | 6.4M / 3.2M |

**Choose i4i over i3en when:** you need maximum IOPS or are starting a new cluster — better hardware for the same price. The multiple NVMe drives on larger sizes allow striping or CommitLog separation.

### i3 (older, still common)

Prior generation but widely deployed. Lower memory ratio (7.6 GB/vCPU).

| Instance | vCPU | RAM | Local NVMe |
|---|---|---|---|
| i3.xlarge | 4 | 30.5 GB | 950 GB (1×) |
| i3.2xlarge | 8 | 61 GB | 1.9 TB (1×) |
| i3.4xlarge | 16 | 122 GB | 3.8 TB (2×) |
| i3.8xlarge | 32 | 244 GB | 6.4 TB (4×) |
| i3.16xlarge | 64 | 488 GB | 12.8 TB (8×) |

Still valid; upgrade path is i3 → i4i.

### is4gen / im4gn (Graviton2 — ARM, cost-optimized)

AWS Graviton2-based instances with local NVMe. 20–40% lower cost than equivalent Intel.

| Family | Focus | Trade-off |
|---|---|---|
| `is4gen` | Storage-dense (up to 30 TB NVMe) | Less RAM per vCPU |
| `im4gn` | Memory-optimised + NVMe | Balanced |

**Use if:** you have validated ARM compatibility in your driver/app stack and are cost-sensitive. ScyllaDB supports Graviton officially.

### Instance Selection Decision Tree

```
Do you need local NVMe? (Always yes for data directory)
  └── YES: use i3en / i4i / i3 / is4gen / im4gn
      │
      ├── New cluster, prefer best price/perf → i4i
      ├── Large storage (>10 TB/node)        → i3en or i3.8xlarge+
      ├── Cost optimisation, ARM OK          → is4gen / im4gn
      └── Existing i3 cluster               → stay i3 or migrate to i4i

Node size guidance:
  Dev/staging       → i3en.2xlarge  (8 vCPU, 64 GB)
  Production (min)  → i3en.3xlarge or i4i.4xlarge (12–16 vCPU)
  Production (std)  → i3en.6xlarge or i4i.8xlarge (24–32 vCPU) ← sweet spot
  High throughput   → i3en.12xlarge or i4i.16xlarge (48–64 vCPU)
```

### Instance Types to Avoid

| Instance Family | Reason |
|---|---|
| `t3`, `t4g` (burstable) | CPU throttling destroys p99 latency — hard no |
| `m5`, `m6i`, `r5`, `r6i` | No local NVMe — requires EBS; ms-level I/O spikes |
| `c5`, `c6i` (compute-optimized) | Low memory ratio; no local storage |
| Any EBS-backed instance | EBS `gp3` gives ~16k IOPS vs NVMe's 500k+ — 30× worse |

### Multi-Volume Layout on i4i.8xlarge (example)

```
i4i.8xlarge: 2× NVMe drives (7.5 TB total)

  /dev/nvme1n1  →  mount /var/lib/scylla/data        (XFS, noatime)
  /dev/nvme2n1  →  mount /var/lib/scylla/commitlog   (XFS, noatime)

No RAID needed — ScyllaDB replication is the redundancy.
Separate disks prevent compaction I/O from starving CommitLog writes.
```

### EBS: When It Is Acceptable

| Use | Acceptable? |
|---|---|
| Data directory (`/var/lib/scylla/data`) | **No** — latency spikes unacceptable |
| CommitLog (`/var/lib/scylla/commitlog`) | **No** — sequential writes hurt less but still risky |
| OS / system disk | **Yes** — not on the hot path |
| Backup staging (S3 upload buffer) | **Yes** |
| Dev/test clusters with no SLA | **Tolerable** |

`gp3` EBS can deliver up to 16,000 IOPS — compared to i4i's 800k+ IOPS this is ~50× slower at equal cost. Never use it for the data path in production.

### Summary

| Priority | Instance | Why |
|---|---|---|
| Best overall | **i4i.8xlarge** (32c/256GB) | Newest gen, highest IOPS, two NVMe drives |
| Best storage density | **i3en.12xlarge** (48c/384GB) | 30 TB NVMe, 50 Gbps |
| Budget / ARM | **im4gn.8xlarge** | Graviton2, 20–40% cheaper |
| Never | **t3/t4g or EBS-backed** | Burstable CPU or network storage |

---

## Backups

### Option 1: ScyllaDB Manager (recommended)

ScyllaDB Manager handles scheduling, parallel uploads to S3/GCS/Azure, retention, rate limiting, and incremental backups.

**Architecture:**
```
ScyllaDB Manager Server  (separate host)
        │
        │ port 5090
        ▼
Manager Agent  (runs on every ScyllaDB node)
```

**Schedule a 6-hour backup:**
```bash
# Add cluster to Manager
sctool cluster add \
  --host <any-scylla-node-ip> \
  --name mycluster

# Create a backup task on a 6-hour cron
sctool backup \
  --cluster mycluster \
  --location s3:my-bucket/scylla-backups \
  --cron "0 */6 * * *" \
  --retention 7d \
  --rate-limit 50 \        # MB/s per shard — throttle to avoid I/O competition
  --name "6h-backup"

# Check scheduled tasks
sctool task list --cluster mycluster

# Check backup status / history
sctool backup/status --cluster mycluster --show-tables
```

**Incremental backups** (only upload changed SSTables — much faster after first run):
```bash
sctool backup \
  --cluster mycluster \
  --location s3:my-bucket/scylla-backups \
  --cron "0 */6 * * *" \
  --retention 7d \
  --method incremental
```

### Option 2: nodetool snapshot + cron (no Manager)

`nodetool snapshot` creates a point-in-time snapshot using hard links — lightweight and fast. Upload to S3 separately.

```bash
# Snapshots land here (hard links to SSTable files)
/var/lib/scylla/data/<keyspace>/<table>/snapshots/<tag>/

nodetool listsnapshots          # list all snapshots
nodetool clearsnapshot -t <tag> # clear after upload — mandatory
```

**Cron script for 6-hour automated backup:**
```bash
# /etc/cron.d/scylla-backup
0 */6 * * * root /usr/local/bin/scylla-backup.sh
```

```bash
#!/bin/bash
TAG=$(date +%Y%m%d-%H%M%S)

nodetool flush                  # flush memtable to disk first
nodetool snapshot -t "$TAG"

aws s3 sync /var/lib/scylla/data/ s3://my-bucket/scylla-backups/$TAG/ \
  --exclude "*" \
  --include "*/snapshots/$TAG/*"

nodetool clearsnapshot -t "$TAG"
```

### Key Considerations

**nodetool flush before snapshot** — snapshots capture SSTables only; data still in memtable is not included. Flush ensures all writes are on disk before snapshotting.

**Snapshot cleanup is mandatory** — snapshots are hard links that prevent SSTable files from being deleted by compaction. If not cleared, NVMe fills up silently.

**Rate limit uploads** — uploading to S3 competes with compaction and replication for I/O. Use Manager's `--rate-limit` or `aws s3 sync --bandwidth` flag.

**Multi-node: every node must be backed up** — each node holds only its token ranges. ScyllaDB Manager handles this automatically; with cron you must run on every node.

### Comparison

| | ScyllaDB Manager | nodetool + cron |
|---|---|---|
| Setup complexity | Medium (separate server) | Low |
| Incremental backup | Yes | No (always full) |
| Multi-node coordination | Automatic | Manual (run on each node) |
| Rate limiting | Built-in | Manual |
| Retention management | Built-in | Manual |
| Restore tooling | Built-in (`sctool restore`) | Manual |
| Best for | Production | Dev/staging or simple setups |

### Restore

#### How Backup Works Internally (Snapshots, Hard Links, Upload)

**Step 1 — Flush memtable**
Before snapshotting, ScyllaDB flushes the memtable to disk so the snapshot captures all acknowledged writes. Data still only in RAM would be missed.

**Step 2 — Hard link creation (the snapshot)**
`nodetool snapshot` does not copy data. It creates a snapshot directory and makes hard links to every current SSTable file:

```
Before snapshot:
  data/mykeyspace/users/
    ├── ma-1-big-Data.db     (inode 1001, link count: 1)
    └── ma-1-big-Filter.db   (inode 1003, link count: 1)

After snapshot (tag=20240501):
  data/mykeyspace/users/
    ├── ma-1-big-Data.db     (inode 1001, link count: 2)  ← same inode
    └── snapshots/20240501/
          └── ma-1-big-Data.db    → inode 1001  (no data copied)
```

Hard link = new directory entry pointing to the same inode. No data is copied — snapshot creation is nearly instantaneous regardless of dataset size (a 1 TB node snapshots in milliseconds).

**Step 3 — Hard links and compaction**
When compaction merges `A + B → C` and deletes `A` and `B` from the live directory, the inodes are not freed — the snapshot's hard links still hold a reference. The OS only frees disk space when ALL hard links to an inode are removed (link count = 0).

```
t=0: snapshot created → inode 1001 link count: 2
t=1: compaction deletes live/A → link count: 1 (snapshot still holds it)
t=2: clearsnapshot → link count: 0 → OS frees disk space
```

**Disk space impact:**
```
t=0: A(100MB) + B(200MB) = 300MB
t=1: snapshot created       → still 300MB  (hard links are free)
t=2: compaction A+B→C(250MB) → 550MB  (snap pins A+B, C is new)
t=3: clearsnapshot          → 250MB   (A+B inodes released)
```
Uncleaned snapshots silently fill NVMe — `clearsnapshot` after upload is mandatory.

**Step 4 — Upload to S3**
Files are uploaded from the snapshot directory, then cleared:
```bash
aws s3 sync /var/lib/scylla/data/ s3://bucket/backups/<tag>/ \
  --include "*/snapshots/<tag>/*"
nodetool clearsnapshot -t <tag>
```
ScyllaDB Manager parallelises across nodes/shards, rate-limits bandwidth, and tracks a manifest of uploaded files for incremental backups.

**Step 5 — Incremental backups (ScyllaDB Manager)**
SSTables are immutable with unique names (generation IDs). Manager exploits this:
```
Backup run 1: SSTables A, B, C → upload all → manifest: {A, B, C}
Compaction: A+B → D; new writes: E, F
Backup run 2: snapshot has C, D, E, F
  → C in manifest → skip
  → D, E, F not in manifest → upload only these 3
  → manifest: {A, B, C, D, E, F}
```
First backup is full; every subsequent run uploads only new SSTables.

**CommitLog is NOT included**
Snapshots capture SSTables only. `nodetool flush` before snapshot ensures all acknowledged writes are in SSTables. ScyllaDB Enterprise supports CommitLog archival for true point-in-time recovery (PITR).

**Full flow:**
```
nodetool flush → memtable → SSTables on NVMe
      ↓
nodetool snapshot → hard links in snapshots/TAG/ (instantaneous)
      ↓
upload snapshots/TAG/ → S3 (parallel, rate-limited)
      ↓
nodetool clearsnapshot → link count → 0 → OS frees disk
```

#### Option 1: ScyllaDB Manager restore

```bash
# List available backups
sctool backup/list --cluster mycluster --location s3:my-bucket/scylla-backups

# Restore entire cluster from a snapshot tag
sctool restore \
  --cluster mycluster \
  --location s3:my-bucket/scylla-backups \
  --snapshot-tag sm_20240501120000UTC \
  --restore-tables

# Restore schema only (no data)
sctool restore \
  --cluster mycluster \
  --location s3:my-bucket/scylla-backups \
  --snapshot-tag sm_20240501120000UTC \
  --restore-schema

# Monitor progress
sctool restore/progress --cluster mycluster
```

Manager downloads SSTables to each node, loads them, and coordinates across the cluster automatically.

#### Option 2: Manual restore via sstableloader

`sstableloader` streams SSTables into a live cluster respecting the current token ring — preferred over raw file copy because it works even if the new cluster has different topology/node count.

**Full cluster restore:**
```bash
# 1. Bootstrap new cluster with same schema
# 2. Download snapshot from S3
aws s3 sync s3://my-bucket/scylla-backups/<tag>/ /var/lib/scylla/data/

# 3. Stream into cluster (run per table)
sstableloader \
  -d <any-node-ip> \
  /var/lib/scylla/data/<keyspace>/<table>/

# 4. Validate
nodetool repair mykeyspace
```

**Single table restore (accidental TRUNCATE / DROP — cluster stays up):**
```bash
# 1. Recreate table schema if dropped
cqlsh -e "CREATE TABLE mykeyspace.users (...)"

# 2. Download just that table's snapshot
aws s3 sync \
  s3://my-bucket/scylla-backups/<tag>/mykeyspace/users/ \
  /tmp/restore/mykeyspace/users/

# 3. Stream into live cluster — no downtime
sstableloader -d <node-ip> /tmp/restore/mykeyspace/users/
```

#### Restore Scenarios

| Scenario | Approach | Downtime? |
|---|---|---|
| Accidental TRUNCATE / DROP TABLE | sstableloader into live cluster | None |
| Single node lost (RF=3) | No restore needed — replication covers it | None |
| Full cluster loss | Bootstrap new cluster + sstableloader from S3 | Yes |
| Corruption on one node | `--replace-address` first, then repair | Minimal |
| Point-in-time restore | Restore schema + sstableloader from that tag | Depends on scope |

#### Gotchas

**Tombstones win over restored data** — if a row was deleted after the snapshot, the tombstone on other replicas still wins after read repair/compaction. Restore quickly, before `gc_grace_seconds` (10 days) expires and tombstones are purged — otherwise the deleted row resurrects permanently.

**Schema must exist before data** — `sstableloader` requires the keyspace and table to already exist. Restore schema first, then data.

**Clock skew on restored data** — restored SSTables carry original write timestamps. Live writes after the snapshot time will not be overwritten by the restore (LWW — higher timestamp wins).

**Verify after restore:**
```bash
nodetool repair mykeyspace users   # reconcile divergence
nodetool verify mykeyspace users   # checksum validation of SSTable files
```

---

## Monitoring: VictoriaMetrics Integration

ScyllaDB exposes Prometheus-format metrics on port `9180`. VictoriaMetrics scrapes any Prometheus-compatible endpoint — so the integration works via the standard Prometheus scrape protocol with no custom plugins needed.

### Setup Options

**Option 1: vmagent scrapes ScyllaDB directly (fewest moving parts)**
```yaml
scrape_configs:
  - job_name: 'scylladb'
    static_configs:
      - targets:
          - 'node1:9180'
          - 'node2:9180'
          - 'node3:9180'
```
`vmagent` scrapes → sends to VictoriaMetrics storage.

**Option 2: Prometheus scrapes ScyllaDB → remote_write to VictoriaMetrics**
```yaml
remote_write:
  - url: http://victoria-metrics:8428/api/v1/write
```
More components but familiar if Prometheus is already in the stack.

### Official ScyllaDB Monitoring Stack

ScyllaDB ships `scylla-monitoring` (GitHub) with pre-built Prometheus scrape configs and Grafana dashboards covering per-shard CPU, latency percentiles, compaction, cache hit rate, and hints.

VictoriaMetrics can replace Prometheus in this stack — it exposes a Prometheus-compatible query API so existing Grafana dashboards work unchanged. VictoriaMetrics' MetricsQL is a superset of PromQL.

### Key Metrics to Watch

| Metric | What it tells you |
|---|---|
| `scylla_reactor_utilization` | CPU utilization per shard |
| `scylla_storage_proxy_coordinator_write_latency` | Write latency (p50/p99) |
| `scylla_storage_proxy_coordinator_read_latency` | Read latency (p50/p99) |
| `scylla_hints_manager_hints_in_progress` | Under-replication signal |
| `scylla_cache_row_hits` / `scylla_cache_row_misses` | Row cache hit rate |
| `scylla_compaction_manager_compactions` | Active compaction count |
| `scylla_transport_requests_served` | CQL request throughput |

### Caveats

- ScyllaDB's official alerting rules are written for Prometheus — minor syntax adjustments may be needed for MetricsQL (mostly compatible but not identical)
- Test alert rules before relying on them in production when switching from Prometheus to VictoriaMetrics

### Collectors and Exporters

#### Built-in (no separate exporter needed)

ScyllaDB ships with a built-in Prometheus exporter — no sidecar, no agent:

```
http://<node>:9180/metrics   ← ready out of the box
```

Exposes thousands of metrics across CPU (per shard), latency, cache, compaction, hints, CQL transport, storage, repair, and streaming.

#### Official ScyllaDB Tools

| Tool | Port | Purpose |
|---|---|---|
| Built-in Prometheus endpoint | `9180` | All database internals — primary metrics source |
| `scylla-jmx` | `7199` | JMX proxy — translates JMX calls to REST API; enables `nodetool` and Cassandra-era tooling |
| ScyllaDB Manager Agent | `5090` | Backup, repair scheduling, health reporting; exposes its own metrics |
| `scylla-monitoring` stack | — | Pre-wired Prometheus + Grafana + Alertmanager stack (GitHub: `scylladb/scylla-monitoring`) |

#### OS-level (deploy alongside ScyllaDB)

| Exporter | Port | What it adds |
|---|---|---|
| `node_exporter` (Prometheus) | `9100` | CPU, memory, disk I/O, network — host-level metrics not covered by `:9180` |

Essential for full observability — `:9180` covers database internals; `node_exporter` covers the host.

#### Ecosystem Scrapers (all scrape `:9180`)

| Tool | How |
|---|---|
| **vmagent** (VictoriaMetrics) | Prometheus scrape_config → VictoriaMetrics storage |
| **Grafana Agent / Alloy** | Prometheus scrape → Grafana Cloud or any remote_write target |
| **OpenTelemetry Collector** | `prometheusreceiver` scrapes `:9180` → any OTLP backend |
| **Telegraf** (InfluxData) | `prometheus` input plugin → InfluxDB or any output |
| **Datadog Agent** | Built-in ScyllaDB integration — scrapes `:9180`, maps to Datadog metrics |

#### Metric Categories from `:9180`

| Category | Example metrics |
|---|---|
| CPU (per shard) | `scylla_reactor_utilization` |
| Write latency | `scylla_storage_proxy_coordinator_write_latency` |
| Read latency | `scylla_storage_proxy_coordinator_read_latency` |
| Row cache | `scylla_cache_row_hits`, `scylla_cache_row_misses` |
| Compaction | `scylla_compaction_manager_compactions` |
| Hints | `scylla_hints_manager_hints_in_progress` |
| CQL transport | `scylla_transport_requests_served` |
| Repair | `scylla_repair_*` |
| Streaming | `scylla_streaming_*` |
| SSTables | `scylla_sstables_*` |

---

## Ports Reference

### Client-facing (app servers → ScyllaDB nodes)

| Port | Protocol | Purpose |
|---|---|---|
| `9042` | TCP | CQL — all application traffic |
| `9142` | TCP | CQL over TLS — use instead of 9042 when encryption in transit is required |
| `9160` | TCP | Thrift — legacy Cassandra protocol; deprecated, avoid in new deployments |

Open `9042` (or `9142`) from your application tier only — never to the public internet.

### Inter-node (ScyllaDB nodes ↔ ScyllaDB nodes)

| Port | Protocol | Purpose |
|---|---|---|
| `7000` | TCP | Gossip, streaming (repair, bootstrap, rebalancing), inter-node RPC |
| `7001` | TCP | Same as 7000 but TLS-encrypted |

Must be open between all nodes in the cluster, including across AZs and regions in multi-DC setups. If 7000 is blocked between DCs, replication silently stops.

### Monitoring & Management

| Port | Protocol | Purpose |
|---|---|---|
| `9180` | TCP | Prometheus metrics scrape endpoint |
| `10000` | TCP | ScyllaDB REST API — used by `nodetool`, ScyllaDB Manager, admin operations |

`10000` should only be open to your ops/monitoring subnet — it exposes cluster management operations.

### ScyllaDB Manager (if deployed)

| Port | Protocol | Purpose |
|---|---|---|
| `5090` | TCP | Manager agent (runs on each ScyllaDB node, talks to Manager server) |
| `5112` | TCP | Manager server HTTP API |

### AWS Security Group Summary

```
App servers      → ScyllaDB nodes:   9042 (or 9142 for TLS)
ScyllaDB nodes  ↔ each other:        7000, 7001
Monitoring server → nodes:           9180, 10000
ScyllaDB Manager  → nodes:           5090
Public internet:                     nothing — no ports open
```

### Common Mistakes

- **Forgetting 7000 across AZs** — replication breaks silently; `nodetool status` shows nodes as UN but data diverges
- **Exposing 9042 to 0.0.0.0** — CQL has no built-in rate limiting; open to the internet = instant attack surface
- **Forgetting 10000** — `nodetool` and ScyllaDB Manager fail with connection refused
- **Opening 9160 (Thrift)** — unnecessary in modern deployments, increases attack surface

---

## Optimizing Write-Heavy Workloads

Write-heavy optimization is a series of trade-offs — every knob you turn to gain write throughput gives up something else.

### 1. Consistency Level — biggest lever, biggest risk

Drop from `LOCAL_QUORUM` to `LOCAL_ONE`:
- Coordinator acks after 1 replica instead of waiting for majority
- 2–3x lower write latency, higher throughput

**Risk:**
```
Write at LOCAL_ONE:
  Replica 1 ✓ ← acked to client
  Replica 2, 3 ← replication in-flight

Coordinator crashes before replication completes → data loss
Replica 1 goes down before Replica 2/3 catch up → data loss
```
Only use `LOCAL_ONE` for data where occasional loss is acceptable (logs, IoT events, metrics).

### 2. Compaction Strategy

**Use STCS (Size-Tiered) — default, best for writes:**
- Merges SSTables only when enough of similar size accumulate → low write amplification
- ScyllaDB-specific **ICS (Incremental)** further reduces space amplification over STCS

**Avoid LCS for write-heavy** — Leveled Compaction maintains strict level structure → more compaction work per write → higher write amplification.

**Risk with STCS:**
- Space amplification up to 50% — need more disk headroom
- More SSTables accumulate between compactions → reads get slower (more bloom filter checks, more SSTable merges)
- Monitor read latency carefully on mixed read/write workloads

**For time-series (append-only by time) — use TWCS:**
- Compacts within time buckets; when a window closes, the whole SSTable expires — no tombstone accumulation
- Risk: only works if writes are strictly time-ordered within a partition; out-of-order writes into closed windows cause compaction problems

### 3. CommitLog Sync Mode

```yaml
commitlog_sync: periodic   # fsync every 10s — fast, default
commitlog_sync: batch      # fsync on every write — safe, 2–5x slower
```

**Risk of `periodic`:** up to 10 seconds of writes lost on power failure / kernel crash. Mitigated by RF — replication across nodes/AZs provides durability even if one node crashes. Only use `batch` when strict single-node durability is required.

### 4. Memtable Size

Increase `memtable_total_space_in_mb`:
- Larger memtable absorbs more writes before flushing → fewer SSTables → less compaction → less read amplification

**Risk:**
- Every MB given to memtable is taken from row cache
- If set too high → OOM on compaction spikes (compaction needs its own memory buffer)
- Rule: memtable + row cache + compaction buffers must fit within ScyllaDB's allocated RAM

### 5. Batching — use carefully

`UNLOGGED BATCH` for writes to the **same partition** only:
```cql
BEGIN UNLOGGED BATCH
  INSERT INTO events (pk, ts, val) VALUES ('user1', now(), 'a');
  INSERT INTO events (pk, ts, val) VALUES ('user1', now(), 'b');
APPLY BATCH;
```

**Risk:**
```
Good: UNLOGGED BATCH → same partition key
  → coordinator handles locally → fast

Bad: UNLOGGED BATCH → many different partition keys
  → coordinator collects all writes, fans out to many nodes
  → coordinator becomes a hotspot — defeats shard-per-core
  → worse than individual writes under load
```
`LOGGED BATCH` uses Paxos — avoid entirely for write-heavy paths.

### 6. Avoid Lightweight Transactions (LWT)

LWT (`IF NOT EXISTS`, `IF col = val`) uses 4 Paxos round trips → ~4x slower than a normal write. Design schema so writes don't need uniqueness checks.

**Risk:** Removing LWT where the schema actually needs a uniqueness guarantee → duplicate or conflicting rows silently survive. Verify the data contract allows it before removing.

### 7. Driver-Level Async Writes

```python
# Bad — sequential, one at a time
for row in rows:
    session.execute(stmt, row)   # blocks until ack

# Good — async, many in-flight
futures = [session.execute_async(stmt, row) for row in rows]
for f in futures:
    f.result()   # collect after sending all
```

**Risk:** Silent error swallowing — a failed write in a batch of async futures requires per-future error checking; naive implementations drop errors silently.

### 8. Partition Key Design — the silent killer

High-cardinality, evenly distributed partition key → all shards on all nodes absorb writes in parallel → near-linear throughput scaling.

**Risk of hot partition:**
```
pk = "us-east-1"  ← all US writes to one partition
  → one shard on one node handles all US writes
  → that shard saturates; rest of cluster idles
  → adding nodes does nothing
```
Hot partitions cannot be fixed by scaling — only schema redesign fixes them. With tablets (v6+) a hot tablet can auto-split, but only if the partition key has enough cardinality to spread across the split halves.

### Risk Summary

| Optimization | Write gain | Risk |
|---|---|---|
| `LOCAL_ONE` vs `LOCAL_QUORUM` | High | Data loss if node down before async replication |
| STCS / ICS over LCS | Medium | Space amplification; reads slow as SSTables pile up |
| TWCS for time-series | High | Out-of-order writes into closed windows corrupt compaction |
| `commitlog_sync: periodic` | High | Up to 10s data loss on crash (mitigated by RF) |
| Larger memtable | Medium | Less row cache; OOM risk on compaction spike |
| `UNLOGGED BATCH` (same partition) | Medium | Misuse across partitions creates coordinator hotspot |
| Remove LWT | High | Duplicate/conflicting writes if uniqueness was needed |
| Async driver writes | High | Silent error swallowing if not handled carefully |
| Bad partition key | Negative | Hot shard — no amount of scaling helps |

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
15. Does ScyllaDB automatically rebalance data?
16. What are the production hardware requirements?
17. What AWS instance types are recommended for ScyllaDB?
18. Why is memory important even with NVMe?
19. How to optimize write-heavy workloads (risk evaluation)?
20. What ports need to be opened for ScyllaDB?
21. Does ScyllaDB integrate with VictoriaMetrics?
22. What collectors/exporters are available for ScyllaDB monitoring?
23. How to enable automatic backups every 6 hours?
24. How to restore from backups?
25. How does backup work internally (snapshots, hard links, upload)?
26. What is async I/O (io_uring/Linux AIO) and how does ScyllaDB use it?
27. How does ScyllaDB's I/O scheduler prioritize reads over compaction?
28. What is a shard in ScyllaDB?
29. How do different tables with different partition keys end up on the same shard?
30. How do shards work with replication and multiple nodes?
31. How is the token range (-2^63 to 2^63) divided across a cluster?
32. What are seed nodes and how does the gossip protocol work?
33. What are secondary indexes and how do local vs global (MV) indexes differ?
34. What is the write path for secondary indexes (Local SI vs Materialized View)?
35. What is the read path for secondary index queries (scatter-gather vs direct lookup)?
36. When should you use local secondary index vs materialized view?
37. What are the performance impacts and best practices for secondary indexes?
