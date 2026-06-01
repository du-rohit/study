# Apache Kafka — In-Depth Study Notes

> Organized to build understanding progressively:
> foundations → data model → broker internals → storage engine → replication → producer/consumer mechanics → application layer → operations → review.

---

# Part 1 — What and Why

## What is Kafka?

Apache Kafka is a distributed, partitioned, replicated **commit log** that doubles as a publish/subscribe messaging system, a streaming platform, and a durable buffer between producers and consumers. Originally built at LinkedIn (2010) and open-sourced via Apache in 2011.

- **Core abstraction:** an append-only, immutable, partitioned log
- **Delivery model:** consumers pull at their own pace; the broker holds data for a configurable retention window
- **CAP classification:** CP-leaning within a cluster — every partition has a single leader; writes block when leadership cannot be elected
- **Replication:** leader-follower per partition (not leaderless like Cassandra)
- **Written in:** Java + Scala (runs on JVM — GC behavior affects p99 latency)
- **Throughput target:** millions of messages/sec per broker on commodity hardware

The mental model that makes Kafka click: it is **not a queue**. It is a log. Consumers don't "take" messages off — they read by offset, and many independent consumers can read the same record without interfering with each other. Messages are deleted by retention policy, not by consumption.

---

## Kafka vs Other Messaging / Streaming Systems

| | Kafka | RabbitMQ | AWS SQS | AWS Kinesis | Pulsar | Redis Streams |
|---|---|---|---|---|---|---|
| Model | Distributed log | Broker queue (AMQP) | Managed queue | Distributed log | Distributed log + queue | In-memory log |
| Persistence | Disk (page cache) | Disk + memory | Disk (managed) | Disk (managed) | Tiered (BookKeeper) | Memory (AOF/RDB optional) |
| Consumer model | Pull, offset-based | Push (mostly) | Pull, ack-based | Pull, shard-iterator | Pull or push | Pull (XREAD) |
| Multi-consumer same data | Yes (offset per group) | Need fanout exchange | Limited (use SNS+SQS) | Yes (shard iterator) | Yes | Yes (consumer groups) |
| Ordering | Per partition | Per queue | None (FIFO opt-in) | Per shard | Per partition | Per stream |
| Throughput | Very high (>1M/s/broker) | Moderate | Moderate | High | Very high | Bounded by RAM |
| Latency (p99) | ~10–50 ms (durable) | Low ms (no durable) | High (managed overhead) | ~70–200 ms | ~10–50 ms | Sub-ms |
| Replication model | Leader-follower, ISR | Mirrored queues | Managed | Managed | Quorum (BookKeeper) | Master-replica |
| Retention | Time/size-based, days–forever | Until acked | 14 days max | 365 days max | Tiered (cold to object store) | Capped by RAM |
| Transactions | Yes (cross-partition) | Per-channel | None | None | Yes | No (MULTI/EXEC) |
| Best for | High-throughput event streaming, log aggregation, stream processing | Complex routing, request/reply | Simple decoupling on AWS | AWS-native streaming | Geo-replication, multi-tenancy | Lightweight in-memory streaming |

**Why Kafka and not RabbitMQ:** RabbitMQ is optimized for routing low volumes of in-flight messages to consumers and forgetting them once acked. Kafka is optimized for storing huge volumes durably and letting many consumers (and replay) read at their own speed.

**Why Kafka and not Kinesis:** Kinesis is the AWS-managed equivalent; the API and limits differ (1 MB/s/shard write, 2 MB/s/shard read, 24h–365d retention). Kafka has higher throughput per partition, more flexible retention, lower latency, but you operate the cluster (or pay MSK / Confluent Cloud).

**Why Kafka and not Pulsar:** Pulsar separates storage (Apache BookKeeper) from serving (brokers), which makes geo-replication and tenant isolation cleaner. Kafka's tiered storage (3.6+) closes much of the gap. Kafka has a larger ecosystem (Connect, Streams, KSQL, Schema Registry).

---

## Versions and Major Releases

- **0.8 (2013):** intra-cluster replication introduced. Before this, Kafka was a single-broker system; 0.8 made it distributed and fault-tolerant.
- **0.9 (2015):** new consumer API (Java) replacing the old Scala "high-level" / "simple" consumers. Security (SASL, SSL). Quotas.
- **0.10 (2016):** Kafka Streams. Message timestamps. Rack-aware replica assignment.
- **0.11 (2017):** **exactly-once semantics** — idempotent producer + transactions. New v2 message format with batches and headers.
- **1.0–2.x (2017–2019):** stability, Connect maturity, JBOD support, incremental cooperative rebalancing (2.4).
- **2.8 (2021):** **KRaft (KIP-500) early access** — Kafka running without ZooKeeper using its own Raft-based metadata quorum.
- **3.0 (2021):** KRaft for new clusters (preview), removed older message formats, deprecated ZooKeeper consumer offsets.
- **3.3 (2022):** KRaft GA for production. Cooperative rebalancing default.
- **3.4–3.5 (2023):** ZooKeeper-to-KRaft migration tooling. Improved KRaft snapshots.
- **3.6 (2023):** **Tiered Storage (KIP-405) early access** — offload old segments to object storage (S3/GCS/Azure Blob).
- **3.7 (2024):** JBOD support in KRaft. Producer transactions improvements.
- **3.8–3.9 (2024):** ZooKeeper deprecated; final ZK-supporting line.
- **4.0 (2025):** **ZooKeeper removed entirely**. KRaft is the only mode. Tiered Storage GA. New consumer rebalance protocol (KIP-848) — server-driven, eliminates eager rebalance entirely.

The single most important inflection point is **KRaft**: it removed Kafka's longest-standing operational pain (ZooKeeper) and enabled clusters with millions of partitions.

---

# Part 2 — Data Model and Log Semantics

## Topics, Partitions, Offsets

```
Cluster
  └── Topic                       (named logical stream; e.g., "orders")
        └── Partition 0           (an independent, ordered append-only log)
        │     └── Record at offset 0
        │     └── Record at offset 1
        │     └── ...
        └── Partition 1
        │     └── Record at offset 0
        │     └── ...
        └── Partition N-1
```

### Topic

- A **topic** is a named stream of records. It is purely a naming/configuration boundary — the unit of distribution is the partition.
- Topics have configurable retention, replication factor, partition count, and cleanup policy.
- A topic is created either by an admin (`kafka-topics.sh --create`) or automatically if `auto.create.topics.enable=true` (don't use this in production — it lets typos create topics).

### Partition

- A **partition** is a single, ordered, immutable log of records.
- A partition is the **unit of parallelism**: one partition is read by at most one consumer in a given consumer group at a time.
- A partition is the **unit of replication**: each partition is replicated across N brokers (one leader, N-1 followers).
- A partition lives on a single broker's disk at any moment (its leader); followers hold copies on other brokers.
- Ordering is guaranteed **within a partition**, not across the topic.

### Offset

- An **offset** is a monotonically increasing 64-bit integer assigned by the partition leader to each appended record.
- Offsets are partition-local — offset 100 in partition 0 has no relation to offset 100 in partition 1.
- Offsets are immutable: once assigned, a record's offset never changes.
- Consumers track their position by storing the next offset to read in a special compacted topic `__consumer_offsets`.

### Why Partitions Matter for Throughput

```
1 topic with 1 partition  → 1 leader broker handles all reads/writes → bottleneck
1 topic with 12 partitions → up to 12 brokers can serve in parallel  → scales horizontally
                            → up to 12 consumers in a group can parallelize work
```

Choosing partition count is a one-way decision in practice — you can increase it but cannot decrease it, and increasing it breaks key-based ordering (existing records keep their old partition; new records with the same key may hash to a new partition).

---

## Records

A Kafka record is a structured envelope:

```
┌─────────────────────────────────────────────────────┐
│ Key       │ Value     │ Headers       │ Timestamp │ │
│ (bytes)   │ (bytes)   │ (k:v list)    │ (int64)   │ │
└─────────────────────────────────────────────────────┘
```

| Field | Purpose |
|---|---|
| Key | Optional. Determines partition (when using default partitioner). Used as the dedup key in compacted topics. |
| Value | The payload — typically Avro, Protobuf, JSON, or raw bytes. |
| Headers | List of `(string, bytes)` pairs — added in Kafka 0.11. Used for tracing context, schema IDs, app metadata. |
| Timestamp | Either `CreateTime` (producer-set) or `LogAppendTime` (broker-set on append). Used by time-based queries and stream processors. |

### Key Semantics

- **Null key:** record is routed by the sticky partitioner (Kafka 2.4+) — batches go to a "sticky" partition until full, then round-robin to another partition. This batches efficiently while still spreading load.
- **Non-null key:** routed by `murmur2(key) % num_partitions` by the default partitioner. **All records with the same key land in the same partition** → same-key ordering is preserved.
- **Empty key (zero-length bytes) vs null key:** these are different. An empty key hashes to a specific partition (always partition 0 in practice); a null key uses sticky partitioning.

### Timestamp Semantics

```properties
# Per-topic config
message.timestamp.type=CreateTime    # default — producer's wall clock at send
message.timestamp.type=LogAppendTime # broker overrides to its wall clock on append
```

- `CreateTime` is the right choice when downstream processors care about event time (clickstream, IoT).
- `LogAppendTime` is the right choice when you want monotonicity guaranteed by the broker — useful for log aggregation where event timestamps from clients are untrustworthy.

`message.timestamp.difference.max.ms` rejects records whose `CreateTime` is too far from the broker clock — guards against malicious or buggy clients.

---

## Retention and Cleanup

A Kafka log doesn't grow forever — segments are deleted or compacted based on the topic's `cleanup.policy`.

### `cleanup.policy=delete` (default)

Records are deleted by time or size.

```properties
retention.ms=604800000         # 7 days (default)
retention.bytes=-1             # no size cap (default); or e.g., 100 GB per partition
segment.bytes=1073741824       # 1 GB segments — segments roll when this is exceeded
segment.ms=604800000           # 7 days — force segment roll after this even if not full
```

- A segment can only be deleted **after it has been rolled** (i.e., a newer active segment exists).
- The active segment (the one currently being written) is never eligible for deletion or compaction.
- This is why a low-volume topic may hold data longer than `retention.ms` — the segment never fills up enough to roll.

### `cleanup.policy=compact`

Instead of deleting by time, Kafka keeps **only the most recent value per key**.

```
Before compaction (partition 0):
  offset 0: key=alice, value="age:25"
  offset 1: key=bob,   value="age:30"
  offset 2: key=alice, value="age:26"
  offset 3: key=alice, value="age:27"
  offset 4: key=bob,   value="age:31"

After compaction:
  offset 3: key=alice, value="age:27"   ← latest alice
  offset 4: key=bob,   value="age:31"   ← latest bob

  (offsets 0, 1, 2 are physically removed; remaining offsets stay numerically the same)
```

- Compaction runs in the background; the `LogCleaner` thread reads old segments and writes a compacted replacement.
- A null value is a **tombstone** — it tells compaction "this key should be deleted entirely." After `delete.retention.ms` (default 24h), the tombstone itself is purged.
- Compaction guarantees: a consumer that reads from offset 0 will see all keys that ever existed, but only the most recent value for each.

### `cleanup.policy=compact,delete`

Combines both: compact, but also delete records older than `retention.ms`. Useful for change data capture where you want the latest state per key but also bound the log size.

### Use Cases for Compacted Topics

- **State stores for Kafka Streams** — restore application state from the topic on restart
- **CDC sinks** — latest version of each row
- **Configuration distribution** — `__consumer_offsets` and `__transaction_state` are compacted
- **Materialized cache rebuild** — rebuild a service's view of "current state" from the log

---

## Topic Configuration in Practice

```bash
# Create a topic with explicit configuration
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic orders \
  --partitions 12 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000 \
  --config compression.type=lz4

# Inspect a topic
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders

# Alter a topic config (some configs cannot be altered after creation)
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --entity-type topics --entity-name orders \
  --add-config retention.ms=2592000000   # 30 days

# Increase partitions (one-way!)
kafka-topics.sh --bootstrap-server localhost:9092 \
  --alter --topic orders --partitions 24
```

**Common pitfall:** changing partition count breaks key→partition stickiness. Records previously written with `murmur2("alice") % 12` may now hash to a different partition under `% 24`. For ordering-critical topics, pick a partition count up-front with growth in mind, or rewrite the topic and migrate consumers.

---

# Part 3 — Cluster Architecture

## Brokers

A broker is a single Kafka server process. A cluster is a set of brokers that coordinate via the controller.

```
Broker (JVM process)
  ├── Network thread pool        — reads/writes socket bytes
  ├── I/O thread pool            — handles request logic
  ├── Request purgatory          — delayed requests (waiting for replication, fetch min.bytes)
  ├── Log Manager                — owns partition log files and segment lifecycle
  ├── Replica Manager            — fetcher threads + ISR management
  ├── Group Coordinator          — consumer group membership + offset commits
  ├── Transaction Coordinator    — producer transaction state machine
  └── (KRaft mode) Metadata cache — local replica of the cluster metadata log
```

Each broker has a unique `broker.id`. In KRaft mode it also has a `node.id` (which may be the same value); a "node" can be a broker, a controller, or both.

---

## Controller

The **controller** is a special role within the cluster — at any moment, exactly one broker (or in KRaft mode, one node in the controller quorum) acts as the active controller.

### What the Controller Does

- Detects broker failures (via heartbeats / session expiry) and triggers leader elections
- Assigns partition leaders and replicas on topic creation, partition reassignment, or broker failure
- Maintains the cluster metadata: which topics, partitions, leaders, ISRs, configs
- Propagates metadata changes to all brokers

### Pre-KRaft (ZooKeeper-based)

Before KRaft, the controller was elected via a ZooKeeper ephemeral znode race. The active controller wrote cluster metadata to ZooKeeper. Brokers read from ZK to learn the state.

**Problems with this model:**
- ZooKeeper is a separate distributed system you must operate, monitor, secure, and upgrade
- Cluster startup time scaled poorly with partition count (metadata load from ZK)
- Controller failover required re-reading all state from ZK — could take minutes on large clusters
- The metadata model was eventually consistent across brokers in ways that caused subtle bugs

### KRaft (Kafka Raft Metadata Mode)

KRaft replaces ZooKeeper with a built-in Raft-based metadata quorum running inside Kafka itself.

```
Controller quorum (3 or 5 nodes — odd for quorum)
  ├── Active controller (Raft leader)
  └── Standby controllers (Raft followers, replicate metadata log)

Brokers (separate processes, or combined with controllers)
  └── Each broker tails the metadata log as a Raft observer
```

**The metadata log itself is a Kafka topic** (`__cluster_metadata`). Every metadata change — broker registration, leader election, topic creation — is appended as a record in this internal log. Brokers replay this log to materialize their local view of the cluster.

**Benefits:**
- One fewer system to operate
- Cluster supports millions of partitions instead of low tens of thousands
- Controller failover is near-instantaneous (the standby already has all metadata)
- Cluster startup time is constant in partition count

**Deployment modes:**
- **Combined mode** (`process.roles=broker,controller`): one process is both. Fine for dev / small clusters.
- **Isolated mode** (`process.roles=controller` or `broker`): production layout — typically 3 dedicated controllers + N brokers.

### KRaft Configuration

```properties
# Controller node
process.roles=controller
node.id=1
controller.quorum.voters=1@controller-1:9093,2@controller-2:9093,3@controller-3:9093
listeners=CONTROLLER://controller-1:9093

# Broker node
process.roles=broker
node.id=10
controller.quorum.voters=1@controller-1:9093,2@controller-2:9093,3@controller-3:9093
listeners=PLAINTEXT://broker-1:9092
```

---

## The Request Processing Pipeline

When a producer or consumer connects, this is what happens inside the broker:

```
TCP socket ──▶ Selector (NIO)
                │
                ▼
        Network threads (default: num.network.threads = 3)
                │
                ▼  parse request header
        Request queue (bounded)
                │
                ▼
        I/O threads (default: num.io.threads = 8)
                │
                ├── Produce request? → append to log → wait for ISR replication → response
                ├── Fetch request?   → read from log → wait for min.bytes / max.wait
                └── Metadata, offsetCommit, etc.
                │
                ▼  if response is ready
        Response queue (per-network-thread)
                │
                ▼
        Network thread writes response back over socket
```

**Two key tunables:**
- `num.network.threads` (default 3): scale with NIC bandwidth; rarely the bottleneck
- `num.io.threads` (default 8): scale with disk throughput and CPU cores

**Purgatory:** when a produce request has `acks=all`, the broker can't respond until enough followers have replicated. Rather than blocking an I/O thread, the request goes into a **purgatory** structure. It's pulled out when either (a) enough ISR members report having replicated up to the request's offset, or (b) `replica.lag.time.max.ms` triggers an ISR shrink. The same purgatory pattern handles delayed fetches (`fetch.min.bytes` + `fetch.max.wait.ms`).

---

## Disk Layout

Kafka relies entirely on the OS page cache for performance — it never maintains a user-space cache. The disk layout is designed so the kernel can do sequential I/O.

```
/var/kafka/log.dirs/
  ├── orders-0/                  (topic "orders", partition 0)
  │     ├── 00000000000000000000.log
  │     ├── 00000000000000000000.index
  │     ├── 00000000000000000000.timeindex
  │     ├── 00000000000000234517.log     ← next segment after rollover
  │     ├── 00000000000000234517.index
  │     ├── 00000000000000234517.timeindex
  │     └── leader-epoch-checkpoint
  ├── orders-1/
  └── __consumer_offsets-37/
```

The filename of a segment is the **base offset** (first offset in the segment), zero-padded to 20 digits. A binary search on filenames quickly locates the segment containing an arbitrary offset.

### Multiple Log Directories (JBOD)

```properties
log.dirs=/disk1/kafka,/disk2/kafka,/disk3/kafka
```

Each partition lives in **one** log directory. New partitions are assigned to the directory with the fewest partitions (not the most free space). A single disk failure only loses the partitions on that disk; in KRaft mode (3.7+), the broker survives and the controller reassigns leadership for those partitions to other replicas.

**Don't put log dirs on RAID 0:** a single disk failure kills the whole RAID set, taking down all partitions on the broker. JBOD with replication factor 3 is the standard production pattern.

---

# Part 4 — Storage Engine and Log Internals

## The Segment

A partition log is split into segments. Only the active (newest) segment is written; older segments are immutable.

```
Partition log = ordered sequence of immutable segments + 1 active segment

Segment "00000000000000234517":
  .log         → record bytes (the actual data)
  .index       → sparse offset → byte position index
  .timeindex   → sparse timestamp → offset index
  .snapshot    → (transactional state, for txns)
```

### Segment Rolls

A new segment is created when:
- `segment.bytes` exceeded (default 1 GB)
- `segment.ms` elapsed (default 7 days)
- The index file fills up (`segment.index.bytes`, default 10 MB)

Why segment rolls matter:
- **Retention deletion** can only remove fully-rolled segments — never the active one
- **Compaction** never touches the active segment
- **Tiered storage** uploads rolled segments to object storage

Tuning trade-off: smaller segments → faster retention deletion, but more file handles and more index overhead. 1 GB is fine for most topics; consider 100 MB for low-volume compacted topics.

---

## Index Files

Two sparse indexes per segment let the broker locate records cheaply.

### Offset Index (`.index`)

Maps `relative_offset → byte_position_in_log`.

```
Sparse — one entry per ~4 KB of log data (configurable via index.interval.bytes).

Relative offset  Byte position
─────────────    ─────────────
       0              0
      48           4096
      99           8192
     151          12288
     ...
```

To read offset 100:
1. Binary search filenames → find segment with base offset ≤ 100 → segment "00000000000000000000"
2. Binary search the `.index` for entry ≤ relative offset 100 → finds (99, 8192)
3. Open `.log` at byte 8192, scan forward through records until offset 100 is reached

The whole lookup is O(log N) in segments + O(log K) in the index + a tiny sequential scan — measured in microseconds.

### Time Index (`.timeindex`)

Maps `timestamp → relative_offset`.

```
Timestamp (ms)        Relative offset
─────────────         ─────────────
1700000000000              0
1700000060000             48
1700000120000             99
```

Used by `offsetsForTimes` API, by stream processors that need to seek by event time, and by retention checks ("is this segment older than `retention.ms`?").

A timestamp in the index is the **largest timestamp** seen in the segment up to the relative offset entry — this preserves the invariant that timestamps in the index are monotonically non-decreasing even if record `CreateTime` values themselves are out of order.

---

## Page Cache and Zero-Copy

Kafka's throughput depends on three OS-level mechanisms.

### Page Cache

When a record is appended to a log file, the bytes are written to the kernel page cache. Disk write happens later, asynchronously, when the kernel flushes dirty pages.

When a consumer reads, the bytes are usually still in page cache — so the read is a memory read, not a disk read.

**Implication:** Kafka allocates **very little JVM heap** (typically 6 GB) and gives the rest of the machine's RAM to the kernel for page cache. A broker with 64 GB of RAM and 6 GB of heap leaves ~58 GB for page cache — enough to serve typical consumer workloads entirely from memory.

Never run other memory-hungry processes on a Kafka broker. Never use a huge heap "to be safe." Both starve the page cache.

### `sendfile` (Zero-Copy)

When a consumer issues a Fetch request, Kafka uses the `sendfile(2)` system call to pipe bytes directly from the page cache to the socket. The data **never enters user space** — no copy into the JVM, no serialization step.

```
Traditional read path:
  Disk → Kernel buffer → User-space buffer → Kernel socket buffer → NIC
                            ↑ unnecessary copy ↑

sendfile path:
  Disk → Kernel buffer ────────────────────────→ Kernel socket buffer → NIC
```

This is the single largest reason Kafka can saturate 10–25 GbE NICs per broker.

**Caveat:** zero-copy only works when SSL is off and the format on disk matches the format on the wire. Modern Kafka versions support both. If you encrypt with SSL/TLS at the broker, you pay a copy + encrypt cost.

### Sequential I/O

Append-only writes are sequential. Modern SATA SSDs do > 500 MB/s sequential and NVMe SSDs > 3 GB/s sequential. Even spinning disks do > 100 MB/s sequential. Kafka's design exploits this — random I/O is for the consumer-offset lookup (small) and segment indexing (also small).

---

## Log Compaction

Compaction runs in the background and rewrites segments to keep only the latest value per key.

### How the Cleaner Works

```
Broker startup
  └── LogCleanerThread (one or more — log.cleaner.threads, default 1)
        │
        ▼
   For each compacted topic-partition:
     1. Build "cleaner offset map":  key → latest offset
        — scan all rolled segments after the last compaction point
        — keeps only one entry per key (highest offset wins)
     2. Read source segments
     3. Write replacement segment:
        — for each record: if its offset matches the cleaner map, keep it; otherwise skip
     4. Atomically replace old segments with the rewritten ones
     5. Advance the "compacted up to" offset
```

### Important Behaviors

- **The active segment is never compacted.** Records in the active segment are "dirty" until it rolls.
- **Tombstones** (records with null value) are kept for `delete.retention.ms` (default 24h) **after** compaction, then physically removed.
- **`min.compaction.lag.ms`** delays compaction so consumers reading recent data see all versions, not just the latest. Useful when you have a consumer that needs to see every update.
- **`max.compaction.lag.ms`** forces compaction even if the "dirty ratio" hasn't been hit — guarantees compaction frequency for low-volume keys.
- **Compaction does not preserve a contiguous offset sequence.** After compaction, offsets have gaps. Consumers see this transparently — `seek(100)` may land on offset 100 or the next available offset ≥ 100.

### Dirty Ratio

The cleaner picks the log whose dirty ratio is highest:

```
dirty ratio = bytes after last compaction point / total log bytes
```

Default threshold (`min.cleanable.dirty.ratio = 0.5`) means a partition becomes eligible for cleaning when half its data is "dirty" (post-compaction-point). Lower → more aggressive compaction, more CPU/disk; higher → less work but larger compacted topics.

---

## Tiered Storage (KIP-405, GA in 4.0)

Tiered storage offloads old, rolled segments to object storage (S3, GCS, Azure Blob, or any RemoteStorageManager implementation). The active segment and recent segments stay on local disk; older data is "cold."

```
Broker local disk:    Recent segments  ←—— hot reads served from page cache
                          │
                          │  (segment rolls + age > local.retention.ms)
                          ▼
Object storage:       Older segments   ←—— cold reads streamed back
```

### Configuration

```properties
# Per-broker
remote.log.storage.system.enable=true
remote.log.storage.manager.class.name=org.apache.kafka.server.log.remote.storage.RemoteLogManager
rsm.config.s3.bucket.name=my-kafka-tier
rsm.config.s3.region=us-east-1

# Per-topic
remote.storage.enable=true
local.retention.ms=86400000        # 1 day on local disk
retention.ms=2592000000            # 30 days total (1 day local + 29 days remote)
```

### When to Use Tiered Storage

- **Replay use cases** where you want long retention (days → months) but most reads target recent data
- **Compliance** scenarios requiring multi-year retention without storing it on hot SSD
- **Cost reduction** — object storage is ~10× cheaper per GB than EBS/local SSD
- **Faster broker recovery** — when a replica needs to catch up from far back, it can fetch cold segments from object storage in parallel instead of streaming from the leader

### Caveats

- Cold reads are slower (S3 latency is tens of ms vs page cache nanoseconds) — only enable when your consumer pattern tolerates this
- Adds an external dependency to broker availability — your S3 outage becomes a Kafka cold-read outage
- The compacted-topic path doesn't tier; compacted topics keep all data local

---

# Part 5 — Replication and Consistency

## Leader-Follower Replication

Every partition has exactly one **leader** broker and zero or more **followers**. All produces and consumes go through the leader; followers exist to provide fault tolerance.

```
Partition orders-0, replication factor 3:
  Broker A (leader)   ──┐
  Broker B (follower) ──┼── ISR (In-Sync Replicas) = {A, B, C}
  Broker C (follower) ──┘

Producer write ─▶ A
   A appends to its log
   B fetches from A, appends to its log, sends ack to A
   C fetches from A, appends to its log, sends ack to A
   A advances the High Watermark when all ISR have replicated
```

This is fundamentally different from Cassandra's leaderless model. **Kafka has a single ordering authority per partition** (the leader). This is what enables strict per-partition ordering and exactly-once delivery semantics.

---

## The In-Sync Replica (ISR) Set

The **ISR** is the dynamic set of replicas (including the leader) that are currently caught up with the leader's log.

### When a Replica Is Considered In-Sync

A follower is in-sync when:
- It has fetched and acknowledged records up to the leader's Log End Offset (LEO) within `replica.lag.time.max.ms` (default 30 seconds).

A follower drops out of ISR when:
- It hasn't sent a fetch request in `replica.lag.time.max.ms` (network partition, broker dead, GC pause)
- It has fallen behind by an unbounded amount (slow disk)

```
ISR = {A, B, C}        ← all caught up

Broker B GC pauses 60 seconds
ISR shrinks to {A, C}  ← B is dropped from ISR

Broker B recovers, catches up via fetcher thread
ISR expands back to {A, B, C}
```

### Why ISR Matters

A produce request with `acks=all` is only acknowledged once **all current ISR members** have replicated the record. ISR shrinks → fewer replicas to wait for → producer latency drops, but durability guarantee drops too.

This is the core tension: do you want availability (small ISR, fast acks) or durability (require many ISR members)?

---

## High Watermark and Log End Offset

Two pointers track the state of a partition log:

- **LEO (Log End Offset):** the offset of the next record to be appended. Each replica has its own LEO.
- **HW (High Watermark):** the highest offset that has been replicated to all ISR members. Consumers can only read records with offset < HW.

```
Leader log:    [0][1][2][3][4][5][6][7]
                                    HW=5  LEO=8

Follower B:    [0][1][2][3][4][5]
                                LEO=6 (caught up to HW)

Follower C:    [0][1][2][3][4][5][6][7]
                                       LEO=8

ISR = {Leader, B, C}, but HW = min(LEO across ISR) = 6

Consumer can read up to offset 5.
```

### Why Consumers Can't Read Past HW

If a consumer were allowed to read uncommitted records (LEO > HW), and the leader then crashed before those records replicated, a new leader could be elected from a follower that never received them. The consumer would have observed records that no longer exist — a **lost-update** anomaly.

The HW prevents this by ensuring consumers only see records that are durable across all ISR replicas.

### HW Advancement

```
Leader appends record at offset 7  → LEO_leader = 8
Follower B fetches, appends         → LEO_B = 8
Follower C fetches, appends         → LEO_C = 8
HW advances to min(8, 8, 8) = 8
Consumers can now read up to offset 7
```

The HW is propagated back to followers in fetch responses so they know what to expose to consumers as well.

---

## Producer Acks Semantics

`acks` is the durability dial on the producer.

| Setting | Behavior | Use Case |
|---|---|---|
| `acks=0` | Fire and forget. Producer doesn't wait for ack. | Throwaway data (metrics where loss is acceptable) |
| `acks=1` | Leader writes to its log, acks. Followers replicate async. | Default in older versions; data loss if leader dies before follower catches up |
| `acks=all` (or `-1`) | Leader waits for all ISR members to replicate. | Production default. Combined with min.insync.replicas, gives strong durability. |

```
acks=all + min.insync.replicas=2 + RF=3:
  Producer write → Leader → waits for 2 ISR members (incl. leader) to confirm → ack
  Tolerates 1 broker failure (ISR shrinks to 2, still meets min ISR)

acks=all + min.insync.replicas=2 + RF=3 + 2 brokers down:
  ISR shrinks to 1 < min.insync.replicas
  Producer gets NOT_ENOUGH_REPLICAS error → produce fails
  → Kafka chooses CONSISTENCY over AVAILABILITY here
```

### `min.insync.replicas`

Set as a **topic-level** config (do not set the broker default — it's too easy to forget). With RF=3, `min.insync.replicas=2` is the production standard.

```bash
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --entity-type topics --entity-name orders \
  --add-config min.insync.replicas=2
```

The interaction matters:
- RF=3, min.insync.replicas=2: 1 broker failure → still accept writes
- RF=3, min.insync.replicas=3: 1 broker failure → ISR shrinks to 2 → **writes blocked**
- RF=3, min.insync.replicas=1: same as `acks=1` durability-wise — not recommended

---

## Leader Election

When a leader broker fails, the controller picks a new leader from the partition's ISR.

### Clean Leader Election (default)

```
Controller observes broker A is unreachable
For each partition where A is the leader:
  - Look at ISR (excluding A)
  - Pick the first surviving member as new leader
  - Broadcast UpdateMetadata to all brokers
  - Followers redirect their fetch requests to the new leader
```

Time to elect: typically sub-second in KRaft mode; a few seconds with ZooKeeper at high partition counts.

### Unclean Leader Election

If `unclean.leader.election.enable=true`, the controller may pick a leader from **outside the ISR** (a follower that had fallen behind).

```
Scenario:
  ISR = {A, B} (C dropped out earlier — fell behind)
  A and B both die

  unclean.leader.election.enable=false (default): wait until A or B comes back
                                                   → partition is unavailable, no data loss
  unclean.leader.election.enable=true:             elect C as leader
                                                   → C's LEO is less than the old HW
                                                   → records that B/A had but C didn't are LOST
                                                   → consumers may see offsets disappear
                                                   → producers continue, but committed data is gone
```

**This is a durability vs availability trade-off.** For most production topics, leave it `false`. For metric/log topics where loss is acceptable and uptime matters more, set it `true`.

---

## Replica Fetcher Protocol

Followers stay in sync by pulling from the leader, not the leader pushing.

```
Each follower broker runs N "ReplicaFetcherThread"s (num.replica.fetchers, default 1).
Each thread is dedicated to one source broker:

ReplicaFetcherThread on Broker B (fetching from A):
  while running:
    request = FetchRequest(partitions=[(orders-0, offset=LEO_B), (orders-1, offset=...)])
    response = send(A, request)
    for each (partition, records) in response:
      append records to local log
      update LEO_partition
    send heartbeat-like FetchRequest at least every replica.fetch.wait.max.ms
```

Tuning:
- `num.replica.fetchers` (default 1) — increase to 4–8 on high-throughput clusters; one thread can be a bottleneck
- `replica.fetch.max.bytes` — max bytes per partition in one fetch
- `replica.fetch.response.max.bytes` — max bytes for the whole fetch response
- `replica.lag.time.max.ms` — how long a follower can be silent before being kicked from ISR

### Leader Epoch

Each time a leadership change happens, a new **leader epoch** is recorded. Followers include the epoch in their fetch requests. The leader rejects fetches from outdated epochs and includes the current epoch in responses. This protects against a subtle class of split-brain bugs where two replicas truncate each other's logs to inconsistent offsets — the epoch mechanism ensures the truncation point is unambiguous.

---

# Part 6 — Producer Mechanics

## Producer Lifecycle and Batching

A `KafkaProducer` is a heavyweight, thread-safe client. **Create one per JVM**, share across threads. Do not create one per message — it's a TCP-pooled, batched, multi-connection client.

### Send Path

```
producer.send(ProducerRecord("orders", "alice", "..."))
   │
   ▼
1. Serializer (key + value) → bytes
   │
   ▼
2. Partitioner → partition number
   │
   ▼
3. Record Accumulator (per-partition queue of batches)
   │   - Batch is appended to the current open batch for that partition
   │   - Batch closes when batch.size is reached or linger.ms elapses
   │
   ▼
4. Sender thread (background) drains batches → groups by broker → ProduceRequest
   │
   ▼
5. Broker leader appends, replicates to ISR, acks
   │
   ▼
6. Callback fired on the producer's I/O thread with the result
```

### Key Tunables

| Parameter | Default | Effect |
|---|---|---|
| `batch.size` | 16384 (16 KB) | Max bytes per batch. Larger → fewer requests, better compression, higher latency for low-throughput producers. |
| `linger.ms` | 0 | How long to wait for more records to fill a batch. 0 = send immediately. 5–20 ms is a common production setting — significant throughput gain. |
| `buffer.memory` | 33554432 (32 MB) | Total memory the producer can use to buffer un-sent records. When full, `send()` blocks or throws. |
| `compression.type` | none | none / gzip / snappy / lz4 / zstd. zstd is the modern default for new pipelines — best ratio at low CPU cost. |
| `max.in.flight.requests.per.connection` | 5 | Max unacknowledged requests per connection. Set to 1 for strict ordering without idempotence. |
| `acks` | all (since Kafka 3.0) | Durability dial. |
| `retries` | 2147483647 (max int, since 2.4) | Number of retries on retriable failures. |
| `delivery.timeout.ms` | 120000 | Hard cap on total time `send()` may take. |
| `request.timeout.ms` | 30000 | Per-request timeout before considering it failed. |

### The Sticky Partitioner

Default since 2.4. For records with **null keys**, the producer sticks to one partition until its batch is full, then picks another. This produces larger batches and dramatically lower per-record cost vs round-robin.

```
Round-robin (old):    partitions: 0,1,2,3,0,1,2,3,... → 12 tiny batches
Sticky:               partitions: 0,0,0,0,0,1,1,1,1,1,... → 3 large batches
```

For records with **non-null keys**, the default partitioner uses `murmur2(key) % num_partitions` — same-key records always go to the same partition.

---

## Idempotent Producer

`enable.idempotence=true` (default in 3.0+) prevents duplicate records caused by retries.

### How It Works

Each producer is assigned a **Producer ID (PID)** by the broker on first connect. The producer tags each record batch with `(PID, sequence_number)`. The sequence number is monotonically increasing per (PID, partition).

```
Producer A sends record to partition 0:
  PID=42, seq=0  → broker appends, acks
  PID=42, seq=1  → broker appends, acks
  PID=42, seq=2  → ack lost in network → producer retries
  PID=42, seq=2  → broker sees duplicate seq → silently drops, returns success
```

The broker remembers the last 5 (default) sequence numbers per (PID, partition). Duplicates within this window are de-duped. Out-of-order arrivals within the window are reordered.

### Required Constraints

- `acks=all`
- `max.in.flight.requests.per.connection ≤ 5`
- `retries > 0` (default is effectively infinite)

Idempotence **only protects against retries within a single producer session**. If the producer process restarts, it gets a new PID, and duplicates across sessions are possible. For end-to-end exactly-once, you need transactions.

---

## Transactions

Kafka transactions let a producer atomically commit a set of records across multiple partitions and topics. The classic use case is **consume-process-produce** in stream processors: read from input topic, write derived records to output topic, commit input offsets — all atomically.

### Transactional API

```java
producer.initTransactions();        // once at startup

while (true) {
  records = consumer.poll();
  producer.beginTransaction();
  try {
    for (rec : records) {
      producer.send(transform(rec));
    }
    producer.sendOffsetsToTransaction(offsets(records), consumer.groupMetadata());
    producer.commitTransaction();
  } catch (Exception e) {
    producer.abortTransaction();
  }
}
```

### How It Works

```
Producer has transactional.id = "tx-1"
  │
  ▼ on initTransactions()
Transaction Coordinator (a specific broker — leader of __transaction_state partition)
  - Looks up "tx-1" → assigns new producer epoch
  - Fences any older producer instance with same transactional.id
  - Returns PID + epoch
  │
  ▼ on first send to partition P
Producer registers P as a partition in the current transaction with the coordinator
  │
  ▼ on send
Records written to log with isTransactional=true marker, but with the current PID/epoch
Records have a "control flag" indicating they're part of an open transaction
  │
  ▼ on commit
Coordinator writes COMMIT marker to __transaction_state
Coordinator writes COMMIT markers to each participating partition log
  │
  ▼
Consumers with isolation.level=read_committed see the records as committed
Consumers with isolation.level=read_uncommitted see them as soon as they're written
```

### `transactional.id` and Fencing

`transactional.id` is a **stable identifier** for the producer instance. If the same `transactional.id` reconnects (e.g., after a restart), the coordinator increments its epoch and fences out any zombie instance still trying to write under the old epoch.

This is critical for stream processors where a stuck instance might still be alive and double-writing. Fencing ensures that only one producer with a given `transactional.id` can write at a time.

### Performance Cost

Transactions add overhead:
- 2 extra round trips per transaction (begin + commit)
- Control records (begin/commit/abort markers) consume offsets in the log
- `read_committed` consumers have to wait for commit markers before exposing records — adds end-to-end latency proportional to transaction size

For high-throughput stream processing, batch records into larger transactions — fewer commits amortizes the overhead. Kafka Streams does this automatically.

---

## Producer Error Handling

### Retriable vs Non-Retriable Errors

| Error Type | Examples | Producer Action |
|---|---|---|
| Retriable | NotLeaderForPartitionException, NetworkException, NotEnoughReplicasException | Retry up to `retries`, with backoff `retry.backoff.ms` |
| Non-retriable | RecordTooLargeException, SerializationException, AuthorizationException | Fail immediately, deliver to callback |

A common production mistake: not handling the callback. The producer's `send()` returns a `Future`, but in async use most code passes a callback. If the callback is empty, errors are silently lost.

```java
// BAD
producer.send(record);   // silently drops errors

// GOOD
producer.send(record, (metadata, exception) -> {
  if (exception != null) {
    log.error("Send failed: topic={}, key={}", record.topic(), record.key(), exception);
    failureMetric.increment();
  }
});
```

---

# Part 7 — Consumer Mechanics

## The Pull Model

Consumers pull records from brokers — they're not pushed. This is a fundamental design choice with several implications:

- **Consumers control their pace.** A slow consumer doesn't fall over; it just lags. Push-based systems (RabbitMQ) need flow control or risk consumer overflow.
- **Replay is natural.** Reset offset to 0, re-read everything. No special replay protocol needed.
- **Multiple independent consumers can read the same data** without the broker tracking each one's state — they each track their own offset.

The downside: there's a polling overhead and a latency floor (the consumer's poll interval).

---

## Consumer Groups

A **consumer group** is a set of consumer instances that cooperate to consume from a topic, with each partition assigned to exactly one consumer in the group.

```
Topic "orders" with 6 partitions

Group "orders-processor" with 3 consumers:
  Consumer 1 ← partitions 0, 1
  Consumer 2 ← partitions 2, 3
  Consumer 3 ← partitions 4, 5

Group "fraud-detector" with 6 consumers:
  Consumer 1 ← partition 0
  Consumer 2 ← partition 1
  ...
  Consumer 6 ← partition 5
```

### Key Properties

- Within a group, each partition is read by **exactly one consumer at a time**.
- More consumers in a group than partitions → some consumers are idle.
- More partitions than consumers → some consumers handle multiple partitions.
- Different groups read independently — they each have their own offset progress in `__consumer_offsets`.

### `__consumer_offsets`

The consumer group's committed offsets are stored in a compacted internal topic, partitioned by `hash(group_id) % 50`. One broker (the leader of that partition) is the **group coordinator** for that group.

```
Consumer commits offset 12345 for orders-0:
  → write to __consumer_offsets partition 17 (say)
  → key = (group_id, topic, partition), value = offset
  → log compaction keeps only the latest offset per (group, topic, partition)
```

The 50-partition default (`offsets.topic.num.partitions`) limits how many group coordinators distribute across the cluster — increase if you have many groups.

---

## Rebalancing

When a consumer joins or leaves the group (scale up, scale down, crash, deploy), partitions must be reassigned. This is **rebalancing**.

### Eager Rebalancing (pre-2.4)

```
1. All consumers revoke ALL their partitions
2. JoinGroup request — all consumers register with coordinator
3. Coordinator picks a leader; leader runs the assignment algorithm
4. SyncGroup — coordinator distributes the new assignment
5. All consumers reset and start consuming new partitions
```

**Problem: "stop-the-world" pause.** During steps 1–5, no consumer is processing anything. On a large group with many partitions, this could be tens of seconds.

### Cooperative Incremental Rebalancing (2.4+)

```
Round 1:
  Coordinator computes ideal new assignment
  Asks consumers to revoke ONLY partitions that need to move
  Other partitions keep being processed

Round 2:
  After revocations complete, newly-freed partitions are assigned to their new owners
```

This eliminates the global pause. The default `partition.assignment.strategy` since 3.0+ includes `CooperativeStickyAssignor`. For most upgrades, this is the single highest-impact change.

### KIP-848: Next-Generation Consumer Rebalance (Kafka 4.0+)

The new protocol moves assignment **into the broker (group coordinator)** instead of computing it on the client. The coordinator pushes assignment changes incrementally. Consumers don't need to know the rebalance protocol — they just receive new assignments. This removes the upgrade complexity of mixed client versions and makes rebalances faster still.

### Static Membership

`group.instance.id` (added in 2.3) prevents a consumer's brief restart from triggering rebalances. The consumer keeps its `group.instance.id` across restarts; when it disappears and reappears within `session.timeout.ms`, the coordinator does not reassign its partitions.

```properties
# Per consumer instance — typically the pod name / hostname
group.instance.id=order-processor-3
session.timeout.ms=45000        # tolerate up to 45s of consumer downtime
```

Without static membership, every Kubernetes pod restart triggers a rebalance — bad. With it, the pod can restart within the session timeout and keep its partitions.

---

## Offset Management

A consumer must commit its progress so that, if it dies and another consumer takes over, processing resumes from the right place. There are two regimes.

### Auto-Commit

```properties
enable.auto.commit=true                # default
auto.commit.interval.ms=5000           # commit every 5 seconds
```

Behavior:
- On every `poll()`, the consumer **may** commit the offset of the last batch returned by the previous poll, if `auto.commit.interval.ms` has elapsed.
- **The commit happens BEFORE the application has processed the records.** This means:
  - If the consumer crashes mid-batch, on restart it skips records that were committed but not actually processed → **data loss**.
  - If the consumer crashes between processing and commit, on restart it re-processes the batch → **duplicates**.

Auto-commit is "at-least-once with risk of loss." Avoid it for production data pipelines.

### Manual Commit

```java
while (true) {
  records = consumer.poll(Duration.ofMillis(100));
  process(records);                           // your business logic
  consumer.commitSync();                      // commit only AFTER processing succeeds
  // or commitAsync() if you don't want to block on commit
}
```

This is "at-least-once": if the consumer crashes after processing but before commit, on restart it re-processes the batch — duplicates, but no loss. For idempotent processing or downstream dedup, this is fine.

### Async Commit Pitfalls

`commitAsync()` doesn't block, but it also doesn't retry on failure (because by the time the failure is observed, a later commit may have superseded it). Common pattern: `commitAsync()` in the steady state, plus `commitSync()` on shutdown.

```java
try {
  while (true) {
    records = consumer.poll(...);
    process(records);
    consumer.commitAsync();
  }
} finally {
  try { consumer.commitSync(); } finally { consumer.close(); }
}
```

### Per-Partition Commit

For high-throughput processing where you process partitions in parallel:

```java
Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
for (rec : records) {
  process(rec);
  offsets.put(new TopicPartition(rec.topic(), rec.partition()),
              new OffsetAndMetadata(rec.offset() + 1));
}
consumer.commitSync(offsets);
```

Note the `+1` — the committed offset is the **next** offset to read, not the last one read.

---

## Isolation Level

`isolation.level` controls how the consumer handles transactional records.

| Level | Behavior |
|---|---|
| `read_uncommitted` (default) | Consumer sees records as soon as they're appended to the log, including records in open transactions. |
| `read_committed` | Consumer only sees records from committed transactions. Records in open transactions are buffered. |

`read_committed` adds latency proportional to the average transaction duration — the consumer can't expose records past the **Last Stable Offset (LSO)**, which is the smallest open-transaction offset.

For stream processors and exactly-once pipelines, `read_committed` is required. For analytics workloads where transactional semantics aren't needed, the default is fine.

---

## Consumer Lag

**Lag** = `(partition LEO) - (consumer committed offset)`. It's the number of records the consumer hasn't yet processed.

```bash
# Per-group, per-partition lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group orders-processor
```

Output:
```
GROUP            TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
orders-processor orders  0          12345           12500           155
orders-processor orders  1          11000           12500           1500   ← lagging!
```

### Why Lag Matters

Lag is the single most important consumer health metric. It captures the question "is my consumer keeping up with the producer?"

**Common causes of growing lag:**
- Slow downstream (database, external API) bottlenecking processing
- Under-provisioned consumer (need more pods or more partitions)
- Hot partition (one partition gets disproportionate keys)
- Rebalance storm
- GC pauses or JVM tuning issues

**Alerting:** lag-based alerts should look at the **rate of lag change**, not absolute value. A lag of 10,000 records is fine if it's stable; a lag of 1,000 growing by 100/sec is a problem.

---

## Fetch Tuning

The consumer's fetch behavior controls throughput vs latency.

| Parameter | Default | Effect |
|---|---|---|
| `fetch.min.bytes` | 1 | Broker waits until this many bytes are available before responding. Higher → larger batches, fewer requests, higher latency. |
| `fetch.max.wait.ms` | 500 | Max time broker waits for `fetch.min.bytes` before responding. |
| `max.partition.fetch.bytes` | 1048576 (1 MB) | Max bytes per partition per fetch. |
| `fetch.max.bytes` | 52428800 (50 MB) | Max bytes for the whole fetch response. |
| `max.poll.records` | 500 | Max records returned by a single `poll()`. Smaller → more frequent commit checkpoints. |
| `max.poll.interval.ms` | 300000 (5 min) | If `poll()` isn't called within this, consumer is considered dead and the group rebalances. |

**`max.poll.interval.ms` is the most common gotcha.** If processing a batch takes longer than 5 minutes (e.g., calling a slow external API for each record), the consumer is kicked out of the group, triggering a rebalance, and the next consumer re-processes the same batch — duplicates plus thrashing. Either reduce `max.poll.records`, increase `max.poll.interval.ms`, or move the slow work to a worker thread pool.

---

# Part 8 — Multi-Region Replication and DR

## MirrorMaker 2

Kafka's built-in cross-cluster replication tool. It's actually a Kafka Connect-based system that runs source and sink connectors to copy topics from one cluster to another.

### Topology Patterns

**Active-Passive (DR):**
```
Primary cluster (active)         DR cluster (passive)
  topic "orders"        ─MM2─▶   topic "primary.orders"   ← renamed by default
                                  (consumers don't run here normally)

Failover: redirect producers + consumers to DR cluster.
RPO: bounded by replication lag (typically seconds).
```

**Active-Active:**
```
Cluster A                        Cluster B
  topic "orders"       ◀─MM2─▶   topic "orders"
  topic "B.orders"    ◀─        ─▶ topic "A.orders"
```

By default, MM2 prefixes mirrored topics with the source cluster's name (e.g., `A.orders` on B). This prevents infinite loops — consumers see two topics: locally-produced (`orders`) and remotely-mirrored (`A.orders`). Active-active is harder than it sounds because you need conflict resolution semantics for the same logical event written on both sides.

### MM2 Configuration

```properties
# mm2.properties
clusters=primary,backup
primary.bootstrap.servers=primary:9092
backup.bootstrap.servers=backup:9092

primary->backup.enabled=true
primary->backup.topics=orders,users,payments
primary->backup.replication.factor=3
primary->backup.offset-syncs.topic.replication.factor=3
primary->backup.checkpoints.topic.replication.factor=3
primary->backup.heartbeats.topic.replication.factor=3

sync.topic.acls.enabled=true
emit.heartbeats.enabled=true
emit.checkpoints.enabled=true
```

### Offset Translation

A consumer reading on the primary at offset 12345 wants, on failover, to resume at the "same" record on the DR cluster — but the offset numbers don't line up (DR cluster may have written different offsets due to its own sequencing).

MM2 maintains an **offset-syncs** topic that maps `primary_offset → backup_offset` per topic-partition. On failover, the `MirrorMakerCheckpoint` tool reads this and translates consumer group offsets so consumers resume at the right record on the DR cluster.

### Lag and Monitoring

MM2 has a built-in lag metric: the difference between the primary's LEO and the last record mirrored to the backup. Alert on this. Typical SLA: < 5 seconds replication lag in steady state.

---

## Stretch Clusters

A single Kafka cluster spread across multiple availability zones (or even regions, though this is rare).

```
Cluster spans 3 AZs:
  Broker 1, 2, 3 in AZ-A
  Broker 4, 5, 6 in AZ-B
  Broker 7, 8, 9 in AZ-C

Topic with RF=3:
  Rack-aware assignment ensures each replica lands in a different AZ.
  AZ failure loses 1 of 3 replicas per partition → ISR shrinks to 2, still serving.
```

### Rack Awareness

```properties
broker.rack=us-east-1a    # set per broker, matching the AZ
```

The controller's replica assignment algorithm spreads replicas across racks (AZs). This ensures one rack failure doesn't take out all replicas of a partition.

### When Stretch Works

- Multiple AZs within a single region (low inter-AZ latency, < 2 ms)
- `min.insync.replicas=2` with RF=3 across 3 AZs — survives one AZ failure
- Producers / consumers can connect to any AZ

### When Stretch Doesn't Work

- Cross-region (10+ ms inter-region latency) — replication latency dominates produce latency
- Asymmetric bandwidth (cross-region transfer cost)
- For cross-region DR, use MM2 or commercial cluster linking, not a stretch cluster

---

## Confluent Cluster Linking and Other Commercial Options

Confluent's **Cluster Linking** provides byte-for-byte mirroring with the source cluster's offsets preserved on the target. Unlike MM2, the target topic has the same offsets as the source — consumers can fail over without offset translation.

Other options: AWS MSK has its own replication primitives, and many providers offer cross-region replication as managed features.

---

## RPO and RTO Targets

| Pattern | RPO (data loss) | RTO (downtime) | Notes |
|---|---|---|---|
| Active-Passive with MM2 | seconds (replication lag) | minutes (manual failover) | Most common DR setup |
| Active-Active with MM2 | near-zero (writes accepted on either) | seconds (DNS / LB switch) | Conflict resolution complexity |
| Stretch cluster across AZs | zero | seconds (auto failover) | Same-region only |
| Confluent Cluster Linking | seconds | seconds–minutes | Lower offset translation overhead |
| Single cluster, no DR | unbounded on disaster | unbounded | Not acceptable for production data |

---

# Part 9 — Application Layer

## Kafka Streams

A Java library for stream processing applications. It runs in-process — no separate cluster — and uses Kafka itself for state storage.

### Mental Model

```
KStream  = unbounded sequence of records (like a Kafka topic)
KTable   = changelog table (latest value per key) — backed by a compacted topic
GlobalKTable = a KTable replicated to every instance (for joins on small reference data)
```

A Streams app builds a topology: source → operators → sinks.

```java
StreamsBuilder builder = new StreamsBuilder();

KStream<String, Order> orders = builder.stream("orders");

KTable<String, Long> orderCountsByUser =
    orders
      .groupBy((k, order) -> order.userId())
      .count();

orderCountsByUser.toStream().to("user-order-counts");

KafkaStreams app = new KafkaStreams(builder.build(), config);
app.start();
```

### State Stores

Stateful operators (joins, aggregations, windowing) keep state in **RocksDB** locally on disk. Every state change is also written to a **changelog topic** (compacted) in Kafka. On restart, the state store is rebuilt by replaying the changelog.

This pattern — local fast state + durable Kafka-backed log — is the heart of Streams. It gives you:
- Fast lookups (RocksDB on local disk)
- Durable state survival (changelog topic)
- Scale-out (changelog topic is partitioned the same as input — each instance owns its partitions' state)

### Exactly-Once Processing

Set `processing.guarantee=exactly_once_v2`. Streams uses transactions to atomically:
- Write derived records to output topics
- Update state store changelogs
- Commit consumer offsets

A crash mid-processing → transaction aborts → no partial output → on restart, reprocess from last committed offset.

This is the killer feature: end-to-end exactly-once across consume-process-produce, in a single API call.

### Streams vs Flink vs Spark Streaming

| Aspect | Kafka Streams | Apache Flink | Spark Streaming |
|---|---|---|---|
| Deployment | Library in your app | Standalone cluster | Standalone cluster |
| Source | Kafka only | Kafka + many | Kafka + many |
| State | RocksDB + changelog | RocksDB + checkpoint | Various |
| Exactly-once | Yes (with txns) | Yes | Yes |
| Latency | Low (ms) | Lowest (sub-ms possible) | Higher (micro-batch) |
| Operational overhead | None (just your app) | Full cluster | Full cluster |
| Complex windowing | Yes | Best | Yes |

Streams is the right choice when you want stream processing without a separate cluster. Flink is the right choice when you need advanced semantics (event-time, complex windows, cross-source joins) or extreme throughput.

---

## Kafka Connect

A framework for moving data between Kafka and external systems. Connectors are JARs that implement source (system → Kafka) or sink (Kafka → system) integrations.

### Architecture

```
Connect Cluster (distributed mode)
  Worker 1, Worker 2, Worker 3, ...   ← each runs tasks
  Connectors → Tasks (the unit of parallelism)
  
Coordination: workers form a group via __connect_offsets, __connect_configs, __connect_status topics
Tasks rebalance across workers like Kafka consumers rebalance partitions
```

### Common Connectors

| Source / Sink | Connector | Use Case |
|---|---|---|
| JDBC | jdbc-source / jdbc-sink | Replicate from / to RDBMS by query polling |
| Debezium | debezium-mysql, debezium-postgres, etc. | CDC — read DB transaction logs into Kafka |
| S3 sink | confluent / aiven-s3 | Archive Kafka topics to S3 |
| Elasticsearch sink | confluent-elasticsearch | Index Kafka records into ES |
| MongoDB | mongo-source / sink | Bi-directional with Mongo |
| File | FileStreamSource (built-in) | Tail a file into Kafka (dev only) |

### Single Message Transforms (SMTs)

Lightweight per-record transformations applied at Connect — rename fields, mask data, route to different topics. Avoid heavy logic in SMTs; for anything stateful, use Streams downstream of Connect.

---

## Schema Registry

A separate service (Confluent originally; alternatives like Karapace and Apicurio exist) that stores schemas (Avro, Protobuf, JSON Schema) and assigns them numeric IDs.

### Wire Format

```
Producer encodes record with Avro schema:
  ┌──┬──┬─────────────────┬─────────────────────┐
  │ 0│ 4│ schema ID       │ Avro-serialized body│
  │  │  │ (32-bit, big-end)│                     │
  └──┴──┴─────────────────┴─────────────────────┘
  │  │
  │  └── Magic byte (0)
  └──── Version (always 0 for confluent format)

Consumer reads:
  1. Skip magic byte
  2. Read schema ID
  3. Fetch schema from registry (cached)
  4. Deserialize body
```

### Compatibility Modes

Set per subject (topic-key, topic-value):

| Mode | Producer can | Consumer can |
|---|---|---|
| BACKWARD (default) | Add optional fields, remove fields | Read records written with old or new schemas |
| FORWARD | Remove optional fields, add new fields | Old consumer reads new-schema records |
| FULL | Both BACKWARD and FORWARD | Either direction works |
| NONE | Anything | Anything (no checks) |

**Rule of thumb:** BACKWARD compatibility for most use cases — consumers usually deploy after producers, and BACKWARD lets a new producer coexist with old consumers.

### Why It Matters

Without a schema registry, every consumer needs to know the producer's schema out-of-band. Schema evolution becomes painful: rename a field in production → every consumer breaks. With a registry, the schema is versioned alongside the data, and compatibility is enforced at produce time.

---

# Part 10 — Operations

## Broker Sizing

A starting heuristic for production:

| Resource | Recommendation |
|---|---|
| CPU | 16–32 cores; Kafka is moderately CPU-bound at high traffic |
| RAM | 64 GB; 6 GB heap, rest for page cache |
| Disk | NVMe SSD or fast SATA SSD; size = (peak write GB/sec) × (retention sec) × RF × 1.2 headroom |
| Network | 10 GbE minimum; 25 GbE for high-throughput clusters |
| Disk layout | JBOD (multiple disks as separate log dirs); avoid RAID for the log volume |

### Key JVM Settings

```bash
# kafka-server-start.sh exports KAFKA_HEAP_OPTS
export KAFKA_HEAP_OPTS="-Xms6g -Xmx6g"

# Use G1GC (default in modern Kafka)
export KAFKA_JVM_PERFORMANCE_OPTS="
  -server -XX:+UseG1GC
  -XX:MaxGCPauseMillis=20
  -XX:InitiatingHeapOccupancyPercent=35
  -XX:+ExplicitGCInvokesConcurrent
  -XX:MaxInlineLevel=15
  -Djava.awt.headless=true
"
```

Keep heap small. The page cache is more valuable than heap.

---

## Key `server.properties`

```properties
# Identity
broker.id=1
node.id=1

# Listeners
listeners=PLAINTEXT://0.0.0.0:9092,SSL://0.0.0.0:9093
advertised.listeners=PLAINTEXT://broker-1.internal:9092,SSL://broker-1.example.com:9093

# Log storage
log.dirs=/disk1/kafka,/disk2/kafka,/disk3/kafka
num.partitions=12                     # default for new topics
default.replication.factor=3
min.insync.replicas=2                 # broker default; can also be per-topic
log.segment.bytes=1073741824          # 1 GB
log.retention.hours=168               # 7 days
log.retention.check.interval.ms=300000

# Replication
num.replica.fetchers=4
replica.lag.time.max.ms=30000
replica.fetch.max.bytes=1048576
unclean.leader.election.enable=false  # default since 0.11; do not enable

# Network and I/O
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
socket.request.max.bytes=104857600    # 100 MB cap on a single request

# Group coordinator
offsets.topic.replication.factor=3
offsets.topic.num.partitions=50
group.initial.rebalance.delay.ms=3000

# Transactions
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

# KRaft
process.roles=broker
controller.quorum.voters=1@controller-1:9093,2@controller-2:9093,3@controller-3:9093
```

---

## Monitoring: The Metrics That Matter

### Broker-Level

| Metric (JMX) | Why |
|---|---|
| `UnderReplicatedPartitions` | Should always be 0. Anything else means a replica is behind — investigate. |
| `OfflinePartitionsCount` | Should always be 0. Anything else means a partition has no leader — production outage. |
| `ActiveControllerCount` | Should be exactly 1 across the cluster. Sum = 0 → no controller. Sum > 1 → split-brain. |
| `RequestQueueSize` | Backed-up request queue → I/O thread saturation. |
| `NetworkProcessorAvgIdlePercent` | < 30% = network threads saturated; scale `num.network.threads`. |
| `RequestHandlerAvgIdlePercent` | < 30% = I/O threads saturated; scale `num.io.threads`. |
| `BytesInPerSec` / `BytesOutPerSec` | Throughput. Use for capacity planning. |
| `LeaderCount` | Should be balanced across brokers; large imbalances → unbalanced load. |
| `LogFlushLatency` | High → disk write latency; possibly disk saturation or contention. |

### Consumer Group Lag

External tools: `burrow`, `kafka-lag-exporter`, the AWS MSK lag CloudWatch metrics, or Confluent Control Center.

### JVM

GC pause time, heap usage, allocation rate. p99 produce/fetch latency tracks GC tail latency closely.

---

## Partition Reassignment

When you add or remove brokers, partitions must move. Done with `kafka-reassign-partitions.sh`:

```bash
# 1. Generate a proposed assignment
echo '{"version":1,"topics":[{"topic":"orders"}]}' > topics.json

kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --topics-to-move-json-file topics.json \
  --broker-list "1,2,3,4,5" \
  --generate > reassign.json

# 2. Inspect and edit reassign.json (it contains "current" and "proposed" plans)

# 3. Execute
kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --reassignment-json-file reassign.json \
  --execute --throttle 50000000      # 50 MB/s

# 4. Monitor
kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --reassignment-json-file reassign.json \
  --verify
```

**Throttle is critical.** Without it, reassignment streams data at line rate and starves producer/consumer traffic. Start with a conservative throttle and increase if reassignment is too slow.

---

## Quotas

Producer, consumer, and request quotas prevent a single client from saturating the cluster.

```bash
# Limit user "etl-job" to 10 MB/s produce and 50 MB/s consume
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --add-config "producer_byte_rate=10485760,consumer_byte_rate=52428800" \
  --entity-type users --entity-name etl-job
```

The broker throttles by delaying responses, not by dropping requests. A throttled producer sees increased `RecordSendRateAvg` latency but no errors. This is gentler than hard rejection.

---

## Security

### Authentication: SASL Mechanisms

| Mechanism | Use |
|---|---|
| `PLAIN` | Username/password over SSL (don't use without SSL — plaintext credentials) |
| `SCRAM-SHA-256` / `SCRAM-SHA-512` | Salted challenge-response; credentials in `__credentials` topic |
| `GSSAPI` (Kerberos) | Enterprise SSO |
| `OAUTHBEARER` | OAuth tokens; common for cloud-managed Kafka |

### Encryption in Transit

```properties
listeners=SSL://0.0.0.0:9093
ssl.keystore.location=/etc/kafka/secrets/broker.keystore.jks
ssl.keystore.password=...
ssl.truststore.location=/etc/kafka/secrets/broker.truststore.jks
ssl.truststore.password=...
ssl.client.auth=required   # mTLS — both sides authenticate
```

SSL has a measurable CPU cost and **disables zero-copy** (sendfile bypasses encryption). For high-throughput pipelines, consider whether transit encryption is needed in private networks.

### Authorization: ACLs

```bash
# Producer "orders-app" can write to topic "orders"
kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:orders-app \
  --operation Write --topic orders

# Consumer "orders-app" can read topic "orders" and use consumer group "orders-processor"
kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:orders-app \
  --operation Read --topic orders \
  --operation Read --group orders-processor
```

Default policy when `allow.everyone.if.no.acl.found=false`: deny. **Set this true only in dev environments.**

---

## Backup and Restore

Kafka has no built-in snapshot/restore concept (it's a log, not a database). The standard patterns:

### Tiered Storage / S3 Sink as Backup

Stream every topic to S3 via Connect (S3 sink connector). On disaster, restore by re-producing from S3 into a new cluster. This is the most common pattern.

### Cross-Cluster Replication (MM2)

Use a DR cluster as a hot standby. Failover is faster than restore-from-S3 but costs more (full replica cluster running).

### Topic-Level Snapshot

For compacted topics (configuration, state), you can periodically consume the whole topic and write it elsewhere as a snapshot. Not standard practice — usually MM2 or S3 sink is enough.

---

## Performance Testing

```bash
# Produce stress
kafka-producer-perf-test.sh \
  --topic test-perf \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 acks=all compression.type=lz4 batch.size=131072 linger.ms=10

# Consume stress
kafka-consumer-perf-test.sh \
  --bootstrap-server localhost:9092 \
  --topic test-perf \
  --messages 1000000 \
  --threads 4
```

The output reports records/sec, MB/sec, p50/p95/p99/p999 latency. Use this for capacity planning and before/after tuning comparison.

---

## Operational Procedures

### Adding a Broker

```bash
# 1. Provision new VM, install Kafka, set unique broker.id / node.id
# 2. Configure listeners and controller.quorum.voters to match cluster
# 3. Start the broker — it registers via KRaft metadata
# 4. Verify it appears in the cluster
kafka-broker-api-versions.sh --bootstrap-server new-broker:9092

# 5. Reassign partitions to balance load (broker initially has no partitions)
# Use kafka-reassign-partitions.sh as shown earlier
```

### Removing a Broker

```bash
# 1. Reassign all partitions off the broker first
# 2. Wait for reassignment to complete and verify the broker has no leaders
kafka-topics.sh --bootstrap-server localhost:9092 --describe | grep -E "Leader: <broker-id>"

# 3. Stop the broker
# 4. In KRaft, remove from controller.quorum.voters if it was a controller
```

### Rolling Restart

Critical for upgrades, config changes, certificate rotation.

```bash
for broker in broker-1 broker-2 broker-3; do
  # 1. Trigger preferred leader election to move leadership off this broker
  kafka-leader-election.sh --bootstrap-server any-other:9092 \
    --election-type PREFERRED --all-topic-partitions

  # 2. Stop the broker gracefully (SIGTERM, wait for in-flight requests to complete)
  systemctl stop kafka@$broker

  # 3. Apply changes (config, version)
  # ...

  # 4. Start the broker
  systemctl start kafka@$broker

  # 5. Wait for ISR to fully expand before moving to next broker
  while kafka-topics.sh --bootstrap-server $broker:9092 --describe \
        --under-replicated-partitions | grep -q "Partition:"; do
    sleep 10
  done
done
```

**Never restart two brokers at once when RF=3 and min.insync.replicas=2.** You'll have only 1 replica in ISR → falls below min ISR → producers fail.

### Topic Deletion

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic obsolete
```

Deletion is asynchronous — brokers schedule the segment files for removal. The topic disappears from metadata immediately; disk reclamation happens within minutes.

**`delete.topic.enable=true` is the default since 1.0.** If it's `false` (legacy), delete is a no-op.

---

# Part 11 — Review: Common Study Questions

**Q: What is the difference between a topic and a partition?**
A topic is a logical name for a stream of records; a partition is an independent ordered log. A topic has 1 or more partitions. Ordering is guaranteed within a partition, not across the topic. Partitions are the unit of parallelism (one consumer per partition per group) and the unit of replication (each partition has its own leader and followers).

**Q: Why is Kafka not a queue?**
A queue's defining property is "consume removes the message." Kafka stores records for a configured retention period regardless of how many times they're read. Multiple independent consumer groups can read the same partition without interfering with each other. The broker doesn't track per-consumer state; the consumer tracks its own offset.

**Q: What is the difference between `acks=1` and `acks=all`?**
`acks=1` means the leader writes to its log and responds — followers replicate asynchronously, so data is lost if the leader dies before replication. `acks=all` means the leader waits for all current ISR members to replicate before responding. Combined with `min.insync.replicas=2` and RF=3, `acks=all` survives one broker failure with no data loss.

**Q: What is the ISR and why does it matter?**
The In-Sync Replica set is the dynamic set of replicas (including the leader) that are caught up with the leader's log within `replica.lag.time.max.ms`. With `acks=all`, the producer waits for all ISR members to replicate. ISR shrinking means fewer replicas to wait for (faster but less durable); ISR shrinking below `min.insync.replicas` means writes are rejected entirely.

**Q: What is the High Watermark and why can't consumers read past it?**
The HW is the highest offset replicated to all ISR members. Consumers can only read records with offset < HW. If a consumer could read uncommitted records (LEO > HW), and the leader then crashed, a new leader from a follower without those records could be elected — the consumer would have observed records that no longer exist. The HW prevents this read-uncommitted anomaly.

**Q: How does Kafka achieve such high throughput?**
Three mechanisms: (1) sequential I/O — appends to log files are sequential writes; (2) zero-copy via `sendfile(2)` — record bytes go directly from page cache to socket without entering user space; (3) page cache — Kafka uses minimal JVM heap and gives the rest of RAM to the kernel for caching, so most reads are served from memory.

**Q: Why is the JVM heap kept small (e.g., 6 GB) on a Kafka broker?**
The page cache is more valuable than heap. A 64 GB machine with a 6 GB heap leaves ~58 GB for the kernel's page cache, where Kafka's hot data lives. A 48 GB heap would starve the page cache, force consumers to read from disk, and cause GC pauses to dominate p99 latency.

**Q: What is the difference between a producer's PID and its `transactional.id`?**
The PID is a broker-assigned, opaque, per-session identifier used for idempotent producer dedup. The `transactional.id` is a user-supplied, stable identifier used for transactions and fencing. When a producer with `transactional.id=X` reconnects, the coordinator bumps its epoch and fences any older instance still trying to write — protecting against zombie processes.

**Q: What is the difference between idempotent producer and transactions?**
The idempotent producer prevents duplicates caused by retries within a single producer session — the broker dedupes on `(PID, sequence_number)`. Transactions extend this to atomic multi-partition writes across producer restarts, and let you commit consumer offsets atomically with output records (the consume-process-produce pattern). Transactions require idempotence.

**Q: What is cooperative rebalancing and why does it matter?**
With eager (legacy) rebalancing, all consumers revoke all partitions during a rebalance, then receive new assignments — a stop-the-world pause. With cooperative rebalancing (2.4+), only partitions that need to move are revoked; others keep being processed. This eliminates the pause and is the default since Kafka 3.0.

**Q: What is `max.poll.interval.ms` and why does it cause problems?**
It's the max time between two `poll()` calls before the consumer is considered dead and rebalanced out. Default is 5 minutes. If processing a batch takes longer (e.g., calling a slow external API), the consumer is kicked out, the group rebalances, and the next consumer re-processes the same batch — duplicates plus thrashing. Fix by reducing `max.poll.records`, increasing the interval, or moving slow work to a worker pool.

**Q: What is `__consumer_offsets`?**
A compacted internal topic with 50 partitions (default) where consumer group offset commits are stored. Each (group_id, topic, partition) → offset is a key-value pair, and compaction keeps only the latest. The partition that handles a given group is computed by `hash(group_id) % 50`; the leader of that partition is the group's coordinator.

**Q: What is unclean leader election and when would you enable it?**
Normally, the controller elects a new leader only from the ISR. With `unclean.leader.election.enable=true`, it may elect from outside the ISR — meaning a follower that fell behind. This trades durability (records the old leader had but the new leader doesn't are lost) for availability (the partition stays online during a worst-case failure). Default is `false`; only enable for tolerable-loss topics like metrics or logs.

**Q: How does log compaction work and what topics use it?**
Compaction keeps only the latest value per key in a partition. A background `LogCleanerThread` reads rolled segments, builds a "latest offset per key" map, and rewrites the segments keeping only those records. Tombstones (null-value records) trigger physical removal of the key. Used for `__consumer_offsets`, `__transaction_state`, and any user topic representing "latest state per key" — CDC, configuration, Kafka Streams state stores.

**Q: What is the difference between compaction and retention deletion?**
Retention deletion removes entire rolled segments older than `retention.ms` or beyond `retention.bytes`. Compaction keeps only the latest value per key, removing older versions but not segments. They can coexist (`cleanup.policy=compact,delete`): keep latest per key, but also bound total log size.

**Q: Why can't you decrease partition count?**
Decreasing would require either dropping data or merging partitions (which would break per-partition ordering and offset numbering, used by every consumer's committed offset). The standard workaround is to create a new topic with fewer partitions and migrate by re-producing. Increasing is allowed but breaks key→partition stickiness — keys may now hash to different partitions, so same-key ordering is broken from the moment of expansion.

**Q: What is the sticky partitioner?**
The default partitioner since 2.4 for null-key records: instead of round-robin per record, the producer "sticks" to one partition until its batch is full, then picks another. This produces much larger batches with the same total record count, reducing per-record overhead and improving compression ratio. For non-null keys, the default still hashes the key.

**Q: What is KRaft and why does it replace ZooKeeper?**
KRaft (Kafka Raft Metadata) replaces ZooKeeper with a built-in Raft quorum running inside Kafka. The cluster metadata is stored in an internal Kafka topic (`__cluster_metadata`), replicated by a small set of controller nodes via Raft. Benefits: one fewer system to operate, support for millions of partitions, near-instant controller failover, constant cluster startup time. KRaft is GA since 3.3, and ZooKeeper is removed entirely in Kafka 4.0.

**Q: What is the difference between `read_committed` and `read_uncommitted`?**
`isolation.level` controls how the consumer handles transactional records. `read_uncommitted` (default) returns records as soon as they're appended, including records in open transactions. `read_committed` returns only records from committed transactions — the consumer waits past the Last Stable Offset (LSO) before exposing later records. Required for exactly-once stream processing.

**Q: How does `min.insync.replicas` interact with `acks=all`?**
`acks=all` waits for all current ISR members to replicate. `min.insync.replicas` is the floor below which writes are rejected: if ISR shrinks to fewer than `min.insync.replicas`, producers with `acks=all` get `NOT_ENOUGH_REPLICAS`. With RF=3 and `min.insync.replicas=2`, one broker failure is tolerated; two broker failures block writes (consistency over availability).

**Q: What is the page cache and why does it dominate Kafka performance?**
The Linux kernel caches recently-read and recently-written file data in unused RAM. Kafka appends go to page cache and are flushed to disk asynchronously; consumer reads typically hit page cache rather than disk. Combined with `sendfile(2)`, this means most reads never leave kernel memory — gigabytes per second throughput on a single broker.

**Q: What does `sendfile(2)` do for Kafka?**
`sendfile` copies bytes from a file descriptor to a socket inside the kernel, without copying into user space. For a fetch, Kafka calls `sendfile` on the log file → socket. The records are sent over the network without being deserialized into JVM objects or copied through application buffers. This is zero-copy, and it disables when SSL is on (encryption requires reading the bytes).

**Q: When does zero-copy NOT apply?**
When SSL/TLS is enabled on the broker listener. SSL requires reading the bytes to encrypt them, so they must be pulled into user space first. This is the main performance cost of enabling TLS on a Kafka broker — typically 30–40% throughput loss on a heavily-trafficked broker.

**Q: What is rack awareness and why is it important?**
Setting `broker.rack=<az>` per broker makes the controller's replica assignment algorithm spread replicas across racks (AZs). With RF=3 across 3 AZs, one AZ failure loses 1 of 3 replicas — ISR shrinks to 2 but `min.insync.replicas=2` is still met, so writes continue. Without rack awareness, all three replicas might land in the same AZ — one AZ failure takes the partition offline.

**Q: How does MirrorMaker 2 differ from a stretch cluster?**
A stretch cluster is a single Kafka cluster spanning multiple AZs (or regions); replication is the standard ISR mechanism, just across more failure domains. MM2 is a separate Kafka Connect-based system that copies records from one cluster to another, with offset translation via the offset-syncs topic. Stretch is for same-region multi-AZ HA; MM2 is for cross-region DR.

**Q: Why doesn't Kafka have a built-in `count()` aggregation?**
Kafka is a log, not a database — it has no concept of "current state" or aggregations. Counting requires Kafka Streams or an external stream processor (Flink, Spark Streaming). For "give me current state per key," use a compacted topic and rebuild a materialized view.

**Q: What is the Log End Offset vs the Last Stable Offset?**
LEO is the offset of the next record to be appended (essentially `log size`). The HW is the highest offset replicated to all ISR (consumer-visible boundary in `read_uncommitted`). The LSO is the smallest offset of any open transaction — consumers in `read_committed` mode can only read past records with offset < LSO. So `LSO ≤ HW ≤ LEO`.

**Q: What is a "preferred leader" and why does it matter?**
When a topic is created, the controller assigns one of the replicas as the **preferred leader** — the first one in the replica list. On failover, a different replica becomes leader, but the preferred leader is the "natural" owner. After the failed broker recovers, leadership can be rebalanced back to the preferred leader (`PREFERRED` election) so that load distribution returns to its original balance.

**Q: What is the role of `delete.retention.ms` in compacted topics?**
After a tombstone (null-value record) is processed by compaction, it's kept for `delete.retention.ms` (default 24h) before being physically removed. This gives consumers time to observe the deletion. If purged immediately, a consumer that started before the tombstone but read after compaction would never know the key was deleted.

**Q: What is `unclean.leader.election.enable=false` and why is it the default?**
With this set to `false`, the controller refuses to elect a leader from outside the ISR. If the entire ISR is unavailable, the partition stays offline until at least one ISR replica recovers. This guarantees no committed data is lost. Set to `true` only for topics where availability trumps durability (and document the loss possibility).

**Q: How do you choose the number of partitions for a new topic?**
Three factors: (1) consumer parallelism — partitions = max concurrency you'll ever need in a single consumer group; (2) producer throughput per partition (~10–30 MB/s sustained for a single partition); (3) operational overhead — millions of partitions strain even KRaft. Start with 6–24 partitions for most topics; reserve 100+ for very high-throughput or high-parallelism workloads.

**Q: What is `linger.ms` and why does setting it improve throughput?**
`linger.ms` is how long the producer waits to accumulate records into a batch before sending. Default 0 (send immediately). Setting it to 5–20 ms lets many records pile into a single batch, dramatically reducing per-record overhead (network, broker request handling, compression ratio). A small p50 latency cost (the linger delay) buys 5–10× throughput gain at moderate load.

**Q: What is `enable.idempotence=true` and what does it require?**
Enables the idempotent producer — broker dedupes duplicate batches caused by retries. Required settings: `acks=all`, `max.in.flight.requests.per.connection ≤ 5`, `retries > 0`. Default `true` since Kafka 3.0. Prevents most "I got a duplicate" complaints from downstream systems without requiring full transactions.

**Q: What's the difference between `commitSync` and `commitAsync`?**
`commitSync` blocks until the commit is acknowledged by the coordinator; on failure, retries automatically. `commitAsync` is fire-and-forget; on failure, does not retry (because a later commit may have superseded the failed one). Common pattern: `commitAsync` in steady state for throughput, `commitSync` on shutdown for guaranteed final commit.

**Q: What is consumer lag and how do you alert on it?**
Lag = `(partition LEO) − (consumer committed offset)`. Alert on **rate of change**, not absolute value: a constant lag of 10,000 is fine if it's steady; a lag growing by 1,000/sec is a problem. Tools: `kafka-consumer-groups.sh`, Burrow, kafka-lag-exporter, MSK CloudWatch metrics, Confluent Control Center.

**Q: What is static group membership?**
`group.instance.id` (2.3+) is a stable per-consumer identifier. If a consumer with the same `group.instance.id` reappears within `session.timeout.ms` after disappearing, the coordinator does **not** rebalance — it keeps the same partition assignment. Essential for Kubernetes: pod restarts (rolling deploy) no longer trigger group-wide rebalances.

**Q: Why is there a `__transaction_state` topic?**
The transaction coordinator writes transaction state (begin, prepare, commit, abort) to this compacted internal topic. On coordinator failover, the new coordinator replays this log to recover open transactions. The topic is partitioned by `hash(transactional.id)`, so each `transactional.id` has a single coordinator.

**Q: What is the difference between a Kafka Streams KStream and KTable?**
KStream is an unbounded sequence of records — every record is independent. KTable is a changelog: each record represents an update to a key's value, and the table's "state" is the latest value per key. Operationally, a KTable is backed by a compacted topic and a local RocksDB store. Joins, aggregations, and windowing semantics differ between the two.

**Q: How does Kafka Streams achieve exactly-once?**
Set `processing.guarantee=exactly_once_v2`. Streams uses transactions to atomically (a) write output records, (b) write state store changelog updates, and (c) commit consumer offsets. A crash mid-processing aborts the transaction → no partial output → on restart, the application reprocesses from the last committed offset, with no duplicates and no loss.

**Q: What does Schema Registry give you that raw Kafka doesn't?**
Centralized schema storage with compatibility enforcement at produce time. Without it, every consumer must know the producer's schema out-of-band, and schema evolution breaks consumers silently. With it, producers register schemas, the registry rejects incompatible changes, and consumers fetch schemas by ID embedded in each record.

**Q: What is BACKWARD compatibility in Schema Registry?**
A new schema is BACKWARD-compatible if a consumer using the new schema can read records produced with the old schema. This allows the typical deploy order: producers upgrade first, consumers upgrade later. Adding optional fields is BACKWARD-compatible; removing required fields is not.

**Q: When would you use Kafka Connect instead of Kafka Streams?**
Connect for **integration** — moving data between Kafka and external systems (databases, S3, search engines). Streams for **transformation** — joining, aggregating, windowing, enriching records within Kafka. A typical pipeline: Connect source (Debezium → Kafka), Streams transform, Connect sink (Kafka → Elasticsearch).

**Q: What is the tiered storage feature?**
Tiered storage (KIP-405, GA in Kafka 4.0) offloads rolled segments to object storage (S3, GCS, Azure Blob) while keeping recent segments on local disk. Per-topic `local.retention.ms` controls how long data stays local; `retention.ms` controls total retention. Cold reads stream from object storage transparently. Saves 5–10× on storage cost and supports very long retention without expensive hot disk.

**Q: How does the request purgatory work?**
When a produce request has `acks=all`, the broker can't respond until ISR members replicate. Rather than block an I/O thread, the request is placed in a **purgatory** structure indexed by the offset it needs. When followers' fetch requests advance their LEO past that offset, the purgatory completes the request. Same mechanism handles delayed fetches (`fetch.min.bytes`).

**Q: What is the difference between log segment, log file, and log directory?**
A **log directory** is one of the broker's `log.dirs` (e.g., `/disk1/kafka`). A **partition log** lives in a subdirectory of one log dir (e.g., `/disk1/kafka/orders-0/`). A **segment** is a set of files within the partition log (one `.log`, one `.index`, one `.timeindex`, possibly `.snapshot`). The partition log is a sequence of segments — only the newest (active) one is written.

**Q: Why is JBOD preferred over RAID for Kafka log volumes?**
With RAID 0, a single disk failure kills the entire array → the broker loses all its partitions. With JBOD (each disk a separate log dir), a disk failure loses only the partitions on that disk; the controller reassigns leadership for those partitions to other replicas. Kafka's RF=3 provides the redundancy that RAID would have provided, more efficiently.

**Q: What is `replica.lag.time.max.ms`?**
The longest a follower can fail to fetch (or fail to catch up to the leader's LEO) before being removed from the ISR. Default 30 seconds. A network blip, GC pause, or slow disk that exceeds this kicks the follower out → ISR shrinks → `acks=all` producers no longer wait for it. When the follower catches up, it rejoins ISR.

**Q: How does the controller detect broker failures?**
In KRaft, brokers heartbeat to the active controller via the broker-to-controller gRPC channel. If heartbeats stop for `broker.session.timeout.ms` (default 9 seconds), the controller marks the broker fenced — removes it from ISR for all its partitions, triggers leader elections for partitions where it was leader. In ZooKeeper mode, ephemeral znode expiry was the trigger.

**Q: What is the difference between `RecordTooLargeException` and `MessageSizeTooLargeException`?**
`RecordTooLargeException` is thrown by the producer client when a single record exceeds `max.request.size` (default 1 MB). `MessageSizeTooLargeException` (server-side) means the broker rejected a batch exceeding `message.max.bytes` (default 1 MB topic-level) or `replica.fetch.max.bytes`. Both indicate the record exceeds size limits — fix by increasing limits on producer + broker + topic, or by splitting the payload.

**Q: What is the role of `group.initial.rebalance.delay.ms`?**
When a consumer group is forming for the first time (no previous members), the coordinator waits this long (default 3 seconds) for additional members to join before assigning partitions. This avoids the rebalance storm of "1 consumer joins, gets all partitions; 2nd joins, full rebalance; 3rd joins, full rebalance; ...". Setting to 0 in dev for fast startup; default in prod is reasonable.

**Q: How does Kafka achieve strict per-partition ordering despite retries?**
Each partition has exactly one leader. The leader serializes appends — offsets are assigned in receive order on the leader. With the idempotent producer, retries are deduplicated by `(PID, sequence)`, so retries don't reorder records. Without idempotence and `max.in.flight.requests.per.connection > 1`, retries CAN reorder — that's why ordering-strict producers need `enable.idempotence=true` or `max.in.flight.requests.per.connection=1`.

**Q: What's the difference between `seek()` and committing an offset?**
`seek(partition, offset)` changes the consumer's in-memory read position — the next `poll()` returns records from that offset. It does not write to `__consumer_offsets`. A commit writes to `__consumer_offsets` so that another consumer (after a rebalance or restart) resumes there. You can `seek()` without committing (e.g., to replay).

**Q: What is `auto.offset.reset`?**
What the consumer does when it has no committed offset (new group, or committed offset no longer exists due to retention). Options: `latest` (start from new records only — default), `earliest` (read all available records from the beginning), `none` (throw if no offset). For analytics replay, use `earliest`; for live processing where missing past data is acceptable, `latest`.

**Q: Why might a consumer's committed offset be "out of range"?**
The committed offset is older than `log.retention.ms` and the segments containing it have been deleted. On `poll()`, the consumer gets an `OffsetOutOfRangeException`. The consumer then uses `auto.offset.reset` to recover. Common cause: a consumer was stopped for longer than retention; on restart, it can't resume where it left off.

**Q: What is the difference between throughput and latency in Kafka?**
Kafka is tuned for throughput by default. Throughput is records/sec or MB/sec — improved by larger batches, more partitions, higher `linger.ms`, compression. Latency is the time from `producer.send()` to consumer receiving the record — improved by smaller batches, lower `linger.ms`, fewer hops, and lower `min.insync.replicas`. They trade off; understand which your application cares about.

**Q: What is the `LeaderEpoch` and what bug does it prevent?**
A monotonically increasing counter incremented on each leader change. Followers include their last-known leader epoch in fetch requests. Without it, a subtle scenario can occur: a follower with stale data becomes leader, accepts new writes, then the old leader returns with different data at the same offsets — log divergence. Leader epoch lets the new leader rejecto stale-epoch fetches and ensures the truncation point on recovery is unambiguous.

**Q: What is the difference between Kafka topics and RabbitMQ exchanges/queues?**
RabbitMQ has exchanges (routing logic — direct, fanout, topic, headers) and queues (per-consumer message storage). Routing is dynamic and flexible. Kafka has only topics+partitions; routing is fixed by the partition key. Kafka doesn't track per-consumer message state; consumers track their own offsets and can replay. RabbitMQ deletes a message once acked; Kafka retains by time.

**Q: Why can't Kafka guarantee message delivery in strict total order across a topic?**
Total ordering requires all writes to go through a single point — a bottleneck. Kafka partitions trade total ordering for parallelism: ordering within a partition is strict, but no ordering exists across partitions. If you need total ordering, use a single partition — at the cost of being unable to scale that topic's throughput beyond one broker.

**Q: How do you ensure same-customer events stay in order?**
Use the customer ID as the record key. The default partitioner hashes the key, so all events for the same customer land on the same partition, where ordering is guaranteed. This is the canonical Kafka pattern for "per-entity ordering": pick a key such that the entity is the unit of ordering.

**Q: What happens when you increase a topic's partition count?**
New partitions are added at the end (highest IDs). Existing records remain on their original partitions. The default partitioner now hashes keys modulo the new partition count, so a key that previously hashed to partition 3 may now hash to partition 7 — **same-key ordering is broken from the moment of expansion onward**. Workaround: copy to a new topic with the desired partition count via a stream processor.

**Q: What's a "consumer rebalance storm"?**
A series of back-to-back rebalances where each consumer's brief unavailability triggers another rebalance, snowballing into an outage. Causes: unstable consumers (frequent restarts, OOM, slow `poll()`), short `session.timeout.ms`, no static membership. Mitigations: static membership (`group.instance.id`), cooperative rebalancing, larger `max.poll.interval.ms`, fix the underlying instability.

**Q: What is `delivery.timeout.ms` on the producer?**
The total time the producer client allows for a record from `send()` to final ack or final failure. Includes `linger.ms`, batching, network round trips, retries, retry backoff. If this elapses, the callback fires with `TimeoutException`. Default 2 minutes. Tune this together with `retries`, `retry.backoff.ms`, and `request.timeout.ms` — `delivery.timeout.ms` should be at least `linger.ms + request.timeout.ms`.

**Q: Why is Kafka considered a "distributed commit log"?**
The defining data structure is an append-only, immutable, ordered log of records — exactly what a commit log is in a database. "Distributed" because it's partitioned and replicated across many brokers. Adding pub/sub semantics on top (consumers track offsets independently, retention is time-based) makes it a streaming platform, but the commit log is the heart.

**Q: How does Kafka compare to a database write-ahead log (WAL)?**
A database's WAL is also an append-only log of changes. The difference: a database's WAL is private to the database and consumed by exactly one process (the database itself) to apply changes to data pages. Kafka's "WAL" is the user-facing storage, consumed by many independent applications. The same insight (sequential append is fast and durable) drives both designs.

**Q: What is end-to-end latency in Kafka?**
Time from `producer.send()` returning to the consumer's `poll()` returning the record. Components: producer batching (`linger.ms`), network to broker, broker append + replicate to ISR (for `acks=all`), broker waits for fetch (`fetch.min.bytes` / `fetch.max.wait.ms`), network to consumer, consumer poll loop overhead. Tuning end-to-end latency means tuning each step; the dominant term varies by configuration.

**Q: Why does Kafka have a __consumer_offsets topic with 50 partitions?**
The 50 partitions (default `offsets.topic.num.partitions`) distribute group coordinator load across many brokers — each broker hosts the leader for some subset of the 50 partitions, and groups whose `hash(group_id) % 50` lands there are coordinated by that broker. Otherwise, all groups in the cluster would funnel through a single coordinator broker.

**Q: What is the cost of running with very many partitions?**
Each partition consumes broker resources: open file handles (5–6 per partition: .log, .index, .timeindex, and replication state), metadata cache memory on every broker, replication fetcher cycles, controller bookkeeping, ZK or KRaft state. With ZooKeeper, ~200K partitions per cluster was a practical cap. KRaft pushes this to millions, but each partition still costs memory and bookkeeping. Don't over-partition "to be safe."

**Q: How would you detect that you're hitting the page cache miss rate?**
Watch broker disk read I/O (`iostat -x 1`). In normal operation, log appends generate write I/O but consumer fetches generate near-zero read I/O — reads come from page cache. High disk read rate while consumers are active means consumers are reading data old enough to have been evicted from page cache. Fix: more RAM, smaller heap, faster disks (if reads from cold tier are expected), or shorter consumer lag tolerance.

**Q: What is the relationship between Kafka and the "log-structured merge tree" (LSM)?**
Kafka is not an LSM. An LSM (Cassandra, RocksDB) is an indexed key-value structure that uses logs as the write path and merges them into sorted on-disk structures for reads. Kafka is just the log — there's no in-memory memtable, no per-key index, no compaction-for-read-speed. The "log compaction" feature is similar in name only — it dedupes by key for retention, not for read efficiency.

**Q: What metric should you alert on for "Kafka is broken"?**
`OfflinePartitionsCount > 0` is the most direct signal — at least one partition has no leader, so producers and consumers for that partition are failing. Follow closely by `UnderReplicatedPartitions > 0` (durability degraded), `ActiveControllerCount != 1` cluster-wide, growing consumer lag, and a sharp drop in `MessagesInPerSec` (clients are failing to produce).

**Q: How does Kafka handle a slow consumer?**
The consumer falls behind — its lag grows. The broker doesn't apply back-pressure on the producer; producers continue writing at their own rate. The slow consumer reads from page cache for recent data and disk for older data. If the consumer falls so far behind that data has been deleted by retention, on the next `poll()` it gets `OffsetOutOfRangeException` and resets per `auto.offset.reset`. The producer is never blocked by a slow consumer — this decoupling is a core design property.

**Q: What is exactly-once semantics (EOS) at the Kafka level?**
End-to-end exactly-once in a consume-process-produce pipeline. Achieved by: (1) idempotent producer (no duplicates from retries), (2) transactions that atomically commit output records and consumer offsets, (3) consumers with `isolation.level=read_committed` (don't read aborted records). The whole chain — input read, processing, output write, offset commit — succeeds or fails atomically. Kafka Streams wraps this in a single configuration: `processing.guarantee=exactly_once_v2`.

**Q: When should you NOT use Kafka?**
Small datasets (< 10 GB total — operational overhead not justified). Strict global ordering required (Kafka's parallelism is per-partition). Complex routing logic (RabbitMQ exchange types fit better). Request/reply RPC patterns (use gRPC). Workloads requiring sub-millisecond latency (Redis Streams or in-process queues are lower latency). Very small teams with no streaming experience (managed service or simpler queue may have lower TCO).
