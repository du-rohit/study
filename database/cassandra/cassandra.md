# Apache Cassandra — In-Depth Study Notes (v2)

> Organized to build understanding progressively:
> foundations → data model → single-node internals → distributed mechanics → application layer → operations → review.

---

# Part 1 — What and Why

## What is Cassandra?

Apache Cassandra is a distributed, wide-column NoSQL database designed for high availability and linear scalability across commodity hardware. Originally developed at Facebook (2008) and open-sourced via Apache in 2009.

- **Data model:** Wide-column store (keyspaces → tables → rows identified by partition key + clustering columns)
- **Query language:** CQL (Cassandra Query Language) — SQL-like syntax
- **CAP classification:** AP system — prioritizes Availability and Partition Tolerance; consistency is tunable per operation
- **Replication:** Leaderless, peer-to-peer (no primary/replica distinction)
- **Written in:** Java (runs on JVM — GC behavior directly impacts latency)

---

## Cassandra vs Other Databases

| | Cassandra | DynamoDB | Redis | MySQL | ScyllaDB |
|---|---|---|---|---|---|
| Model | Wide-column | Key-value/document | In-memory KV | Relational | Wide-column |
| Consistency | Tunable (AP) | Tunable (eventual/strong) | Single-node strong | Strong (ACID) | Tunable (AP) |
| Joins | No | No | No | Yes | No |
| Language | CQL | PartiQL / SDK | Commands | SQL | CQL |
| Written in | Java | Proprietary | C | C | C++ |
| Managed cloud | Astra DB (DataStax) | AWS native | ElastiCache | RDS/Aurora | ScyllaDB Cloud |
| GC pauses | Yes (JVM) | N/A | No | No | No |
| Best for | High-throughput time-series, IoT, messaging | Serverless/AWS-native | Caching, sessions | Transactional | High-throughput C*-compatible |

---

## Versions

- **Cassandra 3.x** (2015–2018): Materialized views, SASI indexes, per-table compaction options
- **Cassandra 4.0** (2021): Java 11 support, audit logging, full query logging, transient replication (preview), virtual tables, improved streaming, incremental repair GA, vnodes default reduced from 256 → 16
- **Cassandra 4.1** (2022): pluggable memtable API, ACCORD distributed transactions (preview), guardrails system
- **Cassandra 5.0** (2024): SAI (Storage Attached Index) replaces SASI, JDK 17 support, trie-based memtables and SSTables (BTI format), vector search (ANN), ACCORD transactions closer to GA

---

# Part 2 — Data Model and CQL

## Data Model

```
Keyspace (analogous to a database/schema)
  └── Table (denormalized; no joins)
        └── Row = Partition Key + Clustering Columns + Regular Columns
```

### Partition Key

- Determines **which node(s)** store the row — hashed via Murmur3 to a token
- All rows sharing the same partition key are **co-located on disk** in sorted order
- The partition is the unit of distribution, replication, and atomic writes
- **Hot partitions** are the #1 performance anti-pattern — one node absorbs disproportionate load
- Max recommended partition size: ~100 MB (larger degrades compaction and repairs)

### Clustering Columns

- Define **sort order** of rows within a partition (ASC/DESC configurable)
- Enable efficient range queries within a partition: `WHERE pk = X AND ck >= Y AND ck <= Z`
- Cannot be skipped in WHERE clause without ALLOW FILTERING

### Primary Key

```cql
PRIMARY KEY ((partition_key), clustering_col1, clustering_col2)
-- (( )) wraps composite partition key
-- clustering columns follow
```

### Skinny vs Wide Rows

- **Skinny row:** one row per partition key (e.g., user profile)
- **Wide row:** many rows per partition key, differentiated by clustering columns (e.g., all events for a user ordered by timestamp)

Wide rows are Cassandra's strength — they enable time-ordered data retrieval with a single partition fetch.

---

## CQL Basics

```cql
-- Create keyspace (single DC)
CREATE KEYSPACE mykeyspace
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

-- Create keyspace (multi-DC)
CREATE KEYSPACE mykeyspace
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3};

-- Create table
CREATE TABLE mykeyspace.events (
  user_id   UUID,
  event_ts  TIMESTAMP,
  event_type TEXT,
  payload   TEXT,
  PRIMARY KEY (user_id, event_ts)
) WITH CLUSTERING ORDER BY (event_ts DESC);

-- Insert
INSERT INTO mykeyspace.events (user_id, event_ts, event_type, payload)
  VALUES (uuid(), toTimestamp(now()), 'click', '{"page": "home"}');

-- Range query within a partition (efficient)
SELECT * FROM mykeyspace.events
  WHERE user_id = <uuid> AND event_ts >= '2024-01-01' AND event_ts <= '2024-01-31';

-- Update (actually an upsert — Cassandra merges by timestamp)
UPDATE mykeyspace.events
  SET payload = '{"page": "pricing"}'
  WHERE user_id = <uuid> AND event_ts = <ts>;

-- Delete
DELETE FROM mykeyspace.events WHERE user_id = <uuid> AND event_ts = <ts>;

-- TTL (auto-expire data after N seconds)
INSERT INTO mykeyspace.sessions (session_id, data)
  VALUES ('abc', 'xyz') USING TTL 3600;

-- Lightweight transaction (CAS — compare-and-set)
INSERT INTO mykeyspace.users (user_id, email)
  VALUES (uuid(), 'alice@example.com') IF NOT EXISTS;

-- Batch (same partition only for atomicity)
BEGIN BATCH
  INSERT INTO mykeyspace.events ...;
  INSERT INTO mykeyspace.events ...;
APPLY BATCH;
```

### CQL Data Types

| Category | Types |
|---|---|
| Numeric | `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL`, `VARINT`, `COUNTER` |
| Text | `TEXT`, `VARCHAR`, `ASCII` |
| Time | `TIMESTAMP`, `DATE`, `TIME`, `DURATION` |
| UUID | `UUID`, `TIMEUUID` |
| Collections | `LIST<T>`, `SET<T>`, `MAP<K,V>`, `FROZEN<T>` |
| Other | `BOOLEAN`, `BLOB`, `INET` |

**TIMEUUID:** version-1 UUID — encodes a timestamp + MAC address. Sortable chronologically. Useful as a clustering column for ordering events without clock skew issues.

**COUNTER:** special column type for distributed counters. Cannot be mixed with non-counter columns in the same table. Uses a special internal protocol (no CAS; eventual consistency per shard).

**FROZEN:** serializes a collection into a single blob — must be replaced entirely on update, but can be used as a primary key component.

---

## NULL Semantics vs. Missing Columns

This is one of the most common pitfalls for developers coming from SQL databases.

### No True NULL in Cassandra

In SQL, `NULL` is a first-class value representing "unknown." In Cassandra, there is no true NULL. A column that has never been written simply **does not exist** in the SSTable — there is no storage cost.

```cql
-- Insert only user_id and name; email column never written
INSERT INTO mykeyspace.users (user_id, name) VALUES (uuid(), 'Alice');

-- Read: email appears as null in the result set
SELECT user_id, name, email FROM mykeyspace.users WHERE user_id = ?;
-- → user_id: <uuid>, name: 'Alice', email: null
```

The `null` you see in the client is the driver's representation of "column absent" — nothing was stored.

### Explicit NULL Write Creates a Tombstone

If you explicitly SET a column to null, Cassandra writes a **tombstone** for that column — a delete marker that costs storage and impacts read performance:

```cql
-- This writes a tombstone for email — not the same as "not writing email"
UPDATE mykeyspace.users SET email = null WHERE user_id = ?;

-- This also writes a tombstone
INSERT INTO mykeyspace.users (user_id, name, email) VALUES (uuid(), 'Bob', null);
```

**Consequence:** applications that populate all columns in an INSERT with null values generate tombstones for every absent field. In a wide table with many optional columns, this causes tombstone accumulation and read degradation.

### Best Practice

Only write columns that have actual values. Never insert null placeholder values. Use the absence of the column to signal "not set."

```cql
-- Bad: generates tombstones for all null fields
INSERT INTO users (user_id, name, email, phone, address)
  VALUES (uuid(), 'Alice', null, null, null);

-- Good: only write the columns you have data for
INSERT INTO users (user_id, name) VALUES (uuid(), 'Alice');
```

### Checking for Absent Columns in CQL

```cql
-- Filter for rows where a column has never been written (no tombstone)
SELECT * FROM mykeyspace.users WHERE email = null ALLOW FILTERING;
-- Note: this requires ALLOW FILTERING and is a full scan — not for production hot paths

-- Better: use IS NOT NULL in materialized view definitions
CREATE MATERIALIZED VIEW ... WHERE email IS NOT NULL AND user_id IS NOT NULL ...
```

---

## Counter Columns

Cassandra `COUNTER` columns implement distributed counters without requiring read-before-write at the application level.

### How Counters Work

Each replica maintains a **local counter cell** containing a per-replica increment delta. The visible counter value is the sum of all deltas across all replicas:

```
Global counter value = sum of all replica-local deltas

Replica 1: +5
Replica 2: +3
Replica 3: +2
→ visible count = 10
```

When you `UPDATE ... SET views = views + 1`, each replica receiving the write increments only its own local delta. No lock, no read needed — write is purely local per replica.

**Read path for counters:**
```
Coordinator reads from all RF replicas at QUORUM
  → each returns its local delta
  → coordinator sums all deltas → returns total
```

### Counter Table Rules

- A table with a `COUNTER` column **cannot** contain any non-counter regular columns (only primary key columns + counter columns)
- `COUNTER` columns cannot have a TTL or TIMESTAMP
- `COUNTER` columns cannot be part of the primary key

```cql
-- Correct: dedicated counter table
CREATE TABLE mykeyspace.page_views (
  page_id TEXT PRIMARY KEY,
  views   COUNTER
);

-- Increment
UPDATE mykeyspace.page_views SET views = views + 1 WHERE page_id = 'home';

-- Decrement
UPDATE mykeyspace.page_views SET views = views - 1 WHERE page_id = 'home';

-- Read
SELECT views FROM mykeyspace.page_views WHERE page_id = 'home';
```

### Counter Consistency and Idempotency

**Counters are not idempotent.** If a write times out and the driver retries, the increment may be applied twice:
```
Client: +1 → reaches all 3 replicas → ack lost in transit
Driver: timeout → retries +1 → applied again → counter = +2 instead of +1
```

Mitigations:
- Accept over-counting in use cases where approximate counts are acceptable (page views, analytics)
- Use LWT (`IF NOT EXISTS`) for exact counts — but this is very slow
- Deduplicate at the application level using idempotency keys

**Consistency:** counters use `QUORUM` / `LOCAL_QUORUM` for reads and writes. `ONE` reads may return stale sums if not all replica deltas have propagated.

### When to Use (and Not Use) Counters

**Good fit:**
- Page view counts, like counts, download counts
- Metrics aggregation where approximate values are acceptable
- High-write-rate counters where the increment doesn't need to be exact

**Poor fit:**
- Inventory counts where exact values are critical (double-increment on retry = oversell)
- Balances or financial figures (use LWT + regular columns instead)
- Situations where you need to read-then-conditionally-increment (use LWT)

---

## User-Defined Types, Static Columns, and Collections

### User-Defined Types (UDTs)

UDTs allow grouping related fields into a named type, reusable across tables.

```cql
-- Define UDT
CREATE TYPE mykeyspace.address (
  street TEXT,
  city   TEXT,
  zip    TEXT,
  country TEXT
);

-- Use in a table (must be FROZEN unless the whole UDT is the value)
CREATE TABLE mykeyspace.users (
  user_id UUID PRIMARY KEY,
  name    TEXT,
  address FROZEN<address>    -- FROZEN = serialized as single blob
);

-- Insert
INSERT INTO mykeyspace.users (user_id, name, address)
  VALUES (uuid(), 'Alice', { street: '123 Main St', city: 'NYC', zip: '10001', country: 'US' });

-- Update (must replace the entire frozen UDT — cannot update individual fields)
UPDATE mykeyspace.users SET address = { street: '456 Oak Ave', city: 'LA', zip: '90001', country: 'US' }
  WHERE user_id = <uuid>;
```

**`FROZEN` requirement:** UDTs used as column values in regular columns can be non-frozen (field-level updates possible). UDTs used in collections or as primary key components must be `FROZEN<T>` — serialized as a single opaque blob. Once frozen, the entire value must be replaced atomically.

### Static Columns

A static column stores **one value per partition** — shared across all rows in the partition, regardless of clustering column values.

```cql
CREATE TABLE mykeyspace.user_events (
  user_id    UUID,
  event_ts   TIMESTAMP,
  event_type TEXT,
  user_name  TEXT STATIC,    -- one value per user_id partition
  payload    TEXT,
  PRIMARY KEY (user_id, event_ts)
);

-- Set the static column value (applies to the entire partition)
UPDATE mykeyspace.user_events SET user_name = 'Alice' WHERE user_id = <uuid>;

-- All rows with user_id=<uuid> will return user_name='Alice'
SELECT user_id, event_ts, user_name, payload FROM mykeyspace.user_events
  WHERE user_id = <uuid>;
```

**Use case:** partition-level metadata that doesn't change per clustering row. Avoids storing the same value (e.g., user display name) in every row of a wide partition — saves storage and simplifies updates (change once, reflected everywhere in the partition).

**Gotcha:** deleting all clustering rows in a partition does NOT delete the static column value. Delete the static column explicitly or delete the partition entirely.

### Collections: Lists, Sets, Maps

Collections store multiple values in a single cell.

```cql
CREATE TABLE mykeyspace.user_tags (
  user_id UUID PRIMARY KEY,
  tags    SET<TEXT>,          -- no duplicates, unordered
  aliases LIST<TEXT>,         -- ordered, allows duplicates
  metadata MAP<TEXT, TEXT>    -- key-value pairs
);

-- Set operations
UPDATE mykeyspace.user_tags SET tags = tags + {'admin', 'beta'} WHERE user_id = ?;
UPDATE mykeyspace.user_tags SET tags = tags - {'beta'}          WHERE user_id = ?;

-- List operations
UPDATE mykeyspace.user_tags SET aliases = aliases + ['Alice']   WHERE user_id = ?;

-- Map operations
UPDATE mykeyspace.user_tags SET metadata['theme'] = 'dark'     WHERE user_id = ?;
DELETE metadata['theme'] FROM mykeyspace.user_tags             WHERE user_id = ?;
```

### Collection Pitfalls

**Unbounded collection growth** is the main hazard. When Cassandra reads a collection, it deserializes the entire collection from disk — not individual elements:

```
user_id=alice has 10,000 tags in a SET<TEXT>
→ SELECT tags FROM users WHERE user_id = alice
→ Cassandra reads ALL 10,000 tags from disk and sends them over the wire
→ even if you only need 1 tag
```

There is no "read one element from a collection" without reading the whole collection (except maps with a specific key query).

**Size limits:** Cassandra warns when a collection exceeds 65,535 elements.

**Frozen collections:** a frozen collection (`FROZEN<SET<TEXT>>`) is serialized as a single blob. The entire collection must be replaced on every update — no append/remove operations.

**Rule of thumb:** keep collections small (< 100 elements) and access patterns whole-collection. For large or independently-accessed element sets, model as separate rows with a clustering column instead.

### USING TIMESTAMP Semantics

Every mutation in Cassandra carries a microsecond-precision timestamp. By default, it is the coordinator's system clock at write time. You can override it:

```cql
INSERT INTO mykeyspace.users (user_id, email)
  VALUES (uuid(), 'alice@example.com')
  USING TIMESTAMP 1704067200000000;   -- microseconds since epoch

UPDATE mykeyspace.users SET email = 'new@example.com'
  WHERE user_id = ?
  USING TIMESTAMP 1704067200000001;
```

**Why this matters:**
- LWW conflict resolution uses these timestamps — the highest timestamp wins
- For data migration or replication from external systems, setting the correct source timestamp preserves the correct merge result
- Clock skew between nodes or between application servers and Cassandra nodes can cause out-of-order writes to silently win

**`USING TTL` with `USING TIMESTAMP`:**
```cql
INSERT INTO mykeyspace.sessions (session_id, data)
  VALUES ('abc', 'xyz')
  USING TIMESTAMP 1704067200000000 AND TTL 3600;
-- TTL countdown starts from the write timestamp, not wall-clock time
```

---

## BATCH Semantics and Anti-Patterns

CQL batches are frequently misused. Understanding what they actually guarantee is critical.

### Three Batch Types

#### Logged Batch (default)

Provides **atomicity** — all statements in the batch are applied or none are (Cassandra replays from a batch log on failure).

```cql
BEGIN BATCH
  INSERT INTO users_by_id   (user_id, email, name) VALUES (?, ?, ?);
  INSERT INTO users_by_email (email, user_id, name) VALUES (?, ?, ?);
APPLY BATCH;
```

**How it works:**
```
1. Coordinator writes the entire batch to a batch log on 2 nodes (quorum-replicated log)
2. Coordinator executes all statements in the batch
3. On success: coordinator deletes the batch log entry
4. On coordinator failure mid-execution: another node finds the batch log entry and replays it
```

**Cost:** +2 extra writes (batch log write to 2 nodes) + 1 extra delete on completion. For large batches or high-throughput paths, this overhead is significant.

#### Unlogged Batch

No atomicity guarantee — statements are sent together to reduce network round trips but are applied independently.

```cql
BEGIN UNLOGGED BATCH
  INSERT INTO events (user_id, ts, data) VALUES (?, ?, ?);
  INSERT INTO events (user_id, ts, data) VALUES (?, ?, ?);
APPLY BATCH;
```

**Only useful when:** all statements target the **same partition** on the **same node** — then they share a single CommitLog write and memtable mutation. For multi-partition unlogged batches, there is no benefit and the coordinator overhead is pure waste.

#### Counter Batch

Required for batching `COUNTER` column mutations — cannot mix counter and non-counter statements:

```cql
BEGIN COUNTER BATCH
  UPDATE page_views SET views = views + 1 WHERE page_id = 'home';
  UPDATE page_views SET views = views + 1 WHERE page_id = 'about';
APPLY BATCH;
```

### The Multi-Partition Logged Batch Anti-Pattern

**This is the most common Cassandra misuse pattern.**

A logged batch across multiple partitions is **slower and more expensive** than individual writes — not faster. The only "transaction" guarantee is that all statements happen eventually (via batch log replay). There is no isolation — other reads can see a partial state between statements.

**When logged batches are appropriate:**
- Maintaining a denormalized table pair where both must be updated atomically (e.g., `users_by_id` and `users_by_email`)
- The number of statements is small (< 10)
- The atomicity guarantee is worth the overhead

**When to avoid:**
- Batching for performance on unrelated partitions (no performance benefit)
- Simulating transactions with many statements (batch log overhead compounds)
- High-throughput paths where the batch log creates a write bottleneck

---

# Part 3 — Architecture

## JVM and Threading Model

Cassandra runs on the JVM (Java Virtual Machine). Each request is handled by a thread from a pool:

```
Incoming request → assigned to a thread from the pool
  Thread: parse CQL → route to replica(s) → wait for response(s) → serialize → return
  While waiting for I/O, the thread is BLOCKED — it holds memory and OS context
```

**Implications:**
- High concurrency → many blocked threads → OS context switching overhead
- Thread pool exhaustion → requests queued or rejected
- JVM GC pauses → latency spikes at p99/p999 — all threads stop during Stop-The-World GC
- Memory fragmentation from Java heap allocation patterns

**Thread pools (key ones):**

| Pool | Purpose |
|---|---|
| `Native-Transport-Requests` | CQL client request handling |
| `ReadStage` | Read operations |
| `MutationStage` | Write operations |
| `CompactionExecutor` | Background compaction |
| `MemtableFlushWriter` | Memtable → SSTable flushes |

Monitor via `nodetool tpstats` — high pending/blocked counts indicate saturation.

### GC Tuning

Cassandra's p99 latency is directly tied to GC behavior. Three common collectors:

| Collector | Use Case | Behavior |
|---|---|---|
| G1GC | General purpose (default since C* 4.0) | Concurrent marking, regional heap; occasional STW |
| ZGC | Low-latency, large heaps | Sub-ms pauses; Java 15+; higher CPU overhead |
| Shenandoah | Low-latency alternative | Concurrent compaction; reduces STW |

Common tuning flags:
```bash
# cassandra-env.sh
JVM_OPTS="$JVM_OPTS -Xms16G -Xmx16G"   # heap: set equal to prevent resize pauses
JVM_OPTS="$JVM_OPTS -XX:+UseG1GC"
JVM_OPTS="$JVM_OPTS -XX:G1RSetUpdatingPauseTimePercent=5"
JVM_OPTS="$JVM_OPTS -XX:MaxGCPauseMillis=300"
```

**Off-heap memory:** Cassandra allocates much of its data structures off-heap (bloom filters, compression metadata, row cache) via `sun.misc.Unsafe` to avoid GC pressure. Memtable data is on-heap unless using the off-heap memtable strategy.

---

## Token Ring and Consistent Hashing

### Murmur3 Partitioner

Every partition key is hashed to a 64-bit signed integer token:
```
Token space: -2^63  ─────────────────────────────── 2^63-1
Total size: 2^64 values (ring wraps around)
```

The token ring is divided among nodes. Each node is responsible for a range of tokens.

### Vnodes (Virtual Nodes)

Without vnodes: ring divided into N equal contiguous slices. Problem: uneven load if nodes have different data volumes; rebalancing requires streaming from a single neighbour.

With vnodes (default 16 per node in C* 4.0+, was 256 in older versions):
```
3 nodes × 16 vnodes = 48 token range slots
Each node owns 16 scattered, non-contiguous ranges

Node 1: [T0-T2], [T6-T8],   [T12-T14], ...  (scattered)
Node 2: [T2-T4], [T8-T10],  [T14-T16], ...
Node 3: [T4-T6], [T10-T12], [T16-T18], ...
```

**Why scatter?** When a node fails, its 16 ranges are spread across all remaining nodes — no single node absorbs all the traffic.

**Adding a node with vnodes:**
- New node gets 16 randomly assigned token positions
- Each existing node gives up ~1/N of its token ranges
- All existing nodes stream data simultaneously → cluster-wide I/O during rebalancing
- After bootstrapping: run `nodetool cleanup` on each existing node to delete data no longer owned

```yaml
# cassandra.yaml
num_tokens: 16   # default in C* 4.0+
```

---

# Part 4 — Storage Engine and Read/Write Path

## Storage Engine: LSM Tree (CommitLog → Memtable → SSTable)

Cassandra uses a **Log-Structured Merge Tree (LSM)** storage engine. Writes are always sequential — never in-place updates on disk.

### Write Path (Storage Layer)

```
Client Write
     │
     ▼
1. CommitLog  ──── appended first (WAL — durability before ack)
     │
     ▼
2. Memtable   ──── written to in-memory sorted structure (per table, per node)
     │
     │  (threshold exceeded / flush triggered)
     ▼
3. SSTable    ──── flushed to disk as immutable sorted file
                        │
                        │  (background, concurrent)
                        ▼
                   Compaction ──── merges SSTables, removes tombstones, re-sorts
```

### Write Path (Network / Coordinator Layer)

This is what happens at the cluster level when a client sends a write:

```
Client → Driver (token-aware routing) → Coordinator node
                                              │
                    ┌─────────────────────────┼─────────────────────────┐
                    │                         │                         │
                    ▼                         ▼                         ▼
              Replica 1                 Replica 2                 Replica 3
         (local write: CommitLog      (same)                    (same)
          + Memtable)
                    │                         │
                    └─────────────────────────┘
                         2 of 3 ack → coordinator acks client (QUORUM)
                         (Replica 3 still writing asynchronously)
```

**Key behaviors:**
- The coordinator sends the write to **all replicas simultaneously** — not sequentially
- It only waits for the CL-required count before acking the client
- Replicas that haven't acked yet still receive and process the write — the client just doesn't wait
- If a replica is down, the coordinator stores a **hint** and moves on (if CL is still met)
- Each replica applies the write independently: CommitLog → Memtable — no coordinator involvement in storage

**Hinted handoff during write:**
```
Write pk=alice, QUORUM, RF=3:
  Replica 1 ✓, Replica 2 ✓ → quorum met → ack to client
  Replica 3 ✗ (down)       → coordinator stores hint for Replica 3
  Replica 3 recovers → hint replayed → data becomes consistent
```

### 1. CommitLog

**Purpose:** Write-ahead log (WAL) — ensures no data is lost if the node crashes before the memtable is flushed.

- Every write appended to CommitLog **before** the memtable write — guarantees crash durability
- Sequential append-only → very fast (no random I/O)
- Divided into fixed-size segments (default 32 MB via `commitlog_segment_size_in_mb`)
- A segment is deleted once all mutations in it have been flushed to SSTables
- On startup after crash: Cassandra replays CommitLog to rebuild unflushed memtable data

**CommitLog sync modes:**
- `periodic` (default): fsync every `commitlog_sync_period_in_ms` (default 10,000 ms). Up to 10s of data could be lost on crash, but much faster writes.
- `batch`: waits for fsync before acknowledging write. Durable but slower (~30% lower write throughput).
- `group`: batches multiple writes into one fsync window — compromise between periodic and batch.

### 2. Memtable

**Purpose:** In-memory write buffer. Absorbs writes; I/O happens in large sequential batches.

- Lives entirely in RAM (one per table per node)
- Data sorted by partition key + clustering columns (enables efficient SSTable flush)
- Reads check the memtable first — cache hit means no disk I/O for recent writes
- Flush triggered by: heap pressure, flush period timer, or `nodetool flush`
- In C* 4.1+: pluggable memtable API allows off-heap or alternative implementations
- In C* 5.0: trie-based memtable is the default (see BTI Format section)

### 3. SSTable (Sorted String Table)

**Purpose:** Immutable on-disk storage. Once written, an SSTable is never modified — it is only replaced by compaction.

An SSTable is a **set of files** written atomically:

| File | Purpose |
|------|---------|
| `*-Data.db` | Actual row data, sorted by partition key + clustering columns |
| `*-Index.db` | Partition key → byte offset in Data.db (sparse — one entry per partition) |
| `*-Summary.db` | Coarser in-memory index of Index.db — held in RAM for fast lookup |
| `*-Filter.db` | Bloom filter — probabilistic "does this partition exist?" check |
| `*-Statistics.db` | Metadata: min/max timestamps, tombstone count, column stats |
| `*-CompressionInfo.db` | Maps compressed chunks to uncompressed offsets |
| `*-TOC.txt` | Table of contents — lists all files in this SSTable |

### C* 5.0 BTI Format (Big Trie-based Index)

C* 5.0 introduces a new SSTable format (`-oa` version, also called BTI — Big Trie-based Index) that replaces the old `Index.db` + `Summary.db` two-level index with a trie-based structure.

**Old format (pre-5.0):**
```
Summary.db (in-memory, coarse) → Index.db (on-disk, fine) → Data.db
  Two disk reads for a partition lookup (Summary → Index → Data)
```

**New BTI format (C* 5.0):**
```
*-Partitions.db  ← trie index of partition keys (replaces Index.db + Summary.db)
*-Rows.db        ← trie index of rows within a partition (replaces row index in Data.db)
*-Data.db        ← actual row data (same concept, new format)
```

**Benefits of trie-based index:**
- **Faster partition lookups:** trie traversal is O(key length) — no binary search over Index.db
- **Smaller memory footprint:** trie shares common key prefixes; more compact than a B-tree-style sparse index
- **Faster startup:** trie can be loaded/memory-mapped faster than scanning Index.db
- **Better prefix query performance:** trie naturally supports prefix lookups (relevant for SAI)

**Trie-based memtable (C* 5.0):**
The in-memory memtable also uses a trie structure instead of a skip-list/red-black tree. Benefits:
- Better cache locality (trie nodes are compact)
- Reduced GC pressure (fewer Java objects per key)
- Faster iteration during flush (sorted output is natural from a trie)

**Migration:** existing SSTables in old format continue to be readable. New flushes produce BTI-format SSTables. Run `nodetool upgradesstables` to rewrite old SSTables to the new format.

#### Read Path Through an SSTable

```
Read pk=alice
     │
     ▼
1. Bloom Filter (Filter.db, in RAM)
     "definitely not here" → skip this SSTable entirely (zero disk I/O)
     "maybe here"         → continue
     │
     ▼
2. (Old) Summary.db (in RAM) → Index.db (disk read) → exact byte offset
   (New BTI) Partitions.db trie → direct offset lookup
     │
     ▼
3. Data.db (disk read)
     → read partition data
```

### Read Path: Full Picture

```
Client Read
     │
     ├──▶ Row Cache (if enabled) ──▶ HIT → return immediately
     │
     ├──▶ Memtable ──▶ check for most recent in-memory writes
     │
     └──▶ SSTables (from newest to oldest, in parallel)
               │
               ├── Bloom Filter → skip if "definitely not here"
               ├── Partition index → Data.db
               └── merge all versions (highest timestamp wins; tombstones applied)
                         │
                         ▼
                    Return merged result to coordinator
```

**Key insight:** a single read may hit multiple SSTables (one per flush since last compaction). This is **read amplification** — the main cost of LSM vs B-tree for reads. Compaction reduces the number of SSTables, directly improving read performance.

### LSM vs B-Tree Trade-offs

| | LSM (Cassandra) | B-Tree (MySQL/Postgres) |
|---|---|---|
| Write speed | Very fast (sequential append to memtable + CommitLog) | Slower (random in-place page updates) |
| Read speed | Slower (may scan multiple SSTables; merge required) | Faster (single tree traversal) |
| Space amplification | Higher (multiple SSTable versions until compaction) | Lower (in-place updates) |
| Write amplification | Lower initially; higher during compaction | Higher during writes |
| Delete semantics | Tombstones (logical delete; physical purge at compaction) | Immediate in-place delete |
| Best for | Write-heavy: time-series, logs, IoT | Read-heavy, transactional workloads |

---

## Compaction

Compaction merges multiple SSTables into fewer, larger ones. It removes tombstones (deleted data markers), deduplicates overwritten values (keeping highest timestamp), and re-sorts data to improve read performance.

### Why Compaction Matters

Without compaction:
- Reads scan an ever-growing number of SSTables → read amplification grows unboundedly
- Tombstones pile up; disk space is not reclaimed
- Old SSTable versions consume disk space indefinitely

### Tombstones and gc_grace_seconds

A `DELETE` in Cassandra does **not** immediately remove data. It writes a **tombstone** — a marker with a timestamp. The actual row data in older SSTables is still there until compaction.

`gc_grace_seconds` (default 10 days): tombstones are physically purged during compaction only after this window. Why? If a tombstone is purged too early and a replica that missed the delete comes back online, the old data "resurrects" — **zombie data**.

```
t=0:    DELETE pk=alice → tombstone written on all replicas
t=3d:   Replica 3 was down and missed the tombstone
t=10d:  gc_grace_seconds expires → tombstone purged from Replicas 1 & 2
t=10d+: Replica 3 rejoins with pk=alice still alive
        → reads may return zombie data
```

**Prevention:** run `nodetool repair` at least once within every `gc_grace_seconds` window.

### Tombstone Thresholds

Tombstone accumulation degrades read performance because Cassandra must scan past them to find live data. Cassandra has configurable thresholds to guard against this:

```yaml
# cassandra.yaml
tombstone_warn_threshold: 1000      # default: log WARN when a read scans > 1000 tombstones
tombstone_failure_threshold: 100000 # default: abort read and throw TombstoneOverwhelmingException
```

**When `TombstoneOverwhelmingException` occurs:**
- A read scanned > 100,000 tombstones before finding enough live rows to satisfy the query
- The read is aborted — the client gets an error, not partial data
- Root causes: deleting data but not running compaction, TTL expiry without compaction, anti-pattern of writing null values in bulk

**Diagnosis and mitigation:**
```bash
# Check tombstone stats per table
nodetool tablestats mykeyspace.events | grep -i tombstone

# Force compaction to physically remove tombstones
nodetool compact mykeyspace events

# If tombstones are from TTL data, switch to TWCS compaction strategy
# which naturally expires whole SSTable windows
```

**Design to avoid tombstone accumulation:**
- Use TWCS for TTL-based data (entire SSTables expire without per-row tombstones)
- Avoid writing null values explicitly — let absent columns be absent
- Run regular compaction and repair

### Compaction Strategies

#### Size-Tiered Compaction Strategy (STCS)

Default strategy. Groups SSTables of similar size, merges them when count reaches a threshold (default 4).

- **Best for:** write-heavy workloads (IoT, logs, time-series with infrequent reads)
- **Problem:** high space amplification — during compaction, old + new files coexist on disk (2× space peak)
- **Problem:** reads may scan many SSTables between compaction cycles

```cql
ALTER TABLE mykeyspace.events
  WITH compaction = {'class': 'SizeTieredCompactionStrategy',
                     'min_threshold': 4,
                     'max_threshold': 32};
```

#### Leveled Compaction Strategy (LCS)

Organizes SSTables into levels. L0 accepts flushes; compaction pushes data up through levels.

```
L0: 4 SSTables (any size, overlapping key ranges)
 → compact all L0 into L1 files (non-overlapping, 160MB each)
L1: N × 160MB files (non-overlapping key ranges)
 → compact one L1 file with overlapping L2 files
L2: N × 1.6GB files
L3: N × 16GB files
```

Each level is 10× larger than the previous. At any level > 0, SSTables have **non-overlapping key ranges** → a partition exists in at most 1 SSTable per level → reads touch at most 1 SSTable per level → low read amplification.

- **Best for:** read-heavy workloads; mixed read/write
- **Reads:** predictable, fast (log(levels) SSTable lookups max)
- **Problem:** high write amplification — data rewritten across levels repeatedly
- **Problem:** compaction is near-continuous → higher I/O than STCS at rest

```cql
ALTER TABLE mykeyspace.users
  WITH compaction = {'class': 'LeveledCompactionStrategy',
                     'sstable_size_in_mb': 160};
```

#### Time-Window Compaction Strategy (TWCS)

Designed for time-series data with TTL. Organizes SSTables into time windows; only compacts within the same window.

- **Best for:** time-series data where old data expires (TTL) and is never updated
- **Why it works:** data naturally ages out — entire window SSTables can be dropped at TTL expiry without reading every tombstone
- **Critical requirement:** all data in a table must use the same TTL; do NOT mix TTL and non-TTL data
- **Never use TWCS without TTL** — windows never compact across boundaries, causing file proliferation

```cql
ALTER TABLE mykeyspace.sensor_readings
  WITH compaction = {'class': 'TimeWindowCompactionStrategy',
                     'compaction_window_unit': 'HOURS',
                     'compaction_window_size': 1}
  AND default_time_to_live = 2592000;   -- 30 days
```

#### Compaction Strategy Decision Matrix

| Workload | Strategy | Reason |
|---|---|---|
| Write-heavy, few reads (logs, IoT) | STCS | Minimal write amplification |
| Read-heavy, mixed reads/writes | LCS | Low read amplification |
| Time-series with TTL | TWCS | Efficient whole-window expiry |
| Time-series without TTL | STCS or LCS | Avoid TWCS |
| User profiles, small tables | LCS | Fast lookups |

---

## Bloom Filters

**Purpose:** Avoid opening SSTables that don't contain a queried partition — entirely in-memory, zero disk I/O.

A bloom filter is a probabilistic bit array + k hash functions:

- **False negatives:** impossible — if a key exists in the SSTable, its bits are always set
- **False positives:** possible — another key's hash pattern matches → unnecessary disk read
- False positive rate controlled via `bloom_filter_fp_chance` (default 0.1 = 10%):
  - Lower → larger filter in RAM → fewer false positives
  - Higher → smaller filter → more unnecessary disk lookups

```cql
ALTER TABLE mykeyspace.events
  WITH bloom_filter_fp_chance = 0.01;  -- 1%: lower FP, more RAM per SSTable
```

---

## Caching Layer

Cassandra has three distinct caches operating at different layers of the read path.

### Key Cache

**What it caches:** partition key → SSTable file + byte offset mapping.

**What it saves:** the partition index lookup — instead of scanning the index to find where a row lives in `Data.db`, Cassandra jumps directly to the offset.

- **Enabled by default** — one of the most cost-effective performance levers
- Stored off-heap (not on JVM heap)
- Saved to disk on shutdown, loaded on startup (`saved_caches_directory`)

```cql
ALTER TABLE mykeyspace.users WITH caching = {'keys': 'ALL'};    -- cache all partition keys
ALTER TABLE mykeyspace.users WITH caching = {'keys': 'NONE'};   -- disable key cache
```

### Row Cache

**What it caches:** fully assembled, deserialized, merged rows — ready to return to the client.

- **Disabled by default** (`row_cache_size_in_mb: 0`)
- Stored off-heap
- Invalidated on write to that partition — any write causes the cache entry to be evicted
- Granularity: one entry per partition

**When row cache is beneficial:**
- Read-heavy tables where the same rows are accessed repeatedly
- Small hot reference data (e.g., config tables, lookup tables)
- Low write rate — writes invalidate cache entries

**When row cache hurts:**
- Write-heavy tables — every write invalidates the partition's cache entry
- Large partitions — caching a 50 MB partition consumes 50 MB of row cache for one entry
- High-cardinality uniform access — if every read is for a different partition, cache hit rate is near 0

### Counter Cache

**What it caches:** recently read counter values — reduces the read-before-write overhead for counter increments.

- Enabled by default for `COUNTER` column tables

### Cache Interaction with the Read Path

```
Read pk=alice:
  1. Row Cache → HIT? → return (fastest path, zero disk I/O)
  2. Row Cache → MISS
  3. Bloom Filter (per SSTable, in RAM) → skip SSTable if "definitely not here"
  4. Key Cache → HIT? → skip index lookup, go directly to Data.db offset
  5. Key Cache → MISS → partition index → get offset → populate key cache
  6. Data.db → read partition data
  7. Merge all SSTable versions by timestamp
  8. Populate row cache (if enabled)
  9. Return result
```

---

## Read Path Internals — Coordinator Protocol

### Digest Reads vs Full Data Reads

Cassandra does NOT send a full data request to every replica. It uses a two-tier approach to reduce network bandwidth:

```
Read pk=alice at QUORUM (RF=3):
  Coordinator → Replica 1: full DATA request   (returns complete row)
  Coordinator → Replica 2: DIGEST request      (returns hash of the row only)
  Coordinator → Replica 3: DIGEST request      (returns hash of the row only)

  Coordinator receives:
    Replica 1: { user_id: alice, email: a@b.com, ts: 1000 }
    Replica 2: digest=0xABCD1234
    Replica 3: digest=0xABCD1234

  Compare Replica 1's digest with Replica 2 and 3's digests:
    Match → return Replica 1's data to client

  If mismatch:
    Coordinator issues full DATA requests to all replicas
    Merges by timestamp → returns result
    Issues background read repair to stale replica
```

**Why this matters:** with RF=3 at QUORUM, only 1 full row is deserialized and returned over the network. The other 2 replicas return small fixed-size digests. This dramatically reduces coordinator bandwidth for large rows.

### Speculative Retry

If a replica hasn't responded within a threshold, the coordinator sends the same request to another replica without waiting for the first to fail. The first response back wins; the slower one is discarded.

```
t=0ms:   Coordinator sends read to Replica 1 (data) + Replica 2 (digest)
t=50ms:  Replica 1 hasn't responded (GC pause?)
t=50ms:  Speculative retry fires → coordinator also sends to Replica 3
t=52ms:  Replica 3 responds first → coordinator uses Replica 3
t=80ms:  Replica 1 responds → discarded (already have quorum)

p99 without speculative retry: up to 200ms (full GC pause)
p99 with speculative retry: ~52ms (another replica responds before GC finishes)
```

Configured per-table:
```cql
ALTER TABLE mykeyspace.users
  WITH speculative_retry = '99percentile';  -- fire if no response by p99 latency of this table
  -- or '50ms'           -- fire after 50ms absolute
  -- or 'always'         -- always send to 2 replicas (hedged read)
  -- or 'none'           -- disabled
```

---

# Part 5 — Replication, Consistency, and Fault Tolerance

## Replication Strategies

Data is replicated across N nodes (Replication Factor = RF):

- **RF=1:** no fault tolerance — node loss = data loss
- **RF=3:** standard production choice — tolerates 1 node failure at QUORUM
- **RF=5:** used when stricter fault tolerance is required

Replication strategies:
- `SimpleStrategy`: single datacenter only — places replicas on next N nodes clockwise on the ring
- `NetworkTopologyStrategy`: multi-DC — places RF replicas in each specified DC, across different racks

```cql
-- Multi-DC setup: 3 replicas in each of 2 datacenters
CREATE KEYSPACE myapp
  WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'us-east': 3,
    'eu-west': 3
  };
```

---

## Transient Replication (C* 4.0 Preview)

Transient replication is a feature that reduces storage costs by allowing some replicas to hold data temporarily (transiently) rather than permanently.

### The Problem It Solves

With RF=5 (for higher durability), you store 5 full copies of every row — 5× storage cost. Transient replication lets you have RF=5 fault tolerance with only 3 full copies:

```
RF=5, transient_replication_factor=2:
  3 full replicas  (hold all data permanently)
  2 transient replicas  (hold data temporarily, only during topology changes)

Fault tolerance: can lose 2 nodes without losing data (same as RF=5)
Storage cost: ~3× instead of 5× (transient replicas don't store all data)
```

### How It Works

Transient replicas hold data only when the cluster is in a transitional state (node joining, leaving, or during repair). During normal operation, they hold minimal data and serve as additional read replicas.

```yaml
# cassandra.yaml (on each node)
# Transient replication is configured per keyspace at creation time

CREATE KEYSPACE myapp
  WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'us-east': '5/2'   -- 5 total replicas, 2 of which are transient
  };
```

### Trade-offs

| | Full RF=5 | Transient RF=5/2 |
|---|---|---|
| Storage per row | 5× | ~3× |
| Repair complexity | Standard | Higher (transient vs full tracking) |
| Read availability | Can route to all 5 | Only route reads to full replicas |
| Status | GA in future releases | Preview in C* 4.0; do not use in production |

**Current status:** Transient replication is still experimental as of C* 5.0. It requires `enable_transient_replication: true` in `cassandra.yaml` and is not recommended for production use until it reaches GA status.

---

## Gossip Protocol

Cassandra uses a **peer-to-peer epidemic protocol** — every node continuously exchanges state with a few random peers; information propagates like a rumor.

**One gossip round (every 1 second):**
```
Node A picks 1–3 random peers (always includes a seed, a random live node, and possibly a random dead node)

SYN:  A → B: digest { node_id, generation, max_version }
ACK:  B → A: newer state A is missing + requests state it needs
ACK2: A → B: state B requested

Both nodes are now synchronized on each other's knowledge.
```

**What gossip carries:**

| State | Content |
|---|---|
| `STATUS` | `NORMAL`, `JOINING`, `LEAVING`, `DECOMMISSIONED`, `BOOT` |
| `TOKENS` | Token ranges this node owns |
| `SCHEMA` | CQL schema hash — mismatch triggers schema sync |
| `DC` / `RACK` | Topology info for NetworkTopologyStrategy |
| `LOAD` | Approximate data size on this node |
| `SEVERITY` | I/O load indicator (used by DynamicSnitch) |
| `RELEASE_VERSION` | Cassandra version |

**Phi Accrual Failure Detector:**
Tracks inter-arrival times of gossip heartbeats; computes phi (failure probability score):
```
phi < phi_convict_threshold (default 8) → node considered UP
phi ≥ 8                                 → node marked DOWN (10–30s after last heartbeat)
```

**Convergence:**
```
Rounds to full convergence ≈ log₃(N)
  N=100 nodes:  ~5 rounds (~5 seconds)
  N=1000 nodes: ~7 rounds (~7 seconds)
```

O(log N) — gossip scales gracefully with cluster size.

---

## Seed Nodes

A seed node is a **known contact point** — its address is hardcoded in `cassandra.yaml` so new nodes have somewhere to call on first boot.

```yaml
seed_provider:
  - class_name: org.apache.cassandra.locator.SimpleSeedProvider
    parameters:
      - seeds: "10.0.1.1,10.0.1.2,10.0.1.3"
```

Seeds have **no special role** in a running cluster — they are only used for initial contact. Best practice: 2–3 seeds per DC, on stable long-lived nodes. Every node in the cluster should have the same seed list.

---

## Snitches and Rack Awareness

A snitch tells Cassandra about the network topology — which DC and rack each node belongs to — so `NetworkTopologyStrategy` can place replicas across racks/AZs.

| Snitch | Use Case |
|---|---|
| `SimpleSnitch` | Single DC, no rack awareness |
| `GossipingPropertyFileSnitch` | Multi-DC production standard — each node declares its own DC/rack via `cassandra-rackdc.properties`; propagated via gossip |
| `Ec2Snitch` / `Ec2MultiRegionSnitch` | AWS deployments — auto-detects region/AZ from EC2 metadata |
| `GoogleCloudSnitch` | GCP deployments |
| `DynamicSnitch` | Wrapper (on by default) — wraps any other snitch; adds latency awareness |

**DynamicSnitch:** wraps the configured snitch and records per-node latency. When routing reads, it prefers the fastest-responding replica rather than purely following rack topology.

### Rack Configuration Best Practices

Rack awareness is how Cassandra spreads replicas across availability zones to survive AZ failures.

```yaml
# cassandra-rackdc.properties (per node — each node declares its own DC/rack)
dc=us-east-1
rack=us-east-1a
```

**Production rules:**
1. **Minimum 3 racks per DC** when RF=3 — each replica goes to a different rack. If you have only 2 racks, one rack holds 2 replicas and losing that rack causes data unavailability.
2. **Map rack = AZ** in cloud deployments — this ensures replicas survive an entire AZ going down.
3. **Never mix rack assignments** — once a node is assigned a rack, changing it requires a full restart and may cause data redistribution issues.
4. **Racks within a DC should be roughly equal in node count** — unequal racks cause uneven token distribution.

**AWS example with 3 AZs:**
```
DC: us-east-1
  rack: us-east-1a → Nodes 1, 4, 7
  rack: us-east-1b → Nodes 2, 5, 8
  rack: us-east-1c → Nodes 3, 6, 9

With RF=3 and NetworkTopologyStrategy:
  Every partition has exactly 1 replica per AZ
  Losing one AZ (3 nodes) → 2 replicas still available → QUORUM met (needs 2 of 3)
```

**Rack count mismatch with RF:**
```
Bad: RF=3, only 2 racks (us-east-1a, us-east-1b)
  → One rack gets 2 replicas for some partitions
  → Losing us-east-1a may lose 2 of 3 replicas → QUORUM unavailable

Good: RF=3, 3 racks (us-east-1a, us-east-1b, us-east-1c)
  → 1 replica per rack guaranteed
  → Any single rack failure → 2 replicas survive → QUORUM available
```

---

## Consistency Levels

A consistency level (CL) is set **per operation** — it specifies how many replicas must respond before success.

#### Single-DC Levels

| CL | Replicas required | Notes |
|---|---|---|
| `ANY` | 1 or just a hint | Writes only — weakest; accepts hinted handoff as ack |
| `ONE` | Any 1 replica | Fastest; stale reads possible |
| `TWO` | Any 2 replicas | Rarely used |
| `THREE` | Any 3 replicas | Rarely used |
| `QUORUM` | `floor(RF/2)+1` across all DCs | Strong consistency within tolerance |
| `ALL` | Every replica | Strongest; any replica down = failure |
| `SERIAL` | Quorum via Paxos | Lightweight transactions (CAS) |
| `LOCAL_SERIAL` | Local quorum via Paxos | LWT in local DC only |

#### Multi-DC Levels

| CL | Replicas required | Notes |
|---|---|---|
| `LOCAL_ONE` | 1 in local DC | Prefer over `ONE` in multi-DC setups |
| `LOCAL_QUORUM` | Majority in local DC only | Most common production choice |
| `EACH_QUORUM` | Majority in every DC independently | Writes only; strict per-DC guarantee |

#### Strong Consistency Rule

A read always sees the latest write when: **write replicas + read replicas > RF**

```
RF=3, Write QUORUM(2) + Read QUORUM(2): 2+2=4 > 3 ✓  → strong consistency
RF=3, Write ONE(1)   + Read ONE(1):    1+1=2 < 3 ✗  → potentially stale
RF=3, Write ALL(3)   + Read ONE(1):    3+1=4 > 3 ✓  → strong (but slow writes)
```

#### Latency by Consistency Level (single-DC)

| CL | p50 | p99 | Notes |
|---|---|---|---|
| `ONE` | 0.5–2 ms | 5–20 ms | Single replica; p99 hits GC pauses + slow outliers |
| `QUORUM` (RF=3) | 1–3 ms | 5–15 ms | Waits for 2nd of 3 — hedges against slowest |
| `ALL` (RF=3) | 2–5 ms | 10–50 ms | Blocked on slowest replica |
| `SERIAL` (LWT) | 10–30 ms | 50–200 ms | 4 Paxos round trips + GC overhead |

**Why QUORUM p99 is often lower than ONE p99:** with `ONE`, the coordinator waits for a single replica — if it GC-pauses, the client waits the full pause. With `QUORUM` (RF=3), the coordinator fans out to all 3 and uses the 2nd response — this hedged request effect absorbs most GC tail latency.

#### Common Production Patterns

| Use Case | Write CL | Read CL |
|---|---|---|
| Multi-DC app (most cases) | `LOCAL_QUORUM` | `LOCAL_QUORUM` |
| High-throughput time-series / IoT | `LOCAL_ONE` | `LOCAL_ONE` |
| Financial records | `EACH_QUORUM` | `LOCAL_QUORUM` |
| Unique constraint enforcement | `LOCAL_SERIAL` | `LOCAL_SERIAL` |
| Single-DC app | `QUORUM` | `QUORUM` |

---

## Hinted Handoff

When a replica is temporarily unreachable at write time, the coordinator stores a **hint** locally and replays it when the replica recovers.

```
Write pk=alice (LOCAL_QUORUM, RF=3):
  Replica 1 ✓, Replica 2 ✓  ← quorum met → ack to client
  Replica 3 ✗ (down)        ← coordinator writes hint to local disk

Replica 3 recovers (gossip detects UP):
  Coordinator finds hint → replays mutation to Replica 3 (rate-limited)
  Deletes hint on successful replay
```

**The Hint Window:** default 3 hours (`max_hint_window_in_ms: 10800000`). Beyond this, hints are dropped and `nodetool repair` is required.

**Key properties:**
- Only **writes and deletes** are hinted — reads are never hinted
- Hints survive coordinator restarts (stored on disk)
- If the coordinator itself dies permanently, its hints are lost — repair is the fallback
- CL=`ANY` accepts a hint as the sole acknowledgment (weakest guarantee)

---

## What Happens When a Node Dies

### Detection (~10–30 seconds)

Cassandra uses the phi accrual failure detector via gossip. When phi exceeds the threshold (~8), the node is marked DOWN and the state propagates to the cluster within seconds.

### Impact on Reads/Writes (RF=3, LOCAL_QUORUM)

```
1 node down → 2 remaining → LOCAL_QUORUM (needs 2) still met → no client-visible impact
2 nodes down → 1 remaining → cannot meet LOCAL_QUORUM → reads/writes fail for affected ranges
```

### Recovery Path

```
Node down < 3 hours (default hint window):
  CommitLog replays pre-crash state on restart
  Gossip detects UP → hint replay fills missed writes automatically
  Fully automatic — no manual intervention needed

Node down > 3 hours:
  Hints for the gap period expired → data gap exists
  On recovery: run nodetool repair on the recovered node

Node permanently lost:
  cassandra -Dcassandra.replace_address=<dead-node-ip>  ← new node bootstraps in its place
  Run nodetool repair after bootstrap completes
```

---

## Repair (Anti-Entropy)

`nodetool repair` runs Merkle tree comparisons between replicas to find and fix diverged data.

### How Merkle Trees Work

```
For a token range on Replica 1 and Replica 2:

  1. Hash every row's data → leaf nodes of a Merkle tree
  2. Combine hashes bottom-up → root hash represents the entire range

  Compare root hashes:
    Match → no divergence → nothing to do (zero data transferred)
    Mismatch → drill down the tree to find exactly which subtree differs
              → only stream the divergent rows (cost ∝ divergence, not total data)
```

### Types of Repair

```bash
# Full repair — scans all SSTables for all token ranges
nodetool repair mykeyspace users

# Incremental repair (C* 4.0+ GA) — only examines SSTables changed since last repair
nodetool repair --incremental mykeyspace users

# Scope to one DC — avoids cross-region traffic
nodetool repair -dc us-east mykeyspace users

# Parallel repair (faster but higher I/O)
nodetool repair --parallel mykeyspace users
```

### When to Run Repair

1. **After node down > 3 hours** — hint window expired; gap must be filled
2. **After replacing or bootstrapping a node** — validates completeness of streamed data
3. **Scheduled at minimum every `gc_grace_seconds` (10 days)** — prevents zombie data resurrection
4. **After schema changes** — ensures all nodes have applied the change to their data
5. **Before decommissioning a node** — confirms data is safely on other replicas

### Read Repair

An opportunistic lighter repair: when a QUORUM read fetches from multiple replicas and detects a mismatch, the coordinator pushes the correct value to the stale replica.

```cql
ALTER TABLE mykeyspace.users
  WITH read_repair = 'BLOCKING';   -- synchronous (default)
  -- or 'NONE' to disable
```

---

# Part 6 — Multi-Region Replication

## NetworkTopologyStrategy

Multi-region deployments use `NetworkTopologyStrategy`, specifying replicas per datacenter:

```cql
CREATE KEYSPACE myapp
  WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'us-east-1': 3,
    'eu-west-1': 3,
    'ap-southeast-1': 3
  };
-- Every row: 3 replicas in each region = 9 total replicas
```

`NetworkTopologyStrategy` places replicas on **different racks within each DC** — losing one AZ never loses all replicas for a partition.

## Write Path Across Regions

A coordinator sends the write to **all replicas in all DCs simultaneously**, but only waits for acknowledgment from the replicas required by the consistency level:

```
Client writes pk=alice (consistency=LOCAL_QUORUM)
        │
        ▼
Coordinator (us-east-1, any node)
        ├──▶ Replica A (us-east-1, AZ-a) ─┐ wait for 2 of 3
        ├──▶ Replica B (us-east-1, AZ-b) ─┘ → ack client
        ├──▶ Replica C (us-east-1, AZ-c)
        │
        ├──▶ Replica D (eu-west-1)  ─┐ fire-and-forget async replication
        ├──▶ Replica E (eu-west-1)   │
        └──▶ Replica F (eu-west-1)  ─┘
```

Cross-region replicas receive the write asynchronously. They are eventually consistent with the local DC.

## Active-Active Multi-Region

In active-active, every region simultaneously accepts both reads and writes.

### Conflict Resolution: Last-Write-Wins (LWW)

Every mutation carries a microsecond-precision client timestamp. When two regions write the same cell concurrently, the **highest timestamp wins** after convergence:

```
eu-west-1: UPDATE users SET email='london@x.com' USING TIMESTAMP 1000
us-east-1: UPDATE users SET email='ny@x.com'     USING TIMESTAMP 1001
→ both regions converge to 'ny@x.com' — 'london@x.com' is silently discarded
```

**Clock skew is critical:** NTP/PTP synchronization across all nodes is mandatory. Clock drift beyond ~1ms can cause correctness issues.

**LWW is safe when:**
- Last writer should win (user profile update, cache invalidation)
- Partition keys are region-scoped (same key rarely written from multiple regions)
- Idempotent data (sensor readings, event logs)

**LWW is dangerous when:**
- Both writes must survive (two users appending to a shared list)
- Distributed counters — concurrent increments from two regions lose one increment

### Partition Key Design for Active-Active

```
Good:  pk = (region || user_id) → EU writes go to eu-west-1, US writes to us-east-1
Bad:   pk = user_id             → any region can write → LWW conflict risk
```

## RPO and RTO

| Failure | LOCAL_QUORUM (multi-DC) | Single-DC | Backup only |
|---|---|---|---|
| Single node | RPO=0, RTO=~30s | RPO=0, RTO=~30s | N/A |
| Single AZ | RPO=0, RTO=~30s | RPO=0, RTO=~30s | N/A |
| Entire DC/region | RPO=ms async lag | Hours–days | Time since backup |
| Accidental DELETE | RPO=0 → replica propagated delete | RPO=0 | Time since snapshot |

**Key insight:** replication propagates deletes faithfully. There is no rollback from an accidental `TRUNCATE` without a backup. Scheduled snapshots + offloading to object storage are the only protection.

---

# Part 7 — Data Access and Application Layer

## Data Modeling Best Practices

Cassandra data modeling is **query-driven** — design tables around the queries your application will run, not around normalized entities.

### Core Principles

1. **Denormalize aggressively** — duplicate data across tables to support different query patterns. Disk is cheap; cross-partition reads are expensive.
2. **One table per query pattern** — if you have 3 different access patterns, consider 3 tables.
3. **Never join across partitions** — all data for a query should be in one partition.
4. **Partition key = access pattern root** — the partition key is your lookup key.
5. **Avoid unbounded partitions** — use bucketing for data that grows over time.

### Common Patterns

#### Time-Series with Bucketing

```cql
-- Problem: all events for a user in one partition grows indefinitely
-- Solution: bucket by time period
CREATE TABLE mykeyspace.events_by_user_month (
  user_id   UUID,
  month     TEXT,          -- 'YYYY-MM' — bucket key
  event_ts  TIMESTAMP,
  event_type TEXT,
  payload   TEXT,
  PRIMARY KEY ((user_id, month), event_ts)
) WITH CLUSTERING ORDER BY (event_ts DESC);

-- Query: last 30 days of events for a user
SELECT * FROM mykeyspace.events_by_user_month
  WHERE user_id = ? AND month = '2024-05'
  LIMIT 100;
```

#### Lookup Table Pattern (multiple access patterns)

```cql
-- Access pattern 1: look up user by user_id
CREATE TABLE users_by_id (
  user_id UUID PRIMARY KEY,
  email TEXT, name TEXT, ...
);

-- Access pattern 2: look up user by email (for login)
CREATE TABLE users_by_email (
  email TEXT PRIMARY KEY,
  user_id UUID, name TEXT, ...
);

-- Maintain both tables with a logged batch (atomicity guarantee)
BEGIN BATCH
  INSERT INTO users_by_id (user_id, email, name) VALUES (?, ?, ?);
  INSERT INTO users_by_email (email, user_id, name) VALUES (?, ?, ?);
APPLY BATCH;
```

#### Avoiding Hot Partitions

```cql
-- Hot partition: all writes for a popular product go to one partition
-- Fix: add a random shard suffix
CREATE TABLE product_events (
  product_id  UUID,
  shard       INT,        -- shard = hash(event_id) % 10
  event_ts    TIMESTAMP,
  event_type  TEXT,
  PRIMARY KEY ((product_id, shard), event_ts)
);
-- Reads must query all N shards and aggregate — tradeoff: write distribution vs read complexity
```

---

## Secondary Indexes

Without a secondary index, any query on a non-partition-key column requires a full cluster scan (`ALLOW FILTERING`).

### Three Options

| | Regular Secondary Index | SAI (C* 5.0) | Materialized View |
|---|---|---|---|
| Created by | `CREATE INDEX` | `CREATE CUSTOM INDEX ... USING 'StorageAttachedIndex'` | `CREATE MATERIALIZED VIEW` |
| Storage | Local to each node | Local to each node | Distributed (own partition key) |
| Extra node hops | Scatter-gather per node | Optimized per-SSTable | 0 (view is a real table) |
| Range queries | Limited | Yes (SAI supports ranges, LIKE) | Depends on schema |
| Write overhead | +1 index write per replica | +1 SAI write per replica | +1 view write per replica (async) |

### Regular Secondary Index (2i)

Creates a local index on each node. When queried without the partition key, the coordinator fans out to **all nodes** for a scatter-gather.

**Never use on high-cardinality columns** (email, UUID) without also filtering by partition key — it triggers cluster-wide fan-out for every query.

### SAI — Storage Attached Index (C* 5.0)

SAI replaces SASI. Per-SSTable on-disk index, more efficient than classic 2i:

```cql
CREATE CUSTOM INDEX ON mykeyspace.users(email)
  USING 'StorageAttachedIndex';

-- Range queries
CREATE CUSTOM INDEX ON mykeyspace.orders(amount)
  USING 'StorageAttachedIndex';
SELECT * FROM mykeyspace.orders WHERE amount > 100 AND amount < 500;
```

Supports equality, range (`>`, `<`, `>=`, `<=`), `LIKE` prefix/suffix, and vector search (ANN). Still requires coordinator fan-out for cluster-wide queries.

### Materialized View (MV)

A full denormalized copy of the base table with a different primary key:

```cql
CREATE MATERIALIZED VIEW mykeyspace.users_by_email AS
  SELECT user_id, email, name
  FROM mykeyspace.users
  WHERE email IS NOT NULL AND user_id IS NOT NULL
  PRIMARY KEY (email, user_id);
```

**Write path for MVs:**
```
Client write to base table:
  1. Base table replica reads the old row (read-before-write) to compute MV delta
  2. Writes to base table
  3. Sends async MV mutation to MV replica(s)
  4. Acks client on base CL only
  → MV write is async → window of inconsistency between base and view
```

**Warning:** MVs have historically been buggy in Cassandra. They were experimental in C* 3.x and are considered stable but still carry risks in C* 4.x. Evaluate carefully before production use.

---

## Driver Behavior and Client Topology Awareness

### Load Balancing Policies

#### TokenAwarePolicy (wraps DCAwareRoundRobinPolicy)

The driver hashes the partition key **locally** using the same Murmur3 algorithm and routes directly to the owning replica as coordinator. Skips one coordinator hop — the coordinator IS the replica.

```python
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy

cluster = Cluster(
    contact_points=['10.0.1.1'],
    load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='us-east-1'))
)
```

**Effect on latency:**
```
Without TokenAwarePolicy:
  Client → random coordinator → coordinator routes to replica → reply → client
  (2 network hops minimum)

With TokenAwarePolicy:
  Client → replica (directly as coordinator) → reads locally → reply → client
  (1 network hop + local read = fastest path)
```

### Prepared Statements

**Always use prepared statements in production.** Two reasons:

1. **Performance:** the CQL string is parsed and the query plan cached once. Subsequent executions reuse the parsed plan.
2. **Security:** parameter values are sent separately from the query string — eliminates CQL injection vulnerabilities.

```python
# Bad: string interpolation (re-parsed every time, injection risk)
session.execute(f"SELECT * FROM users WHERE user_id = {user_id}")

# Good: prepared statement (parsed once, parameters bound separately)
prepared = session.prepare("SELECT * FROM users WHERE user_id = ?")
session.execute(prepared, [user_id])
```

### Connection Pooling

The driver maintains a pool of TCP connections to each node. CQL native protocol v3+ supports multiple in-flight requests per connection (stream IDs 0–32767 per connection). Unlike HTTP/1.1, a single TCP connection can handle thousands of concurrent queries — 1–2 connections per node is often sufficient for high throughput.

### Retry Policies

| Policy | Behavior |
|---|---|
| `DefaultRetryPolicy` | Retry on read timeout (if data was retrieved), write timeout (if logged batch), unavailable (try next host) |
| `DowngradingConsistencyRetryPolicy` | Automatically downgrades CL if not enough replicas available — **use with caution** — may silently return stale data |
| `FallthroughRetryPolicy` | Never retries — propagates all errors to the application |

**Idempotency and retries:** only retry idempotent operations. A failed write that was actually received but the ack was lost will be applied twice if retried.

### Paging

```python
# Automatic paging (driver fetches pages transparently)
rows = session.execute(prepared, [user_id], fetch_size=100)
for row in rows:  # driver fetches next page when current page exhausted
    process(row)

# Manual paging (for stateless APIs — cursor-based pagination)
result = session.execute(prepared, [user_id], fetch_size=100)
page1 = list(result.current_rows)
paging_state = result.paging_state    # opaque token — pass to client

# Next request:
result2 = session.execute(prepared, [user_id],
                           fetch_size=100,
                           paging_state=paging_state)
```

**`fetch_size` (default 5000):** number of rows fetched per page. For most APIs, 100–500 is a safe default.

**Paging state is opaque and node-specific** — if the coordinator changes between pages (e.g., due to node failure), the paging state becomes invalid.

### Tracing

```cql
TRACING ON;
SELECT * FROM mykeyspace.users WHERE user_id = <uuid>;
-- Output: per-step breakdown with timestamps and replica identities
TRACING OFF;
```

**Cost:** tracing writes to `system_traces` keyspace. Never enable globally in production.

---

## Lightweight Transactions (LWT / Paxos)

LWT provides conditional (CAS) operations using Paxos consensus.

```cql
-- Only insert if no row exists (prevents duplicate registration)
INSERT INTO mykeyspace.users (user_id, email)
  VALUES (uuid(), 'alice@example.com') IF NOT EXISTS;

-- Only update if current value matches (optimistic locking)
UPDATE mykeyspace.accounts
  SET balance = 90
  WHERE account_id = 'acc-1'
  IF balance = 100;
```

### Paxos Round Trips (why LWT is slow)

```
Phase 1: Prepare   → Coordinator → all replicas: "I want to write; give me permission"
Phase 2: Promise   → Replicas → Coordinator: "OK, here's latest accepted value if any"
Phase 3: Propose   → Coordinator → all replicas: "Write this value"
Phase 4: Accept    → Replicas → Coordinator: "Accepted"
→ 4 round trips before write is committed
→ ~4× slower than regular QUORUM writes
```

### LWT Consistency Levels

- `SERIAL`: Paxos quorum across all DCs — globally linearizable
- `LOCAL_SERIAL`: Paxos quorum within local DC only — lower latency but only DC-local linearizability

### When to Use LWT

- **User registration uniqueness** (`IF NOT EXISTS`)
- **Optimistic locking** on shared state (`IF col = expected_value`)
- **Leader election** primitives
- **Idempotency keys** for API deduplication

**Do NOT use LWT** for counters (use `COUNTER` columns), for high-throughput paths, or where eventual consistency is acceptable.

---

## ACCORD Distributed Transactions (C* 4.1+ Preview)

### Why ACCORD Was Needed

Cassandra's Paxos-based LWT has two fundamental limitations:
1. **Single-partition only** — Paxos in Cassandra operates on one partition at a time. Multi-partition transactions require multiple Paxos rounds with no atomicity guarantee across them.
2. **Poor scalability** — high-contention scenarios cause contention and starvation.

ACCORD is a new distributed transaction protocol that addresses both:
- **Multi-partition serializable transactions** — atomically read and write multiple partitions across multiple tables
- **Higher throughput under contention** — uses timestamp-based ordering rather than per-value locks

### ACCORD Transaction Model (C* 5.0 Preview)

```cql
-- Multi-partition atomic transaction (preview syntax)
BEGIN TRANSACTION
  LET user = (SELECT * FROM users WHERE user_id = ?);
  LET account = (SELECT * FROM accounts WHERE account_id = ?);
  IF user.status = 'active' AND account.balance >= 100 THEN
    UPDATE accounts SET balance = balance - 100 WHERE account_id = ?;
    INSERT INTO transactions (tx_id, amount) VALUES (uuid(), 100);
  END IF
COMMIT TRANSACTION;
```

**Key properties:**
- **Serializable isolation** — full ACID transactions across partitions
- **Automatic conflict detection** — transactions that conflict are ordered deterministically; no deadlocks
- **Status:** preview in C* 5.0 — not recommended for production

---

# Part 8 — Operations

## cassandra.yaml — Key Performance Parameters

Beyond the defaults, these parameters directly govern Cassandra's read/write throughput and latency. They live in `cassandra.yaml` on each node.

### Concurrency Settings

```yaml
# How many concurrent read/write operations per core
# Default: auto-calculated as (number of data disks × 16)
concurrent_reads: 32
concurrent_writes: 32
concurrent_counter_writes: 32   # separate pool for counter mutations

# Concurrent compaction tasks per node
concurrent_compactors: 2        # default: min(2, num_cpus/2); increase for faster catch-up
```

**Rule of thumb:**
- `concurrent_reads` = 16 × number of disks (increase if reads are frequently queued in `ReadStage`)
- `concurrent_writes` = 32 on SSD, 8 on spinning disk (writes are almost always fast; rarely the bottleneck)
- Too high: thread pool contention and GC pressure; too low: reads/writes queue and latency spikes

### Timeout Settings

```yaml
read_request_timeout_in_ms: 5000      # default: 5000ms (5s) — client sees ReadTimeoutException
write_request_timeout_in_ms: 2000     # default: 2000ms (2s)
counter_write_request_timeout_in_ms: 5000
cas_contention_timeout_in_ms: 1000    # LWT Paxos contention timeout
request_timeout_in_ms: 10000          # global catch-all timeout

# Coordinator waits this long before sending speculative retry
# (superseded by per-table speculative_retry setting in modern versions)
```

**Tuning guidance:**
- Reduce timeouts in low-latency SLAs — shorter timeouts fail faster and trigger retries sooner
- Increase timeouts for bulk-load operations where large partition reads are expected
- `cas_contention_timeout_in_ms` governs how long a Paxos round retries before giving up; increase only if LWT contention errors appear in logs

### Memtable Settings

```yaml
# On-heap memtable allocation before flush is triggered
memtable_heap_space_in_mb: 2048       # default: 1/4 of heap
memtable_offheap_space_in_mb: 2048    # default: 1/4 of heap (if using off-heap memtables)

# How many memtable flush threads
memtable_flush_writers: 2             # default: 1 per data disk; increase if flush queuing
```

### Compaction Throttle

```yaml
# Rate limit compaction I/O (MB/s per compactor)
# Default: 16 MB/s — very conservative; increase on modern SSDs
compaction_throughput_mb_per_sec: 64   # 0 = unlimited (not recommended in production)
```

Compaction backpressure: if compaction falls behind, pending SSTable count grows → reads touch more files → read amplification. Tune `compaction_throughput_mb_per_sec` and `concurrent_compactors` together.

### Key Cache and Row Cache

```yaml
key_cache_size_in_mb: 100         # 0 = auto: min(5% heap, 100MB)
row_cache_size_in_mb: 0           # disabled by default; enable only for specific read-heavy tables
key_cache_save_period: 14400      # save to disk every 4 hours
```

### Heap and GC

```bash
# In cassandra-env.sh — set heap equal min=max to prevent resize pauses
MAX_HEAP_SIZE="16G"
HEAP_NEWSIZE="400M"    # young generation; increase for write-heavy workloads
```

General sizing: heap = min(1/4 of RAM, 8G) for G1GC. For ZGC/Shenandoah, heap can be larger (32–64G) since GC pauses are sub-ms regardless of heap size.

---

## Guardrails (C* 4.1+)

Guardrails is a proactive safety system that warns or blocks operations likely to cause operational problems. Unlike errors, guardrails are configurable by operators — you can set thresholds appropriate for your workload.

### What Guardrails Cover

```yaml
# cassandra.yaml — guardrails section
guardrails:
  # Table limits
  tables_warn_threshold: 150        # warn if keyspace has > 150 tables
  tables_fail_threshold: 200        # reject DDL if > 200 tables in keyspace

  # Partition size
  partition_size_warn_threshold_in_mb: 100    # warn if partition exceeds 100MB
  partition_size_fail_threshold_in_mb: 500    # reject if partition exceeds 500MB (writes blocked)

  # Column limits
  columns_per_table_warn_threshold: 60
  columns_per_table_fail_threshold: 100

  # Batch limits
  batch_size_warn_threshold_in_kb: 5          # warn at 5KB
  batch_size_fail_threshold_in_kb: 50         # reject at 50KB
  unlogged_batch_across_partitions_warn_threshold: 10

  # Collection limits
  collection_size_warn_threshold_in_kb: 1024
  collection_size_fail_threshold_in_kb: 10240

  # Secondary index limits
  secondary_indexes_per_table_warn_threshold: 5
  secondary_indexes_per_table_fail_threshold: 10

  # Tombstone limits (per read)
  tombstone_warn_threshold: 1000
  tombstone_failure_threshold: 100000

  # ALLOW FILTERING
  allow_filtering_enabled: true      # set false to globally block ALLOW FILTERING queries

  # Read consistency downgrade
  read_consistency_levels_disallowed: ''   # e.g., 'ALL' to block CL=ALL reads

  # Write consistency downgrade
  write_consistency_levels_disallowed: ''
```

### Guardrails vs. Errors

| | Error | Guardrail |
|---|---|---|
| Set by | Cassandra internals | Operator via cassandra.yaml |
| Configurable threshold | No | Yes |
| Severity | Fixed (exception) | warn or fail (configurable) |
| Purpose | Prevent crashes / corruption | Prevent operational anti-patterns |

### Why Guardrails Matter in Production

Without guardrails, developers can accidentally:
- Create hundreds of tables in a keyspace → schema bloat → gossip overhead → slow node joins
- Write to a partition that grows past 500 MB → repair becomes extremely slow → read degradation
- Use ALLOW FILTERING in a hot path → full cluster scans → cascade to other queries

Guardrails enforce these boundaries without requiring developers to memorize every Cassandra rule.

### Monitoring Guardrail Violations

```bash
# Guardrail warnings appear in system.log
grep "GuardRail" /var/log/cassandra/system.log

# Virtual table (C* 4.1+)
SELECT * FROM system_views.guardrails_config;
```

---

## Schema Management and Migrations

### How Schema Propagates

Schema changes are stored in the `system_schema` keyspace and propagated via gossip.

```bash
nodetool describecluster
  # shows "Schema versions" — if all nodes show same UUID, schema is in agreement
  # multiple UUIDs = schema disagreement (usually transient; resolves within seconds)
```

### ALTER TABLE Limitations

| Operation | Supported | Notes |
|---|---|---|
| Add a new column | Yes | Safe online operation |
| Drop a column | Yes (C* 3.0+) | Marks column as dropped; data not immediately removed |
| Change a column's type | Limited | Only compatible type widening (e.g., INT → BIGINT) |
| Change primary key components | **No** | Must create a new table and migrate data |
| Change replication strategy | Yes | Follow up with `nodetool repair` |

### Changing Replication Factor

```cql
ALTER KEYSPACE mykeyspace
  WITH replication = {'class': 'NetworkTopologyStrategy', 'us-east': 5};
```

This changes the metadata, but does not immediately copy data to new replicas. Follow up:
```bash
nodetool repair mykeyspace   # propagates data to newly responsible nodes
```

For RF reduction: existing data stays on the nodes that are no longer responsible until `nodetool cleanup` is run.

---

## Security

### Authentication

```yaml
# cassandra.yaml
authenticator: PasswordAuthenticator   # default: AllowAllAuthenticator (no auth)
```

```cql
CREATE ROLE appuser WITH PASSWORD = 'securepassword' AND LOGIN = true;
ALTER ROLE appuser WITH PASSWORD = 'newpassword';
```

**Critical:** the default superuser `cassandra` with password `cassandra` must be changed or disabled immediately on any production cluster.

### Authorization

```yaml
authorizer: CassandraAuthorizer   # default: AllowAllAuthorizer
```

```cql
GRANT SELECT ON TABLE mykeyspace.users TO appuser;
GRANT ALL ON KEYSPACE mykeyspace TO admin_role;
REVOKE SELECT ON TABLE mykeyspace.users FROM appuser;
LIST ALL PERMISSIONS OF appuser;
```

### system_auth Replication — Critical Gotcha

`system_auth` stores all roles and credentials. Its default replication factor is **1**. If that node goes down, **no client can authenticate** — the entire cluster becomes inaccessible.

```cql
-- ALWAYS set this before adding more nodes:
ALTER KEYSPACE system_auth
  WITH replication = {'class': 'NetworkTopologyStrategy', 'us-east': 3};

nodetool repair system_auth
```

### TLS/SSL Encryption

```yaml
# Client-to-node encryption
client_encryption_options:
  enabled: true
  optional: false
  keystore: /etc/cassandra/keystore.jks
  keystore_password: changeit
  require_client_auth: false  # set true for mTLS

# Inter-node encryption
server_encryption_options:
  internode_encryption: all    # options: none, dc, rack, all
  keystore: /etc/cassandra/keystore.jks
```

`internode_encryption: dc` encrypts only cross-DC traffic. `all` encrypts everything including within a DC.

---

## Backup and Restore

### Snapshots (nodetool snapshot)

A snapshot is a **hard-link copy** of the current SSTable files — instant to create regardless of data size.

```bash
nodetool snapshot mykeyspace
nodetool snapshot -t 2024-05-08-before-migration mykeyspace
nodetool listsnapshots
nodetool clearsnapshot -t 2024-05-08-before-migration
```

Snapshot files are hard links to SSTable files. Space grows over time as compaction creates new SSTables and the snapshot retains old ones. Always `clearsnapshot` after offloading.

**Cluster-level snapshot:** must run `nodetool snapshot` on every node independently (or use Medusa).

### Incremental Backups

```yaml
# cassandra.yaml
incremental_backups: true
```

Each flushed SSTable is hard-linked into:
```
/var/lib/cassandra/data/<keyspace>/<table>-<uuid>/backups/
```

Used for point-in-time recovery: start from a full snapshot and apply incremental backup files.

### Offloading Backups to Object Storage

- **Medusa** (open-source, DataStax): `medusa backup`, `medusa restore`, `medusa backup-cluster`
- **Priam** (Netflix): manages backup, restore, token management for AWS
- **Custom scripts:** `aws s3 sync` after `nodetool snapshot`

### Restore Procedure

```bash
# 1. Stop Cassandra on the target node
systemctl stop cassandra

# 2. Clear existing data
rm -rf /var/lib/cassandra/data/mykeyspace/users-*/
rm -rf /var/lib/cassandra/commitlog/

# 3. Copy snapshot files to the data directory
cp -r /path/to/snapshot/files/ /var/lib/cassandra/data/mykeyspace/users-<uuid>/

# 4. Start Cassandra
systemctl start cassandra

# 5. Tell Cassandra to pick up the new SSTable files
nodetool refresh mykeyspace users
```

### CommitLog Archiving (Point-in-Time Recovery)

```yaml
# cassandra.yaml
commitlog_archiving:
  archive_command: /usr/local/bin/archive_commitlog.sh %path %name
  restore_command: cp -f %from %to
  restore_directories: /var/lib/cassandra/commitlog_restore
  restore_point_in_time: 2024-05-08T14:30:00
```

---

## Monitoring and Key Metrics

### Critical Metrics

#### Latency

| Metric | JMX Path | Alert Threshold |
|---|---|---|
| Read latency (p99) | `Table.ReadLatency.99thPercentile` | > 10ms (tune to workload) |
| Write latency (p99) | `Table.WriteLatency.99thPercentile` | > 5ms |
| CAS commit latency | `Table.CasCommitLatency.99thPercentile` | > 50ms |

#### Dropped Messages — CRITICAL

`DroppedMessages` is the most important metric to alert on. When Cassandra can't process a message fast enough (thread pool queue full), it **drops the message silently**. The coordinator treats this as a timeout.

```bash
nodetool tpstats   # shows per-pool pending/active/completed/dropped counts
```

**Any `DroppedMessages > 0` should trigger an alert.** Dropped `MUTATION` messages = writes were silently discarded.

#### Thread Pools

```bash
nodetool tpstats
# Healthy: Active near thread pool size, Pending = 0, Dropped = 0
```

High `Pending` in `MemtableFlushWriter` → memtable flushes are queuing up → writes will stall waiting for flush.

#### Compaction

```bash
nodetool compactionstats   # active compactions, bytes remaining, estimated completion
```

| Metric | Alert If |
|---|---|
| `PendingCompactions` | > 100 → read amplification increasing |
| SSTable count per table | > 50 (STCS) or > 10 (LCS per level) → compaction not keeping up |

#### Disk and Memory

| Metric | Alert |
|---|---|
| `LiveDiskSpaceUsed` | > 70% of disk capacity (compaction needs headroom) |
| JVM heap usage | > 75% heap → GC pressure imminent |

### nodetool tablestats (cfstats) — Key Output Fields

`nodetool tablestats mykeyspace.mytable` produces per-table statistics. Key fields to understand:

```bash
nodetool tablestats mykeyspace.events
# Example output fields:

SSTable count: 12              # number of SSTables — high = compaction behind schedule
Space used (live): 4.21 GiB   # live data on disk
Space used (total): 4.85 GiB  # includes uncompacted/snapshot data

Read count: 1234567            # total reads since node start
Read latency: 1.23 ms          # average read latency (use tablestats histogram for percentiles)
Write count: 5678901
Write latency: 0.45 ms

Number of partitions (estimate): 8432100

Tombstone scan: min/max/median  # how many tombstones reads are hitting
                                 # high median = tombstone accumulation problem

Bloom filter false positives: 1234   # total false positive bloom filter hits
Bloom filter false ratio: 0.01234    # rate — should be near bloom_filter_fp_chance
Bloom filter space used: 45 MiB      # RAM used by bloom filters for this table

Compaction tombstones dropped: 789000   # tombstones removed by last compaction
Dropped mutations: 0                    # non-zero = this table has drops (critical alert)

SSTables in each level (LCS only):
  [2, 14, 182]   # L0=2 files, L1=14 files, L2=182 files
```

**Interpreting SSTable count:**
- STCS: normal operating range is 4–8 SSTables; > 20 = compaction is significantly behind
- LCS: L0 should typically be 0–4; L1 should not exceed `sstable_size × 10 / sstable_size = 10` files; growing L0 = writes arriving faster than compaction can push them to L1

**Interpreting Bloom filter false ratio:**
- Should be close to `bloom_filter_fp_chance` (default 0.1)
- If significantly higher → bloom filter is undersized (SSTable grew past sizing estimates) → consider lowering `bloom_filter_fp_chance` or running compaction to rewrite SSTables

---

## Benchmarking with cassandra-stress

`cassandra-stress` is the official load testing and benchmarking tool bundled with Cassandra. It is the standard tool for capacity planning, performance regression testing, and hardware sizing.

### Basic Usage

```bash
# Write benchmark: 1 million rows, 4 threads, 10k ops/sec throttle
cassandra-stress write n=1000000 -rate threads=4 throttle=10000/s \
  -node 10.0.1.1

# Read benchmark: 500k reads
cassandra-stress read n=500000 -rate threads=8 \
  -node 10.0.1.1

# Mixed workload: 70% reads, 30% writes
cassandra-stress mixed ratio\(write=3,read=7\) n=1000000 -rate threads=16 \
  -node 10.0.1.1

# Duration-based (run for 5 minutes)
cassandra-stress write duration=5m -rate threads=8 -node 10.0.1.1
```

### Custom Schema Stress Tests

The real power is testing your actual schema. Define a YAML profile:

```yaml
# stress-profile.yaml
keyspace: mykeyspace

table: events
table_definition: |
  CREATE TABLE events (
    user_id uuid,
    event_ts timestamp,
    event_type text,
    payload text,
    PRIMARY KEY (user_id, event_ts)
  ) WITH CLUSTERING ORDER BY (event_ts DESC);

columnspec:
  - name: user_id
    size: uniform(4..4)    # 4 bytes = UUID-like
    population: uniform(1..1000000)   # 1M unique users
  - name: event_ts
    cluster: fixed(10)     # 10 events per user (partition width)
  - name: event_type
    size: fixed(10)
  - name: payload
    size: uniform(100..500)

insert:
  partitions: fixed(1)
  batchtype: UNLOGGED

queries:
  read_user_events:
    cql: SELECT * FROM events WHERE user_id = ? LIMIT 10
    fields: samerow
```

```bash
cassandra-stress user profile=stress-profile.yaml ops\(insert=1,read_user_events=3\) \
  -rate threads=32 -node 10.0.1.1
```

### Reading cassandra-stress Output

```
Results:
  op rate                   : 24,532 op/s    ← total throughput
  partition rate            : 24,532 pk/s    ← partitions accessed per second
  row rate                  : 245,320 row/s  ← rows read/written per second
  latency mean              : 1.3 ms
  latency median            : 1.1 ms
  latency 95th percentile   : 2.1 ms
  latency 99th percentile   : 4.7 ms
  latency 99.9th percentile : 18.3 ms
  latency max               : 156.2 ms       ← GC pause spike visible here
  total errors              : 0
```

### Capacity Planning with cassandra-stress

1. **Find single-node saturation:** increase thread count until p99 latency exceeds your SLA. That's your single-node throughput ceiling.
2. **Linear scale check:** verify throughput scales linearly when adding nodes (Cassandra's core guarantee).
3. **Stress before topology changes:** benchmark after adding/removing nodes to confirm expected performance.
4. **Reproduce a production-like workload:** use a profile that matches your partition key distribution (uniform vs. Zipfian for hot keys).

```bash
# Zipfian distribution simulates hot keys (more realistic for many workloads)
cassandra-stress read n=1000000 -rate threads=16 \
  -pop dist=ZIPFIAN\(0..1000000\) -node 10.0.1.1
```

---

## Virtual Tables (C* 4.0+)

Virtual tables expose internal Cassandra state as queryable CQL tables (read-only). No actual data is stored — values are computed on demand.

```cql
SELECT * FROM system_views.settings WHERE name = 'num_tokens';
SELECT * FROM system_views.compactions;
SELECT * FROM system_views.sstable_tasks;
SELECT * FROM system_views.thread_pools;
SELECT * FROM system_views.cdc;
SELECT * FROM system_views.guardrails_config;    -- C* 4.1+
```

---

## Audit Logging (C* 4.0+)

Records all CQL operations to a log — useful for compliance, security audits, and debugging.

```yaml
# cassandra.yaml
audit_logging_options:
  enabled: true
  logger:
    - class_name: BinAuditLogger   # binary log (fast); use FileAuditLogger for plaintext
  audit_logs_dir: /var/lib/cassandra/audit/
  included_keyspaces: mykeyspace
  included_categories: AUTH,DDL,DML
```

```bash
nodetool enableauditlog
nodetool disableauditlog
```

---

## Change Data Capture (CDC)

CDC captures every mutation applied to a table and writes it to a separate log directory, enabling streaming pipelines.

### How CDC Works

```
Client write
     │
     ▼
CommitLog (normal write path)
     │
     ├──▶ Memtable (normal)
     │
     └──▶ CDC Log directory (/var/lib/cassandra/cdc_raw/)
               │
               └── CDC consumer reads and processes
                   (Kafka Connect, Debezium, custom consumer)
```

### Enabling CDC

```yaml
# cassandra.yaml
cdc_enabled: true
cdc_raw_directory: /var/lib/cassandra/cdc_raw
cdc_total_space_in_mb: 4096   # max disk space; writes pause if exceeded
```

```cql
ALTER TABLE mykeyspace.orders WITH cdc = true;
```

### Back-Pressure Risk

If the CDC consumer falls behind and `cdc_raw` fills up, **Cassandra pauses all writes to CDC-enabled tables** until the consumer catches up. This means an unmonitored or stuck CDC consumer can halt production writes cluster-wide. Always monitor `cdc_raw` directory size.

### CDC Limitations

- CDC only captures mutations applied to a node — not the result after merging with other replicas
- Deletes appear as tombstone events — consumers must handle tombstone semantics
- Schema changes are not captured by CDC
- No ordering guarantee across partitions from multiple nodes

---

## Key Operational Commands

```bash
# Cluster health
nodetool status                    # node status (UN=Up/Normal, DN=Down/Normal)
nodetool info                      # local node info: load, uptime, heap usage
nodetool tpstats                   # thread pool stats — find bottlenecks
nodetool compactionstats           # active compaction progress
nodetool describecluster           # cluster name, schema versions

# Data operations
nodetool flush [keyspace] [table]  # force memtable flush to SSTable
nodetool compact [keyspace] [table] # force compaction
nodetool repair [keyspace] [table] # run anti-entropy repair
nodetool cleanup [keyspace]        # remove data no longer owned by this node (after adding nodes)
nodetool snapshot [keyspace]       # create a snapshot (backup)

# Topology changes
nodetool decommission              # gracefully remove this node
nodetool removenode <host_id>      # remove a dead node (use only after permanent failure)
nodetool assassinate <endpoint>    # forcibly remove an unresponsive node (last resort)

# Performance diagnostics
nodetool tablestats [keyspace.table] # per-table statistics (read/write counts, latency, SSTables)
nodetool tablehistograms           # latency distribution histograms per table
nodetool proxyhistograms           # coordinator-level latency histograms
nodetool netstats                  # active streaming operations
nodetool gcstats                   # GC statistics
nodetool tpstats                   # thread pool pending/active/dropped

# Diagnostics
nodetool enablefullquerylog --path /var/lib/cassandra/fql/ --roll-cycle HOURLY
nodetool disablefullquerylog
nodetool enableauditlog
nodetool disableauditlog
```

---

## Operational Procedures

### Bootstrapping a New Node

```bash
# 1. Install Cassandra on new node — do NOT start it yet
# 2. Configure cassandra.yaml:
#    - cluster_name: must match existing cluster
#    - seeds: same as other nodes
#    - listen_address / rpc_address: this node's IP
#    - auto_bootstrap: true (default)

# 3. Start Cassandra
systemctl start cassandra

# Monitor progress:
nodetool status          # new node shows as UJ (Up/Joining) then UN (Up/Normal)
nodetool netstats        # shows active data streaming

# 4. After bootstrap completes (node shows UN):
nodetool cleanup mykeyspace   # run on EACH existing node
```

**Never bootstrap multiple nodes simultaneously** — bootstrap one at a time and wait for `UN` status.

### Decommissioning a Node

```bash
nodetool decommission

# Monitor:
nodetool status   # node transitions: UN → UL (Leaving) → gone from ring
```

`decommission` streams the node's data to the remaining nodes before leaving. **Prerequisite:** ensure `nodetool repair` has been run recently.

### Replacing a Dead Node

```bash
export JVM_OPTS="$JVM_OPTS -Dcassandra.replace_address=<dead-node-ip>"
systemctl start cassandra
# The new node streams data from existing replicas

# After bootstrap (UN status):
nodetool repair mykeyspace
```

### Rolling Upgrade Procedure

```bash
# Upgrade one node at a time — never bring down multiple nodes simultaneously
nodetool drain                    # 1. flush memtables, stop accepting writes
systemctl stop cassandra          # 2. stop Cassandra
# 3. install new version
systemctl start cassandra         # 4. start
nodetool status                   # 5. verify UN status

# After all nodes upgraded:
nodetool upgradesstables          # rewrite SSTables to new format (on each node)
```

### Disk Space Management

```bash
du -sh /var/lib/cassandra/data/*
nodetool cfstats mykeyspace.users | grep "Space used"
nodetool compact mykeyspace users         # force compaction (reclaims tombstone space)
nodetool cleanup mykeyspace               # after adding nodes
nodetool clearsnapshot -t <snapshot-name> # clean up old snapshots
```

**Disk headroom rule:** always keep at least 50% of total disk free to accommodate compaction. STCS requires 2× space during compaction. If disk fills up, compaction stops → read amplification grows → a vicious cycle.

---

# Part 9 — Review: Common Study Questions

**Q: What is the difference between partition key and clustering key?**
The partition key determines which node(s) store the data (via Murmur3 hash → token → node). All rows with the same partition key are co-located. The clustering key defines the sort order of rows within a partition and enables range queries within that partition.

**Q: Why can't Cassandra do joins?**
Data is distributed across nodes by partition key. A join would require fetching data from potentially every node in the cluster and assembling it at the coordinator — equivalent to `ALLOW FILTERING` and would kill performance. The solution is to denormalize: maintain separate tables shaped for each query pattern.

**Q: What is a tombstone? What problems can they cause?**
A tombstone is a delete marker. Cassandra never overwrites data in-place; deletes write a tombstone with a timestamp. Problems: (1) tombstone accumulation degrades read performance — `tombstone_warn_threshold` (default 1,000) and `tombstone_failure_threshold` (default 100,000) guard against this, aborting reads that cross the fail threshold; (2) zombie data resurrection if `gc_grace_seconds` passes without repair and a stale replica rejoins with old data; (3) explicitly writing null values generates tombstones — a common source of unexpected tombstone accumulation.

**Q: How does Cassandra achieve high write throughput?**
Three mechanisms: (1) Sequential writes — CommitLog is append-only, memtable absorbs random writes, SSTable flushes are large sequential I/O batches; (2) Leaderless replication — writes go directly to all replicas in parallel, no bottleneck leader; (3) Tunable consistency — `LOCAL_ONE` skips waiting for multiple replicas.

**Q: When would you choose STCS over LCS?**
STCS: write-heavy workloads where reads are infrequent (logs, IoT ingestion). Less I/O during normal writes. LCS: read-heavy or mixed workloads where low read latency matters. Higher write amplification but predictable O(log N) reads.

**Q: What happens during a Cassandra read at QUORUM with RF=3?**
Coordinator sends a full data request to the fastest expected replica and digest-only requests to the other 2 replicas. It waits for the data response and at least 1 digest (quorum = 2). If the digest matches the data, it returns the result. If not, it fetches full data from all replicas, merges by timestamp, returns the winner, and issues a background read repair to the stale replica.

**Q: What is the role of the coordinator node?**
The coordinator is any node in the cluster that receives the client's request. It is responsible for: determining which nodes own the relevant token ranges (replica set), forwarding the request to all required replicas, waiting for the required number of acknowledgments per the CL, merging responses for reads, and returning the result to the client. Any node can be a coordinator — Cassandra has no fixed coordinator role.

**Q: Why is ALLOW FILTERING dangerous?**
It forces a full partition scan across every node in the cluster. Cost = O(total data in the cluster). In a 100-node cluster with 1 TB per node, a single `ALLOW FILTERING` query may scan 100 TB of data. Never acceptable in production on large tables in hot paths.

**Q: How does hinted handoff differ from replication?**
Replication is synchronous (within the CL requirement) — the coordinator sends to all replicas and waits for the required number before acking the client. Hinted handoff is a fallback for when a replica is temporarily down — the coordinator stores the mutation as a hint and delivers it when the replica recovers. Hints are ephemeral (3h window) and are not a consistency guarantee.

**Q: What is the difference between `nodetool repair` and hinted handoff?**
Hinted handoff is automatic and covers writes missed during a short outage (within the hint window). Repair is manual/scheduled and uses Merkle tree comparison to find and fix any divergence — including divergence from long outages (beyond hint window), silent replica drift, and scenarios where the coordinator itself died.

**Q: How do you prevent zombie data?**
Run `nodetool repair` at least once within every `gc_grace_seconds` window (default 10 days). Repair ensures that tombstones are replicated to all replicas before they are purged. Without repair, a tombstone purged from live replicas may be missing from a stale replica, and when that replica recovers, the old data reappears.

**Q: What consistency level provides strong consistency with RF=3?**
Write `QUORUM` + Read `QUORUM` (or any combination where write replicas + read replicas > RF). With RF=3: 2+2=4 > 3. `LOCAL_QUORUM` both ways provides strong consistency within a single DC.

**Q: What is a hot partition and how do you fix it?**
A hot partition occurs when many reads or writes target the same partition key, causing one node to receive disproportionate load. Fix strategies: (1) add a random shard bucket to the partition key to distribute load across multiple partitions (reads must then query all shards and aggregate); (2) redesign the data model to spread the access pattern; (3) use caching for read-heavy hot partitions.

**Q: What changed in Cassandra 4.0?**
Incremental repair became GA, default vnodes reduced from 256 to 16, full query logging for debugging, audit logging, virtual tables, Java 11 support, improved streaming performance (4× faster in some benchmarks), and transient replication (preview).

**Q: What is SAI and why was it introduced in C* 5.0?**
SAI (Storage Attached Index) is a per-SSTable on-disk index that replaces SASI. It supports equality, range, and LIKE queries with lower overhead than classic 2i, and is the foundation for vector search (ANN) in C* 5.0. SASI was removed in 5.0 due to its inconsistent behavior and maintenance burden.

**Q: Why is TokenAwarePolicy important for driver configuration?**
Without it, the driver picks a random node as coordinator, which then routes the request to the actual replica — 2 network hops. With TokenAwarePolicy, the driver hashes the partition key locally and sends the request directly to the owning replica as coordinator — 1 hop with a local read. One of the highest-leverage performance configurations in a Cassandra application.

**Q: Why are prepared statements mandatory in production?**
Two reasons: performance (CQL is parsed once and the query plan cached; re-use eliminates per-request parse overhead) and security (parameters are sent separately from the query string, preventing CQL injection).

**Q: What is wrong with using a large logged batch for a multi-table write?**
A logged batch across multiple partitions is slower than individual writes, not faster. The coordinator must write a batch log to 2 nodes before executing any statement, then delete the log after. The "atomicity" guarantee means all statements eventually complete, but there is no isolation — other queries can observe partial state during execution.

**Q: What happens if the system_auth keyspace has RF=1 and a node dies?**
All client authentication fails cluster-wide — no client can log in. The cluster is effectively inaccessible even though the data keyspaces are fully available. Fix: `ALTER KEYSPACE system_auth WITH replication = {'class': 'NetworkTopologyStrategy', 'dc': 3}` followed by `nodetool repair system_auth`.

**Q: What is the `DroppedMessages` metric and why is it critical?**
When a Cassandra node's thread pools are saturated and a message cannot be processed within its timeout, the node drops the message silently. Dropped `MUTATION` messages mean writes were silently discarded — direct data loss risk. Any `DroppedMessages > 0` is an immediate alert condition.

**Q: How do Cassandra counters work and why are they not idempotent?**
Each replica maintains a local increment delta for a counter cell. The visible value is the sum of all deltas across all replicas. When you increment, each replica increments its own local delta — no read or lock needed. Non-idempotency: if an increment write is sent, the ack is lost in transit, and the driver retries, the increment is applied twice (once per delivery).

**Q: How does CDC work and what is the back-pressure risk?**
CDC writes a copy of every mutation to a `cdc_raw` directory on each node. A consumer (e.g., Kafka Connect, Debezium) reads from this directory. If the consumer falls behind and `cdc_raw` fills up to `cdc_total_space_in_mb`, Cassandra pauses all writes to CDC-enabled tables until the consumer catches up.

**Q: What is digest read and why does Cassandra use it?**
For a read at QUORUM (RF=3), the coordinator sends a full data request to one replica and digest-only requests to the others. Digests are small fixed-size hashes of the row data. This reduces network bandwidth — instead of fetching a full row from 2 replicas, Cassandra fetches it once and just verifies the other replicas agree via their hashes.

**Q: What is speculative retry and when should you configure it?**
Speculative retry fires a second read request to another replica if the first hasn't responded within a threshold. The first response back wins. It reduces p99 latency by hedging against slow replicas (e.g., a GC pause on one node). Configure as `'99percentile'` (fires if no response by the table's observed p99 latency) or a fixed ms threshold.

**Q: What is the difference between a NULL value and a missing column in Cassandra?**
There is no true NULL in Cassandra. A column that was never written simply does not exist in the SSTable — no storage cost. If you explicitly SET a column to null, Cassandra writes a tombstone for that column — a delete marker that costs storage and accumulates over time. Applications that INSERT with null placeholder values for all optional columns generate tombstones for every absent field, causing read degradation on tables with many optional columns.

**Q: What are guardrails and why do they matter?**
Guardrails (C* 4.1+) are operator-configurable thresholds that warn or block operations likely to cause operational problems — oversized partitions, too many tables per keyspace, over-large collections, excessive batch sizes. Without guardrails, developers can accidentally introduce data model anti-patterns at the application level that only manifest as performance problems months later at production data volumes.

**Q: What is transient replication?**
Transient replication (C* 4.0, preview) allows some replicas to hold data temporarily rather than permanently. With `RF=5/2`, you get 5-way fault tolerance with only 3 full copies of every row — roughly 3× storage cost instead of 5×. Transient replicas participate in reads but only hold data transiently during topology changes. It is still experimental as of C* 5.0.

**Q: What is the BTI (Big Trie-based Index) format in C* 5.0?**
BTI replaces the old two-level `Index.db + Summary.db` SSTable index with a trie-based structure (`Partitions.db` + `Rows.db`). Benefits: faster partition lookups (O(key length) trie traversal vs binary search), smaller in-memory footprint (shared key prefixes in trie), and faster node startup. C* 5.0 also uses a trie-based memtable (instead of skip-list) for better cache locality and reduced GC pressure. Existing SSTables in old format remain readable; `nodetool upgradesstables` rewrites them to BTI.

**Q: What is the correct minimum number of racks per DC, and why?**
Minimum 3 racks (mapped to AZs) when RF=3. `NetworkTopologyStrategy` places one replica per rack. With only 2 racks and RF=3, one rack holds 2 replicas — losing that rack means losing quorum for those partitions. With 3 racks, any single rack failure leaves 2 replicas intact, which meets LOCAL_QUORUM (requires 2 of 3).

**Q: How do you interpret high SSTable count in nodetool tablestats?**
For STCS, a healthy table has 4–8 SSTables; > 20 means compaction is significantly behind. For LCS, L0 should be near 0 (files should be pushed to L1 quickly); a growing L0 means writes are arriving faster than compaction can process them. High SSTable count directly causes read amplification — each read must scan more files and merge more versions.

**Q: What is cassandra-stress used for?**
`cassandra-stress` is the official benchmarking tool bundled with Cassandra. Uses include: capacity planning (find single-node throughput ceiling), performance regression testing (before/after tuning changes), hardware sizing (compare instance types), and reproducing production workloads (custom YAML profiles with realistic key distributions such as Zipfian for hot-key simulation).

**Q: What are the key concurrent_reads and concurrent_writes settings, and how do you tune them?**
`concurrent_reads` and `concurrent_writes` in `cassandra.yaml` control how many parallel read/write operations run on each node. `concurrent_reads` defaults to 16× number of disks — increase if `ReadStage` shows high pending counts in `nodetool tpstats`. `concurrent_writes` is rarely the bottleneck (memtable writes are fast); default 32 is usually fine. Setting these too high causes thread contention and GC pressure without throughput gain.
