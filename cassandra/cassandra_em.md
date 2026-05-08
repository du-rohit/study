# Apache Cassandra — Engineering Manager's Guide

> This document is written for EMs who need to make architectural decisions, review designs,
> lead incident reviews, size teams, and talk about Cassandra trade-offs with engineers and stakeholders.
> It does not cover implementation internals — see `cassandra_v2.md` for that depth.

---

# Part 1 — The Decision Layer: When to Use (and Not Use) Cassandra

## What Problem Cassandra Solves

Cassandra is optimized for one thing above all else: **extremely high write throughput at large scale, with predictable low latency, across multiple datacenters**.

The canonical use cases:
- **Time-series / event streams** — IoT sensor data, clickstreams, application logs, metrics
- **User activity feeds** — "last N events for user X" across millions of users
- **Messaging** — inbox storage, notification history
- **Session / token storage** — high-read-rate, known key lookup
- **Leaderboards / counters** — approximate (not exact) counts at high write rate

The common thread: **write-heavy, known access patterns, high cardinality, tolerable eventual consistency**.

---

## When Cassandra Is a Strong Fit

Ask these questions. More "yes" answers = stronger fit.

| Question | Why it matters |
|---|---|
| Do you have millions to billions of rows? | Cassandra's distributed model adds overhead; small datasets don't justify it |
| Are writes > reads in volume? | LSM storage engine is optimized for writes; read-heavy workloads have better options |
| Do you know all your query patterns upfront? | Cassandra requires query-driven schema design — ad-hoc queries are expensive |
| Can you tolerate eventually consistent reads? | Cassandra is AP — strong consistency is possible but at a latency cost |
| Do you need multi-region active-active? | Cassandra's leaderless model makes active-active natural |
| Is the data time-series or append-heavy? | Time-ordered data with TTL maps perfectly to Cassandra's storage model |
| Is horizontal scale a hard requirement? | Cassandra scales linearly by adding nodes — no resharding, no downtime |

---

## When NOT to Use Cassandra

This is equally important. Cassandra is a poor fit when:

**You need flexible queries or ad-hoc analytics**
Cassandra requires schema design around specific query patterns. If your team regularly writes new queries against the data (BI, analytics, data science), Cassandra will be a constant source of friction. Use a data warehouse (BigQuery, Redshift) or a document database (MongoDB) instead.

**You need joins or complex relationships**
Cassandra has no joins. Every query pattern requires its own denormalized table. A heavily relational domain (e.g., e-commerce with orders → line items → products → suppliers) produces an explosion of denormalized tables. Use PostgreSQL.

**You need strong ACID transactions**
Cassandra has lightweight transactions (Paxos-based) for single-partition conditional writes, but these are slow and limited. Multi-partition transactions (ACCORD) are still in preview. If your domain is financial transfers, inventory deductions, or anything requiring "all-or-nothing" across multiple entities — use a transactional database.

**Your dataset is small (< 100 GB)**
Cassandra's operational overhead (3-node minimum, repairs, compaction tuning, JVM management) is not justified for small datasets. PostgreSQL with read replicas handles hundreds of GB with far less operational complexity.

**You need exact counts or aggregations**
Cassandra's counter columns are approximate (not idempotent — retries can double-count). There is no native `COUNT(*)` across partitions without a full cluster scan. If exact inventory counts, financial balances, or precise aggregations matter, look at transactional databases or purpose-built solutions.

**Your team has no Cassandra experience**
Cassandra is operationally demanding. A team that hasn't run it before will make costly data model mistakes early (wrong partition keys, hot partitions, tombstone accumulation) that are expensive to fix after data is in production. Factor in a learning curve of 3–6 months before the team is truly comfortable.

---

## Cassandra vs Alternatives — Decision Framework

| Scenario | Recommendation | Why |
|---|---|---|
| High-throughput time-series, multi-region | **Cassandra** | Its core strength |
| Same workload, team prefers fully managed | **DataStax Astra** or **AWS Keyspaces** | CQL-compatible, zero ops overhead |
| High-throughput, C* compatible, lower latency | **ScyllaDB** | C++ rewrite of Cassandra; no JVM GC; significantly lower p99 |
| Key-value at massive scale, AWS-native | **DynamoDB** | No ops, serverless scaling, AWS integrations |
| Document store, flexible schema, moderate scale | **MongoDB** | Better query flexibility |
| Relational + read replicas, < 1 TB | **PostgreSQL / Aurora** | ACID, joins, far simpler to operate |
| Time-series with SQL | **TimescaleDB** | PostgreSQL extension; SQL + time-series optimizations |
| In-memory caching layer | **Redis** | Cassandra is not a cache — latency is disk-bound |

---

## Self-Hosted vs Managed Cassandra

This is often the most important architectural decision an EM makes before committing to Cassandra.

### Self-Hosted (Apache Cassandra)

**You own everything:** hardware/VM sizing, OS configuration, JVM tuning, upgrades, repairs, backups, monitoring, on-call.

| Consideration | Detail |
|---|---|
| Minimum viable cluster | 3 nodes per datacenter (RF=3); 6 nodes for any meaningful load |
| Ops overhead | 1 dedicated SRE/DBA per ~5–10 Cassandra clusters in production |
| Repair obligation | Weekly scheduled repairs minimum — skipping causes data divergence |
| Upgrade complexity | Rolling upgrades, one node at a time; major version upgrades are multi-month projects |
| JVM management | GC tuning required for p99 latency SLAs; skill-intensive |
| When it makes sense | Very high traffic (managed services too expensive), strict data residency, deep customization needed |

### Managed Options

| Service | Compatibility | Key Trade-off |
|---|---|---|
| **DataStax Astra DB** | Full CQL / Cassandra-compatible | Most feature-complete managed option; serverless tier available |
| **AWS Keyspaces** | CQL-compatible (subset) | Serverless, auto-scaling; limited feature set (no LWT, limited compaction control) |
| **ScyllaDB Cloud** | CQL-compatible (Scylla engine) | Lower latency than JVM Cassandra; strong managed offering |
| **Instaclustr** | Apache Cassandra | Managed self-hosted clusters on your cloud; more control than pure SaaS |

**When managed makes sense:**
- Team is small (< 5 engineers) and can't dedicate ops capacity to Cassandra
- You're early-stage and data scale doesn't justify self-hosted overhead yet
- AWS-first architecture with DynamoDB as alternative already evaluated
- Cost analysis shows managed is cheaper than engineer-hours for ops

**When self-hosted makes sense:**
- Traffic is high enough that managed pricing is prohibitive (typically > 50 TB or > 1M writes/sec sustained)
- Regulatory requirements demand self-managed infrastructure
- Team already has deep Cassandra expertise
- Need features that managed services don't support (CDC, full compaction control, custom compaction strategies)

---

# Part 2 — Core Concepts an EM Must Own

## The Data Model Mental Model

Cassandra does not store data like a relational database. Understanding this is prerequisite to reviewing any Cassandra schema design from your team.

### The Key Rule: One Table Per Query Pattern

In SQL, you normalize data into entities and write flexible queries. In Cassandra, you design tables around queries — each access pattern typically gets its own table.

```
SQL thinking:           Cassandra thinking:
  users table             users_by_id table      (look up user by ID)
  orders table    →       users_by_email table   (look up user by email for login)
  JOIN on demand          orders_by_user table   (get orders for a user)
                          orders_by_status table (find all pending orders)
```

**What this means for your team:**
- Schema changes are expensive — changing a query pattern often means adding a new table and backfilling data
- Data is duplicated across tables by design — this is intentional, not a bug
- Storage costs are higher than a normalized relational schema (typically 2–4× for the same logical data)
- A "simple" new product requirement (e.g., "let users search by phone number") can be a significant engineering project

### The Partition Key Is Everything

The partition key determines which server holds the data. All data with the same partition key lives on the same servers. This has two consequences your team needs to design around:

**Hot partitions:** if many users or requests hit the same partition key, one server absorbs disproportionate load while others sit idle. Common culprits: partitioning by `date` (all of today's writes go to one node), partitioning by `status` (all "active" records on one partition), or popular content (viral post's comments all on one partition).

**Partition size:** partitions should stay under ~100 MB. A partition that grows without bound (e.g., "all events for user X ever") will eventually cause slow reads, slow repairs, and compaction problems. The engineering fix is "bucketing" — adding a time period to the partition key (`user_id + month` instead of `user_id`).

**When reviewing schema designs, ask:**
- What does the partition key look like for this table?
- Is there any partition key that could grow unboundedly?
- What happens if one partition key is much more popular than others?

---

## CAP Theorem: What "AP" Means for Your Product

Cassandra is an **AP system** — it chooses Availability and Partition Tolerance over strict Consistency.

In plain terms: **if two users write to the same data at the same time from different regions, both writes succeed, and the conflict is resolved later by taking the one with the later timestamp.** The "loser" write is silently discarded.

### Consistency Is Tunable, Not Binary

Cassandra lets you dial consistency per operation:

| Level | What it means | When to use |
|---|---|---|
| `LOCAL_ONE` | 1 server in your region must confirm | Maximum throughput; staleness acceptable (IoT, analytics) |
| `LOCAL_QUORUM` | Majority of servers in your region must confirm | Strong within a region; most production services |
| `QUORUM` | Majority across all regions | Cross-region strong consistency; pays latency penalty |
| `ALL` | Every server must confirm | Admin operations only; any server down = failure |

**For most production services:** `LOCAL_QUORUM` for both reads and writes gives you strong consistency within a region and is the right default.

### Explaining Eventual Consistency to Non-Engineers

For stakeholders or product managers who ask "will users always see up-to-date data?":

> "With Cassandra configured at LOCAL_QUORUM, reads within a region are strongly consistent — a user will see their own writes immediately. The 'eventual' part only applies between regions: if a user writes in the US and immediately reads from Europe, there may be a lag of milliseconds to seconds before the European region has the update. For most user-facing features, this is imperceptible. It becomes relevant for scenarios like financial balances or exact inventory counts — those require a different design approach."

---

## Replication: What RF=3 Means in Practice

Replication Factor (RF) = how many copies of each row exist across the cluster.

**RF=3 is the production standard.** It means:
- Every row is stored on 3 different servers
- The cluster can tolerate 1 server failure with no user impact (at QUORUM)
- Storage cost = 3× raw data size (plus ~50% headroom for compaction)

**Multi-region:** RF=3 per region means 3 copies in the US, 3 in Europe, etc. A full region failure loses the replication lag (milliseconds of writes) but the service continues operating from the surviving region.

**What RF does NOT protect against:**
- Accidental `TRUNCATE TABLE` or `DROP TABLE` — the delete is replicated to all copies immediately
- Application bugs that corrupt data — the bad data is replicated faithfully
- These require point-in-time backups, not replication

---

# Part 3 — Multi-Region and Reliability

## Active-Active vs Active-Passive

**Active-Active:** every region accepts both reads and writes simultaneously. Users are routed to their nearest region.

```
US users → us-east-1 cluster (reads + writes)
EU users → eu-west-1 cluster (reads + writes)
Both clusters replicate to each other asynchronously
```

- **Benefit:** lowest latency globally; survives a full region outage with no failover needed
- **Risk:** if the same record is updated in both regions simultaneously, one update is silently lost (Last-Write-Wins)
- **Works well when:** data is naturally region-scoped (US users rarely update EU users' data), or staleness is acceptable

**Active-Passive:** one region is primary (writes), others are read-only replicas.

- **Benefit:** no write conflicts
- **Risk:** failover requires a manual or automated promotion step; cross-region reads add latency

Cassandra's leaderless architecture makes active-active the more natural choice. Most teams using Cassandra across regions run active-active.

---

## Last-Write-Wins: The Conflict Resolution Model

When two writes to the same record arrive from different regions, Cassandra resolves the conflict using **timestamps** — the write with the later timestamp wins. The other write is permanently discarded with no notification.

**This is correct behavior for:**
- User profile updates ("set my address to X")
- Cache-like data, configuration values
- Sensor readings (latest reading is what matters)

**This silently breaks things for:**
- Counters ("increment my score by 1" — if both regions do this, one increment is lost)
- Appending to a list ("add this item to my cart" — one addition may be dropped)
- Any scenario where both writes represent real user intent and both must be preserved

**When reviewing a design that uses Cassandra active-active, ask:**
> "What happens if the same record is written from two regions at the same millisecond? Is it safe to silently drop one of those writes?"

---

## RPO and RTO — What to Expect

| Failure Scenario | RPO (data loss) | RTO (downtime) | Notes |
|---|---|---|---|
| Single server failure | 0 | ~30 seconds (gossip detection) | Automatic; no action needed if RF=3 |
| Single AZ failure | 0 | ~30 seconds | Requires 3 AZs per region in your rack config |
| Full region failure | Milliseconds (async replication lag) | Near-zero if active-active | Surviving regions continue serving traffic |
| Accidental DELETE / TRUNCATE | 0 (delete propagated) | Hours–days (restore from backup) | Replication cannot protect against this |
| Backup restore (worst case) | Time since last snapshot | Hours | Depends on data size and restore tooling |

**Key insight for EMs:** Cassandra's replication protects against hardware failure extremely well. It does **not** protect against logical data corruption or accidental deletes. Backup and restore procedures are your only defense against those scenarios, and restoring a large Cassandra cluster from backup is a multi-hour to multi-day operation depending on data volume.

---

## Cassandra's Reliability Guarantees

**Cassandra will protect you from:**
- Individual server failures (automatic, transparent)
- AZ-level outages (with proper rack configuration)
- Network partitions between nodes (writes continue; repairs converge afterward)
- Rolling upgrades with zero downtime

**Cassandra will NOT protect you from:**
- Application-level data corruption (bad data is replicated faithfully to all copies)
- Accidental `TRUNCATE` or `DROP` (propagated immediately to all replicas)
- Backup lag — if you need to recover to "30 minutes ago," you need point-in-time backup infrastructure set up in advance
- A poorly designed data model — hot partitions, unbounded partitions, and tombstone accumulation are engineering problems, not infrastructure problems

---

# Part 4 — Operational Overhead and Team Implications

## What It Takes to Run Cassandra in Production

Running Cassandra is operationally heavier than most databases. Be honest about this when evaluating it:

### The Non-Negotiables

**Regular repair runs:** Cassandra requires periodic "repair" operations to keep replicas in sync. If repair is not run at least every 10 days, data can "resurrect" after node recovery (old deleted data reappears). This must be automated and monitored. Neglecting repairs is one of the most common sources of Cassandra production incidents.

**Compaction management:** Cassandra periodically merges on-disk data files in the background. If compaction falls behind, read performance degrades. Disk must stay < 70% full or compaction cannot run. Teams need to monitor this and have runbooks for intervention.

**Schema changes are limited:** you cannot change a table's primary key — that requires creating a new table and migrating all data. Adding columns is safe; most other structural changes are not. Data model decisions made early have long-lasting consequences.

**Backups are manual by default:** Cassandra has no built-in "take a backup every night" feature. Teams must set up tooling (typically Medusa or custom scripts) and verify restore procedures regularly. "I think backups are running" is not sufficient.

### Team Size and Skill Requirements

| Cluster size | Recommended ops staffing | Notes |
|---|---|---|
| 1 cluster (non-critical) | 0.25 FTE (part of a generalist SRE role) | Managed service strongly recommended at this scale |
| 1–3 clusters (production) | 0.5–1 FTE dedicated | Should be someone with 1–2 years Cassandra experience |
| 3–10 clusters (production) | 1–2 FTE dedicated | At least one team member should be a Cassandra specialist |
| 10+ clusters | 2–4 FTE + on-call rotation | Justifies a dedicated data infrastructure team |

**Skills to hire for when building a Cassandra team:**
- Data modeling experience (partition key design, denormalization patterns) — this matters more than ops knowledge
- Experience with JVM tuning and GC behavior
- Understanding of LSM storage and compaction strategies
- Operational experience: repair, bootstrap, rolling upgrade, capacity planning
- Distributed systems fundamentals (quorum, consistency levels, CAP trade-offs)

**Red flag in a Cassandra hire:** someone who has only used Cassandra through an ORM or abstraction layer without understanding partition key design. The abstraction hides the most consequential decisions.

---

## Common Failure Modes and Their Business Impact

These are the incidents your team will face. Know them before they happen.

### Hot Partition

**What:** many requests hitting the same partition key. One server is overwhelmed while others are idle. Reads and writes to that partition slow down or time out.

**Business impact:** latency spike or partial outage for the subset of users hitting the hot key. Often correlated with viral events (a popular post, a promotional item, a celebrity's profile).

**How long to fix:** minutes if the fix is a query-level change (add a cache in front), hours to days if the data model needs to change.

**How to detect:** `nodetool tpstats` shows one node's thread pools saturated; read/write latency spikes on one node while others are healthy.

### Tombstone Accumulation

**What:** data was deleted (or null values written) faster than Cassandra's compaction can clean up the delete markers. Reads slow down as the system must scan through thousands of dead records to find live ones.

**Business impact:** gradual read latency degradation that worsens over time. Can trigger hard read failures (`TombstoneOverwhelmingException`) which surface as errors to users.

**How long to fix:** hours to days — requires forced compaction and potentially a data backfill if the pattern is in the application code.

**How to detect:** `nodetool tablestats` shows high tombstone counts; system logs show tombstone scan warnings.

### Missed Repair Window

**What:** repair hasn't run in > 10 days (the default `gc_grace_seconds`). Tombstones have been physically purged on some replicas but a stale replica still has the old data. Reads may return deleted ("zombie") records.

**Business impact:** users seeing data they deleted (GDPR risk if this is personal data), incorrect application behavior, data integrity issues.

**How long to fix:** a full repair across the affected keyspace — hours to days depending on data volume.

**How to detect:** repair job monitoring alerts; stale nodes; data divergence observed at the application layer.

### Disk Fill

**What:** available disk drops below ~30%. Cassandra's compaction requires free space (up to 2× the data size during compaction). When disk fills, compaction stops → SSTable count grows → reads degrade → disk fills faster.

**Business impact:** read performance degrades rapidly. If disk fills completely, writes stop. Recovery requires urgent intervention — cannot easily add disk to a running node.

**How long to fix:** hours — requires either emergency data deletion, adding a new node to redistribute data, or expanding disk.

**How to detect:** disk utilization alerts at > 60% (warn), > 75% (critical).

### Unmonitored CDC Consumer

**What:** CDC (Change Data Capture) is enabled on a table to stream changes to Kafka. The consumer falls behind and the CDC log directory fills up. Cassandra pauses all writes to CDC-enabled tables.

**Business impact:** writes stop for affected tables. A consumer that hangs at 3am can halt production writes until someone intervenes.

**How long to fix:** minutes once identified — restart consumer or increase CDC disk quota.

**How to detect:** `cdc_raw` directory size alerts; write latency spike on CDC-enabled tables.

### system_auth RF=1

**What:** the internal keyspace that stores credentials has replication factor 1. One server holds the only copy. That server fails.

**Business impact:** **entire cluster becomes inaccessible** — no client can authenticate. Even though your data is available across RF=3, nobody can read it.

**How long to fix:** restore the failed node (minutes to hours), or manually reconfigure `system_auth` replication (requires cluster restart in severe cases).

**How to detect:** authentication errors cluster-wide coinciding with a single node failure.

**How to prevent:** `ALTER KEYSPACE system_auth WITH replication = {'class': 'NetworkTopologyStrategy', 'dc': 3}` — this must be done at cluster setup and is a common omission.

---

## Incident Response Mental Model

When a Cassandra incident is reported, the first three questions to ask:

1. **Is it one node or all nodes?** — Single node issues are almost always hardware or GC. Cluster-wide issues are usually data model problems (hot partition, tombstone storm) or a configuration error.

2. **Did anything change recently?** — Schema changes, new query patterns, increased write rate, new application code. Cassandra incidents often have a "trigger event" 24–48 hours before the symptoms appear.

3. **What is the `DroppedMessages` count?** — This is the single most important metric. If `MUTATION` drops are > 0, writes are being silently discarded. This is an emergency. Everything else is a degradation.

---

# Part 5 — Cost Model

## Storage Cost

The true storage cost of a Cassandra cluster is higher than the raw data size:

```
Raw data:                    100 GB
× Replication factor (RF=3):  × 3   → 300 GB
× Compaction headroom (50%):  × 1.5 → 450 GB
× Index + bloom filter:       × 1.1 → ~500 GB

A 100 GB dataset requires ~500 GB of provisioned storage per region.
```

For multi-region deployments, multiply by number of active regions.

**Practical rule of thumb:** provision 5× the raw dataset size per region when sizing storage.

---

## Infrastructure Sizing Mental Model

**Minimum production cluster:**
- 3 nodes per datacenter (RF=3 requires minimum 3)
- In practice, 6 nodes gives better load distribution and makes rolling upgrades/repairs less risky
- Avoid even-numbered node counts (quorum math is cleaner with odd counts: 3, 5, 7)

**CPU:** Cassandra is CPU-intensive during compaction. Under-provisioned CPU → compaction falls behind → read degradation. Rule of thumb: 8–16 cores per node for moderate workloads.

**Memory:** more RAM = larger memtable + more bloom filters in memory = fewer disk reads. Rule of thumb: 32 GB minimum per production node; 64–128 GB for high-read-rate workloads.

**Disk:** SSDs are effectively mandatory for production. Spinning disks cannot sustain the random read patterns during compaction and range queries. Avoid network-attached storage (NAS/SAN) — Cassandra's performance assumes local disk.

---

## Total Cost of Ownership Comparison

For a 10 TB dataset, high throughput (100k writes/sec), 2 regions, 3-year horizon:

| Option | Infrastructure | Engineering Cost | Total (3yr estimate) |
|---|---|---|---|
| Self-hosted (6 nodes × 2 regions) | ~$150k–250k/yr | 0.5–1 SRE FTE ($75k–150k/yr) | $675k–1.2M |
| DataStax Astra (serverless) | Usage-based (~$0.25/GB + ops) | Minimal ops | Varies widely; can be 2–5× infrastructure at high scale |
| AWS Keyspaces | Usage-based | Minimal ops | Comparable to Astra; AWS credits may apply |
| ScyllaDB Cloud | ~$100–200k/yr | Minimal ops | Lower infrastructure cost than Cassandra due to efficiency |

**Key insight:** managed services are significantly more cost-effective below ~10–20 TB or when engineering headcount is expensive. Self-hosted wins at very high scale (> 50 TB, > 500k writes/sec) where managed pricing becomes prohibitive.

---

# Part 6 — Talking About Cassandra With Stakeholders

## What Questions to Expect (and How to Answer Them)

**"Can we guarantee users always see the latest data?"**

> "Within a single region, yes — we use QUORUM consistency which means a read always reflects the latest confirmed write. Between regions, there's a propagation lag of milliseconds to a few seconds in the worst case. For user-facing features, this is typically invisible. If you have a use case where cross-region read-your-own-writes consistency is critical, we'd need to evaluate that specifically."

**"Can we do a complex search/filter on this data?"**

> "Cassandra is designed for known, fixed access patterns — lookups by a specific key. Complex filters, multi-field searches, or ad-hoc analytics are not its strength. If we need that, we'd either need to maintain a secondary index (with caveats) or feed the data into a search layer like Elasticsearch. What's the specific query you need?"

**"What happens if we lose a data center?"**

> "If we're running active-active across regions at QUORUM per region, the surviving regions continue serving traffic within seconds. We'd lose the replication lag — typically milliseconds of writes that were in-flight to the failed DC at the moment of failure. For the failure mode you're worried about, is that RPO acceptable?"

**"We deleted some records accidentally — can we roll back?"**

> "No — deletes in Cassandra propagate to all replicas immediately. The only recovery is from a backup snapshot. How current is our latest backup, and do we have a tested restore procedure?" *(This is a prompt to verify backup health, not just answer the question.)*

**"Why is Cassandra read performance getting slower over time?"**

> "Cassandra's read performance degrades when compaction can't keep up — deleted data accumulates, and reads have to scan through more files and dead records to find live data. The likely causes are: disk filling up (compaction stops when disk is too full), a write pattern that generates lots of deletions, or compaction falling behind due to I/O saturation. This is a known operational concern — the question is whether monitoring caught it early enough."

---

## The Questions You Should Be Asking Your Team

When your team proposes a Cassandra design or your existing Cassandra clusters start showing problems:

**On schema design:**
- What is the partition key, and can it become a hot partition?
- Are there any partitions that grow without bound? How are they bucketed?
- How many tables exist per keyspace, and is this number growing over time?
- Do we write null values for absent fields, or do we omit absent columns?

**On operations:**
- Are repairs running on schedule? When did the last repair complete?
- What is the current disk utilization, and is compaction keeping up?
- Is there a tested restore procedure from backup? When was the last restore drill?
- Is `system_auth` replicated to RF=3?

**On incidents:**
- What does the `DroppedMessages` count look like? Any non-zero values recently?
- Is there a tombstone accumulation problem on any table?
- What does the SSTable count trend look like per table?

**On scale:**
- What is the largest partition size in production today?
- What is our write rate per node, and how does it compare to node capacity?
- Do we have disk utilization headroom > 50% for compaction?

---

# Part 7 — Hiring: What Good Cassandra Expertise Looks Like

## Interview Questions That Reveal Real Understanding

These questions separate engineers who have used Cassandra from engineers who understand it:

**"Walk me through how you would design the schema for [use case]. What is the partition key, and why?"**
Listen for: query-driven thinking, awareness of access patterns, mention of hot partition risk, bucketing for unbounded data.

**"What happens to write performance and read performance as a Cassandra cluster scales?"**
Listen for: writes scale linearly (this is Cassandra's strength), reads may not scale proportionally if the data model has hot partitions or poor compaction strategy.

**"We're seeing gradually increasing read latency on one table over the past week. Walk me through how you'd diagnose it."**
Listen for: SSTable count, tombstone accumulation, compaction status, bloom filter false positive rate, hot partition investigation.

**"A product manager wants to add a search-by-email feature to a table that only has user_id as the partition key. What are the options and trade-offs?"**
Listen for: secondary index risks (scatter-gather at scale), SAI (C* 5.0), materialized view trade-offs, separate lookup table as the production-safe option.

**"What is gc_grace_seconds and why does it matter?"**
Listen for: tombstone grace period, zombie data risk, repair frequency requirement.

## Red Flags

- "We just used Cassandra's default settings" — implies no performance tuning consideration
- "We use ALLOW FILTERING a lot" — fundamental data modeling anti-pattern
- "We haven't set up scheduled repairs yet" — immediate data integrity risk
- "We use logged batches for multi-table writes for performance" — misunderstanding of what batches do
- "We don't have a backup/restore procedure" — critical operational gap

---

# Part 8 — Review: EM-Level Q&A

**Q: When should I push back on a proposal to use Cassandra?**
Push back when: the use case is read-heavy with flexible queries, the dataset is small (< 100 GB), the team has no Cassandra experience and timeline is tight, ACID transactions are required, or the team hasn't defined the access patterns yet (Cassandra requires these upfront).

**Q: What is the single most common Cassandra failure mode?**
Hot partitions. A partition key that concentrates too many reads or writes on one server causes that server to saturate while others are idle. This is a data model problem, not an infrastructure problem — it cannot be fixed by adding more servers without changing the schema.

**Q: How do I know if our Cassandra clusters are healthy?**
Four signals: (1) `DroppedMessages` is zero across all nodes; (2) repair jobs are completing on schedule (weekly minimum); (3) disk utilization is below 70% on all nodes; (4) read/write p99 latency is within SLA and not trending upward over weeks.

**Q: What does it mean when someone says "we need to change the partition key"?**
It means the data model is wrong for the current access pattern. In Cassandra, changing the partition key requires: creating a new table with the correct schema, backfilling all existing data into the new table, updating all application code to write to both tables during migration, validating the backfill, then cutting over. This is a significant engineering effort — days to weeks depending on data volume and traffic. It's the Cassandra equivalent of a major database migration.

**Q: We're evaluating Cassandra vs DynamoDB for a new service. What are the key deciding factors?**
DynamoDB: fully managed (zero ops), AWS-native, serverless scaling, per-request pricing (good for bursty traffic), limited query flexibility. Cassandra (managed): CQL is more expressive than DynamoDB's query model, better for complex time-series patterns, works across multiple clouds. If your team is AWS-committed and ops overhead is a concern, DynamoDB is lower risk. If you need CQL expressiveness, multi-cloud, or have existing Cassandra expertise, managed Cassandra (Astra, Keyspaces) is the middle ground.

**Q: What is eventual consistency and when does it matter for users?**
In Cassandra at LOCAL_QUORUM (the production default within a region), reads are strongly consistent — users see their own writes immediately. "Eventual" only applies between regions: a write in us-east-1 may not be visible in eu-west-1 for a few milliseconds to seconds. For most product features, this is invisible. It matters when: a user in one region expects to immediately read data written by someone else in another region (e.g., a collaborative document, a financial transaction confirmation). Design those features to read from the write region, or accept the window.

**Q: Why is Cassandra repair so important? What happens if we skip it?**
Repair is Cassandra's anti-entropy mechanism — it compares data across replicas and fixes divergence. If you skip repair for > 10 days (the default gc_grace_seconds): tombstones (delete markers) get physically purged from healthy replicas; if a previously down replica comes back online after this window, it still has the old data (the "zombie" data) and will start serving deleted records to clients. This is a data integrity problem, not just a performance problem. GDPR risk if the "zombied" data is personal data the user requested deleted.

**Q: What is the operational risk of using Cassandra CDC?**
CDC (Change Data Capture) writes every mutation to a local log directory. If the consumer (e.g., Kafka connector) falls behind and the directory fills up, Cassandra **stops accepting writes** to CDC-enabled tables until the consumer catches up. An unmonitored or stuck consumer can silently halt production writes at any time. The mitigation is: monitor consumer lag with the same SLA urgency as write latency, set disk alerts on the CDC directory, and have a runbook for consumer restart.

**Q: What is the realistic RPO/RTO if we lose an entire AWS region?**
Active-active setup at LOCAL_QUORUM: RPO = milliseconds (async replication lag to the failed region at the moment of failure); RTO = near-zero (surviving regions are already serving traffic; driver reroutes automatically within seconds). This is one of Cassandra's genuine strengths — regional failure is largely transparent to users if the architecture is designed correctly. The exception: if the failed region was the only one accepting writes for certain data (active-passive), failover requires explicit promotion and has higher RTO (minutes to hours).

**Q: How should I evaluate whether to use self-hosted Cassandra vs a managed service?**
Start with managed unless you have a clear reason not to. Managed services eliminate the repair obligation, JVM tuning, upgrade management, and on-call burden. Move to self-hosted when: (1) data volume exceeds ~20–50 TB and managed pricing is prohibitive, (2) regulatory requirements demand self-managed infrastructure, (3) you need features the managed service doesn't support (CDC, full compaction control), or (4) your team has deep Cassandra expertise and the operational cost of self-hosting is acceptable.

**Q: What is the minimum rack configuration needed for zero-downtime AZ failure tolerance?**
Three racks (mapped to three AZs) per datacenter when RF=3. Cassandra places one replica per rack. With three AZs, losing one AZ leaves two replicas intact — meeting QUORUM (which requires 2 of 3). With only two AZs, one AZ holds two replicas — losing that AZ loses quorum for the partitions where it holds 2 replicas. This is a setup decision, not easily changed later.
