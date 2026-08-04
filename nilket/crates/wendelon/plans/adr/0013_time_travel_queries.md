# ADR 013: Time Travel Queries and MVCC Retention

## Status
Accepted

## Context
Modern analytical databases (like Snowflake or BigQuery) increasingly support "Time Travel" or point-in-time recovery features. Users need to query the database "AS OF" a specific timestamp for auditing, machine learning model reproducibility, or accidental deletion recovery.

We realized that Wendelon's foundational architecture—specifically its reliance on Hybrid Logical Clocks (HLCs) for ordering, its append-only immutable `io_uring` slabs, and its lazy tiering to S3 (L2)—already implements Multi-Version Concurrency Control (MVCC) by default. Every mutation is written as a new record with a strictly increasing `commit_hlc`.

## Decision
Wendelon will explicitly expose and support **Time Travel Queries**. 

1. **API Support:** `ReadRequestMeta` and `QueryRequestMeta` will eventually be extended with an optional `as_of_hlc` parameter. If provided, the storage engine will scan the index and return the most recent record where `record.hlc <= as_of_hlc`, ignoring any newer mutations.
2. **MVCC Retention Policy:** The Background Sweeper (ADR 006) and future compaction tasks will respect a cluster-configured `RETENTION_WINDOW` (e.g., 7 days). Each node derives its retention cutoff from its local wall clock; overwritten or deleted records will **NOT** be physically compacted or purged from the slabs (L1 or L2) until their `commit_hlc` falls outside that wall-clock window. This is intentionally a bounded-skew policy, not a replicated cluster-wide cutoff: ADR 012's drift guard caps cross-node disagreement by aborting nodes whose physical clocks diverge beyond the configured maximum offset.
3. **Infinite Tiered History:** Because L2 (S3) storage is cheap, operators may configure Wendelon to never drop old versions once they hit L2, enabling infinite time travel at the cost of object storage space.

## Rationale
- **Architecturally "Free":** Because Wendelon already uses append-only slabs and tags every write with an HLC for linearizability, time travel requires almost no structural changes to the write path. We simply need to adjust the read path to filter by an upper HLC bound, and constrain the garbage collector.
- **Ecosystem Alignment:** This is a killer feature for the Polariton/DataFusion analytics ecosystem we are backing, allowing seamless reproducibility of analytical runs.

## Consequences (The "Negatives" / Rejected Ideas)
- **Storage Amplification:** Retaining historical versions means high-churn keys will consume significantly more disk/S3 space than a purely in-place update engine. We accept this storage cost, leveraging the L2 tiering to keep NVMe costs bounded.
- **Index Complexity:** The local `key_index` (`DashMap`) currently assumes "last write wins" and drops pointers to old records. To support time travel, the index will eventually need to map a `key_hash` to a list or tree of `(hlc, RecordLocation)` tuples. This will slightly increase memory overhead and lookup complexity in the future.
- **Retention Window Semantics:** The advertised retention interval is "wall clock ± bounded drift," not an exactly replicated global cutoff. This is acceptable for TTL-style MVCC retention because ADR 012 already treats larger clock error as a fatal condition.
- **Implementation Deferred:** We accept this design now, but defer the actual API exposure and index refactoring to a later target. The critical step today is ensuring we don't accidentally design a compaction system that precludes it.
