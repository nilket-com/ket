# ADR 006: Tiered Storage and Lazy Migration

## Status
Accepted (extended by ADRs 022, 023, 024)

## Context
Storing petabytes of historical data on NVMe SSDs is financially unviable. Furthermore, when a new node joins a cluster, eagerly transferring terabytes of data to rebalance the ring (a "bootstrap storm") saturates the network and disk I/O, causing massive latency spikes for active queries.

## Decision
1. **Tiered Storage:** Wendelon will implement append-only slabs mapped to a strict hierarchy: L0 (DRAM) -> L1 (NVMe) -> L2 (Object Storage / S3). A Background Sweeper will autonomously seal, upload, and stub old slabs to L2.
2. **Lazy Migration:** During an epoch transition, data is **NOT** eagerly pushed to new owners. Migration is entirely lazy, triggered by on-demand "Migration Reads" for hot data, and a heavily throttled Background Sweeper for cold data.

## Rationale
- **Economics:** S3 integration from Day 1 ensures the database is economically viable for massive analytical workloads.
- **Anti-Bootstrap Storm:** Lazy migration ensures that adding or removing nodes never impacts front-door I/O. The cluster instantly updates routing mathematically (`ring(E+1)`), while the physical bytes move only when safe or strictly necessary.

## Consequences (The "Negatives" / Rejected Ideas)
- **Rejected Eager Rebalancing:** We explicitly reject the standard DHT approach of eagerly streaming data to achieve immediate physical balance.
- **Latency Spikes on Migration:** Because data moves lazily, the very first read of a key after a topology change will incur a "proxy hop" latency penalty as the new owner fetches it from the old owner. We accept this temporary p99 latency degradation as a trade-off for overall cluster stability.
- **Complex Sweeper Logic:** The Background Sweeper becomes a critical, highly complex state machine responsible for both Tiering (uploading to S3) and Migration (pulling from old owners).

## Subsequent ADRs

The core framing above stands. The following ADRs fill in the concrete contracts that this ADR deliberately left open:

- **ADR 022 — L2 Slab Format and Footer-Indexed Range Reads.** Freezes the slab's on-object byte layout (magic, header, encrypted chunks, offset index, footer, tail anchor) and the canonical `ObjectStore` trait shape, replacing any earlier ad-hoc upload/download code path.
- **ADR 023 — L2 Upload Lifecycle.** Specifies the Raft-authoritative lifecycle (`Pending → Uploaded → Committed → Retired → Reclaimed`, with `Aborted → Reclaimed` as the failure path) and the non-authoritative local execution ledger that tracks provider-side transient state (multipart UploadId, parts, temp files). Per-replica L1 purge is a **node-local** decision each replica takes after `Committed`; it is not a replicated lifecycle state. Specifies the multipart trigger (aligned with `slab_max_bytes`) and part sizing (8-16 MiB clamped to provider floor).
- **ADR 024 — L2 Orphan Cleanup and Remote Reclaim.** Specifies the leader-only, four-track reclamation sweeper (aborted-multipart, migration-cleanup, retention/tombstone, full-bucket orphan-GC), the freshness recheck that gates every delete, and the three-level rate budget. Archival-tier reclamation is explicitly deferred.

Sweeper policies 1-4 in `plans/ops/storage_tiering.md` §4 remain the higher-level Sealing / Eviction / Promotion / Compaction framing; the ADRs above pin the physical contracts those policies execute against.