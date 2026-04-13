# ADR 006: Tiered Storage and Lazy Migration

## Status
Accepted

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