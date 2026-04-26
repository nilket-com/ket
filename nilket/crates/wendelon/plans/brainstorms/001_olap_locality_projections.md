# Brainstorm 001: Context-Aware Locality & OLAP Optimization

## The Vision
To "beat the brakes off BigQuery," Wendelon must evolve from a simple load-balanced distribution to a **Clustered Distribution** model. This is achieved by decoupling the record's **Identity** (Static/Entropy) from its **Locality** (Dynamic/Spatial).

## The Anchor: Identity (256-bit BLAKE3)
The **Identity** is the record's "True Name." It is computed once and never changes. 
- It provides **Universal Integrity**.
- It ensures that even if two different records project to the exact same Ring coordinate, they are never conflated in storage.
- It enables **Idempotent Ingest**: re-inserting the same data results in the same Identity, allowing the storage engine to deduplicate at the speed of light.

## The Magic: Context-Aware Locality
The **Locality** is the record's "Address" on the Unified Ring. Currently, it is a random projection of the Identity, which provides perfect load balancing. To optimize for OLAP, we can transition to a **Spatial Projection**.

### Evolution of the Projection

```rust
// V1: Random / Load-Balanced (The Foundation)
// Provides uniform distribution across every node in the ring.
let locality = Locality::from(identity); 

// V2: Clustered / OLAP-Optimized (The BigQuery-Killer)
// Moves related data (e.g., same Tenant, same Time) into the same 
// arc of the Ring, enabling massive sequential NVMe throughput.
let locality = Locality::from_z_order(tenant_id, timestamp, dimension_x);
```

## Why this wins at OLAP (BI)

1. **Massive Sequential Reads:** A compute layer asking for a "Time Range" or a "Tenant" maps to a single contiguous range on the Ring. Instead of talking to 1,000 nodes for 1KB each (IOPS bottleneck), it talks to 3 nodes for 1GB each (Throughput champion).
2. **Zero-Copy Active Storage:** Because the records are clustered, the **PBC (Pushdown Bytecode)** can operate on large contiguous Arrow batches in-place, filtering and aggregating before a single byte leaves the node.
3. **Scale-Invariant Skipping:** Combined with **BF-Trees** (Bloom Filter B-Trees) at the Slab level, we can skip entire petabytes of data with a few RAM-local lookups.

## The "One Ring" Invariant
Regardless of how we compute the Locality (Random or Z-Ordered), the **Unified Ring** remains the same architecture. We aren't building a new system; we are simply tuning the "Lens" (the projection function) to bring the data into focus for specific workloads.

## The Physics of Hotspots
Spatial clustering introduces the risk of "Hotspots" (one node getting all the data for a massive tenant). 
- **Mitigation:** Salted Z-ordering. We mix a small amount of entropy from the `Identity` back into the `Locality` to ensure a "clump" of data is spread across a tunable number of nodes, balancing Sequential Throughput vs. Cluster Utilization.
  - **Crucial Detail:** Deriving this salt shouldn't be a simple modulo (`identity_hash % M`) where `M` is the spread factor. If `M` changes (e.g. scaling a hot tenant from 4 clumps to 8), a modulo operation scrambles the mapping for almost all records, triggering a massive data migration storm.
  - **Instead:** We should use **Consistent Hashing** (like Jump Consistent Hash) to pick the salt bucket `[0, M)`. This ensures that if we scale `M` from 4 to 5, only 1/5th of the data moves to the new bucket, leaving the rest undisturbed. It turns the "Spread Factor" into an elastic knob we can dynamically tune without destroying locality.
