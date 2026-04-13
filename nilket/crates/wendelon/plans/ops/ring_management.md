# Ring Management & Bootstrapping

This document extends the hashing semantics defined in `plans/schema/hashing.md` to cover cluster lifecycle operations.

## 1. Bootstrapping the Ring
- When the first node starts, it initializes a single-node Raft cluster.
- It writes the Genesis Epoch (`E=1`) containing the chosen hashing algorithm, the random ring seed, and its own `NodeId`.

## 2. Immutability & Key Rotation
- The hashing algorithm (e.g., HighwayHash) and the secret ring key are **immutable** for the lifetime of the cluster.
- **Why:** Changing the algorithm or key would instantly invalidate the location of every piece of data on the Data Ring, causing a 100% cache miss and a catastrophic rebalance storm.

## 3. Cluster Migration (Algorithm Swaps)
If an organization MUST change the hashing algorithm (e.g., moving from HighwayHash to BLAKE3 for compliance):
1. A completely new Wendelon cluster must be bootstrapped (`Cluster B`).
2. `Cluster B` is configured as a read-replica of `Cluster A`.
3. Clients dual-write to both clusters.
4. A background job performs a full table scan of `Cluster A` and writes to `Cluster B`.
5. Once caught up, traffic is cut over to `Cluster B` and `Cluster A` is destroyed.

In-place hashing algorithm changes are strictly forbidden by the Control Ring.