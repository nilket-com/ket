# Epoch Transitions & Lazy Migration

This document rigorously defines the lifecycle of a cluster topology change (an "Epoch Transition") and the subsequent zero-downtime, zero-storm data migration process.

## 1. The Immutable Epoch: `ring(E)`

At any given moment, the cluster operates within a strictly defined, immutable topology known as the **Epoch (`E`)**.
- The Epoch defines the exact membership of the cluster and the specific mathematical parameters of the unified hash ring (e.g., node IDs, virtual node mapping).
- All bulk read/write data routing in the Data Ring is calculated deterministically against this static `ring(E)`.
- Mid-query topology changes do not exist. A query initiated in Epoch `E` expects the routing rules of Epoch `E`.

## 2. Triggering an Epoch Transition

An Epoch Transition (moving from `E` to `E+1`) is a Control Ring operation, triggered by:
1. **Node Join:** A new node is bootstrapped and requests to join the cluster.
2. **Node Leave (Graceful):** A node is gracefully decommissioned by an operator.
3. **Node Death (Ungraceful):** The Control Ring detects a node failure (via missed heartbeats/lease expirations) and evicts it.

## 3. The Transition State Machine

When a trigger occurs, the following strict sequence executes:

1. **Proposal:** The Raft Leader of the Control Ring proposes a `LogEntryKind::CONFIG` entry containing the new cluster state (`E+1`).
2. **Commit:** The Control Ring achieves quorum and commits the configuration change.
3. **Epoch Advancement:** The global Epoch counter increments to `E+1`.
4. **Lease Invalidation (The Hard Boundary):** The absolute most critical step. The instant `E+1` is committed, **ALL existing per-range leases from Epoch `E` are globally invalidated.** 
   - No Data Node may accept a new write or serve a linearizable read based on an `E` lease.
   - In-flight proxied writes tagged with `expected_epoch=E` will be rejected by the new primaries with `EPOCH_STALE`.
5. **Re-election:** Data Nodes observe the new `ring(E+1)`, calculate their new hash range responsibilities, and immediately request new leases from the Control Ring for their newly assigned ranges.

## 4. Lazy Migration (Anti-Bootstrap Storm)

Wendelon strictly forbids "bootstrap storms" (where a new node joins and immediately saturates network/disk I/O by pulling terabytes of data from peers). All data migration is strictly lazy and on-demand.

Assume a hash range `R` was owned by **Node A** in `ring(E)`, but is reassigned to **Node B** in `ring(E+1)`.

### 4.1 On-Demand Reads (Temperature Trigger)
When Node B (the new primary) receives a `READ` request for Key `K` in range `R`:
1. Node B checks its local storage.
2. **Local Miss:** If the data is not local, Node B knows it might still reside on Node A.
3. **Migration Read:** Node B proxies an internal "migration read" to Node A.
4. **Serve & Store:** Node A returns the data. Node B serves the client immediately. Asynchronously, Node B writes this data to its own local storage.
*Result: Hot data naturally and instantly migrates to the new owner without blocking.*

### 4.2 Writes
When Node B receives a `WRITE` request for Key `K` in range `R`:
1. Node B processes the write normally, replicating to its new `E+1` peers.
2. It does *not* need to communicate with Node A. The new write simply shadows any old data residing on Node A, safeguarded by the `E_safe` watermark and HLC.

### 4.3 The Background Sweeper (Cold Data)
To ensure Node A can eventually reclaim disk space, Node B runs a heavily throttled, low-priority background sweeper task.
- The sweeper slowly queries Node A for keys in range `R` that Node B does not yet possess.
- It pulls this "cold" data at a rate specifically tuned to avoid saturating `io_uring` queues or network links.

## 5. Finalization and Deletion

Node A (the old owner) must retain the data for range `R` until it is mathematically proven that Node B has a complete copy.
- Once Node B's background sweeper finishes scanning the entire range `R` on Node A, Node B sends a cryptographic `MigrationComplete` marker to Node A.
- Only upon receiving this marker may Node A physically delete the chunk/Arrow batches for range `R` from its local storage.
