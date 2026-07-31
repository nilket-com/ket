# Brainstorm 014: Algebraic Record Identity and Homomorphic State (ECMH)

## Status: Harvested (2026-04-29) into ADR 046, ADR 047, ADR 048

Brainstorm 014's algebraic-record-identity proposal was harvested
into the project's lifecycle validation pass:

- **ADR 046 — `StateDigest` substrate.** Extracted commutative
  state identity as a separate primitive. Did **not** replace
  row `Identity`. Defined the substrate-only surface: hash-to-
  scalar projection over Ristretto255, domain-separated digest
  domains, batched accumulation, compressed 32-byte output,
  V1 helper for `digest.table.row.visible.v1`.
- **ADR 047 — Row-level tombstones on append-only.** Validated
  visible-domain `subtract_element` against real deletion
  state. First non-speculative consumer of the substrate.
- **ADR 048 — Per-table retention policy.** Validated the same
  algebra against compaction-driven monotone reversal.
  Composes with ADR 047 pair-GC under per-table retention
  floors. Second consumer.

ADR 046's bench results (`plans/impl/046_state_digest_bench_results.md`,
2026-04-28) cleared the cut-2 promotion gate: per-element
Ristretto path 121k–141k/sec/core (above the 45–60k/sec/core
target), projection-plus-batched 5.4M/sec at 1M rows,
compressed-validation worst case 2.936 µs. The cut-3 multi-row
smoke confirmed digest invariance across multi-row repacking.

### What the harvest preserved from 014

- **Hot/cold split.** ADR 046's projection step is exactly the
  hot-path Scalar / cold-path Point split 014 proposed (§5).
  L1 ingestion stays on BLAKE3 scalars; cold-path projection
  to Ristretto points happens at L2-seal-shaped batches.
- **Order-agnostic accumulation.** `StateDigestAccumulator`
  exposes commutative / associative add / subtract.
- **Constant-time deletion algebra.** Visible-domain subtract
  reflects the logical state instantly, independent of physical
  compaction (validated by ADR 047 §10 and ADR 048 §12).
- **Density choice.** Ristretto255 over LtHash, on the bench
  evidence: faster than per-element Ristretto for the add path,
  but 2 KiB accumulator state vs 32-byte compressed digest is
  unacceptable for Wendelon's storage-density posture.

### What 014 did not get (per `no-premature-backward-compat`)

- **No durable placement.** ADR 046 §7 deferred placement until
  a concrete consumer commits one. ADR 047 §10 and ADR 048 §12
  reaffirmed this — visible digest is computable on demand from
  post-compaction slab contents; not persisted to footer,
  pointer, catalog, local index, durability log, or wire frame.
  The in-memory cache exception is invalidated on compaction-
  output replacement.
- **No event-history digest domain in V1.** ADR 046 §4 reserved
  the byte tag for a future audit / anti-entropy consumer; ADR
  048 §12 declined to use it. The reserved tag stays unallocated
  until a concrete consumer satisfies the deferral test
  (concrete reader, authentication boundary, invalidation rule).

### Remaining future-consumer surface

The substrate has reserved capacity that future ADRs may pull
on:

- **Anti-entropy reconciliation** — the marquee 014 use case
  (O(1) cross-replica set comparison). Not currently scheduled;
  ADR 048 deferred "anti-entropy retention coordination" as a
  follow-up. The lifecycle ADRs leave transient visible-digest
  divergence between replicas at different compaction states
  as legitimate; anti-entropy is the natural reconciler.
- **Secondary-index segment digests** (roadmap #8). Index
  segments would carry `StateDigest` from day one — replica
  reconciliation becomes O(1) point comparison.
- **Physical / content row digests after content-hash identity**
  (roadmap #5). The third `RowIdentityPolicy` variant
  (`AppendOnlyContentHash`) would project content-canonical
  bytes into the digest scheme.
- **Audit / event-history digest domain.** Reserved byte tag,
  awaiting a concrete consumer ADR.
- **CDC cursor checkpoint verification** (roadmap #10). CDC
  consumers may verify cursor checkpoints against partition
  `StateDigest` points algebraically.

### What 014 explicitly did not become

Brainstorm 015 (algebraic ring topology over Ristretto255) is
the sister proposal that tried to apply the same "mathematical
supremacy" framing to `NodeId`. The 2026-04-29 council closed
015 with **Outcome B** (opaque `NodeId` sufficient) — the
elegance lives in the data-identity layer; it does not transfer
to node identity. See the status header in
`plans/brainstorms/015_algebraic_ring_topology_prime_group.md`.

### References

- ADR 046 (`plans/adr/046_state_digest_substrate.md`) and
  impl plan + bench results.
- ADR 047 (`plans/adr/047_row_tombstones_append_only.md`).
- ADR 048 (`plans/adr/048_per_table_retention_policy.md`).
- Brainstorm 015 closure (status header in
  `plans/brainstorms/015_algebraic_ring_topology_prime_group.md`).
- `plans/wip/roadmap.md` — forward sequence past the lifecycle
  validation pass.

## The Premise
While Brainstorm 015 explored mapping the *routing topology* (`NodeId`) to an algebraic prime group, this brainstorm applies that same "mathematical supremacy" one layer up: to the **Data Identity** itself.

Currently, Wendelon defines a record's `Identity` (its "True Name") as a 256-bit BLAKE3 hash (see `src/schema/units.rs`). It is a static, inert cryptographic fingerprint.

What if we replace the standard BLAKE3 hash with a **Hash-to-Curve** algorithm, mapping every piece of data to a point on the prime-order group of **Ristretto255** (built over Curve25519)?

By upgrading the 256-bit `Identity` from a static integer into an algebraic generator, we unlock **Homomorphic Hashing** (specifically, Elliptic Curve Multiset Hashing or ECMH). This fundamentally changes how Wendelon can manage state, replication, and anti-entropy.

## 1. O(1) Set Reconciliation (The End of Merkle Trees)
In distributed databases (like Cassandra or Dynamo), comparing two partitions to see if they hold the exact same millions of rows requires building, maintaining, and exchanging massive Merkle Trees. 

*   **Mechanism:** Because Ristretto255 points can be added together algebraically, the "Identity of a Partition" is simply the algebraic sum of the Identities of all records within it: $P_{Partition} = \sum H(record_i)$.
*   **The Advantage:** If a node wants to verify its partition matches a replica, it only needs to send a **single 32-byte point**. If the two sums match, the sets of data are *mathematically guaranteed* to be identical. No trees to traverse, no complex state synchronization. Anti-entropy becomes an $O(1)$ operation.

## 2. Order-Agnostic State Accumulation
Standard hashing is order-dependent (e.g., $Hash(A || B) \neq Hash(B || A)$). This means streaming replication logs must be applied in the exact same order on every replica to arrive at the same checksum.
*   **Mechanism:** Point addition on an elliptic curve is commutative and associative. $Point_A + Point_B$ is exactly the same as $Point_B + Point_A$.
*   **The Advantage:** Replicas can receive writes out of order. As long as they eventually receive the same set of records, their partition sum will mathematically converge to the exact same coordinate. This perfectly aligns with Wendelon's multi-row fragment architecture and out-of-order L2 compaction.

## 3. Constant-Time Deletions and Tombstones
In an immutable log or LSM-tree structure, deletions are usually handled via Tombstones, which complicate state checksums because the data isn't actually "gone" until a compaction phase.
*   **Mechanism:** Point addition on the curve has a perfect inverse: subtraction. If a record is deleted from a partition, the node simply *subtracts* that record's Identity point from the partition's running total.
*   **The Advantage:** The partition's mathematical identity instantly reflects the *logical* state of the data, completely independently of the physical compaction state.

## 4. The Space-Time Tradeoff: LtHash vs. Ristretto255
There are two primary ways to implement homomorphic hashing for anti-entropy, and they present a severe trade-off between CPU speed and storage density:

*   **LtHash (Lattice-based Hash):** Extremely fast (~1,200,000+ accumulations/sec), limited essentially by memory bandwidth. However, its output state is large (e.g., 2KB+). If we could afford a 2KB footer in our L2 Slabs, we could roll our own LtHash using BLAKE3 as the engine. It would be the fastest part of the system.
*   **Ristretto255 (via curve25519-dalek):** Much slower (~45,000 - 60,000 accumulations/sec per core), but produces a **tiny 32-byte point**.

Since Wendelon is intensely focused on high-density storage and uses a strict 256-bit (`u256`) identity model for its Slab footers and anti-entropy metadata, the 2KB overhead of LtHash is unacceptable. **Ristretto255 is the correct, robust "pure Rust" choice** to get a 256-bit prime-order identity, making the Hot/Cold architecture below absolutely necessary to hide the CPU cost.

*(Note: Raw Ed25519 point addition is slightly faster (~65k/sec) but has a cofactor, meaning it is not a prime-order group. A malicious payload can get trapped in a small subgroup, breaking the mathematical guarantees. Ristretto255 mathematically abstracts the cofactor away, making it the only safe choice for state reconciliation.)*

## 5. The Hot/Cold Path Compromise (BLAKE3+)
Because Ristretto255 is computationally heavy, injecting finite field arithmetic into the critical `io_uring` hot path of every data mutation will instantly bottleneck ingestion.

**The Solution: A Two-Tier Identity Architecture**
*   **The Hot Path (The Scalar):** When a millions-of-rows-per-second ingestion pipeline is running, it computes the record's identity exactly as it does today: using the standard 256-bit BLAKE3 hash. We use this 32-byte output to generate a valid **Scalar** for the curve. We simply take the existing 256-bit identity and reduce it directly into the scalar field:

    ```rust
    // Using curve25519-dalek with the standard 32-byte (256-bit) Wendelon Identity
    let scalar = Scalar::from_bytes_mod_order(blake3_256_bit_hash);
    ```

    The microscopic modulo bias of mapping 256 bits into a ~252.5 bit space is completely irrelevant for a database partition checksum. The database's critical latency path is completely shielded from elliptic curve cryptography.
*   **The Cold Path (The Point):** The true power of the curve is only needed when replicas need to compare their state (background/asynchronous). When Wendelon compacts data from the L1 memory arena into immutable L2 Slabs, background worker threads perform the expensive base point multiplications to project the BLAKE3 scalars onto the curve: `Ristretto_Point = Record_Identity_Scalar * Base_Generator_Point(G)`. Then, they add those points together to form the partition's macroscopic checksum.

### Orchestration Handoff
How does the hot path hand off to the cold path?
The L1 memory arena focuses strictly on the fast BLAKE3 scalars. The transition to the expensive ECMH point accumulation happens exactly when the L1 arena reaches its capacity threshold and begins sealing an L2 slab for compaction. 
The background compaction threads scavenge idle CPU cycles to compute the Ristretto255 points for all records in the batch, summing them to produce the single `Slab_Identity_Point` that is permanently written to the L2 slab footer. This ensures that L1 ingestion is never blocked by elliptic curve math, while L2 cold storage perfectly aligns with the homomorphic anti-entropy guarantees.

This maps flawlessly onto Wendelon's existing L1 (hot) / L2 (cold) tiered storage architecture.

## Conclusion
By shifting the 256-bit data identity into a prime algebraic group, Wendelon's state verification changes from a heavy structural algorithm (Merkle Trees) into a fundamental property of physics. The data mathematically "adds up" to its own proof of consistency, all without sacrificing the raw throughput of the ingestion pipeline.