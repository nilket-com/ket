# ADR 017: Persistent Local Index on redb

## Status
Proposed

## Context
Every Wendelon node maintains a persistent local index mapping logical identifiers (slab IDs, WAL offsets, tenant-scoped metadata) to physical locations. This index is correctness-critical — a lost or corrupted entry can orphan storage or misroute reads. It must survive restart, crash, and power loss, and compose correctly with the WAL durability contract (ADR 016) and segment-based WAL rotation (ADR 015).

The design choice is the backing store. Candidates considered:
- **redb** — pure-Rust embedded ACID key-value store; single-file B-tree with copy-on-write; active maintenance.
- **sled** — pure-Rust embedded KV; historically innovative but with an uncertain maintenance profile and a long history of format changes.
- **Custom on-disk format** — hand-rolled bytemuck-style persistent structure aligned with Wendelon's other POD layouts.
- **Periodic checkpoint files** — no continuous durability; index rebuilt from WAL replay on restart.

## Decision

1. **redb is the persistent local index.** All node-local persistent indexes that require point-query access and transactional semantics live in redb tables.

2. **WAL is authoritative; the index is a derived cache. Strict commit ordering + replay-forward recovery.**
   - **Commit ordering.** For every committed batch of state-machine mutations: (a) WAL fsync completes, producing a durable log position `W`; (b) the corresponding redb transaction commits, stamping the index's `last_applied_wal_position = W` inside the same transaction; (c) the RPC ack is sent only after step (b). Never ack before index commit, never commit the index before the WAL is durable.
   - **Recovery rule.** On restart, read `last_applied_wal_position = I` from redb (if any). Replay WAL entries from `I + 1` through the WAL's durable tail, reapplying each mutation to the index. If the crash landed after (a) but before (b), `I` lags the WAL and replay-forward closes the gap; the replay is idempotent by construction because each mutation is keyed by log index.
   - **Why this works.** Two files cannot be made durable atomically without a third coordinator, so we don't pretend. The WAL is the sole authority; the index is a deterministic function of the WAL up to some position. Any surviving divergence between disk state is resolved by WAL replay-forward, which is the same mechanism the state machine already uses.
   - Batching index writes aligned to WAL group-commit boundaries is a *performance* optimization (one redb transaction per batch instead of per entry), not a correctness mechanism. Correctness lives entirely in the commit ordering and replay-forward rule.

3. **Encryption layer sits above redb.** Values written to redb are pre-encrypted by the Wendelon crypto layer (see ADR 020) before being handed to the database. redb sees only ciphertext. Keys are not encrypted (they must be searchable); they contain no sensitive payload by construction.

4. **Full-scan fallback is the corruption-recovery path.** If redb reports corruption or fails to open, the node falls back to a full WAL scan to reconstruct the index. Recovery is slow but correct.

## Rationale

- **"Don't roll your own persistence" applies even to indexes.** The principle isn't just about WAL format — it extends to every correctness-critical file. redb owns the decade of tail bugs around B-tree splits, page allocation, atomic transactions, and crash consistency that we would otherwise reinvent.
- **redb's profile matches the problem.** Small API surface (tables, transactions, range scans), clean ACID semantics, pure-Rust with no FFI to debug, actively maintained, single-file storage that backs up cleanly. No feature we don't need.
- **Sled has a weaker maintenance profile today.** The crate has had multiple format breaks and its maintenance cadence has slowed. For a correctness-critical path, we prefer the more conservative choice.
- **Custom formats are a trap.** The initial version is always smaller and faster than redb. Two years later, it has its own B-tree bugs, its own page allocator, its own transaction semantics, and nobody remembers the invariants. redb's authors have paid for those mistakes already.
- **Periodic checkpoint files lose transactional semantics.** An index is not just a derived view — it's a write target with concurrency. Checkpoints cannot satisfy write-during-read without reinventing transactions on top.
- **"Half WAL, half index" is resolved by replay-forward, not by atomicity.** Two separate files cannot be made atomically durable without a third coordinator. Instead of pretending they can, we make the WAL the sole authority and treat the index as a cache that replays forward to catch up. The failure mode doesn't disappear; it becomes self-correcting.
- **Idempotency-by-log-index is a load-bearing implementation constraint.** The recovery rule is only correct if every state-mutation applied to the index during replay produces the same result as the original application, regardless of how many times it runs. Concretely: every mutation must be keyed by its originating Raft log index, reapplying a mutation at index `N` when the index already shows `last_applied_wal_position ≥ N` must be a no-op, and there must be no index operation whose effect depends on the index's current state beyond what the WAL entry already specifies. This is not an optional discipline — it is the mechanism that makes the recovery rule work. Design review and prototype review must both explicitly verify that every mutation site honors this constraint. Any mutation that cannot be made idempotent-by-log-index is a correctness bug and blocks acceptance.
- **Encryption above redb is the only safe placement.** redb doesn't expose page-level encryption hooks, and building them in would require forking. Ciphertext-in, ciphertext-out is a clean contract that works with any redb version.

## Consequences (The "Negatives")

- **External-crate dependency on a load-bearing path.** redb is critical; if it ships a breaking change or a correctness regression, we feel it. Mitigation: pin the version, review redb's release notes as part of our dependency bump process, and maintain the full-scan fallback as a real (not theoretical) recovery path.
- **redb's on-disk format constrains us.** We cannot change its page layout, its B-tree fanout, or its transaction semantics. If our workload hits a pathological case, we cannot hand-tune it.
- **Encryption-above-redb means no range-scan on encrypted values.** Range scans over values require decrypting each result, which is fine; but deterministic encryption for value-based indexing would require a second index layer. Not a problem in current design; flagged in case it becomes one.
- **Full-scan recovery is real work.** On corruption, recovery replays the full WAL. For a cluster with a large state machine and few snapshots, this is minutes of downtime. Acceptable for a corruption event (rare); not acceptable as a steady-state operation.
- **Rejected alternatives explicitly:**
  - *sled* — maintenance profile.
  - *Custom on-disk format* — owning a decade of tail bugs for no architectural win.
  - *Periodic checkpoint files* — no transactional guarantees; cannot sustain write-during-read.
  - *LMDB or RocksDB via FFI* — adds C/C++ dependency, debugging surface, and build-system complexity that redb avoids.

## References
- ADR 015: Segmented WAL and Snapshot Lifecycle
- ADR 016: Durability Model — Explicit fsync Group Commit
- ADR 020: L2 Envelope Encryption (planned)
