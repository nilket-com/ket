# W1 — Foundation & Storage Seam

**Effort band:** L (8–16 eng-weeks)
**Predecessors:** none
**Folds:** `plans/020_durability_boundary.md`, `plans/021_wal_recovery.md`

The storage seam, durability model, and simulator-replayable WAL/index that every later workstream depends on. W1 is the longest lead item on the foundation critical path because W4 (crypto), W6 (write path), and W10 (simulator productionization) all consume its abstractions.

## 1. ADRs consumed

- **ADR 008 (Dependency Selection).** Pin the crate set W1 lands: `redb`, `io-uring`, `blake3`, `aws-lc-rs`, `openraft`, `quinn`. No `tokio-uring`.
- **ADR 009 (io_uring Threading Model).** Keep the existing `IoRingHandle` / dedicated-thread / MPSC / `oneshot::Sender<IoCompletion>` architecture. W1 productionizes it and adds the `AsyncFileOps` trait on top.
- **ADR 015 (Segmented WAL and Snapshot Lifecycle).** Segment rotation, directory fsync, snapshot install, per-segment encryption-key rotation contract.
- **ADR 016 (Durability Model — Explicit fsync Group Commit).** `fdatasync` at Raft-ack boundaries; `fsync` on metadata paths; bounded coalescing window.
- **ADR 017 (Persistent Local Index on redb).** All node-local persistent indexes live in redb tables; WAL-replay-forward rebuild on restart is authoritative.
- **ADR 028 (foundation only).** Extract `AsyncFileOps` trait with data-path ops (translate to `IoRequest`) and namespace ops (`spawn_blocking` + `std::fs`/`nix`). Full simulator productionization is W10's responsibility.
- **ADR 029 (Identity vs Locality).** W1 is where `Identity` (`[u8; 32]`, BLAKE3) and `Locality` (`u64`, first-8-bytes projection) land in the persistent surface. Every WAL record, redb table key, and slab header that previously used a `u64` key-hash is reshaped here; downstream workstreams inherit the shape.

## 2. Deliverables

1. **`AsyncFileOps` trait landed** with a production implementation over `IoRingHandle` and a `std`/`nix` implementation for non-data-path uses. Every disk-touching call site outside `src/storage/io_uring.rs` calls the trait, not `tokio::fs` / `std::fs` directly.
2. **WAL segment lifecycle.** Segment rotation, sealed-segment fsync, directory fsync on rotation, truncation on compaction, and crash-recovery replay up to last valid CRC-sealed boundary. All paths go through `AsyncFileOps`.
3. **Durability contract implementation.** Raft-log-write → `fdatasync` → ack boundary is enforced in code; group-commit coalescing window is configurable with a documented upper bound; `LEADER`/`NONE` durability classes are distinguishable in the WAL header.
4. **redb local index.** Replaces ad-hoc on-disk indexes; schemas are declared in a single module; all write transactions go through a single commit path for auditability.
5. **Dependency pin.** Cargo.lock reflects the ADR 008 set; `cargo deny` (or equivalent) gates new transitive additions.
6. **Crash replay test matrix** — torn tail, torn mid-segment, missing directory fsync, partial commit — all reproducible via `AsyncFileOps` fault injection.

## 3. Acceptance criteria (ADR-referential)

- (ADR 015) Snapshot install for state machines up to 10 GiB does not OOM the node — streaming install with bounded memory, confirmed by an integration test.
- (ADR 015) Compaction boundary aligns with per-segment DEK rotation; no re-keying occurs across segment boundaries mid-compaction.
- (ADR 016) Every Raft log write that produces an ack has its containing WAL segment `fdatasync`ed before ack is sent. Property test enforces ack ≥ fdatasync-return in every trace.
- (ADR 016) `fsync` is called on segment creation, rotation, truncation — verified by a `strace`-driven test in CI.
- (ADR 017) redb table schemas are stable across one rolling upgrade (forward/backward compatible reads); schema-change procedure documented in `plans/ops/upgrades.md`.
- (ADR 017) Cold start from crash reconstructs all redb state from WAL replay-forward within the recovery budget in `plans/ops/durability.md`; no index state is taken from disk without WAL verification.
- (ADR 028 §2) Every write and fsync path at `src/storage/*` goes through `AsyncFileOps`. `grep` sweep for `tokio::fs::` / `std::fs::write` outside the trait impl returns only test fixtures and the trait impls themselves.

## 4. Explicit non-goals

- Simulator productionization (turmoil-style network, determinism checkers). **W10.**
- Mocked `ObjectStore` for L2 fault injection. **W10.**
- WAL encryption at rest (the per-segment DEK consumer). **W4.**
- Tenant-aware durability accounting (per-tenant byte counters). **W5.**

## 5. Open items

- **ADR 015 Open Question — compression ratio on real state-machine data.** Run the dictionary-trained zstd benchmark on representative snapshots at levels 3 and 9; if improvement is below 2×, revisit the lz4-rejection rationale. Close during W1 phasing.
- **ADR 016 Open Question — group-commit window upper bound.** Land a default and document the latency/throughput knee measured on the pilot NVMe config.

## 6. Test harness tier

- **Unit:** per-function WAL framing, CRC, segment lifecycle state machine.
- **Property:** ack-implies-fdatasync invariant; recovery-after-crash-is-replay-forward-of-valid-prefix.
- **Integration:** full crash-recovery matrix against `rustfs` and real NVMe.
- **Simulator:** once W10 lands, W1's invariants re-run under turmoil + mocked `AsyncFileOps` faults.

## 7. Phasing (internal)

- **W1.a (S):** `AsyncFileOps` trait extraction + call-site sweep.
- **W1.b (M):** WAL segment lifecycle + durability contract + redb index.
- **W1.c (S):** crash-recovery matrix + dependency pin + acceptance bar.

## 8. Identity/Locality impact (ADR 029 addendum)

W1 owns the primary Identity/Locality surface; every other workstream inherits from W1's shape.

- **Schema.** `SlabRecordHeader.key_hash: u64` → `SlabRecordHeader.key_identity: Identity` (32 bytes). `RecordLocation.key_hash: u64` → `RecordLocation.key_identity: Identity`. The routing-relevant projection, where stored alongside, is `Locality`, never a raw `u64`.
- **Indices.** `DashMap<u64, Vec<RecordLocation>>` → `DashMap<Identity, Vec<RecordLocation>>`. redb persistent-index table keys are `Identity` bytes, not `u64`. Any secondary index keyed on ring position is `Locality`.
- **Hash helper.** `fn highway_hash(&[u8]) -> u64` is retired. Replaced by `fn blake3_identity(&[u8]) -> Identity`. Call sites at `src/network/data_server.rs`, `src/client/core.rs`, and the test fixtures migrate in lock-step. The `highway` crate drops from the ADR 008 dependency pin; `blake3` is added.
- **Read-path verification.** Invariant I-2: neither `WendelonDataServer::read` nor `client::core::read` returns a record without byte-exact `Identity` match against the request. An integration test asserts that a forged/mismatched Identity read returns a structured error rather than silently returning data.
- **Crash replay matrix.** Torn-tail, torn-mid-segment, and partial-commit scenarios all target 32-byte Identity boundaries; the matrix expands to cover partial-Identity truncation explicitly (not just partial-value).
- **Dependency pin (ADR 008).** Added: `blake3`. Removed: `highway`.
- **Acceptance addition.** A `cargo-deny` / grep sweep asserts no remaining `highway_hash` symbol outside archived history, and no bare `u64` key-hash field survives in any persistent struct.
