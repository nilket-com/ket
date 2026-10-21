# ADR 028: Deterministic Simulator and `AsyncFileOps` Abstraction

## Status
Proposed

## Context
Wendelon makes correctness claims that a standard unit-test suite cannot validate. Raft-committed invariants, fsync ordering, crash recovery, WAL replay, multi-node quorum behavior, and per-tenant quota convergence all depend on specific interleavings of concurrent events across processes and storage devices. A cluster-wide test harness that cannot reproduce a failing interleaving from a seed is a test harness that finds bugs once and forgets them.

The earlier `plans/ops/testing.md` position named `MadSim`/`Turmoil` and "chaos engineering" at a high level without pinning: (a) whether one or both harnesses ship; (b) how the storage path is abstracted for deterministic replay; (c) how correctness is verified across generated histories (monotonic counters vs. linearizability analysis); (d) what coverage targets enforce the investment. This ADR closes those gaps.

Forces in play:
- **Process-level chaos and deterministic simulation find different bugs.** Chaos finds real QUIC/TCP/kernel-behavior bugs; the simulator finds race-condition bugs that emerge only at specific interleavings. Neither subsumes the other; shipping one and calling the other "later" loses half the coverage.
- **io_uring is not trivially simulated.** Today's `src/storage/io_uring.rs` is a dedicated OS thread per NVMe device, fed via an MPSC channel from Tokio tasks, that pushes SQEs into a raw `io_uring::IoUring` and reaps CQEs into `oneshot::Sender<IoCompletion>` replies. Those kernel-side semantics (SQE ordering, CQE delivery, `submit_and_wait` blocking) are what no off-the-shelf simulator reproduces. Without a simulation seam, deterministic replay of storage-layer bugs is impossible.
- **MadSim's io_uring story is weak.** `madsim` is a compelling Tokio-native simulator for networked systems, but its storage-side reach does not cover Wendelon's io_uring path. Pairing a turmoil-style network simulator with a **custom storage simulator** is the pragmatic shape.
- **`tokio-uring` is not in the codebase.** Earlier drafts of this ADR framed the production storage path around `tokio-uring`; the accepted architecture (ADR 009) uses the low-level `io-uring` crate behind a dedicated per-device thread with MPSC handoff. The simulator seam is designed against the *actual* submission shape: replace the `IoRingHandle` (or a trait that abstracts over it) rather than replace a Tokio-uring runtime.
- **Correctness checkers have different cost profiles.** Elle (Jepsen's linearizability successor) gives real per-history analysis at non-trivial per-op cost; monotonic-counter checking is cheap and catches a broad class of violations at near-zero cost. Running both on different scopes is strictly better than running one on all scopes.
- **rustfs is already the OpenDAL compatibility test backend (ADR 014).** A mocked `ObjectStore` in the simulator is not a replacement for rustfs in the integration-test harness; they serve different levels of the test pyramid and both have reasons to exist.

## Decision

1. **Dual test harness, both permanent.** Wendelon ships with two correctness harnesses that run concurrently throughout the product's lifetime:
   - **Process-level chaos harness** — real daemons, real QUIC on localhost, controlled network partitions via `nftables`, controlled crashes via `SIGKILL`, real disk with injected faults via a shim layer. Exercises the actual product path. Non-deterministic by design: it catches bugs that depend on real kernel and network behavior.
   - **Deterministic simulator** — single-process scheduler on one thread, logical time, turmoil-style network substitution under `quinn`, custom storage simulator behind `AsyncFileOps`, mocked `ObjectStore` for the L2 path. Every failure prints its seed; every failing seed is reproducible forever.

   Neither is deprecated; neither is "the successor to" the other. They surface different bug classes and the costs are different: chaos is cheap to run and finds real-kernel bugs; simulator is cheap to reproduce and finds interleaving bugs.

2. **`AsyncFileOps` trait extraction, built against the existing `IoRingHandle`.** The production storage path already has a submission seam: `src/storage/io_uring.rs` exposes `IoRingHandle` (cloneable, MPSC-fed) whose `submit(IoOp, fd, AlignedBuf, offset)` method returns a `oneshot::Receiver<IoCompletion>`. One dedicated OS thread per NVMe device owns the `io_uring::IoUring`, drains `IoRequest`s from the channel, pushes SQEs with a monotonic `user_data` cookie, calls `submit_and_wait(1)`, reaps CQEs, and fires the matching oneshot. Today's `IoOp` set is `{Read, Write, Fsync}` over an already-open `RawFd`. That handle is the seam; `AsyncFileOps` is the trait that abstracts over it.

   ```rust
   #[async_trait]
   trait AsyncFileOps: Send + Sync {
       // Data-path ops — production impl submits an IoRequest to IoRingHandle
       // and awaits the oneshot reply. The simulator impl services the same
       // calls against an in-memory model with injectable faults.
       async fn read_at(&self, h: &FileHandle, offset: u64, buf: AlignedBuf) -> Result<(usize, AlignedBuf)>;
       async fn write_at(&self, h: &FileHandle, offset: u64, buf: AlignedBuf) -> Result<(usize, AlignedBuf)>;
       async fn fsync(&self, h: &FileHandle) -> Result<()>;
       async fn fdatasync(&self, h: &FileHandle) -> Result<()>;

       // Namespace-level ops — production impl uses spawn_blocking + std::fs
       // (or nix for fsync_dir) because the current ring only carries
       // Read/Write/Fsync. A future enhancement can route these onto the
       // ring via opcode::OpenAt / Unlink / RenameAt without changing the
       // trait. The simulator impl services them against the in-memory model.
       async fn open(&self, path: &Path, flags: OpenFlags) -> Result<FileHandle>;
       async fn fsync_dir(&self, path: &Path) -> Result<()>;
       async fn rename(&self, from: &Path, to: &Path) -> Result<()>;
       async fn unlink(&self, path: &Path) -> Result<()>;
       async fn fallocate(&self, h: &FileHandle, offset: u64, len: u64) -> Result<()>;
   }
   ```

   The trait **does not hide** io_uring's semantics (page-aligned `AlignedBuf`, completion-driven I/O, buffer-returned-via-reply ownership flow, the fsync-vs-fdatasync distinction from ADR 016, the in-manpage caveat that fsync CQEs are not ordered against in-flight write CQEs). It exposes them through a trait surface so both implementations can satisfy the same contract:

   - **Production `IoUringFileOps`.** Holds an `IoRingHandle` per device (and a device→handle map at higher levels). Data-path calls (`read_at`, `write_at`, `fsync`, `fdatasync`) translate directly into `IoRequest`s over the handle's MPSC channel; the returned `oneshot::Receiver<IoCompletion>` is awaited and its returned `AlignedBuf` is handed back to the caller. Namespace-level calls (`open`, `rename`, `unlink`, `fsync_dir`, `fallocate`) run under `tokio::task::spawn_blocking` against `std::fs` / `nix` today, with a migration path onto the ring (`opcode::OpenAt`, `opcode::RenameAt`, `opcode::UnlinkAt`) as a future optimization that does not change the trait.
   - **Simulator `SimFileOps`.** Services every trait method against an in-memory filesystem model. No kernel thread, no `IoUring`, no MPSC — the simulator's scheduler satisfies calls synchronously in logical time, with injectable faults per §3.

   The refactor's blast radius is the set of callers that today invoke `IoRingHandle::submit` / `submit_fsync` directly, or that reach for raw `std::fs` / `nix` for namespace-level ops. Touched files include `src/storage/core/*` (LocalSlabStore is the primary submit caller today), `src/storage/wal/*`, `src/storage/snapshot/*`, `src/index/redb/*`, and `src/l2/local_stage/*` (approximate list — exact scope determined during the refactor prototype).

3. **Custom storage simulator, not an off-the-shelf library.** The simulator implements `AsyncFileOps` against an in-memory model with these injectable faults:
   - **Slow writes** — configurable latency distribution; fault tokens trigger outliers.
   - **Torn writes** — partial completion at power loss simulation; written bytes are truncated at a random-but-seeded offset.
   - **ENOSPC** — disk-full errors at a configurable probability or at absolute-byte thresholds.
   - **EIO** — scrambled bit-flips on read-after-write; distinct from read-error to exercise the scrubber path.
   - **Reordered completions** — completion events delivered out of submission order.
   - **Crash** — in-memory state snapshotted at a point, resumption from the snapshot with pending I/Os dropped.
   - **Clock skew** — logical clock advance per scheduler step, injectable skew events.

   No off-the-shelf framework matches this surface. `tokio-fs`-based mocks lack completion reordering and torn-write modeling; nothing in the `madsim` / `turmoil` family models the CQE-vs-in-flight-write ordering that the raw `io-uring` submission thread actually exhibits. A purpose-built simulator costs ~3-5k lines and gives every bug a seed-reproducible repro.

4. **turmoil-style network simulation under `quinn`.** The simulator's network layer replaces `quinn`'s I/O driver with a turmoil-style scheduler. Connections between simulated nodes become in-memory channels with injectable latency, loss, reorder, and partition faults. Real `quinn` encode/decode logic runs unchanged; only the transport substrate is simulated.

   `turmoil` is not used as-is — Wendelon's QUIC layer has dependencies that turmoil does not satisfy — but the scheduler shape (one thread, cooperative, deterministic message passing) is copied. Whether this reuses an internal crate or forks turmoil is a scope question tracked in Open Questions.

5. **`ObjectStore` mocking for L2.** The L2 path uses a mock `ObjectStore` trait implementation in the simulator:
   - Configurable latency distributions per operation (put, get, list, delete, multipart).
   - Injectable 5xx errors at operation granularity.
   - Eventual-consistency window modeling: LIST sees PUTs at a configurable delay; DELETEs have configurable propagation.
   - Partial-multipart failures: upload parts 1-3 succeed, part 4 returns 500, configurable retry behavior.

   The mock is not a replacement for **rustfs** (ADR 014) in the integration test harness. rustfs exercises real HTTP and real multipart; the mock exercises specific fault injection against a deterministic substrate. Both exist because they cover different levels of the test pyramid.

6. **Correctness checkers: Elle for linearizability, monotonic counter for breadth.**
   - **Elle** (Jepsen's linearizability successor) is run against a **focused operation set** — read and write against a small keyspace. Elle's per-op cost is non-trivial (linearization analysis is polynomial in history length); scoping narrowly keeps checker cost tractable while getting real per-history linearizability verdicts.
   - **Elle stays out-of-process.** The Rust simulator emits focused history artifacts for external analysis; Elle runs as a separate CI/local step against those artifacts. No Clojure, Java, JNI, or Cargo dependency enters the Wendelon workspace for Elle integration.
   - **Monotonic counter** checker runs against every history generated by the simulator. A monotonic-counter property is cheap to check (one pass through the history), catches a broad class of violations (durability breaks, lost writes, torn-state bugs), and imposes negligible overhead.

   The two are additive: every failure surfaces from one or the other; Elle's output is stronger when it fires, and the monotonic counter's coverage is broader.

7. **Coverage targets enforce the investment.** Specific operation counts per time horizon:
   - **CI per-PR.** 10,000 logical operations per scenario, roughly a dozen scenarios, ~100k-1M ops total. Wall-clock budget: ≤ 10 minutes per PR run.
   - **Nightly.** 1M operations per scenario, random seeds, no fault-budget cap. Wall-clock budget: ~4 hours overnight.
   - **Weekly soak.** 100M operations, random workloads, random fault timings. Wall-clock budget: ~24 hours, scheduled on a dedicated runner.

   Every failure prints its seed. Every failing seed becomes a permanent regression scenario in the simulator's seed registry — the seed is frozen, the scenario's name is the issue number, and the test runs that specific seed on every subsequent CI run until the underlying invariant is re-proven or the seed is explicitly retired.

8. **Chaos harness scope.** The chaos harness exercises the production binary, not the simulator. It runs:
   - 3-node and 5-node real clusters on localhost.
   - Controlled partitions via `nftables` DROP rules.
   - Controlled crashes via `SIGKILL` and `SIGSTOP`/`SIGCONT` for partial-freeze simulation.
   - Disk faults via a thin shim over the block device (cgroup-based bandwidth limits + loop-device-backed fault injection).
   - Real QUIC, real fsync, real kernel behavior.

   Chaos scenarios are scripted in Rust; they use the `wendelon-ctl` CLI for cluster manipulation so the operator surface itself is tested. Each scenario is single-invocation, not continuously running; scenarios live in `tests/chaos/` and run on a dedicated Linux runner in CI (chaos paths require kernel capabilities not present in container CI).

## Rationale

- **Dual harness is not redundancy; it is coverage of distinct bug classes.** The simulator finds interleavings that real scheduling never hits. The chaos harness finds real-kernel behavior the simulator cannot model (io_uring SQE ordering quirks, QUIC congestion-control interactions, kernel page-cache behavior). Both are necessary; shipping one and labeling the other "post-pilot" means half-finished correctness.
- **AsyncFileOps surface is the seam the product needs anyway.** Even without the simulator, the seam is useful: fault injection in tests, metric wrapping for storage latency, a future storage backend (tmpfs, zoned storage, `opcode::OpenAt` migration of namespace ops onto the ring) slotting in without touching call sites. The seam also decouples call sites from the `IoRingHandle` type directly, which makes it straightforward to ship a no-io_uring fallback for non-Linux developer environments. The refactor cost is amortized across these use cases.
- **Custom storage simulator beats every off-the-shelf option for this workload.** The faults Wendelon needs — torn writes, reordered completions, crash-with-pending-I/O — are not in `mockall`-style mocks or in `madsim`'s storage layer. A purpose-built simulator at ~3-5k lines is cheaper than fighting an off-the-shelf framework that does 70% of what we need.
- **Elle + monotonic counter is the right cost/coverage split.** Elle is expensive and narrow; monotonic is cheap and broad. Running only Elle caps the operation count we can verify; running only monotonic misses a class of ordering bugs. Both, scoped correctly, give the best returns.
- **The out-of-process boundary keeps the Rust workspace clean.** Simulator history export is ordinary Rust code and ordinary JSON artifacts. The JVM footprint is confined to an optional external runner step, so the production binary, Rust tests, and Cargo dependency graph stay free of Clojure/Java baggage.
- **Coverage targets by time horizon prevent regressions.** Without a nightly/weekly soak, the simulator becomes dormant — the investment is only realized when it runs on new code. The 10k/1M/100M ladder scales effort to calendar time: quick on PR, deep overnight, exhaustive weekly.
- **Seed-reproducibility turns every failure into a permanent regression.** The practice of freezing failing seeds is the mechanism that accumulates coverage over time. A bug found once and regressed once is a cost; a bug found once and pinned as a seed is an asset.
- **rustfs + mocked ObjectStore both exist because they answer different questions.** rustfs answers "does our OpenDAL remote path drive a real compatibility service correctly?" The mock answers "does our L2 path survive specific fault sequences on a deterministic substrate?" Swapping either for the other loses a property that the other cannot give.

## Consequences (The "Negatives")

- **`AsyncFileOps` refactor is multi-week and touches a lot of files.** Exact cost is in a 2-6 week band depending on how tightly `IoRingHandle::submit` semantics are currently coupled to call sites (and how many sites reach directly for `std::fs` / `nix` for namespace ops). The refactor must land before the simulator can replay storage bugs; until then, storage-bug reproduction remains chaos-harness-only. Prototype before committing is the open-item rule.
- **Dual harness doubles CI runner diversity.** Chaos requires a kernel-capable Linux runner; simulator runs in any Rust environment. CI configuration carries both. Mitigated by the simulator being the per-PR gate (fast, portable) and chaos being nightly/weekly on dedicated runners.
- **Custom storage simulator is ~3-5k lines of Rust to own and maintain.** A real maintenance item. Mitigated by scoping its surface tightly (only what `AsyncFileOps` requires; no general-purpose filesystem semantics) and by the payback on every deterministic-replay hour it saves.
- **Seed registry grows over time.** Every frozen seed is forever. Some seeds will eventually become irrelevant as the underlying invariant drifts; retirement is a manual process. Accepted; the cost is a quarterly sweep.
- **Elle's per-history cost limits scope breadth.** Elle cannot run on 100M-operation histories in tractable time. Mitigated by scoping Elle to small keyspaces and short histories; the monotonic-counter checker covers the 100M-operation soak.
- **Elle analysis needs an external runtime when enabled.** The Rust workspace stays lean, but CI or local analysis machines need a separately provisioned Elle runner/container. This is an explicit runner concern rather than a Cargo concern.
- **Chaos harness requires kernel privileges that CI containers typically do not grant.** The chaos runner is a specific Linux VM, not a generic GitHub Actions container. Real infrastructure cost. Mitigated by chaos being nightly/weekly, not per-PR.
- **Rejected alternatives explicitly:**
  - *Custom deterministic harness built directly around `quinn` internals* — rejected; coupling to `quinn` internals breaks on every `quinn` upgrade.
  - *`madsim` for the full stack* — rejected; storage-side coverage is insufficient for Wendelon's io_uring path.
  - *LocalStack as authoritative compatibility test backend* — rejected at ADR 014 time (license/language mismatch); rustfs retained; mock-in-simulator is additive.
  - *Monotonic-counter-only correctness checking* — rejected; misses ordering bugs that only Elle-style linearizability analysis surfaces.
  - *Linking Elle or other JVM tooling into the Rust workspace* — rejected; it pollutes the Cargo/workspace boundary for a checker that is naturally an out-of-band analysis step.
  - *Single-harness shipping (chaos now, simulator post-pilot, or vice versa)* — rejected; each finds bug classes the other misses, and deferring either extends the window in which those bugs ship.
  - *Continuous simulator runs in CI (rather than per-PR scenario count caps)* — rejected; unbounded CI runtime is not a PR-gate contract.

## Open Questions

All items below are scoped at ADR level; acceptance requires prototype work to close specific ones.

- **`IoRingHandle` refactor cost estimate.** The `AsyncFileOps` extraction is in a 2-6 week band. The exact cost depends on how many call sites hold an `IoRingHandle` directly vs. already flow through a higher-level abstraction, plus how many sites reach for `std::fs` for namespace ops. *Resolves via:* a prototype refactor of one subsystem (proposed: WAL) to the trait, measuring both engineering time and resulting line-count delta. Extrapolate to the full refactor. Commit the full refactor only after the prototype lands.
- **turmoil fork vs. internal scheduler.** Whether the simulator's network scheduler forks `turmoil` (and pays upstream-drift cost) or implements the turmoil-style shape internally (and pays implementation cost). *Resolves via:* a one-afternoon spike against the current `turmoil` crate to measure the gap with Wendelon's `quinn` wiring. Whichever side of the spike's outcome is smaller wins.
- **Chaos runner hosting.** The kernel-capable VM for the chaos runner is an infrastructure commitment (self-hosted CI, or a cloud VM pool). *Resolves via:* an ops-side decision tracked in `plans/ops/testing.md`.
- **Seed registry retention policy.** When a frozen seed's underlying invariant has been re-proven, under what conditions it is retired. *Resolves via:* a short policy note in `plans/ops/testing.md`; default proposal is "retire a seed only after its corresponding issue has been closed for ≥90 days and the test has passed ≥1000 consecutive runs."

## References
- ADR 007: Raft Implementation Strategy (`openraft` — consumes the simulator's network layer)
- ADR 009: io_uring Threading Model (the `IoRingHandle` + dedicated-thread + MPSC + oneshot-reply model this ADR abstracts; `AsyncFileOps` extraction lands against it)
- ADR 014: RustFS as the OpenDAL Compatibility Test Backend (retained alongside mocked `ObjectStore`)
- ADR 015: Segmented WAL and Snapshot Lifecycle (simulator exercises segment rotation, directory fsync, snapshot install)
- ADR 016: Durability Model (simulator exercises fsync and group-commit coalescing)
- ADR 017: Persistent Local Index on redb (simulator exercises WAL-replay-forward recovery)
- ADR 022: L2 Slab Format (planned — simulator's mock `ObjectStore` exercises it)
- `plans/ops/testing.md`
