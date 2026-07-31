# Testing & Resilience

Wendelon makes correctness claims — Raft-committed invariants, fsync ordering, crash recovery, multi-node quorum behavior, per-tenant quota convergence — that a standard unit-test suite cannot validate. The test strategy operationalizes ADR 028: two permanent harnesses (chaos and deterministic simulator) plus a supporting pyramid of unit, property, and integration tests.

Two rules orient everything below:
1. **Neither harness deprecates the other.** Chaos finds real-kernel bugs; the simulator finds interleaving bugs. They target disjoint bug classes; shipping one and labeling the other "later" accepts half-finished correctness.
2. **Every failing run prints its seed.** Every frozen seed becomes a permanent regression scenario — the seed is the asset. A bug found once and regressed once is a cost; a bug found once and pinned as a seed is an asset that accumulates forever.

---

## 1. Test pyramid

| Layer | Purpose | Cadence | Runner |
|-------|---------|---------|--------|
| Unit + property | Module-local correctness, invariants on small state | Every commit locally; every PR in CI | any Rust env |
| Integration (rustfs, redb, local loop) | Cross-module wiring on real dependencies | Every PR in CI | Linux container |
| Deterministic simulator | Cluster-wide interleaving bugs, reproducible from seed | Every PR (scenario-capped); nightly (1M ops); weekly (100M ops) | any Rust env |
| Chaos harness | Real-kernel, real-QUIC, real-fsync behavior under fault | Nightly (scripted scenarios); weekly (long soak) | kernel-capable Linux runner |
| Fuzzing | Parser and state-machine hardening | Per-PR short fuzz; weekly long fuzz corpus | any Rust env |

Bottom two layers catch the bulk of "did we write the code correctly" bugs. Middle two layers catch the "does the cluster preserve its invariants under fault" bugs. Fuzzing catches the "does the parser survive adversarial input" bugs. All five layers are part of the committed test surface; none is optional.

---

## 2. Unit and property tests

- **Unit tests** live alongside their modules and cover pure-function correctness, single-module state machines, and error-path coverage. `cargo test` on every commit; PR gate.
- **Property tests** (via `proptest`) cover invariants that are hard to enumerate: nonce injectivity (ADR 019 acceptance bar), canonical-CBOR round-trip stability (ADR 025 §1), WAL replay idempotence (ADR 017), chunk-index monotonicity. Property tests run per-PR with a bounded shrink budget; weekly CI runs a larger shrink budget.
- Property-test cases that fail are pinned as **regression fixtures** (smallest shrunk input checked into the module's test data). The shrinker is a tool; the fixture is the asset.

No golden-file snapshot tests for dynamic state — they are brittle and hide real regressions behind cosmetic diff churn. Golden fixtures are permitted only for stable on-disk formats (slab footer, audit canonical CBOR).

---

## 3. Integration tests

Integration tests exercise cross-module wiring on real dependencies that do not require a kernel-capable runner:

- **rustfs (ADR 014)** — one OpenDAL compatibility backend used in current pilot coverage. Drives real HTTP, real multipart, real ranged GETs. The production abstraction remains provider-neutral and is expected to stay portable across additional providers.
- **redb** — the real persistent-index backing (ADR 017). Tests exercise WAL-replay-forward against real files.
- **Localhost loop for QUIC framing** — real `quinn` connection pairs within a single process. Useful for RPC-framing tests that don't need a network simulator.
- **Real `io_uring` on the CI runner** — the production `IoRingHandle` is exercised by integration tests that run on Linux runners. Non-Linux developer environments run the `SimFileOps` implementation (see §4.1).

These tests are **fast** (seconds per test) and run on every PR. They do not replace the deterministic simulator; they complement it by validating real dependencies against expected contracts.

---

## 4. Deterministic simulator

The simulator (ADR 028 §2–6) is a single-process, single-threaded scheduler with logical time. Failures reproduce from a seed forever.

### 4.1 `AsyncFileOps` seam

The simulator is possible because every storage-path call site goes through the `AsyncFileOps` trait (ADR 028 §2), not directly through `IoRingHandle::submit`. Two production-path implementations:

- **`IoUringFileOps`** — the real binary's storage backend. Data-path ops (`read_at` / `write_at` / `fsync` / `fdatasync`) translate into `IoRequest`s over the per-device `IoRingHandle` MPSC channel; buffer ownership returns via the oneshot reply. Namespace ops (`open` / `rename` / `unlink` / `fsync_dir` / `fallocate`) run under `tokio::task::spawn_blocking` against `std::fs` / `nix` today.
- **`SimFileOps`** — the simulator's in-memory filesystem model. Services every trait method synchronously in logical time, with injectable faults per §4.2.

Call sites that still reach for `IoRingHandle::submit` or raw `std::fs` directly are a refactor backlog; landing the full `AsyncFileOps` extraction is the prerequisite for simulator-replayability of storage-layer bugs. Until then, storage-bug reproduction is chaos-harness-only for those call sites.

### 4.2 Injectable faults

The storage simulator exposes these fault knobs (ADR 028 §3). All are seeded and reproducible:

| Fault | Shape | What it catches |
|-------|-------|-----------------|
| Slow writes | Configurable latency distribution; fault tokens trigger outliers | Tail-latency-dependent correctness bugs (group-commit coalescing, fsync backoff) |
| Torn writes | Partial completion at power-loss simulation; written bytes truncated at random-but-seeded offset | WAL replay, checksum validation, partial-slab handling |
| ENOSPC | Disk-full errors at configurable probability or absolute-byte thresholds | Quota-path correctness, graceful degradation |
| EIO | Scrambled bit-flips on read-after-write | Scrubber path, AEAD authentication failure handling |
| Reordered completions | CQEs delivered out of submission order | Ordering assumptions in write+fsync barriers (ADR 016) |
| Crash | In-memory state snapshotted at a point; resume from snapshot with pending I/Os dropped | Crash-recovery invariants, WAL replay idempotence |
| Clock skew | Logical clock advance per scheduler step, injectable skew events | HLC causality, `E_safe` staleness rejection |

### 4.3 Network simulation

A turmoil-style scheduler replaces `quinn`'s I/O driver with in-memory channels. Real `quinn` encode/decode runs unchanged; only the transport substrate is simulated. Injectable faults: latency distribution, loss rate, reorder, partition (asymmetric and symmetric), connection-reset.

Whether to fork `turmoil` or implement the scheduler internally is tracked in ADR 028 Open Questions — resolves via a short spike against the current `turmoil` crate's gap with Wendelon's `quinn` wiring.

### 4.4 L2 / ObjectStore mock

A mocked `ObjectStore` implementation services L2 operations in the simulator:

- Configurable latency distributions per operation (`put` / `get` / `list` / `delete` / multipart).
- Injectable 5xx errors per operation.
- Eventual-consistency window modeling: `LIST` sees `PUT`s at configurable delay; `DELETE`s have configurable propagation.
- Partial-multipart failures: parts 1-3 succeed, part 4 returns 500, configurable retry behavior.

The mock is **additive** to rustfs (ADR 014); it does not replace rustfs in the integration harness. rustfs answers "does the OpenDAL remote path drive a real compatibility service correctly?"; the mock answers "does the L2 path survive specific fault sequences on a deterministic substrate?"

### 4.5 Correctness checkers

Every generated history runs under two checkers (ADR 028 §6):

- **Monotonic-counter checker** — runs against every history. Cheap (one pass), catches durability breaks, lost writes, torn-state bugs. Always on.
- **Elle (Jepsen's linearizability successor)** — runs against a **focused** operation set: read/write against a small keyspace. Per-history cost is non-trivial (polynomial in history length); scoped to short histories. The Rust simulator emits history artifacts; Elle runs out-of-process against those artifacts rather than as a Cargo dependency.

The two are additive. Monotonic gives breadth; Elle gives per-history linearizability verdicts when it fires. Running only one loses a bug class the other catches. The current Rust-side bounded register checker is the default CI gate over those focused histories; Elle remains the stronger out-of-process follow-on when a runner is provisioned for a lane.

---

## 5. Chaos harness

The chaos harness exercises the **production binary**, not the simulator (ADR 028 §8). It catches bugs the simulator cannot model: real `io_uring` SQE ordering quirks, real QUIC congestion-control interactions, real kernel page-cache behavior.

### 5.1 Scope

- 3-node and 5-node real clusters on localhost.
- Controlled network partitions via `nftables` DROP rules.
- Controlled crashes via `SIGKILL` (hard) and `SIGSTOP`/`SIGCONT` (partial-freeze).
- Disk faults via a thin shim over the block device: cgroup-based bandwidth limits + loop-device-backed fault injection for `EIO` / `ENOSPC`.
- Real QUIC, real fsync, real kernel behavior.

### 5.2 Scenario shape

Chaos scenarios are scripted in Rust under `tests/chaos/`. The target end-state uses the `wendelon-ctl` CLI for cluster manipulation so the operator surface is itself tested. The current executable catalog is a pre-kernel smoke slice over the in-process transport harness; each scenario is still single-invocation (set up → drive → assert → tear down), not a continuously running loop.

Current executable smoke catalog:

- **`leader_partition_and_rejoin`** — partition the leader from the majority; verify a new leader elects; rejoin the old leader; verify state convergence and no split-brain commits.
- **`rolling_kill`** — kill each node in sequence, wait for cluster re-stabilization, repeat. Asserts durability across the full membership.
- **`packet_drop_single_stream`** — inject a one-shot simulated transport failure on one replication stream; verify quorum progress and follower catch-up on the next control write. This is the smoke-slice approximation of the packet-loss bug class, not the deferred kernel-backed `packet_drop` / `packet_reorder` coverage.

Deferred kernel-backed catalog:

- **`disk_full_on_leader`** — inject `ENOSPC` on the leader's WAL device; verify the leader yields and cluster continues.
- **`clock_skew_on_replica`** — skew one node past the drift guard; verify the node aborts and the cluster stays available.
- **`packet_drop`** — exercise actual kernel-backed loss semantics rather than the current one-shot stream-reset approximation.
- **`packet_reorder`** — exercise true out-of-order delivery rather than the current one-shot stream-drop approximation.

Scenarios are added over time. Every chaos-found bug gets a scenario pinned under its issue number.

### 5.3 Runner hosting

The smoke slice runs on standard CI. Kernel-backed scenarios require capabilities (mount namespaces, `nftables`, cgroup control) that container CI typically does not grant, so the kernel-capable runner stays reserved for the deferred disk-full / clock-skew catalog.

The specific hosting choice is operator-side; ADR 028 Open Questions tracks the kernel-runner procurement decision, not the smoke-slice CI lane.

---

## 6. Coverage ladder

Per ADR 028 §7, operation counts scale with time horizon:

| Tier | Ops per scenario | Scenarios | Total ops (approx) | Wall-clock budget | Runs where |
|------|------------------|-----------|--------------------|--------------------|------------|
| CI per-PR | 10,000 | ~12 | 100k–1M | ≤ 10 min | Every PR |
| Nightly | 1,000,000 | random seeds | up to 100M | ~4 h | Nightly scheduled |
| Weekly soak | 100,000,000 | random workloads, random fault timings | ~100M | ~24 h | Dedicated runner, weekly |

The per-PR tier is a **hard wall-clock gate**: 10 minutes is the contract; growing it is a cross-cutting decision. The nightly and weekly tiers have wall-clock *budgets*, not contracts — exceeding them is a signal to re-scope.

Every tier prints seeds for every failure. Every failing seed becomes a frozen scenario in the seed registry.

Current wiring:

- **Per-PR gate** — `ci/check_simulator_ladder.sh 10000`, run from `.github/workflows/rust-ci.yml`.
- **Chaos smoke gate** — `ci/check_chaos_harness.sh`, run from `.github/workflows/rust-ci.yml` with the harness's pinned default seed.
- **Nightly ladder** — `ci/check_simulator_ladder.sh 1000000`, run from `.github/workflows/simulator-nightly.yml`.
- **Nightly chaos smoke** — `ci/check_chaos_harness.sh`, run from `.github/workflows/simulator-nightly.yml`.
- **Weekly ladder** — `ci/check_simulator_ladder.sh 100000000`, run from `.github/workflows/simulator-weekly.yml`.
- **Kernel-capable manual lane** — `ci/check_chaos_harness.sh`, run from `.github/workflows/chaos-nightly.yml` via `workflow_dispatch` with `WENDELON_CHAOS_REQUIRE=1` until the first kernel-backed chaos scenarios land.
- The ladder script validates the simulator seed registry, replays frozen simulator seeds, then runs the configured mixed-scenario budget with a printed seed.
- When `WENDELON_SIM_HISTORY_DIR` is set, the ladder treats it as a history-artifact root and emits focused register-history artifacts for external Elle consumers beneath it. If `WENDELON_ELLE_RUNNER` is set, `ci/run_elle_handoff.sh` invokes that external runner against the emitted artifacts without adding JVM/Clojure dependencies to Cargo. The scheduled W10 lanes do not provision those env vars yet, so they block on the Rust-side checker and reserve Elle for explicitly configured follow-on lanes.
- The chaos script validates and replays frozen chaos seeds via `tests/chaos.rs`, then runs the rolling-restart and backup/restore scenario tests.

---

## 7. Seed registry

### 7.1 How seeds are captured

A simulator or chaos run that fails prints:
```
FAILED: seed=7f3a2b9c-...  scenario=leader_partition_and_rejoin  iteration=43782
Reproduce: cargo test --test chaos -- --seed 7f3a2b9c...
```

The seed, the scenario name, and the failing assertion go into a new issue. When the bug is fixed, the PR that fixes it includes a new frozen seed in `tests/chaos/seeds/{issue_number}.seed` (or `tests/sim/seeds/{issue_number}.seed` for simulator). The seed's scenario name is the issue number. CI runs every frozen seed on every subsequent run.

Simulator seed files are TOML and currently use this shape:

```toml
issue = 1234
scenario = "coverage_ladder"
seed = 424242
ops = 10000
introduced_at = "2026-04-21"
notes = "Short note describing the regression."
```

The filename stem must match `issue` exactly; malformed seed files fail CI before the ladder runs.

Chaos seed files are also TOML and use this shape:

```toml
issue = 1234
scenario = "leader_partition_and_rejoin"
seed = 424242
introduced_at = "2026-04-21"
notes = "Short note describing the regression."
```

The current chaos catalog is intentionally small: `leader_partition_and_rejoin`, `rolling_kill`, and `packet_drop_single_stream` live in `tests/chaos.rs`, while the rolling-restart and backup/restore scenarios run in their dedicated integration tests and are invoked from `ci/check_chaos_harness.sh`.

### 7.2 Retention policy

Frozen seeds are forever by default, with a managed retirement path:

- **Retire only after all of the following hold:**
  1. The corresponding issue has been closed for ≥ 90 days.
  2. The test has passed ≥ 1000 consecutive runs.
  3. A human (the reviewing engineer on the retirement PR) asserts the underlying invariant is still enforced in the current code.
- Retirement is an explicit PR. It deletes the seed file and adds a one-line entry to `tests/seeds/RETIRED.md` with the issue number, retirement date, and reviewer.
- No automated retirement. The cost of a stale seed is cheap; the cost of retiring a still-meaningful seed is a silently lost regression fixture.

### 7.3 Advisory report

The weekly `seed-retention-advisory.yml` workflow renders a markdown report listing frozen seeds and whether they currently satisfy the retirement bar:

- issue closed for at least 90 days
- at least 1000 consecutive successful relevant workflow runs since the seed was introduced

The workflow uploads the report as an artifact and, when the repository variable `SEED_RETENTION_TRACKING_ISSUE` is configured, refreshes a tracking-issue comment with the same markdown. The report is advisory only; deleting a seed still requires an explicit PR and an entry in `tests/seeds/RETIRED.md`.

---

## 8. Fuzzing

Fuzzing targets parsers and state machines that consume untrusted or adversarial input.

### 8.1 Active fuzz targets

- **`FrameHeader` / metadata zero-copy casts.** Every `bytemuck`/`zerocopy` cast that accepts arbitrary bytes is fuzzed against random byte arrays to confirm no panic, no out-of-bounds, no undefined behavior. This is load-bearing — the QUIC frame path and the slab footer path both rely on these casts.
- **PBC execution.** The pushdown-stack machine is fuzzed against malformed opcodes; invariant: any malformed program returns a typed error, never panics, never runs for unbounded steps.
- **Audit canonical CBOR.** The hashing serializer is fuzzed against random inputs; invariant: round-trip stability (decode(encode(x)) == x) and hash stability across library minor versions.
- **RPC frame decoder.** The outer RPC frame decoder is fuzzed; invariant: no panic on any byte sequence; decode either succeeds, returns a framing error, or signals more-data-needed.

### 8.2 Cadence

- **Per-PR:** short fuzz run (1 minute per target) on changed code.
- **Weekly:** long fuzz corpus run (4 hours per target) on a dedicated runner. Any new crash becomes a regression fixture.

Fuzz corpora are checked into the repo under `fuzz/corpus/{target}/`. New interesting inputs discovered during weekly runs are added to the corpus and carried forward.

---

## 9. Bug-class coverage matrix

Which harness catches what. This table exists so the next "why do we need both?" review has a one-page answer.

| Bug class | Caught by |
|-----------|-----------|
| Logic errors in a single module | Unit + property |
| Cross-module wiring errors against real deps | Integration (rustfs, redb) |
| Raft quorum-stall interleavings | Simulator |
| WAL replay idempotence under torn writes | Simulator |
| Crash-recovery invariants | Simulator |
| HLC / clock-skew causality | Simulator |
| `io_uring` CQE ordering bugs in the kernel | **Chaos only** |
| QUIC congestion-control + partition interactions | **Chaos only** |
| Real-fsync ordering vs. page cache | **Chaos only** |
| `ENOSPC` handling on a real filesystem | Both (simulator for logic; chaos for real-kernel semantics) |
| Parser crashes on adversarial input | Fuzz |
| Lost-write / torn-state violations at scale | Simulator (monotonic checker, 100M-op soak) |
| Linearizability violations on a focused keyspace | Simulator (Elle checker) |

If a bug class has a single cell, losing that harness loses that coverage. That is the argument against "ship one harness first, the other later."

---

## 10. Operational footprint

- **CI runners.** Two classes — container (simulator, unit/integration, fuzz-short) and kernel-capable Linux VM (chaos, weekly fuzz-long). Provisioning the VM class is the one non-trivial ops commitment.
- **Time budget per PR.** ≤ 10 min total: simulator per-PR tier (hard cap), integration tests, unit tests, per-PR fuzz.
- **Time budget per night.** ~4 h: nightly simulator (1M ops, random seeds), nightly chaos scenarios, weekly fuzz partial.
- **Time budget per week.** ~24 h: weekly simulator soak (100M ops), weekly chaos soak, weekly fuzz long corpus.
- **Storage budget.** Seed registry grows unbounded by policy; a quarterly size review is the check. Per-seed size is tiny (seed file + issue link).

---

## 11. What is explicitly not in the test strategy

- **Production traffic replay.** The pilot does not replay production traffic in CI. The chaos harness workloads are synthetic but representative; replay-based testing is a future project with its own ADR.
- **Formal methods (TLA+, Coq).** The simulator is the executable model. A TLA+ model of Raft is a useful reading exercise; it is not a blocking deliverable and does not replace seed-reproducible simulation.
- **LocalStack or a singular S3-mock-as-integration-backend.** Rejected at ADR 014; rustfs is the pilot integration backend, and the simulator's mock `ObjectStore` is additive rather than architectural.
- **"Smoke tests only" on merge-to-main.** Every PR runs the full per-PR tier; `main` is not a relaxed gate.

---

## References

- ADR 028: Deterministic Simulator and `AsyncFileOps` Abstraction (harness architecture)
- ADR 009: io_uring Threading Model (`IoRingHandle` + dedicated-thread model the simulator seam abstracts)
- ADR 014: RustFS as the OpenDAL Compatibility Test Backend (integration-layer compatibility backend)
- ADR 016: Durability Model (fsync semantics the simulator and chaos harnesses validate)
- ADR 017: Persistent Local Index on redb (WAL replay-forward under simulator)
- ADR 027: Observability Posture (metrics that validate SLOs, including `audit_materialization_lag_entries`)
- `plans/ops/observability.md` — SLO targets this harness validates
- `tests/chaos/` and `tests/sim/` — scenario catalogs
- `tests/seeds/` — frozen-seed registry
