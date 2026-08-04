# ADR 430 - Implementation plan: spill placement / cache control

Pairs with `plans/adr/0430_spill_placement_cache_control.md`. Implement the permanent TPC-H disk
placement and prove q05 first, then add the independent engine environment policy. The cut is a
candidate board move from 19/22 to 20/22; correctness and the live envelope decide the claim.

## Phase 0 - permanent harness wiring and live mechanism proof

1. Centralize the two `toron-tpch` execution-context sites in one helper. It applies task telemetry
   and calls `with_join_spill_store` with `wendelon::storage::BlockingFileOps` and
   `PathBuf::from(CACHE_ROOT).join("join_spill")`.
2. Keep `target/tpch/join_spill` exclusively as runtime scratch and `target/tpch/adr430` exclusively
   as evidence. Remove scratch before each canonical run and verify it is empty after normal
   completion.
3. Build the release harness, record mount/free-space preflight, and run q05 sf=10 persisted strict
   under `MemoryMax=4G`, `MemoryHigh=4G`, `TasksMax=512`, and `RuntimeMaxSec=600s`:
   - `perf-run-one` must complete without OOM and retain its row, wall, spill bytes, fingerprint,
     cgroup result, and `MemoryPeak`;
   - `correctness` must independently reach `pass@compare` under the same envelope.
4. Record the Phase-0 result before adding the engine environment selector. If the mechanism does
   not complete, retain the precise OOM/timeout and scratch evidence; do not raise the cap or weaken
   the envelope.

## Phase 1 - engine environment override

1. Add `exec/join/spill_placement.rs` with `TORON_JOIN_SPILL_DIR` and a production
   `default_join_spill_dir()` that reads it once for each `ExecutionContext` construction.
2. Implement resolution through a private injected lookup. A non-empty `OsString` becomes the
   exact `PathBuf`; empty or absent values return `std::env::temp_dir().join("toron_join_spill")`.
   Do not canonicalize, perform I/O, classify a mount, or mutate environment state in tests.
3. Declare and re-export the helper from `exec/join.rs`. Replace only the existing `exec.rs` default
   expression with `crate::exec::join::default_join_spill_dir()`; the existing builder remains the
   final override and therefore has highest precedence.
4. Add `exec/join/spill_placement.rs` to the ADR 176 new-file allowlist with an ADR 430 comment.

## Tests

- Selector with injected lookup: non-empty environment over legacy, empty as unset, unset as
  legacy, and non-Unicode bytes preserved on Unix.
- Context: `with_join_spill_store` replaces the selected default without process environment
  mutation.
- Harness: both execution paths use the centralized context helper and permanent cache-root spill
  path.
- Focused persisted controls: q03, q10, and q19 remain `pass@compare` on the disk-backed path and
  leave runtime scratch empty after normal completion.
- Existing streaming, join-spill, architecture, Toron, and TPC-H suites remain green.

## Phase 2 - fixed-path receipt and board decision

Run q05 once with `TORON_GROUPED_RESERVOIR_RESIDENCY_PROFILE_PATH` pointing under the ADR 430
evidence directory while spill remains under the permanent scratch root. Retain the terminal
receipt and cgroup postmortem. Require no OOM, shmem near zero, flat anonymous memory, and evidence
consistent with reclaimable disk-backed file cache. Do not require `MemoryPeak` below 4 GiB.

Move the board to 20/22 only when both the bounded performance run and persisted correctness run
are green. Otherwise keep 19/22 and name the residual from the live evidence.

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

## Results document

`plans/impl/0430_spill_placement_cache_control_results.md` records mount/free-space preflight, the
Phase-0 harness-only q05 proof, environment/precedence locks, the fixed-path residency receipt,
q05 performance and correctness outcomes, focused controls, scratch cleanup, full gates, board
state, and the honest crash-residue scope.

## Seal

Plan dual: `plan: adr 430 spill placement cache control`. Implementation seal:
`impl: adr 430 spill placement cache control`. Do not commit `.perf.data`.

