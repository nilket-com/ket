# ADR 345 - Implementation plan: spill/stream the semi/anti existence-set build

Pairs with `plans/adr/345_spill_stream_existence_set_build.md`. A `crates/toron` (exec.rs) cut: replace 344's
in-memory existence-set build (`build_in_memory_join_rows`, full existence batch retained) with a **streamed-spilled**
build - existence batches written/indexed one at a time and **dropped**, never `concat_or_empty`'d. Clears q21's
LeftSemi existence-set wall. **Wall-mover-or-green; anti unclaimed until reached.**

## Phase 0 - name the streaming input seam + confirm spill invariants
- **Name the seam (gate 1):** the existence batches are consumed **one at a time**, written/indexed into
  `SpilledJoinBuild`, and **dropped** - identify the exact call site that replaces 344's
  `record_batch_memory_bytes(&existence_batch)` + `build_in_memory_join_rows` (exec.rs:2592/2609) with the streamed
  write. **No `concat_or_empty` on the existence side.**
- Confirm `SpilledJoinBuild`'s write/index path (`SpilledJoinBuild::write/write_inner`, currently exec.rs:7320+)
  accepts batches incrementally and stays valid when source batches are dropped mid-write (page/index invariants).
- Confirm the streamed preserved-side probe can read the spilled build + apply `match_filter` per candidate.

## D1 - the streamed-spilled existence-set build (matched semi/anti shape)
- For the matched LeftSemi/LeftAnti shape, pull existence batches incrementally; for each: write/index into
  `SpilledJoinBuild`, drop the batch. The full existence input is never retained; the build map spills to disk.
- Probe the streamed preserved side against the spilled build; emit LeftSemi preserved-where-match / LeftAnti
  preserved-where-no-match; **apply `match_filter` per candidate** (semi `l2.l_suppkey <> l1.l_suppkey`; anti
  `l3.l_suppkey <> l1.l_suppkey AND l3 late`). Preserved side stays the output (no semantic flip).
- Resident is the 336-budgeted spill state + one input batch + the probe chunk; an over-budget spill resident is the
  named producer (332), not a silent OOM.

## Tests (`toron` lib + `toron-tpch`)
- **No-concat proof (gate 1):** prove the existence path never `concat_or_empty`s / never materializes the full
  existence input: use an instrumented multi-batch existence scan/visitor test that observes one-batch-at-a-time
  consumption, or an architecture guard that pins the matched existence-set path away from `concat_or_empty`. A build
  that exceeds the in-memory cap takes the streamed-spilled path; a producer rename alone is not sufficient.
- **Residual correctness (per candidate):** a synthetic LeftSemi/LeftAnti with a non-equi residual -> the spilled-probe
  result equals the materialized result including the residual (key-match AND `<>`, anti's late conjunct); the ADR
  329-style controls (qualify / NOT-EXISTS-fails / EXISTS-fails) hold. **Not** key-only membership.
- **Spill-resident budget:** an over-budget streamed-spilled existence build emits the named diagnostic, not a silent
  OOM.
- **Shape fence + 343/344 reuse:** non-matching shapes unchanged; the 343 inner-prefix + 344 paths intact.
- sf=0.01 reach/correctness unchanged.

## Phase 3 - re-measure (release, capped, sf=1) - sequential gates
```
scripts/cargo-cgroup build -p toron-tpch --release
# Gate 1: q21 @ sf=1 (4G) - the LeftSemi existence-set wall is GONE (resident drops to the 336-budgeted spill state;
#   332 retention: producer moved off the in-memory existence accumulator).
# Gate 2 (only after gate 1): q21 green, OR a NAMED LeftAnti existence-set wall with measured sf=1 bytes (332).
#   No anti byte table unless the anti boundary is actually reached.
```
- Record gate 1 (semi wall gone) then gate 2 (q21 green or named LeftAnti wall). Update the ADR 330 atlas q21 cell.
- **Green-cell regression:** q01/q03/q05/q06/q10/q13/q16/q22 + q07/q14/q19 stay green, unchanged plans.
- q17/q12/q18/q04/q20 unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01   # residual + order (ADR 329 oracle)
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix     # 22/22 @ sf=0.01 unchanged
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
#   release/cgroup q21 sf=1: gate 1 (semi wall gone) -> gate 2 (green or named LeftAnti wall); green cells unchanged
```

## Results doc
`crates/toron/plans/impl/345_spill_stream_existence_set_build_results.md`: the streaming input seam (no-concat proof);
gate 1 (LeftSemi wall gone, 332 producer move); gate 2 (q21 green or named LeftAnti measured wall - anti only if
reached, no structural anti table); the per-candidate residual-correctness result (329 controls); sf=0.01 q21
pass@compare; the green-cell regression check; the ADR 330 atlas q21 update.

## Risk / blast radius
- **Rename-the-wall (gate 1)** - the no-`concat_or_empty` test is the proof the existence input is streamed/dropped,
  not retained.
- **Anti over-claim (gate 2)** - sequential; no anti bytes/green from structural-only.
- **Residual under spill** - per-candidate `match_filter`, not key-only; the sf=0.01 oracle + residual tests gate it.
- **Spill invariants** - dropping batches mid-write must keep `SpilledJoinBuild` page/index valid (Phase 0).
- **Scope** - matched semi/anti existence-set build only; 336/344/343 reuse; no cost-model change.

## Seal
`impl: adr 345 spill stream existence set build`. Do not commit wip drafts.
