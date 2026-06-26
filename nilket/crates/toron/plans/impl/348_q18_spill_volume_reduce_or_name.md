# ADR 348 - Implementation plan: q18 spill-volume (reduce or name, no cap-bump)

Pairs with `plans/adr/348_q18_spill_volume_reduce_or_name.md`. A `crates/toron` (exec.rs) cut: measure q18's 537.6 MB
spill by producer/page/column, classify the reduction levers, apply the measured-reducible one(s) under the
**unchanged** 512 MB budget, and accept q18 green or a named irreducible spill-volume wall. **No cap-bump.**

## Phase 0 - measure the spill + classify levers (the go/no-go)
- **Measure** (instrument `SpilledInnerJoinBuild`, env-gated, removed before seal): producer, page count, per-page
  bytes, **columns spilled** (all build columns vs output-needed), rows (pre/post dedup), compression ratio. Total =
  537,631,256 B; budget 536,870,912 B.
- **Classify each lever - measured-reducible / measured-not-enough / not-applicable:**
  1. **Column projection (first):** is the spill writing build columns not needed downstream? Pruning to output-needed
     columns is the clearest local cut.
  2. **Page/layout (second):** page size / encoding / compression overhead reducible?
  3. **Dedup-before-spill (only with a proven mechanism):** NOT free - the LeftSemi preserved rows are written before
     the probe. Classify **not-applicable** unless a two-pass membership filter / side flip / narrowed projection that
     avoids writing pre-matched rows is proven. No assumed local early-dedup.
- Record the per-lever classification before D1.

## D1 - apply the measured-reducible lever(s)
- Apply the lever(s) classified measured-reducible (column projection first; page/layout if it adds enough; a proven
  dedup mechanism only if reducible) so the q18 spill fits under the unchanged 512 MB budget.
- If all levers are not-enough / not-applicable: **do not raise the budget**; the deliverable is the named irreducible
  spill-volume wall + the per-lever evidence.

## Tests (`toron` lib + `toron-tpch`)
- **Reduction correctness:** column projection / any dedup mechanism preserves the answer - a synthetic spilled
  LeftSemi join with pruned spill columns -> the materialized output equals the full-column-spill result; q18
  `pass@compare` at sf=0.01.
- **No cap-bump:** the 512 MB `spill_bytes_reserved_per_query` is unchanged (a test/assert it's not raised).
- **Spill breakdown (if instrumented kept as a clean probe):** the spill writes only output-needed columns after the
  cut (or a recorded reason why not).
- sf=0.01 reach/correctness unchanged.

## Phase 3 - re-measure (release, capped, sf=1)
```
scripts/cargo-cgroup build -p toron-tpch --release
scripts/cargo-cgroup run -p toron-tpch --release -- run --query q18 --mode strict --sf 1
# If q18 greens, run the scale correctness gate:
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q18 --sf 1
```
- q18 green or named irreducible wall via 332; update the ADR 330 atlas q18 cell. If green, run q18 sf=1 correctness.
- **Green-cell regression:** q01/q03/q05/q06/q10/q13/q16/q22 + q07/q12/q14/q17/q19/q21 stay green, unchanged plans.
- q04/q20 unchanged (ADR 349).

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
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q18 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix     # 22/22 unchanged
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
#   release/cgroup q18 sf=1 run above is the scale-wall proof; default matrix is a regression guard.
#   q18 -> green or named irreducible spill-volume wall; budget unchanged; green cells unchanged.
```

## Results doc
`crates/toron/plans/impl/348_q18_spill_volume_reduce_or_name_results.md`: the Phase-0 spill breakdown (producer/page/
column/rows/compression) + the per-lever classification (measured-reducible / not-enough / not-applicable, column
projection first); the applied reduction (or the named irreducible wall); q18 sf=1 before/after (spill bytes via 332);
the reduction-correctness result; sf=0.01 q18 pass@compare; confirmation the 512 MB budget is unchanged; the
green-cell regression check; the ADR 330 atlas q18 update.

## Risk / blast radius
- **No assumed early dedup** - dedup is not-applicable unless a real mechanism is proven (Codex trap).
- **No cap-bump** - reduce or name; the 512 MB budget stays.
- **Reduction preserves correctness** - pruned columns not needed downstream (sf=0.01 oracle gate).
- **Scope** - q18 spill only; q04/q20 index is ADR 349; no cost-model change.

## Seal
`impl: adr 348 q18 spill volume reduce or name`. Do not commit wip drafts.
