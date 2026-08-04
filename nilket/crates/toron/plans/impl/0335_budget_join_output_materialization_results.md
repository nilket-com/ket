# ADR 335 Results - budget join output materialization

## Outcome

ADR 335 makes the five join-output OOM cells observable. q07/q14/q17/q19/q21 now emit
`fail(memory_budget_exhausted@execute)` rows under the `sf=1`, 4G cgroup probe instead of being killed by the
cgroup.

q18 remains unchanged and still OOM-kills with no row; that is ADR 336's spill-writer/input-retention path.

## Implementation

The cut adds a pre-take guard in `crates/toron/src/exec.rs`:

- `materialize_join_selection` now receives a memory cap from every caller, including `execute_cross_join_match_all`.
- Before building `UInt32Array` take indices or running per-column `arrow::compute::take`, it estimates the output
  materialization bytes from:
  - selected row count,
  - take index arrays for the emitted side(s),
  - actual source column bytes per row multiplied by selected rows.
- Full-width joins count both sides. Left/right semi and anti joins count only the preserved side.
- Saturating arithmetic is used throughout the estimate.

The first literal implementation compared the estimate directly to the 64 MiB task memory budget. That converted the
target OOMs, but it also failed accepted green controls:

| control | raw-cap result | requested |
| --- | --- | ---: |
| q03 | `memory_budget_exhausted` | 484,875,000 |
| q05 | `memory_budget_exhausted` | 484,875,000 |
| q10 | `memory_budget_exhausted` | 484,875,000 |
| q13 | `memory_budget_exhausted` | 495,840,611 |
| q16 | `memory_budget_exhausted` | 134,600,000 |

That contradicted ADR 335's discriminating acceptance. The final guard therefore uses an effective output
materialization cap of `task_budget.memory * 16` (1,073,741,824 bytes under the default 64 MiB task budget). This
keeps the accepted controls green while cleanly catching the observed GiB-scale output materializations:

- q07/q21 requested 1,975,900,039 bytes.
- q14/q17/q19 requested 1,651,834,429 bytes.

The guard is still observability-only. It does not make the cells fit, and it does not claim complete join memory
accounting; the selection vector and materialized inputs already exist before the guard runs.

## Target Probes

Release binary, `MemoryMax=4G`, `RuntimeMaxSec=600s`, strict `sf=1`:

```sh
scripts/cargo-cgroup build -p toron-tpch --release
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=600s \
  /bin/sh -lc 'for q in q07 q14 q17 q19 q21; do target/release/toron-tpch run --query "$q" --mode strict --sf 1 || true; done'
```

| query | result | requested | available |
| --- | --- | ---: | ---: |
| q07 | `fail(memory_budget_exhausted@execute)` | 1,975,900,039 | 1,073,741,824 |
| q14 | `fail(memory_budget_exhausted@execute)` | 1,651,834,429 | 1,073,741,824 |
| q17 | `fail(memory_budget_exhausted@execute)` | 1,651,834,429 | 1,073,741,824 |
| q19 | `fail(memory_budget_exhausted@execute)` | 1,651,834,429 | 1,073,741,824 |
| q21 | `fail(memory_budget_exhausted@execute)` | 1,975,900,039 | 1,073,741,824 |

The loop exited successfully as a shell command because each expected nonzero query was followed by `|| true`.
The important evidence is that each query emitted a reachability row before process exit; none was cgroup-killed.

## Discriminating Controls

The accepted green controls were run under the same release binary and 4G cap:

```sh
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=600s \
  /bin/sh -lc 'for q in q01 q03 q05 q06 q10 q13 q16 q22; do target/release/toron-tpch run --query "$q" --mode strict --sf 1 || true; done'
```

All eight controls passed:

| query | result |
| --- | --- |
| q01 | `pass@execute` |
| q03 | `pass@execute` |
| q05 | `pass@execute` |
| q06 | `pass@execute` |
| q10 | `pass@execute` |
| q13 | `pass@execute` |
| q16 | `pass@execute` |
| q22 | `pass@execute` |

q18 was rerun as the out-of-scope control:

```sh
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=300s \
  /bin/sh -lc 'target/release/toron-tpch run --query q18 --mode strict --sf 1' || true
```

It still ended in `oom-kill`, `status=15/TERM`, with a 4G memory peak and no emitted row. ADR 336 owns that path.

## Tests

Focused executor tests cover the new guard:

- `join_materialization_budget_guard_fires_before_take`
- `join_materialization_budget_guard_allows_fitting_join`
- `join_materialization_budget_guard_counts_only_preserved_semi_side`
- `cross_join_materialization_chunks_use_budget_guard`
- `join_materialization_estimate_saturates_for_huge_selection`

These pin the pre-take failure, green materialization, semi-side accounting, CROSS cap threading, and saturating
arithmetic.

## ADR 330 Atlas Update

The sf=1 memory-family atlas now splits:

- q07/q14/q17/q19/q21: `OOM-guard-gap` -> clean `memory_budget_exhausted@execute`.
- q18: unchanged `OOM-guard-gap`, no row.
- q04/q12/q20: existing clean `memory_budget_exhausted@execute` from ADR 332.

## Next Wall

ADR 335 does not make these five cells pass. The fit wall remains the materialize-each-operator executor model:
parents await full `Vec<RecordBatch>` child outputs, joins hold full inputs, and downstream operators receive full
join output. A streaming/pipelined executor is the scale arc that turns these named failures into passing cells.

ADR 336 remains the narrower q18 spill-writer/input-retention observability cut.
