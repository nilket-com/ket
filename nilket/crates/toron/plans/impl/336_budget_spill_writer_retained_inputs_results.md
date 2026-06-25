# ADR 336 Results - budget spill-writer retained inputs

## Outcome

ADR 336 closes the last sf=1 silent memory OOM. q18 now emits
`fail(memory_budget_exhausted@execute)` under the 4G cgroup probe instead of being killed by the cgroup.

The cut remains observability-only. It does not make q18 or the memory family pass; it makes the wall in-engine,
named, and byte-reporting.

## Phase 0 Measurement

Temporary `TORON_ADR336_TRACE` spill-entry logging was added to `execute_spilled_hash_join`, run in the release
binary under the 4G cgroup cap, then removed before seal. The trace printed one compact line per spill entry:
input batch bytes, fallback build-map estimate, total retained spill-entry bytes, and the raw task memory cap.

Release probe:

```sh
scripts/cargo-cgroup build -p toron-tpch --release
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=900s \
  /bin/sh -lc 'for q in q01 q03 q05 q06 q10 q13 q16 q22 q18; do TORON_ADR336_TRACE=1 target/release/toron-tpch run --query "$q" --mode strict --sf 1 || true; done'
```

Measured spill-completers and q18:

| query | outcome | left bytes | right bytes | build-map bytes | retained footprint |
| --- | --- | ---: | ---: | ---: | ---: |
| q05 | green spill-completer | 396,722,952 | 1,686,250 | 42,356,320 | 440,765,522 |
| q16 | green spill-completer | 28,800,384 | 22,985,950 | 21,080,064 | 72,866,398 |
| q18 | oom-kill before ADR 336 | 452,262,956 | 805,868,269 | 165,440,512 | 1,423,571,737 |

q01/q03/q06/q10/q13/q22 emitted no spill-entry trace in the Phase 0 run.

The chosen cap is `task_budget.memory * 16`, which is 1,073,741,824 bytes under the default 64 MiB task memory cap.
This is not reused blindly from ADR 335: it is above the largest measured green spill-completer (q05,
440,765,522) and below q18 (1,423,571,737).

## Implementation

The guard lives in `crates/toron/src/exec.rs`, in `execute_spilled_hash_join`, after the fallback
`HashMap<Vec<u8>, Vec<u32>>` build map exists and before `SpilledJoinBuild::write(...)`.

It estimates the retained spill-entry footprint with saturating arithmetic:

- exact `RecordBatch::get_array_memory_size()` for the left concat'd input,
- exact `RecordBatch::get_array_memory_size()` for the right concat'd input,
- fallback build-map resident estimate: hash table capacity overhead, key payload capacity, and row-index vector
  capacity.

Both concat'd input batches are counted for every spilled hash join kind. This is retained-input accounting, not ADR
335 output-side accounting.

If the retained footprint exceeds the effective cap, the executor returns
`BudgetExhausted { dimension: Memory, requested, available }` before the spill writer starts page/index construction.
The existing `SpilledJoinBuild::write_inner` resident check remains in place.

## Target Probe

Final release binary, `MemoryMax=4G`, `RuntimeMaxSec=600s`, strict `sf=1`:

```sh
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=600s \
  /bin/sh -lc 'target/release/toron-tpch run --query q18 --mode strict --sf 1' || true
```

Result:

| query | result | requested | available |
| --- | --- | ---: | ---: |
| q18 | `fail(memory_budget_exhausted@execute)` | 1,423,571,737 | 1,073,741,824 |

The service exited with the query's nonzero status, not `oom-kill`; the process emitted a reachability row first.

## Green Controls

Final release binary, `MemoryMax=4G`, strict `sf=1`:

```sh
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=900s \
  /bin/sh -lc 'for q in q01 q03 q05 q06 q10 q13 q16 q22; do target/release/toron-tpch run --query "$q" --mode strict --sf 1 || true; done'
```

All green controls remained `pass@execute`, including the measured spill-completers q05 and q16.

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

## Memory-Family Probe

Final release binary, `MemoryMax=4G`, strict `sf=1`:

| query | result | requested | available | producer |
| --- | --- | ---: | ---: | --- |
| q04 | `fail(memory_budget_exhausted@execute)` | 121,631,060 | 67,108,864 | existing spill-writer resident check |
| q07 | `fail(memory_budget_exhausted@execute)` | 1,975,900,039 | 1,073,741,824 | ADR 335 output guard |
| q12 | `fail(memory_budget_exhausted@execute)` | 1,136,180,270 | 1,073,741,824 | ADR 336 retained-spill guard |
| q14 | `fail(memory_budget_exhausted@execute)` | 1,651,834,429 | 1,073,741,824 | ADR 335 output guard |
| q17 | `fail(memory_budget_exhausted@execute)` | 1,651,834,429 | 1,073,741,824 | ADR 335 output guard |
| q18 | `fail(memory_budget_exhausted@execute)` | 1,423,571,737 | 1,073,741,824 | ADR 336 retained-spill guard |
| q19 | `fail(memory_budget_exhausted@execute)` | 1,651,834,429 | 1,073,741,824 | ADR 335 output guard |
| q20 | `fail(memory_budget_exhausted@execute)` | 72,131,060 | 67,108,864 | existing spill-writer resident check |
| q21 | `fail(memory_budget_exhausted@execute)` | 1,975,900,039 | 1,073,741,824 | ADR 335 output guard |

q12 now fails earlier through the retained-spill guard. That is an honest reclassification within the same named
memory family, not a pass claim.

## Tests

Focused executor tests cover the new guard:

- `spill_retained_footprint_budget_guard_fires_before_write`
- `spill_retained_footprint_budget_guard_allows_fitting_entry`
- `spill_retained_footprint_counts_build_map_term`
- `spill_retained_footprint_counts_both_input_batches`

These pin pre-write failure, fitting retained footprints, the fallback build-map term, and the rule that both concat'd
inputs count regardless of output-preserved side.

## ADR 330 Atlas Update

The sf=1 memory-family atlas now has zero silent OOM cells:

- q04/q07/q12/q14/q17/q18/q19/q20/q21: `memory_budget_exhausted@execute`
- q01/q03/q05/q06/q10/q13/q16/q22: green controls remain `pass@execute`

## Next Wall

ADR 336 completes the observability half of the memory arc. The remaining scale wall is fit, not classification:
the materialize-each-operator executor still retains full inputs and outputs. A streaming/pipelined executor is the
substantive arc for turning these named memory failures into passing sf=1+ cells.
