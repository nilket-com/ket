# ADR 333 - Results: memory-family scale re-measure

Pairs with `333_memory_family_scale_remeasure.md`.

## Outcome

The release + diagnostic-retention re-measure did **not** produce free wins and did **not** convert the six stale
debug OOM cells into clean `memory_budget_exhausted` rows.

All six cells are **OOM-guard-gap** at `sf=1`: systemd killed the service at the 4G cgroup ceiling before
`toron-tpch` emitted a reachability row. That means ADR 332's diagnostic retention had no row to retain for these
queries. The next arc is therefore budget-guard coverage first; streaming/spill sizing should wait until these
paths can fail cleanly and named inside the engine.

## Probe

Release binary, cgroup capped (`MemoryMax=4G`, `RuntimeMaxSec=300s`), strict reach at `sf=1`:

| query | classification | service result | runtime | CPU | memory peak | row emitted |
| --- | --- | --- | --- | --- | --- | --- |
| q07 | OOM-guard-gap | `oom-kill`, `status=15/TERM` | 4.403s | 4.441s | 4G | no |
| q14 | OOM-guard-gap | `oom-kill`, `status=15/TERM` | 16.320s | 16.377s | 4G | no |
| q17 | OOM-guard-gap | `oom-kill`, `status=15/TERM` | 17.085s | 17.119s | 4G | no |
| q18 | OOM-guard-gap | `oom-kill`, `status=15/TERM` | 7.236s | 7.251s | 4G | no |
| q19 | OOM-guard-gap | `oom-kill`, `status=15/TERM` | 19.189s | 19.155s | 4G | no |
| q21 | OOM-guard-gap | `oom-kill`, `status=15/TERM` | 4.295s | 4.313s | 4G | no |

`target/tpch/reachability.jsonl` was checked after the run. The latest rows remained the ADR 332
q04/q12/q20 `memory_budget_exhausted` rows; these six killed probes appended no rows.

## Split Counts

| bucket | count | queries |
| --- | ---: | --- |
| now-pass | 0 | - |
| clean `memory_budget_exhausted` | 0 | - |
| OOM-guard-gap | 6 | q07, q14, q17, q18, q19, q21 |
| time | 0 | - |
| other row-fail | 0 | - |

## Route

The next fix arc is **budget-guard coverage** for these six query shapes. The engine currently allows at least one
allocation path to reach the process/cgroup ceiling before the reachability harness can receive an
`ExecutionError::BudgetExhausted { dimension: Memory, ... }` row. Until that is fixed, the cost-bound
streaming/spill arc cannot be scoped from a clean in-engine failure.

q04/q12/q20 remain the clean `memory_budget_exhausted` examples from ADR 332. Together the sf=1 memory family now
has two named subfamilies:
- clean in-engine budget rows: q04, q12, q20;
- cgroup OOM before any row: q07, q14, q17, q18, q19, q21.

q17 remains reach-only at scale; its correctness oracle is still fixed to sf=0.02.

## ADR 330 Atlas Update

The ADR 330 atlas cells for q07/q14/q17/q18/q19/q21 at `sf=1` are updated from stale debug
`memory (oom-kill)` to release-measured `OOM-guard-gap` with no emitted row. q04/q12/q20 keep the ADR 332
`memory_budget_exhausted` classification. q02 at sf=10/100 remains out of scope for this ADR.

## Verification

Probe commands run:

```text
scripts/cargo-cgroup build -p toron-tpch --release
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=300s \
  /bin/sh -lc 'target/release/toron-tpch run --query q07 --mode strict --sf 1'
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=300s \
  /bin/sh -lc 'target/release/toron-tpch run --query q14 --mode strict --sf 1'
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=300s \
  /bin/sh -lc 'target/release/toron-tpch run --query q17 --mode strict --sf 1'
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=300s \
  /bin/sh -lc 'target/release/toron-tpch run --query q18 --mode strict --sf 1'
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=300s \
  /bin/sh -lc 'target/release/toron-tpch run --query q19 --mode strict --sf 1'
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=300s \
  /bin/sh -lc 'target/release/toron-tpch run --query q21 --mode strict --sf 1'
```

All six probe commands intentionally exited nonzero because systemd killed them with `result: oom-kill`. That
nonzero exit is the measured artifact for this ADR, not a verification failure.

Seal gate:

```text
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup build -p toron-tpch --release
git diff --check
```

`fmt`, `fmt -- --check`, `check -p toron-tpch --all-targets`, `test -p toron-tpch`, and the release build all
passed. `correctness-matrix` rendered all 22 strict cells as `pass@compare`.

`matrix` rendered q04/q12/q20 strict as `fail(memory_budget_exhausted@execute)`. The six ADR 333 OOM-guard-gap
probes do not render as new matrix rows because the processes were killed before emitting reachability rows; that is
the measured gap. q02 still renders the older later-scale `fail(unrecognized@execute)` row from before diagnostic
retention, already documented by ADR 332 as out-of-scope for this sf=1 memory-family remeasure.
