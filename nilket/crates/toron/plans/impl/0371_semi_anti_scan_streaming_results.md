# ADR 371 results - semi/anti scan streaming

## Summary

ADR 371 is a measured q04 wall-mover, not a three-cell green. The q04 scan-output accumulator is gone and the cell now
lands on the semi/anti existence-set spill writer. q18 and q22 did not share the q04 miss and remain scan-output
accumulator reds.

The convergence finding is real: q04 now lands on the same producer family as q21's remaining existence-set wall, so the
deferred existence-set spill-volume cut is no longer a q21-only concern. q18 and q22 still need their own scan-output
streaming cuts before they can join that producer table.

## Phase 0 - q04 miss

The q04 pre-fix producer was the persisted Parquet scan output accumulator:

```
BudgetExhausted { dimension: Memory, producer: "scan output accumulator",
  requested: ByteCount(1073762000), available: ByteCount(1073741824) }
```

Temporary tracing showed the materialized scan was `TableId(3)` from the q04 table set, the preserved/orders side:

```
scan start table_id=TableId(3) projection=Some([0,1,2,3,4,5,6,7])
scan materialized rows=8497152 bytes=1073762000 cap=1073741824
```

The miss class was a recognizer miss. The existing semi/anti chain recognizer saw a top `Filter` and rejected it as
`top_not_left_anti`; the existing projected left-semi grouped path expected `Exchange(Join(LeftSemi))`, while q04 is
`Exchange(Filter(Join(LeftSemi)))`.

## Implementation

Added a shape-fenced streaming path for `Exchange(Filter(LeftSemi(preserved Scan, existence))) -> GroupedAggregate`.
The path:

- prepares the existing streamed/spilled semi/anti existence runtime,
- scans the preserved side through the shared `bound_scan_request`,
- applies the scan predicate and post-semi filter batch-by-batch,
- probes the semi runtime,
- repartitions each selected chunk, and
- folds directly into the grouped aggregate state.

The matcher is intentionally filtered-semi only. Unfiltered `Exchange(LeftSemi)` continues to use the older projected
left-semi grouped path, and unrelated uncorrelated EXISTS plans fall back to their previous path.

The semi/anti residual became optional because q04 has no residual match filter; q21-style semi/anti residuals still pass
`Some(filter)` and remain covered by the existing residual tests.

## Producer table

| query | semi/anti kind | scan-output producer before | miss class | producer after | coverage verdict |
| --- | --- | --- | --- | --- | --- |
| q04 | semi (EXISTS) | `scan output accumulator` (`1073762000 / 1073741824` bytes) | recognizer miss: filtered left-semi grouped shape | `semi/anti existence-set spill writer` (`536881184 / 536870912` bytes) | canary covered; wall moved |
| q18 | semi (IN) | `scan output accumulator` (`1073762000 / 1073741824` bytes) | not shared | `scan output accumulator` (`1073762000 / 1073741824` bytes) | not covered; split |
| q22 | anti (NOT EXISTS) | `scan output accumulator` (`1073762000 / 1073741824` bytes) | not shared | `scan output accumulator` (`1073762000 / 1073741824` bytes) | not covered; split |

Final release re-measure commands used the fixed 4 GB systemd cap and persisted data:

```
target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 \
  --query q04 --engines toron --data-source parquet --data-dir target/tpch/data
target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 \
  --query q18 --engines toron --data-source parquet --data-dir target/tpch/data
target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 \
  --query q22 --engines toron --data-source parquet --data-dir target/tpch/data
```

Outputs:

- q04: `status=fail`, `reason=spill_budget_exhausted`, producer `semi/anti existence-set spill writer`, `execute_ms=283567.1`, `peak_rss_kib=489552`.
- q18: `status=fail`, `reason=memory_budget_exhausted`, producer `scan output accumulator`, `execute_ms=1119.7`, `peak_rss_kib=1346556`.
- q22: `status=fail`, `reason=memory_budget_exhausted`, producer `scan output accumulator`, `execute_ms=1407.6`, `peak_rss_kib=1369412`.

No cell greened in this ADR, so no persisted sf=10 correctness row was required. The q04 small-scale correctness oracle
still passes on the new path.

## Verification

```
scripts/cargo-cgroup fmt
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup check -p toron --all-targets
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron scan_filtered_left_semi_grouped_aggregate_bypasses_scan_output_accumulator -- --nocapture
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron projected_left_semi_grouped_aggregate_prunes_unused_preserved_columns -- --nocapture
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron --test group_by_test uncorrelated_exists_passes_all_rows_iff_subquery_nonempty -- --nocapture
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron --test architecture_test architecture::exec_decomposition::adr354_exec_root_is_bounded_after_expr_carve -- --nocapture
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron --no-fail-fast
systemd-run --user --same-dir --wait --collect --quiet --pipe --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 --property=RuntimeMaxSec=300 target/release/toron-tpch correctness --query q04 --sf 0.01 --mode strict
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch --release
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron-tpch
git diff --check
```

Verification results:

- Focused filtered-semi stream regression: pass.
- Existing projected left-semi grouped regression: pass.
- Uncorrelated EXISTS regression: pass.
- Exec root line-count ratchet: pass.
- Full `toron --no-fail-fast`: pass.
- q04 strict correctness at sf=0.01: `pass@compare`.
- Release `toron-tpch` build: pass.
- `toron-tpch` tests: pass.
- `git diff --check`: pass.

Also checked:

- no `ADR371` / `TORON_ADR371` temporary instrumentation remains,
- no qNN references in `crates/toron/src/exec*`,
- no memory or spill cap changed.
