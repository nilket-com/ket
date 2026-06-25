# ADR 344 results - q21 semi/anti extension

## Summary

ADR 344 landed as a bounded executor-local extension in `crates/toron/src/exec.rs`.

The implementation reuses the ADR 343 reoriented inner-prefix primitive for the matched inner prefix, then handles the
semi/anti suffix as a shape-named streaming path. The core executor symbols intentionally avoid TPC-H query-number
names; the TPC-H-specific scope is recorded here and in the ADR.

The result is a named wall, not a green claim: q21 now moves off the ADR 341 `streamed exchange output accumulator` and
fails cleanly at the semi/anti existence-set build/spill accumulator.

## Phase 0 grounding

Pre-ADR 344 q21 at `sf=1` under the 4G scale cap:

| query | result | diagnostic producer | requested | available |
| --- | --- | --- | ---: | ---: |
| q21 | `fail(memory_budget_exhausted@execute)` | `streamed exchange output accumulator` | 1,074,142,886 | 1,073,741,824 |

The bound shape still has the same split measured in ADR 342/343:

- the inner prefix is `Inner` / `HashPartitioned` and can reuse the reoriented inner-chain runtime;
- the suffix is `LeftSemi` then `LeftAnti`, both `HashPartitioned`, both with a non-equi `match_filter`;
- the semi existence side is a full lineitem scan at `sf=1` scale, not a small dimension.

Side-proving table:

| join_id | kind | preserved_side | preserved_source | preserved_bytes | existence_side | existence_source | existence_bytes | verdict |
| --- | --- | --- | --- | ---: | --- | --- | ---: | --- |
| 3 | `LeftSemi` | inner-prefix output | structural-only; natural sf=1 reaches the existence wall before streaming preserved rows | n/a | l2 lineitem existence set | sf=1 trace through ADR 344 failure; bound-shape scan exact batch bytes | 1,119,431,072 retained bytes; 1,526,014,069 requested with hash estimate | first named wall; not a build-small reorient |
| 4 | `LeftAnti` | semi output | structural-only; unreached after the join 3 wall | n/a | l3 late lineitem existence set | structural-only; unreached after the join 3 wall | n/a | next probe after the semi existence-set wall; no green claim |

This satisfies the ADR's provenance rule: the measured semi wall justifies a named `memory_budget_exhausted` producer.
The anti side is explicitly not used to justify a green target because it remains past an unreached boundary.

## Implementation

- Added a guarded grouped-aggregate path for the shape:
  - inner prefix stages use the existing reoriented inner-chain runtime;
  - post-inner filters are applied batch-by-batch before the semi/anti suffix;
  - `LeftSemi` / `LeftAnti` stages preserve the left rows and apply the join `match_filter` inside the probe.
- Added a budgeted semi/anti existence-set build:
  - retained existence batch bytes are counted with `record_batch_memory_bytes`;
  - hash-build bytes are added via the existing hash-build estimator;
  - over-cap existence sets emit
    `BudgetExhausted { producer: "semi/anti existence-set build/spill accumulator", ... }`.
- Added a budgeted preserved-side materializer for fitting semi/anti probes:
  - semi emits preserved rows only when a residual-qualified match exists;
  - anti emits preserved rows only when no residual-qualified match exists.

The path deliberately does not implement the next spill/streaming fix for the large existence set. That is the next
named wall.

## Correctness guard

Default-scale strict q21 still passes the ADR 329 oracle:

```text
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01
```

Result:

```text
tpch_correctness query=q21 sf=0.01 mode=strict phase=compare status=pass reason=none elapsed_ms=3900.2
```

This is the correctness gate for the semi/anti residuals: the oracle exercises the EXISTS and NOT EXISTS controls with
the non-equi supplier residual.

## Scale evidence

Release q21 at `sf=1` under the 4G scale-atlas cap:

```text
systemd-run --user --scope --property=MemoryMax=4G --property=RuntimeMaxSec=600s \
  /bin/sh -lc 'target/release/toron-tpch run --query q21 --mode strict --sf 1'
```

Result:

```text
tpch_reachability query=q21 sf=1 mode=strict phase=execute status=fail reason=memory_budget_exhausted elapsed_ms=997.6 digest=none
{"diagnostic_message":"BudgetExhausted { dimension: Memory, producer: \"semi/anti existence-set build/spill accumulator\", requested: ByteCount(1526014069), available: ByteCount(1073741824) }", ...}
```

Wall movement:

| before ADR 344 | after ADR 344 |
| --- | --- |
| `streamed exchange output accumulator`, requested 1,074,142,886 | `semi/anti existence-set build/spill accumulator`, requested 1,526,014,069 |

The wall is now the semi existence set. q21 remains red, honestly named.

## Focused tests

New executor tests:

- `streaming_semi_anti_probe_applies_residual_and_preserves_left_rows`
- `streaming_left_anti_probe_applies_late_residual_conjunct`
- `streaming_semi_anti_existence_set_accumulator_is_budgeted`
- `streaming_inner_prefix_semi_anti_shape_fence_rejects_unmatched_topology`

Focused commands run:

```text
scripts/cargo-cgroup test -p toron --lib streaming_semi_anti -- --nocapture
scripts/cargo-cgroup test -p toron --lib streaming_left_anti_probe_applies_late_residual_conjunct -- --nocapture
scripts/cargo-cgroup test -p toron --lib streaming_inner_prefix_semi_anti_shape_fence_rejects_unmatched_topology -- --nocapture
```

Results:

- `streaming_semi_anti`: 2 passed.
- `streaming_left_anti_probe_applies_late_residual_conjunct`: 1 passed.
- `streaming_inner_prefix_semi_anti_shape_fence_rejects_unmatched_topology`: 1 passed.

## Seal

Commands run:

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup build -p toron-tpch --release
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup test -p toron --lib streaming_semi_anti -- --nocapture
scripts/cargo-cgroup test -p toron --lib streaming_left_anti_probe_applies_late_residual_conjunct -- --nocapture
scripts/cargo-cgroup test -p toron --lib streaming_inner_prefix_semi_anti_shape_fence_rejects_unmatched_topology -- --nocapture
scripts/cargo-cgroup test -p toron --lib reoriented_inner_chain_ -- --nocapture
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
git diff --check
```

All listed commands passed.

Summary:

- `toron --lib`: 1148 passed.
- `architecture_test`: 2057 passed.
- `toron --no-fail-fast`: all targets passed; one pre-existing storage-admin fixture remains ignored.
- `toron-tpch`: library, binary, architecture, decimal, correctness, harness, and probe tests passed.
- `correctness-matrix`: all 22 strict queries `pass@compare`.
- Release `sf=1` controls under the 4G cap:
  - q21: `fail(memory_budget_exhausted@execute)`, producer `semi/anti existence-set build/spill accumulator`.
  - q07/q14/q19: `pass@execute`.

No `q21`/`Q21` references are present in `crates/toron/src`; query-number scope stays in `toron-tpch` and the planning
artifacts.
