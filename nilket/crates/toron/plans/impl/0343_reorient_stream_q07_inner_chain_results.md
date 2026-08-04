# ADR 343 results - reorient + stream the q07 inner-join chain

## Summary

ADR 343 landed as a bounded executor-local streaming helper in `crates/toron/src/exec.rs`.

The implementation does **not** change the bound plan or the optimizer/cost model. It recognizes the matched all-Inner
left-deep shape at grouped-aggregate execution time, builds the small dimension side for each downstream inner join,
streams the growing intermediate as the probe, restores the original observable column order, and folds the grouped
aggregate without materializing the downstream join chain.

The core executor symbols intentionally avoid TPC-H query-number names. The query-specific scope is recorded here and
in the ADR; the engine names the primitive by shape: reoriented inner-chain streaming into a grouped aggregate.

## Phase 0 grounding

- q07's downstream chain is all `Inner` and `HashPartitioned`; q21 remains excluded because its downstream shape includes
  semi/anti joins.
- The reorientation is executor-local. The new path lives in the grouped aggregate execution arm and only engages after
  a strict shape fence:
  - top grouped aggregate input is `Exchange(Project(Filter(Join(...))))`;
  - downstream joins have replay ids 1 through 4;
  - each downstream join is `Inner`, `HashPartitioned`, non-null-safe, and has no match filter;
  - the growing side appears under an `Exchange` and is streamed as the probe;
  - all nonmatching shapes fall back to the existing materializing path.
- The HashPartitioned physical plan and cost model are unchanged.

## Implementation

- Refactored grouped aggregate into fold/finalize helpers so a stream can update the same aggregate state used by the
  materialized path.
- Added a matched-shape reoriented inner-chain helper:
  - prepares small dimension build tables once per stage;
  - streams the base Join(0) -> Filter -> Exchange batches from the ADR 340/341 visitor path;
  - probes each reoriented downstream inner join batch-by-batch;
  - applies the outer filter/project and folds into the grouped aggregate state.
- Added a swapped materializer for the reoriented inner join that emits probe/original-left columns first and
  build/original-right columns second, matching the bound plan's observable schema.
- Added budgeted producers for residual walls:
  - `reoriented join build accumulator`;
  - `reoriented join output materialization`.

## Tests

Focused ADR 343 tests:

```text
scripts/cargo-cgroup test -p toron reoriented_inner_chain_ -- --nocapture
```

Result: 3 passed.

- `reoriented_inner_chain_grouped_aggregate_matches_materialized_float64_and_decimal_order`
- `reoriented_inner_chain_removes_intermediate_build_accumulator`
- `reoriented_inner_chain_shape_fence_keeps_other_replay_ids_materialized`

Broader test coverage run after the implementation:

```text
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
```

Results:

- `toron --lib`: 1144 passed.
- `architecture_test`: 2057 passed.
- `toron --no-fail-fast`: all test targets passed; one pre-existing ignored storage test remains ignored.
- `toron-tpch`: all tests passed.

## Scale acceptance

Release binary rebuilt with:

```text
scripts/cargo-cgroup build -p toron-tpch --release
```

q07 at sf=1 under the 4G scale-atlas cap:

```text
systemd-run --user --scope --property=MemoryMax=4G --property=RuntimeMaxSec=600s \
  /bin/sh -lc 'target/release/toron-tpch run --query q07 --mode strict --sf 1 && \
  target/release/toron-tpch correctness --query q07 --sf 1'
```

Result:

- `q07 sf=1 strict`: `pass@execute`, elapsed 3010.6 ms.
- `q07 sf=1 strict correctness`: `pass@compare`, elapsed 6828.3 ms.

This moves q07 from the ADR 342 build-side/output wall to a green sf=1 cell.

## q21 fence

q21 remains out of scope and still reports the existing semi/anti-chain wall:

```text
systemd-run --user --scope --property=MemoryMax=4G --property=RuntimeMaxSec=600s \
  /bin/sh -lc 'target/release/toron-tpch run --query q21 --mode strict --sf 1'
```

Result:

- `q21 sf=1 strict`: `fail(memory_budget_exhausted@execute)`.
- Diagnostic producer: `streamed exchange output accumulator`.

The all-Inner helper does not claim or alter the q21 semi/anti path.

## Default-scale correctness

```text
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07 --sf 0.01
```

Results:

- Correctness matrix: all 22 strict queries `pass@compare`.
- Explicit `q07 sf=0.01`: `pass@compare`, elapsed 3820.2 ms.

## Green-cell controls

The listed sf=1 green controls stayed green under the same release binary and 4G cap:

```text
q01 q03 q05 q06 q10 q13 q16 q22 q14 q19
```

All returned `status=pass reason=none` in strict reachability mode.

## Notes

- No `q07`/`Q07` references remain in `crates/toron/src`; TPC-H query numbering is scoped to `toron-tpch` and planning
  artifacts.
- The default reachability matrix still reflects unrelated current strict memory walls for q04/q12/q18/q20/q21; ADR 343
  changes q07 only.
