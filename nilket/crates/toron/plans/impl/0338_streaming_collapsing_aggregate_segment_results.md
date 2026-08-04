# ADR 338 Results - streaming collapsing aggregate segment

## Outcome

ADR 338 implemented the bounded streaming proof cut predicted by ADR 337.

The executor now recognizes the direct collapsing segment before materializing the aggregate input:

`Aggregate(global) <- Filter? <- Inner Join`

For that shape, the join still materializes its retained inputs, but it no longer materializes the full join output
as one `Vec<RecordBatch>` for the aggregate. Instead it walks the join selection in bounded contiguous chunks,
optionally applies the filter to each chunk, folds the global aggregate state, and drops the chunk.

Measured result at `sf=1` under the 4G cgroup:

| query | before ADR 338 | after ADR 338 |
| --- | --- | --- |
| q14 | `fail(memory_budget_exhausted@execute)` | `pass@execute`, `pass@compare` |
| q19 | `fail(memory_budget_exhausted@execute)` | `pass@execute`, `pass@compare` |

The no-overclaim gate also held:

| query | ADR 337 expectation | ADR 338 result |
| --- | --- | --- |
| q07 | expected-partial | still `fail(memory_budget_exhausted@execute)` |
| q17 | expected-partial | still `fail(memory_budget_exhausted@execute)` |
| q21 | expected-partial | still `fail(memory_budget_exhausted@execute)` |

So the primitive works for the measured direct segment, and the broader join-to-join / exchange streaming substrate
remains a separate arc.

## Implementation

Production changes are in `crates/toron/src/exec.rs`.

- `ExecutionPlan::Aggregate` now checks for the streaming global segment before `execute_bound(input)`.
- `bound_global_aggregate` was refactored into init/fold/finalize helpers used by both the existing materialized
  path and the streaming path.
- The streaming path covers both in-memory hash join output and spilled hash join output.
- Chunk folding uses contiguous `JoinOutputRow` ranges in the same order as the materialized path, preserving
  Float64 fold order rather than relying on associativity.
- Grouped aggregates, non-inner joins, join-to-join pipelines, exchange pipelines, retained-input walls, and
  spill-writer walls stay on the existing materialized path.

One architecture guard required preserving the public `filter_batches_with_bound_predicate` source shape: the
public helper still owns the boolean predicate downcast, while the new single-batch helper serves the streaming
chunk path.

## Focused Tests

New unit coverage:

- `streaming_collapsing_aggregate_matches_materialized_float64_and_decimal_order`
  - proves streaming and materialized paths are bit-identical for Float64 and value/type-identical for Decimal128;
  - uses more than one chunk to exercise fold order.
- `streaming_collapsing_aggregate_covers_spilled_join_route`
  - forces the spilled hash join route while keeping the chunk output under the per-chunk memory guard;
  - verifies parity with a high-budget materialized baseline and records spill bytes.
- `streaming_collapsing_aggregate_shape_fence_keeps_grouped_path_materialized`
  - proves grouped aggregate does not take the global streaming shortcut.

Focused command:

```sh
scripts/cargo-cgroup test -p toron --lib streaming_collapsing_aggregate -- --nocapture
```

Result: 3 passed.

## Scale Evidence

Final release binary was rebuilt after the last source edit:

```sh
scripts/cargo-cgroup build -p toron-tpch --release
```

Scale probes were run under:

```sh
systemd-run --user --same-dir --wait --collect --pipe \
  --property=MemoryMax=4G \
  --property=MemoryAccounting=yes \
  --property=RuntimeMaxSec=900s \
  /bin/sh -lc '...'
```

Target cells:

| query | command | result |
| --- | --- | --- |
| q14 | `target/release/toron-tpch run --query q14 --mode strict --sf 1` | `pass@execute`, 15.36s |
| q19 | `target/release/toron-tpch run --query q19 --mode strict --sf 1` | `pass@execute`, 15.67s |
| q14 | `target/release/toron-tpch correctness --query q14 --sf 1` | `pass@compare`, 16.62s |
| q19 | `target/release/toron-tpch correctness --query q19 --sf 1` | `pass@compare`, 18.71s |

The capped combined run peaked at 2.9G resident memory.

Expected-partial controls:

| query | command | result |
| --- | --- | --- |
| q07 | `target/release/toron-tpch run --query q07 --mode strict --sf 1` | `fail(memory_budget_exhausted@execute)`, requested 1,975,900,039 bytes |
| q17 | `target/release/toron-tpch run --query q17 --mode strict --sf 1` | `fail(memory_budget_exhausted@execute)`, requested 1,651,834,429 bytes |
| q21 | `target/release/toron-tpch run --query q21 --mode strict --sf 1` | `fail(memory_budget_exhausted@execute)`, requested 1,975,900,039 bytes |

This matches ADR 337's expected-vs-actual split: q14/q19 move; q07/q17/q21 do not.

## Matrix Checks

Default correctness matrix:

```sh
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
```

Result: all 22 strict queries are `pass@compare`.

Default reachability matrix:

```sh
scripts/cargo-cgroup run -p toron-tpch -- matrix
```

Relevant rows after ADR 338:

- `q14 dev=pass@execute strict=pass@execute`
- `q19 dev=pass@execute strict=pass@execute`
- q07/q17/q21 remain strict `fail(memory_budget_exhausted@execute)`.

## Seal

Commands run:

```sh
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```

Results:

- formatting and diff whitespace clean;
- toron all-targets check clean;
- toron-tpch all-targets check clean;
- toron library suite: 1134 passed;
- toron architecture suite: 2057 passed;
- full toron package sweep: passed, including integration tests and doc tests;
- toron-tpch suite: passed;
- q14/q19 `sf=1` reach and correctness passed under the 4G cap.

## Follow-Up

ADR 338 proves the streaming primitive on the smallest measured direct segment. The remaining sf=1 memory cells are
not the same cut:

- q07/q17/q21 need streaming across join-to-join and/or exchange boundaries;
- q12/q18 remain retained-input / spill-entry walls;
- q04/q20 remain spill-writer resident-state walls.

The next streaming decision is therefore substrate-sized, not another small direct-segment extension.
