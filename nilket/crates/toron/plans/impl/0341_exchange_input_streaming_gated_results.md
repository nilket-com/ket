# ADR 341 Results - Exchange input streaming, output-handling gated

## Outcome

ADR 341 implemented the Branch 2 wall-mover from the plan. The executor still uses the materialize-each
`execute_bound(...) -> Vec<RecordBatch>` consumer interface, so 341 does not add a broad streaming consumer path and
does not make q21 green. Instead, it removes q21's ADR 340 Filter accumulator wall and exposes the next named wall: the
returned Exchange output accumulator.

| query | before ADR 341 | after ADR 341 |
| --- | --- | --- |
| q21 | `memory_budget_exhausted@execute`, producer `streamed join filter accumulator`, requested 1,074,312,896 | `memory_budget_exhausted@execute`, producer `streamed exchange output accumulator`, requested 1,074,142,886 |
| q07 | `memory_budget_exhausted@execute`, producer `join output materialization`, requested 1,177,978,914 | `memory_budget_exhausted@execute`, producer `join output materialization`, requested 1,177,978,914 |

q21's producer moved off the ADR 340 Filter accumulator, proving the filtered batches are partitioned into Exchange
before a Filter result `Vec` exists. q07 composes through the Exchange and remains at the later join-output wall already
measured by ADR 340.

## Phase 0 Gate

The current executor boundary is Branch 2:

- `ExecutionPlan::Exchange` still returns `Vec<RecordBatch>`.
- The downstream consumer (`Join(1)` for q21/q07) still calls `execute_bound(Exchange) -> Vec`.
- Adding consumer-pull/partition-forward would be the ADR 342 substrate interface, so ADR 341 stayed bounded to
  Exchange input streaming plus a budgeted returned-output accumulator.

## Implementation

Production changes are in `crates/toron/src/exec.rs`.

- `ExecutionPlan::Exchange` detects `Exchange <- Filter <- Inner JoinReplayId(0)` before `bound_exchange` calls
  `execute_bound(input)`.
- ADR 340's chunk/filter loop was refactored into a visitor-style producer, so the Exchange path receives each
  filtered join-output chunk before the Filter accumulator exists.
- The materialized and streamed Exchange paths share `repartition_exchange_batch` and `finalize_exchange_partitions`,
  preserving partition order and empty-partition behavior.
- Branch 2 now budgets the total retained returned Exchange output, not only individual partition batches. When it
  crosses the effective output cap, it emits
  `BudgetExhausted { dimension: Memory, producer: "streamed exchange output accumulator", ... }`.
- Non-matching Exchange shapes keep the existing materialized path.

## Scale Evidence

Final release binary, `MemoryMax=4G`, strict `sf=1`:

```sh
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=900s \
  /bin/sh -lc 'target/release/toron-tpch run --query q21 --mode strict --sf 1 || true'
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=900s \
  /bin/sh -lc 'target/release/toron-tpch run --query q07 --mode strict --sf 1 || true'
```

| query | result | diagnostic producer | requested | available | memory peak |
| --- | --- | --- | ---: | ---: | ---: |
| q21 | `fail(memory_budget_exhausted@execute)` | `streamed exchange output accumulator` | 1,074,142,886 | 1,073,741,824 | 3.8 GB |
| q07 | `fail(memory_budget_exhausted@execute)` | `join output materialization` | 1,177,978,914 | 1,073,741,824 | 3.6 GB |

q21 is not green; it moved to the named Branch 2 Exchange-output wall. q07 is unchanged at the later join wall, which is
the expected composition result for ADR 341.

## Correctness Guard

Strict correctness at `sf=0.01` remains green for the two streamed cells:

| query | command | result |
| --- | --- | --- |
| q21 | `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01` | `pass@compare` |
| q07 | `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07 --sf 0.01` | `pass@compare` |

This is the row-order guard for the Exchange repartition path: partitioned output matches the materialized Exchange
order at default scale.

## Focused Tests

New executor tests cover the bounded surface:

- `streaming_exchange_join0_filter_matches_materialized_float64_and_decimal_order`
- `streaming_exchange_join0_filter_replaces_filter_accumulator_with_exchange_budget`
- `streaming_exchange_join0_filter_shape_fence_keeps_other_join_ids_materialized`

Existing ADR 340 filter tests were rerun after the helper refactor:

- `streaming_join0_filter_accumulator_is_budgeted`
- `streaming_join0_filter_matches_materialized_float64_and_decimal_order`
- `streaming_join0_filter_replaces_raw_join_materialization`
- `streaming_join0_filter_shape_fence_keeps_other_join_ids_materialized`

Focused commands:

```sh
scripts/cargo-cgroup test -p toron streaming_exchange_join0_filter -- --nocapture
scripts/cargo-cgroup test -p toron streaming_join0_filter -- --nocapture
```

Results: 3 passed for ADR 341; 4 passed for ADR 340.

## ADR 330 Atlas Update

The q21 `sf=1` cell remains `fail(memory_budget_exhausted@execute)`, but its first wall moved again:

- ADR 340: `streamed join filter accumulator`, requested 1,074,312,896.
- ADR 341: `streamed exchange output accumulator`, requested 1,074,142,886.

This confirms ADR 341 as a wall-mover. q21 now needs ADR 342's consumer streaming / join-to-join boundary to avoid
returning the full Exchange output `Vec`.

## Seal

All commands completed under `scripts/cargo-cgroup` except the explicit `systemd-run` release probes:

```sh
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron streaming_exchange_join0_filter -- --nocapture
scripts/cargo-cgroup test -p toron streaming_join0_filter -- --nocapture
scripts/cargo-cgroup build -p toron-tpch --release
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07 --sf 0.01
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```

Results:

- Formatting and both package checks passed.
- Focused ADR 341 tests: 3 passed. ADR 340 regression tests: 4 passed.
- `toron --lib`: 1141 passed.
- `toron --test architecture_test`: 2057 passed.
- `toron --no-fail-fast`: passed, including integration and doc-test suites.
- `toron-tpch`: passed, including 172 correctness oracle tests and 41 harness tests.
- `correctness-matrix`: all 22 strict queries `pass@compare`.
- `matrix`: current sf=1 wall rows stay named as expected; q21 is
  `fail(memory_budget_exhausted@execute)` at `streamed exchange output accumulator`, q07 remains
  `fail(memory_budget_exhausted@execute)` at `join output materialization`, and q14/q19 remain `pass@execute`.
- `git diff --check`: clean.
