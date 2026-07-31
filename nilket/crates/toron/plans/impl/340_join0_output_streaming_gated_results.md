# ADR 340 Results - Join(0) output streaming, selectivity-gated

## Outcome

ADR 340 implemented the first q07/q21 streaming substrate wall-mover. It does not make q07 or q21 green at `sf=1`.
It removes the raw `Join(0)` output materialization wall measured by ADR 339 and exposes the next boundary:

| query | before ADR 340 | after ADR 340 |
| --- | ---: | ---: |
| q07 | `memory_budget_exhausted@execute`, producer `join output materialization`, requested 1,975,900,039 | `memory_budget_exhausted@execute`, producer `join output materialization`, requested 1,177,978,914 |
| q21 | `memory_budget_exhausted@execute`, producer `join output materialization`, requested 1,975,900,039 | `memory_budget_exhausted@execute`, producer `streamed join filter accumulator`, requested 1,074,312,896 |

q07 moves past `Join(0)` and reaches a later join-output materialization. q21 moves past raw `Join(0)`, but its
post-filter accumulator crosses the 1,073,741,824-byte output cap before the stream reaches Exchange. Both results are
honest red rows and preserve ADR 340's no-pass-claim scope.

## Phase 0 Selectivity Gate

A temporary `TORON_ADR340_SELECTIVITY_PROBE` path was added to the Filter arm, run in the release binary under a 4G
cgroup, and removed before seal. The probe chunk-materialized `Join(0)`, applied the bound Filter, and recorded
post-filter rows/bytes because the production ADR 335 output guard fires before the Filter runs.

Probe command shape:

```sh
scripts/cargo-cgroup build -p toron-tpch --release
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=900s \
  /bin/sh -lc 'TORON_ADR340_SELECTIVITY_PROBE=1 target/release/toron-tpch run --query q07 --mode strict --sf 1 || true'
```

Measured gate:

| query | raw rows | raw bytes | filtered rows | filtered bytes | gate |
| --- | ---: | ---: | ---: | ---: | --- |
| q07 | 6,001,215 | 1,975,900,039 | 1,828,450 | 555,336,226 | proceed |
| q21 | 6,001,215 | 1,975,900,039 | 3,793,296 | 1,150,423,298 | proceed, but filtered accumulator must be budgeted |

q07's filtered output fits under the effective output cap. q21's full filtered output is materially smaller than raw
but still over cap, so ADR 340 proceeded with the cut and made the filtered accumulator itself a named budgeted wall.

## Implementation

Production changes are in `crates/toron/src/exec.rs`.

- `ExecutionPlan::Filter` checks for `Filter <- Inner JoinReplayId(0)` before `execute_bound(input)`.
- The matched path builds the normal in-memory hash-join selection, materializes contiguous selection chunks, applies
  the Filter to each chunk, and accumulates only filtered chunks into the returned `Vec<RecordBatch>`.
- The raw 1.98 GB `Join(0)` output is never materialized as one batch.
- The filtered accumulator uses the same effective cap as ADR 335 (`task_budget.memory * 16`). If the accumulator
  crosses that cap, it emits `BudgetExhausted { dimension: Memory, producer: "streamed join filter accumulator", ... }`.
- Non-`JoinReplayId(0)` filters and non-inner joins stay on the existing materialized path.

`ExecutionError::BudgetExhausted` now carries a `producer` label. Existing status/classification is unchanged:
`reason_for_error` still maps the retained debug message to `memory_budget_exhausted`, and protocol/distributed
failure mapping ignores the producer. The label exists so wall-mover cuts can prove that a red row moved to a new
producer instead of relying only on requested bytes.

`crates/toron/tests/architecture/binder_decomposition.rs` also allowlists `src/error.rs` as an ADR 340 later surface:
the producer label is a deliberate diagnostic runtime-surface change, not binder decomposition drift.

## Scale Evidence

Final release binary, `MemoryMax=4G`, strict `sf=1`:

```sh
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=900s \
  /bin/sh -lc 'target/release/toron-tpch run --query q07 --mode strict --sf 1 || true'
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=900s \
  /bin/sh -lc 'target/release/toron-tpch run --query q21 --mode strict --sf 1 || true'
```

| query | result | diagnostic producer | requested | available | memory peak |
| --- | --- | --- | ---: | ---: | ---: |
| q07 | `fail(memory_budget_exhausted@execute)` | `join output materialization` | 1,177,978,914 | 1,073,741,824 | 3.7 GB |
| q21 | `fail(memory_budget_exhausted@execute)` | `streamed join filter accumulator` | 1,074,312,896 | 1,073,741,824 | 3.8 GB |

q07's new requested bytes differ from ADR 339's `Join(0)` 1,975,900,039-byte wall and match a later materializing
join boundary. q21's new producer proves ADR 340 did not replace the raw `Join(0)` wall with a silent filtered-Vec
OOM.

## Correctness Guard

Strict correctness at `sf=0.01` remains green for the two streamed cells:

| query | command | result |
| --- | --- | --- |
| q07 | `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07 --sf 0.01` | `pass@compare` |
| q21 | `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01` | `pass@compare` |

This is the row-order guard for the chunked Filter path: q07/q21 use the streamed `Join(0)` path at default scale and
still compare against the independent strict oracles.

## Focused Tests

New executor tests cover the bounded surface:

- `streaming_join0_filter_matches_materialized_float64_and_decimal_order`
- `streaming_join0_filter_replaces_raw_join_materialization`
- `streaming_join0_filter_accumulator_is_budgeted`
- `streaming_join0_filter_shape_fence_keeps_other_join_ids_materialized`

Focused command:

```sh
scripts/cargo-cgroup test -p toron streaming_join0_filter -- --nocapture
```

Result: 4 passed.

## ADR 330 Atlas Update

The q07/q21 `sf=1` cells remain `fail(memory_budget_exhausted@execute)`, but their first wall moved:

- q07: ADR 335 `Join(0)` raw output guard, requested 1,975,900,039 -> later join output materialization, requested
  1,177,978,914.
- q21: ADR 335 `Join(0)` raw output guard, requested 1,975,900,039 -> ADR 340 streamed-filter accumulator, requested
  1,074,312,896.

This confirms ADR 339's staged substrate shape: ADR 340 is a wall-mover. q07 now points at the next downstream
materialization; q21 needs the next staged boundary to avoid accumulating the post-filter result.

## Seal

Commands run:

```sh
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron streaming_join0_filter -- --nocapture
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=900s \
  /bin/sh -lc 'target/release/toron-tpch run --query q07 --mode strict --sf 1 || true'
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=900s \
  /bin/sh -lc 'target/release/toron-tpch run --query q21 --mode strict --sf 1 || true'
```

Results:

- Formatting/checks passed.
- Focused ADR 340 tests: 4 passed.
- `toron --lib`: 1138 passed.
- `toron --test architecture_test`: 2057 passed.
- `toron --no-fail-fast`: passed.
- `toron-tpch`: passed.
- `correctness --query q07/q21 --sf 0.01`: both `pass@compare`.
- `correctness-matrix`: all 22 strict queries `pass@compare`.
- `matrix`: q07/q21 remain `fail(memory_budget_exhausted@execute)`, as expected for this wall-mover.
- `sf=1` release probes under 4G cgroup: q07/q21 emit named `memory_budget_exhausted` rows with the post-ADR-340
  producers and requested bytes recorded above.
