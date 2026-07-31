# ADR 353 results - aggregate executor submodule

## Decision

Carved `crates/toron/src/exec/aggregate.rs`.

Phase 0 found the production aggregate executor reaches expressions through the stable `Executor`
bound-expression API (`evaluate_bound_program` / `evaluate_bound_single_root`), not private expr internals, so the
expr-first no-go branch was not needed.

## Coupling map

| Bucket | Symbols / callers | Evidence |
| --- | --- | --- |
| Move now | `bound_global_aggregate`, `fold_global_aggregate_state`, `init_global_aggregate_state`, `finalize_global_aggregate_state`, `bound_grouped_aggregate`, `fold_bound_grouped_aggregate_batch`, `finalize_bound_grouped_aggregate`, `AggState`, `GroupValue`, state init/update/finalize helpers, group materialization helpers | One-way deps into root expression evaluation and `aggregate_kernel.rs`; no private expr internals. |
| Move now, test/legacy helper | `#[cfg(test)] grouped_aggregate`, `cast_to_f64` | Legacy unbound aggregate helper moved with the aggregate module; the root legacy/unbound dispatch still owns routing and calls the helper. |
| Move now, local tests | Decimal AVG finalize tests; Decimal128 `group_value` / `group_column` tests | Self-contained aggregate helper tests moved into `exec/aggregate.rs`. |
| Stay root | `execute` / `execute_bound` dispatch, legacy/unbound aggregate routing and inline global mock, scan/exchange/window dispatch | ADR 353 is not a dispatch cleanup. |
| Stay root but call aggregate API | Window aggregate helpers and legacy unbound global mock | They reuse aggregate state helpers through a deliberate intra-`exec` API. |
| Re-point callers | `exec.rs`, `exec/join.rs`, `exec/join/streaming.rs` | Path/visibility only; no logic change. |
| Retarget guards | Aggregate/timestamp architecture guards that read only `src/exec.rs` | Updated to read combined `exec_source()` where the aggregate implementation now lives. |

## API surface

`exec.rs` declares `mod aggregate;` and re-exports the aggregate executor surface inside `crate::exec`:

- State and keys: `AggState`, `GroupValue`.
- Global aggregate stream helpers: `init_global_aggregate_state`, `finalize_global_aggregate_state`.
- Shared state helpers used by windows and legacy tests: `sum_state_for_data_type`, `avg_state_for_data_type`,
  `min_state_for_data_type`, `max_state_for_data_type`, `numeric_value`, `add_sum_value`, `add_avg_value`,
  `update_min_state`, `update_max_state`, `sum_state_array`, `avg_state_array`, `min_max_state_array`.
- Test-only legacy naming helpers: `aggregate_state_keyword`, `aggregate_state_output_name`.

The bound aggregate methods are split `impl Executor` methods in `exec/aggregate.rs`. The root bound aggregate dispatch
calls them; the legacy/unbound logical aggregate branch remains rooted.

## Line-count ratchet

Before ADR 353:

- `exec.rs`: 11,611 lines.

After ADR 353 and fmt:

- `exec.rs`: 9,772 lines.
- `exec/aggregate.rs`: 1,886 lines.

`exec_decomposition.rs` now enforces:

- `exec.rs` declares `mod aggregate;`.
- `src/exec/aggregate.rs` exists.
- carved aggregate executor markers are absent from the root.
- root line count is <= 10,000.
- executor modules remain de-branded (the existing no-qNN guard covers the new file).

## Dispatch and tests

Dispatch stayed rooted. The bound aggregate dispatch re-points to the aggregate module methods. The legacy/unbound
aggregate branch, including the inline global mock, stays in `exec.rs`; only its grouped helper moved.

Local aggregate helper tests moved into `exec/aggregate.rs`. Mixed executor/streaming harness tests stayed in the root
test module.

## Verification

Full seal completed:

- `scripts/cargo-cgroup fmt`
- `scripts/cargo-cgroup fmt -- --check`
- `scripts/cargo-cgroup check -p toron --all-targets`
- `scripts/cargo-cgroup check -p toron-tpch --all-targets`
- `scripts/cargo-cgroup test -p toron --lib` -> 1160 passed / 0 failed
- `scripts/cargo-cgroup test -p toron --test architecture_test` -> 2075 passed / 0 failed
- `scripts/cargo-cgroup test -p toron --no-fail-fast`
- `scripts/cargo-cgroup test -p toron-tpch`
- `scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix` -> 22/22 `pass@compare`
- `scripts/cargo-cgroup run -p toron-tpch -- matrix` -> 22/22 dev + strict `pass@execute`
