# ADR 397 results - q03 substrate re-profile + scan projection strike

## Verdict

ADR 397 re-profiled the post-396 Rune q03 materialized path, found the new dominant fixable
producer, and landed a generic strike: join inputs now push required-column projection through
joins instead of treating joins as a projection barrier.

The result is a large substrate win on the same value-gated q03 path:

| metric | ADR 396 | ADR 397 |
|---|---:|---:|
| Rune q03 sf=1 persisted median | 1135.070092 ms | 410.468097 ms |
| Rune / Polars | 10.16x | 3.97x |
| Toron / Polars | 26.45x | 11.41x |
| spill bytes | 0 | 0 |
| output rows | 11,620 | 11,620 |

Primary metric: Rune absolute median improves by 724.6 ms, a 63.8% reduction. The Polars ratio
also moves, but Polars remains short-time unstable at sf=1, so the stable Toron/Rune medians are
the primary evidence.

## Phase 0 ledger

The post-396 profile loop confirmed the ADR 396 join row-list strike persisted:

| field | value |
|---|---:|
| plan fingerprint | `2b72c561c2595512` |
| materialized path | true |
| spill_bytes_max | 0 |
| output rows | 11,620 |
| build rows | 3,241,776 |
| unique keys | 829,958 |
| duplicate keys | 700,918 |
| max rows per key | 7 |
| overflow_heap_capacity_bytes | 0 |

The profile then named the new cost class:

| frame | pre-strike share |
|---|---:|
| `__memmove_avx_unaligned_erms` | 14.89% |
| `parquet::arrow::buffer::offset_buffer::OffsetBuffer<I>::extend_from_dictionary` | 5.96% |
| `parquet::encodings::rle::RleDecoder::get_batch_with_dict` | 4.82% |
| `toron::exec::join::build_in_memory_join_rows` | 4.65% |
| `_int_malloc` | 3.77% |
| `arrow_select::filter::FilterBytes<OffsetSize>::extend_idx` | 3.08% |

ADR 397 did not infer "pushdown" just from Arrow copy frames. It added an off-by-default Parquet
scan ledger (`TORON_TPCH_PARQUET_SCAN_LEDGER=1`) and recorded the actual scan requests:

| table | requested columns before | PBC | bytes before |
|---|---|---|---:|
| lineitem | 0..13 | none | 869,291,056 |
| customer | 0..7 | `c_mktsegment` | 32,675,534 |
| orders | 0..7 | none | 189,655,389 |

Classification: missing/insufficient projection pushdown through joins. The q03 Rune plan was
reading full join inputs even though the final plan needed only keys, filter columns, group keys,
and revenue inputs.

## Implementation

The committed strike is generic:

- `binder::optimize_for_required` now pushes parent required columns through `ExecutionPlan::Join`.
- `binder/pushdown.rs` computes each side's required columns from join output columns, join keys,
  and match-filter columns, rewrites join keys and filters through the child remaps, and rebuilds
  the join schema from optimized children.
- Join inputs retain one row-bearing column when pruning would otherwise request zero columns from
  a non-empty side. This preserves EXISTS/count/multiplicity semantics.
- The q03 harness ledger is env-gated and off by default.
- The persisted Parquet PBC inflation path now marks omitted synthetic columns nullable when it
  fills them with null arrays.
- A local cross-join test fixture now honors `ScanRequest.projected_columns`; this matches the
  real scan contract and keeps the projection pushdown test surface honest.

No q03/lineitem/customer/orders branch was added to core binder code.

## Post-strike ledger

The same q03 Rune materialized path now reads only required columns:

| table | requested columns after | PBC | bytes after |
|---|---|---|---:|
| lineitem | 1,2,3,8 | none | 266,309,472 |
| customer | 0 (+ PBC column 1) | `c_mktsegment` | 3,700,308 raw / 255,248 output |
| orders | 0,1,2,3 | none | 36,566,400 |

Total raw scan bytes dropped from about 1.09 GiB to 306.6 MiB. The final release loop confirmed
the new plan:

| field | value |
|---|---:|
| plan fingerprint | `666e0ac028036f70` |
| materialized path | true |
| spill_bytes_max | 0 |
| output rows | 11,620 |
| final loop iterations | 399.231, 394.164, 380.644, 376.913 ms |
| overflow_heap_capacity_bytes | 0 |

## Profile delta

Post-strike profile (`target/tpch/adr397_rune_q03_loop_after.perf.data`, not committed) captured
5,853 samples with DWARF symbols.

| frame | before | after |
|---|---:|---:|
| `__memmove_avx_unaligned_erms` | 14.89% | 5.69% |
| `OffsetBuffer<I>::extend_from_dictionary` | 5.96% | 0.03% |
| `FilterBytes<OffsetSize>::extend_idx` | 3.08% | 0.05% |
| `_int_malloc` | 3.77% | 1.59% |
| `build_in_memory_join_rows` | 4.65% | 10.94% |
| hashbrown entry path | - | 7.96% |
| `RleDecoder::get_batch_with_dict` | 4.82% | 5.23% |

The struck producer moved exactly as expected: string/dictionary/filter materialization and bulk
copy collapse once joins stop forcing full-table scan inputs. The new dominant Toron-owned
residual is back in join build / hash-table insertion.

## Four-way run

Command:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
  target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines toron,rune,polars_sql,duckdb \
    --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr397_q03_sf1
```

| engine | status | median | spread | flags |
|---|---|---:|---:|---|
| toron | pass | 1180.655554 ms | 0.020 | none |
| rune | pass | 410.468097 ms | 0.052 | none |
| polars_sql | pass | 103.520009 ms | 0.238 | unstable |
| duckdb | pass | 34.0 ms | 0.265 | unstable, schema_mismatch |

Ratios:

| ratio | value | note |
|---|---:|---|
| Rune / Polars | 3.97x | peer-substrate gap, Polars denominator unstable |
| Toron / Polars | 11.41x | end-to-end peer gap |
| SQL / Rune | 2.88x | logical-shape penalty after substrate strike |
| Rune / DuckDB | 12.07x | directional, DuckDB unstable |
| Polars / DuckDB | 3.04x | directional, both short-time rows unstable |
| Toron / DuckDB | 34.72x | directional, DuckDB unstable |

The baseline rail correctly refused to bank the run because DuckDB/Polars are short-time unstable
at sf=1. That does not affect the primary Toron/Rune absolute medians.

## Correctness and gates

Correctness:

- `target/release/toron-tpch correctness --query q03 --sf 1 --mode strict --data-source parquet`:
  pass (`elapsed_ms=2725.5`).
- Rune q03 final loop: 11,620 rows, materialized path, spill 0.

Focused tests:

- `binder::tests::join_projection_pushdown_keeps_join_keys_and_parent_columns`: pass.
- `parquet_scan_projection_tests::projected_pbc_inflation_allows_omitted_non_nullable_columns`:
  pass.
- Regressions from full suite diagnosis:
  - `cross_join_test::adr319_or_branch_key_extraction_matches_explicit_factored_filter`: pass.
  - `group_by_test::{not_exists_lowers_to_anti_join, not_in_with_null_in_subquery_returns_no_rows,
    uncorrelated_exists_passes_all_rows_iff_subquery_nonempty}`: pass.

Broad gates:

- `cargo fmt -- --check`: pass.
- `check -p toron --all-targets`: pass.
- `check -p toron-tpch --features polars-sql-reference --all-targets`: pass.
- `architecture_test`: pass (2079/0), binder root remains below the ADR 176 cap.
- `test -p toron --no-fail-fast`: pass.
- `test -p toron-tpch --features polars-sql-reference --no-fail-fast`: pass.
- Release `toron-tpch --features polars-sql-reference` rebuild with debug symbols: pass.

The broad `toron` test gate was run through the existing `cargo-cgroup` script with the `affinity`
backend and `CARGO_CGROUP_CPUS=0-11`, because the systemd 12 GiB cgroup silently killed the
12-job test-target link. CPU remained capped to 12 cores.

## Forward

ADR 397 moved the substrate gap from 10.16x to 3.97x vs Polars on the Rune q03 materialized path.
That is close enough that the next strike should be narrower:

- re-profile the post-397 plan;
- target the now-dominant join-build/hash-table insertion residual, or prove the residual is an
  irreducible hash-build floor;
- keep the same four-way Rune/Polars scorecard.

The planner/logical-shape penalty is now larger proportionally (`SQL/Rune=2.88x`), so a later
planner-shape cut is also justified. The substrate loop should still take one more measured strike
first, because the post-397 profile names a concrete Toron-owned residual.
