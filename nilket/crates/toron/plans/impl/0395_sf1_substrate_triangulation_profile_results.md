# ADR 395 results - sf=1 substrate triangulation + materialized q03 profile

## Phase 0 value gate

Command:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch polars-q03-value-gate \
    --sf 1 --mode strict --data-source parquet --data-dir target/tpch/data
```

Verdict: pass. The Rust `polars_sql` explicit-join q03 path produced 11,620 rows at sf=1 and compared against the
independent q03 oracle with q03 order checking.

| field | value |
| --- | ---: |
| engine_version | 0.54.4 |
| rows | 11,620 |
| max revenue diff (raw scale-4) | 222 |
| tolerance (raw scale-4) | 1,000 |
| plan fingerprint | `fnv1a64:a3b5c15847833ac3` |
| schema fingerprint | `fnv1a64:e6f7a214e444a631` |

No ratio below is trusted without this gate.

## sf=1 persisted four-way

Command:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
  target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines toron,rune,polars_sql,duckdb \
    --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr395_q03_sf1_fourway
```

Artifacts: `target/tpch/perf_adr395_q03_sf1_fourway/latest_raw.jsonl` and
`target/tpch/perf_adr395_q03_sf1_fourway/latest_summary.jsonl`.

| engine | status | execute_ms median | min | spread | peak RSS KiB | rows | spill | version | flags |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| toron | pass | 3,199.965 | 3,109.791 | 0.045 | 2,747,648 | 11,620 | 0 | n/a | none |
| rune | pass | 1,339.752 | 1,331.773 | 0.038 | 1,170,104 | 11,620 | 0 | n/a | none |
| polars_sql | pass | 107.951 | 104.981 | 0.265 | 902,968 | 11,620 | 0 | 0.54.4 | unstable |
| duckdb | pass | 43.000 | 39.000 | 0.209 | 151,816 | 11,620 | 0 | v1.5.4 | unstable, schema_mismatch |

DuckDB and Polars are still short-time/noisy at sf=1, so ratios with those denominators are directional, not banked
baselines. Toron and Rune are stable.

## Ratios

| ratio | value | note |
| --- | ---: | --- |
| Toron / Polars | 29.64x | headline peer gap; Polars denominator unstable |
| Rune / Polars | 12.41x | headline substrate gap for hand-authored logical shape; Polars denominator unstable |
| Polars / DuckDB | 2.51x | both peer rows unstable at sf=1 |
| Rune / DuckDB | 31.16x | DuckDB denominator unstable |
| Toron / DuckDB | 74.42x | DuckDB denominator unstable |
| SQL / Rune | 2.39x | stable; logical-shape penalty remains real |

Verdict: the logical-shape penalty is measurable but smaller than the substrate gap. Rune's hand-authored q03 is still
about 12x slower than the Rust/Arrow peer row and about 31x slower than noisy DuckDB at sf=1.

## Materialized profile gate

Unprofiled loop command:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_PROFILE_LOOP=8 \
  target/release/toron-tpch perf-run-one --engine rune --query q03 --mode strict --sf 1 \
    --run-index 0 --data-source parquet --data-dir target/tpch/data
```

Loop summary:

| field | value |
| --- | ---: |
| prepare_ms | 2.732 |
| warmup_execute_ms | 1,298.785 |
| loop_execute_ms | 9,142.015 |
| iterations | 8 |
| plan fingerprint | `2b72c561c2595512` |
| output rows | 11,620 |
| output schema | `fnv1a64:52ced4a401714e39` |
| spill_bytes_max | 0 |
| materialized_path | true |

This is the Rune q03 materialized path, not the sf=3 streaming reach path.

## Symbol gate and profile

Build settings: `CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_STRIP=false`, release.

Symbol gate:

```bash
readelf -S target/release/toron-tpch | rg -i 'debug|symtab'
nm -C target/release/toron-tpch | rg \
  'run_rune_q03_profile_loop|execute_bound_profile_iteration|execute_bound|rune_q03_logical_plan'
```

Verdict: pass. `.debug_*` sections and `.symtab` are present; profile-loop and executor symbols resolve.

Profile command:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_PROFILE_LOOP=8 \
  perf record -D 2000 -F 999 --call-graph dwarf \
    -o target/tpch/adr395_rune_q03_loop_only.perf.data -- \
    target/release/toron-tpch perf-run-one --engine rune --query q03 --mode strict --sf 1 \
      --run-index 0 --data-source parquet --data-dir target/tpch/data
```

The `-D 2000` delay skips prepare and the warmup. Perf reported `Events disabled` then `Events enabled`, and captured
8,681 samples. The profile loop under DWARF reported `loop_execute_ms=8,969.682`, versus the unprofiled loop
`loop_execute_ms=9,142.015`; no DWARF overhead inflation was observed.

Primary report section: `cpu_core/cycles/P` (8K samples). The small `cpu_atom/cycles/P` section had only 14 samples and
is not used for the verdict.

## Dominant frames

| frame / class | children | self | read |
| --- | ---: | ---: | --- |
| `__memmove_avx_unaligned_erms` | 14.48% | 12.54% | bulk Arrow/materialization copies |
| `_int_malloc` | 13.96% | 13.23% | allocator churn |
| `__libc_malloc2` | 11.07% | 2.88% | allocator entry, mostly `_int_malloc` |
| page faults / anonymous page allocation | 8.04% | 0.07% | allocation pressure, page initialization |
| `_int_free_chunk` | 7.51% | 1.75% | allocator free/merge churn |
| `_int_realloc` / `__libc_realloc` | 7.71% / 6.81% | 0.00% / 0.68% | growing buffers |
| `toron::exec::join::insert_join_build_row` | 5.73% | 5.73% | in-memory join build row insertion |
| `parquet::arrow::buffer::offset_buffer::OffsetBuffer<I>::extend_from_dictionary` | 5.73% | 5.73% | Parquet string/dictionary materialization |
| `arrow_select::filter::FilterBytes<OffsetSize>::extend_idx` | 2.88% | 2.88% | string filter materialization |
| `parquet::arrow::array_reader::primitive_array::coerce_i64` | 1.23% | 1.23% | Parquet decimal/int coercion |
| `arrow_select::take::take_bytes` | 0.98% | 0.98% | byte-array take/copy |
| `toron::exec::join::build_in_memory_join_rows` | 1.09% | 1.09% | join build loop |

Named substrate cost: q03 is allocation/copy/materialization dominated, not arithmetic dominated. The highest Toron-owned
site is join-build insertion, but the larger class is the materialize-each pipeline around Parquet decode -> Arrow
filter/take/concat -> hash-build row insertion, with allocator churn and memmove/page faults dominating.

## Next precision strike

ADR 396 should target q03 materialized allocation/copy pressure before broader execution redesign:

- measure allocation counts/bytes around the materialized q03 path (`execute_bound -> Vec`, scan/filter batches, join
  build insertion);
- reduce obvious batch-copy/allocation churn in the join build/materialization path, likely starting at
  `insert_join_build_row`/`build_in_memory_join_rows` and the string/dictionary/filter materialization boundary;
- re-measure `Rune/Polars` after the strike.

This is distinct from the sf=3 streaming reach thread. ADR 395 answers the completing-scale question: with a
hand-authored logical shape and no spill, Toron's materialized substrate is still materially slower than a Rust/Arrow
peer, and the cost is concentrated in data movement and allocation.

## Hygiene

- Diagnostic-only. No `crates/toron/src` changes.
- The profiling loop is off-by-default (`TORON_TPCH_PROFILE_LOOP`) and does not affect normal perf rows.
- `.perf.data` and generated reports are under `target/tpch/` and are not committed.
- No qNN special cases.
