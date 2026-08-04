# ADR 396 results - q03 materialized allocation/copy precision strike

## Verdict

ADR 396 lands the first substrate precision strike from the ADR 395 four-way measurement. The
winning cut is narrow and behavior-neutral: the in-memory join build no longer allocates a
heap `Vec<u32>` for every duplicate join key on the q03 hot path. `BuildRowList` now keeps the
first row inline and stores up to six overflow rows inline before falling back to the heap.

This is the arena/locality idea from `brainstorms/004_memory_arenas_and_allocators.md` applied
only where the profile justified it: indexed row storage at the execution hot spot, no allocator
dependency, no arena lifetime threaded through the engine.

## Phase 0 attribution

ADR 395 named the q03 materialized substrate cost as allocation/copy/materialization dominated.
The highest Toron-owned site was the join build (`build_in_memory_join_rows` /
`insert_join_build_row`). ADR 396 added an off-by-default join-build profile hook to verify that
site before changing behavior.

The pre-strike profile loop showed:

| metric | pre-strike |
|---|---:|
| build rows | 3,241,776 |
| unique keys | 829,958 |
| duplicate-key rows | 700,918 |
| overflow rows | 2,411,818 |
| max rows per key | 7 |
| overflow heap capacity bytes | 15,663,640 |
| row-list value bytes | 19,918,992 |
| join-build micros | 262,646 |

That shape made the local cut obvious: q03's measured maximum is one first row plus six overflow
rows, so an inline overflow capacity of six removes the measured per-key overflow heap allocation
without changing row order or join semantics.

Several plausible alternatives were measured and rejected before the final cut:

| experiment | outcome |
|---|---|
| compact persisted PBC scan batch | Correct, but no timing win on q03 sf=1. |
| compact enum row-list representation | Correct, but slower than baseline. |
| O(1) join-build memory guard counter | Exact guard parity, but no median win. |
| XxHash64 build hasher | No median win. |

Those experiments were not kept. The committed strike is the inline overflow storage plus the
off-by-default profile telemetry used to prove it.

## Implementation

`BuildRowList` moved to `exec/join/build_rows.rs` and now uses:

- `first: u32` for the first matching build row.
- `BuildRowOverflow::Inline { rows: [u32; 6], len }` for the measured q03 duplicate shape.
- `BuildRowOverflow::Heap(Vec<u32>)` only when a key exceeds the inline bound.

Iteration preserves the original insertion order. The heap fallback keeps correctness for
higher-duplication joins that do not match q03's measured shape. Join column helpers were also
split into `exec/join/columns.rs` to keep the architecture source-size ratchet green.

The profile hook records `JoinBuildProfile` through task telemetry only when
`TORON_JOIN_BUILD_PROFILE=1`; default execution is unchanged.

## Measurements

Primary source-matching run:

`target/tpch/perf_adr396_q03_sf1_inline_rowlist_source_final`

| metric | ADR 395 baseline | ADR 396 |
|---|---:|---:|
| Rune q03 sf=1 persisted median | 1339.751785 ms | 1135.070092 ms |
| Rune q03 sf=1 min | - | 1102.162446 ms |
| spread | - | 0.062938 |
| peak RSS | - | 1,162,644 KiB |
| spill bytes | 0 | 0 |
| output rows | 11,620 | 11,620 |
| plan fingerprint | 2b72c561c2595512 | 2b72c561c2595512 |

Result: the stable Rune materialized q03 path improves by 204.7 ms, a 15.3% reduction.

Profile-loop counter run:

`target/tpch/adr396_q03_rune_loop_inline_rowlist_source_final.json`

| metric | pre-strike | ADR 396 | change |
|---|---:|---:|---:|
| profile loop execute | 9142.015 ms | 8427.516041 ms | -7.8% |
| join-build micros | 262,646 | 181,547 | -30.9% |
| overflow heap capacity bytes | 15,663,640 | 0 | eliminated |
| row-list value bytes | 19,918,992 | 33,198,320 | inline trade |
| max rows per key | 7 | 7 | unchanged |
| spill bytes max | 0 | 0 | unchanged |

The `row_list_value_bytes` increase is the intended trade: the duplicate-row storage moves from
many small heap vectors into inline hash-map values. Peak RSS stayed within the same band, while
the struck-site allocation/copy cost dropped.

Four-engine sf=1 persisted run:

`target/tpch/perf_adr396_q03_sf1_inline_rowlist_fourway_source_final`

| engine | status | median | spread | flags |
|---|---|---:|---:|---|
| toron | pass | 2796.770959 ms | 0.010 | none |
| rune | pass | 1074.074234 ms | 0.032 | none |
| polars_sql | pass | 105.735575 ms | 0.136 | none |
| duckdb | pass | 46.0 ms | 0.652 | unstable, schema_mismatch |

Trust DuckDB-denominator ratios directionally only; DuckDB remains below the sf=1 stability floor.
The Polars denominator is stable and value-gated.

| ratio | ADR 395 | ADR 396 |
|---|---:|---:|
| Rune / Polars | 12.41x | 10.16x |
| Toron / Polars | 29.64x | 26.45x |
| SQL / Rune | 2.39x | 2.60x |
| Rune / DuckDB | directional | 23.35x |
| Polars / DuckDB | directional | 2.30x |
| Toron / DuckDB | directional | 60.8x |

ADR 396 reduces the stable Rune/Polars substrate ratio by 18.1% relative to ADR 395. It does not
close the gap; it proves the first precision strike can move the measured substrate number.

## Correctness and gates

Correctness and compatibility gates:

- `q03` persisted correctness, strict sf=0.01: pass.
- Polars q03 value gate, sf=1 persisted: pass (`max_revenue_diff_raw_scale4=222`, tolerance 1000;
  11,620 rows).
- `BuildRowList` focused tests: pass.
- Architecture test: pass (2079/0).
- `toron --no-fail-fast`: pass.
- `toron-tpch --features polars-sql-reference --no-fail-fast`: pass.
- Release `toron-tpch --features polars-sql-reference` rebuild: pass.
- `cargo fmt -- --check`: pass.

No cap changed, no spill was introduced, and no qNN-branded branch was added to core execution.

## Forward

The next substrate strike should keep the same discipline: use the four-way ratio as the score,
attribute allocation/copy to a Toron-owned producer, change one producer, then re-measure
Rune/Polars. The obvious remaining candidates from ADR 395 are upstream Arrow/string
materialization and join probe/output materialization, not another generic attack on `memmove` or
the allocator.
