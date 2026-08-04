# ADR 399 results - post-397 q03 substrate strike

ADR 399 re-profiled the post-397 Rune q03 materialized path and found a narrow
Toron-owned cause in the join-build hash path. The implemented strike keeps the
same row-list representation from ADR 396, but changes the private in-memory
join-build maps from the default `HashMap` hasher to a per-map keyed `ahash`
state.

This is a behavior-neutral substrate strike, not a planner or reach change:

- plan fingerprint stays `666e0ac028036f70`;
- `materialized_path=true`;
- `spill_bytes_max=0`;
- output row count stays `11620`;
- q03 sf=1 persisted correctness passes.

## Phase 0 - cause ledger

Command artifacts:

- `target/tpch/adr399_q03_rune_loop_phase0.json`
- `target/tpch/adr399_rune_q03_loop_phase0.perf.data`
- `target/tpch/adr399_rune_q03_phase0_perf_report_flat.txt`
- `target/tpch/adr399_rune_q03_phase0_perf_report_graph.txt`

Baseline loop:

| item | value |
|---|---:|
| loop iterations | 8 |
| iteration median | 391.392 ms |
| loop total | 3148.186 ms |
| join build time | 158043 us |
| build rows | 3241776 |
| unique keys | 829958 |
| duplicate keys | 700918 |
| overflow rows | 2411818 |
| max rows per key | 7 |
| overflow heap capacity | 0 bytes |
| row-list value bytes | 33198320 |
| key plan | int64 |

Classification: hash path. The build rows were already preallocated and ADR 396's
small-inline row lists removed the duplicate-overflow heap. The remaining hot
hash-path frames pointed at key hashing and map entry work, not row-list growth:

| phase0 frame | self/core-cycle share |
|---|---:|
| `build_in_memory_join_rows` | 11.81% |
| `hashbrown::rustc_entry` | 8.27% |
| `<core::hash::sip::Hasher<S> as Hasher>::write` | 4.90% |
| `core::hash::BuildHasher::hash_one` | 4.45% |
| `HashMap::get` | 3.45% |
| `record_join_build_profile` | 2.97% |

Stop-lines checked:

- map growth was not the strike: `build_in_memory_join_rows` preallocates for input rows;
- duplicate overflow heap was not the strike: ADR 396 reports `overflow_heap_capacity_bytes=0`;
- memory-guard overhead was not the strike: it did not dominate the profile;
- generic `memmove` was not the strike: no copy primitive was targeted without an owning producer.

## Implementation

The cut adds a private join-build map type:

```rust
pub(in crate::exec) type JoinBuildRows = HashMap<JoinKey, BuildRowList, RandomState>;
```

`new_join_build_rows(capacity)` constructs the map with `ahash::RandomState::new()`,
so each map is keyed independently. This is intentionally not a fixed hasher.
The maps are executor-internal, per-task structures, but their keys still come
from table data, so the implementation avoids a deterministic public hash
surface.

Touched surfaces:

- `crates/toron/src/exec/join/build_rows.rs`
- `crates/toron/src/exec/join.rs`
- `crates/toron/src/exec/join/build_profile.rs`
- `crates/toron/src/exec/join/streaming.rs`
- `crates/toron/Cargo.toml`
- `Cargo.lock`
- `crates/toron/tests/architecture/binder_decomposition.rs`

The architecture allow-list entry is for `crates/toron/Cargo.toml` only; this ADR
adds a direct executor dependency, but no runtime, wire, storage, Wendelon,
descriptor, optimizer-fact, or frontend surface.

## Results

Post-strike loop artifacts:

- `target/tpch/adr399_q03_rune_loop_after.json`
- `target/tpch/adr399_q03_rune_loop_after_perf.json`
- `target/tpch/adr399_rune_q03_loop_after.perf.data`
- `target/tpch/adr399_rune_q03_after_perf_report_flat.txt`
- `target/tpch/adr399_rune_q03_after_perf_report_graph.txt`

Loop movement:

| metric | before | after | change |
|---|---:|---:|---:|
| loop iteration median | 391.392 ms | 355.823 ms | -9.1% |
| loop total | 3148.186 ms | 2854.905 ms | -9.3% |
| join build time | 158043 us | 130190 us | -17.6% |
| overflow heap capacity | 0 bytes | 0 bytes | unchanged |
| output rows | 11620 | 11620 | unchanged |
| spill bytes | 0 | 0 | unchanged |

The profile confirms the target moved:

- `core::hash::BuildHasher::hash_one` and `core::hash::sip::Hasher::write` no
  longer appear in the post-strike top report;
- `hashbrown::rustc_entry` remains hot, which is expected: the map still does
  hash-table entry work, just without the default SipHash cost;
- `build_in_memory_join_rows` remains a residual producer, now mixed with
  remaining hash-table work and materialization/copy costs.

Post-strike formal sf=1 four-way:

Artifact: `target/tpch/perf_adr399_q03_sf1_full/latest_summary.jsonl`.

| engine | status | median | spread | peak RSS KiB | rows | spill | flags |
|---|---|---:|---:|---:|---:|---:|---|
| toron | pass | 1147.625 ms | 0.018 | 1333932 | 11620 | 0 | none |
| rune | pass | 395.397 ms | 0.026 | 576700 | 11620 | 0 | none |
| polars_sql | pass | 105.787 ms | 0.147 | 898128 | 11620 | 0 | none |
| duckdb | pass | 39.000 ms | 0.333 | 148712 | 11620 | 0 | unstable, schema_mismatch |

Ratios:

| ratio | value | note |
|---|---:|---|
| Rune / Polars | 3.74x | peer-substrate gap; Polars stable enough for context |
| Toron / Polars | 10.85x | end-to-end peer gap |
| SQL / Rune | 2.90x | logical-shape penalty after substrate strike |
| Rune / DuckDB | 10.14x | directional; DuckDB denominator unstable at sf=1 |
| Polars / DuckDB | 2.71x | directional; DuckDB denominator unstable at sf=1 |
| Toron / DuckDB | 29.43x | directional; DuckDB denominator unstable at sf=1 |

Compared with ADR 397's formal Rune median (`410.468 ms`), ADR 399 moves the
banked Rune q03 materialized path by `15.072 ms` (`-3.7%`). The loop and join
profile show the targeted producer moved more sharply; the full query median
movement is intentionally recorded as a modest first hash-path strike, not a
large substrate jump.

## Value gate

The Rust Polars explicit-join path remains value-compatible on the same sf=1
persisted data:

- artifact: `target/tpch/adr399_polars_q03_value_gate.json`;
- rows: `11620`;
- comparator: independent q03 oracle with q03 ordering;
- max revenue diff: `222` raw scale-4 units;
- tolerance: `1000` raw scale-4 units.

Toron q03 persisted correctness also passes:

```text
tpch_correctness query=q03 sf=1 mode=strict phase=compare status=pass reason=none elapsed_ms=2744.4
```

## sf=10 reach signal

The cheap post-strike sf=10 Rune q03 probe did not complete:

- service: `run-p3344227-i15886893.service`;
- result: `timeout`;
- wall: `6min 264ms`;
- CPU: `5min 57.372s`;
- memory peak: `4G`;
- no JSON perf row was emitted.

This is not a regression and not a green. ADR 399 improves the sf=1 materialized
substrate path, but q03 sf=10 remains time/no-row under the bounded window.

## Reproduction commands

These are intentionally recorded because the scoped benchmark commands are easy
to lose in shell history. Rust build/test commands below cap rustc to 12 cores on
this 24-core machine.

Build the release binary with symbols for profile attribution:

```bash
CARGO_PROFILE_RELEASE_DEBUG=true \
CARGO_PROFILE_RELEASE_STRIP=false \
CARGO_CGROUP_BACKEND=affinity \
CARGO_CGROUP_JOBS=12 \
CARGO_CGROUP_CPUS=0-11 \
scripts/cargo-cgroup build -p toron-tpch --features polars-sql-reference --release
```

Run the q03 Rune profile loop:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --setenv=TORON_TPCH_PROFILE_LOOP=8 \
  --setenv=TORON_JOIN_BUILD_PROFILE=1 \
  target/release/toron-tpch perf-run-one \
    --engine rune \
    --query q03 \
    --mode strict \
    --sf 1 \
    --run-index 0 \
    --data-source parquet \
    --data-dir target/tpch/data \
  > target/tpch/adr399_q03_rune_loop_after.json
```

Record the post-strike profile:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --setenv=TORON_TPCH_PROFILE_LOOP=16 \
  --setenv=TORON_JOIN_BUILD_PROFILE=1 \
  perf record -D 1000 -F 999 --call-graph dwarf \
    -o target/tpch/adr399_rune_q03_loop_after.perf.data \
    -- target/release/toron-tpch perf-run-one \
      --engine rune \
      --query q03 \
      --mode strict \
      --sf 1 \
      --run-index 0 \
      --data-source parquet \
      --data-dir target/tpch/data \
  > target/tpch/adr399_q03_rune_loop_after_perf.json
```

Render the profile reports:

```bash
perf report --stdio \
  -i target/tpch/adr399_rune_q03_loop_after.perf.data \
  --no-children \
  --sort symbol \
  > target/tpch/adr399_rune_q03_after_perf_report_flat.txt

perf report --stdio \
  -i target/tpch/adr399_rune_q03_loop_after.perf.data \
  --sort symbol \
  > target/tpch/adr399_rune_q03_after_perf_report_graph.txt
```

Run the four-way sf=1 perf rail:

```bash
rm -rf target/tpch/perf_adr399_q03_sf1_full && \
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/1.5.4/duckdb \
  target/release/toron-tpch perf \
    --sf 1 \
    --mode strict \
    --runs 7 \
    --warmup 1 \
    --query q03 \
    --engines toron,rune,polars_sql,duckdb \
    --data-source parquet \
    --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr399_q03_sf1_full \
  > target/tpch/adr399_q03_sf1_perf_full.out
```

Run the sf=10 reach signal probe:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --property=RuntimeMaxSec=360s \
  target/release/toron-tpch perf-run-one \
    --engine rune \
    --query q03 \
    --mode strict \
    --sf 10 \
    --run-index 0 \
    --data-source parquet \
    --data-dir target/tpch/data \
  > target/tpch/adr399_q03_rune_sf10_probe.json \
  2> target/tpch/adr399_q03_rune_sf10_probe.err || true
```

## Verification

Commands run:

```bash
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup test -p toron join_build --lib
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --features polars-sql-reference --all-targets
target/release/toron-tpch correctness --query q03 --sf 1 --mode strict --data-source parquet
CARGO_CGROUP_BACKEND=affinity CARGO_CGROUP_JOBS=12 CARGO_CGROUP_CPUS=0-11 scripts/cargo-cgroup test -p toron --no-fail-fast
CARGO_CGROUP_BACKEND=affinity CARGO_CGROUP_JOBS=12 CARGO_CGROUP_CPUS=0-11 scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
```

Results:

- targeted join-build tests pass, including unique inline rows, duplicate
  overflow order, null skipping, generic key order, and preallocation;
- architecture passes (`2079` tests);
- `toron --no-fail-fast` passes;
- `toron-tpch --features polars-sql-reference --no-fail-fast` passes;
- q03 sf=1 persisted correctness passes.

## Forward

ADR 399 removed the default SipHash cost from the private join-build map path and
kept the four-way substrate scorecard moving, but the residual q03 substrate gap
is still real:

- hash-table entry/probe work remains hot;
- Parquet RLE/dictionary decode and copy/materialization remain visible;
- sf=10 q03 remains time/no-row.

The next substrate step should re-profile the post-399 path before cutting. A
valid ADR 400 target needs the same cause-level discipline: name the
Toron-owned producer behind the residual frame first, then strike that producer
or seal as a diagnostic if no narrow behavior-neutral lever exists.
