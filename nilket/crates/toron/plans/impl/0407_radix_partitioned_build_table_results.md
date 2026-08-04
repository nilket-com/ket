# ADR 407 results - radix-partitioned build table

Status: diagnostic stop-line. No engine source change is shipped.

ADR 407 tested the build-only radix follow-on named by ADR 405/406. Phase 0 confirmed the
primitive join table is a real cache-heavy producer, but the build-only radix candidate did not
bank a q03 sf=1 median improvement. The local candidate was reverted; the source tree remains on
the 406 primitive table.

## Phase 0 - cache-bound proof

Symbol gate used a separate profiling target because the normal release build is stripped:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe --nice=19 \
  --property=CPUQuota=300% --property=MemoryHigh=8G --property=MemoryMax=12G \
  --property=TasksMax=512 --property=IOWeight=10 \
  taskset -c 0-3 ionice -c 3 env \
  CARGO_TARGET_DIR=target/adr407_profile CARGO_BUILD_JOBS=4 RUST_TEST_THREADS=1 \
  CARGO_TERM_PROGRESS_WHEN=never CARGO_PROFILE_RELEASE_DEBUG=true \
  CARGO_PROFILE_RELEASE_STRIP=false CARGO_PROFILE_RELEASE_LTO=false \
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
  cargo build -p toron-tpch --features polars-sql-reference --release
```

`file`, `readelf -S`, and `nm -C` confirmed debug info, `.symtab`, and resolved
`PrimitiveBuildTable<K>::get`, `build_in_memory_join_rows`, and
`run_rune_q03_profile_loop`.

Cache stat command:

```bash
systemd-run --user --scope -p MemoryMax=4G -p MemoryHigh=4G -p TasksMax=512 \
  perf stat -e cache-misses,llc-load-misses,l1-dcache-load-misses,instructions,cycles \
  -- env TORON_TPCH_PROFILE_LOOP=16 TORON_JOIN_BUILD_PROFILE=1 \
     target/adr407_profile/release/toron-tpch perf-run-one --engine rune --query q03 \
       --mode strict --sf 1 --run-index 0 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr407_q03_phase0_cache_stat.out 2>&1
```

Key Phase 0 counters:

| metric | value |
|---|---:|
| loop time | 5,628 ms / 16 q03 executes |
| join build time | 122,864 us / execute |
| build rows | 3,241,776 |
| unique keys | 829,958 |
| duplicate keys | 700,918 |
| max rows per key | 7 |
| overflow heap | 0 bytes |
| row-list value bytes | 33,198,320 |
| key plan | `int64` |
| cache misses | 308,253,334 |
| LLC load misses | 74,757,306 |
| L1 dcache load misses | 388,066,613 |
| instructions | 40,459,326,352 |
| cycles | 29,691,956,260 |

Delayed user-space cycles profile:

```bash
systemd-run --user --scope -p MemoryMax=4G -p MemoryHigh=4G -p TasksMax=512 \
  perf record -D 2000 -F 999 -e cycles:u --call-graph dwarf \
  -o target/tpch/adr407_q03_phase0_user_delayed.perf.data -- env \
  TORON_TPCH_PROFILE_LOOP=32 TORON_JOIN_BUILD_PROFILE=1 \
  target/adr407_profile/release/toron-tpch perf-run-one --engine rune --query q03 \
    --mode strict --sf 1 --run-index 0 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr407_q03_phase0_user_delayed.out 2>&1
```

Top relevant user-space self frames:

| frame | self share | verdict |
|---|---:|---|
| `JoinBuildRows::insert` | 28.53% | primitive-table build path |
| `PrimitiveBuildTable<K>::get` | 6.87% | primitive-table probe path |
| `build_in_memory_join_rows` | 2.74% | build loop |
| `RleDecoder::get_batch_with_dict` | 7.54% + 2.81% | decode residual |
| `__memmove_avx_unaligned_erms` | 4.72% self / 7.36% children | copy residual |
| `record_join_build_profile` | 0.99% | measurement overhead |

Cache-miss sampled profile:

```bash
systemd-run --user --scope -p MemoryMax=4G -p MemoryHigh=4G -p TasksMax=512 \
  perf record -D 2000 -e cache-misses:u -c 100000 --call-graph dwarf \
  -o target/tpch/adr407_q03_phase0_cache_misses.perf.data -- env \
  TORON_TPCH_PROFILE_LOOP=32 TORON_JOIN_BUILD_PROFILE=1 \
  target/adr407_profile/release/toron-tpch perf-run-one --engine rune --query q03 \
    --mode strict --sf 1 --run-index 0 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr407_q03_phase0_cache_misses.out 2>&1
```

Top relevant cache-miss self frames:

| frame | self share | verdict |
|---|---:|---|
| `JoinBuildRows::insert` | 21.75% | build path is cache-miss-heavy |
| `PrimitiveBuildTable<K>::insert<i64>` | 8.27% | inlined build insert |
| `PrimitiveBuildTable<K>::find_bucket<i64>` | 5.44% | bucket probe inside build |
| `PrimitiveBuildTable<K>::get` | 6.51% | probe path |
| `__memmove_avx_unaligned_erms` | 20.22% | separate copy/materialization residual |
| `arrow::array::primitive_array::PrimitiveArray<T>::unary` | 6.02% | Arrow residual |

Classification: the primitive join table is a real cache-heavy producer, so the Phase 0 gate
allowed a build-only radix candidate. Probe and copy/decode residuals were also visible, so ADR 407
remained scoped to build-only and carried the build+probe/output-order boundary as a follow-on.

## Local candidate

The unshipped candidate added a generic radix wrapper over `PrimitiveBuildTable<K>`:

- `primitive_radix_partition_count(estimated_bytes)`:
  - minimum build estimate: 64 MiB;
  - target partition size: 8 MiB;
  - maximum partitions: 64;
  - formula: `next_power_of_two(ceil(estimated_bytes / target_partition_bytes))`, clamped.
- production seam: `build_in_memory_join_rows`;
- first version: non-null count pass, partition histogram pass, insert pass;
- tightened version: non-null count and partition histogram combined when the initial and
  non-null-derived partition counts match;
- memory guard: `max(old row-count estimate, radix table allocation estimate)`;
- probe remained in probe-row order; no probe partitioning.

The local candidate passed the targeted build-row regression suite:

```text
CARGO_BUILD_JOBS=12 cargo test -p toron build_rows --lib
21 passed; 0 failed
```

Important: this candidate was reverted after measurement. The patch is preserved only as an ignored
artifact at `target/tpch/adr407_rejected_radix.diff` for local reference.

## q03 sf=1 measurement

Baseline used the existing pre-radix `target/release/toron-tpch` binary, whose timestamp predates
the source edits. Post rows used rebuilt release binaries from the local candidate.

Command shape:

```bash
rm -rf target/tpch/perf_adr407_q03_sf1_base && \
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=$(command -v duckdb) \
  target/release/toron-tpch perf \
    --sf 1 \
    --mode strict \
    --runs 7 \
    --warmup 1 \
    --query q03 \
    --engines toron,rune,polars_sql,duckdb \
    --data-source parquet \
    --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr407_q03_sf1_base \
  > target/tpch/adr407_q03_sf1_base.out
```

Same command with `_post` and `_post2` output directories for the two local candidate rows.

| row | engine | median | spread | flags | fingerprint |
|---|---|---:|---:|---|---|
| base | toron | 372.561 ms | 0.047 | none | `666e0ac028036f70` |
| base | rune | 366.506 ms | 0.032 | none | `666e0ac028036f70` |
| base | polars_sql | 111.157 ms | 0.333 | `unstable` | `fnv1a64:a3b5c15847833ac3` |
| base | duckdb | 45.000 ms | 0.422 | `unstable,schema_mismatch` | `fnv1a64:73b142150e59739f` |
| post | toron | 376.015 ms | 0.028 | none | `666e0ac028036f70` |
| post | rune | 376.141 ms | 0.042 | none | `666e0ac028036f70` |
| post | polars_sql | 106.816 ms | 0.137 | none | `fnv1a64:a3b5c15847833ac3` |
| post | duckdb | 56.000 ms | 0.875 | `unstable,schema_mismatch` | `fnv1a64:73b142150e59739f` |
| post2 | toron | 387.183 ms | 0.127 | none | `666e0ac028036f70` |
| post2 | rune | 381.326 ms | 0.064 | none | `666e0ac028036f70` |
| post2 | polars_sql | 102.891 ms | 0.222 | `unstable` | `fnv1a64:a3b5c15847833ac3` |
| post2 | duckdb | 37.000 ms | 0.486 | `unstable,schema_mismatch` | `fnv1a64:73b142150e59739f` |

Verdict: build-only radix did not beat the runs=7 band. The cleaner first post row is essentially
flat to slightly slower; the tightened second post row is slower and noisier. SQL/Rune shape stayed
closed (`666e0ac028036f70`) and correctness digests matched, so this is an execution-mechanism
non-bank rather than a shape/correctness issue.

## Stop-line

Do not ship build-only radix for the current primitive table.

Evidence:

- Phase 0 was right that the primitive table is cache-miss-heavy.
- The tested build-only radix mechanism adds histogram/pre-count work and an extra partition
  routing step.
- For q03 sf=1, that overhead did not pay back as a median win.
- Probe-side cache misses and copy/decode residuals remain visible; build-only radix is too narrow
  to move the user-visible loop.

Named follow-on: if this thread is reopened, it should be a broader build+probe/output-aware design
that can partition both build and probe or otherwise change the hash-table representation without
adding isolated build overhead. That is an execution-model cut, not a small 407 continuation. The
probe-order restoration and `matched_build_rows` semantics are the load-bearing design issues.

## Verification

Run before reverting the local candidate:

```text
CARGO_BUILD_JOBS=12 cargo check -p toron --lib
CARGO_BUILD_JOBS=12 cargo test -p toron build_rows --lib
```

Final shipped state:

- no engine source changes;
- no `.perf.data` staged or committed;
- q03 artifacts under `target/tpch/perf_adr407_q03_sf1_{base,post,post2}`;
- candidate patch kept only under ignored `target/tpch/adr407_rejected_radix.diff`.
- `target/release/toron-tpch` was last built from the rejected candidate; rebuild release from
  clean source before using it as any future baseline.

## Seal

Seal as diagnostic:

```text
impl: adr 407 radix partitioned build table
```
