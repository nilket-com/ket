# ADR 401 results - SQL/Rune planner-shape lever

ADR 401 found and fixed the q03 SQL/Rune planner-shape delta that was safe to
fix generically: SQL comma/CROSS normalization left side-local predicates above
the lifted equi join for scan-side children. SQL now pushes those predicates
only through proven-transparent child wrappers and into scan/Cross leaves before
rebinding the lifted join, which exposes the same filtered scan-side shape Rune
already had without crossing limit/window/project-alias safety boundaries.

## Implementation

Touched code:

- `crates/toron/src/binder/outer_join_demotion.rs`
- `crates/toron/src/binder/tests.rs`
- `crates/toron-tpch/src/lib.rs`
- `crates/toron-tpch/src/main.rs`
- `crates/toron-tpch/Cargo.toml`

Planner rule:

- `push_into_cross_child` carries original and routed conjunct forms, so a
  remapped predicate can still return to the parent residual in the original
  scope if a later boundary blocks pushdown.
- It descends through exchanges, distincts, unbounded sorts, direct-column
  projects, identity aliases, and guard-safe child Filters; it stops at Take,
  bounded/offset sorts, windows, repeated inputs, aggregates, and opaque leaves.
- Pre-existing child Filters split incoming side-local conjuncts only when the
  existing child predicate and the incoming conjunct are both safe to reorder.
  This lets safe conjuncts continue through safe Filters so a nested Cross can
  still receive an equi key, while preserving eager hard-error fences for
  arithmetic division, strict casts, and other guard-sensitive predicates on
  either side of the child Filter.
- Scan leaves still get the side-local filter early, preserving the ADR 401 q03
  selectivity lever. Nested Cross children still receive side-local bridge
  predicates and can lift their own equi joins.

This is generic comma/CROSS predicate placement. There is no q03 branch in core
planner or binder code.

Harness evidence:

- `rune_q03_isolation_proof_with_data_source` supports memory and persisted
  Parquet data sources.
- `q03-plan-proof` prints persisted sf=1 proof JSON with fingerprints, compact
  shapes, join decisions, join-order edges, table row/byte facts, and a computed
  optimizer-evidence gap summary.
- The Polars reference path uses lazy Parquet scans again. Enabling the Polars
  streaming feature fixes the q03 collect panic and keeps the adapter as a lazy
  Parquet reference instead of an eager full-table measurement.

## Persisted Proof

Artifact: `target/tpch/adr401_q03_plan_proof_sf1.json`.

Command:

```bash
target/release/toron-tpch q03-plan-proof \
  --sf 1 \
  --mode strict \
  --data-source parquet \
  --data-dir target/tpch/data \
  > target/tpch/adr401_q03_plan_proof_sf1.json
```

Proof row:

| field | value |
|---|---|
| data source | `parquet` |
| manifest | `fnv1a64:29596fd55c6078ac` |
| SQL plan fingerprint | `d1d275a0a8181d73` |
| Rune plan fingerprint | `666e0ac028036f70` |
| SQL decisions | `#0 hash_partitioned natural hash_by_default, #1 hash_partitioned natural hash_by_default` |
| Rune decisions | same |
| SQL edges | `customer.c_custkey = orders.o_custkey`, then `orders.o_orderkey = lineitem.l_orderkey` |
| Rune edges | `c.c_custkey = o.o_custkey`, then `o.o_orderkey = l.l_orderkey` |
| table facts | lineitem `rows=6001215 bytes=855082112`; customer `rows=150000 bytes=41014504`; orders `rows=1500000 bytes=199886136` |
| optimizer gap | `sql_join_order_rows_missing=true; rune_join_order_rows_missing=true; join_order_edge_fields=join_index,kind,probe,dim,left_keys,right_keys,rows,linearity; missing_evidence=post_filter_cardinality,multi_relation_cost` |

The compact shapes now both place q03 side-local filters before the joins:

- `c_mktsegment = 'BUILDING'` under customer scan;
- `o_orderdate < date '1995-03-15'` under orders scan;
- `l_shipdate > date '1995-03-15'` under lineitem scan.

## Shape Delta Table

| aspect | layer | before | after | verdict |
|---|---|---|---|---|
| data path | harness | q03 proof was memory-only | q03 proof binds persisted Parquet sf=1 | closed |
| filter placement | binder comma/CROSS normalization | SQL kept scan-side predicates as parent residuals after lifted equi recognition | SQL pushes side-local predicates through safe wrappers into scan/Cross leaves before rebinding | closed |
| join order | binder join-order policy boundary | SQL reached policy without the same child filter placement | SQL and Rune expose equivalent q03 edges | closed for classified delta |
| build side / strategy | binder strategy choice | no SQL-vs-Rune build-side delta found | both joins remain `hash_partitioned natural hash_by_default` | unchanged |
| projection timing | physicalized plan | no q03-specific projection delta found after ADR 397 | compact shapes keep equivalent projected scan columns | no action |
| exchange placement | SQL grouped aggregate lowering | SQL has `Aggregate(... input=Exchange(parts=4,keys=3,input=Join...))`; Rune aggregates directly over the final join | unchanged | stop-line residual |

## Stop-Lines

The remaining q03 SQL/Rune structural difference is the SQL grouped-aggregate
exchange immediately under `Aggregate`. Removing it as a q03 heuristic would
cut across distributed grouped-aggregate correctness and existing architecture
coverage. The missing lever is a generic local-vs-distributed aggregate
placement policy with correctness proof.

The optimizer evidence gap is also explicit: persisted table row/byte facts are
available, but the current join-order explain edge rows still report
`rows=None`. That means the proof does not claim post-filter cardinality or
multi-relation cost evidence exists.

## Post-Review Fence Fix

Codex review found the inverse eager-error fence bug: the staged rule prevented
guard-sensitive incoming predicates from crossing a child `Filter`, but still
allowed an otherwise-safe incoming predicate to cross an existing error-prone
child `Filter`, which could suppress hard errors on rows filtered out by the
pushed predicate. The fix only lets incoming conjuncts cross a child `Filter`
when the existing child predicate and the incoming conjunct are both safe to
reorder. `comma_join_safe_conjunct_stays_above_error_prone_child_filter` locks
that case.

Post-review correctness and perf were rerun after this fix. The correctness row
passed, and the same-machine perf probe showed the intended SQL/Rune shape
improvement.

Baseline build:

```bash
rm -rf /tmp/nilket-adr401-base-ws && \
git worktree add --detach /tmp/nilket-adr401-base-ws 9f6a376

CARGO_TARGET_DIR=/home/me/work/nilket/target/adr401_base_ws \
CARGO_BUILD_JOBS=12 \
CARGO_CGROUP_JOBS=12 \
CARGO_CGROUP_BACKEND=affinity \
CARGO_CGROUP_CPUS=0-11 \
CARGO_PROFILE_RELEASE_DEBUG=true \
CARGO_PROFILE_RELEASE_LTO=thin \
  /home/me/work/nilket/scripts/cargo-cgroup build -p toron-tpch --release
```

Baseline q03 sf=1 perf command:

```bash
rm -rf target/tpch/perf_adr401_q03_sf1_base_workstation && \
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/1.5.4/duckdb \
  /home/me/work/nilket/target/adr401_base_ws/release/toron-tpch perf \
    --sf 1 \
    --mode strict \
    --runs 7 \
    --warmup 1 \
    --query q03 \
    --engines toron,rune,duckdb \
    --data-source parquet \
    --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr401_q03_sf1_base_workstation \
  > target/tpch/adr401_q03_sf1_base_workstation.out
```

Post-review q03 sf=1 perf command:

```bash
rm -rf target/tpch/perf_adr401_q03_sf1_post_fence_workstation && \
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
    --engines toron,rune,duckdb \
    --data-source parquet \
    --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr401_q03_sf1_post_fence_workstation \
  > target/tpch/adr401_q03_sf1_post_fence_workstation.out
```

Artifact:
`target/tpch/perf_adr401_q03_sf1_post_fence_workstation/latest_summary.jsonl`.

Baseline artifact:
`target/tpch/perf_adr401_q03_sf1_base_workstation/latest_summary.jsonl`.

| engine | baseline median | post-review median | post-review spread | fingerprint after | flags after |
|---|---:|---:|---:|---|---|
| toron | 1121.087 ms | 669.843 ms | 0.013 | `d1d275a0a8181d73` | none |
| rune | 384.076 ms | 376.201 ms | 0.021 | `666e0ac028036f70` | none |
| duckdb | 42.000 ms | 41.000 ms | 0.439 | `fnv1a64:73b142150e59739f` | `unstable,schema_mismatch` |

SQL/Rune moved from `2.92x` to `1.78x` on the same machine and same profile.
DuckDB is context only for this row because the short sf=1 DuckDB run is
unstable.

The table above is the same-machine before/after row. The Polars-enabled table
below is a separate post-fix four-engine run; Toron and Rune differ slightly
between the two post-fix artifacts but preserve the same shape and conclusion.

Polars-enabled post-review q03 sf=1 perf command:

```bash
rm -rf target/tpch/perf_adr401_q03_sf1_post_fence_polars_workstation && \
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
    --output-dir target/tpch/perf_adr401_q03_sf1_post_fence_polars_workstation \
  > target/tpch/adr401_q03_sf1_post_fence_polars_workstation.out
```

Artifact:
`target/tpch/perf_adr401_q03_sf1_post_fence_polars_workstation/latest_summary.jsonl`.

| engine | median | spread | peak RSS | fingerprint | flags |
|---|---:|---:|---:|---|---|
| toron | 679.639 ms | 0.020 | 832084 KiB | `d1d275a0a8181d73` | none |
| rune | 378.148 ms | 0.041 | 576576 KiB | `666e0ac028036f70` | none |
| polars_sql | 101.989 ms | 0.156 | 905124 KiB | `fnv1a64:a3b5c15847833ac3` | none |
| duckdb | 40.000 ms | 0.825 | 151808 KiB | `fnv1a64:73b142150e59739f` | `unstable,schema_mismatch` |

Stable peer-reference ratios from this artifact:

- SQL/Rune: `1.80x`.
- SQL/Polars: `6.66x`.
- Rune/Polars: `3.71x`.
- Polars is the most variable passing engine in this artifact (`spread=0.156`)
  but still passes the value gate and has no harness flags.
- Polars/DuckDB: `2.55x`, context only because DuckDB is unstable at this short
  sf=1 run.

## Polars Reference Status

Feature check and tests pass after enabling the optional Polars streaming
executor needed by lazy SQL collect:

```text
scripts/cargo-cgroup check -p toron-tpch --features polars-sql-reference --all-targets
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
cargo tree -p toron-tpch --features polars-sql-reference -i polars-ooc
cargo tree -p toron-tpch --features polars-sql-reference -i polars-stream
```

The dependency inspection now intentionally shows `polars-stream` and its
transitive `polars-ooc` dependency under the optional `polars-sql-reference`
feature. They are not part of default `toron-tpch` builds.
Because streaming is enabled at the optional Polars reference feature level,
future Polars reference rows for other queries may shift; each banked row still
needs its own value gate or status classification.

Runtime Polars q03 is banked in the post-review evidence. The value gate passes
on the Rust adapter:

```text
{"data_source":"parquet","engine":"polars_sql","engine_version":"0.54.4","max_revenue_diff_raw_scale4":222,"mode":"strict","plan_fingerprint":"fnv1a64:a3b5c15847833ac3","query":"q03","revenue_tolerance_raw_scale4":1000,"rows":11620,"scale_factor":1.0,"schema_fingerprint":"fnv1a64:e6f7a214e444a631","status":"pass","value_gate":"oracle_decimal_tolerance_and_q03_order"}
```

The earlier eager-registered four-engine artifact
`target/tpch/perf_adr401_q03_sf1_feature_thin/latest_summary.jsonl` is
superseded by the lazy streaming artifact above. Its Polars row included full
all-column table materialization and a different timing window, so it is not
used for ADR 401 comparison.

## Correctness And Siblings

Persisted sf=1 correctness:

```text
tpch_correctness query=q03 sf=1 mode=strict phase=compare status=pass reason=none
tpch_correctness query=q05 sf=1 mode=strict phase=compare status=pass reason=none
tpch_correctness query=q10 sf=1 mode=strict phase=compare status=pass reason=none
```

Shape coverage:

- `sql_comma_join_pushes_local_conjunct_before_lifted_equi` covers the generic
  comma-join rule and verifies the filtered dimension can drive an estimated
  broadcast decision.
- `comma_join_side_local_conjunct_does_not_cross_take` fences side-local
  predicates above `Take`.
- `comma_join_side_local_conjunct_stays_above_child_filter_guard` fences
  side-local arithmetic predicates above an existing child Filter guard.
- `comma_join_safe_conjunct_stays_above_error_prone_child_filter` fences an
  otherwise-safe side-local predicate above an existing error-prone child
  Filter, preventing the pushed predicate from suppressing hard errors.
- `comma_join_safe_equi_conjunct_crosses_child_filter_guard_to_key_nested_cross`
  verifies guard-safe side-local equality can still cross a child Filter and key
  a nested comma join.
- `comma_join_alias_child_recurses_to_nested_cross` verifies alias remapping can
  reach and key a nested Cross.
- `comma_join_project_child_remaps_only_passthrough_conjuncts` verifies direct
  project passthrough is remapped and computed project outputs stay residual.
- `rune_q03_persisted_isolation_proof_uses_persisted_path` asserts persisted
  q03 data source, manifest, fingerprints, optimizer table facts, computed
  evidence gap, matching join decisions, matching filter placement, and the
  residual aggregate exchange.
- `strict_q05_stays_keyed_at_default_scale` and
  `strict_q10_stays_keyed_at_default_scale` guard sibling keyed shape.

## Gates

Passed after the post-review planner and harness edits:

```text
scripts/cargo-cgroup fmt -- --check
git diff --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron comma_join_ --lib
scripts/cargo-cgroup test -p toron --test architecture_test adr212_residuals_preserve_original_form -- --nocapture
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup check -p toron-tpch --features polars-sql-reference --all-targets
scripts/cargo-cgroup test -p toron-tpch --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch rune_q03_persisted_isolation_proof_uses_persisted_path --lib
cargo tree -p toron-tpch --features polars-sql-reference -i polars-ooc
cargo tree -p toron-tpch --features polars-sql-reference -i polars-stream
find . -name '*.perf.data' -print
```

Polars q03 probes:

```text
scripts/cargo-cgroup run -p toron-tpch --features polars-sql-reference -- polars-q03-value-gate --sf 1 --mode strict --data-source parquet --data-dir target/tpch/data
systemd-run --user --same-dir --wait --collect --quiet --pipe --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/1.5.4/duckdb target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data --output-dir target/tpch/perf_adr401_q03_sf1_post_fence_polars_workstation
```

The value gate exits `0`, and the perf row records
`engine=polars_sql status=pass execute_ms_median=101.989`.

## Hygiene

- No cap bump: perf ran under `MemoryMax=4G`, `MemoryHigh=4G`, `TasksMax=512`.
- No q03 branch in core planner or binder code.
- No `.perf.data` file was produced by the ADR 401 runs.
- Scope honesty: this closes the fixable SQL/Rune q03 planner-shape delta. It
  is not an sf=10 heavy-cell fix, and the grouped-aggregate exchange remains a
  named follow-up.
