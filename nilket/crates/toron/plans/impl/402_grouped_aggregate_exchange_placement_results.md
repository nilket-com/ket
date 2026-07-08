# ADR 402 results - grouped-aggregate exchange placement

## Summary

Implemented the generic binder normalization from the 402 plan: a pre-aggregate hash `Exchange` is removed only when
the aggregate input is already structurally co-located for the group keys. The q03 SQL path now matches the Rune q03
aggregate shape on the persisted sf=1 path; Rune remains unchanged.

This is not a local-only shortcut and not a cardinality/cost rule. The proof is structural:

- `Exchange(parts=1, ...)` proves single-partition input.
- `Exchange(parts=N, key columns)` proves hash co-location by those columns.
- `Inner` + `HashPartitioned` join proves co-location by each equi-key pair; either side of a join key may satisfy a
  group key because the join condition equates them.
- `Filter`/`Take` preserve the proof; simple column `Project` remaps it; other shapes do not prove it.

## Phase 0

Baseline current seal (`a7800fc`, pre-402 implementation), q03 sf=1 persisted, release, fixed 4 GB:

| engine | median | spread | fingerprint | status |
|---|---:|---:|---|---|
| toron SQL | 656.965 ms | 0.016 | `d1d275a0a8181d73` | pass |
| rune | 372.891 ms | 0.028 | `666e0ac028036f70` | pass |
| duckdb | 41.000 ms | 0.293 | `fnv1a64:73b142150e59739f` | pass, unstable/schema_mismatch |

Probe results:

| probe | SQL median | Rune median | SQL/Rune | SQL fingerprint | verdict |
|---|---:|---:|---:|---|---|
| baseline `parts=4` | 656.965 ms | 372.891 ms | 1.76x | `d1d275a0a8181d73` | residual present |
| `parts=1` aggregate exchange | 733.463 ms | 426.639 ms | 1.72x | `8d9c831811d35c1c` | not closure; slower |
| true pre-aggregate elision | 412.465 ms | 393.303 ms | 1.05x | `666e0ac028036f70` | material and shape-closing |

The `parts=1` probe validated the plan tightening: it is not a no-op, because the executor still evaluates keys and
repartitions. Only true elision closes the shape.

Elidability classification: **safe with a structural co-location proof**, not with row/cardinality facts. q03's final
join is hash-partitioned by the `orders`/`lineitem` order-key equi-key pair; the aggregate groups by `l_orderkey` plus
order attributes, so each group is already co-located. No partial/final aggregate was required.

## Implementation

Changed `crates/toron/src/binder.rs` and added `crates/toron/src/binder/aggregate_exchange.rs`:

- Added a private `PlanPartitioningProof` helper in the extracted binder submodule.
- `bind_bound_aggregate` now normalizes its already-bound input through
  `collapse_redundant_pre_aggregate_exchange`.
- The collapse fires only when the exchange key is covered by the aggregate group columns and the exchange input's
  structural partitioning proof is also covered by the group columns.
- The binder architecture ratchet was updated to include the new semantic submodule; `binder.rs` remains under the
  ADR 176 line budget.

The rule is generic and de-branded: no q03/table/query names in core logic.

## Shape Proof

Persisted q03 sf=1 proof after implementation:

| item | value |
|---|---|
| SQL fingerprint | `666e0ac028036f70` |
| Rune fingerprint | `666e0ac028036f70` |
| `plans_differ` | `false` |
| SQL aggregate input | `Join(...)` |
| Rune aggregate input | `Join(...)` |

Artifact: `target/tpch/adr402_q03_plan_proof_impl.json`.

## Performance

Post-implementation q03 sf=1 persisted, release, fixed 4 GB:

| engine | median | spread | fingerprint | status |
|---|---:|---:|---|---|
| toron SQL | 398.532 ms | 0.058 | `666e0ac028036f70` | pass |
| rune | 399.133 ms | 0.037 | `666e0ac028036f70` | pass |
| duckdb | 39.000 ms | 0.462 | `fnv1a64:73b142150e59739f` | pass, unstable/schema_mismatch |

Primary result:

- SQL absolute median improved `656.965 -> 398.532 ms` (`-39.3%`).
- SQL/Rune closed `1.76x -> 1.00x`.
- Rune remained the same shape and same correctness digest; its median varied within the short sf=1 run band.
- DuckDB remains context-only at sf=1 because the row is unstable and carries the known schema mismatch flag.

Perf command:

```bash
rm -rf target/tpch/perf_adr402_q03_sf1_impl && \
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
    --output-dir target/tpch/perf_adr402_q03_sf1_impl \
  > target/tpch/adr402_q03_sf1_impl.out
```

Artifact: `target/tpch/perf_adr402_q03_sf1_impl/latest_summary.jsonl`.

## Correctness / Regression

Targeted tests:

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron grouped_aggregate_ --lib
scripts/cargo-cgroup test -p toron-tpch q03_isolation --lib
```

The binder tests include both sides of the correctness gate and the sibling `Distinct` path:

- `grouped_aggregate_drops_exchange_when_input_already_colocated_by_group_key`
- `grouped_aggregate_keeps_exchange_without_colocation_proof`
- `grouped_aggregate_keeps_exchange_after_keyless_multi_partition_input`
- `distinct_drops_exchange_when_input_already_colocated`

TPC-H correctness controls:

| query | sf | status |
|---|---:|---|
| q03 | 0.01 | pass@compare |
| q05 | 0.01 | pass@compare |
| q10 | 0.01 | pass@compare |

The q03 proof tests were updated from "SQL/Rune must differ" to "SQL/Rune aggregate exchange residual is gone";
persisted q03 now proves exact shape closure.

Full gates:

```text
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch --no-fail-fast
```

Results:

| gate | status |
|---|---|
| `fmt -- --check` | pass |
| `check -p toron --all-targets` | pass |
| `toron --test architecture_test` | 2079 passed |
| `toron --no-fail-fast` | pass |
| `toron-tpch --no-fail-fast` | pass |

## Scope

402 closes the q03 SQL/Rune grouped-aggregate exchange residual on the measured sf=1 path. It does not implement
two-phase aggregation and does not address the sf=10 heavy-cell execution-model/time wall. The next big lever remains
the execution-model / streaming-completion thread, now with q03's logical-shape gap essentially closed.
