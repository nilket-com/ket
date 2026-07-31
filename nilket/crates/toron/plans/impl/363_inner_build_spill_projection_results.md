# ADR 363 Results - Inner-Build Spill Projection

## Verdict

**Reduced the old spill-byte wall; q21 did not green.**

ADR 362's first sf=10 wall was `spill_budget_exhausted` at producer
`streamed inner join build spill writer`, 536,872,016 B requested against the unchanged 512 MiB budget
(536,870,912 B). ADR 363 routes the streamed reoriented-inner scan build through projected spill pages when downstream
operators do not need the full build schema.

The live sf=10 row no longer fails at the spill-byte producer. It advances to a different named wall:

```text
BudgetExhausted { dimension: Memory, producer: "streamed inner join build spill resident state",
requested: ByteCount(1073753480), available: ByteCount(1073741824) }
```

That is 11,656 B over the 1 GiB retained-state guard. Because this guard fires during spill construction, the run does
not prove final total spill bytes yet; it proves the prior spill-byte wall is no longer the first wall. The result is an
honest next-wall, not a pass claim.

## Phase 0 Measurement

Baseline from ADR 362:

| query | sf | status | reason | producer | requested | available |
| --- | ---: | --- | --- | --- | ---: | ---: |
| q21 | 10 | fail | `spill_budget_exhausted` | `streamed inner join build spill writer` | 536872016 | 536870912 |

The responsible edge was the streamed reoriented inner build side from ADR 362: `JoinReplayId(1).right` through
`Exchange(Scan orders)`. ADR 363 handles both direct scan and exchange/repartitioned scan branches through the same
projected writer, so the branch is no longer special-cased.

Per-lever classification:

- **Column projection:** measured-reducible for the spill-byte wall. The streamed inner build was written as the full
  build schema even though downstream semi/anti, group, aggregate, join, and residual expressions only need a subset.
- **Page/layout:** still relevant after the projection: the next wall is retained spill resident state, not final spill
  bytes.
- **Dedup-before-spill:** not a local lever. As in ADR 348, early dedup would require a separate mechanism; this cut
  does not introduce one.

## Implementation

- Propagates required output columns backward through the shape-named reoriented inner stages.
- Retains build join keys plus columns referenced by later filters, semi/anti preserved keys, residuals, group keys,
  and aggregate arguments.
- Uses `SpilledInnerJoinBuild::write_projected_batch_chunks` for streamed scan builds when retained columns are narrower
  than the full build schema.
- Remaps build-key ordinals into the projected schema before constructing `JoinKeyPlan`.
- Carries projection metadata in the spilled runtime and reconstructs ordinal-compatible full build chunks on probe.
  Dropped build columns are restored only as sparse default arrays; real values are present for retained observable
  columns.
- Accounts those sparse default arrays in the projected materialization budget.

The implementation stays topology-named; no `qNN` / `QNN` references were added under `crates/toron/src/exec*`.

## Focused Regression Tests

Added:

- `projected_reoriented_inner_runtime_remaps_keys_and_restores_observable_schema`

The test covers the projected streamed-spilled reoriented runtime over an `Exchange(Scan)` build side:

- proves scan build batches are written separately, not concatenated;
- proves projected pages omit a dead payload column;
- remaps build keys into the projected schema;
- compares retained output columns against the unprojected materialized runtime for both `Float64` and `Decimal128`;
- verifies the observable schema/order is restored and dropped payload columns are sparse defaults.

Existing non-projected coverage remains green:

- `reoriented_inner_runtime_streams_scan_build_batches_and_preserves_order`

## Live Results

Strict correctness:

```text
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01
tpch_correctness query=q21 sf=0.01 mode=strict phase=compare status=pass reason=none elapsed_ms=5088.7
```

```text
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 1
tpch_correctness query=q21 sf=1 mode=strict phase=compare status=pass reason=none elapsed_ms=227561.3
```

sf=10 persisted perf, fixed 4 GiB cap, release:

```text
target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 --query q21 \
  --engines toron --data-source parquet --data-dir target/tpch/data --output-dir target/tpch/perf_adr363

tpch_perf engine=toron query=q21 status=fail execute_ms_median=1482695.2 execute_ms_min=1482695.2 \
  spread=0.000 peak_rss_kib=1806864 spill_bytes=0 flags=none
```

Raw diagnostic:

```text
reason=memory_budget_exhausted
diagnostic_message=BudgetExhausted { dimension: Memory, producer: "streamed inner join build spill resident state",
requested: ByteCount(1073753480), available: ByteCount(1073741824) }
```

Peak RSS stayed bounded at about 1.72 GiB under the fixed 4 GiB cap. The remaining wall is not a cgroup kill and not
the old 512 MiB spill-byte producer.

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron projected_reoriented_inner_runtime_remaps_keys_and_restores_observable_schema -- --nocapture
scripts/cargo-cgroup test -p toron reoriented_inner_runtime_streams_scan_build_batches_and_preserves_order -- --nocapture
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup build -p toron-tpch --release
```

Observed counts:

- `toron --lib`: 1166 passed.
- `architecture_test`: 2079 passed.
- `toron --no-fail-fast`: passed, including the integration tests that caught the ADR 361 regression family.
- `toron-tpch`: passed.

Other checks:

```text
rg -n "q21|Q21|\bq[0-9]{2}\b|\bQ[0-9]{2}\b" crates/toron/src/exec crates/toron/src/exec.rs -g '*.rs'
# no matches

rg -n "spill_bytes_reserved_per_query|512 \* 1024 \* 1024" crates/toron/src crates/toron-tpch/src
crates/toron/src/distributed/budget.rs:97: spill_bytes_reserved_per_query: ByteCount::new(512 * 1024 * 1024),
```

## Forward

q21's sf=10 frontier moved from spill-byte volume to retained spill resident state. The next cut should target that
named producer directly: reduce or partition the streamed inner build resident state without raising the 1 GiB
retained-state guard or the 512 MiB spill-byte budget. The overage is small (11,656 B), but the fix should still be a
real bounded-state change, not a cap adjustment.
