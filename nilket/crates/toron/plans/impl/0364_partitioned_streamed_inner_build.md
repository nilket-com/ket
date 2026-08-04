# ADR 364 - Implementation plan: partitioned streamed inner-build spill (q21)

Pairs with `plans/adr/0364_partitioned_streamed_inner_build.md`. A `crates/toron` engine cut: a narrow partitioned
streamed-inner-build path reusing the 349 primitives + 363's projected materialization, bounding q21's resident index.
One active partition at a time; cap-matched partition count; no cap bump; no shared substrate; engine-cut seal gate.

## Phase 0 - resident terms + partition topology (the go/no-go)
- Measure q21's reoriented inner-build resident (env-gated): **index bytes, writer/page state, retained inputs, build
  map, concat/input copy**. Confirm `index_bytes` dominant + which terms are data-proportional (the 11,656 B over 1 GB
  is the index just over the resident-state cap).
- **Record the partition topology:** default **re-scan once per active partition** (only one partition's index
  resident). If a one-pass staging design is proposed instead, **prove it does not hold all partition indexes
  resident** before adopting it.
- **Cap source:** partition count via `streamed_spill_partition_count(estimated_build_bytes, CAP)` where `CAP` is the
  **effective resident-state cap** that `guard_resident_state` compares against
  (`join_spill_retained_footprint_memory_cap(task_budget.memory.get())`, ~1 GB for q21), not the raw 64 MB task-memory
  input - unless Phase 0 documents the stricter target as intentional.
- Record the column sets (363 projection) + the build-key remap that the partitioned path must preserve.

## D1 - the narrow partitioned streamed-inner-build path
- Partition by key hash: `streamed_spill_partition_count` (cap-matched) + `select_join_partition_batch` per batch.
- Per active partition: build only that partition's **projected** `SpilledInnerJoinBuild` (363's
  `write_projected_batch_chunks` + key-remap), probe, materialize matches with 363's schema-restore/column-order, then
  **drop the partition** before the next. Re-scan the source per partition (default) so only one partition's index +
  build map + input is resident.
- **Inner-join semantics via 363's materialization** - do NOT route through the LeftOuter visitor (null-fill).
- Topology-named symbols; no q21/qNN in core. No shared substrate.

## Tests / safety net
- **Strict correctness:** q21 `pass@compare` at sf=0.01 and sf=1.
- **Dev parity (not the command):** partitioned inner build == un-partitioned inner build, dev (Float64) mode, sf=0.01
  (digest/batch equality).
- **Partitioned-build correctness:** a synthetic build spanning multiple partitions -> same matches/answer + column
  order as the single-partition (363) path.
- **Resident bound:** a synthetic build larger than one partition -> only one partition's index resident at a time
  (assert no full-build index / no all-partition-index residency); peak resident bounded vs the build size.
- **sf=10-durability:** partition count grows with build size so per-partition resident stays bounded (a 10x-build
  test, mirroring 349's `streamed_spill_partition_count_grows...`).
- **Cap source:** a test that the partition count is computed against the resident-state cap, not 64 MB.
- **De-branding guard:** no q21/qNN in core executor.

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # MANDATORY (engine cut, ADR 361 lesson)
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01   # strict
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 1      # strict
# dev parity is the focused unit test above, NOT `correctness --mode dev`.
# sf=10, fixed 4 GB, persisted surface, live: q21's resident-state wall GONE (green) or named next wall:
#   systemd-run --user --property=MemoryMax=4G --setenv=TORON_TPCH_DUCKDB_BIN=<duckdb> ... \
#     target/release/toron-tpch perf --sf 10 --mode strict --runs N --warmup K --query q21 --engines toron \
#     --data-source parquet --data-dir target/tpch/data --output-dir target/tpch/perf_adr364
git diff --check
#   partitioned streamed inner build; one partition resident; cap-matched; q21 green or named next wall;
#   strict + dev parity; --no-fail-fast green; no q21 in core; no cap bump
```

## Results doc
`crates/toron/plans/impl/0364_partitioned_streamed_inner_build_results.md`: Phase-0 resident-term breakdown + the
partition topology (re-scan-per-partition vs proven-bounded one-pass) + the cap source; the narrow partitioned path
(reused primitives + 363 materialization, not the LeftOuter visitor); q21 sf=10 before/after (resident bytes; green or
named next wall via 332); the resident-bound + sf=10-durability evidence (one partition resident, partition count grows
with build); strict `pass@compare` (sf=0.01/sf=1) + the dev parity-test result; confirmation no cap bump, `--no-fail-fast`
green, no q21 in core, no shared substrate.

## Risk / blast radius
- **Visitor mis-reuse** - inner semantics via 363's materialization, not the LeftOuter null-fill visitor.
- **All-partition-index residency** - default re-scan-per-partition; one-pass only if proven bounded.
- **Cap mismatch** - partition count sized to the resident-state cap, tested.
- **Projection/key-remap drift** - strict + dev parity + the partitioned-build correctness test.
- **Regression** - `--no-fail-fast` mandatory; the empty-batch/schema family already green-guarded.
- **No shared substrate** - reuse primitives only; factor after a third caller.

## Seal
`impl: adr 364 partitioned streamed inner build`. Do not commit wip drafts.
