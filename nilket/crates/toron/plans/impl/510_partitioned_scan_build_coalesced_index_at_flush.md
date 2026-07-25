# ADR 510 implementation plan - coalesced indexed-at-flush direct ablation

This plan implements one query-neutral control/candidate lifecycle and measures it directly. It does
not add an estimator for rebuild reconstruction and does not select a fallback mechanism.

## Immutable inputs

Decision grounding:

- ADR 508 clean q17 baseline: `632777908780f17bf15d1d7d43125b6e28f67d99`;
- ADR 509 Phase 0 implementation: `a52b4773f4f7e04d184d78e80505753ecd757f23`;
- ADR 509 red receipt: `db12c395`;
- fixed materiality floor: 3,554 ms;
- exact dataset: strict sf=8 persisted Parquet with `full_hash`;
- task memory: 2,147,483,648 B, explicit;
- spill reserve: 2,147,483,648 B, explicit.

ADR 509's 64 MiB exploratory predecessors remain inadmissible for any time, partition, group, cache,
or resident figure.

## Step 1 - lifecycle and reach seal

### Split the merged diagnostic

Replace the node-attribution meaning of `write_partition_routing` with three hook-specific phases:

```text
write_exchange_repartition
write_active_group_partitioning
write_page_key_extraction
```

The correlated-scalar vocabulary predeclares variants with and without exchange before measurement.
Commit may select only among those fixed definitions. The active-group and page-key stages remain
required on every `PartitionedScanBuild` candidate/control profile.

Preserve q21's existing `GroupedChainProfile` schema by mapping the three hook identities back to its
legacy aggregate, or explicitly version that schema and update compatibility tests and its receipt.
No silent field disappearance or semantic rename.

Acceptance:

- q17's three phases plus the unchanged build refinements reconcile to `join0_build` inside its
  declared boundary, with exactly 4x + 4x + 1x row multiplicities;
- an exchange-free fixture does not require `write_exchange_repartition`;
- an exchange-bearing fixture fails if that stage is absent;
- q21's existing grouped profile still reconciles and remains reader-compatible.

The split is diagnostic hygiene only. It cannot gate materiality or predict the paired result.

### Route census

Enumerate every q01-q22 and m00-m10 cell whose committed `Join0ChunkRoute` is
`PartitionedScanBuild`. Record route, replay id, spill bytes, key plan, null-safe mode, and whether
exchange is structurally present.

Existing evidence requires at least q17 and m06. A newly observed cell expands correctness coverage,
not the q17 performance claim.

## Step 2 - typed candidate lifecycle

Add:

```rust
enum CoalescedPageIndexMode {
    RebuildAfterWrite,
    IndexAtFlush,
}
```

Latch `TORON_PARTITIONED_SCAN_BUILD_INDEX_AT_FLUSH` once at the route entry using explicit truthy
semantics. Explicit `0`/`false` always selects control; explicit `1`/`true` always selects candidate;
absence selects the compiled production default. Pass the typed mode through the route and writer;
do not reread the environment below the route.

For the candidate:

1. create every active `SpilledInnerJoinBuild`;
2. before its first write, select compact indexing iff the existing control rule would select it
   before rebuild;
3. pass `IndexAtFlush` through direct-full-page, buffered-full-page, and partial-tail flushes;
4. insert the page/row refs already computed by `write_page_batch_inner`;
5. run the existing checked resident guard after every page;
6. finish without changing page/file behavior;
7. assert index completeness and skip `rebuild_index_from_pages_profiled`.

For control, preserve current call order and behavior exactly: unindexed coalescing, finish, scratch
release, representation selection, rebuild.

The environment switch remains off by default. No automatic fallback is allowed after the first
candidate write.

### Writer tests

Pin both arms on:

- compact Int32 and Int64;
- Generic and null-safe legacy keys;
- duplicate keys and insertion order;
- null-skipping and all-null fragments;
- multiple fragments composing one page;
- fragments crossing a page boundary;
- a direct full page;
- a partial final page;
- multiple pages and at least two partitions;
- an empty partition and an empty build;
- identical page count, page-row vector, IPC/ciphertext lengths, and spill bytes;
- exact lookup multisets before any probe;
- candidate zero rebuild/read counts and control nonzero counts;
- candidate completeness at finish;
- typed guard failure during incremental index growth;
- injected file write, finish/fsync, index insert, and cleanup failures;
- empty scratch after success and every failure.

Mutation-check at least:

- candidate accidentally rebuilding;
- compact selection occurring after the first candidate page;
- one candidate flush still passing unindexed mode;
- candidate silently falling back to control after an insertion failure.

## Step 3 - live residency and reached-shape correctness

Extend ADR 509's exact-owner profile to observe the real candidate at each group:

- index bytes after every page and at finish;
- writer plaintext/ref high-water;
- page reserve and collection capacity;
- per-object current/peak cache;
- file/path/page metadata;
- production guard charge;
- literal task-budget charge;
- minimum headroom.

The candidate must retain 16 partitions / four groups on q17 and fit both the unchanged production
guard and literal 2 GiB task budget in every group. The control must reproduce ADR 509 geometry and
spill bytes.

Run every reached query or primitive through its supported strict correctness rail in both arms.
The q17 correctness rail supports sf=0.02 or sf=10, not sf=8; use its supported coordinate with
explicit 2 GiB / 2 GiB budgets, and rely on the exact-coordinate shape gate below for sf=8
result/route invariants. Do not widen the oracle policy for this ADR.

Any correctness, geometry, cleanup, or residency red closes the ADR. Do not build the paired rail to
negotiate around it.

## Step 4 - shape-preserving paired gate

ADR 469's custom-env gate requires distinct plans and must remain unchanged. Add an explicit gate
selection to the paired harness:

```text
PairedGate
    PlanDistinct
    ShapePreservingSpillIndex
```

The new gate runs one diagnostic child per arm with node attribution and mechanism counters. It
requires equal:

- build and `full_hash` dataset identity;
- plan fingerprint, route, and spill observation;
- digest, schema, and output row count;
- partition/group counts;
- page count, rows per page, ciphertext bytes, and whole-query spill;
- explicit task and spill budgets.

It requires opposite lifecycle facts:

```text
control:
    coalesced_index_mode = rebuild_after_write
    indexed_flush_pages = 0
    rebuild_calls > 0
    build_physical_read_pages > 0

candidate:
    coalesced_index_mode = index_at_flush
    indexed_flush_pages > 0
    rebuild_calls = 0
    build_physical_read_pages = 0
```

Missing, stale, nonnumeric, inverted, or same-arm evidence fails closed. Clear every diagnostic path
and join-spill scratch before each child.

Serialize into the paired journal's configuration lock:

- gate selection;
- environment variable plus exact on/off values;
- memory and spill budgets;
- strict sf=8 persisted-data coordinate;
- fixed floor policy and 3,554 ms value.

Resume must reject any mismatch. The existing plan-distinct gate and its tests remain unchanged.

## Step 5 - implementation seal

Seal Steps 1-4 as one cohesive implementation commit, run all gates, push it, and never amend it.
The clean pushed commit is the sole binary identity for measurement.

Complete all build and test work before starting a cohort. No Cargo wrapper or unrelated process may
compete with a perf child.

Build the release binary once from the pushed identity:

```bash
scripts/cargo-cgroup build --release -p toron-tpch
```

Reject any row whose build is dirty or differs from the seal.

## Step 6 - pre-cohort gates

Run the shape-preserving gate at strict sf=8 with:

```text
TORON_TPCH_MEMORY_PER_TASK=2147483648
TORON_TPCH_SPILL_BYTES_RESERVED=2147483648
```

Require `full_hash` and persist the complete gate row. Run the reached-shape correctness matrix and
retain join-spill cleanup evidence.

Run one diagnostic on/off pair from the same binary with ADR 509 node attribution. Record:

| diagnostic | control | candidate | candidate - control |
|---|---:|---:|---:|
| three write phases | | | |
| build reread/decrypt/decode | | | |
| rebuild CPU envelope | | | |
| probe reread/decrypt/decode | | | |
| build/probe cache hits and misses | | | |
| page count, decoded bytes, spill bytes | | | |
| `join0_build`, `join0_probe`, whole query | | | |
| minimum resident headroom | | | |

The diagnostic pair explains transfer and verifies lifecycle. Its times do not enter the primary
paired estimator.

## Step 7 - primary paired cohort

Use a fresh output directory and the configuration-locked journal:

```bash
TORON_TPCH_MEMORY_PER_TASK=2147483648 \
TORON_TPCH_SPILL_BYTES_RESERVED=2147483648 \
target/release/toron-tpch perf \
  --query q17 \
  --mode strict \
  --sf 8 \
  --data-source parquet \
  --data-dir target/tpch/data \
  --engines toron \
  --runs 10 \
  --warmup 1 \
  --paired-ablation \
  --paired-env TORON_PARTITIONED_SCAN_BUILD_INDEX_AT_FLUSH \
  --paired-on 1 \
  --paired-off 0 \
  --paired-gate shape-preserving-spill-index \
  --paired-materiality-floor-ms 3554 \
  --output-dir target/tpch/adr510_q17_paired_<build>
```

Use ADR 447's systematic adjacent ordering and durable per-row fsync. Record all 20 measured rows,
the discarded warmup, arm order, paired differences, Hodges-Lehmann estimate, and two-sided 95%
Wilcoxon signed-rank interval.

Verdict:

| interval | point estimate | result |
|---|---|---|
| improving, excludes zero | at least 3,554 ms | activate |
| improving, excludes zero | below 3,554 ms | detectable, immaterial; close |
| includes zero | at least 3,554 ms | material, unresolved; close |
| includes zero | below 3,554 ms | close |

No post-hoc sample extension, alternate estimator, recomputed floor, or threshold negotiation.

## Step 8 - receipt and activation

Land a separate receipt commit citing the immutable implementation identity and artifact directory.
It records:

- route census and reached-shape controls;
- all lifecycle, page, spill, cache, and residency invariants;
- shape-preserving gate record and configuration identity;
- all primary raw rows and paired statistics;
- fixed-floor and interval verdicts;
- join-spill cleanup;
- final production default.

If and only if the verdict is `activate`, the receipt commit flips the production default to the
exact candidate mode measured, then runs a clean default-arm q17 correctness smoke and every reached
shape control. If the verdict is anything else, production remains rebuild-after-write and the
receipt contains no mechanism pivot.

The implementation seal and measurement receipt are independently valuable and revertible; review
pivots inside either deliverable remain working-tree history rather than separate commits.

## Repository gates

Run every Cargo leg through `scripts/cargo-cgroup`, never raw `cargo`.

Council seal:

```bash
scripts/cargo-cgroup fmt -p toron -- --check
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
git diff --check
```

Changed-surface checks:

```bash
scripts/cargo-cgroup fmt -p toron-tpch -- --check
scripts/cargo-cgroup check -p toron --features telemetry --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup check -p toron-tpch --features polars-sql-reference --all-targets
```

Also run both mechanism arms through every reached shape's supported strict correctness rail with
explicit 2 GiB / 2 GiB budgets. Read the asserted command's own exit status; do not pipe a gate
through `tail` or a zero-match `grep`.

Repeat the applicable seal gate after any code change made in response. Finish all gates before
starting measurement.
