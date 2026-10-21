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

## Measured outcome

### Immutable seal and artifacts

Steps 1-4 were sealed and pushed as `ac5e870861f4`. All evidence below was produced by the release
binary built from that clean, unamended identity after the repository gates completed.

| evidence | artifact |
|---|---|
| q01-q22 production-route census | `target/tpch/adr510_route_census_ac5e8708/` |
| m00-m10 ladder census | `target/tpch/adr510_ladder_census_ac5e8708/` |
| production m06 control/candidate | `target/tpch/adr510_m06_{control,candidate}_ac5e8708/` |
| reached legacy m06 control/candidate | `target/tpch/adr510_m06_reached_{control,candidate}_ac5e8708/` |
| q17 gate, diagnostic pair, and cohort | `target/tpch/adr510_q17_paired_ac5e8708/` |

Every persisted row carries `dataset_verification = full_hash`, explicit 2 GiB task and spill
budgets, release profile, and `build_commit = ac5e870861f4`.

### Reach and correctness

The sf=8 production census completed 22/22 q-series cells. q17 is the only production cell whose
committed spill observation is `partitioned_streaming_scan_build`; q11, q14, and q19 are the only
other cells with a stamped join-spill path, and all three are `in_memory`.

The primitive census corrects the plan's inherited reach premise. After ADR 492, production m06
honors its physical build and commits `in_memory` at the 2 GiB coordinate. It is not a second
production-reached ADR 510 cell. The m00-m10 ladder nevertheless passed in both Toron and the
one-thread DuckDB reference. Re-running m06 with ADR 492's documented control
`TORON_TPCH_PHYSICAL_SCAN_BUILD=0` re-exposes the legacy partitioned shape. Both ADR 510 arms then
passed with identical digest, schema, one-row count, plan fingerprint, replay-0
`partitioned_streaming_scan_build` route, and 88,648,880 spill bytes. This is compatibility
coverage, not an m06 performance claim.

Strict q17 correctness at the supported sf=0.02 oracle coordinate passed in both mechanism arms.
The exact sf=8 gate below supplies the persisted result, route, geometry, and lifecycle invariants
that the q17 oracle rail deliberately does not admit.

### Shape-preserving gate

The live gate passed before the first cohort child. Both arms had the same digest, schema, one-row
count, plan fingerprint, route, 622,544,968 spill bytes, 16 partitions, four groups, 6,059 pages,
page-row vector, ciphertext-size vector, and explicit budgets.

| lifecycle fact | control | candidate |
|---|---:|---:|
| mode | `rebuild_after_write` | `index_at_flush` |
| indexed flush pages | 0 | 6,059 |
| checked guards after indexed flush | 0 | 6,059 |
| index rebuild calls | 16 | 0 |
| build physical-read pages | 6,059 | 0 |
| live guard after every indexed flush | n/a | true |

The diagnostic split reproduced exactly 191,956,028 exchange rows, 191,956,028 active-group rows,
and 47,989,007 page-key rows: `4x + 4x + 1x`. Node time and node spill each reconcile with zero
residual in both arms, with zero interleaved frames, bridge mismatches, or stage mismatches.

Residency passed in all four groups. The minimum literal task-budget headroom was 1,958,014,137 B
for control and 1,991,231,697 B for candidate. `target/tpch/join_spill` was empty after the gate and
after the cohort.

### Diagnostic transfer

The gate pair is explanatory only; its timings do not enter the paired estimator.

| diagnostic | control | candidate | candidate - control |
|---|---:|---:|---:|
| `join0_build` | 19,356.351 ms | 18,470.573 ms | -885.778 ms |
| `join0_probe` | 1,000.475 ms | 1,027.437 ms | +26.962 ms |
| execute-bound span | 34,844.363 ms | 34,018.714 ms | -825.649 ms |
| build physical read | 303.612 ms | absent / zero pages | -303.612 ms |
| build decrypt | 108.819 ms | absent | -108.819 ms |
| build IPC decode | 636.016 ms | absent | -636.016 ms |
| build index-rebuild CPU envelope | 6,260.442 ms | absent | -6,260.442 ms |

The removed rebuild leaves do not become a like-sized net saving. The `join0_build` refinement
residual expands from 468.625 ms in control to 7,260.937 ms in candidate. That residual is an
envelope, not a component: indexed-at-flush moves retained insertion work into the write lifecycle,
but the diagnostic does not separately price insertion there. The direct paired result below is
therefore the authority on net movement.

There is no observed cache-transfer penalty. The control build has 6,059 decoded and plaintext
cache misses while the candidate build has zero build reads. Both probe arms retain exactly 45,150
decoded-cache hits, 6,059 decoded-cache misses, and 6,059 plaintext-cache misses. Probe physical
read, decrypt, and decode times remain close: 129.569/90.512/582.179 ms in control and
138.491/92.206/591.342 ms in candidate.

### Locked paired cohort

The journal contains one discarded warmup and exactly 20 measured rows: 10 candidate and 10
control. All 21 rows are passing, full-hash, clean `ac5e870861f4` rows with one shared digest,
schema, plan, route, spill total, and budget coordinate.

| pair | candidate | control | control - candidate |
|---:|---:|---:|---:|
| 1 | 33,850.697 ms | 34,564.042 ms | 713.345 ms |
| 2 | 33,354.442 ms | 34,962.683 ms | 1,608.241 ms |
| 3 | 33,660.109 ms | 34,195.269 ms | 535.160 ms |
| 4 | 33,161.851 ms | 35,124.330 ms | 1,962.479 ms |
| 5 | 33,768.168 ms | 34,437.300 ms | 669.132 ms |
| 6 | 33,673.773 ms | 34,585.356 ms | 911.583 ms |
| 7 | 33,708.523 ms | 34,774.118 ms | 1,065.594 ms |
| 8 | 33,206.738 ms | 57,522.572 ms | 24,315.834 ms |
| 9 | 45,591.324 ms | 38,042.522 ms | -7,548.802 ms |
| 10 | 35,687.826 ms | 36,914.158 ms | 1,226.331 ms |

The predeclared paired result is:

```text
Hodges-Lehmann point        = 1,068.957 ms
two-sided 95% interval      = [-2,970.280, 12,425.497] ms
fixed materiality floor     = 3,554 ms
interval excludes zero      = false
point reaches floor         = false
activate                    = false
```

The generic unpaired summary pools both mechanisms and flags the resulting 0.706 spread as
`unstable`, so it correctly refuses to write a normal performance baseline. That flag does not
replace or invalidate the configuration-locked paired analysis; the two large late-run excursions
are retained in the predeclared estimator and are why its interval remains unresolved. There is no
post-hoc sample extension.

### Closure

ADR 510 closes red. Indexed-at-flush is correct, shape-preserving, and comfortably resident, but
the measured point is below the fixed floor and the interval includes zero. Production therefore
remains `RebuildAfterWrite`; no default flip or successor-mechanism pivot is included in this
receipt.
