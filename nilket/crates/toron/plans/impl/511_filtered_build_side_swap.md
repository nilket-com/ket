# impl 511 - filtered build-side swap plan

One bounded placement-guard relaxation, measured directly. No new estimators, no join-order change,
no spill-mechanism change.

## Immutable inputs

- ADR 508 clean q17 baseline: `632777908780f17bf15d1d7d43125b6e28f67d99`;
- fixed materiality floor: 3,554 ms (inherited, not recomputed);
- exact dataset: strict sf=8 persisted Parquet with `full_hash`;
- task memory: 2,147,483,648 B, explicit; spill reserve: 2,147,483,648 B, explicit;
- grounding facts this plan builds on: census `local_join_admissions` build estimate
  5,758,680,840 B on q17 join0; probe pair `Natural`/`HashByDefault` with the part filter vs
  `SwappedRightBuild`/`HashSmallerBuild { 240,000,000 vs 5,758,680,840 }` without it.

The grounding probes were transient; step 2 re-lands them as named regression tests.

## Step 1 - guard relaxation behind a typed config

In `hash_join_decision_with_placement` (`binder/join.rs:1194`), replace

```text
unknown_size = left.filtered_without_estimate || right.filtered_without_estimate
```

with a config-gated asymmetric decline: when `config.filtered_build_side_swap` is on, decline only
for `left.filtered_without_estimate`; when off, keep the current symmetric decline byte-for-byte.
A filtered-without-estimate right side compares on its raw byte bound. `comparison_scaled_bytes`
is untouched — without an estimate there is nothing to scale, and the raw bound is the
conservative comparator.

`PlannerConfig` gains `filtered_build_side_swap: bool`, compiled default **false** at the seal.
The comment on the field states the invariant, not the history: the swap toward an unestimated
filtered side is valid because bounds are true upper bounds and filters are volume-non-increasing,
so staleness on the build side only overstates it.

Gauge wiring mirrors `gauge_hash_smaller_build` exactly: `TORON_TPCH_FILTERED_BUILD_SIDE_SWAP`
with "0"/"1" explicit, absence = compiled default, anything else a loud error; latched once; every
persisted row carries `filtered_build_side_swap` and `filtered_build_side_swap_provenance`.

## Step 2 - tests

Unit trio on `choose_join_strategy` with the knob on:

- right side `filtered_without_estimate: true`, bounds 700 MiB vs 100 MiB → `SwappedRightBuild`,
  `HashSmallerBuild` with the raw right bound — fails without the relaxation;
- left side filtered — the existing `adr472_unknown_size_side_declines_the_swap` pin, unchanged;
- both sides filtered → `Natural`.

Knob-off invariance: the right-filtered shape above with `filtered_build_side_swap: false` keeps
`Natural` — the compiled default arm is exactly ADR 472 behavior.

Bind-level discriminating pair (re-landed grounding probes, `bind_with_trace` receipts):

- q17 shape — `lineitem, part` comma join, part filter `p_brand`/`p_container`, correlated scalar
  subquery on `l_partkey` — with seeded bounds 5,758,680,840 vs 240,000,000: knob on →
  `SwappedRightBuild` on join0; knob off → `Natural`;
- mirrored shape with the unestimated filter on the **left** side: `Natural` under both arms.

Harness: a `TORON_TPCH_FILTERED_BUILD_SIDE_SWAP` knob-contract test beside
`adr469_hash_smaller_build_knob_contract`.

Architecture: reconcile any guard scanning `filtered_without_estimate` or the placement decline;
the ADR retargets 472's "either side" prose to "the trusted-larger side".

## Step 3 - census diff and reached-shape correctness

From the sealed binary, run the q01-q22 and m00-m10 censuses in both arms at the exact coordinate.
Enumerate every cell whose plan fingerprint differs between arms; record route, spill observation,
admission receipts, and fingerprint per arm. No silent caps — the diff lists every changed cell,
not a preselected set.

Every changed cell runs its supported strict correctness rail in both arms with explicit
2 GiB / 2 GiB budgets. The q17 oracle rail supports sf=0.02 or sf=10, not sf=8; use its supported
coordinate and rely on the sf=8 gate invariants (digest, schema, row count) from the paired
children for the exact-coordinate result claim. q19 is a named sentinel and is correctness-run in
both arms whether or not its fingerprint changes.

Expected engagement (verified, not assumed): candidate q17 binds `SwappedRightBuild` on join0,
commits no `partitioned_streaming_scan_build` observation at replay 0, and its
`local_join_admissions` no longer records the 5,758,680,840 B decline.

## Step 4 - implementation seal

Seal steps 1-3 as one cohesive commit, run all gates, push, never amend. Build the release binary
once from the pushed identity via `scripts/cargo-cgroup build --release -p toron-tpch`. Reject any
measured row whose build is dirty or differs from the seal.

## Step 5 - pre-cohort gates

Run the ADR 469 plan-distinct gate at the exact coordinate with

```text
TORON_TPCH_MEMORY_PER_TASK=2147483648
TORON_TPCH_SPILL_BYTES_RESERVED=2147483648
```

and the paired switch `TORON_TPCH_FILTERED_BUILD_SIDE_SWAP` (`--paired-on 1 --paired-off 0`). The
arms must bind distinct plans; identical digest, schema, and output row count; `full_hash`.

Run one diagnostic on/off pair from the same binary with node attribution:

| diagnostic | control | candidate | candidate - control |
|---|---:|---:|---:|
| `join0_build` | | | |
| `join0_partition_plan` | | | |
| `join0_probe` | | | |
| join0 spill bytes | | | |
| admission receipts | | | |
| execute-bound span | | | |

The diagnostic pair explains transfer and verifies the decision fired; its times do not enter the
primary estimator.

## Step 6 - primary paired cohort

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
  --paired-env TORON_TPCH_FILTERED_BUILD_SIDE_SWAP \
  --paired-on 1 \
  --paired-off 0 \
  --paired-gate plan-distinct \
  --paired-materiality-floor-ms 3554 \
  --output-dir target/tpch/adr511_q17_paired_<build>
```

ADR 447 systematic adjacent ordering, durable per-row fsync, all 20 measured rows retained,
Hodges-Lehmann point and two-sided 95% Wilcoxon signed-rank interval.

Verdict:

| interval | point estimate | result |
|---|---|---|
| improving, excludes zero | at least 3,554 ms | activate |
| improving, excludes zero | below 3,554 ms | detectable, immaterial; close |
| includes zero | at least 3,554 ms | material, unresolved; close |
| includes zero | below 3,554 ms | close |

No post-hoc sample extension, alternate estimator, recomputed floor, or threshold negotiation.

## Step 7 - receipt and activation

Land a separate receipt commit citing the immutable implementation identity and artifact
directories: census diff with per-arm fingerprints/routes/admissions, changed-cell correctness
results, gate record, diagnostic pair, all raw rows, paired statistics, and verdicts.

If and only if the paired verdict is `activate` **and** no changed cell prices a material
regression (ADR 472's rule), the receipt flips `filtered_build_side_swap` to compiled default
true, then runs a clean default-arm q17 correctness smoke and the q19 sentinel. Otherwise
production keeps the symmetric decline and the receipt contains no mechanism pivot.

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

Repeat the applicable seal gate after any code change made in response. Finish all gates before
starting measurement.
