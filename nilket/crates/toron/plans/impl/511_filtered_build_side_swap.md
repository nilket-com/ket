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

## Measured outcome

### Immutable seal and artifacts

Steps 1-3 were sealed and pushed as `085704eb18c7`. All evidence below was produced by the release
binary built from that clean, unamended identity after the repository gates completed with direct
exit statuses (full `--no-fail-fast` suite exit 0, toron-tpch exit 0, lib 1,428 ok, full
architecture_test 2,093 ok after two named reconciliations: the adr176 binder-root bound moved
6,120 → 6,125 for the new placement knob, and the knob comment was worded to keep the adr132
PlannerConfig ban intact).

| evidence | artifact |
|---|---|
| q01-q22 route census, both arms | `target/tpch/adr511_route_census_{off,on}_085704eb/` |
| m00-m10 ladder census, both arms | `target/tpch/adr511_ladder_census_{off,on}_085704eb/` |
| diagnostic pair with node attribution | `target/tpch/adr511_q17_diag_{off,on}_085704eb/` |
| plan-distinct gated 10-pair cohort | `target/tpch/adr511_q17_paired_085704eb/` |

Every persisted row carries `dataset_verification = full_hash`, explicit 2 GiB task and spill
budgets, release profile, `build_commit = 085704eb18c7`, and the arm's
`filtered_build_side_swap` value with explicit provenance.

### Census diff and reach

The q-series census engaged exactly two cells; digests and row counts are identical between arms on
all 22, and the m00-m10 ladder is invariant (0 of 11 fingerprints changed, all digests equal).

| cell | change under the swap |
|---|---|
| q17 | fingerprint `0bb3976ddac68006` → `9062c1de21622849`; join0 admission `declined(estimate_exceeds_budget, estimate=5,758,680,840)` → `admitted(estimate=192,000,000)`; spill 622,544,968 → 0 B; single census run execute 32,685.374181 → 11,601.401144 ms |
| q20 | fingerprint `e92495e248cd91be` → `33294d017f55ba80`; admission estimate 9,600,000 → 3,000 B; zero spill in both arms; execute 8,840 → 8,680 ms (single run, no movement claim) |

Strict correctness passed in both arms for every changed cell and the named sentinel: q17 at its
supported sf=0.02 oracle coordinate, q20 and q19 at sf=0.01, each with explicit budgets and direct
exit 0. q19's fingerprint did not change.

### Plan-distinct gate and cohort integrity

The ADR 469 plan-distinct gate ran as gate-zero inside the paired rail over the full-hash dataset
fingerprint `fnv1a64:60b35fe2a04ed8ec`; a failed gate aborts before any child. The journal holds
one discarded warmup and exactly 20 measured rows in adjacent pairs: every row passing, full-hash,
clean `085704eb18c7`, one shared digest across arms, per-arm fingerprints
`0bb3976ddac68006` (control) / `9062c1de21622849` (candidate), control spill 622,544,968 B and
candidate spill 0 B on every measured row.

One rail-semantics refinement against the plan text: the plan-distinct rail defines `--runs` as
child rows (only ADR 510's named gate doubles pairs into children), so the predeclared
10-pairs/20-rows cohort is `--runs 20`. Ordering, floor, switch, and budgets are unchanged.

### Locked paired cohort

| pair | candidate | control | control - candidate |
|---:|---:|---:|---:|
| 1 | 11,671.757 ms | 33,827.374 ms | 22,155.617 ms |
| 2 | 11,652.836 ms | 33,168.384 ms | 21,515.548 ms |
| 3 | 11,758.104 ms | 35,225.845 ms | 23,467.741 ms |
| 4 | 11,739.515 ms | 33,641.615 ms | 21,902.099 ms |
| 5 | 11,760.838 ms | 33,540.457 ms | 21,779.619 ms |
| 6 | 12,131.883 ms | 35,050.119 ms | 22,918.236 ms |
| 7 | 11,987.491 ms | 33,674.875 ms | 21,687.384 ms |
| 8 | 11,929.875 ms | 33,614.471 ms | 21,684.597 ms |
| 9 | 11,875.914 ms | 34,626.437 ms | 22,750.522 ms |
| 10 | 11,946.621 ms | 33,918.449 ms | 21,971.828 ms |

The predeclared paired result, computed from the persisted journal rows:

```text
Hodges-Lehmann point        = 22,133.035 ms
two-sided 95% interval      = [21,732.108, 22,684.920] ms
fixed materiality floor     = 3,554 ms
interval excludes zero      = true
point reaches floor         = true (6.2x; the interval lower bound alone is 6.1x)
activate                    = true
```

The generic unpaired summary pools both mechanisms, flags the 1.041 pooled spread as `unstable`,
and refuses to write a normal performance baseline — correct behavior, and no replacement for the
configuration-locked paired analysis, exactly as in ADR 510.

### Diagnostic transfer

Explanatory only; these timings are not in the primary estimator. Node attribution reconciles in
both arms with zero bridge, stage, or interleaved-frame mismatches and zero unattributed spill.

| fact | control | candidate |
|---|---:|---:|
| execute | 33,454.4 ms | 11,641.6 ms |
| aggregate node exclusive | 33,422.8 ms | 9,533.9 ms |
| join node exclusive | 0.0 ms | 1,161.6 ms |
| scan node exclusive | 0.0 ms | 917.6 ms |
| `correlated_scalar_left_outer_global` invocation | 23,274.0 ms, 622,544,968 B | not routed |
| inner `scan_grouped` invocation | 10,148.9 ms | 9,533.8 ms |

The swapped plan no longer matches the monolithic correlated-scalar handler shape: the comma join
runs as an ordinary in-memory hash join (admitted at 192,000,000 B against the 2 GiB budget) and
the terminal global fold routes through the generic path at negligible cost. The 23.3-second
spill-replay invocation collapses to about 2.1 seconds of join and scan work, which matches the
paired estimate. The surviving cost is the inner per-partkey aggregate over full lineitem
(9.5 s, now roughly 82% of the cell) — precisely the Remedy B target named in the ADR's forward
arc.

### Activation

The verdict is `activate` under ADR 472's rule verbatim: the interval excludes zero far above the
fixed floor, and neither engaged cell prices a regression (q17 improves 2.8x; q20 is flat with an
identical digest). This receipt flips `PlannerConfig::filtered_build_side_swap` to compiled default
**true**, retargets the gauge knob-contract test's default arm, and re-runs the full seal gate plus
default-arm strict correctness smokes for q17, q19, and q20 from the flipped tree. The
implementation seal `085704eb18c7` remains the measurement identity; this receipt commit is
independently revertible.
