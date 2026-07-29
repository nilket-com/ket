# impl 514 - compact build partition sizing plan

One scoped, fail-closed sizing fork at the streamed scan-build partitioning choice, measured on
q21 through a plan-identical paired gate. The shared admission constant and the route condition
are read, never written.

## Immutable inputs

- control tree: post-ADR-513 main (`0d54536b` lineage);
- fixed materiality floor: 3,554 ms (inherited, not recomputed);
- exact dataset: strict sf=8 persisted Parquet with `full_hash`; budgets 2 GiB / 2 GiB explicit;
- grounded anchors: runtime-1 build = σ_F(orders) → o_orderkey, 5,845,938 rows; legacy estimate
  701,512,560 B (= rows x 120); threshold 536,870,912 B (`spill_budget / 4`,
  `streamed_inner_build_writer_target_bytes`); observed decoded pages 46,837,968 B, per-partition
  index peak 134,217,728 B (= 4,194,304 slots x 32 B), pinned peak 23,426,616 B, probe reservoir
  873,395,520 B over 30,343,541 buffered rows; chain-stage instrument
  `TORON_CHAIN_STAGE_PROFILE_PATH`; deferred/direct condition
  `partition_at_a_time_replay_spec` (streaming.rs:14500) reading
  `streamed_inner_build_partition_count` (exec/join.rs:2092) via
  `choose_streaming_scan_inner_build_partitioning` (streaming.rs:7473).

## Phase 0 - owner-complete pricing proof (no mechanism code)

1. Verify the index allocation model from receipts: slot width and power-of-two capacity growth
   (the 134,217,728 B peak at ~2.9 M per-partition keys), and pin where write scratch and
   reference vectors charge during a resident (non-spilled) build by reading the loaded-build
   path. Compose the counterfactual `partition_count = 1` owner sum:
   decoded pages + pow2(5,845,938) x 32 B index + references + scratch.
2. Apply the predeclared 1.25 safety factor and state the fit against 536,870,912 B with the
   margin in bytes. Projection from the grounding: ~409 MB x 1.25 ≈ 511 MB — a ~5% margin. If
   the owner-complete bound (not the factor) moves over the threshold, close the ADR red here.
3. Pin the direct variant's residency guard arithmetic on the projected resident set (the
   `guard_membership_owner_resident` family): the flip must fit the guards at 2 GiB, or the ADR
   closes red at Phase 0 — a route flip into a guard violation is a red, not a win.
4. Explain the measurement split in the record: 23.4 MB pinned and 46.8 MB decoded are
   deferred-route artifacts (incremental per-partition pinning); the priced footprint is the
   direct route's simultaneous owners. The receipt carries both numbers side by side.

Phase 0 results land in this document before step 1 code is written.

### Phase 0 result — green, with the owner model corrected downward

The count=1 path routes through the standard coalescing `SpilledInnerJoinBuild`: pages are
written with an index and page residency is bounded by `JOIN_SPILL_CACHE_TARGET_BYTES` = 8 MiB —
decoded pages are never simultaneously resident in full. That corrects the plan's provisional
409 MB projection (which wrongly charged all decoded pages as resident) and is the precise
explanation of the measurement split: 23.4 MB pinned and 46.8 MB decoded are deferred-route and
cache-bounded artifacts; the priced owners are the index allocations plus bounded cache and
scratch.

Owner-complete bound at the pre-scan's exact count (5,845,938 rows, 46,837,968 decoded bytes),
priced with the layout the spill module actually allocates:

| owner | bytes |
|---|---:|
| buckets: pow2(2 x rows) x 4 B | 67,108,864 |
| entries: pow2(rows) x 24 B (`PrimitiveSpilledBatchIndexEntry<i64>`) | 201,326,592 |
| worst-case duplicate-ref overflow: rows x 16 B | 93,535,008 |
| decoded-page cache target | 8,388,608 |
| page metadata + write scratch | 445,184 |
| **owner-complete bound** | **370,804,256** |
| x 5/4 contingency | **463,505,320** |
| threshold (`spill_budget / 4`) | 536,870,912 |
| **margin** | **73,365,592 (13.7%)** |

The fit holds with the contingency as pure headroom above an owner-complete sum — no owner was
omitted to make room for it. The composed threshold-edge unit test pins this arithmetic
(5,845,938 rows price to one partition; 12,000,000 price to two; the edge is exercised on both
sides of 536,870,912). The guard check (`guard_membership_owner_resident` at 2 GiB) has
1.68 GiB of headroom above the priced resident set plus the measured runtime baseline.

Two scope facts recorded for the receipt: the join0 spill route's call into the partitioning
choice passes no compact facts this cut (it keeps legacy sizing; its census route facts must not
change), and the probe-side page access through the 8 MiB cache under the direct route is the
principal residual risk the cohort prices.

## Step 1 - the sizing fork and control

At `choose_streaming_scan_inner_build_partitioning` (the only call site):

- eligibility: single build key, `JoinKeyPlan::Int32 | Int64`, `null_safe_keys == false`, and
  the representation selection provably compact for this shape — mirroring the executor's own
  compact-selection rule, not a parallel reimplementation of it. Anything else: legacy estimate,
  byte-for-byte today's behavior;
- eligible: price the owner-complete bound (decoded batch bytes summed by the existing pre-scan,
  plus pow2-slot index bytes, plus reference bytes, plus scratch), saturating arithmetic,
  multiplied by the predeclared 1.25 factor; overflow or any non-computable component fails
  closed to legacy;
- the fork feeds only this partitioning choice. `HASH_JOIN_BUILD_ROW_OVERHEAD_BYTES` and every
  binder-side consumer are untouched; an architecture guard pins the single-call-site scope.

Control: `TORON_COMPACT_BUILD_PARTITION_SIZING` latched once at the route entry (ADR 510's
pattern), explicit "0"/"1", absence = compiled default (off at the seal); no rereads below the
route. Harness knob-contract test beside the 510-era env tests.

## Step 2 - tests

- Sizing-fork units: eligible compact shapes price owner-complete; generic/composite/null-safe
  retain legacy exactly; saturation on overflow falls back closed.
- Threshold-edge tests: synthetic bounds straddling 536,870,912 B with and without the factor,
  pinning partition_count 1 versus 2 at the exact predeclared edge — these fail under any silent
  factor or threshold drift.
- Route-fact tests on the q21 shape fixture: knob on selects partition_count 1 and the direct
  stage vocabulary; knob off reproduces today's partition_count 2 and the deferred vocabulary;
  digests identical.
- Guard tests: the direct-route resident set passes the membership owner guards at the projected
  footprint; an artificially inflated footprint trips them (the guard is live, not vestigial).

## Step 3 - the route-distinct paired gate

Add `PairedGate::RouteDistinct` to the harness (ADR 510's named-gate precedent): one diagnostic
child per arm requiring equal build identity, `full_hash`, plan fingerprint, digest, schema, row
count, and explicit budgets; and opposite route facts — partition_count 2 versus 1, deferred
versus direct stage set, reservoir spill nonzero versus zero. Missing, stale, nonnumeric, or
same-arm evidence fails closed. The existing plan-distinct and shape-preserving gates and their
tests are unchanged. Serialize gate selection, switch values, budgets, coordinate, and the fixed
floor into the paired journal's configuration lock.

## Step 4 - census and reached-shape correctness

Both arms, q01-q22 and m00-m10, exact coordinate, sealed binary. Plan fingerprints must be
identical per cell across arms (the switch changes no plans); enumerate every cell whose ROUTE
facts change (partition counts, stage vocabularies, spill), and run each one's strict
correctness rail in both arms. q21 at its supported coordinate; q19 sentinel regardless.
Residency, route, correctness, and cleanup evidence complete before any performance row.

## Steps 5-7 - seal, cohort, receipt

ADR 512's protocol: cohesive seal of steps 1-4, full gate battery with direct exit statuses,
push, release build from the pushed identity, then the gate and diagnostic pair, then:

```bash
TORON_TPCH_MEMORY_PER_TASK=2147483648 \
TORON_TPCH_SPILL_BYTES_RESERVED=2147483648 \
target/release/toron-tpch perf \
  --query q21 --mode strict --sf 8 --data-source parquet --data-dir target/tpch/data \
  --engines toron --runs 20 --warmup 1 \
  --paired-ablation \
  --paired-env TORON_COMPACT_BUILD_PARTITION_SIZING \
  --paired-on 1 --paired-off 0 \
  --paired-gate route-distinct \
  --paired-materiality-floor-ms 3554 \
  --output-dir target/tpch/adr514_q21_paired_<build>
```

20 child rows = 10 adjacent pairs; Hodges-Lehmann point and two-sided 95% Wilcoxon interval from
the persisted journal; the standard verdict table; no post-hoc extension, factor adjustment, or
floor negotiation. The diagnostic's causal split separates the reservoir-removal saving from the
estimate pre-scan's second input scan (the deferred route scans the build input twice; the
direct route once) so neither is credited to the other. Receipt flips the compiled default iff
`activate` and no route-changed cell prices a material regression; default-arm smokes for q21
and q19 from the flipped tree.

## Repository gates

The standard council seal and changed-surface blocks via `scripts/cargo-cgroup`, direct exit
statuses preserved, gates re-run after any code change made in response.
