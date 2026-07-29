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
