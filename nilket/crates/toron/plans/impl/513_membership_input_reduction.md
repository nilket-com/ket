# impl 513 - membership input reduction plan

One declining recognizer extension over ADR 512's derivation walk, applied to semi/anti
membership inputs, measured on q21. The deferred/direct route condition is read, never written.

## Immutable inputs

- control tree: post-ADR-512 main (`c5c67684` lineage), both 511/512 knobs default on;
- fixed materiality floor: 3,554 ms (inherited, not recomputed);
- exact dataset: strict sf=8 persisted Parquet with `full_hash`; budgets 2 GiB / 2 GiB explicit;
- grounded anchors: q21 fingerprint `aa4d618880dd4c55` invariant across 511/512 seals; semi/anti
  `PinnedByAuthor` receipts; stage split runtime_prep 5,728.7 ms / deferred_replay 9,899.1 ms
  (reservoir spill 897,712,848 B; inner_replay_1 4,619.5 ms = compact-index rebuild);
  route condition `partition_at_a_time_replay_spec` streaming.rs:14500; semi/anti runtimes are
  `RecognizedNeqSummary` built in runtime_prep from the l2/l3 scans; Alias preserves facts.

## Phase 0 - residency and route probe (before the mechanism)

From the current tree with a temporary residency/attribution probe (no mechanism code):

1. Pin suffix runtime 1's build input (which subplan; its `estimated_build_bytes`) and record the
   loaded-versus-partitioned admission arithmetic that today selects `PartitionedScan` — the
   exact numbers behind the deferred route.
2. Project the post-reduction resident baseline: measured summary bytes per membership row today,
   scaled by the σ_F(orders) bound (count its keys and the qualifying l2/l3 row fractions from
   one census-style run), against the admission threshold from item 1. State plainly whether the
   route is projected to flip, with the margin.
3. Predeclare the no-flip floor path: reachable savings = runtime_prep share at the measured
   membership cut, plus any summary-probe second-order effects; state whether that plausibly
   reaches 3,554 ms. If not, say so in the plan-of-record before measurement — the cohort then
   decides `activate` versus `detectable, immaterial` honestly.

Phase 0 results land in this document before step 1 code is written. A Phase 0 result that
falsifies the mechanism's value (for example, summaries already too small to move the baseline)
closes the ADR red at Phase 0, ADR 509 style.

## Step 1 - recognizer and config

`PlannerConfig::membership_input_reduction: bool`, compiled default **false** at the seal. Gauge
env `TORON_TPCH_MEMBERSHIP_INPUT_REDUCTION`, loud "0"/"1" contract, row fields with provenance,
knob-contract test beside the 511/512 ones.

Extend `binder/scalar_reduction.rs` with a sibling recognizer at the `LogicalPlan::Join` bind
arm (direct bind path only, same no-oscillation rule as ADR 512):

- kinds `LeftSemi` and `LeftAnti`, `null_safe_keys == false`, `on` non-empty;
- derive the source with the existing walk from the **left (probe) side** plan, requiring every
  `on` key jointly traced to one shared deterministic filtered base-relation chain — the
  complete correlation-key tuple, or decline;
- reduce the **right (membership) side**: `LeftSemi(membership, source)` on the full tuple,
  leaving `on_filter`/match-filter residuals and all membership-local predicates untouched;
- decline an already-reduced membership (idempotence guard, as in 512);
- the inserted semi is an ordinary auto join: ADR 512's smaller-build placement applies to it by
  the existing rules, no special casing.

## Step 2 - tests

Extend the ADR 512 equivalence battery in `tests/scalar_reduction_test.rs` (same fixture family,
EXISTS/NOT EXISTS forms):

- EXISTS and NOT EXISTS, knob on/off, identical results: probe-domain-present keys, absent keys,
  null keys on both sides, duplicate keys;
- residual preservation: a NOT-EXISTS fixture where the membership residual (`<>`-style or a
  local filter) decides the result — identical across arms, and a variant proving the reduction
  did not absorb or reorder the residual;
- complete-tuple boundary: a two-key correlation where only one key traces → decline
  (byte-identical plans);
- decline pins: null-safe shape, unfiltered probe-side source, knob off;
- binder receipts: q21-shape fixture engages both membership reductions with the σ(orders)-class
  source; engagement absent with the knob off.

Architecture: reconcile module/line-bound guards as in 511/512; name every retarget.

## Step 3 - census diff and reached-shape correctness

Both arms, q01-q22 and m00-m10, exact coordinate, sealed binary. Engagement hypothesis: q21
(both membership sides); q04/q22 EXISTS shapes are census questions, not claims. Every changed
cell runs its strict correctness rail in both arms; q19 sentinel regardless.

Required engagement evidence on q21, causally split:

- mechanism: membership scan rows and summary build bytes shrink to the bounded subset;
  runtime_prep moves; digest identical;
- route: which lifecycle variant ran per arm (deferred stages present or absent), reservoir
  bytes, whole-row spill — reported as observed consequence, with the unchanged condition cited.

## Steps 4-7 - seal, gates, cohort, receipt

Identical protocol to ADR 512's steps 4-7: cohesive seal, full gate battery with direct exit
statuses, release build from the pushed identity, plan-distinct gate, diagnostic pair with node
attribution (mechanism metrics and route metrics reported separately), then:

```bash
TORON_TPCH_MEMORY_PER_TASK=2147483648 \
TORON_TPCH_SPILL_BYTES_RESERVED=2147483648 \
target/release/toron-tpch perf \
  --query q21 --mode strict --sf 8 --data-source parquet --data-dir target/tpch/data \
  --engines toron --runs 20 --warmup 1 \
  --paired-ablation \
  --paired-env TORON_TPCH_MEMBERSHIP_INPUT_REDUCTION \
  --paired-on 1 --paired-off 0 \
  --paired-gate plan-distinct \
  --paired-materiality-floor-ms 3554 \
  --output-dir target/tpch/adr513_q21_paired_<build>
```

20 child rows = 10 adjacent pairs; Hodges-Lehmann point and two-sided 95% Wilcoxon interval from
the persisted journal; the standard verdict table; no post-hoc extension or floor negotiation.
Receipt flips the default iff `activate` and no engaged cell prices a material regression
(ADR 472's rule); the receipt's causal split states how much of the movement was mechanism and
how much was the route consequence, so a no-flip close leaves the successor an exact residual.

## Repository gates

The standard council seal and changed-surface blocks, via `scripts/cargo-cgroup`, direct exit
statuses preserved, gates re-run after any code change made in response.
