# impl 522 - join-branch reachable-key reduction plan

Reduce q05's amplified lineitem branch by the keys reachable from its filtered
supplier/nation/region subtree, under the ADR 512/518 superset theorem lifted to the join
graph. Phase 0 runs first with red-close authority and ADR 513's honest acquisition ledger.

## Immutable inputs

- tree: post-pin-fix main (`75ecd13a` lineage), all activated knobs default on;
- fixed materiality floor: 3,554 ms (inherited, not recomputed); per-cell, never summed;
- exact dataset: strict sf=8 persisted Parquet, manifest format 2, full-hash loads; budgets
  2 GiB / 2 GiB explicit;
- grounding receipts (q05 at `75ecd13a`): span 6.70 s attributed; aggregate node 4.31 s with
  runtime_prep 2.86 s over a 47,989,007-row lineitem suffix build probed by 1,820,547 keys;
  suffix-1 emissions 7,283,512 cut to 292,167 (supplier + nationkey cycle edge) then 58,990
  (region, last stage); exchange 1.67 s; zero spill; data counts 16,042 / 80,000 ASIA
  suppliers, 9,623,900 / 47,989,007 ASIA lineitem rows;
- ADR 513's ledger lesson: price acquisition, added probes, and cloned work; record
  physically-shared vs executed-twice.

## Phase 0 - SQL twin with the honest ledger (no mechanism code)

Twin: a temporary probe patch of `STRICT_Q5_SQL` adding a derived-table join —
`, (SELECT DISTINCT s_suppkey AS ak_suppkey FROM supplier, nation, region WHERE s_nationkey =
n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA') AS ak` with
`AND l_suppkey = ak_suppkey` — DISTINCT keys make the join a pure row filter (no fanout). The
twin inherently pays source acquisition, the 48M key matches, and the query's own original
joins, so its delta is the attainable net, not a ceiling.

Protocol: preserve the sealed `75ecd13a` release binary; build the twin probe binary; run 5
interleaved original/twin pairs at the exact coordinate; verify twin digest/rows equal the
original's; remove the probe with a targeted edit. Ledger rows: acquisition cost (from the
twin's bound-plan receipts / node attribution), added probe volume, cloned-work disclosure.
**Decision, predeclared: median twin delta < 3,554 ms closes the ADR red with the
per-component ledger.** Phase 0 results land in this document before Step 1 code.

## Step 1 - the rule and control

`PlannerConfig::join_branch_key_reduction`, compiled default **false** at the seal; gauge env
`TORON_TPCH_JOIN_BRANCH_KEY_REDUCTION`, loud "0"/"1" contract, row fields with provenance,
knob-contract test beside the prior ones.

Binder rule behind the declining gate: for an inner-join chain branch joined on a
plain-equality key into a subtree whose chain carries deterministic filters (the ADR 512
filtered-chain evidence class), insert `branch ⋉ distinct-keys(filtered subtree)` before
runtime build placement. Declines fail closed (non-deterministic filters, non-equality edge,
outer/anti context, unfiltered subtree); knob-off byte-identical. Pass body beside the
scalar-reduction family per the ADR 176 root discipline.

## Step 2 - tests

Structural: q05 shape engages (semi below the branch, source = filtered dimension keys; single
insertion, no clone; spine keys/intents/exchanges/schemas otherwise unchanged); declines
pinned per gate; knob-off byte-identity.

Semantic (both arms identical): NULL branch keys, duplicate branch rows and key matches, empty
and all-filtered sources, and the cycle-edge fixture (`c_nationkey = s_nationkey` analog on
the fixture schema).

Architecture: reconcile scanning guards; name retargets in the receipt.

## Step 3 - census, correctness, and engagement evidence

Both arms, q01-q22 and m00-m10, exact coordinate, sealed binary. Required q05 evidence: suffix
build 47,989,007 → ~9,623,900 rows and suffix-1 emissions 7,283,512 → ~1.46M (ADR 516
instrument), digests identical; every fingerprint-changed cell runs strict correctness both
arms; q19 sentinel; every engaged cell beyond q05 gets at least a regression-check cohort.

## Steps 4-7 - seal, gates, cohorts, receipt

Standard protocol: cohesive seal, full gate battery with direct exit statuses, push, release
build; q05 paired cohort (20 runs / 10 adjacent pairs, plan-distinct, floor 3,554 ms,
Hodges-Lehmann + 95% Wilcoxon from the persisted journal) plus engaged-cell regression
cohorts; activation per ADR 472's rule with default flip, knob-test retarget, full gates, and
default-arm smokes from the flipped tree; both-way verdicts.

## Repository gates

The standard council seal and changed-surface blocks via `scripts/cargo-cgroup`, direct exit
statuses preserved, gates re-run after any code change made in response.
