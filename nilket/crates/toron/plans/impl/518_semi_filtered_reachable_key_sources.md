# impl 518 - semi-filtered reachable-key sources plan

One bounded coverage completion of ADR 512's reachable-key reduction: membership semis become
traversable filter evidence in the key trace, and the semi derives on the traceable subset of
correlation keys. No Phase 0 twin: the directly priced removable component (≈ 4.5 s of q20's
5.81 s aggregate) already clears the 3,554 ms floor, so the bind-level probes are the shape
gate.

## Immutable inputs

- tree: post-517 main (`79370296` lineage), all activated knobs default on;
- fixed materiality floor: 3,554 ms (inherited, not recomputed); per-cell, never summed;
- exact dataset: strict sf=8 persisted Parquet with `full_hash`; budgets 2 GiB / 2 GiB explicit;
- pinned anchors: `trace_key_to_filtered_base`'s `Inner`-only partner arm
  (binder/scalar_reduction.rs:141 area), `base_chain_has_deterministic_filter` ending at any
  join (scalar_reduction.rs:42), `derive_reachable_key_source`'s all-keys rule
  (scalar_reduction.rs:180 area);
- grounding receipts: q20 aggregate = 5.81 s of 9.55 s (stream_pull 0.90 / fold 2.86 / finalize
  1.71), input 47,989,007 rows → 6,396,683 groups; reachable 17,203 parts → 68,812 pairs →
  517,084 lineitem rows; the 2x2 probe matrix (plain-filter shapes engage, semi shapes decline).

## Step 1 - the two relaxations and control

`PlannerConfig::semi_filtered_reachable_keys`, compiled default **false** at the seal; gauge env
`TORON_TPCH_SEMI_FILTERED_REACHABLE_KEYS`, loud "0"/"1" contract, row fields with provenance,
knob-contract test beside the prior ones.

In `trace_key_to_filtered_base`, gated by the knob: at a membership semi whose ON pairs are
plain-column equalities, queue the build-side partner for the traced key (orientation-general),
and descend the probe side as a row-removing node. In `derive_reachable_key_source`, gated by
the knob: partition keys into traced and untraced; if the traced set is nonempty and shares one
source subtree, derive on that subset; different-source keys and zero-traced shapes decline
exactly as today. Knob off is byte-identical.

## Step 2 - tests

Structural (bind receipts): the four grounding probes ride this commit with the two semi shapes
flipped to engagement; new declines pinned (keys tracing to different filtered sources; no
traceable key; non-equality membership ON); the derived source asserted to be the filtered
dimension scan (no join clone in the semi build subtree); knob-off byte-identity.

Semantic (execution, both arms identical, the 512/515/517 fixture family): NULL keys both
sides, duplicate outer rows and duplicate matches, empty membership result, and the
subset-reduction case (untraced key selective, traced key not — results identical, the weaker
semi is a pure superset).

Architecture: reconcile any guards scanning the reduction pass; name retargets in the receipt.

## Step 3 - census, correctness, and engagement evidence

Both arms, q01-q22 and m00-m10, exact coordinate, sealed binary. Engaged set is empirical.
Required q20 evidence: reduction semi present with the part-key subset; aggregate fold input
47,989,007 → ~517,084 and finalized groups 6,396,683 → ≤68,812 (ADR 516 instrument vocabulary);
digests identical; fingerprint-changed cells run strict correctness in both arms; q19 sentinel.

## Steps 4-7 - seal, gates, cohorts, receipt

Standard protocol: cohesive seal, full gate battery with direct exit statuses, push, release
build; q20 paired cohort (20 runs / 10 adjacent pairs, plan-distinct, floor 3,554 ms,
Hodges-Lehmann + 95% Wilcoxon from the persisted journal) plus a regression-check cohort for
every other engaged cell; activation per ADR 472's rule, default flip + knob-test retarget +
full gates + default-arm smokes for the engaged set and q19 from the flipped tree; both-way
verdict reporting.

## Repository gates

The standard council seal and changed-surface blocks via `scripts/cargo-cgroup`, direct exit
statuses preserved, gates re-run after any code change made in response.
