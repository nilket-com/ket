# impl 518 - semi-filtered reachable-key sources plan

One bounded coverage completion of ADR 512's reachable-key reduction: an enclosing membership
semi is first exposed on a deferred scalar's outer input, membership semis become traversable
filter evidence in the key trace, and the reduction derives on the traceable subset of
correlation keys. No Phase 0 twin: the directly priced removable component (≈ 4.5 s of q20's
5.81 s aggregate) already clears the 3,554 ms floor, so the bind-level probes are the shape gate.

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

## Step 1 - shape exposure, the two trace relaxations, and control

`PlannerConfig::semi_filtered_reachable_keys`, compiled default **false** at the seal; gauge env
`TORON_TPCH_SEMI_FILTERED_REACHABLE_KEYS`, loud "0"/"1" contract, row fields with provenance,
knob-contract test beside the prior ones.

At the binder Join arm, gated by the knob, first recognize an enclosing plain-equality
LeftSemi/RightSemi whose preserved side is exactly `Exchange(DeferredScalarSubquery)`. Require
the membership subtree to pass ADR 515's whitelist and every preserved-side ON/exchange key to
resolve exclusively in the deferred scalar's outer schema. Move that exact semi and exchange
metadata onto the deferred scalar's outer input and remove the former wrapper; body, ON tuple,
strategy intent, scalar inner, aggregate, and alias are moved unchanged. Re-entry terminates
because the rebuilt semi no longer wraps a deferred scalar.

In `trace_key_to_filtered_base`, gated by the knob: at a membership semi whose ON pairs are
plain-column equalities, queue the membership-side partner only when the traced key belongs to
the semi's preserved/output side (LeftSemi: left → right; RightSemi: right → left), and descend
that preserved side as a row-removing node. In `derive_reachable_key_source`, gated by the knob:
partition keys into traced and untraced; if the traced set is nonempty and shares one source
subtree, derive on that subset and retain each key's original tuple index. Both decorrelation
paths select their inner/group key by those indices; they never zip a shortened source-key vector
from position zero. Different-source keys and zero-traced shapes decline exactly as today. Knob
off is byte-identical.

## Step 2 - tests

Structural (bind receipts): the four grounding probes ride this commit with the two semi shapes
flipped to engagement; a non-prefix subset fixture proves that a traceable second key maps to the
second inner/group key; mirrored LeftSemi/RightSemi fixtures pin preserved-side-only traversal;
new declines pin keys tracing to different filtered sources, no traceable key, and no
plain-equality membership partner for the traced key. The derived source is asserted to be the
filtered dimension scan (no join clone in the semi build subtree); knob-off byte-identity.
An official-shaped deferred fixture additionally pins one membership invocation, one reduction
semi, unchanged output schema, and no cloned membership join.

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

## Measured outcome

### Census, correctness, and engagement evidence

Both arms from the release binary at the sealed identity `c5a74a06`, exact coordinate. The
census engaged exactly one cell — q20, fingerprint `33294d017f55ba80` → `cf6b5af17c0c31c8`,
digest and row count identical, census wall 8,775 → 2,054 ms — with all 21 other route cells
plan-identical and digest-equal and all 11 ladder cells plan-invariant. Strict correctness
green in both arms for q20 and the q19 sentinel (4/4).

**Engagement witness, with one honest substitution.** The plan named the ADR 516 fold-input
counters as the witness, but the reduced aggregate no longer commits `scan_grouped` — it falls
to `uncovered_generic_fold`, whose vocabulary carries no flow counters. The substituted witness
triangulates the same fact: the aggregate node's exclusive time collapses **5.48 s → 0.014 s**
(391x) with the fold's 5,859-batch lifecycle gone from the on-arm profile entirely; the
reachable set is data-counted (17,203 forest parts → 68,812 pairs → 517,084 lineitem rows,
92.8x below the 47,989,007 off-arm input); and the digest equality plus the plan-distinct
fingerprints close the loop. The on-arm span is 2.03 s, now scan-dominated (scan 1.07 s,
exchange 0.45 s, join 0.29 s, filter 0.22 s) — the aggregate is no longer on the envelope at
all. The route observation is named for the record: the reduced shape lands on the generic
fold, which at 517,084 rows costs nothing material here.

### Locked paired cohort

10 adjacent pairs under the plan-distinct gate at the fixed 3,554 ms floor, from the persisted
journal (candidate / control / delta, ms): 2,071.8/8,780.1/6,708.3; 2,063.9/8,782.8/6,718.9;
2,016.4/8,742.4/6,726.0; 2,053.4/8,811.9/6,758.5; 2,039.7/8,757.9/6,718.2;
2,015.0/8,778.8/6,763.9; 2,044.3/8,723.4/6,679.1; 2,060.8/8,757.5/6,696.7;
2,061.0/8,775.4/6,714.4; 2,068.3/8,736.6/6,668.3.

```text
Hodges-Lehmann point        = 6,716.1 ms
two-sided 95% interval      = [6,693.6, 6,738.7] ms
fixed materiality floor     = 3,554 ms
interval excludes zero      = true
point reaches floor         = true (1.89x; the interval lower bound alone is 1.88x)
activate                    = true
```

Zero spill in both arms; peak RSS 2,510 → 2,247 MiB; one digest across all 20 child rows. The
delivered delta (medians 8,766.7 → 2,057.1 ms, 4.26x) exceeds the directly priced ≈4.5 s
because the recombine join and its exchanges shrink with the 6,396,683 → ≤68,812 group
collapse — named in the plan as a consequence, not priced.

### Activation

Per ADR 472's rule: `activate`, with q20 the only engaged cell and no plan, digest, or route
change anywhere else. This receipt flips `semi_filtered_reachable_keys` to compiled default
true, retargets the gauge knob-contract default arm, and re-runs the full seal gate plus
default-arm strict correctness smokes for q20 and q19 from the flipped tree. q20 moves from
~8.8 s to ~2.1 s at the coordinate; against the post-517 board its one-thread DuckDB gap
closes from 8,123 ms (14.2x, the board's worst ratio) to ~1.4 s (~3.3x).

Named residuals, none claimed: the `uncovered_generic_fold` route on reduced inputs (shared
with q18's commuted outer fold — cheap here, but the recognizer gap is now two cells wide);
multi-source key composition stays out of scope; q13 grounding is next on the board.

| evidence | artifact |
|---|---|
| censuses, both arms | `target/tpch/adr518_{route,ladder}_census_{off,on}_c5a74a06/` |
| instrument runs, both arms | `target/tpch/adr518_instr_q20_{off,on}_c5a74a06/` |
| locked cohort | `target/tpch/adr518_q20_paired_c5a74a06/` |
