# ADR 469 - price the engaged swap: census first, cohort only where it engages

The smaller-build placement is active (468); this cut measures whether it matters on
the board. Census before cohort: an env knob makes the flag A/B-able in the gauge, a
plan-fingerprint census over all 22 queries finds where the swap actually engages,
and only an engaged cell earns a paired cohort — under a plan-distinctness gate, the
swap's analog of gate zero. "Nothing engages on the board" is an admissible sealed
outcome, not a failure.

## Context

- 468 activated `hash_smaller_build_placement` with correctness proven on armed
  sources, but no throughput claim — deliberately. The q05 sf=8 sentinel showed its
  fingerprint unchanged: the arc's best-measured query never engages the swap.
- The paired instrument (449/451 protocol) is currently wired to one ablation (the
  runtime filter, via env) and one query (q05). Pricing the swap needs the same
  statistical machinery on a different switch — which is a rail generalization, not
  a new estimator.

## Decision — three rails, strictly ordered

**Rail 1 — the gauge knob.** `TORON_TPCH_HASH_SMALLER_BUILD` (unset → engine
default on; `0`/`1` explicit) threads into the gauge's `PlannerConfig`, recorded in
every child row and summary with provenance (`default`/`explicit`), the standing
knob contract (loud rejection of garbage).

**Rail 2 — the engagement census.** One child per query per arm (flag on/off) at
the board scale (sf=1, strict parquet, explicit budget coordinates): record plan
fingerprints and digests. The engaged set = queries whose fingerprints differ
between arms; every engaged query must have identical digests (output invariance,
re-proven at the cell, not assumed). Census cost is ~44 cheap children.

**Rail 3 — contingent pricing.** If the engaged set is non-empty: generalize the
paired rail minimally — the paired-ablation driver takes the switching env as a
parameter (`--paired-env NAME`, default the existing filter env, preserving 449
byte-compatibly), and the reservoir gate zero is replaced for this switch by a
**plan-distinctness gate**: arms must differ in plan fingerprint and agree in
digest, in-run, before any timed child. Then the standing protocol (release, 20
runs, durable journal, budget/fingerprint-locked manifest, OFF = flag-off arm as
baseline, 10% floor, mandatory order) prices the cheapest engaged cell.

If the engaged set is empty: the receipt seals it — the swap engages nowhere on
this board at these coordinates, pricing is deferred until a workload engages it,
and the follow-on (placement-aware order costing) loses urgency accordingly.

## Acceptance

- Knob parsed/validated/recorded like the budget knobs; a test pins the contract.
- Census artifact: per-query, per-arm fingerprints + digests; engaged set named;
  any digest mismatch halts the cut as a correctness defect (it would falsify 468).
- If Rail 3 runs: the generalized rail's q05/filter path proves byte-compatible
  (the 449 fixtures still pass); the plan-distinctness gate is tested (same-plan
  arms refuse; differing-digest arms refuse loudly); the cohort receipt carries the
  switch name, both arms' fingerprints, and all standing coordinates.
- No cross-switch or cross-cell ranking; sealed verdicts stand.

## Guardrails and risks

- **Census before cohort** — a cohort at an unengaged cell measures noise with
  perfect integrity; the census is the reachability rail of this cut.
- **The digest check in the census is load-bearing**: an engaged query with a
  digest mismatch is a 468 falsification and outranks everything else in this cut.
- **Generalizing the paired rail must not orphan 449**: the default env preserves
  the existing behavior; the journal/manifest formats gain the switch name without
  breaking pre-469 artifacts (fresh dirs regardless).
- **Do not re-judge sealed verdicts.**

## Forward arc

An engaged, priced cell feeds placement-aware order costing with its first real
number. An empty census parks pricing behind workload evidence. Behind both: q21
residual, q19 re-diagnosis + 438 widening, q13, Cut A (439 mirror), the O(1) LRU
rewrite, and the budget-derivation gauge remain queued.
