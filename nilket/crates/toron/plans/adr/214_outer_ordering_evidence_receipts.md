# ADR 214: Outer-Ordering Evidence Receipts

**Status:** Promoted. **Migration:** `plans/impl/214_outer_ordering_evidence_receipts.md`.
**Category:** Toron optimizer evidence and instrumentation — an evidence-design cut, no actuation or
policy change. No executor, fragmenter, storage-machinery, wire, Wendelon, demotion-pass-behavior, or
advisory-application change.

## Why this cut exists

ADR 213 grounded that the outer-ordering thread was **inexpressible**, not merely unfired: a demoted
outer join accrues verdicts as its resulting tree/graph kind (its outer provenance erased), and a true
outer join — fenced from ordering by 209-D4 — produces no verdict at all. The system cannot answer the
two questions every future outer-ordering decision needs: *is demotion actually producing measured
wins?* and *how much is the preserved-side fence leaving on the table?* This cut makes both countable,
durable, and queryable, with zero policy consuming them — the 182/196/203 pattern: the receipt family
precedes any machinery it might someday justify. (The cycle also re-priced the written readout-gate on
operator rationale — instrumentation-completeness: the evidence clock can't start later than the cut
that builds the clock.)

## Grounding (verified against the code; council-triple-checked)

- The verdict lane's key is the resulting decision kind (`measured_explain_analyze_verdicts` over the
  binding); the binding's `render_trace.outer_join_demotions` (explain.rs:75) carries demotion receipts
  at exactly the record site — provenance is *available* and currently dropped.
- True-outer joins reach no verdict: recognizers decline on `inner_equi`, so no ordering decision exists
  to judge. The counterfactual is constructible from existing parts: the scratch-recognition idiom
  (`NoOpDecisionRecorder`, decision_log.rs's documented pattern), the edges with their `inner_equi`
  flags on the render trace, the existing recommendation functions, and the existing measured-verdict
  math (declared vs preferred costs — the 198 shape).
- **The observation plumbing is the load-bearing wrinkle (Codex):** measured costs key by replay ids;
  a scratch counterfactual's verdict can only be judged if the *real executed* join targets are in the
  measured replay-id allowlist — otherwise every outer verdict is permanently Indeterminate.
- The durable matrix (35 keys) is "fixed by current kinds, not sacred against a promoted expansion";
  totals' key spelling is byte-compatible history that must not migrate.

## Decisions (promotion-locked)

**D1 — Exactly one new runtime kind: `PreservedOuterOrder`.** "The query still contained an outer join
at bind time, so ordinary tree/graph ordering could not legally own the decision." Fully-demoted joins
keep their tree/graph kind (no `DemotedOuterStar`, no generic `OuterJoin` bucket); a partially-demoted
join (FullOuter→sided, 209-D3 stepwise) is still outer at bind and lands here.

**D2 — Provenance is a dimension with dual-write accrual; totals never change spelling.** Verdict facts
carry `OptimizerRuntimeVerdictProvenance { Native, Demoted }`. Durable lanes: `Total` = the existing
`optimizer_evidence/verdict/{kind}/{verdict}` spelling (byte-compatible with all accrued history);
`Demoted` = the new sibling `…/{kind}/demoted/{verdict}`. Native facts increment Total only; demoted
facts increment both. **There is no `native` durable key** — public rows derive
`native = total − demoted` (saturating) and render a `provenance` column (tail-appended) on
`optimizer_runtime_verdicts`. This is what keeps the 213 tree advisory's evidence unchanged — the bug
the council caught in iteration (a kind-replacement design would have starved it) is pinned by gate.

**D3 — Provenance is binding-level in this cut, named honestly.** Any `OuterJoinDemotionReceipt` on the
binding marks its measured verdicts Demoted. Intentionally coarse (a binding mixing a demoted join with
an unrelated native star is wholly attributed), pinned by the mixed-binding fixture; per-decision
receipt mapping is the named refinement.

**D4 — The preserved-outer receipt is a scratch counterfactual with real observation mapping.** For a
sampled measured bind whose join graph retains outer equi-edges: a scratch recognition pass under
`NoOpDecisionRecorder` with the preserved edge(s) treated as inner-equivalent produces the
counterfactual recommendation; its verdict is judged with the existing measured-verdict math (declared
= the executed AST order's **measured** cost; preferred = the counterfactual's estimate). The scratch
receipt is stored **only when its join targets map back to real execution targets, and those targets
join the measured replay-id allowlist** (Codex's wrinkle — without it the kind is born Indeterminate).
Live plan, trace, decision log, strategy choices: byte-identical, gate-pinned.

**D5 — Outer verdict classes are opportunity classes.** The measured producer emits only `MissedWin`
(the inner-equivalent ordering measurably won — the on-the-table count), `Vindicated` (the fence cost
nothing), and `Indeterminate` (incomplete mapping/estimates) for `PreservedOuterOrder`. The measured
path has no constructor for `OverEager` or `MisRanked` on this kind — the ADR permits no outer rewrite
to be over-eager about. The keys still exist for matrix uniformity; synthetic or future callers are the
only way to populate them.

**D6 — The outer family has no advisory knob.** The 213 partition becomes tree/graph/**outer** (the
exhaustive scope matches force every site); `PreservedOuterOrder` is excluded from both margin
advisories and its sampled rows count toward each scope's symmetric `excluded_sample_size` (Gemini).

**D7 — Matrix expansion: 35 → 80, enumerable and deliberate.** 8 kinds × 5 classes × {Total, Demoted}.
Absent → 0 as always; the durable store's machinery is untouched (key population only — 076 agnostic);
no new `SystemTableKind` (adr102 unchanged, third cut running).

## Honesty gates

Red on `main`:
- `outer_ordering_receipts_distinguish_demoted_inner_from_true_outer`;
- `preserved_outer_ordering_decline_reaches_runtime_verdict_lane`.

**Load-bearing discriminators:**
- `demoted_inner_ordering_still_counts_as_tree_when_outer_context_is_erased` — kind stays tree,
  provenance carries the context.
- `tree_advisory_evidence_unchanged_by_provenance_split` — the kind-replacement pin (the caught bug).
- `demoted_star_verdict_accrues_to_total_and_demoted_sibling` — the dual-write pin, both directions.
- `counterfactual_bind_is_scratch_and_byte_identical` — NoOp recorder + plan equality under every
  seeding × mode.
- `preserved_outer_verdicts_are_opportunity_classes_only` — D5's measured producer restriction.
- `preserved_outer_kind_excluded_from_both_margin_advisories` + the symmetric-exclusion accounting.
- `mixed_binding_provenance_is_binding_level_and_documented` — D3's coarseness, pinned not hidden.
- `outer_ordering_evidence_survives_durable_refresh` — the 80-key matrix round-trips (live fixture).
- `counterfactual_targets_map_to_real_observations` — D4's replay-id wrinkle (fails if the allowlist
  step is dropped: the kind degenerates to all-Indeterminate).
- `rune_outer_ordering_receipts_match_sql` — parity.
- `no_graph_or_tree_application_change_from_outer_evidence`.

## Architecture guards (promotion-locked)

- `adr214_outer_kind_vocabulary_is_exactly_preserved_outer_order`
- `adr214_provenance_is_dual_write_totals_unchanged`
- `adr214_counterfactual_uses_noop_recorder_scratch_bind`
- `adr214_outer_verdicts_are_opportunity_classes`
- `adr214_runtime_evidence_matrix_is_80_keys_enumerable`
- `adr214_outer_family_has_no_advisory_knob`
- `adr214_no_executor_fragmenter_storage_wire_or_wendelon_change`

**Reconciled prior guards (named):** the 207 fixed-matrix guards retarget 35 → the enumerated 80 with
the Total-spelling byte-compatibility asserted by codec test; 213's family-partition test and scope
helpers retarget to the three-family partition (exhaustive matches enumerate the sites); the
verdicts-table completeness gates cover the 80-key space with the tail `provenance` column. adr102:
unchanged. adr076: no code change (Wendelon is key-population-agnostic; its "fixed matrix" wording
referred to point-addressing discipline, which holds).

## Rejected alternatives

**Two outer kinds** (`DemotedOuterStar`/`DemotedOuterOrder`) — the erasure discriminator empties the
demoted kind to partial-demotions only; provenance carries the distinction with less vocabulary.
**Kind-replacement for demoted bindings** — starves the tree advisory (the caught bug). **A separate
census table** — the verdict lane carries everything; opportunity classes ARE the census. **Per-decision
provenance now** — no stable receipt→level mapping exists in the render trace; binding-level is honest
and pinned. **Windowing/decay first** — make the missing lane expressible before correcting staleness
in existing lanes. **Any actuation** — the receipts this cut creates are the gate for that debate.

## Consensus (transparent)

3-of-3 on the spine. The trail: Codex's iter-0 set the frame (evidence-not-actuation, the
inexpressibility grounding carried from 213); Claude's iter-1 contributed the one-kind-plus-dimension
synthesis (selected by Codex's own erasure discriminator) and caught the kind-replacement bug in
Gemini's draft (Gemini adopted with credit — his second self-corrected cycle running); Codex's final
contributed the two load-bearing sharpenings folded into D4/D5 (the replay-id allowlist wrinkle; the
opportunity-class restriction) plus the "no native durable key" spelling of D2; Gemini's final added
the symmetric-exclusion accounting (D6). The operator re-priced the written readout-gate on
instrumentation-completeness rationale — recorded here per the steer-over-written-condition precedent.

## Forward arc

`214 outer-ordering evidence (this) → rotation by receipts over a complete lane: graph advisory
application (Q1), per-kind graph knobs (Q2), safe-side outer ordering (gated on the MissedWin counts
this cut creates), windowing/decay (cumulative staleness, now observable across three families) →
mixed-chain pushdown (named shape) → memo (receipts showing decisions fighting)`.

## Deferred (named)

Per-decision provenance mapping; outer-ordering actuation (gated on this cut's MissedWin counts);
demotion-decline disposition census (a decision-lane question); durable decision lane; windowing/decay;
graph advisory application (Q1-gated); per-kind graph knobs (Q2-gated).
