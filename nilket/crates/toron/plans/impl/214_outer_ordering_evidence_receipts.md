# Impl Plan — ADR 214: Outer-Ordering Evidence Receipts

Pairs with `plans/adr/214_outer_ordering_evidence_receipts.md`. Touch surface:
`binder/decision_log.rs` (vocabulary), `binder/runtime_evidence.rs` (key shape + codec),
`binder/explain.rs` (counterfactual emission + rendering), `binder/join_order.rs` (213 partition
retargets), `binder.rs` (replay-id allowlist), coordinator record sites (provenance threading),
tests, guards. No Wendelon, no new info-schema table, no demotion-pass behavior change. Phased so
the provenance half lands green before the counterfactual half begins.

## Anchors (re-ground line numbers at implementation — this cut opens 207/213 surfaces)

| Concern | Anchor |
|---|---|
| Kind vocabulary | `OptimizerDecisionKind` decision_log.rs:8-19 (+`PreservedOuterOrder`); label fns :186+ |
| Verdict key (gains storage lane) | `OptimizerRuntimeVerdictKey { kind, verdict }` decision_log.rs:56-60 |
| Key enumeration + codec | `all_verdict_counter_keys` / `verdict_counter_key` runtime_evidence.rs — Total spelling unchanged (`{kind}/{verdict}`); siblings `{kind}/demoted/{verdict}`; 80 total |
| Record sites (provenance + flush) | coordinator.rs streaming + local success sites; provenance source = `binding.render_trace.outer_join_demotions` (explain.rs:75) |
| Scratch-bind idiom | `NoOpDecisionRecorder` decision_log.rs (the documented scratch-recognition pattern) |
| Counterfactual math (reused) | the recommendation fns + `measured_graph_order_verdict` (join_order.rs) — declared = measured AST, preferred = counterfactual estimate |
| Outer edges + replay ids | `render_trace.join_order_edges` (`inner_equi` flags); the measured replay-id allowlist (`analyze_observation_mode(binding.replay_ids…)`, coordinator.rs measured path) — **D4: real targets of counterfactual-covered joins must join this allowlist** |
| 213 partition + scopes (retarget) | `is_tree_margin_advisory_kind` / `is_graph_margin_advisory_kind` / `MarginAdvisoryScope` (exhaustive — compiler enumerates the sites) |
| Verdicts table (provenance column at tail) | `information_schema_optimizer_runtime_verdicts_schema`/`_rows` explain.rs |
| Matrix guards (retarget 35→80) | adr207 fixed-matrix guards optimizer_accrual.rs:1266/:1476 |
| Durable round-trip fixture | the 207 LiveWendelonFixture family (wendelon_storage_execution_test.rs) — extend for siblings |

## Phase 0 — Red tests first

1. `outer_ordering_receipts_distinguish_demoted_inner_from_true_outer` — red on main.
2. `preserved_outer_ordering_decline_reaches_runtime_verdict_lane` — red on main (true-outer query →
   a `PreservedOuterOrder` row).
3. `tree_advisory_evidence_unchanged_by_provenance_split` — written against the new shape asserting
   the old advisory inputs (the kind-replacement pin).
4. `demoted_star_verdict_accrues_to_total_and_demoted_sibling` — the dual-write pin, both directions
   (sibling-only and total-only both fail it).
5. `counterfactual_bind_is_scratch_and_byte_identical` — plan equality + shared log untouched.
6. `counterfactual_targets_map_to_real_observations` — with the allowlist step present, a measurable
   fixture yields a non-Indeterminate verdict; the test asserts the verdict class, which degenerates
   to Indeterminate if the allowlist step is dropped (D4's wrinkle, pinned from the green side).

## Phase 1 — Vocabulary (decision_log.rs)

`OptimizerDecisionKind::PreservedOuterOrder` (label `preserved_outer_order`);
`OptimizerRuntimeVerdictProvenance { Native, Demoted }` (labels `native`/`demoted`);
`OptimizerRuntimeEvidenceLane { Total, Demoted }`; `OptimizerRuntimeVerdictKey` gains `lane`.
**`--all-targets` immediately** — the key is constructed across coordinator/evidence/tests; the 213
partition test goes red here by design (Phase 4 retargets it).

## Phase 2 — Key shape + durable codec (runtime_evidence.rs)

- Enumeration: 8 × 5 × {Total, Demoted} = 80. **Codec test pins the Total spelling against the
  207-era literal strings** (byte-compatibility is a promoted decision, not an accident).
- Accrual: native → Total only; demoted → Total + sibling. Render derivation:
  `native = total − demoted` saturating; decide derived-rows vs raw-rows at the explain site
  (legibility), test either.
- Extend the durable restart/round-trip fixture with sibling keys (flake discipline applies).

## Phase 3 — Provenance threading + the counterfactual (binder.rs + coordinator.rs + explain.rs)

- Provenance: `binding.render_trace.outer_join_demotions.is_empty()` → Native/Demoted at both record
  sites (binding-level, D3); the mixed-binding fixture pins the coarseness.
- Counterfactual (sample-gated, measured path only): when the real render trace retains still-outer
  equi-edges, build an inner-equivalent scratch plan and bind it under `NoOpDecisionRecorder`; run
  the recommendation helpers over the scratch render trace; if a recommendation materializes AND its
  join targets map to real executed targets, add those targets to the measured replay-id allowlist
  for this binding (D4) and emit `PreservedOuterOrder` + provenance + verdict via the existing
  measured math. Unmappable or estimate-incomplete → Indeterminate. One verdict per preserved edge,
  bounded by the existing per-query verdict loop.
- **Verify-first:** the scratch altitude. Prefer a scratch re-derivation so the no-shared-log
  invariant rides the existing no-op-recorder path; only use a contained edge-vector helper if it can
  preserve the same target mapping and no-log guarantees. If it grows beyond a contained helper, stop
  and surface (swap-trigger discipline).
- D5 emission restriction: the measured outer arm can only construct
  MissedWin/Vindicated/Indeterminate — make it structural (a narrow constructor), not a filter.

## Phase 4 — 213 retargets + rendering

- Family partition tree/graph/**outer**; both scope helpers exclude the new kind (exhaustive matches
  force each site); partition test spans three families; symmetric `excluded_sample_size` counts
  outer rows for both knob scopes (D6).
- `optimizer_runtime_verdicts`: `provenance` column at tail; population covers 80 keys
  (zero-omission unchanged).

## Phase 5 — Guards

The seven `adr214_*` from the ADR. Retargets (named): adr207 matrix guards → 80 with codec
assertion; 213 partition/scope guards → three families; verdicts-table completeness → 80 + tail
column. Assert adr102 unchanged. Full architecture binary in the gate.

## Named falsifications (for the review)

1. Kind-replacement for demoted bindings: `tree_advisory_evidence_unchanged_by_provenance_split`
   must fail.
2. Sibling-only write (drop the Total increment): `demoted_star_verdict_accrues_to_total_and_
   demoted_sibling` must fail.
3. Counterfactual on the shared recorder: `counterfactual_bind_is_scratch_and_byte_identical` must
   fail.
4. Drop the replay-id allowlist step: `counterfactual_targets_map_to_real_observations` must fail
   (all-Indeterminate degeneration).
5. Outer kind into a margin advisory scope: `preserved_outer_kind_excluded_from_both_margin_
   advisories` must fail.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib outer_ordering
scripts/cargo-cgroup test  -p toron --lib provenance
scripts/cargo-cgroup test  -p toron --lib counterfactual
scripts/cargo-cgroup test  -p toron --test wendelon_storage_execution_test sibling
scripts/cargo-cgroup test  -p toron --test architecture_test adr214
scripts/cargo-cgroup test  -p toron --test architecture_test adr207
scripts/cargo-cgroup test  -p toron --test architecture_test adr213
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL binary — non-optional
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Hygiene (the bitten-before list)

- No m-word; the ordinal-version terminology guard now polices vocabulary mechanically.
- The durable Total spelling is the one deliberate byte-compat surface — codec-test-pinned; any
  other golden shift is a finding.
- `--all-targets` immediately after the key-struct change (Phase 1).
- LiveWendelonFixture flake discipline (rerun isolated failures 3-5×).
- Plain names; cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. The scratch altitude (Phase 3) — scratch re-derivation preferred; contained helper or
   surface-and-stop if the target mapping gets broad.
2. Native-row rendering: derived rows vs raw totals+demoted rows (explain-site legibility).
3. Whether the replay-id allowlist addition composes at `analyze_observation_mode` or needs a
   binding-level extension (Phase 3's D4 step — ground before building).
