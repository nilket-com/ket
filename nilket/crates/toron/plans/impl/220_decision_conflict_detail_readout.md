# Impl Plan — Toron ADR 220: Decision-Conflict Detail Readout

Pairs with `plans/adr/220_decision_conflict_detail_readout.md`. A read-only pivot
over the 216 durable decision lane; **no new durable surface**, no policy consumer,
no view (Toron has none — it's a `SystemTableKind` table). The conflict model is the
217 gate's, shared via one report builder.

Touch surface:
- `binder/join_order.rs` — `DecisionConflictReport` + `decision_conflict_reports`
  (promoted from `strongest_decision_conflict_signal`); `optimizer_decision_conflict_rows`.
- `binder/explain.rs` — schema + projection.
- `binder.rs` — table consts + dispatch branch.
- `ir/execution.rs` — `SystemTableKind::InformationSchemaOptimizerDecisionConflicts`.
- `binder/tests.rs` — behavior net.
- `tests/architecture/{optimizer_accrual,optimizer_rotation,nominal_type_identity}.rs` — guards + retargets.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| 217 conflict partition (the model to share) | `strongest_decision_conflict_signal` (join_order.rs:770–800): `Reordered→reordered`; `KeptThinMargin\|Declined(_)→blocked_or_near_miss`; `KeptOptimal => {}` |
| 217 gate row to re-source | `decision_conflict_pressure_gate` (:716); floor `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE=20` (:7) |
| 14-row basis | `decision_recording_kinds()` = 7 (decision_log.rs:151) × `{Native, Demoted}` |
| Single snapshot | `receipt_gate_snapshot()` (runtime_evidence.rs) |
| Raw decisions table (per-reason source of truth) | `information_schema_optimizer_decisions_schema` (explain.rs:262) |
| SystemTableKind (10) + adr102 guards | ir/execution.rs:213; `adr102_system_table_kind_has_finite_introspection_family` + `…_information_schema_dispatch_is_explicit` (nominal_type_identity.rs) |
| 216 matrix guard (stay green) | `adr216_decision_matrix_is_140_keys_and_evidence_matrix_220` (optimizer_accrual.rs:295/303) |
| 219 decision-conflict non-consumer guard (retarget) | `optimizer_rotation.rs` |

## Phase 0 — Red tests first

1. `decision_conflict_gate_and_detail_share_strongest_report` — 217 cockpit row == max-conflict detail row (D4 invariant). Red on main.
2. `decision_conflict_keptoptimal_is_neutral_not_blocked` — pins the model.
3. `optimizer_decision_conflicts_projects_static_14_rows` — cold → 14 rows.

## Phase 1 — Shared typed aggregator (join_order.rs)

`DecisionConflictReport { kind, provenance, reordered, kept_optimal, kept_thin_margin, declined, blocked_or_near_miss, total, conflict_strength, status, top_decline_reason, top_decline_count, evidence_health, reason, next_cut }`. Promote `strongest_decision_conflict_signal` to
`decision_conflict_reports(&OptimizerReceiptGateSnapshot) -> Vec<DecisionConflictReport>`:
initialize **exactly 14** reports (`decision_recording_kinds()` × `{Native, Demoted}`;
assert `len()==14`), keep the **exact** 217 partition (KeptOptimal neutral),
`blocked_or_near_miss = kept_thin_margin + declined`, `conflict_strength = min(...)`,
status by floor=20, top decline reason with stable enum tie-break, and render
`evidence_health` by reusing the 217 gate's evidence-health label. **Refactor
`decision_conflict_pressure_gate` to `max_by` over this same vector** (no second
threshold/comparator) — enforces the D4 invariant with no cockpit behavior change.

## Phase 2 — Rows + schema (join_order.rs + explain.rs)

`optimizer_decision_conflict_rows(config)` renders all 14.
`information_schema_optimizer_decision_conflicts_schema()` — the 15 columns in the
locked order (incl. `evidence_health`, `reason`, `next_cut`); `u64_to_i64` counts;
pin column spelling/order with a codec/golden test (new readout surface).

## Phase 3 — Binder wiring + adr102 retarget

`ir/execution.rs`: add `InformationSchemaOptimizerDecisionConflicts` (10 → 11).
`binder.rs`: table consts + explicit dispatch branch + unsupported-relation message.
Retarget `adr102_system_table_kind_has_finite_introspection_family` (count 11 + the
variant) and `adr102_information_schema_dispatch_is_explicit` (const + branch).

## Phase 4 — Rune parity

`rune_queries_optimizer_decision_conflicts` — identical via Rune (primary) and SQL.

## Phase 5 — Guards + retargets

Add the eight `adr220_*` guards. Retarget the ADR 219 decision-conflict non-consumer
guard to allow this read-only detail readout + the 217 cockpit selector
(resolver/graph/outer policy still forbidden). **Assert
`adr216_decision_matrix_is_140_keys_and_evidence_matrix_220` unchanged**, the 217
five-row cockpit manifest unchanged, and windowing/counter-handoff stay
substrate-blocked.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib decision_conflict
scripts/cargo-cgroup test  -p toron --lib optimizer_decision_conflicts
scripts/cargo-cgroup test  -p toron --test architecture_test adr220
scripts/cargo-cgroup test  -p toron --test architecture_test adr217
scripts/cargo-cgroup test  -p toron --test architecture_test adr216
scripts/cargo-cgroup test  -p toron --test architecture_test adr102
scripts/cargo-cgroup test  -p toron --test architecture_test adr219
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL binary — retargets participate
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. Count `KeptOptimal` as a conflict side: `decision_conflict_keptoptimal_is_neutral_not_blocked` + `adr220_conflict_model_matches_217_gate` fail.
2. Use `>0` instead of the floor: `decision_conflict_detail_marks_conflicted_only_when_both_sides_ready` fails.
3. Compute the detail with a second/independent aggregation: `decision_conflict_gate_and_detail_share_strongest_report` fails (drift).
4. Variable rows / drop cold groups: `optimizer_decision_conflicts_projects_static_14_rows` fails.
5. Derive conflict from verdict rows: `decision_conflict_detail_uses_decision_rows_not_verdicts` fails.
6. Add a durable key or change 140/220: the no-durable / 216 guards fail.
7. Add the SystemTableKind without retargeting both adr102 guards: full `architecture_test` fails.

## Hygiene

- No m-word; no version-suffix/product-stage idiom; no council-internal labels in code/promoted text.
- New readout format surface → pin column spelling + order with a codec/golden test.
- `--all-targets` after the new `SystemTableKind` variant (widely-constructed enum → an E0063/match-arm break can hide in a bench).
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. That `strongest_decision_conflict_signal` promotes to the report vector with the 217 gate as a thin `max_by` wrapper and **no cockpit behavior change** (217 stays coarse + identical).
2. The exact provenance set on decision-snapshot rows (`{Native, Demoted}`) — assert the manifest enumerates exactly 14.
3. `top_decline_reason` tie-break (stable enum order) when two reasons tie; `evidence_health` rendering reuse from the 217 gate row.
