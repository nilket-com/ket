# Impl Plan — Toron ADR 217: Receipt-Gated Optimizer Rotation Readout

Pairs with `plans/adr/217_receipt_gated_optimizer_rotation_readout.md`. A
read-only instrument over the 207→216 durable substrate — no planner path
changes, no new durable key/counter/flush.

Touch surface:
- `binder/runtime_evidence.rs` — one report-only, single-lock snapshot.
- `binder/join_order.rs` — gate-row derivation (reuses the advisory margin math).
- `binder/explain.rs` — schema + scalar row projection.
- `binder.rs` — information-schema dispatch + table-name consts.
- `ir/execution.rs` — `SystemTableKind::InformationSchemaOptimizerReceiptGates`.
- Tests and architecture guards.

No Wendelon production change.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Existing optimizer tables | `bind_information_schema_relation` in `binder.rs`; schemas in `binder/explain.rs` (`information_schema_optimizer_{decisions,runtime_verdicts,margin_advisory}_schema`, :262/307/384) |
| Verdict / decision / health rows | `verdict_snapshot()` / `decision_snapshot()` / `status_snapshot()` (runtime_evidence.rs) |
| Graph family/detail math + floor | `margin_advisory_report_rows`, `is_graph_margin_advisory_kind` (join_order.rs:189), `recommend_soft_margin` (:103), `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE=20` (:7) |
| Outer kind (verdict lane) | `OptimizerDecisionKind::PreservedOuterOrder` — excluded from `decision_recording_kinds()` and tree/graph predicates |
| SystemTableKind (9 variants) | `pub enum SystemTableKind` ir/execution.rs:213 |
| adr102 guards | `adr102_system_table_kind_has_finite_introspection_family` + `adr102_information_schema_dispatch_is_explicit` (nominal_type_identity.rs:2101,2120) |
| 216 read-only guard | `adr216_decision_telemetry_is_read_only_no_policy_consumers` (optimizer_accrual.rs:384) |

## Phase 0 — Red tests first

1. `optimizer_receipt_gates_projects_five_static_rows` — red on main.
2. `receipt_gates_render_insufficient_evidence_on_cold_ledger`.
3. `receipt_gate_readout_is_read_only_no_plan_change` — Manual + ApplyAdvisory binding byte-identical with/without the readout (written before any derivation).
4. `windowing_and_counter_handoff_never_actionable`.

## Phase 1 — Single runtime-evidence snapshot (runtime_evidence.rs)

`pub(crate) struct OptimizerReceiptGateSnapshot { verdicts, decisions, health }` +
`receipt_gate_snapshot(&self) -> OptimizerReceiptGateSnapshot` building all three
parts under **one** state-lock acquisition (D3). Extract shared private
row-builders rather than calling the three public `*_snapshot()` methods (which
would relock and risk an inconsistent cockpit). Unit test: interleaved
pending/in-flight deltas in both lanes appear in one returned snapshot (a
consistency pin, not a durability fixture).

## Phase 2 — Gate model + derivation (join_order.rs)

- `enum OptimizerReceiptGateStatus { InsufficientEvidence, Inactive, Actionable, BlockedExternalSignal }`.
- `enum`/manifest of the five gate names + `struct OptimizerReceiptGateRow { gate_name, gate_family, status, sample_count, primary_signal, secondary_signal, reason, next_cut }`.
- `pub(crate) fn optimizer_receipt_gate_rows(config: &PlannerConfig) -> Vec<OptimizerReceiptGateRow>` — consumes the Phase-1 snapshot once; stable row order. Labels lower_snake_case, pinned by tests.
- Derivation (D5):
  - **graph_margin_divergence:** reuse the `MarginAdvisoryScope::GraphKind` path + `recommend_soft_margin`; a kind is "ready" when its advisory ≠ `InsufficientEvidence`; `Actionable` iff ≥2 ready kinds with **distinct** `recommended_step`; `sample_count` = sum of ready-kind samples; `primary_signal` names the divergent scopes.
  - **preserved_outer_ordering:** filter snapshot verdicts to `PreservedOuterOrder`; `recommend_soft_margin` over that slice; `Actionable` only on a sample-ready `Loosen`; never use `tree_/graph_margin_advisory_input`.
  - **decision_conflict_pressure:** group decision rows by `(kind, provenance)`; per group `reordered` vs `blocked_or_near_miss = KeptThinMargin + Declined(*)`; `Actionable` only when both ≥ floor (20); strongest group → `primary_signal`, ties by stable kind/provenance order.
  - **windowing_decay_pressure:** always `Inactive`, reason `substrate_unbuilt`; cumulative magnitude in `reason` if cheap; never `Actionable`.
  - **counter_handoff_readiness:** always `BlockedExternalSignal`, reason `elasticity_signal_unbuilt`; no placeholder config flag; never `Actionable`.

## Phase 3 — Schema + projection (explain.rs)

- `information_schema_optimizer_receipt_gates_schema() -> SchemaRef` — column order: `gate_name`, `gate_family`, `status`, `sample_count`, `primary_signal`, `secondary_signal`, `reason`, `next_cut`.
- `information_schema_optimizer_receipt_gate_rows(&[OptimizerReceiptGateRow]) -> Result<Vec<Vec<ScalarValue>>, BindingError>` — reuse the existing `u64_to_i64` count style. Pin column spelling + order with a codec/golden test (new readout surface; no existing goldens).

## Phase 4 — Binder wiring + adr102 retargets (binder.rs + ir/execution.rs + guard)

- `ir/execution.rs`: add `SystemTableKind::InformationSchemaOptimizerReceiptGates` (9 → 10).
- `binder.rs`: add `INFORMATION_SCHEMA_OPTIMIZER_RECEIPT_GATES` + table-name consts; add the explicit dispatch branch in `bind_information_schema_relation`; update the unsupported-relation message.
- Retarget `adr102_system_table_kind_has_finite_introspection_family` (count 10 + the new variant) and `adr102_information_schema_dispatch_is_explicit` (the new const + branch) — the named reconciliations (do not leave to fail / silently delete).

## Phase 5 — Rune parity

`rune_queries_optimizer_receipt_gates` — the table is reachable and identical via
the Rune frontend (primary) and SQL (compatibility), the 213/214/216 pattern.

## Phase 6 — Architecture guards + remaining retargets (optimizer_accrual.rs)

Add the nine `adr217_*` guards from the ADR. Retarget:
- `adr216_decision_telemetry_is_read_only_no_policy_consumers` — allow the
  receipt-gate report surface; keep `decision_snapshot()`/gate rows out of
  planner/resolver policy paths.
- ADR 213/215 graph-advisory + ADR 214 outer-family guards — assert family graph
  application and the outer exclusion are unchanged (the readout only observes).

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib receipt_gate
scripts/cargo-cgroup test  -p toron --lib optimizer_receipt
scripts/cargo-cgroup test  -p toron --test architecture_test adr217
scripts/cargo-cgroup test  -p toron --test architecture_test adr102
scripts/cargo-cgroup test  -p toron --test architecture_test adr216
scripts/cargo-cgroup test  -p toron --test architecture_test adr213
scripts/cargo-cgroup test  -p toron --test architecture_test adr214
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL binary — retargets all participate
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

No Wendelon fixture is required for this readout unless implementation unexpectedly
touches durable flush/counter paths. It should not.

## Named falsifications (for the review)

1. Make graph divergence actionable with one ready kind, or aggregate graph kinds: `graph_kind_divergence_row_requires_two_ready_distinct_recommendations` fails.
2. Feed `PreservedOuterOrder` through tree/graph margin scope: the outer-separation gate fails.
3. Read verdicts for the conflict gate instead of the decision lane: `decision_conflict_pressure_uses_durable_decision_rows` fails.
4. Re-snapshot per row: `receipt_gates_use_single_evidence_snapshot` fails.
5. Let any gate touch a planner path: `receipt_gate_readout_is_read_only_no_plan_change` fails.
6. Render windowing/counter-handoff `actionable` (or counter-handoff `inactive` instead of `blocked_external_signal`): `windowing_and_counter_handoff_never_actionable` fails.
7. Add the SystemTableKind without retargeting both adr102 guards: the full `architecture_test` fails.

## Hygiene

- No m-word; no version-suffix/product-stage idiom; no council-internal labels in code/promoted text (write the gate referent).
- New readout format surface → pin column spelling + order with a codec/golden test.
- `--all-targets` after the new `SystemTableKind` variant (widely-constructed enum → an E0063/match-arm break can hide in a bench).
- Full architecture binary is non-optional — adr102/213/214/216 retargets all participate.
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. Whether per-kind verdict rows are cleanly separable in the snapshot for the three graph kinds, or whether the report re-derives the partition via `is_graph_margin_advisory_kind`.
2. The conflict-ratio threshold (reuse floor=20 for sample-readiness; the conflict mix is the one new number — keep it simple, pin it).
3. Whether `receipt_gate_snapshot()` can reuse private row-builders without a second lock, or needs a small refactor of the existing `*_snapshot()` internals.
