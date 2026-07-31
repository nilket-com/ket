# Impl Plan - Toron ADR 223: Conflict-Scoped Memo Admission Readout

Pairs with `plans/adr/223_conflict_scoped_memo_admission_readout.md`. Adds a
read-only `information_schema.optimizer_memo_admission` table over the existing
decision-conflict and window-health snapshot. No memo engine, no planner behavior,
no durable evidence change.

Touch surface:

- `binder/join_order.rs` - typed admission structs/status labels, row builder,
  window-health helper, bounded-shape classifier stub.
- `binder/explain.rs` - schema + row rendering for the new table.
- `binder.rs` - information_schema constants and explicit dispatch.
- `ir/execution.rs` - one new `SystemTableKind` variant.
- `binder/tests.rs` - behavior and Rune parity tests.
- `tests/architecture/{optimizer_memo_admission,optimizer_rotation,optimizer_evidence_window,nominal_type_identity}.rs` -
  ADR223 guards and named retargets.

No `runtime_evidence.rs`, no daemon, no executor/fragmenter/storage/wire/Wendelon
change. No new durable key strings.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Conflict detail aggregation | `decision_conflict_reports(&OptimizerReceiptGateSnapshot)` in `join_order.rs` |
| Conflict statuses | `DecisionConflictStatus::{Cold, Inactive, OneSidedReordered, OneSidedBlocked, Conflicted}` |
| Window health math | `windowing_decay_pressure_gate(&snapshot)` and `snapshot.window` |
| Single evidence snapshot | `OptimizerRuntimeEvidence::receipt_gate_snapshot()` |
| Existing table pattern | `information_schema_optimizer_decision_conflicts_*` and `optimizer_receipt_gates_*` in `binder/explain.rs` |
| Explicit dispatch | `bind_information_schema_relation` in `binder.rs` |
| System table family count | `SystemTableKind` in `ir/execution.rs` |
| No-policy-consumer pins | `optimizer_rotation.rs` ADR219/220/221 guards and `optimizer_evidence_window.rs` ADR222 guard |

## Phase 0 - Red tests first

Add the behavior tests before implementation:

1. `optimizer_memo_admission_projects_static_14_rows` - red because the table does
   not exist.
2. `memo_admission_stale_when_conflict_recent_window_missing` - use the ADR222 manual
   window source to move current evidence out of the current+previous window.
3. `memo_admission_blocked_shape_when_conflicted_recent_and_healthy` - seed a
   conflicted row in the current window; expect `blocked_shape`, not `admissible`.
4. `memo_admission_readout_is_read_only_no_plan_change`.
5. `rune_queries_optimizer_memo_admission`.

## Phase 1 - Typed admission model (join_order.rs)

Add:

```rust
pub(crate) enum MemoAdmissionStatus {
    Cold,
    StaleEvidence,
    DegradedEvidence,
    BlockedShape,
    Admissible,
}

pub(crate) struct MemoAdmissionRow {
    kind: OptimizerDecisionKind,
    provenance: OptimizerRuntimeVerdictProvenance,
    status: MemoAdmissionStatus,
    conflict_strength: u64,
    recent_window: u64,
    stale_lifetime: u64,
    lifetime_total: u64,
    bounded_candidate_count: u64,
    evidence_health: String,
    reason: String,
    next_cut: &'static str,
}
```

Keep names local to the readout. Do not create `src/optimizer.rs`, a memo arena, a
candidate graph, or a planner policy enum.

Factor the 222 window row math into a typed helper if needed:

```rust
fn windowing_decay_pressure(snapshot: &OptimizerReceiptGateSnapshot)
    -> WindowingDecayPressure
```

`windowing_decay_pressure_gate` should render from the helper, and memo admission
should consume the helper. This avoids parsing `primary_signal` strings and pins
that both readouts use identical window health.

## Phase 2 - Admission row builder

Add:

```rust
pub(crate) fn optimizer_memo_admission_rows(config: &PlannerConfig) -> Vec<MemoAdmissionRow>
```

It must:

1. Take exactly one `receipt_gate_snapshot()`.
2. Call `decision_conflict_reports(&snapshot)` once.
3. Derive typed window health from the same snapshot.
4. Iterate all 14 reports in their existing ADR220 order.
5. Compute status:
   - non-`Conflicted` -> `Cold`
   - conflicted but window health is not `Inactive` for
     `recent_window_matches_lifetime` -> `StaleEvidence`
   - conflicted and recent but `snapshot.health.state != Ok` -> `DegradedEvidence`
   - conflicted, recent, healthy, and no bounded shape contract -> `BlockedShape`
   - only if a future classifier returns count >= 2 -> `Admissible`

Initial bounded-shape helper:

```rust
fn bounded_memo_candidate_contract(
    kind: OptimizerDecisionKind,
    provenance: OptimizerRuntimeVerdictProvenance,
) -> MemoCandidateContract
```

It may return blocked for every group in this ADR. The guard should pin that
`BlockedShape` is not treated as `Admissible`.

Window health is consumed GLOBALLY here (one `windowing_decay_pressure` status for all
rows), which is sufficient because no row is `Admissible` (D4). Do **not** pre-build
per-group window recency in this cut; it is the named prerequisite for the actuation
ADR (when `Admissible` becomes reachable, "recent" must be the group's own window, not
the global substrate — computable from 222's per-key window namespace).

## Phase 3 - Information schema surface

Follow the `optimizer_decision_conflicts` pattern:

- `INFORMATION_SCHEMA_OPTIMIZER_MEMO_ADMISSION`
- `INFORMATION_SCHEMA_OPTIMIZER_MEMO_ADMISSION_TABLE`
- `SystemTableKind::InformationSchemaOptimizerMemoAdmission`
- `information_schema_optimizer_memo_admission_schema()`
- `information_schema_optimizer_memo_admission_rows(&rows)`
- explicit `bind_information_schema_relation` arm

Schema order:

```text
decision_kind, provenance, admission_status, conflict_strength,
recent_window, stale_lifetime, lifetime_total, bounded_candidate_count,
evidence_health, reason, next_cut
```

Retarget ADR102 guards from 11 to 12 and add the explicit branch. Keep the 217
receipt-gate five-row manifest unchanged.

## Phase 4 - Tests and guard retargets

Behavior:

- `optimizer_memo_admission_projects_static_14_rows`
- `memo_admission_cold_when_conflict_not_conflicted`
- `memo_admission_stale_when_conflict_recent_window_missing`
- `memo_admission_degraded_when_evidence_health_not_ok`
- `memo_admission_blocked_shape_when_conflicted_recent_and_healthy`
- `memo_admission_uses_same_snapshot_as_conflict_and_window_health`
- `memo_admission_readout_is_read_only_no_plan_change`
- `rune_queries_optimizer_memo_admission`
- `memo_admission_adds_no_durable_key_counter_or_flush`

Architecture:

- Add `tests/architecture/optimizer_memo_admission.rs` with the eight ADR223 guards.
- Retarget ADR220 no-policy-consumer guard to allow the read-only admission helper.
- Retarget ADR222 no-policy-consumer guard to allow the admission helper to observe
  typed window health, while continuing to ban planner consumption.
- Retarget ADR221/ADR197 no-memo fences to allow the readout name/helper only. Keep
  bans on `src/optimizer.rs`, `src/optimizer/`, `fn memo`, `Cascades`, memo arenas,
  candidate memoization, and policy consumers.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib memo_admission
scripts/cargo-cgroup test  -p toron --lib optimizer_memo
scripts/cargo-cgroup test  -p toron --test architecture_test adr223
scripts/cargo-cgroup test  -p toron --test architecture_test adr220
scripts/cargo-cgroup test  -p toron --test architecture_test adr222
scripts/cargo-cgroup test  -p toron --test architecture_test adr221
scripts/cargo-cgroup test  -p toron --test architecture_test adr102
scripts/cargo-cgroup test  -p toron --test architecture_test
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. Mark conflicted+recent rows admissible without a bounded contract:
   `memo_admission_blocked_shape_when_conflicted_recent_and_healthy` fails.
2. Compute admission from lifetime totals only: the stale-window discriminator fails.
3. Re-snapshot decisions/window separately: the single-snapshot guard fails.
4. Add a planner consumer of memo admission: read-only/no-policy guards fail.
5. Add `src/optimizer.rs`, `fn memo`, `Cascades`, a memo arena, or candidate
   memoization: no-engine guards fail.
6. Add durable keys/flush paths: ADR216/222 durable-surface guards fail.
7. Change receipt-gate five-row cockpit: ADR217 manifest guard fails.

## Hygiene

- Source code may contain the table/helper name `optimizer_memo_admission`; it must
  not contain a memo engine.
- Do not add a config knob, planner mode, or admission flag.
- Do not parse `primary_signal` strings; share typed helper output.
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. Whether the first cut returns `BlockedShape` for every group or admits one tiny
   pre-existing bounded contract. Lean: all blocked, because this ADR is the contract
   readout and not a search implementation.
2. Exact health condition: start with `OptimizerEvidenceState::Ok`; mode may be
   `LocalOnly` in tests, while production durability is reflected by the status table.
3. Whether to expose `bounded_candidate_count = 0` or nullable; choose non-null zero
   to keep the system-table schema simple and finite.
