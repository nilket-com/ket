# Impl Plan — Toron ADR 224: Conflict-Group Window Recency

Pairs with `plans/adr/0224_conflict_group_window_recency.md`. A read-only consumer-side
refinement: project per-`(kind, provenance)` current+previous decision-window evidence
into the existing one-lock snapshot, and make `optimizer_memo_admission`'s
`stale_evidence`/`blocked_shape` boundary row-local. **No new durable surface, no new
table/SystemTableKind, no engine, no fence retirement; still zero `admissible`.**

Touch surface:
- `binder/runtime_evidence.rs` — outcome-bearing current+previous decision-window rows on
  `OptimizerReceiptGateSnapshot`, projected `native = total - demoted` under the snapshot lock.
- `binder/join_order.rs` — group-local recent-conflict helper (220 model over the recent
  window); row-local recency in `optimizer_memo_admission_row`; group-local window columns.
- `binder/tests.rs` — behavior net.
- `tests/architecture/{optimizer_memo_admission,optimizer_evidence_window}.rs` — `adr224_*` guards + 222/223 retargets.

No daemon, executor, fragmenter, storage, wire, or Wendelon change; no new durable keys.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Per-key window namespace | `OptimizerEvidenceWindowCounterKey::Decision { window, key }`; `all_window_counter_keys(current, previous)` |
| Native/demoted projection to mirror | `decision_rows_from_state` → `native = total.saturating_sub(demoted)`, demoted separate |
| 220 conflict model + floor | `decision_conflict_reports`; `conflict_strength = reordered.min(blocked_or_near_miss)` (join_order.rs:931); `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE` |
| 223 readout to refine | `optimizer_memo_admission_row` / global `windowing_decay_pressure(snapshot)` (join_order.rs:1036–1039) |
| Single snapshot | `receipt_gate_snapshot()` |
| Window-id test control | `set_window_id_for_test` / `local_only_with_window_id` |

## Phase 0 — Red tests first

1. `memo_admission_stale_when_group_conflict_is_outside_recent_window` — the cardinal (global fresh, group stale → `stale_evidence`). Red on main.
2. `memo_admission_blocked_shape_when_same_group_conflict_is_recent`.
3. `memo_admission_group_recency_is_lane_scoped`.
4. `memo_admission_group_recency_requires_recent_conflict_not_one_sided_activity`.

## Phase 1 — Group-window decision projection (runtime_evidence.rs)

Under the `receipt_gate_snapshot()` lock, derive current+previous per-key **decision**
window counts from `window_pending` + `window_in_flight` + `window_durable_cache`
(restricted to the current+previous window ids — the same ids the global
`OptimizerEvidenceWindowSnapshot` uses). Project `native = total.saturating_sub(demoted)`,
demoted separate (mirror `decision_rows_from_state`). Expose outcome-bearing recent
decision rows on `OptimizerReceiptGateSnapshot`, preferably:

```rust
pub(crate) recent_decisions: Vec<OptimizerDecisionRow>
```

Do not collapse the snapshot view to `(kind, provenance)` aggregates in
`runtime_evidence.rs`; the point of this cut is that the recent path can reuse the 220
conflict model over the same outcome-bearing row shape as the lifetime path. No durable
store.

## Phase 2 — Group-local recent-conflict helper (join_order.rs)

Refactor the 220 aggregation so both lifetime and recent rows use the same helper:

```rust
fn decision_conflict_reports_from_rows(
    rows: &[OptimizerDecisionRow],
    health: &OptimizerEvidenceStatusSnapshot,
) -> Vec<DecisionConflictReport>
```

`decision_conflict_reports(&snapshot)` remains the lifetime wrapper over
`snapshot.decisions`; admission also builds recent reports from `snapshot.recent_decisions`.
Recent iff the matching recent report is `DecisionConflictStatus::Conflicted`, equivalent
to `min(recent_reordered, recent_blocked_or_near_miss) >= JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE`.
This keeps KeptOptimal neutral, keeps Declined summed through the shared model, and keeps
lane scoping tied to the existing native/demoted row projection.

## Phase 3 — Row-local admission recency + columns (join_order.rs)

In `optimizer_memo_admission_rows_from_snapshot`, build lifetime reports and recent
reports, then match by `(decision_kind, provenance)` in the fixed 14-row manifest. In
`optimizer_memo_admission_row`, replace the global recency check with the matching recent
report's `Conflicted` status. Status: lifetime-conflicted & not recent →
`stale_evidence`; recent & health != Ok → `degraded_evidence`; recent & healthy →
`blocked_shape` (bounded_candidate_count still 0 → never `admissible`). Set the window
columns group-local (D5): `recent_window = recent conflict_relevant`,
`lifetime_total = lifetime conflict_relevant`, `stale_lifetime =
lifetime_total.saturating_sub(recent_window)`; `conflict_strength` stays the lifetime
`min`. 14-row manifest + schema unchanged.

## Phase 4 — Tests + guards

Behavior: the ADR's discriminators (cardinal stale, blocked-recent, lane-scoped,
conflict-not-activity, group-local columns, single-snapshot, zero-admissible,
global-cockpit-unchanged, Rune parity, no-durable-key). Guards: the ten `adr224_*`.
Retarget the ADR 222 snapshot guard (allow the per-key decision-window view; window-key
matrix + flush unchanged) and the ADR 223 recency guard (global → group-local). Assert
217 five-row cockpit, 216 140/220, the 222 window-key count, and the no-memo fences
unchanged.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib memo_admission
scripts/cargo-cgroup test  -p toron --lib window_recency
scripts/cargo-cgroup test  -p toron --test architecture_test adr224
scripts/cargo-cgroup test  -p toron --test architecture_test adr223
scripts/cargo-cgroup test  -p toron --test architecture_test adr222
scripts/cargo-cgroup test  -p toron --test architecture_test adr220
scripts/cargo-cgroup test  -p toron --test architecture_test adr217
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL binary — 222/223 retargets participate
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. Use global window health for the row: the cardinal stale test fails.
2. Use sum instead of `min` (one side recent counts as recent): `…requires_recent_conflict_not_one_sided_activity` fails.
3. Refresh a lane from its sibling: `…group_recency_is_lane_scoped` fails.
4. Leave the window columns global: `memo_admission_window_columns_are_group_local_conflict_samples` fails.
5. Render any `admissible` row: `memo_admission_still_renders_zero_admissible` fails.
6. Add a durable key / second snapshot: the no-durable / single-snapshot guards fail.
7. Change the 222 cockpit five-row manifest: the global-manifest guard fails.

## Hygiene

- No m-word; no version-suffix idiom; `memo`/`Cascades` stay out of engine surfaces (fences unchanged; the readout name pre-exists from 223).
- No new durable key strings; no goldens (read-only projection of existing window keys).
- `--all-targets` after the snapshot struct extension.
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. The recent decision-window view should carry outcome-bearing rows (prefer
   `Vec<OptimizerDecisionRow>`), not only the two aggregates per `(kind, provenance)`;
   the aggregates belong in the shared 220 report helper.
2. Confirm the projection reuses the exact current+previous window ids the global
   `OptimizerEvidenceWindowSnapshot` uses (one source of window-id truth).
3. The `degraded_evidence` precedence vs recency (recent + health-not-Ok → degraded, per D6).
