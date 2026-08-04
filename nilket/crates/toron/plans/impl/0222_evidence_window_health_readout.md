# Impl Plan — Toron ADR 222: Evidence-Window Health Readout

Pairs with `plans/adr/0222_evidence_window_health_readout.md`. Adds a bounded durable
window side-namespace to runtime evidence and retargets the existing
`windowing_decay_pressure` cockpit row to a computed status. No new table, no policy
consumer, no `SystemTableKind`. The lifetime 80/140/220 matrix is untouched.

Touch surface:
- `binder/runtime_evidence.rs` — window key types, pending/in-flight/cache maps, key
  encoding, injected window-id source, dual recording, bounded refresh/flush, snapshot
  extension.
- `binder/join_order.rs` — computed `windowing_decay_pressure_gate`.
- `daemon.rs` — the 60s evidence loop calls `flush_window_deltas` next to `flush_decision_deltas`.
- `binder/tests.rs` — behavior net.
- `tests/architecture/{optimizer_accrual,optimizer_decision_conflicts,optimizer_rotation}.rs` — `adr222_*` guards + 217/220 retargets and the no-policy-consumer pin.

No `ir/execution.rs`, no `binder.rs` info-schema dispatch, no `explain.rs` schema, no
executor/fragmenter/storage/wire/Wendelon change.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Placeholder to retire | `windowing_decay_pressure_gate` (join_order.rs) — hard-coded `Inactive`/`windowing_decay_substrate_unbuilt` |
| Single snapshot | `receipt_gate_snapshot()` (runtime_evidence.rs) |
| Lifetime keys to preserve | `all_verdict_counter_keys` / `all_decision_counter_keys` |
| Local record path (one extra increment) | `record_verdict`/`record_decision` (:172/199) |
| Flush precedent to mirror | `flush_decision_deltas` (:253) |
| 60s loop to ride | `OPTIMIZER_EVIDENCE_REFRESH_INTERVAL` (daemon.rs:73), tick :306–313 |
| Blocked-placeholder guards to retarget | `adr217_blocked_placeholders_cannot_render_actionable` (optimizer_accrual.rs:2185); `adr220_windowing_and_counter_handoff_stay_substrate_blocked` |
| Floor | `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE` |

## Phase 0 — Red tests first

1. `windowing_gate_actionable_when_lifetime_dominated_by_stale_windows` — seed lifetime + stale/empty windows → `actionable`. Red on main.
2. `windowing_gate_inactive_when_recent_window_matches_lifetime` — large lifetime counts stay inactive when the same evidence is present in the current+previous window; this is the discriminator against lifetime-total-only actionability.
3. `lifetime_matrix_counts_unchanged_by_windowing` (80/140/220).
4. `counter_handoff_readiness_stays_blocked_external_signal`.

## Phase 1 — Window model (runtime_evidence.rs)

`OPTIMIZER_EVIDENCE_WINDOW_KEY_PREFIX = "optimizer_evidence/window/"`;
`OptimizerEvidenceWindowId(u64)`; `OptimizerEvidenceWindowCounterKey::{Verdict{window,key}, Decision{window,key}}`.
Add an injected window-id source (production default + a test setter, deterministic).
Extend `RuntimeEvidenceState` with `window_pending`/`window_in_flight`/`window_durable_cache`.
`record_verdict`/`record_decision` increment the current-window key **under the same
lock** as the lifetime key (Total + Demoted lanes match lifetime semantics). Gate math
consumes snapshots only — no wall-clock in the gate.

## Phase 2 — Durable keys + bounded refresh/flush

Key encoding: `optimizer_evidence/window/{window_id}/verdict/{lifetime_verdict_suffix}`
and `…/decision/{lifetime_decision_suffix}`. `all_window_counter_keys(current, previous)`
emits **only** the two ids × the 220 lifetime suffixes (guarded; no prefix scan).
`flush_window_deltas()` mirrors `flush_decision_deltas` (fetch-add, timeout, ack,
ambiguous-drop, replication-unsupported latch); call it from the daemon loop next to
`flush_decision_deltas`. `refresh_durable_cache` reads current+previous window keys after
the lifetime keys into `window_durable_cache` (older cached windows untouched; snapshot
math considers only current+previous).

## Phase 3 — Snapshot + computed gate

`OptimizerEvidenceWindowSnapshot { current_window, previous_window, recent_count, stale_lifetime_count, lifetime_count }`,
built inside the existing `receipt_gate_snapshot()` lock; extend
`OptimizerReceiptGateSnapshot` with it. Retarget `windowing_decay_pressure_gate(&snapshot)`
to the D5 math (insufficient/inactive/actionable), `sample_count = lifetime_count`,
`primary_signal = "recent_window=…,stale_lifetime=…,lifetime_total=…"`, reasons
`requires_lifetime_evidence_floor` / `recent_window_matches_lifetime` /
`stale_window_evidence_dominates`. Do not reference any optimizer-family report.

## Phase 4 — Tests + guards

Split the old `windowing_and_counter_handoff_never_actionable` into
`windowing_gate_inactive_when_recent_window_matches_lifetime` +
`counter_handoff_readiness_stays_blocked_external_signal`. Add the ADR's discriminators,
read-only, single-snapshot, Rune parity, and runtime unit tests for key encoding +
bounded manifest. Add the eight `adr222_*` guards; retarget
`adr217_blocked_placeholders_cannot_render_actionable` and
`adr220_windowing_and_counter_handoff_stay_substrate_blocked` (windowing now built;
counter still blocked); assert 216 (140/220) and adr102 unchanged.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib windowing
scripts/cargo-cgroup test  -p toron --lib optimizer_receipt_gate
scripts/cargo-cgroup test  -p toron --test architecture_test adr222
scripts/cargo-cgroup test  -p toron --test architecture_test adr217
scripts/cargo-cgroup test  -p toron --test architecture_test adr220
scripts/cargo-cgroup test  -p toron --test architecture_test adr216
scripts/cargo-cgroup test  -p toron --test architecture_test adr102
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL binary — retargets participate
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. Compute the gate from lifetime totals: `windowing_gate_inactive_when_recent_window_matches_lifetime` fails.
2. Add window keys to the lifetime matrices: 216 continuity fails.
3. Prefix-scan / read all historical windows: the bounded-manifest guard fails.
4. Separate snapshot locks for lifetime vs window: the single-snapshot guard fails.
5. Add a window detail table / `SystemTableKind`: the adr102/no-new-table guard fails.
6. Wire the window gate into a planner path: the read-only/no-consumer guards fail.
7. Make counter handoff actionable: the counter-blocked guard fails.

## Hygiene

- No m-word; no version-suffix idiom; `memo`/`Cascades` stay out of source (deferred).
- New durable key strings (the window namespace) → pin the spelling + the bounded
  current+previous manifest with a codec/unit test; the lifetime matrix has no new keys.
- `--all-targets` after the snapshot struct + state-map extensions.
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. Production window duration — start with the 60s evidence interval as the bucket
   width; tests must not depend on wall time (use the injected source).
2. Refresh reads both current+previous every interval (simpler) vs current-only +
   previous-on-boundary — lean: both, fixed manifest.
3. The actionable comparator `stale_lifetime >= floor && stale_lifetime > recent_window`
   — keep it simple and pin it; a stricter ratio is a future policy readout if needed.
4. **Window-key GC is a named deferral** — the additive namespace grows unbounded; a
   later cut adds expiry/GC. Do not block 222 on it, but record the growth rate
   (≤ 220 keys per window id).
