# Impl Plan — Toron ADR 229: Probe-Linearity Fence Cause Readout

Pairs with `plans/adr/229_probe_linearity_fence_cause_readout.md`. Record the typed
`ProbeLinearityFence` at the two committed-decline sites into a durable detail namespace, flush
through the daemon loop, expose `information_schema.optimizer_probe_linearity_fences` (14 rows,
recent-window dominance) with a Rune handle. **Read-only: no plan / fragmenter / policy change;
coarse `ProbeNotScanLinear` and the 140/220 matrix unchanged.**

Touch surface:
- `binder/decision_log.rs` — `ProbeLinearityConsumer` enum + labels; extend
  `OptimizerDecisionRecorder` with a default no-op `record_probe_linearity_fence`.
- `binder/join.rs` — keep `choose_auto_join_strategy` pure, but make `ProbeLinearityFence`
  usable as a counter-key/manifest enum (ordering derives and finite manifest helper if needed);
  no classifier behavior change.
- `binder/runtime_evidence.rs` — probe-linearity lifetime+window key types + prefix;
  `record_probe_linearity_fence(consumer, fence)` (mirror `record_decision`);
  `flush_probe_linearity_deltas` (mirror `flush_decision_deltas`); snapshot rows.
- `binder.rs` — info-schema constants + explicit dispatch arm; record hash-fallback causes
  after `bind_scoped_join` has the committed `DecisionReceipt` reason.
- `binder/join_order.rs` (star accrual, 4222/4226) — record at the **committed** star decline
  site under the recorder guard; **not** join_order.rs:2392 (analyze render).
- `binder/explain.rs` — schema + row rendering (status/reason/next_cut from a typed helper).
- `ir/execution.rs` — `SystemTableKind::InformationSchemaOptimizerProbeLinearityFences` (12→13).
- `rune_engine.rs` — `ctx.optimizer_probe_linearity_fences()` handle; `RUNE_OPTIMIZER_READOUT_TABLES`
  7→8.
- `daemon.rs` — call the probe-linearity flush beside `flush_decision_deltas`.
- tests + `tests/architecture/{optimizer_probe_linearity_fences (new adr229), rune_optimizer_readouts (228→8), optimizer_accrual/adr102, optimizer_evidence_window/adr222}`.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Record pattern | `record_decision` (runtime_evidence.rs:307); `OPTIMIZER_EVIDENCE_*_KEY_PREFIX` (:20/22); `window_pending` |
| Flush pattern | `flush_decision_deltas` + daemon evidence loop |
| Window discipline | `OptimizerEvidenceWindowId`, `all_window_counter_keys(current, previous)` (222) |
| Hash reason source | join.rs:662 constructs `HashByDefaultReason::ProbeNotScanLinear(fence)` |
| Commit sites (record) | `bind_scoped_join` after the final `DecisionReceipt` reason is known; join_order.rs:4222/4226 (star accrual) |
| Render-only (DO NOT record) | join_order.rs:2392 (`analyze()`) |
| Fence enum (sole source) | `ProbeLinearityFence` + `::label()` (join.rs); derive/order or manifest helper for the 7-key loop |
| Floor | `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE` = 20 |
| Recorder guard | `OptimizerDecisionRecorder` vs `NoOpDecisionRecorder` |
| Counts to retarget | `SystemTableKind` 12→13 (adr102); `RUNE_OPTIMIZER_READOUT_TABLES` 7→8 + `adr228_…_are_finite_seven` |

## Phase 0 — Red discriminators

`optimizer_probe_linearity_fences_projects_static_14_rows`;
`real_bind_records_committed_fence_not_analyze_render`;
`probe_linearity_fence_readout_ignores_counterfactual_explain`;
`probe_linearity_fence_dominance_requires_recent_majority`;
`rune_optimizer_probe_linearity_fences_matches_ctx_sql_plan_and_rows`;
`durable_probe_linearity_fence_counters_survive_restart_and_share_across_coordinators`. Red on
`main`.

## Phase 1 — Recorder boundary + evidence namespace (decision_log.rs + runtime_evidence.rs)

`ProbeLinearityConsumer { HashFallback, StarDecline }` lives beside the recorder trait in
`decision_log.rs`, with labels and a default no-op recorder method. `ProbeLinearityFence`
remains in `join.rs`; add only the ordering/manifest surface needed for counter keys and
static rows, not a second classifier. `runtime_evidence.rs` owns the counter keys and caches.
Prefixes:
`optimizer_evidence/probe_linearity/{consumer}/{fence}` for lifetime counters and
`optimizer_evidence/window/{window}/probe_linearity/{consumer}/{fence}` for window counters.
Lifetime + window key types are keyed by `(consumer, fence)`.
`record_probe_linearity_fence` increments lifetime + current-window pending under the
`record_decision` lock. Exhaustive `match` on `ProbeLinearityFence` (no wildcard).
`all_probe_linearity_keys(current, previous)` = 14 lifetime + 28 window, no prefix scan.

## Phase 2 — Record at committed sites (binder.rs + join_order.rs)

In `bind_scoped_join`, after `choose_join_strategy` returns and the final `DecisionReceipt`
reason is known, record
`record_probe_linearity_fence(HashFallback, fence)` for
`HashByDefaultReason::ProbeNotScanLinear(fence)`. Leave join.rs:662 pure: it constructs the
decision reason, but does not write evidence. Record
`record_probe_linearity_fence(StarDecline, fence)` at the star accrual (4222/4226). Both writes
go through the existing recorder guard (no-op for counterfactual/EXPLAIN). Leave 2392 untouched.

## Phase 3 — Flush (runtime_evidence.rs + daemon.rs)

`flush_probe_linearity_deltas` mirrors `flush_decision_deltas` (fetch-add, timeout, ack,
ambiguous-drop, replication latch); call from the daemon loop. Refresh current+previous window
cache like the 222 path.

## Phase 4 — Readout + Rune handle + dispatch

`optimizer_probe_linearity_fence_rows(snapshot)` from one snapshot → the 14-row manifest with
recent-window status (D4). Info-schema schema/rows/constants + explicit dispatch arm +
`SystemTableKind` variant (adr102 12→13). `ctx.optimizer_probe_linearity_fences()` + manifest
7→8 (retarget the 228 finite-seven + no-drift guards).

## Phase 5 — Tests + guards

ADR honesty gates + the seven `adr229_*` guards. Retarget adr228 (8 handles), adr102 (13),
adr222 (sibling namespace allowed; decision-window manifest unchanged). Assert adr216 (140/220),
adr227 (fence taxonomy), adr226 (locks) unchanged.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib probe_linearity
scripts/cargo-cgroup test  -p toron --lib optimizer_probe_linearity_fences
scripts/cargo-cgroup test  -p toron --test architecture_test adr229
scripts/cargo-cgroup test  -p toron --test architecture_test adr228
scripts/cargo-cgroup test  -p toron --test architecture_test adr222
scripts/cargo-cgroup test  -p toron --test architecture_test adr102
scripts/cargo-cgroup test  -p toron --test architecture_test adr216
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL — retargets participate
scripts/cargo-cgroup test  -p toron --test wendelon_storage_execution_test probe_linearity
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. Recording at `join.rs:662` or the `analyze()` render (2392): `real_bind_records_committed_fence_not_analyze_render` fails.
2. EXPLAIN/no-op bind increments a count: `probe_linearity_fence_readout_ignores_counterfactual_explain` fails.
3. Lifetime-based dominance: `probe_linearity_fence_dominance_requires_recent_majority` fails.
4. New `OptimizerDecisionOutcome` / matrix off 140/220: adr216 + the detail-namespace guard fail.
5. SQL-only readout (no Rune handle): `adr229_probe_linearity_rune_handle_present_and_aliased` + the retargeted adr228 count fail.
6. Plan change: `probe_linearity_fence_readout_is_read_only_no_plan_change` fails.
7. Process-local-only counters or refresh that omits the new namespace:
   `durable_probe_linearity_fence_counters_survive_restart_and_share_across_coordinators` fails.

## Hygiene

- No m-word; no version-suffix idiom; the cause stays typed until render (no parallel string
  classifier); coarse decline reason + matrix untouched.
- New durable key strings (the probe-linearity namespace) → pin the spelling + the bounded
  current+previous manifest with a codec/unit test.
- `ProbeLinearityFence` may gain ordering derives/manifest helpers for BTreeMap/static-row use,
  but its classifier and labels remain the ADR 227 source of truth.
- `--all-targets` after the snapshot/enum/`SystemTableKind` additions.
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. Confirm the hash recording site is in `bind_scoped_join` after the final decision reason is
   known, and the star committed-decline accrual is exactly 4222/4226 (with 2392 render-only),
   so recording lands on real binds only — the cardinal correctness point.
2. Probe-linearity window-key GC: additive like 222 (≤ 28 keys/window); record the rate, do not
   block on GC.
3. `recent_share_per_mille` rounding convention — pin it in the dominance test.
