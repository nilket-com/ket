# Impl Plan — Toron ADR 230: Probe-Linearity Dominance Rotation Gate

Pairs with `plans/adr/230_probe_linearity_dominance_rotation_gate.md`. Add the sixth
`optimizer_receipt_gates` row `probe_linearity_fence_dominance`, computed from ADR 229's fence
evidence in the same locked receipt snapshot, plus the actuation-boundary discipline guard.
**No new table / `SystemTableKind` / Rune handle / durable key / daemon flush / Wendelon /
schema change.**

Touch surface:
- `binder/runtime_evidence.rs` — extend `OptimizerReceiptGateSnapshot` with
  `probe_linearity_fences: Vec<OptimizerProbeLinearityFenceRow>` and populate it inside
  `receipt_gate_snapshot()` via the 229 private state-row builder (same locked state); no second
  snapshot, no new method on the public path.
- `binder/join_order.rs` — add the sixth gate row `probe_linearity_fence_dominance` to
  `optimizer_receipt_gate_rows` (after `counter_handoff_readiness`) and the dominance-summary
  helper (status + primary/secondary signal).
- `binder/tests.rs` — the gate discriminators.
- `tests/architecture/{optimizer_accrual (217 5→6), optimizer_memo_admission (223/224/225 manifest), optimizer_probe_linearity (new adr230 guards)}`.

No planner/resolver/fragmenter/executor/daemon/Wendelon change; ADR 229 evidence, the 228
handle, and the 222 windowing gate untouched.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Gate rows manifest | `optimizer_receipt_gate_rows`; existing 5 at join_order.rs:678/797/853/1254/1275 |
| Closed status vocabulary | `OptimizerReceiptGateStatus::{InsufficientEvidence,Inactive,Actionable,BlockedExternalSignal}` (join_order.rs:467) |
| Single locked snapshot | `receipt_gate_snapshot()`; guard `adr217_receipt_gate_snapshot_is_single_locked_snapshot` |
| 229 fence rows + Dominant | `OptimizerProbeLinearityFenceRow`, `OptimizerProbeLinearityFenceStatus::Dominant` (runtime_evidence.rs:1445); builder `probe_linearity_fence_rows_from_state` (1331) |
| Row schema (reuse) | `OptimizerReceiptGateRow` (join_order.rs:475) — `primary_signal`/`secondary_signal` exist |
| 5-row guard to retarget | `adr217_receipt_gates_are_explicit_five_row_system_table` (optimizer_accrual.rs:2032) |
| No-manifest-change guards | `adr223_no_receipt_gate_manifest_change` / `adr224_global_windowing_receipt_gate_manifest_unchanged` / `adr225_no_new_system_table_kind_receipt_gate_or_220_schema_change` (optimizer_memo_admission.rs) |

## Phase 0 — Red discriminators

`probe_linearity_gate_is_cold_without_evidence`;
`probe_linearity_gate_selects_strongest_dominant_fence`;
`probe_linearity_gate_rejects_split_pressure`;
`probe_linearity_gate_uses_same_locked_snapshot_as_229_detail`;
`probe_linearity_gate_requires_dominant_real_lane`. Red on `main`.

## Phase 1 — Snapshot carries 229 rows (single lock)

Extend `OptimizerReceiptGateSnapshot` with `probe_linearity_fences`, populated inside the
existing `receipt_gate_snapshot()` lock by the same `probe_linearity_fence_rows_from_state`
builder 229 uses. Do **not** call `probe_linearity_fence_snapshot()` from the report path (that
re-locks). This keeps the summary consistent with the snapshot-carried 229 rows it consumes
(D4).

## Phase 2 — The sixth gate row (join_order.rs)

A `probe_linearity_fence_dominance(snapshot)` helper: scan `snapshot.probe_linearity_fences`;
`actionable` if any is `Dominant` (pick the strongest by `recent_count`, then 229 manifest
order; `primary_signal = consumer=…,fence=…`, `secondary_signal = recent_count=…,
consumer_recent_total=…,recent_share_per_mille=…,health=…`); `inactive` if recent evidence
exists but none dominant; `insufficient_evidence` if no sample-ready row. Append the row after
`counter_handoff_readiness`. No new status term.

## Phase 3 — Guards + retargets

Add the six `adr230_*` guards (incl. the D6 actuation-boundary guard
`adr230_no_optimizer_actuation_without_actionable_dominance_gate` — pins that no
positive fragmenter/fence-relaxation policy consumes ADR 229 probe-linearity evidence except
via an actionable gate; existing refusal classifiers and committed-decline recording remain
allowed). Retarget `adr217_*` five-row → six; `adr223`/`adr224`/`adr225` manifest guards →
allow the sixth row. Assert 222 windowing gate, 229 evidence, 228 handle, 216 matrix unchanged.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib probe_linearity_gate
scripts/cargo-cgroup test  -p toron --lib receipt_gate
scripts/cargo-cgroup test  -p toron --test architecture_test adr230
scripts/cargo-cgroup test  -p toron --test architecture_test adr217
scripts/cargo-cgroup test  -p toron --test architecture_test adr222
scripts/cargo-cgroup test  -p toron --test architecture_test adr223
scripts/cargo-cgroup test  -p toron --test architecture_test adr224
scripts/cargo-cgroup test  -p toron --test architecture_test adr225
scripts/cargo-cgroup test  -p toron --test architecture_test adr228
scripts/cargo-cgroup test  -p toron --test architecture_test adr229
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL — retargets participate
scripts/cargo-cgroup test  -p toron --lib
scripts/cargo-cgroup fmt -p toron --check
git diff --check
```

## Named falsifications (for the review)

1. Re-locking / calling `probe_linearity_fence_snapshot()` in the report path:
   `probe_linearity_gate_uses_same_locked_snapshot_as_229_detail` fails.
2. Reimplementing dominance math (own floor/majority) instead of consuming 229 status: the
   split-pressure + strongest-fence discriminators diverge from 229 and fail.
3. A plurality shortcut (no strict majority): `probe_linearity_gate_rejects_split_pressure` fails.
4. Seeded detail rows making the gate actionable off the real lane:
   `probe_linearity_gate_requires_dominant_real_lane` fails.
5. Changing the receipt-gate schema or the prior five rows' order: 217 + 223/224/225 retargets fail.
6. A planner/fragmenter path reading the gate or ADR 229 probe-linearity evidence directly for
   positive actuation: `adr230_no_optimizer_actuation_without_actionable_dominance_gate` +
   no-policy-consumer fail.

## Hygiene

- No m-word; no version-suffix idiom; reuse the closed receipt-gate status vocabulary (no new
  term); no new durable keys / goldens (the row is computed from 229's existing evidence).
- `--all-targets` after the snapshot struct extension.
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. Confirm `probe_linearity_fence_rows_from_state` is callable from the `receipt_gate_snapshot()`
   path against the same locked state (visibility); if not, lift it to the shared module — do
   not add a second snapshot.
2. `primary_signal`/`secondary_signal` exact format — pin in the strongest-fence test.
3. The D6 guard's source scan: it should allow existing probe-linearity classifiers/refusals,
   but fail if any positive fragmenter/recognizer/actuation policy consumes ADR 229 evidence
   without going through the actionable gate.
