# Impl Plan — Toron ADR 225: Memo Admission Blocker-Class Readout

Pairs with `plans/adr/225_memo_admission_blocker_class_readout.md`. A read-only consumer-side
projection of fields the ADR 220 conflict report already carries: classify the blocked side
into margin / hard / capability, surface it on `information_schema.optimizer_memo_admission`,
and make `reason`/`next_cut` honest per blocker. **No new evidence, no new durable surface,
no new `SystemTableKind`, no ADR 220 table change, no engine, zero `admissible`.**

Touch surface:
- `binder/join_order.rs` — `margin_blocked_count`/`hard_decline_count`/
  `capability_decline_count` on `DecisionConflictReport` (preserve
  `blocked_or_near_miss_count` + `conflict_strength`); decline-class partition; blocker
  classifier; blocker fields + `reason`/`next_cut` on the memo-admission row.
- `binder/explain.rs` — six explanatory columns on the `optimizer_memo_admission` schema +
  row projection. `optimizer_decision_conflicts` (220) schema **untouched**.
- `binder/tests.rs` — behavior net.
- `tests/architecture/optimizer_memo_admission.rs` — `adr225_*` guards + 223/224 retargets.

No `runtime_evidence.rs`, daemon, executor, fragmenter, storage, wire, or Wendelon change.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Per-reason decline detail already accumulated | `join_order.rs:911` `decline_counts` per reason; `top_decision_conflict_decline` (≈:932) |
| Aggregate + pressure to preserve byte-stable | `blocked_or_near_miss_count = kept_thin_margin + Σ declined`; `conflict_strength = reordered.min(blocked_or_near_miss)` |
| Decline taxonomy | `OptimizerDecisionDeclineReason` (decision_log.rs); labels (`multiple_probe_relations`, `missing_estimate`, …) at decision_log.rs:141–147 |
| Star has no thin-margin | `StarOrderActuationResult { NotCandidate, Actuated, Declined }` |
| Floor (= 20) | `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE` (join_order.rs:8) |
| Row builder | `optimizer_memo_admission_row` / `optimizer_memo_admission_rows_from_snapshot` |
| Table schema/rows (11 cols → 17) | `information_schema_optimizer_memo_admission_schema` / `_rows` (explain.rs:385) |

## Phase 0 — Red tests first

1. `memo_admission_margin_limited_conflict_points_to_margin_tuning` — red (no `blocker_class`).
2. `memo_admission_hard_limited_conflict_points_to_hard_blocker_resolution`.
3. `memo_admission_capability_limited_conflict_points_to_contract_investigation`.
4. `memo_admission_mixed_blocker_does_not_overclaim_capability`.
5. `memo_admission_star_family_is_never_margin_limited`.

## Phase 1 — Report split (join_order.rs)

Add to `DecisionConflictReport`: `margin_blocked_count` (= `kept_thin_margin_count`),
`hard_decline_count`, `capability_decline_count`, summed in the existing accumulator fold
from per-reason `decline_counts` via a decline-class partition:

```text
hard:       NonInnerEquiJoin, MissingRelationLabel, DuplicateDimension, MissingEstimate
capability: MultipleProbeRelations, MixedStrategyStar, ProbeNotScanLinear
```

Keep `blocked_or_near_miss_count` and `conflict_strength` exactly as ADR 220/224 compute
them (assert byte-stable; `margin_blocked + hard_decline + capability_decline ==
blocked_or_near_miss`). No new outcome variant, no new evidence. Use an exhaustive `match`
on `OptimizerDecisionDeclineReason` for the partition (no wildcard) so a future decline
reason forces a classification choice at compile time.

## Phase 2 — Blocker classifier (join_order.rs)

```rust
enum MemoBlockerClass { NotConflicted, MarginLimited, HardLimited, CapabilityLimited, MixedBlocker }
```

Floor = `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE`. Per ADR D2:

```text
not lifetime-conflicted                       -> NotConflicted
ready = { class : class_count >= floor }
|ready| == 1                                  -> that class
|ready| >  1                                  -> MixedBlocker
|ready| == 0 && blocked_or_near_miss >= floor -> MixedBlocker
```

Computed from the lifetime report. It refines `reason`/`next_cut` only for `BlockedShape`
rows (D4/D5); `NotConflicted` for cold rows; stale/degraded rows render the measured class
+ counts but keep their 224 status and `next_cut`. `bounded_candidate_count` stays `0` for
every row.

## Phase 3 — Table surface (explain.rs)

Append after the existing 11 columns: `blocker_class`, `margin_blocked_count`,
`hard_decline_count`, `capability_decline_count`, `top_decline_reason` (snake label or empty
string), `top_decline_count`. Project in `_rows`. Do **not** touch
`information_schema_optimizer_decision_conflicts_*` (220). `SystemTableKind` count and the
217 receipt-gate manifest unchanged.

## Phase 4 — Tests + guards

The ADR discriminators (margin/hard/capability/mixed, star-never-margin, precedence-not-
bypassed ×2, additive-schema, zero-admissible, 220-schema-unchanged, Rune parity) + the
twelve `adr225_*` guards. Retarget the 223/224 memo-admission schema guards for the six
columns; keep 224 zero-admissible, 221/197 no-engine, and 220 conflict-model/table guards
intact.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib memo_admission
scripts/cargo-cgroup test  -p toron --lib blocker
scripts/cargo-cgroup test  -p toron --test architecture_test adr225
scripts/cargo-cgroup test  -p toron --test architecture_test adr224
scripts/cargo-cgroup test  -p toron --test architecture_test adr223
scripts/cargo-cgroup test  -p toron --test architecture_test adr220
scripts/cargo-cgroup test  -p toron --test architecture_test adr217
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL — retargets participate
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. Light any `admissible` / `bounded_candidate_count > 0`: zero-admissible + 224 fail.
2. Let a star row read `margin_limited`: `…star_family_is_never_margin_limited` fails.
3. Classify capability when margin/hard also ≥ floor: `…mixed_blocker_does_not_overclaim_capability` fails.
4. `next_cut = candidate_generation` for declines: `…investigation_not_generation` fails.
5. Compute blocker class before status precedence: the stale/degraded bypass tests fail.
6. Change `conflict_strength`/`blocked_or_near_miss` or 220's table:
   `optimizer_decision_conflicts_220_schema_unchanged` + 220 guards fail.
7. Record a new durable key/counter: durable-surface guard fails.

## Hygiene

- No m-word; no version-suffix idiom; `memo`/`Cascades` stay out of engine surfaces (the
  readout name pre-exists from 223; fences unchanged).
- No new durable key strings; no goldens (consumer-side projection of existing fields).
- `--all-targets` after the `DecisionConflictReport` field additions.
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. The decline partition must be an exhaustive `match` (no wildcard) so a future
   `OptimizerDecisionDeclineReason` variant forces a hard/capability choice at compile time.
2. `top_decline_reason` for `margin_limited` rows (no declines) → empty string (non-null
   finite schema, mirroring 223).
3. Confirm whether ADR 220's `optimizer_decision_conflicts` table already exposes
   `top_decline_reason`; if it omits it, keep 225's exposure to memo-admission only and do
   not add it to the 220 table.
