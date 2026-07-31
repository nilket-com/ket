# Impl Plan — ADR 130: Right-Side Build Selection

**Migration for** `plans/adr/130_right_side_build_selection.md`.
**Scope:** `crates/toron/src/binder.rs` (decision arm + `BroadcastBuildSide` on the
decision + swap + restoring projection + join scope ordering), guards, tests.
**Non-scope:** `ir.rs` (no `ExecutionPlan::Join` build-side field, no new
`JoinStrategy` variant), `exec.rs`, `distributed/fragment.rs`,
`distributed/scheduler.rs`, wire, Wendelon, `sql_planner.rs` (unhinted SQL
already emits `Auto` from 129), config.

## Pre-flight anchors (grounded)
| Concern | Anchor |
|---|---|
| `choose_auto_join_strategy` (add right arm) | `binder.rs:3732` |
| `choose_join_strategy` (single seam) | `binder.rs:3716`; `_right_facts` parked `:3720` |
| `bind_join` (swap + auto-exchange shaping today) | `binder.rs:1060`, auto-exchange `:1092-1111` |
| `validate_strategy_shape` Broadcast arm (unchanged) | `binder.rs:~3925` |
| `join_schema` / `join_fields` (left-then-right) | `binder.rs:3980-4006` |
| `RelationScope::join` (left entries then right-offset) | `binder.rs:3544-3570` |
| project fusion does NOT fuse reorder | `binder.rs:4767` (`column.ordinal != idx → false` `:4783`) |
| `exchange_for_join_keys` (129 helper, reuse for the Exchange(1)) | `binder.rs` (129 auto path) |
| 129 right-tiny-stays-hash test (retarget) | `binder.rs` `sql_unhinted_unknown_large_and_tiny_right_joins_stay_hash` |
| fragmenter left-build extract (unchanged) | `distributed/fragment.rs:~484` |

## Phase 0 — Failing gates first
Author the gates; verify on `main`:
- `sql_fact_left_tiny_right_infers_broadcast` **fails** (right facts ignored → hash).
- `right_build_broadcast_preserves_output_schema_and_values` is the honesty spine
  ([[feedback_cut_must_deliver_the_name]]): write it to assert **column
  names/order first, then row digest**, so it fails on `main` by *strategy* and
  fails on a swap-without-restore implementation by *column order*.
- `both_sides_tiny_prefers_left_build`, `right_tiny_left_deep_join_stays_hash`,
  `pinned_broadcast_still_wins_and_fail_loud` pass on `main` and stay green.

## Phase 1 — Decision rule (D1)
Extend `JoinStrategyDecision` with `build_side: BroadcastBuildSide { Left, Right }`
(fold into the reason receipt). In `choose_auto_join_strategy`, after the existing
left-eligible check:
1. left eligible (129) → `Broadcast`, `Left` (unchanged);
2. else right eligible — **same** single-base + hard-byte-bound + effective-threshold
   checks applied to `right_facts` → `Broadcast`, `Right`;
3. else hash.
Tie (both eligible) → `Left` (D6). Drop the underscore on `_right_facts`. Reason →
`BroadcastInferred { build_bytes, threshold, build_side }`.
**Probe-side fence (D5):** right-build is gated on the *probe* (now-left fact)
being a single base relation too — reject if the opposite side is a
join/exchange/derived plan (defers multi-way ordering).

## Phase 2 — Physical swap in `bind_join` (D2)
When `decision.strategy == Broadcast && build_side == Right`:
- **capture declared `join_schema` + declared `RelationScope::join`** from the
  pre-swap `(left, right)` first (Phase 3 needs them);
- swap `left_plan`↔`right_plan`, `left_keys`↔`right_keys`,
  `left_resolved_keys`↔`right_resolved_keys`;
- run the existing broadcast shaping — `exchange_for_join_keys(now_left, .., 1)`
  on the tiny dim, bare probe — via the 129 path, unchanged;
- `validate_strategy_shape(kind, Broadcast, &left, &right, …)` passes unchanged.

## Phase 3 — Restoring projection + declared scope (D3/D4)
- Build the permutation: execution order is `(dim_cols, fact_cols)`, declared is
  `(fact_cols, dim_cols)`; roots = column refs remapping execution ordinal →
  declared position (a permutation `BoundExprProgram`, each root a column ref).
- Wrap the swapped `ExecutionPlan::Join` in `ExecutionPlan::Project { exprs:
  permutation, output_schema: declared_join_schema }`.
- Return `ScopedExecutionPlan::new_with_facts(project, declared_scope,
  PlanOptimizerFacts::composite_unknown())` — declared scope (Phase 2 captured),
  composite facts (a join output is composite, per 129). Swap is invisible upward.

## Phase 4 — Guards + retarget (discipline #4)
- Retarget 129's `sql_unhinted_unknown_large_and_tiny_right_joins_stay_hash`:
  split out the right-tiny case → it now **infers broadcast** (the positive 130
  gate); unknown/large stay hash. Don't silently delete the right-tiny assertion —
  move it to the 130 positive gate.
- Add the four `adr130_*` guards; extend `adr129_strategy_decision_is_single_helper`
  to cover the new arm (it must still find no strategy selection outside the
  helper).

## Phase 5 — Verification
`cargo test -p toron` · `cargo test -p toron --test architecture_test adr125` ·
`cargo test -p toron --test architecture_test adr128` ·
`cargo test -p toron --test architecture_test adr129` ·
`cargo test -p toron --test architecture_test adr130` ·
`cargo check -p toron --all-targets` (the decision-struct field add ripples to its
match/construct sites — [[feedback_verify_all_targets_on_new_ir_field]]) ·
`git diff --check`. Confirm 125/128/129 digest+cap stay green (the swap must not
move pinned or left-build behavior).

## Risk register
- **Output-order regression (load-bearing):** a swap without the restoring
  projection silently reorders `SELECT *`. The byte-identical gate (asserting
  names/order) + `adr130_right_build_wraps_restoring_projection` are the backstops.
- **Scope drift:** the join's `RelationScope` must be the **declared** (pre-swap)
  one, else qualified refs above the join resolve wrong. Phase 2 captures it
  pre-swap; pin with a `SELECT fact.col, dim.col` ordering test.
- **Key-swap correctness:** `left_keys`↔`right_keys` must swap *with* the plans; a
  half-swap mispairs equi-keys. INNER equi-join symmetry makes it correct iff both
  swap.
- **Permanent projection (accepted):** grounded not fused (`:4783`); cost is an
  `ArrayRef`-clone column permutation plus a new `RecordBatch`, not per-row work.
  Dropping it = Fork B follow-on.
- **Probe-side smuggling:** without the D5 probe fence, a left-deep probe carries
  an unfragmented nested join into the broadcast consumer. `right_tiny_left_deep_join_stays_hash`
  pins it.
- **Tie-break drift:** both-tiny must prefer left (no swap) — pinned explicitly.

## Acceptance
Tiny-right INNER joins with seeded hard facts + config infer build-side-right
broadcast; `SELECT *` output is byte-identical (column order + digest) to the hash
plan; both-tiny prefers left; left-deep probe stays hash; pinned + 129 left-build +
125/128 digest/cap green; no executor/fragmenter/scheduler/wire/IR-execution-join
change; full Toron + architecture gates green.
