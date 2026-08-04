# ADR 130: Right-Side Build Selection (Tiny-Right Broadcast Inference)

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** at
mechanism level, verified against the actual iter-1 peer drafts. The second auto
rule on ADR 129's optimizer fact spine: infer broadcast for the canonical
star-schema shape `SELECT * FROM fact f JOIN dim d ON …` where the tiny dimension
is the **right/probe** input — the case 129 deliberately left as hash. Achieved by
*swapping operands* so the tiny dim becomes the existing left/build broadcast
input, then restoring declared column order with a projection. No new execution
strategy, no executor/fragmenter change.
**Category:** Toron interior — optimizer rule + a binder-local physical rewrite.
**Predecessors:** 129 (fact spine, `choose_auto_join_strategy`, the parked
`_right_facts`), 128 (pinned SQL broadcast), 125 (left-build broadcast contract +
`validate_strategy_shape` / `fragment_broadcast_join_plan`), 105 (adjacent-identity
project fusion — grounded to *not* fuse a reordering projection).
**Migration:** `plans/impl/0130_right_side_build_selection.md`.

## Doctrine — symmetric inference, preserved output contract
> 129 proved the spine with one arm (tiny *left* base relation → broadcast) and a
> spine-honesty gate promising the next rule would be a new arm in
> `choose_auto_join_strategy`, not a new branch. 130 is that arm, for the shape
> that actually matters — `fact JOIN dim` with the dim on the right. The optimizer
> recognizes a tiny build candidate on *either* side; the user's declared column
> order is preserved regardless of physical strategy. Grounded: broadcast is
> left-build-only at three layers (`validate_strategy_shape` `binder.rs:~3925`;
> `fragment_broadcast_join_plan` `fragment.rs:~484`; `execute_hash_join` builds
> from left `exec.rs:~2088`), and join output is left-then-right (`join_schema` /
> `join_fields` `binder.rs:3980-4006`; `RelationScope::join` `:3544-3570`). So
> right-build = **swap to reuse left-build** + a restoring projection — never
> re-teaching the executor a build side. The swap is provably safe for INNER
> equi-join (same row multiset under operand swap + paired key swap); the
> projection makes column order identical too.

## Decisions

**D1 — Second auto rule, same helper (spine-honesty).** Extend
`choose_auto_join_strategy` to consider the right child when the left is **not**
broadcast-eligible: if right is a single base relation with a hard
`byte_upper_bound` below the effective threshold → broadcast, build side right.
Reuses 129's eligibility predicate verbatim on `right_facts` (the `_right_facts`
129 parked). The decision receipt grows `BroadcastBuildSide::{Left, Right}`;
`bind_join` is its only consumer; `choose_join_strategy` stays the single decision
seam.

**D2 — Right-build is a binder-only physical swap (Fork A).** When right-build is
inferred, `bind_join` swaps the left/right bound plans **and** the paired join
keys (`left_keys`↔`right_keys`, resolved keys too), then proceeds through the
*existing* left-build broadcast path: one-partition Exchange on the (now-left) tiny
dim, bare (now-right) fact probe. `ExecutionPlan::Join` stays
`JoinStrategy::Broadcast` — **no new strategy variant**; `validate_strategy_shape`,
the fragmenter, and the executor see a textbook left-build broadcast and are
**unchanged** (grounded: the swapped plan satisfies their existing left-build
checks).

**D3 — Restoring projection (a permanent node).** The swapped physical join exposes
`dim_columns + fact_columns`; the declared order is `fact_columns + dim_columns`.
The binder wraps the join in an `ExecutionPlan::Project` whose roots are a
permutation remapping execution ordinals back to declared positions, with
`output_schema` = the pre-swap `join_schema(kind, left, right)`. Grounded honesty:
105's `collapse_adjacent_identity_project` fuses only *identity* projections
(`column.ordinal == idx`, `binder.rs:4783`); a reorder permutation returns `false`
and is **not** fused — so this Project is permanent. It is cheap (a column
permutation whose column roots clone `ArrayRef`s and create a new `RecordBatch`,
not per-row recomputation). Eliminating it is build-side independence (Fork B), a
named follow-on.

**D4 — Scope reflects declared order.** The join node's `RelationScope` is built
from the **pre-swap** `(left, right)` scopes (captured before the physical swap),
so qualified column resolution above the join is swap-invisible. Everything
upstream sees declared order.

**D5 — Conservative fences (build AND probe).** INNER only (129's broadcast is
already INNER-only; the swap changes null-extension semantics for outer joins —
excluded). `Auto` only; pinned/Rune authoring keeps fail-loud behavior. Hard byte
upper bounds only. Build side must be a single base relation (alias allowed).
**Probe side must also be a single base relation** (no nested join/exchange probe)
in this ADR — a left-deep probe would risk an unfragmented nested join inside the
broadcast consumer; this fence defers multi-way star-join ordering to a later ADR
(3-of-3 fence, named by Codex/Gemini). Both-sides-tiny prefers left build.

**D6 — Tie-break: left wins (placement, not cost).** If both sides are
broadcast-eligible, prefer the existing left-build path — no swap, no projection.
Right-build fires only when left is ineligible and right is eligible. This keeps
130 a placement rule; smaller-of-two is the first genuine *cost comparison* and is
deferred (129 doctrine: prove bounds before weighing costs).

**D7 — Reuse 129 substrate; do-no-harm; pinned precedence.** Same dark-launch
`auto_broadcast_max_build_bytes` + cap margin + hard facts — no new config, no new
knob, no Wendelon facts. Pinned hints still win and stay fail-loud (128/129). Auto
right-inference never downgrades or errors a query that would have run as hash;
unknown/soft facts → hash. Reason receipt extends to
`BroadcastInferred { build_bytes, threshold, build_side }`.

**D8 — Scope boundary.** Binder-only production change (decision arm + swap +
restoring projection + join scope ordering). **No** change to `ExecutionPlan::Join`
shape, executor, fragmenter, scheduler, wire, the `JoinStrategy` enum, or Wendelon.
No build-side field on the execution join (that's Fork B). No selectivity, no
smaller-of-two, no multi-way ordering, no non-INNER, no new knob.

## Behavior gates
- `sql_fact_left_tiny_right_infers_broadcast` — `fact f JOIN dim d` with a seeded
  tiny `dim` (right) + config set → concrete broadcast, build side right. **Fails
  on `main`** (right facts ignored; right-tiny → hash today).
- `right_build_broadcast_preserves_output_schema_and_values` (**the load-bearing
  spine**) — `SELECT * FROM fact JOIN dim` returns byte-identical **column
  names/order AND row digest** whether dim is inferred-broadcast (swapped +
  projected) or hash. Asserts order, not just digest, so the swap-without-restore
  bug fails it.
- `right_build_broadcast_byte_identical_to_hash` — local executor + distributed
  digest parity (mirrors 128's three-way harness).
- `both_sides_tiny_prefers_left_build` — no swap/projection when left qualifies.
- `right_tiny_left_deep_join_stays_hash` — probe-side join not eligible (D5 fence;
  defers multi-way ordering).
- `pinned_broadcast_still_wins_and_fail_loud` — 128/129 precedence intact.
- `right_build_swapped_plan_is_valid_left_build_broadcast` — the swapped plan
  passes the existing `validate_strategy_shape` broadcast checks unchanged.
- Existing 125/128/129 broadcast cap/digest/left-build gates remain green.

## Architecture guards
- `adr130_right_build_is_binder_physical_rewrite_not_new_strategy` — right-build is
  a binder swap + `Project`; no new `JoinStrategy` variant; `ExecutionPlan::Join`
  has no build-side field.
- `adr130_right_build_wraps_restoring_projection` — the right-build path emits a
  `Project` whose `output_schema` is the pre-swap `join_schema`.
- `adr130_decision_helper_single_seam` — the right rule is a new arm in
  `choose_auto_join_strategy`; `bind_join` selects no strategy outside the helper
  (extends 129's `adr129_strategy_decision_is_single_helper`).
- `adr130_no_fragmenter_executor_wire_change` — no `adr130` markers in
  exec/fragment/scheduler; no execution-join shape change.
- Existing 129 guards remain.

## Council history
- **iter-0:** all three independently recommended right-side build selection with
  Fork A (swap + restoring projection), left-wins tie-break, no executor change.
  Claude framed the A/B fork + output-order crux with a *tentative* "105 fusion
  may elide the projection." Codex committed the swap mechanism + the probe-side
  single-base fence (no multi-way smuggling). Gemini ratified the shape.
- **iter-1:** Claude grounded the fusion question — `collapse_adjacent_identity_project`
  fuses only identity maps (`:4783`), so the restoring projection is **permanent**
  — and corrected the iter-0 hand-wave. Codex and Gemini both independently adopted
  that grounding (projection as a first-class, non-fused node) and the probe-side
  fence. Mechanism-level 3-of-3; no load-bearing divergence.

The honest arc: one grounding correction (fusion does not elide a reorder), caught
before promotion by reading the actual fusion predicate — the projection's
permanence is named, not hidden ([[feedback_cut_must_deliver_the_name]]).

## What to do next
Smaller-of-two cost comparison (the first real cost weigh); build-side independence
(Fork B) to drop the restoring projection if a workload justifies it; multi-way
star-join ordering (lifts the probe-side fence); selectivity-aware broadcast;
Wendelon-backed facts (flips dark-launch); optimizer `EXPLAIN` over reason receipts.

## Promotion criteria
Tiny-right seeded INNER join infers build-side-right broadcast; the byte-identical
output gate passes (fails on `main` first, by strategy and — on a swap-without-
restore mistake — by column order); tie-break prefers left; the probe-side fence
keeps left-deep probes hash; pinned + 129 left-build + 125/128 digest/cap gates
green; the four `adr130_*` guards + the extended 129 single-seam guard hold;
`cargo test -p toron`, separate architecture-filter runs for `adr125`, `adr128`,
`adr129`, and `adr130`, `cargo check -p toron --all-targets`, and
`git diff --check` green.
