# Impl Plan — ADR 129: Optimizer Fact Spine for Tiny Broadcast Inference

**Migration for** `plans/adr/129_optimizer_fact_spine.md`.
**Scope:** `ir.rs` (`JoinStrategyIntent` on `LogicalPlan::Join`), `catalog.rs`
(`TableOptimizerFacts` on `TableBinding`), `binder.rs` (facts propagation +
`choose_join_strategy` + auto-exchange placement + `PlannerConfig` field),
`sql_planner.rs` (emit intent, not concrete strategy), `rune_engine.rs` (pinned),
guards.
**Non-scope:** executor, fragmenter, scheduler, worker protocol, wire, Wendelon
stats producer, a `ScanNamed.stats` field, planner catalog access, right-side
build, join reorder, zero-worker dispatch.

## Pre-flight anchors (grounded)
| Concern | Anchor |
|---|---|
| `SqlPlanner` catalog-free (unit struct) | `sql_planner.rs:1468` |
| `PlannerConfig` (binder-owned threshold home) | `binder.rs:56` |
| binder holds catalog → `TableBinding` facts | `binder.rs:62/65`, `ScanNamed` resolve `:125/:133` |
| `TableBinding` attach point / constructor chain | `catalog.rs:76`, `:88–195` |
| `JoinStrategy` enum (2 variants) | `ir.rs:615` |
| pinned pre-bind exchange shaping (stays) | `sql_planner.rs:4129/4144` |
| binder validates shape today | `binder.rs:3645/3664` (`validate_strategy_shape`) |
| SQL join loop (emit intent) | `sql_planner.rs:2715–2722` |
| `Take` (row-fact source) | `ir.rs:686`; local exec `exec.rs:779`, `server/mod.rs:3733` |
| `EmptyRelation` (zero-row leaf) | `ir/execution.rs:192`, `binder.rs:1485` |
| one-task fragmentation (unchanged) | `distributed/fragment.rs:600` |
| zero-partition exchange rejected | `distributed/fragment.rs:59` |
| `Auto` blast radius | confined to ir+planner+binder (53 `JoinStrategy::` / 8 files) |
| guards to retarget | `tests/architecture/rune_frontend.rs` 122/125/128 join block |

## Phase 0 — Failing gates first
Author all gates + guards. Verify on `main`:
`sql_unhinted_tiny_left_join_infers_broadcast` **fails** (no spine; unhinted →
concrete hash). The other negatives (unknown/large/right-side stay hash) pass on
`main` and stay green. Honesty check ([[feedback_cut_must_deliver_the_name]]).

## Phase 1 — Fact types (D1)
```rust
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TableOptimizerFacts {
    pub byte_upper_bound: Option<ByteCount>,
    pub row_upper_bound: Option<RowCount>,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PlanOptimizerFacts { origin: PlanOrigin, byte_upper_bound: Option<ByteCount>, row_upper_bound: Option<RowCount> }
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum PlanOrigin { #[default] Unknown, SingleBaseRelation, Composite }
```
Reuse existing `ByteCount`/`RowCount` (local optimizer newtype only if the
`distributed::task::RowCount` dependency direction is unacceptable). **No
`confidence`.**

## Phase 2 — Facts on `TableBinding` (D2)
Add `optimizer_facts: TableOptimizerFacts`; default unknown through the
constructor chain (`catalog.rs:88–195`). Add a test-catalog seeder by
`TableRef`/`TableId`. **No
`ScanNamed.stats`; no `get_stats_for_table` in `sql_planner.rs`** (the rejected
Gemini-impl placement — risk register).

## Phase 3 — `JoinStrategyIntent` (D3)
Add `pub enum JoinStrategyIntent { Auto, Pinned(JoinStrategy) }`; `LogicalPlan::Join`
carries `strategy_intent`. `build_join_plan` stays the **pinned** authoring helper
(Fork A); add `build_auto_join_plan(left, right, kind, on)` for unhinted SQL.
`lower_exists_join` + Rune default → `Pinned(HashPartitioned)`; SQL hint + Rune
`.broadcast_join` → `Pinned(Broadcast)`; unhinted SQL → `Auto`. Run `cargo check -p
toron --all-targets` after the enum-field add
([[feedback_verify_all_targets_on_new_ir_field]] — `LogicalPlan::Join` is widely
constructed).

## Phase 4 — `PlanOptimizerFacts` propagation (D8)
`ScopedExecutionPlan` carries `facts: PlanOptimizerFacts`:
- `Scan`/`ScanNamed` → table facts, origin `SingleBaseRelation`.
- `Alias` → preserve.
- `Exchange` → preserve byte/row bounds but mark origin `Composite` for this ADR,
  so a pre-shaped exchange cannot satisfy tiny-left single-base eligibility.
- `EmptyRelation` → `row_upper_bound: Some(0)`, `byte_upper_bound: Some(0)`, origin
  `Composite`.
- `Take` → `row_upper_bound: min(child, limit)` (`Some(0)` for `LIMIT 0`), origin
  `Composite`.
- `Filter`/`Project`/`Aggregate`/`Join`/`Sort` → `Composite`, drop byte unless a
  hard bound is proven + tested.
The tiny rule only accepts scan/alias single-base left inputs, so row-fact
propagation cannot widen broadcast. Row facts are **consumed by nothing** this cut
— pin with `adr129_row_facts_carried_but_not_scheduled_on`.

## Phase 5 — `choose_join_strategy` (D4/D6/D7/D9, the spine)
```rust
struct JoinStrategyDecision { strategy: JoinStrategy, reason: JoinStrategyDecisionReason }
```
Single helper: `choose_join_strategy(intent, kind, left_facts, right_facts,
config)`. Rules: `Pinned(s)` → `s` + `PinnedByAuthor`; `Auto`+disabled-config →
hash; non-inner → hash; left origin not `SingleBaseRelation` → hash; missing
byte bound → hash; bound above effective threshold → hash; else `Broadcast` +
`BroadcastInferred { build_bytes, threshold }`. `bind_join` calls it and **never
selects `JoinStrategy::Broadcast` itself** — pinned by
`adr129_strategy_decision_is_single_helper`.

## Phase 6 — Binder auto-exchange (D5, Fork A)
For `Auto` only: bind children → bind/validate keys → `choose_join_strategy` →
insert concrete exchange shape (hash: both sides by keys; broadcast: left
`Exchange(partition_count:1)`, right bare) → run existing `validate_strategy_shape`
→ emit concrete `ExecutionPlan::Join`. **Pinned joins unchanged** (planner-shaped
exchange + existing validation). Do not unify exchange insertion (Fork B) — guard
`adr129_exchange_insertion_split_is_temporary` names the debt.

## Phase 7 — Config (D10)
Add `auto_broadcast_max_build_bytes: Option<ByteCount>` to `PlannerConfig`,
**default `None`** (dark-launch). Tests opt in with a small explicit threshold.
Effective threshold is the configured threshold capped by a static low ceiling
derived from default `broadcast_build_bytes_per_task` (for example 25%). If a
future binding path threads actual `QueryBudget`, use the actual cap instead;
tenant-cap awareness remains a follow-on.

## Phase 8 — Guards + retarget (D11/discipline #4)
Retarget 122/125/128 join guards (SQL loop threads `JoinStrategyIntent`; unhinted →
`Auto`; hinted/Rune-broadcast → pinned broadcast; EXISTS/Rune-default → pinned
hash; `Auto` only in ir/planner/binder; execution/fragmenter/scheduler/Wendelon
have no `Auto`; pinned broadcast still left-exchange-one/bare-right). Add the
eight `adr129_*` guards from the ADR.

## Phase 9 — Verification
`cargo fmt -p toron` · `cargo check -p toron --all-targets` · `cargo test -p toron
auto_broadcast` · `cargo test -p toron sql_broadcast_hint` · `cargo test -p toron
--test architecture_test adr122 adr125 adr128 adr129` · `cargo test -p toron` ·
`git diff --check`. Confirm 125/128 digest+cap stay green (the intent split must
not move pinned behavior).

## Risk register
- **Planner-stats regression** — no `ScanNamed.stats`, no `get_stats_for_table`
  in `sql_planner.rs`; facts live on `TableBinding`, consumed in the binder.
- **Spine-in-name-only** — buried `if tiny`; backstop `adr129_strategy_decision_is_single_helper`.
- **`Auto` leak** — never in execution/fragmenter/scheduler;
  `auto_intent_never_survives_binding` gate.
- **False tiny classification** — hard byte bounds only.
- **Fork A debt** — split exchange insertion guarded + named; unify when a
  pinned-reshaping rule lands, not before.
- **Default-on before real facts** — dark-launch `None`; evidence-gate enablement.
- **Zero-worker scope creep** — row facts carried only; no dispatch change
  (`adr129_row_facts_carried_but_not_scheduled_on`).

## Acceptance
Tiny-left joins with seeded hard byte facts + explicit config bind to concrete
broadcast; missing/large/right-side-small bind to hash; hints + Rune pinned
authoring preserve 125/128 behavior; row facts carried without scheduling change;
`Auto` never reaches execution; the spine + Fork-A + no-scheduling guards hold;
full Toron + architecture gates green; `git diff --check` clean.
