# ADR 137: Fact-Spine Preservation Through Row-Local Volume-Non-Increasing Nodes

**Status:** Promoted. **Genuine 3-of-3 (Claude, Codex, Gemini).** Now that ADR 136
lets real table-size facts flow, this cut makes those facts **survive the row-local
shape between a base scan and a join build** — so a filtered or narrowed tiny
dimension still infers broadcast, instead of every `WHERE`/projection silently
demoting it to hash.

**Category:** Toron interior — optimizer IQ (binder-local).
**Predecessors:** 129 (fact spine, `PlanOrigin`), 130/132 (the byte rules that spend
facts), 134 (EXPLAIN — validation surface), 136 (made base facts real in production),
116 (Take row-fact precedent).
**Migration:** `plans/impl/137_fact_spine_row_local_preservation.md`.

## Doctrine — row-local volume-non-increasing nodes keep the spine
> 136 made `byte_upper_bound`/`row_upper_bound` real, but the binder throws them away
> the moment a relation is wrapped: `Filter` (`binder.rs:380`) and `Project` (`:396`)
> each replace the child facts with `composite_with_row(...)` —
> `PlanOrigin::Composite`, `byte_upper_bound: None`. The join chooser then rejects the
> build (`binder.rs:3950/3953` require `SingleBaseRelation`; the byte gate at
> `:3889/:3897` needs a bound). Yet a filter only *removes* rows and a column-subset
> project only *drops* columns — neither makes the relation larger, so the child's
> hard upper bounds stay valid and the origin stays single-base. Preserving them is
> the whole cut: bounds that are provably still valid were simply being discarded.

## Decisions

**D1 — `Filter` preserves the full single-base spine.** Origin + `byte_upper_bound`
+ `row_upper_bound` carried from the input (`binder.rs:380`). Static-false stays on
the existing `EmptyRelation` path; 136's known-empty (`row_upper_bound` 0) composes
through unchanged.

**D2 — `Project` preserves byte facts only for a pure column subset/permutation.**
After binding `exprs`, if every output root is a **distinct** `BoundExprNode::Column`
of the input, preserve origin + byte + row. If the projection computes expressions,
casts, literals, or duplicates a column, it may grow bytes — preserve the input origin
and row bound but **drop `byte_upper_bound`**. Thus a single-base expression project
keeps lineage without becoming broadcast-eligible, while a project over an already
`Composite` input remains `Composite` (`binder.rs:396`). `Aggregate` stays
`Composite`.

**D3 — Single-column PK equality caps rows, carried not spent.** A `Filter` proven to
include `col = <non-null literal>` where `col` is the table's sole primary key
(`primary_key_columns.len() == 1`) sets `row_upper_bound = 1`. It **never shrinks
`byte_upper_bound`**, so by itself it does not flip a large table to broadcast — the
broadcast decision is byte-only (`binder.rs:3889/3897`; `row_upper_bound` is unread).
The cap is carried for future cost use; the byte-from-rows derivation that would let a
PK row broadcast a large table is **ADR 138** (it needs a hard per-row width model
that does not exist today). Gated honestly as *caps rows, still hashes* — no name the
mechanism cannot deliver. The proof is **sole-PK**, not "any PK column": a descriptor
with multiple primary-key columns does not cap rows from a predicate on one component.

**D4 — Centralize the safety proof.** One guarded place, reused by both row-local arms
so the invariant cannot silently rot:
`PlanOptimizerFacts::preserve_volume_non_increasing` (Filter),
`preserve_through_project(&exprs)` (column-subset test), `with_pk_row_cap`.

**D5 — Binder-local.** No new fact unit, no width model, no `ExecutionPlan`/wire/
fragmenter/scheduler change. EXPLAIN (134) is the validation surface.

## Sort: considered and dropped (grounded)
`Sort` is the third `composite_with_row` site (`binder.rs:479`) and is
volume-non-increasing, so folding it looked free. It is **not**: every surface
`ORDER BY` lowers as `Sort { input: Exchange { partition_count: 1, .. } }`
(`sql_planner.rs` `apply_order_by_limit_tail`), and the one-partition `Exchange`
already resets origin to `Composite` *below* the Sort. So preserving facts in the Sort
arm changes nothing observable for SQL/Rune `ORDER BY` — the `Exchange` (a genuine
repartition) is the correct demotion. Including Sort would be the
[[feedback_cut_must_deliver_the_name]] trap in reverse: a node whose preservation is
masked by its own input. Revisit only if a direct-IR `Sort` over an un-exchanged
single-base relation becomes reachable. (Credit Codex for the catch; I verified the
`Exchange` is in the logical lowering, not just the fragmenter.)

## Scope fence
In: `Filter`, column-subset `Project`, single-column PK row cap. Out (correctly):
`Sort` (input is Exchange-wrapped — inert, above); `Aggregate` (`:413`,
cardinality-changing → `Composite`); `Take` (`:496`, already carries its row fact via
116); the per-row-width byte derivation that turns a row cap into a broadcast (→ 138);
multi-column / range / histogram selectivity; join reordering. `Alias` already
preserves facts (`binder.rs:353`). There is **no `LogicalPlan::Distinct` variant** —
dedup is an `Aggregate` and stays `Composite`.

## Behavior gates
These gates use binder/direct-`LogicalPlan` fixtures for build-side row-local inputs:
SQL currently rejects joins around derived tables (`sql_planner.rs:2779`) and native
Rune `.join` is pinned hash, so this ADR does not claim a new derived-table SQL
surface.

- `filtered_tiny_dim_infers_broadcast` — direct `fact ⋈ (dim WHERE region='X')`
  broadcasts via preserved byte bound + origin. Headline; fails on `main`.
- `narrowing_project_tiny_dim_infers_broadcast` — a direct `LogicalPlan::Project`
  selecting `id, region` from `dim` as the build input preserves the byte bound.
- `expression_project_drops_byte_bound_and_hashes` — a direct expression project keeps
  row + origin, drops byte → hash (no false broadcast).
- `pk_filtered_large_table_caps_rows_but_still_hashes` — the honest D3 gate: row cap
  to 1 shows in binder facts, strategy stays hash (no width model yet).
- `composite_pk_single_column_filter_does_not_cap_rows` — a predicate on one component
  of a multi-column descriptor PK is not a ≤1-row proof.
- `aggregate_or_expanding_project_stays_composite` — non-subset project / aggregate
  stays `Composite`.
- `filtered_dim_without_facts_still_hashes` — missing facts → hash (do-no-harm).
- Regressions: 129–136 optimizer / EXPLAIN / star gates green.

## Architecture guards
- `adr137_filter_preserves_single_base_facts`.
- `adr137_project_preserves_byte_bound_only_for_column_subset`.
- `adr137_pk_row_cap_does_not_shrink_byte_bound` (D3 honesty).
- `adr137_row_local_facts_go_through_shared_helper` (D4 — no inline fact
  reconstruction in a row-local arm).
- `adr137_no_substrate_or_wire_change`.

## Non-goals
The per-row-width byte model that lets a row cap broadcast a large table (→ 138);
`Sort` fact preservation (inert while surface `ORDER BY` is Exchange-wrapped);
multi-column/range/histogram selectivity; join reordering; the 136-W Wendelon
producer; any substrate/wire/scheduler change.

## Council history
iter-0 converged on selectivity-through-filters (Gemini's pick from 136, unblocked
once 136 made base facts real). iter-1: Claude + Codex independently grounded that the
broadcast decision is byte-only (`binder.rs:3889/3897`), so a PK *row* cap broadcasts
nothing this cut — Gemini's `pk_filtered_large_table_infers_broadcast` gate was the
deliver-the-name trap; resolved to "carry the row cap honestly, defer the width model
to 138" (B′). iter-2: Claude floated folding `Sort`; **grounding then disproved it** —
surface `ORDER BY` wraps the Sort input in a one-partition `Exchange`, so Sort
preservation is inert (Codex's caught caveat, confirmed against `sql_planner.rs`). Sort
dropped; the cut returns to the clean Filter/Project core plus the honest PK cap.
[[feedback_cut_must_deliver_the_name]].

## Follow-ons
**ADR 138** — hard per-row materialized-width model so a row cap (PK / known-small)
converts to a `byte_upper_bound` and a PK-filtered row from a large table actually
broadcasts (bounded-width schemas: fixed Arrow types always, `Utf8`/`Binary` via a
refinement max-length, else keep input byte bound). Then: remote-live multi-producer
fan-in; richer cost units; multi-column / range / histogram selectivity; join
reordering; the 136-W Wendelon producer.

## Promotion criteria
Direct binder fixtures for a filtered and a narrowing-projected tiny dimension each
infer broadcast over real facts with no `seed_optimizer_facts` (fail on `main`); an
expression project and an aggregate stay `Composite`; a single-column-PK-equality
filter on a large table caps rows to 1 yet still hashes; a composite-PK component
filter does not cap rows; missing facts hash; all row-local fact computation routes
through the shared helpers (guarded); the five `adr137_*` guards + 129–136 guards
green; `cargo test -p toron`, `--test architecture_test -- adr137`, `cargo check -p
toron --all-targets`, `git diff --check` all green.
