# ADR 101: Rune `LIKE`/`ILIKE` Pattern Predicates

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — all three independently selected
this topic and converged on the same shape (Codex seeded + sharpened the infallible return; Claude
grounded the substrate + the exact binder error + the fixture/guard anchors; Gemini ratified). A **pure
Rune authoring admission** over the already-complete `Expr::Like` substrate: four `RuneExpr` methods,
registration, stale guard retargets, behavior gates — no lower-layer movement. **Toron-only, ring-local Rune
frontend.** **Predecessors:** 075 (`LIKE`/`ILIKE` SQL semantics + literal-only pattern policy), 083
(`logical_not` — the negation spelling), 086 (`in_list`/`not_in_list` — the predicate-method precedent).
**Migration:** `plans/impl/101_rune_like_ilike_pattern_predicates.md`.

## Doctrine — admit existing IR to Rune; own no new semantics

`LIKE`/`ILIKE` is built and proven end-to-end for SQL; Rune is simply the missing authoring consumer.
ADR 101 adds zero semantics: it builds the existing `Expr::Like` and lets the existing binder, executor,
and PBC-decline path do everything they already do. The cut is "register four methods + retarget the
stale guards that still classify `like`/`ilike` as absent."

## Grounding (verified — the substrate is complete below Rune)

| Layer | Anchor | State |
|---|---|---|
| Logical IR | `Expr::Like { expr, pattern, negated, case_insensitive }` `ir.rs:528` | exists |
| Bound IR | `BoundExprNode::Like { .. }` `ir/execution.rs:90` | exists |
| SQL lower | `lower_like` `sql_planner.rs:3713` (literal pattern; rejects `LIKE ANY` `:3723`, `ESCAPE` `:3726`) | exists |
| Binder | `bind_like` `binder.rs:4704`; Utf8 check → `"LIKE expects Utf8 input, got {expr_type:?}"` `binder.rs:4720` | exists |
| Executor | Arrow `like`/`nlike`/`ilike`/`nilike` on `(case_insensitive, negated)` in `exec.rs` | exists |
| PBC | `BoundExprNode::Like` → `ExpressionShapeUnsupported` `pbc.rs:128`, in the residual set `:181` | declines → residual |
| **Rune (the gap)** | `rune_engine.rs` registers `in_list`/`not_in_list`/`logical_not` but not `like`/`ilike`; deferred guard `rune_frontend.rs:700` = `["not","like","ilike","safe_cast"]` | **missing** |

## Decisions (promotion-locked, D1–D6 are 3-of-3)

**D1 — Four explicit methods, no generic `not`.** Register `like`, `not_like`, `ilike`, `not_ilike`.
Negation already has a spelling — `logical_not` (`rune_engine.rs:475`, ADR 083, in the retained set); the
keyword-shaped `not` stays deferred. Mirrors the `in_list`/`not_in_list` both-polarity precedent.

**D2 — Literal-only pattern by construction.** The pattern is a host `String`, not a `RuneExpr`
(`bind_like` takes `pattern: &str`; the 075 policy). No dynamic/column patterns, `LIKE ANY`, `ESCAPE`,
`SIMILAR TO`, `RLIKE`, `REGEXP`, or regex.

**D3 — Infallible methods: return `RuneExpr`, not `VmResult<RuneExpr>`.** The comparison/boolean builders
(`eq`/`gt`/`and`/… `rune_engine.rs:427-469`, `logical_not` `:475`) return `RuneExpr` directly; **only**
`in_list`/`not_in_list` use `VmResult`, and solely because `build_in_list` rejects empty/oversized lists
(`:559-565`). `LIKE` has no Rune-local failure (`String` is non-null, empty patterns are valid, the binder
owns type validation), so it follows the dominant infallible pattern.

**D4 — Binder owns type policy.** Rune does not pre-check column types; `bind_like` remains the single Utf8
admission point and emits the existing `binder.rs:4720` message.

**D5 — Residual-only.** No PBC support; `BoundExprNode::Like` stays pushdown-ineligible (`pbc.rs:128`),
evaluated in Toron, and composes beside a pushable conjunct (the residual-split gate).

**D6 — Retarget stale surface guards, don't delete them.** The ADR 082 deferred-list site
(`adr082_typed_literal_cut_does_not_expand_operator_surface`, `rune_frontend.rs:674-705`) lists `like`/
`ilike` as deferred. Move `like`/`ilike` (and add `not_like`/`not_ilike`) into its **retained** set; the
deferred set becomes `["not", "safe_cast"]`. The ADR 086 in-list registration guard also has a
forbidden-name list that currently includes `like`/`ilike`; remove only those admitted names there while
leaving `in`, `not`, and `safe_cast` forbidden.

**No lower-layer or cross-ring movement.** No SQL grammar / `lower_like`, no logical/bound IR variant, no
`bind_like` semantic change, no executor kernel, no PBC opcode or eligibility change, no
storage/wire/Wendelon/descriptor/worker surface.

## Behavior gate (over the existing `sales` fixture, `rune_relation_handles_test.rs:1300`)

Fixture ground truth (verified): `id:Int64 [1..6]`, `amount:Int32`, `status:Utf8 =
[open,open,closed,open,open,open]` (only id 3 is `closed`).

- `rune_like_filters_utf8_column_residual` — `col("status").like("o%")` → `BoundExprNode::Like { negated:
  false, case_insensitive: false }`, `FilterDisposition::EvaluateInToron`, scan has no PBC predicate,
  rows `[1,2,4,5,6]`.
- `rune_ilike_filters_utf8_column_residual` — `col("status").ilike("CLO%")` → `case_insensitive: true`,
  rows `[3]`.
- `rune_not_like_and_not_ilike_filter_rows` — `not_like("o%")` → `[3]`; `not_ilike("CLO%")` →
  `[1,2,4,5,6]`; both `negated: true`.
- `rune_like_residual_splits_from_pushable_conjunct` — `col("id").eq(lit_i64(3)).and(col("status")
  .like("c%"))` → equality eligible for scan PBC, `Like` stays residual, disposition `EvaluateInToron`,
  rows `[3]`.
- `rune_like_reuses_binder_utf8_type_error` — `col("amount").like("1%")` → bind fails via `bind_like`
  with `LIKE expects Utf8 input, got Int32` (amount is `Int32`).

## Architecture guards

- `adr101_rune_like_ilike_methods_are_registered` — `RuneExpr` defines the four methods (`pattern:
  String`, `-> RuneExpr`); `create_toron_module` registers all four; bodies lower to `Expr::Like`, no new
  IR variant.
- `adr101_rune_like_methods_are_infallible` — the four do **not** return `VmResult`; no Rune-local
  empty-pattern rejection; `build_in_list`'s `VmResult` stays the sole list-validation precedent.
- `adr101_stale_surface_guards_are_retargeted` — post-101 deferred set is exactly `["not", "safe_cast"]`;
  `like`/`ilike` are in the retained set; the ADR 086 forbidden-name guard no longer rejects the admitted
  names; no keyword-shaped `not` registered.
- `adr101_reuses_075_like_policy` — `lower_like`/`bind_like`/`evaluate_like` remain the semantic owners;
  Rune adds no `LIKE ANY`/`ESCAPE`/dynamic-pattern/regex surface.
- `adr101_like_remains_residual_only` — `pbc.rs` still declines `BoundExprNode::Like`; no new PBC opcode.
- `adr101_no_cross_ring_surface` — no SQL planner / storage / wire / Wendelon / worker change.

## Scale boundary & seam check

Toron exterior composition (Rune authoring), ring-local. The change is `rune_engine.rs` (4 methods + a
`build_like` helper + 4 registrations) + `rune_relation_handles_test.rs` fixtures + `rune_frontend.rs`
guard retargets + the new `adr101_*` guards. No load-bearing seam.

## What to do next

- **ADR 102 lead — `information_schema.tables`/`.columns`**, gated on the framing fork (3-of-3 named):
  **(a)** local-cache-only introspection over descriptors Toron already holds, vs **(b)** refreshed
  authoritative tenant descriptor listing (099's refresh-before-read, but a *listing* RPC — Toron has
  per-table `get_table_schema_descriptor`, Wendelon has `descriptors_for_tenant`, so (b) adds a cross-ring
  listing surface). Name (a)/(b) at iter-0.
- Carried behind: derived-table alias column lists (`AS p(pid)`) + other derived-source widenings (let
  100 bake); Wendelon `*V1` de-version sweep (Wendelon-home, needs an audit taxonomy); `DROP`/`ALTER
  LOGICAL TYPE` + PK/FK identity (cross-ring schema authority).

## Council history

100 closed the derived-source deferral; the roadmap head had no operator-locked next. iter-0: Codex
seeded Rune `LIKE`/`ILIKE` as the smallest fully-pre-proven cut; Claude independently grounded the
end-to-end substrate + teed up `information_schema.tables`/`.columns` (with the (a)/(b) fork) for 102;
Gemini concurred. iter-1: Codex sharpened the return type to infallible `RuneExpr`; Claude verified the
exact `bind_like` message and conceded the return-type correction on grounding (`in_list`'s `VmResult` is
the exception, not the rule). iter-2 (dual): Claude verified the `sales` fixture rows/types and the stale
guard retarget sites; 3-of-3. Migration:
`plans/impl/101_rune_like_ilike_pattern_predicates.md`.

## Promotion criteria

The four `RuneExpr` methods build `Expr::Like` and are registered; `LIKE`/`ILIKE`/negated variants filter
a Utf8 column with the established executor behavior; pattern predicates stay residual and split beside a
pushable conjunct; non-Utf8 input reuses the exact `bind_like` error; stale surface guards allow admitted
`like`/`ilike` methods while keeping `not` and `safe_cast` out; no
SQL/IR/binder/executor/PBC/storage/Wendelon movement; full Toron suite green. Migration:
`plans/impl/101_rune_like_ilike_pattern_predicates.md`.
