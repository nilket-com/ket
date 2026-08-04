# ADR 081: Rune Surface Naming — Delete the `.select` Keyword Shim

**Status:** Promoted (plan). **Genuine 3-of-3 (Codex, Claude, Gemini)** — verified
against the latest peer drafts (Codex iter-1, Claude iter-1, Gemini iter-2): all
three lock
`.project_named` + delete the shim. Two non-load-bearing divergences resolved 2-of-3
(Codex+Claude): fixtures renamed to drop stale `select` wording; a clean
2-retargeted + 2-new guard set over Gemini's redundant 4. Originating from the ADR
080 review Finding #2.
**Category:** Toron frontend hygiene / Rune authoring surface. Refinement ADR, not a
new capability.
**Scale boundary:** Toron-only; production change localized to
`crates/toron/src/rune_engine.rs` (a rename + deletions). **Behavior-zero** — no
Wendelon/worker/wire/storage/PBC/IR/binder/executor/dependency/`syn` change; the
composed `LogicalPlan` and all results are byte-identical, only the authored spelling
changes.

## Summary

ADR 080 shipped named projection but, on discovering `select` is a Rune 0.14 keyword,
preserved the `.select(...)` spelling with a ~90-line pre-compile source-text
rewriter (`normalize_rune_named_select_surface` + a hand-rolled scanner + dual
`"select"`/`"select_named"` registration). ADR 081 makes the surface honest:
**`.project_named(...)`** becomes the authored name, the rewriter is deleted, and two
durable guards prevent the whole class of problem from recurring.

## Doctrine

**Rune authoring method names must not collide with Rune keywords, and the Rune
frontend must never rewrite user source text.** A source rewriter of sandboxed user
scripts is the wrong permanent contract for a naming preference; encode both halves
as guards so no future surface word can quietly pull a shim back in.

## Grounding ledger (verified)

- **`select` is a Rune 0.14 keyword** (`generated.rs:8549`; `expr_select.rs`) —
  `df.select(...)` can't parse as a method call, so the rename is the only way to
  drop the shim.
- **Collision surface is tiny** — of dataframe-relevant words, only **`select`** and
  **`as`** are Rune keywords; `join`/`where`/`group`/`order`/`limit`/`having`/`union`/
  `project`/`filter` are all free. `.project_named` is safe, and the no-collision
  convention is cheap to keep (`.named(...)` already sidesteps the `as` trap).
- **Two 080 guards pin the shim and must be reconciled (discipline #4), not just
  left to fail:**
  - `adr080_named_select_lowers_to_existing_project_output_names`
    (`rune_frontend.rs:328`) reads `pub fn select` and asserts the `normalize_*`
    block + `FROM=".select"` / `TO=".select_named"` + the `compile_script`
    normalizer call.
  - `adr080_rune_sandbox_allow_list_remains_toron_authoring_only` (`:515-516`)
    asserts both the `"select"` and `"select_named"` registrations.
- **Surfaces to change:** `RuneDataFrame::select` (the method), the shim fns
  (`normalize_rune_named_select_surface` + `skip_rune_quoted` /
  `skip_rune_line_comment` / `skip_rune_block_comment` / `is_ascii_ident_continue`),
  the `compile_script` two-line normalization call, the shim unit test
  `named_select_surface_normalizes_keyword_method_only_in_code`, the dual
  registration, and **6 `.select(` call-sites** across the 8 ADR 080 fixtures.

## Decisions (7, promotion-locked)

**D1 Authored surface = `.project_named(Vec<RuneNamedExpr>)`** — pairs with the
existing anonymous `.project(Vec<RuneExpr>)`, names the actual IR op
(`LogicalPlan::Project`), keyword-free. (Over `.select_named` (reads as a
workaround) and `.select_cols`/`.columns` (third vocabulary).)
**D2 Rename the Rust method too** (`RuneDataFrame::select` → `project_named`) so
impl + source-read guards share one vocabulary; no registration-only alias.
**D3 Delete the shim wholesale** — the rewriter, its four scanner helpers, the
`compile_script` normalization lines, and the shim unit test.
**D4 Register only `"project_named"`** — drop the dead `"select"` and the
`"select_named"` registrations.
**D5 Clean break, no compatibility alias** — `.select(...)` becomes a parse
error, not a silent dual surface. (We're mid plan/implement cadence; 080 just
landed.)
**D6 Reconcile the inverted guards (discipline #4):**
- Retarget `adr080_named_select_lowers_…`: marker `pub fn select` → `pub fn
  project_named`; keep the `RuneNamedExpr`/`Project`/`output_names`/no-Join/no-Aggregate
  assertions; **drop** the `normalize_*` + `FROM`/`TO`/`compile_script` clauses.
  Recommend renaming the guard `named_select` → `named_projection` (the word "select"
  in a guard that now forbids the select shim is actively misleading; provenance
  `adr080_` prefix preserved).
- Retarget `adr080_rune_sandbox_allow_list_…`: registration assertions → the single
  `m.associated_function("project_named", RuneDataFrame::project_named)?`.
- **Add** `adr081_rune_frontend_does_not_rewrite_user_source` — no `normalize_*` fn
  in `rune_engine.rs`; `compile_script` calls no normalizer.
- **Add** `adr081_authored_rune_methods_avoid_reserved_keywords` — registered
  associated-function name strings contain no Rune keyword (at minimum no
  `"select"`/`"as"`).
**D7 Everything else from 080 unchanged** — `RuneNamedExpr`, `RuneExpr::named`, the
aggregate helper pack, `Project.output_names` lowering, `ctx.sql`, SQL grammar,
binder, IR.

## Behavior gate (respelled, identical results)

The 8 ADR 080 fixtures move `.select(...)` → `.project_named(...)`; the multi-hop
gate must still **bind + execute** to `bucket="east", gross=30, n=1`, and the
duplicate-aggregate-name case must still hit the same `AmbiguousColumn`. Identical
composed `LogicalPlan` and results is the behavior-zero proof. The deleted shim unit
test is replaced by the two `adr081_*` source guards. Fixtures are **renamed** to
drop the stale `select` wording (`rune_named_select_*` → `rune_project_named_*`) so
no test name implies a `.select` surface that no longer exists (Codex+Claude 2-of-3;
Gemini kept the old names — body-only respell — recorded as the minority).

## Architecture Guards (post-081)

Retargeted: `adr080_named_projection_lowers_to_existing_project_output_names`,
`adr080_rune_sandbox_allow_list_remains_toron_authoring_only`. New durable:
`adr081_rune_frontend_does_not_rewrite_user_source`,
`adr081_authored_rune_methods_avoid_reserved_keywords`. The other 13 ADR 080 guards
are untouched (they pin carrier/alias-free/helper-pack/duplicate-name/no-join/etc.,
none of which change).

## Out of Scope

Alias-on-`RuneExpr`, mixed named/unnamed projection, aggregate aliases, join /
relation aliases / qualified-column sugar, `lit` widening, predicate residual
minimization, `CASE`. (ADR 082+ candidates.)

## Council History

ADR 080 review (Claude) flagged the shim as Finding #2 (necessary+documented+guarded
but a source rewriter for a cosmetic name). iter-0: Codex (pick C) + Gemini + Claude
converged on `.project_named` + delete shim; **Claude grounded the Rune keyword set**
(only `select`/`as` collide) and the **two-guard inversion** (discipline #4); Codex
adopted both. iter-1: adr/impl dual; consensus verified across Codex iter-1,
Claude iter-1, and Gemini iter-2 (genuine 3-of-3). Minor divergences (fixture
naming; guard granularity) resolved 2-of-3 to Codex+Claude.

## Promotion criteria

Behavior-zero (gate row + duplicate-name unchanged); ~90 lines + a unit test deleted,
no production machinery added; two 080 guards reconciled + two durable 081 guards;
Toron-only,
`rune_engine.rs`-localized. Migration:
`plans/impl/0081_rune_surface_naming.md`.
