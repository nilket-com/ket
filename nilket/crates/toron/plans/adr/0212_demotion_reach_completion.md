# ADR 212: Demotion Reach Completion

**Status:** Promoted. **Migration:** `plans/impl/0212_demotion_reach_completion.md`.
**Subtitle:** OR-Aware Null Rejection and Identity-Remapped Alias/Project Descent.
**Category:** Toron optimizer logical normalization. No executor, fragmenter, storage, wire, Wendelon,
runtime-filter, strategy, authoring-syntax, or durable-evidence change. The last unconditional cut of the
outer-join arc: after this, the arc carries zero ungated items and the next cycle rotates by receipts.

## Why this cut exists

ADR 209 made null-rejected outer joins demotable; 210 made the proof path-scoped through inner joins,
`Exchange`, and pure `Sort`; 211 made the safe broadcast shape authorable. The remaining unconditional gap
is reach: filters that are *already safe* still fail to arrive at the demotion point in two common shapes —
boolean forms (`WHERE d.tier = 1 OR d.tier = 2`) and wrapper forms (a filter above an `Alias` or a
direct-column `Project`, the derived-table cleanup pattern). This cut widens what the demotion machinery
*sees* without changing what demotion *is*: the 209 classifier allowlist grows exactly one arm
(intersection-ruled `Or`), and the 210 descent router gains exactly two wrapper arms (identity-only).
Graph-family advisory readout and evidence-driven ordering remain named follow-ons — evidence-gated, where
this cut has direct red-on-main discriminators.

## Grounding (verified against the code)

- `null_rejecting_sides` unions `And` branches but explicitly denies `Expr::Or` (outer_join_demotion.rs:695)
  — while `conjunct_column_sides` **already walks `Or`** (:794): ownership analysis is broader than
  rejection classification, so OR-awareness is purely a classifier arm, not new traversal.
- `route_filter_conjuncts` descends inner joins, `Exchange`, and pure `Sort`; `Alias` and `Project` sit in
  the stop group.
- `Alias { input, alias, columns }` (ir.rs:948) is a positional rename;
  `alias_schema_for_outer_demotion` (:858) already enforces the arity match and the empty-column-list
  (qualifier-only) case — the descent remap is its inverse.
- `logical_scope_for_outer_demotion` **intentionally returns `None` for `Project`** — project motion must
  build its own passthrough map rather than treating `Project` as transparent.
- The 210 ownership helper (`conjunct_column_sides`) is already available at the same altitude as the
  demotion proof. ADR 212 must use it at the non-inner relocation edge: OR intersection can prove a
  null-supplied side while the full conjunct still references both sides, so proof side and ownership side
  are no longer equivalent.
- Receipts carry `propagation_path: Vec<JoinSideStep>` (210) — wrapper steps extend the vocabulary, no new
  row kind.

## Decisions (promotion-locked)

**D1 — OR null rejection uses intersection.** `Or(a, b)` rejects side S iff **both** branches reject S.
`NullRejectedSides::intersect` lands beside `union`; only `Expr::Or` moves from the deny group to a
classify arm. No OR distribution, no algebraic rewrites; `IsNull`, negated `Like`/`InList`, `Case`, casts,
and unknown shapes stay default-deny. The dangerous cases, pinned: `d.x = 1 OR d.x = 2` rejects `d`;
`d.x = 1 OR d.x IS NULL` rejects nothing; `l.x = 1 OR r.x = 1` rejects neither side.

**D2 — Demotion proof and relocation stay separate.** A conjunct can prove rejection without being
movable: `d.x = 1 OR (d.y = 2 AND f.z = 3)` demotes the left join (both branches reject `d`) while
remaining residual above it (cross-side). The receipt records the original proof conjunct; only side-local
demotion-owned conjuncts install below an input. This is the existing 209 structure, now load-bearing for
OR — pinned by its own gate.

Implementation consequence: the non-inner join arm must compute both the proof sides
(`null_rejecting_sides`) and the full ownership sides (`conjunct_column_sides`) for each routed conjunct.
Relocation is allowed only when ownership is exactly the demotion-owned side and not ambiguous. Proof sides
alone are not sufficient after D1.

**D3 — Alias descent is identity remapping with unique input resolution.** A conjunct routes below an
`Alias` only when every referenced alias output maps positionally to an input field **and the remapped
reference resolves uniquely in the input scope** (a derived table over a join can hide name collisions
below the alias's flat namespace — ambiguity stops the conjunct). Empty column lists (qualifier-only
rename) are supported; the alias wrapper is preserved around the routed input.

**D4 — Project descent is direct-column passthrough only.** A per-output map is built only from direct
`Expr::Column` projection entries (rename allowed); a conjunct moves iff every referenced output maps
through it. Computed expressions, duplicate output names, missing names, and ambiguous unqualified
references stop the conjunct. `SELECT d.x AS x` moves; `SELECT d.x + 1 AS x` does not.

**D5 — Residuals preserve original form (pair tracking, not un-remapping).** Wrapper arms route
`(original, routed)` conjunct pairs: relocation installs the routed form below the wrapper; a conjunct
that descends but stops deeper hoists back re-emitting its **original** outer-scope spelling — inner names
never leak above a wrapper, and no inverse-remap function exists.

**D6 — Every prior fence stays closed.** The motion set is exactly: inner joins, `Exchange`, **pure
`Sort`** (`limit: None && offset == 0`), `Alias` (D3), and identity `Project` (D4). `Take`,
limited/offset `Sort`, `Aggregate`, non-target non-inner joins, scans, statements, and unknown shapes
remain explicit stop arms — no wildcard; a new `LogicalPlan` variant forces a decision.

**D7 — Receipts name the wrapper steps.** The propagation-path vocabulary gains alias/project steps
(e.g. `left.alias(t).project.right`) so EXPLAIN shows the proof's full route. No new row kind, table, or
durable surface.

**D8 — One semantic pass, both frontends.** The semantic change is confined to the shared pre-bind pass
(`outer_join_demotion.rs`), with EXPLAIN rendering updated only if the path-step representation changes.
SQL and Rune stay identical by construction, with the Rune parity gates named anyway (the 209 lesson:
prove parity, don't assume it).

## Honesty gates

Red on `main`:
- `or_both_branches_null_rejecting_demotes_left_join` (the classifier denies OR today);
- `alias_wrapped_left_join_filter_reaches_demotion` (the router stops at Alias today);
- `project_passthrough_filter_reaches_demotion` (likewise for Project).

**Load-bearing discriminators:**
- `or_one_branch_is_null_does_not_demote` + `or_branches_reject_different_sides_do_not_demote` — fail
  under intersection→union (the named falsification).
- `or_cross_side_conjunct_demotes_without_moving` — D2's split, pinned.
- `computed_project_column_stops_motion` — fails under unconditional Project transparency (the second
  named falsification).
- `alias_project_residuals_preserve_original_filter_shape` — fails if pair tracking is dropped (the third
  named falsification: a routed residual leaking inner names).
- `alias_remap_ambiguous_resolution_stops` + the empty-columns qualifier-only twin (D3's two edges).
- `remapped_conjunct_results_match_unwrapped_query` — the A/B net over both wrappers.
- `rune_or_filter_and_alias_descent_match_sql` — the parity loop.
- The 209/210/211 behavioral suites pass without weakening; named source-guard retargets are listed below.

## Architecture guards (promotion-locked)

- `adr212_or_null_rejection_uses_intersection`
- `adr212_demoting_cross_side_or_keeps_conjunct_residual`
- `adr212_alias_descent_requires_identity_remap_unique_resolution`
- `adr212_project_descent_requires_direct_column_passthrough`
- `adr212_residuals_preserve_original_form`
- `adr212_receipt_path_names_wrapper_steps`
- `adr212_motion_fences_remain_closed`
- `adr212_rune_and_sql_share_outer_join_demotion_entry`
- `adr212_no_executor_fragmenter_wire_runtime_filter_strategy_or_wendelon_change`

**Reconciled prior guards:** `adr209_null_rejection_classifier_is_default_deny_allowlist` stops pinning
`Expr::Or(_, _)` as denied and instead pins the intersection arm plus the OR discriminators.
`adr210_motion_set_is_inner_exchange_and_pure_sort_only` gains the two constrained wrapper arms in its
allowlist — the widening its own text anticipated, named here per discipline #4.
`adr210_propagation_extends_existing_outer_join_demotion_receipt` accepts the wrapper path vocabulary if
`JoinSideStep` is generalized. All other `adr209_*`/`adr210_*`/`adr211_*` guards pass unchanged or the
design is wrong.

## Rejected alternatives

**OR-aware alone** — right direction, below the ADR grain; the split spends two cycles re-opening the same
file and harness. **OR distribution / algebraic rewriting** — changes the classifier's character from
allowlist to rewriter; deferred. **Unconditional wrapper transparency** — `Project` with computed columns
is unsound to cross; `Alias` over colliding names is unsound to remap blindly. **Inverse-remap on hoist** —
pair tracking is simpler and cannot drift. **Rotation to a gated thread** — every non-join candidate's
condition remains unfired; building ahead of evidence is the discipline this arc was built to end.

## Consensus (transparent)

3-of-3 on the fold and the full D-set. Trail: Claude proposed the fold (grain argument); Codex
independently converged from his own graph-readout candidate (gating argument) and contributed the
residual-original-form rule, the D2 elevation, and the falsification set; Gemini seeded the OR framing and
the receipt-step vocabulary. The merged text corrects the unqualified-`Sort` slip from the drafts: D6
carries the pure-Sort rule explicitly, preserving the ADR 210 fence. Claude's iter-0 concession trigger
(split if the remap outgrew the positional-bijection read) did not fire: unique resolution + pair tracking
keep it v1-honest.

## Forward arc

`211 safe-side authoring → 212 demotion reach completion (this) → **rotation by receipts**: graph-family
advisory readout or evidence-driven outer ordering, whichever the accumulated verdicts justify →
mixed-chain pushdown (named demanding shape required) → memo only when receipts show local decisions
fighting`.

## Deferred (named)

Algebraic predicate rewriting (OR distribution, De Morgan); computed-projection motion; multi-level alias
chains beyond natural unique resolution; general mixed-chain pushdown; graph-family advisory readout and
evidence-driven outer ordering (receipts accumulating); memo/Cascades.
