# ADR 105: Adjacent Identity `Project` Coalescing

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — converged on the post-bind
optimizer hook and the same helper shape across the latest duals. **Toron-only** (binder
optimizer only); no SQL/Rune surface, IR/executor variant, PBC, storage, coordinator, Wendelon, or
`information_schema` change — the cut *removes* an execution node, adds none. **Predecessors:** 104
(unified alias column lists — the motivating redundant `Project`), 100 (transparent-receipt tracing
through `Project` chains), 089 (metadata-preserving rename). **Migration:**
`plans/impl/105_adjacent_identity_project_coalescing.md`.

## Doctrine — coalesce an adjacent identity `Project`, keep the parent schema

ADR 104 deliberately closed by leaving a redundant node: an alias rename is an ordinal-identity
`ExecutionPlan::Project`, so an explicit-derived alias stacks one over the inner `SELECT`'s `Project`.
The outer is a **value** identity but **not** a no-op **schema** identity — it renames (`id`→`pid`), so the
naive "drop a `Project` whose output schema equals its input schema" rule misses it. The correct transform
is adjacent composition: keep the parent's output schema, reuse the child's input and expression program,
drop one evaluator node.

```text
Project(output_schema = parent names, exprs = identity over child)          Project(output_schema = parent names,
  Project(output_schema = child names, exprs = child program)      ──►         exprs = child program)
    grandchild                                                                   grandchild
```

This is a plan-shape **normalization**, not a truth source: it changes the executed shape and nothing
observable about results, names, types, or identity.

## Grounding (verified)

- **The pattern recurs beyond ADR 104** (the second-site evidence that justifies a general helper over an
  alias-local fold):
  - *Explicit-derived alias* — `bind_alias_column_rename` (`binder.rs:845`) emits the rename `Project`
    over the inner derived `Project`.
  - *Plain derived pass-through* — `SELECT id FROM (SELECT id, note FROM people) AS p` already lowers to
    adjacent projects (outer select + inner derived select both bound). Predates 104.
  - *Rune projection chains* — `RuneDataFrame::project` / `.project_named` (`rune_engine.rs:215,226`) each
    wrap the plan in `LogicalPlan::Project`; chained calls bind 1:1 into adjacent `ExecutionPlan::Project`s.
- **The insertion point already exists.** `bind()` runs a recursive post-bind optimizer:
  `bind_plan(plan).await?` then `push_down_scan_projection(plan)` (`binder.rs:64`), which delegates to
  `optimize_for_required(plan, None)` (`:3588`). Its `ExecutionPlan::Project` arm (`:3676`) already prunes
  requested roots, computes child-required ordinals, optimizes the child, rewrites parent ordinals through
  the child remap, and rebuilds the output schema before reconstructing the `Project`. The coalescer is
  **one helper call at that final reconstruction** — not a new walker, not an optimizer framework, not an
  alias-binder special-case.
- **Elision is not the motivation.** Static filter classification runs during `bind_plan`, *before* this
  pass; `stored_refinement_receipt` (`binder.rs:1605`) already traces transparent `Project` chains
  (ADR 104's `alias_columns_preserve_refinement_elision_*` proves elision through the stack). Coalescing
  runs after elision is decided and cannot affect it.
- **The cost removed is per-morsel structural overhead.** `ExecutionPlan::Project` (`exec.rs:704`) runs
  `evaluate_bound_program` + `RecordBatch::try_new` per batch — for an identity project, Arc-array clones
  (cheap, not data copies) plus a `Vec`/`RecordBatch` allocation and an async frame. Real, repeated,
  avoidable; **not** a perf-regression fix. Claims stay modest.
- **Program APIs.** `BoundExprProgram.roots` is a public field (`ir/execution.rs:108`); `node` (`:586`),
  `root_count` (`:555`), `root_logical_type` (`:602`) return their values/`Option`.
  `decode_column_logical_type` (`ir.rs:320`) returns `Ok(LogicalTypeId::NAKED)` for an unannotated field
  and is already imported in `binder.rs`.

## Decisions (promotion-locked, 3-of-3)

**D1 — Use the existing post-bind optimizer path.** Implement in `binder.rs` inside
`optimize_for_required`'s `ExecutionPlan::Project` arm, replacing only the final reconstruction with
`collapse_adjacent_identity_project(input, exprs, output_schema)`. No SQL planner rewrite, no `LogicalPlan`
rewrite, no new pass entry point, no new IR/`ExecutionPlan` variant.

**D2 — Coalesce only adjacent `Project` over `Project`.** The helper fires only when the already-optimized
child input is `ExecutionPlan::Project`. It does not remove standalone projects and does not fold into
`Scan`, `SystemTable`, `EmptyRelation`, `Filter`, `Aggregate`, `Exchange`, `Sort`, `Join`, or DML sources.

**D3 — Parent must be a dense positional identity after pruning/remap.** Every parent root a direct
`BoundExprNode::Column`; parent root count equals child output arity; root `i` references ordinal `i`.
Required-output pruning may have narrowed the parent first; the coalescer accepts only the dense `0..N`
shape after that. Reorder, subset, duplicate, computed, cast, arithmetic, `LIKE`, `IN`, and
logical-ascription parent roots do not collapse. This constrains the **parent** only: an identity parent
over a *computed child* still collapses (the bare-column rename folds into the computed projection, e.g.
`(SELECT id + 0 FROM people) AS p(c)` → one computed `Project`), and the merged computed expr remains
non-transparent for elision — pinned by the green `alias_columns_computed_projection_still_not_transparent`
regression, not blocked.

**D4 — Metadata transparency is required; names are deliberately ignored.** Parent column physical type,
nullability, and durable `LogicalTypeId` must match the child output field at the same ordinal. Field
names are **not** compared — preserving the parent schema *is* the rename. NAKED matches NAKED on both
sides (`root_logical_type` → `Some(NAKED)`; `decode_column_logical_type` → `Ok(NAKED)`), so all-naked
columns coalesce; only a genuine identity *change* (ascription/cast) blocks.

**D5 — Parent schema wins; child input and child exprs carry the values.** On collapse:
`Project { input: child_input, exprs: child_exprs, output_schema: parent_output_schema }`. `output_remap`
from the existing arm is **unchanged** — it is computed from the requested outputs (`binder.rs:3694`)
independent of node construction, and D3's arity gate means collapse fires only when the parent passes
through *all* child columns in `0..N` order, so the collapsed node surfaces the parent's requested outputs
in the parent's positions.

**D6 — Base-table and wildcard-derived alias renames keep their project carrier.** Those rename projects
sit over scans, not child projects; folding them into `Scan` would require a separate scan-output-schema
rewrite and is out of scope.

**D7 — No general project fusion.** No arbitrary expression substitution, reorder folding,
`Project(Filter(Project))` collapse, standalone no-op removal, or rule registry. One adjacent-identity
composition.

**D8 — No semantic surface change.** SQL/Rune authoring, logical identity, refinement elision,
duplicate-alias ambiguity, PBC, executor variants, storage, Wendelon, and `information_schema` are
unchanged. The new contract is bound plan **shape**: one project, not two, where the adjacency is
redundant.

## Correctness backbone

1. **Value-equivalence.** When the predicate holds, the parent is a dense identity over the child output,
   so `parent(child(gc))` is child's output relabeled — exactly `Project{exprs=child_exprs,
   output_schema=parent_schema}(gc)`. `RecordBatch::try_new` is well-typed because parent field `i`'s
   type/nullability equal child output `i`'s (Arrow validates types/nullability, not name correspondence).
2. **`output_remap` invariance.** See D5 — positions are identical, so the grandparent's ordinal rewrite
   stays correct unchanged.
3. **Chain composition.** Collapse is decided post-order, so an N-deep identity chain coalesces pairwise to
   one `Project`; each collapsed node's `output_schema` is its parent's, exactly what the next level checks.
4. **Conservative on bad metadata.** The predicate returns `false` (not an error) on decode failure;
   catalog/binder fail-closed paths (088/090) own invalid-metadata errors.

## Behavior gates

- `explicit_derived_alias_columns_collapse_adjacent_identity_project` — `SELECT * FROM (SELECT id, note
  FROM people) AS p(pid, label)` → fields `pid,label`, one `Project`.
- `explicit_derived_alias_columns_collapse_after_outer_projection` — `SELECT pid FROM (SELECT id, note FROM
  people) AS p(pid, label)` → field `pid`, one `Project` (pruning + coalesce compose).
- `plain_derived_outer_projection_collapses_adjacent_projects` — `SELECT id FROM (SELECT id, note FROM
  people) AS p` → one `Project` (alias-independent recurrence).
- `base_table_alias_columns_keep_project_boundary` — `SELECT * FROM people AS p(pid, label)` → renamed
  fields, exactly one `Project` carrier over the scan (not zero).
- `wildcard_derived_alias_columns_keep_project_boundary` — `SELECT * FROM (SELECT * FROM people) AS p(pid,
  label)` → renamed fields, one carrier over the scan.
- `rune_project_named_chains_collapse_adjacent_projects` — chained `.project_named` → one
  `ExecutionPlan::Project` with the final names.
- Regression green: `alias_columns_preserve_refinement_elision_base_explicit_and_wildcard`,
  `alias_columns_computed_projection_still_not_transparent`, `alias_columns_duplicate_names_inherit_ambiguity`,
  `derived_table_source_scope_rejects_general_subquery_features`, and the 104 arity / type-annotation gates.

## Architecture guards

- `adr105_project_coalescing_lives_in_existing_post_bind_optimizer`
- `adr105_coalescing_is_adjacent_project_only`
- `adr105_identity_requires_ordinal_and_metadata_match`
- `adr105_identity_check_ignores_names_parent_schema_wins`
- `adr105_no_sql_ir_exec_pbc_storage_wendelon_information_schema_surface`

No prior guard inverts — ADR 104 guards pin rename correctness (all held by D7/D8). The new contract is
plan shape (one project, not two), which the 104 tests did not assert.

## Council history

ADR 104 closed naming "a future general project-collapse pass" as the right fix for its redundant identity
`Project`; the operator selected it for 105. Iter 0: Gemini seeded the lead but with a naive "drop a
project whose output schema equals its input schema" rule, which Codex and Claude independently corrected
(an alias rename changes the visible schema, so that rule never fires on the motivating case). Claude
framed a Fork A (fold locally in `bind_alias_column_rename`) vs Fork B (post-bind coalescing pass), leaning
A unless a second current recurrence justified B's weight. **Codex dissolved the fork** by grounding that
`optimize_for_required` already runs post-bind on every plan and its `Project` arm already reconstructs the
node — so B is one helper call in an existing arm, neither an alias special-case nor new machinery. Gemini
and Claude grounded the recurrence (plain derived pass-through + Rune chains), so the general helper earns
its keep. Iter 2: the latest duals converged on the same helper shape, the same D1–D8, the same five
guards, and the same gate set; Codex added the derived-source scope regression gate. The lesson carried
forward: before framing a placement fork, grep for an existing pass that already runs at that point — the
fork may be false.

## What to do next

If project chains persist after 105 (reorder/subset/computed adjacencies), promote a separate **general
project-fusion** ADR with explicit expression-substitution, metadata, ordinal, and eager-error proofs. Do
not widen 105. Named deferrals: project-into-`Scan` (base/wildcard scan-output rename); standalone no-op
identity removal over non-`Project` children; a rule-based optimizer tier.

## Promotion criteria

Adjacent parent-identity `Project` over child `Project` coalesces inside the existing post-bind optimizer;
parent output schema wins; SQL alias and Rune `.project_named` names survive; base-table and
wildcard-derived rename projects keep their scan carrier; computed/non-transparent projections gain no
refinement receipt; `output_remap` unchanged; N-deep chains collapse pairwise; ADR 100/104 gates green; no
SQL/IR/executor/PBC/storage/Wendelon/`information_schema` surface; Toron suite green. Migration:
`plans/impl/105_adjacent_identity_project_coalescing.md`.
