# ADR 173: Converted Logical-Unit Predicate Pushdown

**Status:** Promoted. **Migration:** `plans/impl/0173_converted_logical_unit_predicate_pushdown.md`.

The unit arc (167-172) made unit filters *correct* but not *fast*:
`WHERE CONVERT(distance, Kilometer) > 10` desugars (167 D7) to
`distance * factor > 10`, the PBC compiler **rejects arithmetic**, so the predicate
cannot push and every unit-filtered query scans. ADR 173 closes 167 D10's named
follow-on: normalize a metadata-authenticated converted linear-unit comparison
into a **bare-column predicate** that the existing PBC compiler accepts, as a
conservative storage pre-filter backed by an exact Toron residual.

### The idea (intuition)

`CONVERT(distance, Kilometer) > 10` is, after desugaring, `distance * 1.609344 > 10`
— mathematically identical to `distance > 6.2137…`. The second form is a plain
column-vs-constant predicate the storage layer already knows how to push. So 173 is
a bind-time, unit-metadata-authenticated algebra rewrite: invert the positive
linear scale onto the constant. Because the rewrite divides by a float, it pushes a
*slightly widened* predicate as a pre-filter and keeps the original CONVERT
predicate as the exact Toron re-check — correctness never depends on the float
division being exact.

## Grounding

- The PBC predicate compiler **explicitly rejects** `Add|Sub|Mul|Div` —
  `ExpressionShapeUnsupported("arithmetic … not PBC-eligible")` (`pbc.rs:107`,
  `pbc.rs:353`). So a converted predicate (`col * factor`) fails `compile_pbc`
  (`binder.rs:2114`).
- `predicate_pushdown_plan` (`binder.rs:3030`) calls
  `partition_bound_predicate` (`binder.rs:3091`) → `(pushed, residual)`, compiles
  `pushed` via `compile_pbc`, attaches the PBC program to the `Scan`
  (`attach_pbc_program:2127`), and runs `residual` as a Toron filter. A converted
  conjunct lands in `residual` (it isn't PBC-shaped), so a purely converted
  predicate pushes nothing today.
- The `PredicatePushdownPlan { pushed_predicate, residual_predicate }` split is
  already the conservative-pre-filter + exact-residual structure 173 needs.
- CONVERT desugar shapes (167 D7 / 171 D3): **linear** units → `col * factor`
  (single `Mul`, `bind_sql_convert:9059`); **affine points** → `col * factor +
  shift` (extra `Add`). 173 targets the linear shape only.
- `BoundExprProgram` carries `node_logical_type` metadata (`ir/execution.rs:596`),
  and CONVERT stamps the output node with the target logical identity
  (`bind_sql_convert:9158`). The pushdown recognizer can therefore authenticate
  that the `Mul` node is a converted unit expression, not arbitrary user
  arithmetic.
- For authenticated CONVERT, `factor = source.scale / target.scale > 0` always
  (167 validates positive scale), so the comparison direction never flips.
- Filter range selectivity is computed before `plan_filter_with_predicate_pushdown`
  (`binder.rs:632` before `:639`). Spending source-column range facts is therefore
  not automatic from PBC derivation; it needs an explicit converted range-atom path
  in `filter_range_selectivity_estimated_row_count`.
- `BoundExprNode` is a flat node graph: `Binary { op, left, right }`,
  `Column(ResolvedColumn)`, `Literal(u32)` (`ir/execution.rs:81`). Literal values
  read back via `BoundExprProgram::literal(id) -> Option<&ScalarValue>`
  (`ir/execution.rs:608`); match `ScalarValue::Float64(Some(_))`. The target shape
  is `Binary{cmp, Binary{Mul, Column(c), Literal(factor)}, Literal(const)}` (and the
  commuted form), with `c` a base scan column.

## Decisions

**D1 - Target shape: a metadata-authenticated linear CONVERT comparison on a base
column.** Recognize a comparison conjunct whose one side is `col * factor` (a `Mul`
of a base-column `Column` node and a `Float64` literal) and whose other side is a
`Float64` literal, with `cmp ∈ {<, <=, >, >=}`. Both operand orders are handled
(constant on either side, with `cmp` mirrored).

The shape alone is not enough. The recognizer must also prove:

- the `Mul` output node has a target logical type with unit metadata;
- the source column node has a source logical type with unit metadata;
- source and target dimensions and quantity kinds match;
- the observed `factor` exactly matches the registry-derived CONVERT factor.

If any of that metadata proof is absent, the optimization declines. Naked arithmetic
like `distance * 1.609344 > 10.0` stays on the pre-existing residual path.

**D2 - Rewrite by inverting the authenticated scale onto the constant.** Derive the
bare-column predicate:

```text
col * factor  cmp  const     →     col  cmp  (const / factor)
```

`factor > 0`, so `cmp` is preserved (no flip). The derived constant is computed in
`f64`. Non-finite factors, non-finite constants, and non-finite inverted thresholds
decline fail-open.

**D3 - Push a conservative superset; keep the original as exact residual.** The
derived constant is a float division and may round. The derived predicate is pushed
as a **storage pre-filter only**, nudged outward by one ULP so it can never drop a
row the original keeps (`>`/`>=`: nudge the constant **down** via `f64::next_down`;
`<`/`<=`: nudge **up** via `f64::next_up`). The **original** converted conjunct
stays in the Toron residual and re-checks exactly. Correctness is therefore
independent of the division's rounding; the pushdown only reduces the rows storage
returns.

**D4 - Integration: derive pushable conjuncts, leave residual exact.** In
`predicate_pushdown_plan`, after the existing `partition_bound_predicate`, add a
step that scans the predicate for D1-shaped conjuncts using the logical type
registry and produces derived bare-column conjuncts (D2/D3). The PBC program compiles
`AND(existing_pushable_conjuncts, derived_conjuncts)`; the residual remains the
original predicate minus only the conjuncts that were *exactly* pushable before.
The converted conjuncts always remain in the residual.

**D5 - The PBC compiler stays arithmetic-free.** 173 adds no `Mul` handling to the
PBC compiler; it feeds the compiler bare-column predicates produced upstream. The
`pbc.rs` arithmetic rejection is unchanged (and guarded).

**D6 - Scope: linear/delta units only this cut.** Only the `col * factor` shape
(offset 0, no `shift`) pushes. Affine-point converted predicates (`col * factor +
shift`, 171) stay residual-only and are a named follow-on — affine points also have
narrower comparison semantics (point-vs-point, 171 D6). Equality (`=`/`!=`) on a
converted column stays residual-only this cut (exact float equality after division
is fragile; a bracketed-equality push is deferred).

**D7 - No new storage/wire/executor surface.** The derived predicate flows through
the existing `pbc_predicate` plumbing (`storage.rs`/`exec.rs`); no PBC opcode,
scan-request field, descriptor, or wire change.

**D8 - Frontend-shared; selectivity reconnect is explicit.** The rewrite is on the
post-bind predicate, so SQL and Rune CONVERT predicates push identically. The range
selectivity path must also derive the same source-column range atom, because it runs
before the pushdown planner. No new fact schema or producer is needed, but
`filter_range_selectivity_estimated_row_count` must spend the source column's
existing range facts through the authenticated converted atom.

## Headline Gates

- `converted_linear_predicate_pushes_as_bare_column` -
  `WHERE CONVERT(distance_mile, Kilometer) > 10.0` attaches a PBC program to the
  `Scan` over the base `distance_mile` column (today: none), and the query returns
  the correct rows.
- `converted_pushdown_keeps_exact_residual` - a row whose value sits inside the
  one-ULP widening is returned by the storage pre-filter but dropped by the exact
  residual, so the final result equals the unpushed result (boundary correctness).
- `linear_convert_pushdown_matches_unpushed_results` - for a range of constants and
  both directions (`<`,`<=`,`>`,`>=`), pushed and unpushed plans return identical
  rows.
- `converted_linear_predicate_spends_source_range_fact` -
  `WHERE CONVERT(distance_mile, Kilometer) > 10.0` over a source `distance_mile`
  range fact changes the estimated row count through the source-column range path.

## Supporting Gates

- `affine_point_converted_predicate_stays_residual` - `WHERE CONVERT(temp_c,
  Fahrenheit) > CAST(50 AS Fahrenheit)` does not push (no PBC program); result
  correct.
- `equality_on_converted_column_stays_residual` - `CONVERT(distance_mile, Kilometer)
  = 10.0` does not push; result correct.
- `non_converted_predicate_pushdown_unchanged` - existing bare-column and mixed
  predicates push exactly as before (regression).
- `mixed_predicate_pushes_converted_and_bare_conjuncts` -
  `WHERE CONVERT(distance_mile, Kilometer) > 10 AND region = 'us'` pushes both a
  derived `distance_mile` bound and the `region` predicate; residual re-checks the
  converted conjunct.
- `rune_converted_predicate_pushes` - the same filter built through Rune pushes
  identically (frontend parity).
- `converted_pushdown_direction_not_flipped` - `<` stays `<`, `>` stays `>` (factor
  positive).
- `naked_arithmetic_scaled_predicate_stays_residual` -
  `distance_mile * 1.609344 > 10.0` without CONVERT logical metadata does not push.
- `converted_pushdown_rejects_factor_metadata_mismatch` - a `Mul` factor that does
  not match the source/target unit registry is not pushed.

## Architecture Guards

- `adr173_converted_linear_predicate_normalized_before_pbc` -
  `predicate_pushdown_plan` derives bare-column conjuncts from `col * factor cmp
  const` before `compile_pbc`.
- `adr173_convert_pushdown_is_metadata_authenticated` - the recognizer checks
  source/target logical unit metadata and the registry-derived factor, not just a
  `Mul` node shape.
- `adr173_pbc_compiler_stays_arithmetic_free` - `pbc.rs` still rejects
  `Add|Sub|Mul|Div` (reuse/extend the existing rejection assertion).
- `adr173_converted_pushdown_keeps_exact_residual` - the original converted conjunct
  remains in the residual; the derived conjunct is push-only.
- `adr173_range_selectivity_uses_converted_source_atoms` -
  `filter_range_selectivity_estimated_row_count` has a converted source-atom path
  with logical type context.
- `adr173_affine_and_equality_pushdown_deferred` - affine-shift predicates and
  `=`/`!=` on converted columns are not pushed.
- `adr173_no_pbc_opcode_or_storage_wire_surface` - no scan-request/PBC/storage/
  descriptor change.
- `adr173_converted_pushdown_is_frontend_shared` - the rewrite is on the post-bind
  predicate, not a frontend path.

## Non-Goals

- no affine-point converted predicate pushdown (linear/delta only);
- no `=`/`!=` converted pushdown (residual-only);
- no PBC arithmetic eligibility (the compiler stays bare-shape);
- no new scan-request, storage, descriptor, or wire field;
- no change to non-converted predicate pushdown;
- no executor kernel or optimizer-fact producer change; range selectivity reuses
  existing source-column facts through an explicit consumer hook.

## Consensus Trail

The council took the completed unit arc into production performance. Grounding
confirmed the cut is real (PBC rejects arithmetic, so converted predicates scan
today) and handed the mechanism: invert the positive linear scale onto the
constant, push a one-ULP-widened bare-column pre-filter, keep the original CONVERT
predicate as the exact residual. The conservative-push + exact-residual split
reuses the existing `PredicatePushdownPlan` structure, so correctness never depends
on float-exact division. The final promotion keeps the smaller linear/delta scope
and defers affine-point and equality pushdown, while making two grounded constraints
explicit: the matcher authenticates CONVERT through logical-unit metadata, and
range selectivity needs its own source-atom hook rather than a free reconnect.

## Forward Arc

`172 point/delta algebra -> 173 converted linear-unit predicate pushdown ->
affine-point + equality converted pushdown / unit display formatting`.

After 173, the unit arc is production-performant: a unit filter reaches storage as a
bare-column predicate and rides the existing selectivity facts, while the exact
residual guarantees the converted semantics.

## Deferred (named)

Affine-point converted predicate pushdown; bracketed equality pushdown on converted
columns; teaching PBC linear-transform shapes directly (kept upstream for now);
unit display formatting; cross-unit point delta; richer Rune affine syntax.
