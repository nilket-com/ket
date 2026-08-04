# ADR 170: Complete Logical-Identity Arithmetic Strictness

**Status:** Promoted. **Migration:** `plans/impl/0170_complete_logical_identity_arithmetic_strictness.md`.

ADR 169 made a non-unit logical identity (e.g. `CustomerId`) reject binary `+`,
`SUM`, and `AVG`. It scoped to additive operations and left `-`, `*`, `/` on the
old physical-fallback path. The result is a half-closed surface: `customer_id + 1`
rejects, but `customer_id - 1`, `customer_id * 2`, `customer_id / 2`, and
`customer_id - customer_id` still silently do naked carrier arithmetic — the same
meaningless operation 169's headline set out to stop, leaking through three other
operators.

ADR 170 closes the surface: a logical identity rejects **every** arithmetic
operator. It also pins, with prose and a gate, that the identity class already
includes refinement types (the scope 169 implemented but documented only in
`CustomerId` terms).

The cut is Toron-local binder safety. `BoundBinaryOp::{Sub, Mul, Div}` join `Add`
in rejecting logical identities unless the author erases the identity with a
primitive cast. Unit quantities and naked numerics keep their ADR 167 behavior.

## Grounding

- The arithmetic operator set is exactly `Add`, `Sub`, `Mul`, `Div`
  (`BoundBinaryOp`, `ir/execution.rs:58`); there is no `Mod`. 169 gated only
  `Add`.
- For a non-unit identity, `logical_unit_for_node` returns `None`
  (`binder.rs:9545`, `unit.is_some()` is false). In `bind_dimensioned_arithmetic`
  the both-units-`None` arm short-circuits to `Ok(None)` (`binder.rs:9404`), so
  `-`/`*`/`/` fall through to physical numeric arithmetic and an identity is
  silently carried.
- 169 already classifies operands with `AdditiveLogicalDomain`
  (Naked/Quantity/Identity, `binder.rs:9301`/`9312`) and rejects identities via
  `non_additive_logical_type_error`, used at exactly 4 identity sites
  (`binder.rs:9366`, `9382`, `10678`). Extending to all operators reuses this
  model; ADR 170 renames the binder-local surface to logical arithmetic so the
  helper name matches its widened job.
- The untyped-NULL path has two early-return arms — `(true,false)` and
  `(false,true)` on `is_untyped_null` (`binder.rs:9183`, `9204`) — each currently
  gating the reject on `matches!(op, Add)` only. That lets
  `customer_id - NULL` leak, and it can also let a unit quantity bypass
  `bind_dimensioned_arithmetic` and bind as naked physical arithmetic.
- Identity `-`/`*`/`/` are meaningless under **any** algebra, so — unlike
  Quantity subtraction — they need no affine point/delta model (refining 169's
  D7: "subtraction is where affine matters" is a *Quantity* statement, not an
  *Identity* one).
- Rune lowers `sub`/`mul`/`div` to `Expr::Sub/Mul/Div` (`rune_engine.rs:916/923/930`)
  into the same `bind_arithmetic` seam, so the gate is frontend-shared by
  construction.
- **Blast radius (audited): zero existing identity `-`/`*`/`/` usages.** A crate-wide
  grep for `customer_id`/`product_id`/`person_id`/bare-`id` followed by `-`/`*`/`/`
  (tests + src) returns nothing. Unlike 169's `+` (three `id + 0` fixtures), 170
  rewrites no existing query — the only edits to landed code are the D2 string
  and helper-name travel on affected 169 tests/guards.
- `people.id` is `PersonId AS BIGINT CHECK (VALUE > 0)` — a refinement type with
  `unit.is_none()`. 169 already rejects `people.id + 0` (the three CAST-wrapped
  fixtures in `logical_type_ddl_test.rs` are the footprint); 170 documents and
  pins this.

## Decisions

**D1 - A logical identity rejects every arithmetic operator.** For
`BoundBinaryOp::{Add, Sub, Mul, Div}`, if either operand is an Identity (non-naked,
`unit.is_none()`), bind rejects before physical/dimensioned fallback. 169 already
covers `Add`; 170 adds `Sub`, `Mul`, `Div`. Covers `customer_id - 1`,
`customer_id * 2`, `customer_id / 2`, `customer_id - customer_id`,
`customer_id * raw`, `customer_id / CAST(other AS BIGINT)`, and the mixed
`identity ∘ quantity` forms (any Identity operand rejects regardless of the other
side).

**D2 - Generalize the binder-local naming from "additive" to "arithmetic."**
Rename the classifier and diagnostic surface:
`AdditiveLogicalDomain` -> `LogicalArithmeticDomain`,
`additive_logical_domain_for_node/type` ->
`logical_arithmetic_domain_for_node/type`, and
`non_additive_logical_type_error` -> `non_arithmetic_logical_type_error`.
The diagnostic changes from "is not an additive quantity for {context}" to "is
not an arithmetic quantity for {context}", with the same "use an explicit
primitive CAST" hint. `{context}` is the operator symbol (`+`/`-`/`*`/`/`) or
aggregate name (`SUM`/`AVG`). This is behavior-neutral generalization that
travels with the surface 170 touches; it updates the strings asserted by 169's
`strict_additive_operations_reject_identity` test and any 169 guards that
reference the old names/message. Chosen over keeping "additive" wording because
the diagnostic is user-visible on the new operators, where "not an additive
quantity for *" reads wrong.

**D3 - Untyped-NULL parity across all operators.** Generalize
`reject_logical_addition_with_untyped_null` ->
`reject_logical_arithmetic_with_untyped_null` (threaded with `op`) and call it from
both `is_untyped_null` arms for `Add|Sub|Mul|Div`. An untyped-NULL operand
opposite an Identity rejects for all four operators. An untyped-NULL operand
opposite a Quantity also rejects: the shortcut returns before
`bind_dimensioned_arithmetic`, so accepting it can silently drop the quantity's
logical identity. Authors can still write an explicit typed NULL or primitive
cast where ADR 167 already allows a naked scalar operand, such as
`distance_mile * CAST(NULL AS DOUBLE)` or
`distance_mile / CAST(NULL AS DOUBLE)`. This is not a new quantity algebra rule;
it is a fail-closed inference rule for untyped NULL.

**D4 - Refinement types are non-arithmetic identities (pin the 169 finding).** The
identity class is "non-naked logical type with `unit.is_none()`," which includes
refinement-only specs (e.g. `PersonId CHECK (VALUE > 0)`). Stated in the model and
pinned by a behavioral gate so the scope is explicit rather than discovered via
fixture edits. Refinements are erased, not preserved, by arithmetic (consistent
with ADR 113's computation-erases rule); the receipt is the same primitive `CAST`.

**D5 - Quantity arithmetic for `-`/`*`/`/` is unchanged.** ADR 167 rules stay in
force and continue to flow through `bind_dimensioned_arithmetic`:

- same-unit `-` preserves the unit; same-dimension different-unit `-` requires
  `CONVERT`; cross-dimension `-` rejects;
- `unit * naked` / `naked * unit` / `unit / naked` preserve the unit;
- `unit * unit`, `unit / unit`, `naked / unit` stay deferred (derived dimensions /
  ratios);
- Quantity subtraction's affine point/delta semantics remain deferred.

The identity gate returns control (no reject) for quantities on `-`/`*`/`/`, so
they reach `bind_dimensioned_arithmetic` exactly as before.

**D6 - Comparison, selection, and cardinality are unchanged.** `Eq/Neq/Lt/Lte/Gt/Gte`
keep the `nominal_compatible` path; `MIN`/`MAX` preserve identity (ADR 113/168);
`COUNT` stays naked. An identity can be compared and selected; it just cannot be
arithmetic'd.

**D7 - Frontend-shared.** The gate lives in the shared `bind_arithmetic` seam;
SQL and Rune (`sub`/`mul`/`div` -> `Expr::Sub/Mul/Div`) hit it identically, and
the escape hatch is each frontend's primitive cast (SQL `CAST(... AS BIGINT)`,
Rune `expr.cast(type_i64())`). No frontend-specific arithmetic path.

**D8 - Binder-only.** Bind-time domain check. No aggregate kernel, executor, PBC,
Wendelon, descriptor, storage, optimizer-fact, or wire change. No aggregate
surface change (169 settled `SUM`/`AVG`; there is no subtract/multiply aggregate).

## Headline Gates

- `strict_identity_arithmetic_rejects_all_operators` - `customer_id - 1`,
  `customer_id * 2`, `customer_id / 2`, and `customer_id - customer_id` reject
  with the non-arithmetic diagnostic (and `customer_id + 1` still rejects).
- `explicit_cast_erases_identity_for_all_arithmetic` -
  `CAST(customer_id AS BIGINT) - 1`, `* 2`, and `/ 2` bind and execute as naked
  numeric.
- `quantity_subtraction_and_scalar_scaling_unchanged` - `distance - distance`,
  `distance * 2`, and `distance / 2` still carry `Mile`.
- `refinement_identity_arithmetic_rejects` - `person_id + 0`, `- 1`, `* 2`, `/ 2`
  all reject; `CAST(person_id AS BIGINT) ∘ k` binds naked.

## Supporting Gates

- `logical_null_arithmetic_rejects_for_all_operators` - identity operands such
  as `customer_id - NULL`, `NULL * customer_id`, and `customer_id / NULL`
  reject; quantity operands opposite untyped NULL also reject, while typed NULL
  scalar cases remain valid only where ADR 167 allows naked scalar operands.
- `naked_numeric_arithmetic_unchanged` - `raw - raw`, `raw * raw`, `raw / raw`
  stay naked and execute.
- `rune_identity_arithmetic_rejects_and_cast_erases_all_operators` - through Rune,
  `sub`/`mul`/`div` over an identity reject; `.cast(type_i64())` first binds naked.
- `identity_comparison_and_selection_still_allowed` - 169 regression holds
  (`= 7`, `MIN`/`MAX`, `COUNT`).
- `same_dimension_subtraction_still_requires_convert` /
  `cross_dimension_subtraction_still_rejects` - ADR 167 unit regressions.
- 169 additive gates remain green (the `+`/`SUM`/`AVG` rejects keep firing under
  the generalized diagnostic).

## Architecture Guards

- `adr170_all_arithmetic_operators_gate_logical_identity` - `bind_arithmetic`
  routes `Add|Sub|Mul|Div` through the logical-arithmetic classifier before
  dimensioned/physical fallback (ordering asserted positionally, as in 169).
- `adr170_identity_rejects_every_arithmetic_operator` - the logical-arithmetic
  helper rejects `Identity` for all four operators via
  `non_arithmetic_logical_type_error`.
- `adr170_untyped_null_arithmetic_rejects_logical_operands_for_all_operators` -
  the untyped-NULL guard fires for `Add|Sub|Mul|Div`, rejects Identity, and
  rejects Quantity rather than bypassing `bind_dimensioned_arithmetic`.
- `adr170_refinement_types_are_non_arithmetic_identities` - the classifier treats
  a non-naked `unit.is_none()` (refinement) spec as `Identity`, not `Naked`.
- `adr170_quantity_arithmetic_unchanged` - `bind_dimensioned_arithmetic` still
  owns Quantity `-`/`*`/`/`; the identity gate is not applied to quantities for
  those ops.
- `adr170_comparison_selection_stay_identity_aware` - the arithmetic discriminator
  does not leak into `bind_comparison`, `bind_in_list`, `static_filter_atom_class`,
  or the `MIN`/`MAX` arms.
- `adr170_identity_arithmetic_gate_is_frontend_shared` - Rune `sub`/`mul`/`div`
  lower to `Expr::Sub/Mul/Div` into the shared seam; no Rune-specific path.
- `adr170_no_runtime_storage_or_wire_surface` - no kernel/executor/PBC/Wendelon/
  storage/descriptor/wire change.
- Retarget the 169 guards/test whose asserted strings change under D2.

## Non-Goals

- no affine units, offsets, temperature, or point/delta algebra (Quantity
  subtraction stays deferred);
- no derived dimensions or ratios (`unit * unit`, `unit / unit`);
- no `Mod` operator (none exists);
- no change to comparison, `MIN`/`MAX`/`COUNT`, or any aggregate;
- no new public SQL/Rune surface; erasure reuses each frontend's primitive cast;
- no Wendelon, storage, PBC, optimizer-fact, or executor-kernel change.

## Consensus Trail

The council selected this as the tight completion of the 169 surface. The one
fork — generalize the diagnostic (D2, touches landed 169 strings) vs keep
"additive" wording — is decided in favor of generalize: behavior-neutral, fixes a
user-visible message on the new operators, reversible. Grounding closed the main
implementation risk: the crate has zero existing identity `-`/`*`/`/` usages, so
the cut is additive (new rejects + new tests + D2 string travel), reopening no
landed query. Codex's remaining correction is the untyped-NULL shortcut: it must
reject logical operands broadly rather than becoming a naked-arithmetic bypass
for identities or quantities.

## Forward Arc

`169 strict additive -> 170 complete identity arithmetic strictness -> affine
units & point/delta algebra (Temperature; Quantity subtraction semantics) -> unit
display formatting -> converted predicate pushdown -> Rune optimizer cockpit`.

After 170, the logical-identity arithmetic surface is fully closed: identities are
comparable and selectable; only quantities are arithmetic; refinements are
explicitly non-arithmetic; the primitive cast is the uniform receipt. The next big
cut (affine) reopens *Quantity* arithmetic, not identity arithmetic.

## Deferred (named)

Affine units and temperature; Quantity point/delta subtraction; derived dimensions
and ratios; unit display formatting; converted predicate pushdown; Rune optimizer
cockpit and receipt API; `DISTINCT`/window aggregate identity policy; statistical
aggregate unit propagation; richer Rune erasure syntax.
