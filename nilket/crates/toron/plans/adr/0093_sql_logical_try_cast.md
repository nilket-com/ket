# ADR 093: SQL `TRY_CAST(x AS LogicalType)` — Nullable Coercing Ascription

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — verified against the
latest iter-1 drafts (Claude, Codex, Gemini): all three carry the identical shape
(reuse transient `SqlLogicalCast { try_cast: true }`; un-gate the durable pre-resolution + bind;
reconcile-before-coerce with identity conflict still rejecting at bind; **contract γ** —
categorically-nullable, binds wherever strict CAST binds; binder-local, no shared-gate/kernel/Wendelon
change). **Completes the CAST/TRY_CAST 2×2** for the nominal type system (ADR 026 physical CAST +
TRY_CAST; 092 logical CAST; 093 logical TRY_CAST). **Predecessor:** ADR 092 (`87ea732`). **Toron-only.**
**Scope:** `binder.rs` (two un-gate sites + one branch), tests. **No** Wendelon, executor kernel, PBC
opcode, Rune surface, planner/IR shape, or coordinator→worker payload change; **no new nominal gate**
(087/088 enforce post-bind); typed CTAS via 089 composition (no new CTAS grammar).

## Doctrine — the receipt, softened at the value domain only

`TRY_CAST(x AS T)` is **a nullable physical conversion plus a logical ascription**. The "try"
applies *only* to runtime physical value failures (overflow / range-domain failure in the existing
numeric lattice) — it emits `NULL`, not a
`Kernel` error. It does **not** soften the *identity* contract: a logical mismatch is a compile-time
program error, and there is nothing to "try" at runtime. The rule:

> `TRY_CAST(x AS T)` binds **iff** `T` is durably defined ∧ `reconcile(x.identity, T)` does not
> conflict ∧ `x`'s physical → `T.physical` is same-physical or in the existing numeric lattice.
> The reconcile runs **before** any physical `TryCast` node is appended.

```sql
TRY_CAST(raw_i64 AS SmallCustomerId)               -- ok: Int64->Int32 failures -> NULL; nullable
TRY_CAST(product_id AS CustomerId)                 -- reject at bind (NominalTypeMismatch), not NULL
TRY_CAST(CAST(product_id AS BIGINT) AS CustomerId) -- ok: explicit primitive erase (087/092 D9)
```

TRY_CAST is **not a bypass for the nominal type checker.** This is the whole feature: it preserves
"magic enforces only when it has a receipt" — softening physical failure must not become an
identity-laundering escape hatch.

## Grounding (the facts that fixed the shape)

- **Two rejection sites, both string-identical.** Logical TRY_CAST is rejected in *two* places:
  the async pre-resolution pass `collect_logical_cast_names` (`binder.rs:809-813`) — so logical
  TRY_CAST names are never even collected for durable resolution — and the defensive bind-time gate
  in `bind_sql_logical_cast` (`binder.rs:3119-3123`). Both carry the literal
  `"TRY_CAST/SAFE_CAST to logical type is not supported"`, which the assertion inside
  `adr092_binder_resolves_sql_logical_casts_registry_only_before_sync_bind`
  (`nominal_type_identity.rs:773`) scans for; removing both is exactly what inverts it (discipline #4).
- **Same-physical is categorically nullable and free.** `root_nullable(TryCast) = Some(true)`
  (`ir/execution.rs:603`); `evaluate_try_cast` has a same-type passthrough
  (`exec.rs:4118`: `if array.data_type()==target { return Ok(array.clone()) }`), so `Utf8→Utf8` /
  `Boolean→Boolean` need **no new kernel**. The physical "must not elide identity TryCast" guard
  (`sql_planner_surface.rs:874,878`) establishes that a TryCast node is always nullable — logical
  TRY_CAST matches that principle for schema consistency.
- **Stay out of physical `bind_try_cast`.** That helper is intentionally identity-free per ADR 087's
  cast-erase doctrine (`nominal_type_identity.rs:187,192-193` assert it never calls
  `set_node_logical_type`) and carries a numeric-only gate. So γ is implemented **locally in
  `bind_sql_logical_cast`** — which already does its own physical-step decision — not by routing
  through `bind_try_cast` and not by widening the shared `is_supported_explicit_numeric_cast`.
- **CTAS nullability already flows.** `schema_for_exprs_with_output_names` (`binder.rs:3735`) and
  `schema_for_projected_exprs_with_existing_names` (`binder.rs:3764`) thread
  `nullable = root_nullable(idx)` into `field_with_logical_type_metadata`; `infer_ctas_schema`
  preserves `field.is_nullable()` (`binder.rs:941`). A logical TRY_CAST projection root is nullable,
  so 089 materializes a **nullable** logical column with no 089 change.

## Decisions (11, promotion-locked)

**D1 Reuse `Expr::SqlLogicalCast { try_cast: true }`** — no planner/IR shape change; SQL
`TRY_CAST`/`SAFE_CAST(x AS <one-part, unquoted ASCII, no-mod, non-`naked`>)` already lowers here
(`sql_planner.rs:3246`); `SAFE_CAST` is the alias. **D2 Collection becomes try_cast-agnostic** —
delete the `if *try_cast` reject in `collect_logical_cast_names` (`binder.rs:809-813`); names are
collected and durably resolved by the *same* async pass (`resolve_logical_casts`, `binder.rs:330`),
durable-only (seed excluded), refresh-on-miss, unknown → `LogicalTypeNotFound`. **D3 Resolved spec
→ sync binder** — the binder uses the resolved `id` + `physical` from the `HashMap<String,
LogicalTypeSpec>`, **not** the seed-only `logical_type_spec` (the 092 seed-only-spec trap holds).
**D4 Reconcile before coerce** — `reconcile_identity(existing, spec.id, &source)` (`binder.rs:3140`,
088, unchanged) runs **first**, for both strict and try paths; **identity conflict rejects at bind**
under TRY_CAST. **D5 Logical TRY_CAST is categorically nullable (contract γ)** — delete the gate
(`binder.rs:3119-3123`); when `try_cast`, bind iff `source == spec.physical ∨
is_supported_explicit_numeric_cast(source, spec.physical)`, then append `BoundExprNode::TryCast`
**on every bound path** (same-physical included) and stamp the reconciled logical id on that node.
**D6 No new kernel / no shared-gate change** — same-physical served by the `exec.rs:4118`
passthrough; numeric conversions reuse the existing `evaluate_try_cast` arms;
`is_supported_explicit_numeric_cast` is read, never widened. **D7 TRY_CAST binds wherever strict
logical CAST binds** — same-physical (incl. `Utf8`/`Boolean`) binds; cross-physical non-numeric
(e.g. `Utf8→CustomerId(Int64)`) rejects at bind, identical to 092. **D8 Strict logical CAST
unchanged** — the 092 `else` branch (same-physical reuses `inner`, non-nullable) is byte-identical.
**D9 Explicit primitive erase is the only cross-identity override** (087/092 D9). **D10 Stay out of
physical `bind_try_cast`** — identity stamping lives only in `bind_sql_logical_cast`; `bind_try_cast`
stays identity-free (087). **D11 Typed CTAS via 089 composition** — `CREATE TABLE t AS SELECT
TRY_CAST(raw AS CustomerId) AS cid` materializes a **nullable** CustomerId column through 089's
writer and survives the 090/088 decode round-trip; no new CTAS grammar; explicit typed CTAS column
lists still reject. **No** `BoundExprProgram`/exec presence for `SqlLogicalCast` (the binder consumes
it; `exec.rs` keeps its exhaustiveness arm only); no PBC opcode; no Wendelon/Rune change.

## Behavior gate

`try_cast_widening_logical_type_binds_nullable` (Int32→`CustomerId`(Int64): root Int64, logical
CustomerId, **nullable**, never NULL); **`try_cast_narrowing_failure_yields_null_keeps_identity`**
(out-of-range Int64→`SmallCustomerId`(Int32) → NULL row, logical id stamped — *the case that
differs from CAST*); `try_cast_same_physical_numeric_binds_nullable` (Int64→`CustomerId`(Int64):
appends TryCast, nullable schema); **`try_cast_same_physical_utf8_binds_nullable_without_new_kernel`**
(`Utf8`→`Sku`(TEXT): passthrough via `exec.rs:4118`); `try_cast_same_physical_boolean_binds_nullable`
(`Boolean`→`Flag`(BOOLEAN)); **`try_cast_conflicting_identity_rejects_at_bind`** (`ProductId`→
`CustomerId` → `NominalTypeMismatch`, *not* NULL — the doctrine);
`try_cast_explicit_primitive_erase_then_relabel_binds`; `try_cast_undefined_fails_closed`
(`LogicalTypeNotFound`, durable-only); `try_cast_unsupported_cross_physical_rejects`
(`Utf8`→`CustomerId`(Int64), == CAST); `safe_cast_logical_type_aliases_try_cast`;
**`ctas_try_cast_materializes_nullable_logical_identity_and_survives_resync`** (089 ∘ 090, nullable
preserved); 087–092 + all-naked + strict-CAST 092 gates green.

## Architecture guards

- **Invert (discipline #4):** remove the `assert!(collect.contains("TRY_CAST/SAFE_CAST to logical
  type is not supported"))` from `adr092_binder_resolves_sql_logical_casts_registry_only_before_sync_bind`
  (`nominal_type_identity.rs:773`) — both rejection strings (`binder.rs:811`, `:3121`) are deleted;
  the behavior fixture `logical_cast_try_cast_to_logical_target_rejects` (`logical_type_ddl_test.rs:518`)
  flips to the new behavior.
- **Add:** `adr093_binder_collects_both_cast_and_try_cast_logical_names` (the positive inverse of the
  removed assertion); `adr093_logical_try_cast_reconciles_identity_before_try_cast`;
  `adr093_logical_try_cast_categorically_nullable_no_same_physical_elide` (logical mirror of the
  physical no-elide guard — same-physical logical TRY_CAST appends a node, never reuses `inner`);
  `adr093_logical_try_cast_uses_existing_try_cast_node_no_executor_or_pbc`;
  `adr093_physical_bind_try_cast_remains_identity_free` (re-pins 087/091);
  `adr093_ctas_try_cast_nullability_flows_through_089_writer`.
- **Keep green (verified physical-`bind_try_cast`-scoped, untouched by 093):**
  `sql_planner_surface.rs:867-878` (physical no-elide + numeric gate) and
  `nominal_type_identity.rs:187,192-193` (physical TryCast identity-free); 092's resolved-spec-not-
  seed-only + transient-IR guards; PBC Cast/TryCast-ineligibility guards.

## Scale boundary

Expression-position SQL semantics only — single-row, in-binder. No new distributed surface, wire
format, executor branch, PBC opcode, IR variant, or coordinator→worker payload change. Reuses the
ADR 092 expression-position scale exactly.

## What to do next

*(Per the roadmap convention: this section is the next-topic source.)*

- **Refinement contracts** (the next integrity *tier*; strongest lead after 093 implements the
  complete authoring surface) — value predicates on a durable `LogicalTypeId` → static plan
  elision. Grounded as a **mega-scope that must split** (no `EmptyRelation` node, no
  constant-folding pass anywhere, no
  predicate field on the 090 definition + cross-ring ripple); the only non-inert first slice is
  **elision-first** (`EmptyRelation` + a contradiction simplifier, Toron-only), with durable-
  predicate persistence a separate cross-ring follow-on.
- **`DROP`/`ALTER LOGICAL TYPE`** (net-new IR variants + parsing + cross-ring DROP w/ referential
  integrity); read-only `information_schema.logical_types` (net-new virtual-table surface);
  **PK/FK-derived identity** (blocked on FK support — rejected at `sql_planner.rs:663`); aggregate-
  output identity semantics; the optional Wendelon-validate-at-authority (090 knob).
- Carried Rune leads (Like/ILike, joins); non-Rune 078 residual-minimization follow-on.

## Council history

iter-0: 3-of-3 topic selection on TRY_CAST (Gemini first; Claude grounded refinement-contracts as a
must-split mega-scope; Codex flagged the two rejection sites + the same-physical question). iter-1:
Claude grounded the same-physical contract to **γ** (categorically nullable, binder-local, no
shared-gate/kernel change), correcting Codex's feared cost; Codex independently reached γ and closed
the CTAS-nullability TODO; Gemini ratified the state transition. Promotion dual: both iter-1 TODOs
closed — CTAS nullability flows through the 089 writer (`binder.rs:3735`/`3764`/`941`), and the two
keep-green guards are verified physical-`bind_try_cast`-scoped. Migration:
`plans/impl/0093_sql_logical_try_cast.md`.

## Promotion criteria

`TRY_CAST(x AS T)` resolves durable-only (seed excluded, spec threaded → physically
validated), reconciles conflicts **before** any physical conversion (identity conflict rejects at
bind), is categorically nullable, binds wherever strict logical CAST binds (same-physical incl.
Utf8/Boolean via the executor passthrough, numeric lattice; cross-physical non-numeric rejects),
yields NULL on out-of-range physical narrowing, materializes a nullable logical column through CTAS
via 089 + survives resync; `SAFE_CAST` aliases; strict CAST / `LogicalAscription` / physical
`bind_try_cast` / Rune unchanged; no Wendelon/executor/PBC/IR change; the 092 deferral guards
inverted in-change (suite green, not red); 087–092 + all-naked green. Migration:
`plans/impl/0093_sql_logical_try_cast.md`.
