# ADR 096: Durable Refinement Predicate Persistence + Write Enforcement

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — verified against the iter-3
duals. Convergence trail: Gemini conceded the soundness reordering, then corrected its impl to a
neutral AST + complete enforcement; Codex ratified the storage-only/CAST-unchanged/Toron-receipt
resolutions and supplied the portable AST. **One reconciliation:** iter-3 considered a compatibility
envelope (Claude); **superseded** by the operator's pre-release directive — change the contract in
place, drop the numeric suffixes, no compat scaffolding (Codex), consistent with the
no-version-suffix convention. **Cross-ring** (bounded 090 re-open), **Toron-sequence** placement.
**Predecessor:** ADR 095. The split is soundness-ordered: **096 persists + enforces (the receipt);
ADR 097 spends it on elision.**

## Doctrine — the receipt before the magic

`PersonId AS BIGINT CHECK (VALUE > 0)` is an integrity contract. It is a valid basis for 097's
optimization (`WHERE id = -5` → `EmptyRelation`) **only if stored data is guaranteed to satisfy it** —
so 096 persists the predicate and enforces it on every write; 097 relies on that receipt. Eliding on
an unenforced declaration is "magic without a receipt" (silent wrong results). **096 does no elision.**

## Grounding (the cross-ring surface)

- **Wendelon contract** (`compute.rs`): the current logical-type definition struct
  `{ logical_type_id, name, physical }` (`:341`), create struct `{ name, physical }` (`:328`),
  physical-carrier enum (`:318`), postcard encode/decode helpers (`:501-547`), and catalog (`:360`)
  stored inside Raft `SmState` (`raft_storage.rs:1535`), applied at `:4087`. Create flows via
  `LogicalTypeDefinitionCreateRequest` (`wendelon-client/lib.rs:308`) → `envelope()` (`:330`).
  **These structs `#[derive(Eq, Hash)]`** — so a refinement literal cannot be a bare `f64` (see D1).
- **Toron sync/registry:** `register_logical_types_for_tenant` (`catalog.rs:1110`) →
  `register_definition` (`ir.rs:101`) → `LogicalTypeSpec { id, name, physical }` (`ir.rs:33`); phys↔
  DataType (`ir.rs:233/244`); RPC `storage.rs:487`.
- **Authoring:** `try_lower_create_logical_type` (`sql_planner.rs:167`) is a *rigid 6-token*
  whitespace parser (`tokens.len() != 6` rejects); `CHECK (...)` requires reworking it.
- **Enforcement:** `coerce_insert_literal` (INSERT VALUES, bind-time); the shared
  `materialize_write_select_batch` (`coordinator/write.rs:60`) backing
  `materialize_insert_select_batch`/`_update_select_batch` (`:37`/`:50`) + CTAS (runtime).
- **Evaluator reuse:** `Executor::evaluate_bound_program_without_storage` (`exec.rs:1017`, the 095
  closed evaluator). `infer_ctas_schema` preserves logical metadata.

## Decisions (promotion-locked)

**D1 — Portable refinement AST, Wendelon-owned, equality-safe.** NOT `BoundExprProgram`, NOT Toron
`ScalarValue`, NOT raw SQL:
```rust
enum LogicalTypeRefinementPredicate { Comparison { left, op, right }, And(Vec<Self>) }
enum LogicalTypeRefinementValue { Value, Int64(i64), Int32(i32), Float64(u64 /*canonical bits*/), Float32(u32), Utf8(String), Boolean(bool) }
enum LogicalTypeRefinementComparisonOp { Eq, Ne, Lt, Le, Gt, Ge }
```
**Float literals are stored as canonical bits** (`f64::to_bits`, finite-only) so the containing
`#[derive(Eq, Hash)]` structs still derive (Codex's catch). Toron lowers the AST into local check
machinery; Wendelon stores it opaquely (090 authority-vs-representation). **D2 — Bounded language.**
`VALUE` is the only identifier (case-insensitive), the non-null physical carrier; ordered comparisons
for numerics, `=`/`<>` for all primitives, `AND`; **reject `OR`/`NOT`/functions/casts/column-refs/
subqueries/non-determinism** at authoring; stored normalized. **D3 — Null semantics.** Refinement
applies after physical coercion to non-null values only; a nullable refined column still stores
`NULL`; no `NOT NULL` implication. **D4 — Contract change in place + de-version rename (pre-release
directive; no compat scaffolding).** 096 touches the Wendelon logical-type contract, so it **renames
the touched surface to unversioned names** — `LogicalTypePhysical`, `LogicalTypeDefinitionCreate`,
`LogicalTypeDefinition`, `LogicalTypeCatalog` + matching `encode/decode_*` helpers — and adds
`refinement: Option<LogicalTypeRefinementPredicate>` directly. **No compatibility envelope, no
fallback decoder, no historical migration** (pre-release; no production installations). The de-versioning is a
behavior-neutral refactor that travels with the ADR. **D5 — `CHECK` scoped to `CREATE LOGICAL TYPE`.**
Table-column `CHECK` stays rejected (`sql_planner.rs:668`) — guarded (the spelling is reused).

**D6 — Complete write enforcement (all four paths — not literal-only).** A *shared runtime validation
helper* over materialized scalars/arrays + the resolved-logical-type registry, lowering the AST to a
single-`VALUE` check program (reuse `evaluate_bound_program_without_storage`); a non-null result other
than `TRUE` → reject. Hooked at **INSERT VALUES** (`coerce_insert_literal`, one-row batch, precise
bind-time error), **INSERT SELECT** + **UPDATE** + **CTAS** (the shared `materialize_write_select_batch`,
per-row over the materialized carrier array, before write). Bind-time-literal-only is rejected — UPDATE/
INSERT SELECT could store violating rows, defeating the receipt. **D7 — Receipt scope = complete Toron
write path.** Sufficient because Toron is the sole compute/write ring and the refinement
is defined at `CREATE LOGICAL TYPE` *before* any column adopts it (no pre-existing data; `ALTER`
deferred). Wendelon-authority-side validation (the 090 knob — future non-Toron writers / `ALTER`-over-
existing-data) is **carried, not required**; 097's elision relies on the stated receipt: *stored
refined-column data passed Toron enforcement*. **D8 — `CAST`/`TRY_CAST` unchanged (092/093).** Enforce
at the storage boundary only: `SELECT CAST(-5 AS PersonId)` is an ephemeral logical value;
`INSERT … SELECT CAST(-5 AS PersonId)` into a refined column rejects at the write (the backstop). 097
relies on stored-column receipts only. **D9 — No elision in 096** — `static_filter_class`
(`binder.rs:1166`) stays blind to durable refinements; that's 097.

## Behavior gate

`create_logical_type_with_check_round_trips` (Wendelon create/get/list + Toron sync + registry);
**`recreate_same_refinement_idempotent_different_refinement_rejects`** (Codex — same name+physical+
refinement is idempotent; differing refinement rejects); `refinement_authoring_rejects_non_value_identifier`
+ `..._rejects_or_not_function`; `table_column_check_still_rejected`; `insert_literal_violating_rejects`
+ `..._satisfying_admits`; `insert_null_into_nullable_refined_column_admits`;
**`insert_select_violating_rejects` + `update_to_violating_rejects` + `ctas_materializing_violating_rejects`**
(completeness — load-bearing); `literal_and_batch_validation_use_same_lowered_check`;
`cast_to_refined_type_unchanged_but_insert_select_backstops` (D8); `refined_definition_survives_resync`;
`float_refinement_literal_round_trips_via_canonical_bits` (D1); `filter_does_not_yet_elide_from_refinement`
(097 boundary).

## Architecture guards

`adr096_refinement_predicate_portable_not_bound_expr_program_or_scalar_value` (Wendelon imports
neither; float literals stored as bits); `adr096_refinement_stored_in_contract_not_parsed_from_sql_at_sync`;
`adr096_refinement_authoring_only_value_identifier`; `adr096_table_column_check_still_rejected`;
`adr096_enforcement_covers_insert_values_select_update_ctas`; `adr096_cast_remains_pure_physical_ascription`
(092/093 untouched); `adr096_no_refinement_elision_yet`; **`adr096_logical_type_contract_unversioned_no_compat_scaffolding`**
(no numeric-suffix names + no fallback decoder remain in the touched surface — replaces/retargets the
old naming guards).

## Scale boundary & placement

Cross-ring (bounded 090 re-open: refinement field + portable AST + the de-version rename in Wendelon's
contract + sync) + a new Toron runtime write-validation seam; no distributed/PBC/elision/CAST change.
Toron-sequence (bounded interior substrate). The de-version rename is a bundled behavior-neutral
refactor (refactoring-travels-with-the-ADR).

## What to do next

- **ADR 097 — refinement-driven read-time elision** (the payoff): feed the enforced durable fact into
  095's `static_filter_class` with a single-column interval/literal-contradiction model
  (`VALUE > 0` vs `id = -5`), sound on 096's receipt.
- Carried: Wendelon authority-side validation; `DROP`/`ALTER LOGICAL TYPE`; `information_schema.logical_types`
  (render the normalized refinement); Rune `Like`/`ILike`; PK/FK-derived identity; broader CHECK/DEFAULT.

## Council history

095 built the elision target. 096 iter-0: Claude grounded the **soundness catch** (elide needs
enforce — Gemini's persist+elide was a wrong-results bug); Codex independently agreed. iter-1: 3-of-3
on persist+enforce / elide→097; Codex grounded the portable-AST + versioning catches. iter-2: Claude
resolved storage-only enforcement + CAST-unchanged + Toron-write-path receipt (Codex ratified). iter-3
(dual): Gemini corrected to neutral-AST + complete enforcement; Codex added the float-literal-bits +
idempotency catches and the **pre-release de-version directive** (superseding Claude's compatibility
envelope — no-version-suffix convention). Migration:
`plans/impl/096_durable_refinement_persistence_enforcement.md`.

## Promotion criteria

`CREATE LOGICAL TYPE … CHECK (VALUE > 0)` persists a portable, equality-safe refinement (round-trips
Wendelon+Toron); authoring rejects non-`VALUE`/`OR`/`NOT`/functions; table-column `CHECK` still
rejected; **all four write paths** reject non-null violating values + admit conforming + admit NULL
into nullable; `CAST`/`TRY_CAST` byte-identical (092/093) with the write backstop; the Wendelon
logical-type contract is unversioned with no compat scaffolding; re-create idempotent on matching
refinement; no filter elision yet. Migration:
`plans/impl/096_durable_refinement_persistence_enforcement.md`.
