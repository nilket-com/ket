# Impl Plan — ADR 096: Durable Refinement Persistence + Write Enforcement

**Migration for** `plans/adr/096_durable_refinement_persistence_enforcement.md`. **Cross-ring.**
Persist a portable refinement on a logical type, enforce it on **all** write paths, and **de-version
the touched Wendelon logical-type contract** (no numeric suffixes, no compat — pre-release directive). **No**
elision (097), **no** CAST change (092/093).

## Pre-flight anchors (grounded)

| Concern | Anchor |
|---|---|
| Wendelon contract (to rename + extend) | current logical-type definition `compute.rs:341`; create struct `:328`; physical carrier enum `:318`; catalog `:360` |
| Wendelon codecs (postcard) | current logical-type encode/decode helpers `compute.rs:501-576` |
| Wendelon store/apply | `SmState.logical_types` `raft_storage.rs:1535`; `…Catalog::create` `compute.rs:400`; apply `raft_storage.rs:4087` |
| Create request/envelope | `LogicalTypeDefinitionCreateRequest` `wendelon-client/lib.rs:308`; `envelope()` `:330` |
| Toron sync→registry→spec | `register_logical_types_for_tenant` `catalog.rs:1110`; `register_definition` `ir.rs:101`; `LogicalTypeSpec` `ir.rs:33`; phys↔DataType `ir.rs:233/244` |
| Toron RPC | `storage.rs:487` |
| Authoring parser (rigid 6-token) | `try_lower_create_logical_type` `sql_planner.rs:167`; `LogicalPlan::CreateLogicalType` `:220` |
| Enforcement hooks | `coerce_insert_literal` (binder); shared `materialize_write_select_batch` `coordinator/write.rs:60` (backs `materialize_insert_select_batch` `:37` / `_update_select_batch` `:50`); `materialize_insert_batch` `:26` |
| Evaluator reuse | `Executor::evaluate_bound_program_without_storage` `exec.rs:1017` |
| `Eq`/`Hash` constraint | `LogicalTypeDefinition*` derive `Eq, Hash` → float literals as canonical bits |

## Phase 1 — Wendelon: de-version the contract + add the portable refinement

**De-version (D4 — no numeric suffixes, no compat, no fallback decoder; pre-release).** Rename the
touched logical-type surface to unversioned names: `LogicalTypePhysical`, `LogicalTypeDefinitionCreate`,
`LogicalTypeDefinition`, `LogicalTypeCatalog`, and matching `encode/decode_*` helpers. (Scope = the
logical-type contract 096 touches; the broader Wendelon suffix sweep is a separate chore — see
roadmap.) Add the Wendelon-owned AST (NOT `BoundExprProgram`, NOT Toron
`ScalarValue`, NOT SQL text):

```rust
pub enum LogicalTypeRefinementPredicate { Comparison { left: LogicalTypeRefinementValue,
    op: LogicalTypeRefinementComparisonOp, right: LogicalTypeRefinementValue }, And(Vec<Self>) }
pub enum LogicalTypeRefinementValue { Value, Int64(i64), Int32(i32),
    Float64(u64 /* f64::to_bits, finite */), Float32(u32), Utf8(String), Boolean(bool) }
pub enum LogicalTypeRefinementComparisonOp { Eq, Ne, Lt, Le, Gt, Ge }
```

Float literals stored as canonical bits so `LogicalTypeDefinition`'s `#[derive(Eq, Hash)]` still
derives (Codex's catch). Add `refinement: Option<LogicalTypeRefinementPredicate>` to
`LogicalTypeDefinition` + `…Create` **in place**. Thread through `LogicalTypeCatalog::create`
(`compute.rs:400`) + the raft apply (`raft_storage.rs:4087`). Validate predicate-literal physical
matches the carrier. Wendelon `--lib` tests prove the AST round-trips.

## Phase 2 — wendelon-client + Toron RPC

`LogicalTypeDefinitionCreateRequest` (`wendelon-client/lib.rs:308`) gains `refinement:
Option<LogicalTypeRefinementPredicate>`; `envelope()` (`:330`) encodes it.
`StorageAdminImpl::create_logical_type` (`storage.rs:487`) passes it through.

## Phase 3 — Toron authoring: `CHECK` parse → portable AST

Rework `try_lower_create_logical_type` (`sql_planner.rs:167`): the rigid `tokens.len() != 6` gate
becomes a parse of `CREATE LOGICAL TYPE <Name> AS <primitive> [CHECK ( <refinement> )]`. A small
refinement parser + mini-binder: resolve `VALUE` (case-insensitive) → `LogicalTypeRefinementValue::Value`,
type literals against the carrier (`data_type_to_logical_type_physical`, `ir.rs:244`; floats → bits),
map ops + `AND`, **reject** any other identifier / `OR` / `NOT` / function / cast / subquery.
`LogicalPlan::CreateLogicalType` (`:220`) gains the refinement. Keep table-column `CHECK` rejected
(`:668`) untouched. **Idempotency:** re-`CREATE` with identical physical+refinement is a no-op; same
name with differing refinement rejects.

## Phase 4 — Toron sync + registry

`LogicalTypeSpec` (`ir.rs:33`) gains `refinement: Option<LogicalTypeRefinementPredicate>` (carried as
data; lowered to a check at enforcement). `register_definition` (`ir.rs:101`) copies it from the synced
`LogicalTypeDefinition`; `register_logical_types_for_tenant` (`catalog.rs:1110`) + the sync path
(`catalog.rs:1597`) carry it.

## Phase 5 — Enforcement: one shared validator, all four write paths

A shared helper lowers `LogicalTypeRefinementPredicate` → a single-`VALUE` bound check program and
validates a materialized scalar/array (**non-null result ≠ `TRUE` → reject**; NULL skipped, D3),
resolving the column's refinement from the registry by `LogicalTypeId`. Reuse
`evaluate_bound_program_without_storage`. Hook **all four**: **INSERT VALUES** after
`coerce_insert_literal` (one-row batch, precise bind-time error); **INSERT SELECT / UPDATE / CTAS** in
the shared `materialize_write_select_batch` (`coordinator/write.rs:60`), per-row over the materialized
carrier array, before write (CTAS uses `infer_ctas_schema`'s preserved logical metadata). Error: reuse
`BindingError`/`ExecutionError::InvalidExpression` with an intentional CHECK-violation message (or add
a `ConstraintViolation` variant — not required). **Not literal-only.**

## Phase 6 — Behavior fixtures

`create_logical_type_with_check_round_trips`; `recreate_same_refinement_idempotent_different_rejects`;
`refinement_authoring_rejects_non_value_identifier` + `..._rejects_or_not_function`;
`table_column_check_still_rejected`; `insert_literal_violating_rejects` + `..._satisfying_admits`;
`insert_null_into_nullable_refined_column_admits`; **`insert_select_violating_rejects` +
`update_to_violating_rejects` + `ctas_materializing_violating_rejects`** (completeness);
`literal_and_batch_validation_use_same_lowered_check`;
`cast_to_refined_type_unchanged_but_insert_select_backstops` (D8);
`float_refinement_literal_round_trips_via_canonical_bits` (D1); `refined_definition_survives_resync`;
`filter_does_not_yet_elide_from_refinement` (097 boundary).

## Phase 7 — Architecture guards

`adr096_refinement_predicate_portable_not_bound_expr_program_or_scalar_value`;
`adr096_refinement_stored_in_contract_not_parsed_from_sql_at_sync`;
`adr096_refinement_authoring_only_value_identifier`; `adr096_table_column_check_still_rejected`;
`adr096_enforcement_covers_insert_values_select_update_ctas`; `adr096_cast_remains_pure_physical_ascription`;
`adr096_no_refinement_elision_yet`; **`adr096_logical_type_contract_unversioned_no_compat_scaffolding`**
(no numeric-suffix names / fallback decoders remain in the touched logical-type surface — retargets
the old naming guards).

## Phase 8 — Verification

```
cargo test -p wendelon --lib            # de-version rename compiles; AST + encode/decode round-trip; catalog create
cargo test -p toron --test logical_type_ddl_test --test logical_type_definition_test
cargo test -p toron --test insert_test --test update_test --test ctas_test   # enforcement completeness
cargo test -p toron --test architecture_test          # adr096_* + retargeted contract guard
cargo test -p toron && cargo test -p wendelon          # full suites, green not red
```

**Acceptance:** the de-version rename compiles cluster-wide; round-trip persists the refinement; all
four write paths reject violating non-null + admit conforming + NULL; `CAST`/`TRY_CAST` unchanged with
the write backstop; the logical-type contract carries no numeric-suffix names or compat scaffolding; re-create
idempotent on matching refinement; no elision yet; both suites green.

## Risks / notes

- **No numeric suffixes, no compat** — pre-release, no historical installations. The rename is the
  contract; retarget (don't leave red) the old naming guards.
- **Wendelon must not import Toron** (`BoundExprProgram`/`ScalarValue`); float literals as bits to keep
  `Eq`/`Hash`.
- **Completeness is the cost** — the runtime per-row validator over computed writes is the bulk;
  literal-only is insufficient.
- **CAST stays 092/093** — storage boundary is the only enforcement point.
- **Rename scope = the logical-type contract 096 touches**; the broader Wendelon suffix sweep is a
  separate behavior-neutral chore (roadmap), not bundled here.
