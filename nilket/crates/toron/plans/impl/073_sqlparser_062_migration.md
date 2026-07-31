# Impl Plan 073: sqlparser 0.54 → 0.62 (Parser-Foundation Refresh)

**Status:** Promoted, paired with `plans/adr/073_sqlparser_062_migration.md`.
**Posture:** Single-file, behavior-neutral parser bump at the sqlparser→IR
anti-corruption boundary. Same SQL → same Toron IR; new 0.62 syntax rejected;
sqlparser stays quarantined to `sql_planner.rs`; exhaustive external-AST field
accounting. The promotion bar is the **existing SQL corpus staying green**, not
"it compiles."

## Grounded probe (re-run at impl start to confirm)

`sqlparser = "0.62"` + `cargo update -p sqlparser --precise 0.62.0` + `cargo
check -p toron` ⇒ **62 errors, all in `sql_planner.rs`** (containment held). The
11 categories + 4 behavior-bearing risks are the work plan below.

## Invariants carried in

- Behavior-neutral (same SQL → same IR; unsupported still rejected). **No `..`
  rest patterns on changed/lowered external AST structs** (every field bucketed:
  lowered / rejected / parser-metadata). Wildcard enum arms are allowed only when
  they immediately reject or mark unsupported syntax. sqlparser AST **does not
  leak** past `sql_planner.rs`. Single version (no compat layer). No Wendelon/
  SQL-wire/worker-wire/IR-shape/storage/audit change.

## Phase 0 — Anchors

- **The one boundary file:** `crates/toron/src/sql_planner.rs` (sqlparser imports
  at `:13-22`; the lowering match sites).
- **Manifest:** `crates/toron/Cargo.toml:29` (`sqlparser = "0.54"`→`"0.62"`) +
  `Cargo.lock`.
- **Prior 0.54 pins to reconcile:** `adr063_sql_planner_destructures_sqlparser_
  054_update_fields` (`architecture_test.rs:14054`), `adr063_sqlparser_062_
  upgrade_is_not_part_of_update_surface` (`:14080`, asserts `!"0.62"`),
  `adr064_sqlparser_stays_054` (`:14592`, asserts `!"0.62"`).
- **Exhaustive-destructure guards to keep green on 0.62:**
  `adr028/029/030/034/036_..._destructure_is_exhaustive`.
- **The INSERT-source VALUES-only reject:** `lower_insert_source` (`:294-311`).

## Phase 1 — Bump + top-down compile fix (`sql_planner.rs`)

Bump manifest + lock, then fix the 11 categories top-down (exhaustive, no rest
patterns on changed external AST structs):
1. **Object-name + identifier helpers** — `ObjectName`→`Vec<ObjectNamePart>`
   (one/two-part names, unqualified columns; preserve quoting + qualification).
2. **Literal/value helpers** — `Value` reshape (string/bool/null/signed-numeric
   INSERT literals, scalar literal lowering).
3. **`Query` + INSERT-source clause accounting** — `limit`/`offset`/`limit_by`
   → `limit_clause`; preserve the D5 contract (ordered LIMIT/OFFSET lower;
   unordered + LIMIT BY/FETCH/locks/settings/FORMAT reject; INSERT VALUES-only).
4. **`OrderBy`** — `exprs`→`kind: OrderByKind` (reject `ORDER BY ALL` if unsupp.).
5. **`Select`** — projection/grouping + new fields (`optimizer_hints`,
   `select_modifiers`, `exclude`, `flavor`, `index_hints`) bucketed; new
   `SelectItem::ExprWithAliases` → `unsupported`.
6. **`Expr`** — new `array` field; new variants named or routed to
   `unsupported` (no catch-all that accepts).
7. **DDL/DML destructures** — `CreateTable` field churn (20+ dialect fields, each
   named + rejected/parser-metadata), `Statement::Update`/`TableConstraint::
   PrimaryKey`/`ColumnOption::Unique` struct↔tuple flips, Insert/Delete token
   fields, `connect_by` `Option`→`Vec` (`.is_empty()`), `ColumnDef.collation`
   removed.

## Phase 2 — Guard reconciliation

- Retarget `adr063_..._054_update_fields` → the 0.62 UPDATE fields.
- **Invert/remove** `adr063_sqlparser_062_upgrade_is_not_part_of_update_surface`
  + `adr064_sqlparser_stays_054` (both assert `!"0.62"` — now false by design).
- Add the 073 guards: `..._pinned_to_062`, `..._ast_is_quarantined_to_sql_planner`,
  `..._lowered_external_ast_structs_do_not_use_rest_patterns`,
  `..._semantic_wildcard_arms_only_reject`, `..._query_limit_clause_preserves_
  ordered_limit_offset_else_rejects`, `..._insert_values_source_rejects_query_
  decorations`, `..._new_062_select_item_and_expr_variants_are_explicitly_
  rejected`, `..._no_new_sql_capability_surface`, `..._no_wendelon_wire_storage_
  or_ir_leakage`.
- Confirm `adr028/029/030/034/036` exhaustive guards still pass (0.62 fields).

## Phase 3 — Behavior-equivalence regression

- **The whole existing SQL corpus stays green** (the real net): `sql_listener_
  test`, `update_test`, create-table/insert/delete/aggregate/cast fixtures, `--lib`.
- Add reject-surface-parity fixtures (ADR list): ordered-limit-offset-lowers;
  unordered-limit/offset rejected; limit-by/fetch/locks/settings/format rejected;
  order-by-all rejected; insert-source-values-only; expr-with-aliases rejected;
  qualified-name resolution unchanged; insert/null-literal lowering unchanged.
- **Accepted/rejected SQL corpus table** (before/after) in the evidence.

## Phase 4 — Evidence

`plans/impl/073_sqlparser_062_migration_evidence.md`: the probe (version +
command + error categories); `cargo check -p toron --all-targets`; `cargo clippy
-p toron --all-targets -- -D warnings`; `architecture_test` (incl. retargeted
063/064 + the 028/029/030/034/036 exhaustive guards green + the new adr073 guards);
`sql_listener_test`; `update_test`; `--lib`; the accept/reject corpus table;
`git diff --check`.

## Watch-items

1. **Limit/order/INSERT contract (D5)** — the highest-risk silent drift; the
   reject-surface-parity fixtures are the net (compiler-satisfied ≠ behavior-equal).
2. **No rest-pattern drift** — exhaustive external-struct accounting; the
   no-rest-patterns guard pins it.
3. **New variants reject** — no catch-all `_ =>` that accepts new 0.62 syntax.
4. **Containment** — sqlparser stays in `sql_planner.rs` (no leak into IR/wire).
5. **Name/literal extraction** — `ObjectNamePart`/`Value` reshape must stay
   byte-identical (helper tests).
6. **Guard reconciliation** — 063/064 0.54-pins inverted/retargeted;
   028/029/030/034/036 exhaustive guards green on 0.62.

## Acceptance checklist

- [ ] Phase 1: manifest+lock bumped; 11 categories fixed exhaustively (no rest
      patterns on changed external AST structs); D5 contract preserved; new
      variants → `unsupported`.
- [ ] Phase 2: 063/064 0.54-pins reconciled; 9 adr073 guards added;
      028/029/030/034/036 green on 0.62.
- [ ] Phase 3: full existing SQL corpus green; reject-surface-parity fixtures.
- [ ] Phase 4: evidence incl. probe + accept/reject table; clippy; diff-check.
