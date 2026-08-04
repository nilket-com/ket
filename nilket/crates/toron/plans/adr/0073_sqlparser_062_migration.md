# ADR 073: sqlparser 0.54 → 0.62 (Parser-Foundation Refresh)

**Status:** Promoted (plan) — **3-of-3 (Codex, Gemini, Claude)**, with no
substantive fork.
**Seed:** Toron pins `sqlparser = "0.54"`; the repo has guards that *reject* 0.62.
073 retires that pin deliberately — a **single-file, behavior-neutral** parser
upgrade at the sqlparser→IR anti-corruption boundary, de-risking later SQL
widening.
**Category:** Toron compute-ring parser dependency migration. **No Wendelon /
storage / SQL-wire / worker-wire / IR-shape / event-schema change.**

## Grounded probe (Claude)

Bumped to `sqlparser = "0.62"`, `cargo update -p sqlparser --precise 0.62.0`
(resolves + fetches), `cargo check -p toron`: **62 errors, 100% in
`crates/toron/src/sql_planner.rs`** — the anti-corruption boundary held (sqlparser
is quarantined to the planner; everything downstream is on Toron's own IR). So
this is a **single-file migration** over **11 mechanical breakage categories** +
a downstream mismatched-types ripple; **4 are behavior-bearing** (D5/D6).

The 11 categories: `ObjectName`→`Vec<ObjectNamePart>` (×11); `Value` reshape
(insert/null literal lowering); `Query.{limit,offset,limit_by}`→`limit_clause`;
`OrderBy.exprs`→`kind: OrderByKind`; `Expr::Array` new field; `SelectItem::
ExprWithAliases` new variant; `CreateTable` field churn (20+ dialect fields);
struct↔tuple flips (`Statement::Update`, `TableConstraint::PrimaryKey`,
`ColumnOption::Unique`); new statement token/hint fields; `connect_by`
`Option`→`Vec<ConnectByKind>`; `ColumnDef.collation` removed.

## Decisions (8, Promotion-Locked)

**D1 Behavior-neutral refresh.** The accepted SQL corpus lowers to the **same
Toron IR**; unsupported SQL still fails with a planning error. New 0.62-only
syntax is **rejected** unless it is pure parser metadata with no semantic effect.
**No** expression/DDL widening, `UNIQUE`, `ALTER`, `CREATE INDEX`, CTAS
`RETURNING`, or planner feature work is bundled — those are separate follow-ons.

**D2 Single-version, no compatibility layer.** Toron moves to 0.62 as the only
parser; no dual 0.54/0.62 abstraction or feature flag. Private helpers that
localize 0.62 AST vocabulary are fine but stay in `sql_planner.rs` and return
Toron-owned types / primitives.

**D3 Anti-corruption containment (verified).** `sqlparser` imports stay
quarantined to `sql_planner.rs`; the bump touches only `crates/toron/Cargo.toml`,
`Cargo.lock`, `sql_planner.rs`, focused Toron tests/guards, and ADR/impl/evidence
docs. `sqlparser` AST types must **not** leak into Toron IR, worker protocol,
Wendelon request metadata, storage descriptors, audit payloads, or SQL-listener
response shapes.

**D4 Exhaustive external-AST accounting beats rest patterns.** The planner
deliberately destructures external AST structs exhaustively so parser bumps fail
**loudly** (existing `adr028/029/030/034/036_..._destructure_is_exhaustive`
guards). 073 keeps this: **every** field on a changed/lowered AST node is
bucketed — **Lowered** / **Rejected** (explicit planning error) /
**Parser-metadata** (named + grouped in a local `_parser_metadata_only` tuple).
No `..` rest patterns on changed/lowered external AST structs. Wildcard enum arms
remain allowed only when they immediately reject or mark unsupported syntax; they
must not silently accept, ignore, or lower new 0.62 semantics.

**D5 Query limit/order + INSERT-source contract is a release blocker (Codex,
precise).** 0.62 folds `limit`/`offset`/`limit_by` into `limit_clause` and
`OrderBy.exprs` into `kind`. The migration must preserve the **exact** contract:
- `SELECT … ORDER BY …`, `… ORDER BY … LIMIT n`, `… ORDER BY … OFFSET n`,
  `… ORDER BY … LIMIT n OFFSET m` work iff they work today;
- unordered `LIMIT`/`OFFSET`, `LIMIT BY`, `FETCH`, locks, query settings,
  `FORMAT`, and `ORDER BY ALL` (if unsupported) are **rejected**;
- INSERT source stays **VALUES-only** (`lower_insert_source` rejects every query
  decoration).
Highest-risk drift: the compiler can be satisfied while the planner accidentally
accepts/rejects/ignores a clause.

**D6 Name + literal extraction get helpers + focused tests.** `ObjectNamePart`
(one/two-part table names, unqualified columns; quoting + schema-qualified) and
`Value` reshape (string/bool/null/signed-numeric INSERT literals, scalar literal
lowering) are easy to make compile and hard to notice semantically — pin them.

**D7 New AST variants reject explicitly.** New 0.62 select-item / group-order /
expression-wrapper / function-arg / table-hint / output-returning / statement-
decoration variants must route to `unsupported(...)`, never fall through a
catch-all that looks like success. A guard verifies known semantic 0.62 variants
are **named** in source or covered by an immediate unsupported arm, not hidden
behind a wildcard that succeeds.

**D8 Meta-guard reconciliation (exact).** Retarget
`adr063_sql_planner_destructures_sqlparser_054_update_fields` to the 0.62 UPDATE
fields; **invert/remove** `adr063_sqlparser_062_upgrade_is_not_part_of_update_
surface` and `adr064_sqlparser_stays_054` (both currently assert `!"0.62"` — 073
makes them false by design); keep `adr028/029/030/034/036` exhaustive-destructure
guards green against the 0.62 fields.

## Architecture Guards (9 + meta)

`adr073_sqlparser_is_pinned_to_062`,
`adr073_sqlparser_ast_is_quarantined_to_sql_planner` (containment — load-bearing),
`adr073_lowered_external_ast_structs_do_not_use_rest_patterns`,
`adr073_semantic_wildcard_arms_only_reject`,
`adr073_query_limit_clause_preserves_ordered_limit_offset_else_rejects`,
`adr073_insert_values_source_rejects_query_decorations`,
`adr073_new_062_select_item_and_expr_variants_are_explicitly_rejected`,
`adr073_no_new_sql_capability_surface` (behavior-neutral),
`adr073_no_wendelon_wire_storage_or_ir_leakage`.
**Meta:** `adr073_retargets_adr063_064_sqlparser_054_pins`,
`adr073_prior_028_029_030_034_036_exhaustive_destructure_guards_pass_on_062`.

## Integration Fixtures

The net is the **existing corpus staying green** (ADRs 011/023/024/025/026/029/
030/034/036 + the SQL paths under 062–072), plus reject-surface parity:
`ordered_limit_offset_still_lowers`; `unordered_limit_is_rejected`;
`offset_without_order_by_is_rejected`; `limit_by_fetch_locks_settings_format_
rejected`; `order_by_all_is_rejected`; `insert_source_is_values_only`;
`expr_with_aliases_select_item_is_rejected`; `object_name_qualified_resolution_
unchanged`; `insert_and_null_literal_lowering_unchanged`. Plus an
**accepted/rejected SQL corpus table** (before/after) in the evidence.

## Out of Scope / Carried

Expression-surface widening (`CASE`/`IN`/`BETWEEN`/`LIKE`), `CHECK`/`DEFAULT`,
`UNIQUE`/`ALTER`/composite-PK (cross-ring schema-V2), CTAS `RETURNING` — all
explicitly *not* bundled (073 is a behavior-neutral foundation refresh; these are
follow-ons it de-risks). Typed table-mutation trace continuation (Wendelon ≈075).

## Council History

iter-0: **3-of-3 from the first round** — all three chose a behavior-neutral,
anti-corruption-contained, exhaustive-accounting bump. Claude grounded it with a
real compile probe (62 errors, all in `sql_planner.rs`, 11 categories). Codex
sharpened two points: **rest-pattern-free external struct accounting** and the
**precise limit/order/INSERT-source contract** (D5). Gemini anchored the ADR-010
exhaustive-destructure invariant. No substantive fork; promoted at iter-0.

## Plan promotion bar

D-set 8; 9 + meta guards; existing-corpus-green net + reject-surface parity;
single-boundary-file (`sql_planner.rs` + manifest/lock/tests) behavior-neutral
migration; containment verified by probe; the 063/064 0.54-pins reconciled.
Migration: `plans/impl/0073_sqlparser_062_migration.md`.

## Conclusion

073 is a contained, behavior-neutral parser-foundation refresh: bump to 0.62, fix
`sql_planner.rs`'s 11 breakage categories with **exhaustive external-AST
accounting**, preserve the **exact** SQL accept/reject contract (especially
limit/order + INSERT-VALUES-only), reject new 0.62 syntax explicitly, keep
`sqlparser` quarantined to the planner, and reconcile the 063/064 0.54 pins —
implementation promotion bar is the existing SQL corpus staying green, not
merely "it compiles."
