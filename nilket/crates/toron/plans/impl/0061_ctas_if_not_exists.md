# Impl Plan 061: CTAS `IF NOT EXISTS` with Race-Safe Catalog Create Outcome

**Status:** Promoted, paired with `plans/adr/0061_ctas_if_not_exists.md`.
**Posture:** Toron SQL/coordinator only. Coordinator-owned, non-fragmentable;
**no new storage mutation primitive, no PK DDL, no Wendelon server/wire
change, no worker/SQL-wire change.** Five phases. The novel piece is the
catalog **created-vs-existing outcome** + the bind-always /
write-only-if-created CTAS flow, including the Wendelon-backed create-race
reconciliation (D12).

## Invariants carried in

- ADR 033/041/049–060 behavior unchanged except the two retargeted CTAS
  guards. `crates/nilket` runtime-free; fixture worker unchanged; no-`*V1`.
- CTAS result shape stays empty success; no `RETURNING` / row count.

## Phase 0 — Audit (verified anchors)

- `sql_planner.rs:911` `unsupported("CTAS does not support IF NOT EXISTS in
  v1")` — the reject to remove; `:905` OR-REPLACE+IF-NOT-EXISTS conflict —
  keep; explicit-column-list reject — keep. Lowering:
  `lower_create_table_as_select`.
- `catalog.rs:431–460` `create_table(name, schema, if_not_exists) ->
  Result<TableBinding, CatalogError>` — already exact-schema:
  compatible → `Ok(existing)`, incompatible → `SchemaMismatch`, absent →
  creates. Returns only `TableBinding` today.
- `coordinator.rs:1007–1044` `ExecutionPlan::CreateTableAsSelect { table,
  schema, or_replace, source }` — **no `if_not_exists`**; flow:
  `execute_read_plan(source)` → (or_replace drop) → `create_table(…, false)`
  → materialize → `writer.write(...)`. The execution seam.
- Wendelon-backed catalog create can race remotely (a create RPC rejection
  after a local miss may mean "exists now") — D12.
- `ctas_test.rs::ctas_if_not_exists_remains_rejected` — retarget.

## Phase 1 — Catalog created-vs-existing outcome (catalog.rs)

- `pub struct CreateTableOutcome { pub binding: TableBinding, pub created:
  bool }`; `create_table_with_outcome(name, schema, if_not_exists) ->
  Result<CreateTableOutcome, CatalogError>` (`created=false` on the
  compatible-existing IF-NOT-EXISTS path, `true` on fresh create). Keep
  `create_table(...) -> TableBinding` as a `.map(|o| o.binding)` wrapper so
  schema-only call sites are unchanged. Apply across all `Catalog` impls,
  including production `MockCatalog` / `WendelonCatalog` and test-only impls
  such as `RefreshingCatalog`.
- **D12 (Wendelon impl):** under `if_not_exists`, a schema-create rejection
  that may mean "exists" → resolve/sync the current descriptor: matches →
  `created=false`; differs → `SchemaMismatch`; unprovable → surface the
  original admin error.

## Phase 2 — IR threads `if_not_exists` (sql_planner.rs, exec.rs)

- Add `if_not_exists: bool` to `LogicalPlan::CreateTableAsSelect` and
  `ExecutionPlan::CreateTableAsSelect`; planner sets it; lowering carries it.

## Phase 3 — Planner accept + keep conflicts (sql_planner.rs)

- In `lower_create_table_as_select`: **remove** the `:911` IF-NOT-EXISTS
  reject; **keep** the OR-REPLACE conflict (`:905`) and the explicit-column-
  list reject. Planner carries the flag; binding/inference remain unchanged
  and **bind-always** (source errors surface regardless of `if_not_exists`).

## Phase 4 — Coordinator CTAS execution (coordinator.rs:1007)

- **`if_not_exists` true:** before `execute_read_plan(source)`, resolve the
  target. Compatible existing → empty success **without executing source
  rows** (D7); incompatible existing → `SchemaMismatch` (D8).
- **Absent target** (or `if_not_exists` false): execute source as today, then
  `create_table_with_outcome(table, schema, if_not_exists)`. **Write payload
  only if `outcome.created`** (D10). Compatible raced-in (`created==false`) →
  empty success, no write (D11); incompatible raced-in → `SchemaMismatch`.
  Wendelon-create race follows D12: descriptor sync proves compatible
  (`created=false`), proves incompatible (`SchemaMismatch`), or cannot prove
  current state (surface the original admin error).
- `or_replace` path unchanged; result shape empty success (D14); distributed
  compatible-skip path does **not** request source workers (fixture 10).

## Phase 5 — Guards + retargets + fixtures + evidence + roadmap

- 11 focused + 3 convention + meta guards (ADR list). **Retarget** `adr033…
  ctas_column_list_and_if_not_exists` (keep column-list reject, drop IF-NOT-
  EXISTS claim) and `adr041_ctas_if_not_exists_remains_rejected` (flip to
  positive; OR-REPLACE conflict preserved). Meta scoped `…_except_retarget`.
- **Retarget** `ctas_test.rs::ctas_if_not_exists_remains_rejected`; add the
  12 fixtures.
- `plans/impl/0061_ctas_if_not_exists_evidence.md`: `architecture_test
  adr061`, full `architecture_test`, `cargo test -p toron` (planner/ctas/
  coordinator/catalog suite counts), `clippy -p toron --all-targets -- -D
  warnings`, `git diff --check` (capped-linker procedure).
- Roadmap: 061 promoted; ADR 062 = `UPDATE` (table-policy Phase 0).

## Watch-items

1. **Write-only-if-`created`** is the race fix; the pre-execution skip (D7)
   is an optimization — `created==false` must suppress the write even after
   source execution (D11/D12). Don't rely on the skip alone.
2. **Wendelon-backed race (D12):** the create race is at the descriptor
   layer, not only the local catalog — resolve/sync before deciding
   created=false / SchemaMismatch / surface-admin-error.
3. **Bind-always** — static source errors surface even when the target
   exists; only row execution/write is skipped.
4. **OR-REPLACE + IF-NOT-EXISTS still rejected; column-list still rejected.**
5. **Non-fragmentable / coordinator-owned** — no new distributed mutation
   protocol; write-back stays coordinator `writer.write`.
6. **Result shape** unchanged (empty success).
7. **Prior-guard reconciliation** — `adr033`/`adr041` retargeted + the
   `ctas_test` fixture; meta scoped `…_except_retarget`.

## Acceptance checklist

- [ ] Phase 1: `CreateTableOutcome { binding, created }` +
      `create_table_with_outcome`; `create_table` wrapper; all impls; D12
      Wendelon-create-race recovery.
- [ ] Phase 2: `if_not_exists` on Logical + Execution `CreateTableAsSelect`.
- [ ] Phase 3: planner accepts IF-NOT-EXISTS CTAS; OR-REPLACE + column-list
      rejects kept; bind-always.
- [ ] Phase 4: resolve-before-source; compatible→empty success no source
      exec; incompatible→`SchemaMismatch`; absent→source + create-with-outcome
      + write-only-if-`created`; raced-in (local + Wendelon) handled.
- [ ] Phase 5: 11 + 3 + meta guards; `adr033`/`adr041` + `ctas_test`
      retargets; 12 fixtures; evidence + roadmap; no UPDATE / PK-DDL /
      RETURNING / Wendelon-server/wire / worker-protocol drift.
