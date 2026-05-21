# Impl Plan 063: PK-Table `UPDATE … WHERE` via Full-Row Upsert

**Status:** Promoted, paired with `plans/adr/063_pk_table_update.md`.
**Posture:** Toron SQL/planner/binder/coordinator. Composition over ADR 062;
reuses the INSERT-SELECT write-with-source path + `StorageWrite` PK
supersession. **No new Wendelon wire/storage primitive, no worker write-back,
no transaction model, no sqlparser upgrade.** Six phases.

## Invariants carried in

- ADR 054/061/062 unchanged **except** the retargeted
  `adr062_update_surface_remains_absent` guard (now present under the PK-table
  contract). **DELETE semantics unchanged** (UPDATE is stricter — WHERE
  required). `crates/nilket` runtime-free; fixture worker unchanged; no-`*V1`;
  no `RETURNING`.

## Phase 0 — Audit (verified anchors)

- **sqlparser 0.54.0** `Statement::Update { table: TableWithJoins,
  assignments: Vec<Assignment>, from, selection: Option<Expr>, returning, or }`
  (`…/sqlparser-0.54.0/src/ast/mod.rs:2494`) — 6 fields; no `OUTPUT`/`ORDER
  BY`/`LIMIT`. **0.62 upgrade is out of scope** (breaks `cargo check -p toron
  --tests` — broad AST drift). Toron's dispatch has no Update arm.
  `lower_delete` (`:599`, `selection: Option` `:683`) is the target-strictness
  + WHERE template; `lower_expr` lowers RHS + predicate.
- `binding.row_identity: TableRowIdentity` (catalog.rs:79, ADR 062) +
  `primary_key_columns` — the PK gate + ordinal.
- `coordinator.rs:961 execute_write_plan_with_source`; `InsertSelect` arm
  (`:994`, `execute_read_plan(source)` `:1002`) is the mirror.
  `MutationSourceExecutor` (Local `:899` / Distributed `:928`).
  `coordinator/write.rs:37 materialize_insert_select_batch` +
  `affected_count_batch` (`:15`).
- `server/mod.rs:656–697` — worker defensive mutation-reject + label arms →
  add an Update arm. ADR 054 descriptor-evolved write contract.
- **`adr062_update_surface_remains_absent`** (architecture_test) — the
  forward-looking guard 062 shipped; **goes false** when 063 adds the surface
  → **must retarget**.

## Phase 1 — Planner (sql_planner.rs)

- Add a `Statement::Update { .. }` arm that passes the six 0.54.0 fields into
  `lower_update`; retarget the top-level unsupported-statement message to
  include UPDATE.
- `lower_update`: exhaustively destructure the 6 fields. Accept `table`
  (single `TableFactor::Table`, no alias, **`table.joins` empty**),
  `assignments` (non-empty; unqualified single-ident targets; reject tuple/
  qualified/duplicate/empty), `selection` (**required** → predicate via
  `lower_expr`). Reject `from.is_some()`, `returning.is_some()`,
  `or.is_some()`. Build `LogicalPlan::Update { table, assignments, predicate }`.

## Phase 2 — IR (ir.rs, ir/execution.rs)

- `LogicalPlan::Update { table: TableRef, assignments: Vec<(String, Expr)>,
  predicate: Expr }` (name-based; no Wendelon types). `ExecutionPlan::Update {
  legacy_table_id, table, fingerprint, schema, source: Box<ExecutionPlan>,
  profile_hint }`. Exhaustiveness (walkers, schema labels, exchange/
  distributed-leaf helpers).

## Phase 3 — Binder (binder.rs)

- Resolve binding; **require `TableRowIdentity::PrimaryKey { column, ordinal }`**
  (reject AppendOnly/`Unsupported` → "UPDATE requires a PRIMARY KEY table");
  **reject SET of the PK column**; resolve targets to real user columns.
- Build the full-row **source projection** in target-schema order (assigned ←
  bound RHS; unassigned + PK ← matched-row col refs); `Filter(predicate)`;
  `Scan(all user columns, row_identity=None)`. **All RHS read the original
  matched row** (simultaneous — one projection over one input row). Preserve
  `QueryProfileHint::PointRead` for PK-equality (reuse 062 scan-binding). Emit
  `ExecutionPlan::Update { source, descriptor pins, … }`.

## Phase 4 — Coordinator + write (coordinator.rs, coordinator/write.rs)

- Route UPDATE through the write bucket (with INSERT/INSERT-SELECT/CTAS/
  DELETE), **before** fragmentation. `execute_write_plan_with_source`
  `ExecutionPlan::Update` arm: `execute_read_plan(source)`; **zero rows →
  `affected_count_batch(0)`, no `StorageWrite`**; else materialize/coerce via
  generalized `materialize_write_select_batch` (INSERT-SELECT lattice; null-
  into-non-null fails) → `StorageWrite::write` (target pins) →
  `affected_count_batch(outcome.rows)`. **ADR 054:** `DescriptorEvolved` →
  invalidate + return, no retry. `StorageWrite`, never `StorageDeleteRows`.

## Phase 5 — Distributed / worker (distributed/fragment.rs, server/mod.rs)

- UPDATE **non-fragmentable**; add defensive `ExecutionPlan::Update` arms to
  the fragmenter + worker reject/label match arms (`server/mod.rs:656–697`),
  label "update". Source read distributes via the existing read path; write
  stays coordinator-owned.

## Phase 6 — Guards + retarget + fixtures + evidence + roadmap

- 23 architecture + 3 convention + meta guards (ADR list), incl.
  `adr063_sqlparser_062_upgrade_is_not_part_of_update_surface`. **Retarget**
  `adr062_update_surface_remains_absent` → present-under-PK-contract (preserve
  the narrow intent: no RETURNING/no-WHERE/PK-mutation/append-only/Wendelon-
  change/worker-write-back); `adr062_returning_surface_remains_absent` stays.
  Meta scoped `…_except_retarget`.
- 18 fixtures (ADR list), incl. `update_assignments_read_original_row_values`
  (`SET a=b,b=a` swap) + `live_wendelon_sql_pk_update_supersedes_visible_row`.
- `plans/impl/063_pk_table_update_evidence.md`: `architecture_test adr063`,
  full `architecture_test`, `cargo test -p toron` (planner/binder/coordinator/
  update suite counts), `clippy -p toron --all-targets -- -D warnings`,
  `git diff --check` (capped-linker procedure).
- Roadmap: 063 promoted — core DML (SELECT/INSERT/DELETE/UPDATE/CTAS) complete
  for PK tables.

## Watch-items

1. **Retarget `adr062_update_surface_remains_absent`** — it goes false; the
   one prior-guard reconciliation 063 needs (Codex caught it). Don't ship the
   stale assertion.
2. **WHERE required, DELETE unchanged** — document the deliberate asymmetry;
   don't touch `lower_delete`.
3. **SET-can't-modify-PK** — guarantees same-PK supersession (no collision,
   no two-op). Reject at bind.
4. **Simultaneous assignment** — all RHS read the original row; `SET a=b,b=a`
   swaps. Pin the fixture.
5. **Per-row atomic, NOT transaction** — precise wording; no rollback/lock/CAS.
6. **INSERT-SELECT coercion reuse**; null-into-non-null fails before write.
7. **ADR 054 descriptor-evolved** — invalidate, no retry.
8. **`StorageWrite` not `StorageDeleteRows`; non-fragmentable; worker
   defensive reject.**
9. **Exhaustive 0.54.0 destructure**; reject from/returning/or/joins/alias;
   **no opportunistic 0.62 upgrade** (guarded).
10. **No Wendelon change; no `RETURNING`; Complete-only, row_count =
    outcome.rows.**

## Acceptance checklist

- [ ] Phase 1: `Statement::Update` arm; exhaustive 6-field destructure; WHERE
      required; reject from/returning/or/joins/alias/tuple/qualified/dup/empty;
      top-level message retargeted.
- [ ] Phase 2: `LogicalPlan::Update` (name-based) + `ExecutionPlan::Update`
      (source-bearing); exhaustiveness.
- [ ] Phase 3: PK-identity gate + reject-PK-assignment + full-row simultaneous
      projection + point-read preserved.
- [ ] Phase 4: coordinator Update arm; zero-rows-no-write; INSERT-SELECT
      coercion; `StorageWrite`; 054 invalidate-no-retry; row_count =
      outcome.rows.
- [ ] Phase 5: non-fragmentable; fragmenter + worker defensive Update arms.
- [ ] Phase 6: 23+3+meta guards; **`adr062_update_surface_remains_absent`
      retargeted**; 0.62-not-in-scope guard; 18 fixtures; evidence + roadmap;
      no RETURNING / no-WHERE / PK-mutation / append-only / Wendelon / worker-
      write-back / parser-upgrade drift.
