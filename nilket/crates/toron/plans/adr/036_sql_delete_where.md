# ADR 036: SQL `DELETE WHERE` — Local Composition over Row-Identity Substrate

## Status

Promoted. Three-way council convergence (Claude /
Codex / Gemini) reached in three iterations — **fourth
composition cycle** at 3-iter / 0-flip-flop cadence
matching ADR 032 + 033 + 034. **First composition
cycle in the new substrate phase** following the
composition trilogy (032 → 033 → 034) + substrate
cycle (035). Settles SQL `DELETE FROM table [WHERE
predicate]` as a narrow local composition over the
ADR 035 row-identity / predicate-delete substrate,
honoring the ADR 035 D15 sequencing commitment.

**Third independent three-way iter-0 topic
convergence in council history** (after ADR 033 +
ADR 035). All three councilors picked DELETE WHERE
as ADR 036 lead at iter-0 with no topic dissent —
pattern: when prior ADR sequencing pin commits the
next topic, three-way iter-0 convergence becomes
default.

**Ninth consecutive cycle of the Codex correctness-
clarification pattern.** Codex iter-0 R1 ("residual
predicates are the correctness trap") was the load-
bearing audit: without explicit Toron-side residual
filtering between scan and identity extraction, a
non-PBC predicate like `DELETE FROM t WHERE amount
+ 1 > 10` would silently become a full-table
DELETE — the scan returns every row, identity
append happens, every identity gets extracted.
Catastrophic silent data loss. This audit forced
the **Claude iter-0 C3 retraction** (PBC-only
predicates → accept-with-residual-filter), the
second substantive Claude retraction in council
history after the ADR 033 four-default retraction.

**Gemini iter-1/2 retraction streak advances to
9-of-11** after the 8-of-10 ADR 035 mark. Gemini
iter-0 produced two wrong defaults (side-channel
result counters; thin predicate IR without
`pbc_program` / disposition); Gemini iter-1
retracted both cleanly per established retraction
discipline; Gemini iter-2 fully convergent with
zero dissent.

**Claude iter-0 / iter-1 combined 12-of-14
contribution promotion rate** (with one substantive
C3 retraction). iter-0 C1, C2, C4, C5, C6, C8 all
promoted to Codex iter-2 D-level decisions; iter-1
C10, C11, C12, C13 all promoted to D6, guard #13,
D3, D5; C7 + C9 + C14 confirmed as cycle property
data points; C3 retracted.

Eighteenth instance of the ADR 015 cycle (after
016 / 017 / 018 / 019 / 022 / 023 / 024 / 025 /
026 / 027 / 028 / 029 / 030 / 031 / 032 / 033 /
034 / 035).

## Scale boundary

**Primary:** Toron SQL planner + binder + local
coordinator + existing ADR 035 `coordinator/delete.rs`
submodule + existing executor filter logic. Surface
adds the SQL `DELETE` statement form atop the ADR
035 substrate — `coordinator/delete.rs` exists with
`#[allow(dead_code)]` annotations that ADR 036
removes by wiring SQL DELETE routing through them.

In scope:

- `SqlPlanner::plan` dispatches `Statement::Delete`
  to new `lower_delete`. The exhaustive
  `Statement::Delete` destructure (no `..`) follows
  ADR 029 D2 + ADR 033 D3 + ADR 034 D3 discipline,
  destructuring all 7 fields (`tables`, `from`,
  `using`, `selection`, `returning`, `order_by`,
  `limit`).
- Exhaustive `TableFactor::Table` destructure (no
  `..`) of all 9 fields (`name`, `alias`, `args`,
  `with_hints`, `version`, `with_ordinality`,
  `partitions`, `json_path`, `sample`).
- Neutral DML table-name helper
  `lower_table_ref_object_name(name, "DELETE")`
  (or equivalent factor); does not reuse DDL-named
  helpers (ADR 034 D4 `lower_ddl_table_name`
  reserved for DDL forms).
- New `LogicalPlan::Delete { table: TableRef,
  predicate: Option<Expr> }` variant.
- New `ExecutionPlan::Delete` variant carrying
  Shape A pre-resolved identity:
  - `legacy_table_id: TableId`;
  - `table: TableObjectId`;
  - `fingerprint: PhysicalDescriptorFingerprint`;
  - `schema: SchemaRef`;
  - `predicate: Option<BoundExprProgram>`;
  - `pbc_program: Option<PbcProgram>`;
  - `profile_hint: QueryProfileHint`.
- **No `FilterDisposition` field** on
  `ExecutionPlan::Delete`. State is derivable from
  `predicate` + `pbc_program`:
  - `predicate == None`: full-table delete;
  - `predicate.is_some() && pbc_program.is_some()`:
    storage evaluates the full predicate;
  - `predicate.is_some() && pbc_program.is_none()`:
    Toron residual filtering is required.
- Binder pass: `bind_delete` resolves the target
  via `catalog.resolve_table`, binds the optional
  predicate via the existing
  `bind_expr_program`, validates Boolean root via
  a new private helper `require_single_boolean_root(&program,
  "DELETE WHERE")`, attempts `compile_pbc(&program)`,
  preserves the bound predicate when PBC
  compilation returns `None`, derives `profile_hint`
  via the same primary-key-equality logic as SELECT
  filters.
- `bind_delete` arms added to `collect_qualifiers`,
  `terminal_exchange`, and `optimize_for_required`.
  `optimize_for_required` returns the DELETE plan
  unchanged with an empty `OrdinalRemap`.
- Local `QueryCoordinator::execute_query`
  intercepts `LogicalPlan::Delete` **before** the
  SELECT descriptor-evolution retry loop. Bind
  once, execute the mutation once — no retry loop
  around mutations.
- Coordinator delete-path:
  1. Build `ScanRequest` with `row_identity:
     RowIdentityProjection::Append`,
     `projected_columns: None`,
     `pbc_predicate: pbc_program.as_ref().map(|p| p.0.clone())`,
     `runtime_filter: None`.
  2. Call `materialize_delete_candidate_batches`.
  3. If `predicate.is_some() && pbc_program.is_none()`,
     apply residual filtering via the new shared
     `filter_batches_with_bound_predicate` helper
     (context `"delete predicate"`), preserving all
     input columns including the trailing
     `__row_key_id__`.
  4. Call `extract_delete_row_key_ids`.
  5. Call `execute_delete_with_candidates` (ADR 035
     empty-set short-circuit preserved).
  6. Return `delete_count_batch(outcome.deleted)`.
- ADR 035 `coordinator/delete.rs` helper refactored:
  - `materialize_delete_candidate_batches(storage,
    scan_request) -> Vec<RecordBatch>` (NEW).
  - `extract_delete_row_key_ids(batches) ->
    Vec<Identity>` (NEW).
  - `materialize_delete_row_key_ids(storage,
    scan_request) -> Vec<Identity>` (preserved
    wrapper; calls the two new helpers in sequence).
  - `delete_count_batch(count: usize) ->
    RecordBatch` (NEW; located in
    `coordinator/delete.rs`, not
    `coordinator/write.rs`).
  - All ADR 035 validation diagnostics preserved
    (missing identity column; wrong final column
    name; non-`FixedSizeBinary(32)` type; wrong
    array type; null identity values; non-32-byte
    identity values).
- `Executor::execute_bound`
  `ExecutionPlan::Filter` arm refactored to call
  new `pub(crate) filter_batches_with_bound_predicate`
  helper in `exec.rs`; helper takes a context
  `&'static str` for error scoping; preserves the
  existing diagnostic `"filter predicate must
  return boolean"` byte-identical. **Byte-identical
  refactor invariant** applies (Claude iter-1 A1).
- SQL result shape: exactly one `RecordBatch`, one
  non-null `Int64` column named `deleted`, one row,
  value from `i64::try_from(outcome.deleted)`.
  Other counters (`requested`, `already_deleted`,
  `not_found`) remain storage accounting and are
  not exposed through SQL.
- New `FragmentError::DeleteNotFragmentable`
  variant; fragmenter rejects `ExecutionPlan::Delete`
  before `contains_join` / `count_exchanges`.
- Distributed `QueryCoordinator::execute_query`
  rejects `ExecutionPlan::Delete` **before**
  `fragment_plan` with stable diagnostic:

  ```text
  distributed DELETE execution is not wired to a write-context-bearing coordinator
  ```

- Defensive `Executor::execute_bound` reject for
  uncoordinated `ExecutionPlan::Delete`:
  `ExecutionError::NotImplemented { feature:
  "DELETE execution is coordinator-only" }`.
- Defensive `LogicalPlan::Delete` reject for
  unbound execution:
  `ExecutionError::NotImplemented { feature:
  "unbound DELETE execution" }`.
- Distributed / scheduler / server walkers treat
  `Delete` as a leaf (join detection returns
  false; exchange counting returns zero; scan
  metadata returns none; replacement walkers leave
  the variant unchanged).
- Telemetry: `server::operator_kind_label(ExecutionPlan::Delete
  { .. })` returns `"delete"` (bare, mirrors
  `"insert"` discipline from ADR 030; distinct
  from `"insert_select"`, `"create_table_as_select"`,
  `"drop_table"`, `"catalog"`).
- 23 ADR-036-specific architecture guards + paired
  adjustment to the existing
  `architecture_guards_unchanged_plus_sort_variant`
  variant-count catch-all (LogicalPlan 14 → 15;
  ExecutionPlan 12 → 13).
- New test file
  `crates/toron/tests/delete_test.rs` (Claude
  iter-1 A3 file-naming convention carry-forward
  from ADR 034: one statement form, one test
  file) with 14 required functional fixtures + 1
  optional Wendelon-end-to-end fixture.

Out of scope:

- Distributed DELETE coordinator routing.
  Deferred to **ADR 037 cross-ring distributed-
  write-context substrate** (Codex iter-2 D11 +
  Claude iter-0 C8 sequencing pin).
- `USING` clause (`DELETE FROM a USING b WHERE
  ...`). Reject in v1.
- `RETURNING` clause. Future RETURNING substrate
  ADR.
- `ORDER BY` / `LIMIT` clauses. Reject in v1; SQL
  DELETE doesn't need them under tombstone
  semantics.
- Target table aliases. Reject in v1; supporting
  aliases requires delete-specific scoped binding
  not yet present.
- Multi-target DELETE (`DELETE FROM a, b WHERE
  ...`). Reject in v1.
- Subqueries in WHERE clause. Future correlated-
  subquery substrate ADR.
- `TRUNCATE`-style metadata-only DELETE. Future
  storage-admin / DDL ADR.
- New Wendelon-side change (Wendelon `Client::table_delete`
  + `Client::table_query` privileged identity
  projection already exist per ADR 035).
- New `StorageError` variant.
- New `StorageDeleteRows` trait method.
- New predicate-to-storage delete API.
- Physical storage reclaim / tombstone compaction.
  Future storage-admin ADR.
- Stale-bound-plan invalidation. Future ADR.
- `UPDATE WHERE`. Future composition cycle after
  the partial-write substrate ADR.
- Plan Arena / index-based plan IR. Fourth
  consecutive composition cycle preserving Plan
  Arena as named-debt-not-yet-pressured.
- `blake3` / external allocator crate (`bumpalo`
  / `typed-arena` / `slab` / `generational-arena`).

## Context

After ADR 035 promotion, the local CRUD substrate
is complete:

- **C**reate: ADR 029 CREATE TABLE.
- **R**ead: ADR 001-028 SELECT.
- **U**pdate: future ADR after partial-write
  substrate.
- **D**elete: ADR 035 row-identity / predicate-
  delete substrate.

ADR 035 D15 firmly committed ADR 036 to SQL
`DELETE WHERE` as a narrow composition over the
substrate. The substrate was designed to make ADR
036 a 3-iter composition cycle:

- `RowIdentityProjection::Append` exists for
  identity discovery scans;
- `DeleteRowsRequest` / `DeleteRowsOutcome` /
  `StorageDeleteRows` typed trait exists;
- `materialize_delete_row_key_ids` materializer
  exists with full identity-column validation
  shape;
- `execute_delete_with_candidates` empty-set short-
  circuit helper exists;
- `InMemoryTableStorage` tombstones + `WendelonStorageDeleteRows`
  adapter both wired and tested with 10 Toron
  fixtures + 3 Wendelon fixtures.

ADR 036 wires SQL DELETE through this existing
substrate. The `#[allow(dead_code)]` annotations
on `materialize_delete_row_key_ids` /
`execute_delete_with_candidates` / `QueryCoordinator::deleter`
field (all introduced in ADR 035) are removed at
ADR 036 promotion as the helpers become load-
bearing.

**Codex iter-0 R1 audit was decisive.** The naïve
composition shape (compile predicate to PBC, push
to storage, materialize identities) has a silent
correctness trap: if the predicate doesn't fully
compile to PBC (e.g. `amount + 1 > 10` arithmetic),
the scan returns every row, identity append
happens, every identity extracts → full-table
DELETE. Without explicit Toron-side residual
filtering between the scan and identity
extraction, non-PBC predicates become silent data
loss.

The corrected shape (Codex iter-0 D3 / iter-2 D3
+ D6):

- Binder accepts every Boolean-rooted predicate.
- Binder tries PBC compilation; on full success
  → `pbc_program: Some(_)`; on partial / failure
  → `pbc_program: None`.
- Coordinator delete-path applies residual
  filtering via a new shared `filter_batches_with_bound_predicate`
  helper when `pbc_program.is_none()`.
- The shared helper is extracted from the existing
  `Executor::execute_bound`
  `ExecutionPlan::Filter` arm with byte-identical
  preservation of SELECT WHERE behavior.

This shape matches the existing SELECT WHERE
clause discipline (`FilterDisposition::EvaluateInToron`
already exists for non-pushdown SELECT predicates).
DELETE WHERE inherits the same accept-with-residual-
filter shape, just at a different mutation surface.

The council converged in three iterations with
zero flip-flops and one substantive Claude
retraction (C3 PBC-only → accept-with-residual-
filter at iter-1). Iter-0 produced three-way
topic consensus + Codex R1 audit + Claude / Gemini
sketches with substrate-shape divergences (Claude
PBC-only; Gemini side-channel result + thin
predicate IR). Iter-1 settled load-bearing shape
via Codex iter-1 D1-D10 + Claude iter-1 C10-C14 +
Gemini iter-1 retractions. Iter-2 was promotion-
drafting with Codex iter-2 absorbing all iter-1
work into D1-D11 + 23 guards + 15 fixtures.

## Decision

### D1 — Narrow SQL `DELETE` surface

Support exactly:

```sql
DELETE FROM table_name;
DELETE FROM schema.table;
DELETE FROM table_name WHERE predicate;
DELETE FROM schema.table WHERE predicate;
```

No-WHERE DELETE is lowered as `predicate: None`
(not a synthetic `WHERE true` literal). The IR
already has `Option<Expr>` for predicate; using
`None` for full-table DELETE is cleaner than
fabricating a literal.

Reject in v1 with stable diagnostics:

- nonempty `Delete.tables` (multi-table MySQL form)
  → `"DELETE supports exactly one target table"`;
- `FromTable::WithoutKeyword` → `"DELETE requires
  FROM keyword"`;
- multiple `FROM` tables → `"DELETE supports
  exactly one target table"`;
- joins in the target table → `"unsupported DELETE
  target: joins not supported"`;
- `USING` clause → `"unsupported DELETE clause:
  USING"`;
- `RETURNING` clause → `"unsupported DELETE
  clause: RETURNING"`;
- `ORDER BY` clause → `"unsupported DELETE clause:
  ORDER BY"`;
- `LIMIT` clause → `"unsupported DELETE clause:
  LIMIT"`;
- target alias → `"unsupported DELETE target:
  table alias"`;
- non-table table factors (table-function, derived,
  function, unnest, nested-join) → stable per-form
  diagnostics;
- `TableFactor::Table` non-default options (`args`,
  `with_hints`, `version`, `with_ordinality`,
  `partitions`, `json_path`, `sample`) → stable
  per-clause diagnostics.

### D2 — Exhaustive `Statement::Delete` and `TableFactor::Table` destructure

`lower_delete` exhaustively destructures sqlparser
0.54's `Delete` with all 7 fields and no `..` rest
pattern:

```rust
let Delete {
    tables,
    from,
    using,
    selection,
    returning,
    order_by,
    limit,
} = delete;
```

The accepted `TableFactor::Table` exhaustively
destructures all 9 fields with no `..`:

```rust
TableFactor::Table {
    name,
    alias,
    args,
    with_hints,
    version,
    with_ordinality,
    partitions,
    json_path,
    sample,
}
```

This mirrors ADR 029 D2 + ADR 033 D3 + ADR 034 D3
exhaustive-destructure discipline. sqlparser
upgrades fail fast if `Delete` or `TableFactor::Table`
gain fields.

### D3 — Neutral DML table-name helper

DELETE adds a neutral table-name helper:

```rust
fn lower_table_ref_object_name(
    name: &sqlparser::ast::ObjectName,
    context: &'static str,
) -> Result<TableRef, FrontendError>
```

This helper handles one- or two-part table names
with the same discipline as ADR 029's
`lower_ddl_table_name` (rejects three-part names),
but accepts a `context` parameter so diagnostics
say `"DELETE"` rather than `"DDL"`.

**Do not reuse `lower_ddl_table_name` for DELETE.**
DDL wording in DML diagnostics is confusing.
Factor or add the neutral helper at impl time.
Future DML forms (UPDATE, MERGE) reuse the same
neutral helper with their own context strings.

### D4 — Leaf IR variants (Shape A)

```rust
LogicalPlan::Delete {
    table: TableRef,
    predicate: Option<Expr>,
}

ExecutionPlan::Delete {
    legacy_table_id: TableId,
    table: TableObjectId,
    fingerprint: PhysicalDescriptorFingerprint,
    schema: SchemaRef,
    predicate: Option<BoundExprProgram>,
    pbc_program: Option<PbcProgram>,
    profile_hint: QueryProfileHint,
}
```

`ExecutionPlan::Delete` is a **leaf**: no
`Box<ExecutionPlan>`, no `Box<LogicalPlan>`, no
`source`, no `input`. The coordinator generates the
candidate scan request directly from the bound
metadata.

**Shape A** (pre-resolved identity) chosen over
Shape B (resolved at execute time). Rationale:

- DELETE binds the predicate against the target's
  current schema — identity has been resolved at
  bind time anyway.
- Coordinator needs identity for both scan + delete
  operations on the same table.
- ADR 030 INSERT precedent uses Shape A.
- ADR 033 CTAS R1 / ADR 034 DROP R1 Shape B
  reasoning was about avoiding pre-mutation state
  in catalog-mutating IR variants; DELETE doesn't
  mutate catalog.

**No `FilterDisposition` field on
`ExecutionPlan::Delete`.** State is fully derivable
from `predicate` + `pbc_program`:

| Predicate | pbc_program | Execution path |
|---|---|---|
| `None` | `None` | Full-table delete |
| `Some(_)` | `Some(_)` | Storage evaluates full predicate via PBC |
| `Some(_)` | `None` | Toron residual filtering required |

Avoiding the extra field prevents inconsistent
states like `predicate: None` + `EvaluateInToron`.

Variant counts:

- `LogicalPlan`: 14 → 15;
- `ExecutionPlan`: 12 → 13.

### D5 — Binder validates Boolean predicate + classifies pushdown

`Binder::bind_plan` adds a `LogicalPlan::Delete`
arm calling `bind_delete`. The binder:

1. Resolves the target table with
   `catalog.resolve_table(name)`. Errors if not
   found.
2. Binds the optional predicate against the target
   user schema via existing `bind_expr_program`.
3. Validates Boolean root using a new private
   helper `require_single_boolean_root(&program,
   "DELETE WHERE")`:
   - requires `program.root_count() == 1`;
   - requires root type `DataType::Boolean`;
   - returns a stable `BindingError::TypeMismatch`
     diagnostic.
4. Attempts `compile_pbc(&program)`.
5. Sets `pbc_program: Some(_)` only when the **full
   predicate** compiles to PBC. Partial-pushdown
   is treated as `pbc_program: None` for
   simplicity in v1.
6. Preserves the bound `BoundExprProgram` for
   Toron residual evaluation when `pbc_program`
   is `None`.
7. Derives `profile_hint` from the same primary-
   key-equality logic SELECT filters use.

**Do not reject non-PBC predicates.** Predicates
like `amount + 1 > 10` are accepted; the
coordinator applies them in Toron after
materialization (D7). This matches the existing
SELECT WHERE clause discipline.

`bind_delete` arms added to:

- `collect_qualifiers` — DELETE is a leaf, no
  source qualifier contribution.
- `terminal_exchange` — DELETE is not a terminal
  exchange; return `None`.
- `optimize_for_required` — return the DELETE
  plan unchanged with an empty `OrdinalRemap`.

### D6 — Residual filter helper extraction

The existing bound filter logic in
`Executor::execute_bound`'s `ExecutionPlan::Filter`
arm is extracted to a new shared `pub(crate)`
helper in `exec.rs`:

```rust
pub(crate) fn filter_batches_with_bound_predicate(
    &self,
    predicate: &BoundExprProgram,
    batches: Vec<RecordBatch>,
    context: &'static str,
) -> Result<Vec<RecordBatch>, ExecutionError>
```

The helper:

- evaluates exactly one Boolean root;
- downcasts to `BooleanArray`;
- calls `filter_record_batch`;
- preserves the full input schema and all columns
  (including the trailing `__row_key_id__` when
  called from the DELETE coordinator).

`ExecutionPlan::Filter` arm calls the helper with
context `"filter predicate"` and **must preserve
its current behavior byte-identical**, including
the existing error message `"filter predicate must
return boolean"`.

The DELETE coordinator calls the same helper with
context `"delete predicate"` only when
`predicate.is_some() && pbc_program.is_none()`.

**Byte-identical refactor invariant** (Claude
iter-1 A1): the extracted helper must preserve
existing SELECT WHERE filter behavior. Architecture
guard #23 source-checks the call site replaces the
inlined logic. Reference fixture pass-count must
match pre-refactor for all SELECT WHERE corpus
tests.

### D7 — Coordinator candidate path

Local `QueryCoordinator::execute_query` intercepts
`LogicalPlan::Delete` **before** the SELECT
descriptor-evolution retry loop. Bind once,
execute the mutation once — **no retry loop
around mutations** (R4 from Codex iter-0).

Coordinator sequence:

1. Build `ScanRequest` from execution metadata +
   coordinator context:
   - `session_id` / `query_id` / `tenant_id` /
     `read_bound` / `profile_hint` /
     `trace_context` from context;
   - `table_id` / `table_object_id` /
     `physical_descriptor_fingerprint` from
     `ExecutionPlan::Delete`;
   - `projected_columns: None`;
   - `pbc_predicate: pbc_program.as_ref().map(|p| p.0.clone())`;
   - `runtime_filter: None`;
   - `row_identity: RowIdentityProjection::Append`.
2. Call `materialize_delete_candidate_batches(storage,
   scan_request)` (returns `Vec<RecordBatch>` with
   appended identity column).
3. If `predicate.is_some() && pbc_program.is_none()`,
   apply residual filtering via
   `filter_batches_with_bound_predicate(predicate,
   batches, "delete predicate")`. The trailing
   `__row_key_id__` column survives the filter
   because `filter_record_batch` preserves all
   columns.
4. Call `extract_delete_row_key_ids(batches)`
   (returns `Vec<Identity>`).
5. Build `DeleteRowsRequest` with the extracted
   row-key identities.
6. Call `execute_delete_with_candidates(deleter,
   request)` (ADR 035 empty-set short-circuit
   preserved — if `row_key_ids.is_empty()`,
   returns zero `DeleteRowsOutcome` without
   calling storage).
7. Return `vec![delete_count_batch(outcome.deleted)]`.

**`runtime_filter: None` is intentional**
(Claude iter-1 C11 / Codex iter-2 guard #13).
DELETE has no join build side producing a runtime
filter; inheriting an ambient runtime filter from
the executor context would silently narrow a
mutation candidate set — a critical correctness
hazard. The architecture guard pins both the
positive literal `runtime_filter: None` and the
negative `runtime_filter: self.context.runtime_filter`
inheritance pattern.

### D8 — ADR 035 helper split

`coordinator/delete.rs` is refactored to split the
existing materializer into three helpers, preserving
the load-bearing wrapper name:

```rust
pub(crate) async fn materialize_delete_candidate_batches(
    storage: &dyn StorageScan,
    scan_request: ScanRequest,
) -> Result<Vec<RecordBatch>, StorageError>;

pub(crate) fn extract_delete_row_key_ids(
    batches: &[RecordBatch],
) -> Result<Vec<Identity>, StorageError>;

pub(crate) async fn materialize_delete_row_key_ids(
    storage: &dyn StorageScan,
    scan_request: ScanRequest,
) -> Result<Vec<Identity>, StorageError> {
    let batches = materialize_delete_candidate_batches(storage, scan_request).await?;
    extract_delete_row_key_ids(&batches)
}
```

The wrapper name `materialize_delete_row_key_ids`
is preserved so ADR 035 architecture guards + tests
remain green.

All ADR 035 validation diagnostics are preserved in
`extract_delete_row_key_ids`:

- missing identity column;
- wrong final column name;
- non-`FixedSizeBinary(32)` type;
- wrong array type;
- null identity values;
- non-32-byte identity values.

A new helper `delete_count_batch(count: usize) ->
Result<RecordBatch, ExecutionError>` is added to
`coordinator/delete.rs` (not `coordinator/write.rs`).
DELETE is the only DML form in v1 returning a row-
count batch; future RETURNING ADR can refactor
when multiple DML forms produce result batches.

The `#[allow(dead_code)]` annotations on
`materialize_delete_row_key_ids` /
`execute_delete_with_candidates` /
`QueryCoordinator::deleter` field (introduced in
ADR 035) are removed once coordinator routing uses
the helpers.

### D9 — SQL result shape

Successful DELETE returns:

- exactly one `RecordBatch`;
- schema with one non-null `Int64` field named
  `deleted`;
- one row;
- value from `i64::try_from(outcome.deleted)`.

Overflow (count > `i64::MAX`) maps to an internal
query error. Other counters (`requested`,
`already_deleted`, `not_found`) remain storage
accounting and are not exposed through SQL.

This is intentionally different from existing
INSERT / DDL coordinator paths, which currently
return empty batch vectors. ADR 036 does not
retrofit INSERT / DDL row-count behavior; that's a
future ADR (likely bundled with RETURNING).

### D10 — Distributed reject

`DistributedQueryCoordinator::execute_query`:

1. Binds first;
2. Recognizes `ExecutionPlan::Delete`;
3. Returns stable diagnostic before `fragment_plan`:

   ```text
   distributed DELETE execution is not wired to a write-context-bearing coordinator
   ```

`fragment_plan` still gets a defensive
`FragmentError::DeleteNotFragmentable` arm,
placed before `contains_join` and
`count_exchanges` (mirrors ADR 030 / 032 / 033 /
034 distributed-write reject discipline).

Cross-ring distributed-write-context substrate
(ADR 037 Path C, firmly committed by D11) will
lift this reject + the four prior local-only
write rejects (INSERT / INSERT SELECT / CTAS /
DROP).

### D11 — Defensive executor + walker leaves + telemetry

Defensive arms:

- unbound executor `LogicalPlan::Delete` →
  `ExecutionError::NotImplemented { feature:
  "unbound DELETE execution" }`;
- bound executor `ExecutionPlan::Delete` →
  `ExecutionError::NotImplemented { feature:
  "DELETE execution is coordinator-only" }`;
- fragmenter `ExecutionPlan::Delete` →
  `FragmentError::DeleteNotFragmentable`;
- distributed / scheduler / server walkers treat
  `Delete` as a leaf:
  - `contains_join` returns false;
  - `count_exchanges` returns zero;
  - `first_scan_metadata` returns `None`;
  - `first_scan_fingerprint` returns `None`;
  - server/scheduler plan-label helpers return
    explicit unreachable or `"delete"`.

`operator_kind_label(ExecutionPlan::Delete { .. })`
returns `"delete"` (bare, mirrors `"insert"`
discipline from ADR 030; distinct from
`"insert_select"`, `"create_table_as_select"`,
`"drop_table"`, `"catalog"`).

### D12 — Identity remains SQL-invisible

The private identity column:

- cannot be referenced in SQL projection
  resolution (planner / binder synthetic-name
  rejects from ADR 029 D12 + ADR 030 D7 carry
  forward unchanged);
- is appended only by coordinator delete candidate
  scans (architecture guard #14 asserts this is
  the only Toron-side source of `RowIdentityProjection::Append`);
- survives residual filtering through
  `filter_batches_with_bound_predicate` (which
  preserves all input columns);
- is consumed by `extract_delete_row_key_ids`;
- never appears in the user result batch.

### D13 — Plan Arena remains debt

DELETE adds two leaf plan variants (one logical,
one execution) with no boxed source children.
Plan Arena pressure essentially unchanged.

ADR 036 must not add `bumpalo` / `typed-arena` /
`slab` / `generational-arena` / `blake3` /
storage synthetic row construction.

Fourth consecutive composition cycle preserving
Plan Arena as named-debt-not-yet-pressured (after
ADR 032 / 033 / 034 + extending to ADR 036).

### D14 — ADR 037 sequencing commitment

ADR 037 should be the **cross-ring distributed-
write-context substrate** (Claude iter-0 C8 Path
C / Codex iter-2 D11). This substrate cycle lifts
the accumulated local-only write rejects from
five prior cycles:

- ADR 030 D13 distributed INSERT reject;
- ADR 032 D1 distributed INSERT SELECT reject;
- ADR 033 D11 distributed CTAS reject;
- ADR 034 D12 distributed DROP TABLE reject /
  `WendelonCatalog::drop_table` `AdminUnavailable`;
- ADR 036 D10 distributed DELETE reject.

ADR 037 unblocks distributed write execution +
transactional CTAS + atomic DROP simultaneously
without entangling UPDATE or RETURNING.

This restores the **topic-concession-compresses-
next-cycle pattern at 5-of-7** — substrate-defer
ADRs seed the next cycle (ADR 035 D15 → ADR 036;
ADR 036 D14 → ADR 037).

## Reject removal (ADR 015 cycle's eighteenth instance)

ADR 036 lifts the implicit DELETE rejection that
all prior cycles assumed (`SqlPlanner::plan`
previously rejected `Statement::Delete` as an
unsupported statement). The new `lower_delete`
accepts the narrow form. The supported-statement
diagnostic updates to include `DELETE`.

**Added (planner, lower time):**

- USING / RETURNING / ORDER BY / LIMIT / multi-
  target / alias / non-default table-factor option
  each rejects with stable per-clause diagnostic;
- non-table table factors reject with stable
  per-form diagnostic.

**Added (binder, bind time):**

- non-existent table: existing
  `BindingError::UnresolvedTable`;
- predicate column not in target schema:
  existing `BindingError::UnknownColumn`;
- non-Boolean predicate root: new
  `BindingError::TypeMismatch` via
  `require_single_boolean_root`.

**Added (distributed coordinator):**

- `ExecutionPlan::Delete` reaches `fragment_plan`:
  new `FragmentError::DeleteNotFragmentable`.

**Added (executor):**

- unbound `LogicalPlan::Delete` and bound
  `ExecutionPlan::Delete` defensive
  `NotImplemented` rejects.

Existing CREATE TABLE / INSERT / INSERT SELECT /
CTAS / DROP rejects remain unchanged. ADR 035
guard `adr035_no_sql_delete_yet` is removed /
replaced by an ADR 036 positive guard set. No
reject is silently widened or narrowed beyond
the explicit lift.

## Architecture Guard Set

**23 ADR-036-specific guards** in
`crates/toron/tests/architecture_test.rs`.

| # | Guard | Pins | File anchor |
|---|---|---|---|
| 1 | `adr036_sql_planner_accepts_delete` | Statement dispatcher contains `Statement::Delete`; supported diagnostic includes `DELETE` | `sql_planner.rs` |
| 2 | `adr036_delete_destructure_is_exhaustive` | `lower_delete` body destructures all 7 `Delete` fields; no `..` | `sql_planner.rs` |
| 3 | `adr036_delete_target_table_destructure_is_exhaustive` | Accepted `TableFactor::Table` destructures all 9 fields; no `..` | `sql_planner.rs` |
| 4 | `adr036_delete_reject_surface_is_narrow` | USING / RETURNING / ORDER BY / LIMIT / alias / multi-target each have stable reject literals | `sql_planner.rs` |
| 5 | `adr036_logical_variant_count_is_15` | `LogicalPlan` 14 → 15 growth | `ir.rs` |
| 6 | `adr036_execution_variant_count_is_13` | `ExecutionPlan` 12 → 13 growth | `ir/execution.rs` |
| 7 | `adr036_ir_delete_variants_are_leafs` | Both variants exist; `ExecutionPlan::Delete` contains no `Box<`, `input`, or `source` | `ir.rs`, `ir/execution.rs` |
| 8 | `adr036_execution_delete_carries_shape_a_identity` | `ExecutionPlan::Delete` body contains `legacy_table_id`, `table`, `fingerprint`, `schema` | `ir/execution.rs` |
| 9 | `adr036_delete_has_no_filter_disposition_field` | `ExecutionPlan::Delete` body contains no `FilterDisposition` literal | `ir/execution.rs` |
| 10 | `adr036_binder_delete_uses_boolean_root_validator` | `bind_delete` body calls `require_single_boolean_root` or equivalent shared helper; no inline `DataType::Boolean` literal | `binder.rs` |
| 11 | `adr036_binder_delete_allows_residual_predicates` | `bind_delete` does NOT reject `compile_pbc(_) -> None`; residual path is preserved | `binder.rs` |
| 12 | `adr036_delete_candidate_scan_uses_append_only` | Coordinator delete-path ScanRequest contains `row_identity: RowIdentityProjection::Append` + `projected_columns: None` | `coordinator.rs`, `coordinator/delete.rs` |
| 13 | `adr036_delete_candidate_scan_uses_no_runtime_filter` | Coordinator delete-path ScanRequest contains `runtime_filter: None` literal; does NOT contain `runtime_filter: self.context.runtime_filter` or `runtime_filter: context.runtime_filter` inheritance | `coordinator.rs`, `coordinator/delete.rs` |
| 14 | `adr036_delete_residual_filter_before_identity_extraction` | Coordinator delete-path applies residual filter via `filter_batches_with_bound_predicate` BEFORE calling `extract_delete_row_key_ids` | `coordinator.rs`, `coordinator/delete.rs` |
| 15 | `adr036_adr035_materializer_name_preserved` | `materialize_delete_row_key_ids` wrapper name remains; delegates through `materialize_delete_candidate_batches` + `extract_delete_row_key_ids` | `coordinator/delete.rs` |
| 16 | `adr036_delete_uses_execute_delete_with_candidates` | Coordinator delete-path calls `execute_delete_with_candidates` for empty-set short-circuit + delete invocation | `coordinator.rs`, `coordinator/delete.rs` |
| 17 | `adr036_delete_result_uses_deleted_only` | SQL result-mapping block reads `outcome.deleted`; that result-mapping block does NOT read `outcome.requested`, `outcome.already_deleted`, or `outcome.not_found` (do not scan ADR 035's empty-outcome helper body for the negative pattern) | `coordinator.rs`, `coordinator/delete.rs` |
| 18 | `adr036_distributed_delete_rejected_before_fragment` | Distributed coordinator intercept precedes `fragment_plan` call site for `ExecutionPlan::Delete` | `coordinator.rs` |
| 19 | `adr036_fragmenter_delete_defensive_reject` | `FragmentError::DeleteNotFragmentable` variant exists; rejected before `contains_join` / `count_exchanges` | `distributed/fragment.rs` |
| 20 | `adr036_executor_delete_defensive_arms` | Unbound + bound executor diagnostics present | `exec.rs` |
| 21 | `adr036_operator_label_is_delete` | `operator_kind_label` maps `Delete` to `"delete"` | `server/mod.rs` |
| 22 | `adr036_synthetic_identity_still_not_sql_visible` | ADR 035 guard #18 carries forward: planner / binder synthetic-name rejects for `__row_key_id__` / `__row_hlc__` remain green over SQL DELETE syntax | `sql_planner.rs`, `binder.rs` |
| 23 | `adr036_filter_helper_preserves_execution_plan_filter` | `ExecutionPlan::Filter` arm calls `filter_batches_with_bound_predicate` helper; existing `"filter predicate must return boolean"` diagnostic preserved byte-identical | `exec.rs` |

Paired adjustment:

| Existing guard | Adjustment |
|---|---|
| `architecture_guards_unchanged_plus_sort_variant` | `LogicalPlan` count assert bumps 14 → 15 with `Delete` content assert; `ExecutionPlan` count assert bumps 12 → 13 with `Delete` content assert |

ADR 035 guard `adr035_no_sql_delete_yet` is
**removed** (replaced by ADR 036 guards #1 + #5
+ #6 + #7 which positively assert the new
DELETE surface).

## Required functional fixtures

**14 fixtures required for promotion + 1 optional
Wendelon-end-to-end fixture.** Live in new
`crates/toron/tests/delete_test.rs` (Claude
iter-1 A3 file-naming convention; one statement
form, one test file).

1. `planner_lowers_delete_where` — `DELETE FROM t
   WHERE id = 1` + `DELETE FROM app.t WHERE col >
   0`; assert `LogicalPlan::Delete` fields.
2. `planner_lowers_delete_without_where` — `DELETE
   FROM t`; assert `LogicalPlan::Delete { table,
   predicate: None }`.
3. `planner_rejects_unsupported_delete_shapes` —
   USING / RETURNING / ORDER BY / LIMIT / multi-
   target / alias / non-table factors / non-
   default `TableFactor::Table` options. Each
   produces a stable diagnostic.
4. `binder_resolves_delete_shape_a_identity` —
   bind `DELETE FROM t WHERE col > 0`; assert
   `ExecutionPlan::Delete { legacy_table_id,
   table, fingerprint, schema, predicate: Some(_),
   pbc_program: Some(_), profile_hint }` with
   Shape A identity.
5. `binder_rejects_unknown_delete_column` —
   `DELETE FROM t WHERE missing_col > 0`; assert
   `BindingError::UnknownColumn`.
6. `binder_rejects_non_boolean_delete_predicate`
   — `DELETE FROM t WHERE col + 1`; assert
   `BindingError::TypeMismatch` via
   `require_single_boolean_root`.
7. `coordinator_deletes_rows_with_pbc_predicate`
   — write 5 rows; `DELETE FROM t WHERE id > 2`;
   assert result batch has `deleted: Int64`
   column, one row, value 3; subsequent SELECT
   returns 2 rows.
8. `coordinator_deletes_rows_with_residual_predicate`
   — write 5 rows; `DELETE FROM t WHERE amount +
   1 > 10` (non-PBC arithmetic); assert correct
   matching rows deleted (residual filter
   applied); assert subsequent SELECT returns
   only non-matching rows. **Critical R1
   correctness fixture** — proves residual
   predicate path does not degrade to full-table
   delete.
9. `coordinator_deletes_all_rows_without_where` —
   write 5 rows; `DELETE FROM t`; assert result
   batch `deleted = 5`; subsequent SELECT
   returns 0 rows.
10. `coordinator_delete_zero_matches_returns_zero_count`
    — write 5 rows; `DELETE FROM t WHERE id >
    100`; assert result batch `deleted = 0`;
    subsequent SELECT returns 5 rows.
11. `coordinator_repeated_delete_reports_deleted_only`
    — write 5 rows; `DELETE FROM t WHERE id =
    1` twice; first returns `deleted = 1`;
    second returns `deleted = 0` (already-
    deleted not counted in SQL result); proves
    SQL row count maps only to `.deleted` field.
12. `coordinator_empty_candidate_set_short_circuits`
    — write 0 rows; `DELETE FROM t WHERE id =
    1`; assert result batch `deleted = 0`; ADR
    035 empty-set short-circuit preserved.
13. `distributed_coordinator_rejects_delete` —
    `DistributedQueryCoordinator` + `DELETE`;
    assert stable diagnostic; assert reject
    fires before `fragment_plan`.
14. `fragmenter_rejects_delete_defensively` —
    `fragment_plan(ExecutionPlan::Delete)`;
    assert `FragmentError::DeleteNotFragmentable`;
    assert reject before `contains_join` /
    `count_exchanges`.
15. *(Optional, if Wendelon test-harness cost is
    low)* `wendelon_sql_delete_hides_rows_from_future_scan`
    — live Wendelon node; CREATE + INSERT +
    `DELETE FROM t WHERE col > 0` via SQL;
    assert deletion visible in subsequent
    SELECT.

## SoA-readiness check

ADR 021's four SoA-readiness invariants preserved:

1. **Catalog stays pure metadata for kernels.**
   ADR 036 doesn't touch the catalog API.
2. **`#[repr(u8)]` on `StableAggregateKernel`
   unchanged.** No aggregate kernel touched.
3. **Centralized dispatch unchanged.** DELETE
   adds one coordinator routing path; no new
   dispatch site for kernels.
4. **No row-path dyn dispatch.** Coordinator
   helper operations are arrow column-wise
   filter + identity extraction; no `dyn` added
   on the row path. The new
   `filter_batches_with_bound_predicate` helper
   is a method, not a trait object.

## Brainstorm-seam touch check

- **Brainstorm 001 (Cranelift JIT).** Untouched.
- **Brainstorm 002 (WeakControlRing).** Untouched.
- **Brainstorm 003 (Disaggregated Compute).**
  Touched but preserved — adds one distributed
  DELETE reject site (D10), matching ADR 030 /
  032 / 033 / 034 pattern. No fragmentation
  work; ADR 037 cross-ring substrate engages
  this seam.
- **Brainstorm 004 (Memory Arenas).** Untouched
  — DELETE adds one leaf variant pair with no
  boxed children. Fourth consecutive composition
  cycle preserving Plan Arena as named-debt-
  not-yet-pressured.

No seam closes.

## What Not To Do

- **Don't reject non-PBC predicates** (D5 —
  load-bearing correctness invariant; accept-
  with-residual-filter matches SELECT WHERE
  discipline; Claude iter-0 C3 retracted on
  this exact point).
- **Don't skip residual filtering when
  `pbc_program.is_none()`** (D7 — silent full-
  table delete is the catastrophic R1 trap;
  guard #14 enforces residual filter ordering
  before identity extraction).
- **Don't inherit ambient `runtime_filter` from
  executor context** (D7 — DELETE has no join
  build side; ambient inheritance would silently
  narrow mutation candidates; guard #13 positive
  + negative pattern enforces).
- **Don't add `FilterDisposition` field to
  `ExecutionPlan::Delete`** (D4 — state is
  derivable from `predicate` + `pbc_program`;
  adding the field allows inconsistent states
  like `predicate: None` + `EvaluateInToron`).
- **Don't wrap DELETE in the descriptor-evolution
  retry loop** (D7 — mutations must not retry
  after partial success; mutation idempotency is
  a future ADR).
- **Don't accept `USING` / `RETURNING` / `ORDER
  BY` / `LIMIT` / multi-target / alias in v1**
  (D1 — each requires substrate work; defer to
  future ADRs).
- **Don't accept non-table table factors** (D1 —
  table-function / derived / function / unnest /
  nested-join all rejected).
- **Don't use `lower_ddl_table_name` for DELETE**
  (D3 — DDL wording in DML diagnostics is
  confusing; factor neutral DML helper).
- **Don't lower no-WHERE DELETE as `WHERE true`
  literal** (D1 — IR already has `Option<Expr>`;
  use `None`).
- **Don't fork sqlparser destructure with `..`**
  (D2 — exhaustive destructure mirrors ADR 029 /
  033 / 034 discipline; sqlparser upgrades fail
  fast).
- **Don't expose `requested` / `already_deleted`
  / `not_found` counters through SQL** (D9 —
  only `deleted` maps to SQL row count; other
  counters remain storage accounting).
- **Don't add a generic `mutation_count_batch` to
  `coordinator/write.rs`** (D8 — `delete_count_batch`
  lives in `coordinator/delete.rs` since DELETE
  is the only DML form returning a row-count
  batch in v1; future RETURNING ADR can refactor).
- **Don't move identity extraction into storage**
  (D8 — `extract_delete_row_key_ids` stays in
  the coordinator boundary; storage trait
  remains identity-typed).
- **Don't break ADR 035 helper names** (D8 —
  `materialize_delete_row_key_ids` and
  `execute_delete_with_candidates` are load-
  bearing; preserve through Phase 3 split).
- **Don't break `ExecutionPlan::Filter` SELECT
  WHERE behavior in Phase 4 helper extraction**
  (D6 — byte-identical refactor invariant
  applies; reference fixture pass-count must
  match pre-refactor).
- **Don't add `truncate_table` / new
  `StorageWrite` overload / new
  `StorageDeleteRows` method / new `StorageError`
  variant** (D14 / out-of-scope).
- **Don't add Wendelon-side change** (D14 /
  out-of-scope — ADR 035 substrate covers
  Wendelon side already).
- **Don't add `blake3`** (out-of-scope; ADR 030
  D7 + ADR 031 + ADR 032 + ADR 033 + ADR 034 +
  ADR 035 carry-forward invariant).
- **Don't add external allocator crates**
  (`bumpalo` / `typed-arena` / `slab` /
  `generational-arena` — out-of-scope; fourth
  composition cycle preserving Plan Arena as
  named debt).
- **Don't bundle DELETE coverage in
  `delete_substrate_test.rs`** (A3 — new file
  `crates/toron/tests/delete_test.rs` matches
  ADR 034 `drop_table_test.rs` precedent; one
  statement form, one test file).

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | **Non-PBC predicate degrades to full-table DELETE** (the load-bearing correctness trap Codex iter-0 surfaced) | D5 accept-with-residual-filter + D7 residual filter ordering + guard #14 + fixture #8 `amount + 1 > 10` |
| R2 | Ambient `runtime_filter` inherits from executor context and silently narrows mutation candidates | D7 `runtime_filter: None` + guard #13 positive + negative pattern |
| R3 | Identity column leaks into SQL result paths | D8 + D12 + guards #17 + #22 |
| R4 | `ExecutionPlan::Delete` `FilterDisposition` field drift | D4 + guard #9 (no `FilterDisposition` literal) |
| R5 | Mutation retry after partial delete | D7 no retry loop; route outside SELECT descriptor-evolution retry |
| R6 | ADR 035 helper name churn breaks substrate tests | D8 wrapper-name preservation + guard #15 |
| R7 | `ExecutionPlan::Filter` SELECT WHERE behavior changes during Phase 4 helper extraction | D6 byte-identical refactor invariant + guard #23 + reference fixture pass-count check |
| R8 | Boolean-root validation duplicates inline logic | D5 + Phase 2 `require_single_boolean_root` + guard #10 |
| R9 | Premature generic DML row-count abstraction | D8 `delete_count_batch` location in `coordinator/delete.rs`; defer generalization to RETURNING ADR |
| R10 | sqlparser `Delete` or `TableFactor::Table` shape change accepted silently | D2 + guards #2 + #3 enforce exhaustive destructure with no `..` |
| R11 | DELETE coordinator inherits descriptor-evolution retry behavior | D7 explicit "bind once, execute once" |
| R12 | `pbc_program: None` accidentally treated as full-table delete | D4 derivable-state table; guard #14 residual-filter ordering |
| R13 | `lower_ddl_table_name` reused for DELETE diagnostics | D3 neutral helper + diagnostic context parameter |
| R14 | Plan Arena pressure increases | DELETE adds 1 leaf variant pair, no boxed children. Pressure essentially unchanged. |
| R15 | UPDATE / RETURNING / cross-ring distributed-write smuggled into this ADR | D14 sequencing pin + D1 out-of-scope explicit |

## Recorded dissent

**None at iter-2.**

Iter-0 Claude C3 (PBC-only predicates → reject
non-PBC at bind) was retracted at iter-1 after
Codex iter-0 R1 surfaced the correctness trap.
Retraction was clean — non-PBC predicates are
accepted; residual filtering applies. Second
substantive Claude retraction in council history
(first was ADR 033 four-default retraction).
Pattern: scope-trimming defaults benefit from
Codex iter-0 code-evidence audit pressure on
correctness boundaries.

Iter-0 Gemini (side-channel result counters +
thin predicate IR) was retracted at iter-1
cleanly per established retraction discipline.
Gemini iter-2 fully convergent with zero
dissent.

## Process observations

### Fourth 3-iter / 0-flip-flop composition cycle

| ADR | Shape | Iters | Flip-flops | Notes |
|---|---|---|---|---|
| ADR 032 | Composition (pure local INSERT SELECT) | 3 | 0 | First composition data point |
| ADR 033 | Composition (pure local CTAS; four Claude defaults retracted) | 3 | 0 | First substantive Claude retraction |
| ADR 034 | Composition (pure local DROP TABLE; Gemini DELETE retracted) | 3 | 0 | First Gemini topic-split retraction |
| **ADR 036** | **Composition (SQL DELETE WHERE; Claude C3 PBC-only retracted)** | **3** | **0** | **Second substantive Claude retraction; load-bearing residual-filter correctness invariant** |

Four data points. The composition-cycle floor is
firmly established at 3 iters / 0 flip-flops.

### Ninth consecutive Codex correctness-clarification cycle

Codex iter-0 R1 (residual predicates are the
correctness trap) was the load-bearing audit
this cycle. Pattern firmly established across 12
cycles (ADR 024 / 025 / 026 / 028 / 029 / 030 /
031 / 032 / 033 / 034 / 035 / 036).

### Second substantive Claude iter-0 retraction cycle

First was ADR 033 four-default retraction
(D3 column rename list, D5 all-nullable target,
D7 atomicity, D8 empty-source no-write). Now
ADR 036 C3 PBC-only.

Pattern: scope-trimming defaults at the
correctness boundary benefit from Codex iter-0
code-evidence audit pressure. "Defer to future
ADR" is sometimes substrate-replacement masquerading
as scope-trim.

### Third independent three-way iter-0 topic convergence

| Cycle | Driver |
|---|---|
| ADR 033 | Pure-Toron + pure-local + ADR 029 + 030 + 032 helpers directly reused |
| ADR 035 | ADR 034 D15 firm sequencing commitment |
| **ADR 036** | **ADR 035 D15 firm sequencing commitment** |

Pattern: when prior ADR sequencing pin commits
the next topic, three-way iter-0 convergence
becomes default.

### Twelve Claude iter-0/1 contributions promoted

| Claude contribution | Promoted to |
|---|---|
| iter-0 C1 accept no-WHERE DELETE | D1 (lowered as `predicate: None`) |
| iter-0 C2 Shape A pre-resolved identity | D4 |
| ~~iter-0 C3 PBC-only predicates~~ | **RETRACTED** at iter-1 |
| iter-0 C4 bare `"delete"` telemetry label | D11 |
| iter-0 C5 new test file `delete_test.rs` | Required fixtures section |
| iter-0 C6 new narrow guard for Append site | Guards #12 + #13 |
| iter-0 C7 composition cycle properties at 4 data points | Process observations |
| iter-0 C8 ADR 037 Path C cross-ring distributed-write-context | D14 |
| iter-0 C9 3-iter / 0-flip-flop prediction | Process observations |
| iter-1 C10 byte-identical refactor invariant | D6 + guard #23 |
| iter-1 C11 `runtime_filter: None` positive + negative pattern | Guard #13 |
| iter-1 C12 Boolean-root validation helper reuse audit | D5 + guard #10 |
| iter-1 C13 `delete_count_batch` location in `coordinator/delete.rs` | D8 |
| iter-1 C14 architecture-guard naming discipline lock | Cycle property |

12-of-14 Claude contribution promotion rate
(iter-0 C3 retracted; C7 + C9 + C14 are cycle
properties, not D-level).

### Composition-cycle property catalog after four cycles

After ADR 032 + 033 + 034 + 036:

1. **Composition cycles are zero-substrate-
   invariant-break by construction.** (4 data
   points)
2. **Composition cycles converge in 3 iterations.**
   (4 data points)
3. **Composition cycles preserve Plan Arena as
   named-debt.** (4 data points)
4. **Broader-scope framings are substrate-cycle
   phenomena, not universal.** (Reinforced by
   ADR 036 absence of broader-scope framings
   despite the Codex R1 audit being the load-
   bearing iter-0 finding)
5. **Topic-concession-seed pattern returns when
   a cycle explicitly defers substrate work to
   the next cycle.** (ADR 036 D14 → ADR 037
   seed; pattern data point at 5-of-7)
6. **Scope-trimming defaults at correctness
   boundaries benefit from Codex iter-0 audit
   pressure.** (ADR 033 four-default retraction
   + ADR 036 C3 retraction; pattern data point
   at 2 cycles)
7. **Composition cycles add 1-2 leaf IR variants
   without boxed children.** (ADR 032: 1 pair;
   ADR 033: 1 pair; ADR 034: 1 pair; ADR 036:
   1 pair — pattern consolidating at 4 data
   points)

## Future work

ADR 037+ candidates carry forward the ADR 029 →
036 arc:

- **ADR 037 — cross-ring distributed-write-
  context substrate** (firm commitment per D14).
  Lifts ADR 030 D13 + ADR 032 D1 + ADR 033 D11 +
  ADR 034 D12 + ADR 036 D10 distributed-write
  rejects + ADR 034 D7
  `WendelonCatalog::drop_table` `AdminUnavailable`
  simultaneously.
- **UPDATE WHERE** — composition over ADR 035
  row-identity substrate + new partial-write
  contract. Likely a two-ADR sequence: partial-
  write substrate cycle + SQL UPDATE composition.
- **`DELETE ... RETURNING` / `INSERT ...
  RETURNING` / `CTAS ... RETURNING`** — DML
  result-shape extension. Future RETURNING
  substrate ADR + SQL composition.
- **`CREATE OR REPLACE TABLE`** — lower to `DROP
  IF EXISTS x; CREATE TABLE x AS SELECT ...`.
  Composition over ADR 029 / 033 / 034 / 036.
- **Cross-ring metadata-write contract ADR.**
  Lifts `WendelonCatalog::create_table` /
  `WendelonCatalog::drop_table`
  `AdminUnavailable`; engages transactional CTAS
  + atomic DROP.
- **Streaming / multi-batch
  `StorageWrite::write` /
  `StorageDeleteRows::delete_rows`** — lifts
  ADR 032 D5 + D6 + ADR 033 D8 + ADR 035 single-
  batch cap for bulk loads / deletes.
- **`ON CONFLICT` / `UPSERT` / `MERGE`** — DML
  extensions; need PK / unique-constraint
  substrate first.
- **Subqueries in WHERE clause** (e.g. `DELETE
  FROM t WHERE id IN (SELECT ...)`). Future
  correlated-subquery substrate ADR.
- **`TRUNCATE` / table aliases in DELETE / `USING`
  clause / `RETURNING` in DELETE / `ORDER BY` +
  `LIMIT` in DELETE** — deferred per D1 out-of-
  scope.
- **Storage reclaim ADR.** Physical file
  deletion, in-memory storage map cleanup,
  Wendelon descriptor deletion, tombstone
  collection.
- **Stale-bound-plan invalidation ADR.**
- **Read-your-writes session HLC advancement
  ADR.**
- **Per-statement durability override.**
- **Mutation idempotency / write-retry-after-
  descriptor-evolution ADR.**
- **Typed expression tape / opcode interpreter.**
- **Plan Arena / index-based plan IR.**
- **CSE + literal deduplication.**
- **Cranelift JIT compilation.**
- **Logical `Expr` arena migration.**
- **PBC partial-pushdown of computed-arg
  aggregates.**
- **Temporal types (DATE, TIMESTAMP).**
- **DECIMAL.**
- **Utf8 ↔ numeric CAST + TRY_CAST.**
- **Boolean ↔ numeric CAST + TRY_CAST.**
- **`ALTER TABLE` / `CREATE INDEX` / `CREATE
  VIEW` / `DROP VIEW` / `DROP SCHEMA` / `DROP
  INDEX`** — DDL siblings using the renamed
  `lower_ddl_table_name` helper pattern.
- **Constraints** (PRIMARY KEY / FOREIGN KEY /
  CHECK / column-level UNIQUE) / DEFAULT /
  GENERATED.
- **Wendelon AGG consumer wiring.**
- **Kernel graduations.**

## Definition of Done

- [ ] `LogicalPlan::Delete { table, predicate }`
      defined (14 → 15 variant count).
- [ ] `ExecutionPlan::Delete` defined with Shape A
      identity + `predicate: Option<BoundExprProgram>` +
      `pbc_program: Option<PbcProgram>` + no
      `FilterDisposition` field + no boxed children
      (12 → 13 variant count).
- [ ] `SqlPlanner::plan` dispatches
      `Statement::Delete` to `lower_delete`;
      supported-statement diagnostic includes
      `DELETE`.
- [ ] `lower_delete` exhaustively destructures
      `Delete` (all 7 fields) and `TableFactor::Table`
      (all 9 fields) with no `..` rest pattern;
      rejects USING / RETURNING / ORDER BY / LIMIT
      / multi-target / alias / non-table factors /
      non-default `TableFactor::Table` options
      with stable diagnostics.
- [ ] Neutral DML table-name helper
      `lower_table_ref_object_name(name, "DELETE")`
      (or equivalent factor) added; not reusing
      `lower_ddl_table_name`.
- [ ] `Binder::bind_plan` `Delete` arm calls
      `bind_delete`; `bind_delete` resolves
      target, binds optional predicate, validates
      Boolean root via `require_single_boolean_root(&program,
      "DELETE WHERE")`, attempts `compile_pbc`,
      preserves residual on non-full-PBC, derives
      `profile_hint`.
- [ ] `bind_delete` does NOT reject when
      `compile_pbc` returns `None` (residual-filter
      path preserved).
- [ ] `Delete` arms added to `collect_qualifiers`,
      `terminal_exchange`, `optimize_for_required`.
- [ ] `coordinator/delete.rs` split into
      `materialize_delete_candidate_batches` +
      `extract_delete_row_key_ids` + wrapper
      `materialize_delete_row_key_ids` (name
      preserved); `delete_count_batch(count)` added.
- [ ] `Executor::execute_bound`
      `ExecutionPlan::Filter` arm refactored to
      call new `pub(crate)
      filter_batches_with_bound_predicate` helper
      with context `"filter predicate"`; existing
      error message `"filter predicate must return
      boolean"` preserved byte-identical.
- [ ] Local `QueryCoordinator::execute_query`
      intercepts `LogicalPlan::Delete` before
      SELECT retry loop; builds candidate
      ScanRequest with `runtime_filter: None` and
      `row_identity: RowIdentityProjection::Append`;
      applies residual filter when `pbc_program.is_none()`;
      calls Phase 3 + Phase 4 helpers; returns
      `delete_count_batch(outcome.deleted)`.
- [ ] `DistributedQueryCoordinator::execute_query`
      rejects `ExecutionPlan::Delete` before
      `fragment_plan` with stable diagnostic.
- [ ] `FragmentError::DeleteNotFragmentable`
      reject fires before `contains_join` /
      `count_exchanges`.
- [ ] Distributed / scheduler / server walkers
      treat `Delete` as leaf.
- [ ] Defensive `Executor` arms for unbound
      `LogicalPlan::Delete` + bound
      `ExecutionPlan::Delete`.
- [ ] Telemetry label `"delete"`.
- [ ] `#[allow(dead_code)]` annotations on
      `materialize_delete_row_key_ids` /
      `execute_delete_with_candidates` /
      `QueryCoordinator::deleter` field removed.
- [ ] ADR 035 guard `adr035_no_sql_delete_yet`
      removed; replaced by ADR 036 positive
      guards.
- [ ] **All 23 ADR-036-specific architecture
      guards pass.**
- [ ] **Paired adjustment to
      `architecture_guards_unchanged_plus_sort_variant`
      passes** (LogicalPlan 14 → 15;
      ExecutionPlan 12 → 13).
- [ ] **All 14 required functional fixtures
      pass.**
- [ ] (Optional) Wendelon SQL DELETE fixture
      passes if added.
- [ ] All existing CREATE TABLE / VALUES INSERT /
      INSERT SELECT / CTAS / DROP TABLE tests
      green.
- [ ] All existing SELECT-only plan-digest
      fixtures green.
- [ ] All existing SELECT WHERE fixtures green
      after Phase 4 helper extraction (byte-
      identical refactor invariant).
- [ ] ADR 029 / 030 / 031 / 032 / 033 / 034 / 035
      invariants preserved (including ADR 029
      catalog metadata-only; ADR 030 D7 Toron-side
      synthetic-column construction prohibition;
      ADR 030 D13 distributed-write reject; ADR
      032 / 033 materialization helper contracts;
      ADR 034 `Catalog::drop_table` shape; ADR
      035 substrate types / trait / helper names).
- [ ] `StorageWrite` / `StorageScan` /
      `StorageDeleteRows` trait shapes byte-
      identical pre/post-ADR-036.
- [ ] No new `StorageError` variant added.
- [ ] No new Wendelon-side change.
- [ ] No `blake3` dependency added.
- [ ] No external allocator crate added.
- [ ] No storage reclaim trait / helper
      introduced.
- [ ] Roadmap moves ADR 036 from candidate to
      promoted; advances active section to ADR 037
      (cross-ring distributed-write-context
      substrate per D14 sequencing commitment).
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.
