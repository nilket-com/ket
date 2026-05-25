# Toron Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) —
git holds the archaeology, this file looks forward.

**Update cadence.** Rewritten at planning sessions, not at
implementation commits. The council operates on a block-chain
rhythm: plan → implement → plan → implement, and this file is
a snapshot of the most recent plan. Between sessions the
roadmap lags the code on purpose — an item can read "ADR N
implementation phases pending" while several phases have
already landed on `main`. That is expected. Check `git log`
and the relevant impl plan / bench-results file for actual
status, and roll roadmap edits into the next planning session
along with the forward-sequence rewrite.

**Finding the next topic.** Each ADR carries its own §"What to
do next" (and the live `## Forward sequence` head lists the
current candidates) — that, not a per-ADR recap here, is the
next-topic mechanism. So `## Status` is a one-line index;
decision detail lives in `plans/adr/NNN_*.md`. The roadmap
stays lean (~target a few hundred lines) by holding only the
forward view + the active line, not the archaeology.

## Status

ADRs 001–089 are promoted (implemented unless a `feat` is still pending — check
`git log`). Per-ADR decisions, fixtures, and follow-ons live in
`plans/adr/NNN_*.md` + each ADR's §"What to do next"; this section is a **one-line
ledger**, not a recap. Prose is kept only for the active line (the Rune authoring
run, 079+).

### Index (ADR 001–078 — promoted + implemented)

- **001** Foundation and Engine Contract
- **002** Storage-Backed Execution Contract
- **003** Distributed Runtime and Data Movement
- **004** Distributed Inner Equi-Join
- **005** Hash-Join Build Path Efficiency
- **006** Runtime Filter Pushdown (advisory Bloom, two-wave)
- **007** Rune Sandbox Enforcement
- **008** Outer / Semi / Anti Joins
- **009** Correlated `EXISTS` / `NOT EXISTS` Decorrelation
- **010** Top-Level SQL Clause Accounting + Deterministic Result Ordering
- **011** Aggregate Projection Accounting + `GROUP BY` Reachability
- **012** Mixed-Wildcard Projection Accounting + Planner Hardening
- **013** Runtime Filter Application at the Storage-Scan Boundary
- **014** Exact Small-Set Runtime Filters
- **015** Terminal Ordering for Grouped Aggregate SQL
- **016** Post-Aggregate Filtering with `HAVING`
- **017** `ORDER BY <aggregate-call>` for Grouped Aggregate SQL
- **018** Scalar-Aggregate `HAVING` / `ORDER BY` SQL
- **019** MIN / MAX Aggregate Kernels
- **020** AVG Aggregate Kernel
- **021** Aggregate Kernel Catalog Refactor
- **022** Float MIN / MAX Aggregate Kernels
- **023** Post-Aggregate Projection
- **024** Scalar Arithmetic Expression IR
- **025** SQL `CAST(x AS T)` Syntax (numeric primitive)
- **026** `TRY_CAST` / `SAFE_CAST(x AS T)`
- **027** `ORDER BY` Computed Expressions
- **028** Computed Aggregate Arguments
- **029** SQL `CREATE TABLE` (narrow DDL surface)
- **030** SQL `INSERT INTO … VALUES (...)`
- **031** Bound Expression Program (index-based)
- **032** SQL `INSERT INTO … SELECT` (local read-write)
- **033** SQL `CREATE TABLE AS SELECT` (local DDL + populate)
- **034** SQL `DROP TABLE` (local DDL deletion)
- **035** Row Identity / Predicate-Delete substrate
- **036** SQL `DELETE WHERE`
- **037** Distributed Write Context (cross-ring)
- **038** Storage-Admin / Reclaim Substrate
- **039** Wendelon DROP Migration Substrate
- **040** Unified Nilket CLI Entrypoint
- **041** `CREATE OR REPLACE TABLE`
- **042** Toron Compute Daemon Substrate
- **043** Public Toron SQL Listener
- **044** Authenticated Remote Toron QUIC Listeners
- **045** Toron Daemon Observability Listeners
- **046** Nilket Toron Inspect Real Wiring
- **047** Shared `tls-pki` Workspace Crate
- **048** Shared `observability-http` Workspace Crate
- **049** Authenticated Tenant Routing for SQL
- **050** Per-Tenant Memory Governance for Toron SQL
- **051** Tenant-Explicit Descriptor Bootstrap
- **052** Per-Tenant Concurrency Admission + Bounded Queue
- **053** Governed Bind-Miss Descriptor Sync Consumer
- **054** Reactive Descriptor Invalidation
- **055** Bounded Fair Admission Queue
- **056** Per-Tenant Query-Rate Admission
- **057** Two-Tier SQL Connection Admission
- **058** Bounded SQL Request Ingress
- **059** Distributed Query Cancellation Completeness
- **060** Wendelon Table-Query Cancellation Consumer
- **061** CTAS `IF NOT EXISTS` (race-safe catalog create)
- **062** Primary-Key DDL for Toron-Created Tables
- **063** PK-Table `UPDATE … WHERE` via Full-Row Upsert
- **064** DML `RETURNING` Rows
- **065** `crates/observability` Namespace + Audit Substrate
- **066** Toron Audit Emission Consumer
- **067** `observability::telemetry` Substrate (OTLP)
- **068** Toron Telemetry Consumer (OTLP trace)
- **069** Toron Table-Query Trace Consumer
- **070** Toron `UPDATE` without `WHERE` (PK-table symmetry)
- **071** Worker + Observability-Access Audit Expansion
- **072** `audit.fail_closed` (high-assurance audit)
- **073** sqlparser 0.54 → 0.62 (parser-foundation refresh)
- **074** Predicate Widening: `IN` (`OP_IN_LIST`) + `BETWEEN`
- **075** Partial-Pushdown Residual Split + Literal `LIKE`/`ILIKE`
- **076** Architecture Guard Corpus Modularization
- **077** Architecture Guard Sub-Bucketing
- **078** Predicate Pushdown Boolean Distribution (safe `OR`/`NOT` factoring)

### Active line — Rune authoring run (079–086) + nominal-typing spine (087–093, detail retained)

- **ADR 079 — Rune Relation Handles (SQL as a Composable DataFrame Source).**
  Promoted (plan). A small frontend-composition seam that makes **SQL subordinate
  to Rune composition**: `ctx.sql("<query>")` parses one SQL query, lowers it to a
  lazy `LogicalPlan`, wraps it as a `RuneDataFrame`, and lets the **existing**
  `.filter`/`.aggregate` Rune methods add more IR above it — one composed plan
  through the normal Toron binder/pushdown/placement/execution (no Rune-local
  optimizer/executor). **Doctrine:** SQL = relation-producing frontend, Rune =
  composition frontend, Toron IR = shared substrate (future SQL work improves the
  substrate, not SQL-as-top-authoring-model). Primary new surface: `ctx.sql` (+
  `plan_relation`) and a `sum` helper; implementation also repaired the existing
  aggregate-only grouped SQL projection shape (`SELECT COUNT(*) ... GROUP BY key`)
  that the binding gate exposed. **Two grounded guardrails:** (1)
  **explicit multi-statement rejection** (`SqlPlanner::plan` parses all statements
  but lowers only `ast[0]`, so `ctx.sql` enforces `ast.len()==1` — else a
  `SELECT…; DROP…` silently drops the DROP); (2) **DDL/DML rejected** (no side
  effects in lazy graphs). **Proof = binding, not construction:** the gate fixture
  binds `aggregate([],[sum(col("cnt"))])` over a SQL `… COUNT(1) AS cnt … GROUP BY`
  — resolving `col("cnt")` via **schema continuation** across the boundary (the SQL
  `Project` output names; a well-exercised path). ADR-007 sandbox intact; no
  execution action (`execute_script` still returns a plan; evidence may execute via
  an existing harness); no worker-side Rune; no `FROM`-subquery / `syn` / new dep.
  Final D-set: **13 decisions, 14 guards, 8 fixtures.** Council: **genuine 3-of-3**,
  no fork (Claude identified the binding-proof hinge; Codex grounded the
  multi-statement catch; Claude softened the nested-shape risk to schema-
  continuation; Gemini converged the same 13-decision shape). Placement: Toron
  frontend composition / Rune authoring. Migration:
  `plans/impl/079_rune_relation_handles.md`.
- **ADR 080 — Rune Named Projection Continuation.** Promoted. Makes schema names
  first-class at the Rune composition layer: a `RuneNamedExpr` carrier returned by
  `RuneExpr::named`, a `RuneDataFrame::select` lowering to **existing**
  `Project.output_names`, and the stable aggregate helper pack
  (`count_all`/`count`/`avg`/`min`/`max`). Proven by a multi-hop `ctx.sql`
  continuation that **binds and executes** (`SELECT … WHERE status='open'` → Rune
  `.filter`/`.select`/`.aggregate`/`.select`/`.filter`) to a single row
  `bucket="east", gross=30, n=1` — schema continuation across four rebinding
  boundaries. Council: **genuine 3-of-3**; the iter-1 alias-on-`RuneExpr` fork
  resolved to the `RuneNamedExpr` carrier (existing `RuneExpr→RuneAggr` idiom +
  type safety + parallel-vector invariant by construction). D10 duplicate-name
  `AmbiguousColumn` grounded by the existing `group_by_test` ambiguity test.
  Production change localized to `rune_engine.rs`; **no IR variant**, no
  dependency/wire/worker-Rune/SQL-expansion/`syn`. **15 decisions, 15 guards, 8
  fixtures.** Migration: `plans/impl/080_rune_named_projection_continuation.md`.
- **ADR 081 — Rune Surface Naming Cleanup.** Promoted. Behavior-zero follow-on to
  ADR 080: renames the authored named-projection surface `.select(...)` →
  **`.project_named(...)`** and **deletes** the ADR 080 keyword shim (the
  `normalize_rune_named_select_surface` source rewriter + scanner helpers + unit test
  + dual registration) — `select` is a Rune 0.14 keyword. Supersedes ADR 080's
  `.select` spelling (no compatibility alias). **Doctrine:** Rune authoring method
  names must not collide with Rune keywords, and the frontend never rewrites user
  source. Two 080 guards reconciled (discipline #4) + two durable guards
  (`adr081_rune_frontend_does_not_rewrite_user_source`,
  `adr081_authored_rune_methods_avoid_reserved_keywords`). Gate respelled, identical
  result (`bucket="east", gross=30, n=1`). ~95 lines deleted, no production
  machinery added.
  Genuine **3-of-3** (minor fixture-naming + guard-granularity divergences resolved
  2-of-3, Codex+Claude). Originating from the ADR 080 review Finding #2. Migration:
  `plans/impl/081_rune_surface_naming.md`.
- **ADR 082 — Rune Typed Literals.** Promoted. Adds typed literal constructors
  `lit_str`/`lit_f64`/`lit_bool`/`lit_i64` (free functions lowering to existing
  `Expr::Literal(ScalarValue::{Utf8,Float64,Boolean,Int64})`); keeps `lit(i32)`. No
  operator expansion (deferred to ADR 083), no IR/binder/executor change — production
  localized to `rune_engine.rs`. Unblocks same-type Rune filters
  (`col("status").eq(lit_str("open"))`). **Grounded:** `coerce_literal_value` has no
  `Float64->Int32` arm, so all `lit_f64`/`lit_bool` fixtures are same-type (test-only
  `price: Float64` + `active: Boolean` columns); `lit_i64` included because
  `sales.id` is `Int64` and `lit(i32)` truncates wide integers. 4 same-type behavior
  fixtures (ids `[1,2,4,5,6]`/`[2,4]`/`[1,3,4,6]`/`[4]`) + a wide-int (`3000000000`)
  plan fixture; 5 `adr082_*` guards. Genuine 3-of-3. Migration:
  `plans/impl/082_rune_typed_literals.md`.
- **ADR 083 — Rune Predicate Operator Surface.** Promoted. Completes the Rune filter
  authoring surface: 9 `RuneExpr` methods — comparison parity (`.lt`/`.lte`/`.gte`/
  `.ne`), boolean combinators (`.and`/`.or`/**`.logical_not`**), null predicates
  (`.is_null`/`.is_not_null`) — each lowering to an existing `Expr` variant. No
  IR/binder/executor change; `rune_engine.rs`-localized. `.logical_not` (not `.not`)
  because `not` is a Rune keyword (ADR 081 doctrine). `is_pbc_pushdown_eligible` is
  true for the whole predicate set, so Rune predicates keep the fully-pushed scan
  path; arithmetic is **deferred to ADR 084** (pushdown-ineligible, residual/computed
  proof). Reconciles the 082 operator-scope guard (discipline #4). Gate: a compound
  3VL filter executes to ids `[1,2,3]` (incl. `NULL OR TRUE`) and the same predicate
  binds `FullyPushed` with scan PBC. Genuine 3-of-3. Migration:
  `plans/impl/083_rune_predicate_operator_surface.md`.
- **ADR 084 — Rune Arithmetic Expressions.** Promoted. Adds `.add`/`.sub`/`.mul`/
  `.div` `RuneExpr` methods → existing `Expr::Add/Sub/Mul/Div` (ADR 024 substrate),
  closing the basic Rune numeric-expression gap (literals + predicates + arithmetic).
  No IR/binder/executor change; `rune_engine.rs`-localized. **Texture change:**
  arithmetic is PBC-ineligible, so the proof is computed projection + **residual**
  filter (`EvaluateInToron`, not pushed) — the inverse of 083's gate. Inherits
  ADR-024 semantics: overflow → `Kernel` error, int div-by-zero → NULL, truncating
  int division, Int+Float→Float64. Reconciles the scope guard (discipline #4:
  `add/sub/mul/div` → required). Cast/TryCast deferred (ADR 085+). 5 gates: computed
  projection, residual predicate (ids `[2,4]`), aggregate-arg sum `71`, div-by-zero
  NULL, overflow `Kernel`. Genuine 3-of-3 (Gemini's iter-0 overflow-→-NULL slip
  corrected + pinned). Migration: `plans/impl/084_rune_arithmetic_expressions.md`.
- **ADR 085 — Rune Cast / TryCast.** Promoted. Explicit type control: `.cast(type)` /
  `.try_cast(type)` `RuneExpr` methods → existing `Expr::Cast/TryCast` (ADR 024
  substrate). Target type is a **typed carrier** (`RuneDataType` + `type_i32/i64/f32/
  f64()`), not a string — compile-time-safe, guardable, no parser (the ADR 080 carrier
  doctrine; `time_bucket`'s silent-zero string API is the counter-example).
  Numeric-only `{Int32,Int64,Float32,Float64}`; no `safe_cast`. Semantics inherited
  from ADR 024: `cast` strict → `Kernel "cast failed"`, `try_cast` → NULL.
  PBC-ineligible → residual (like 084). **Reconciles two stale guards (discipline
  #4):** the 082 scope guard *and* the 084 registration guard, both of which forbade
  `cast`/`try_cast`. `rune_engine.rs`-localized. Genuine 3-of-3 (Gemini conceded its
  string proposal to the carrier; Codex caught the 2nd guard). Migration:
  `plans/impl/085_rune_cast_try_cast.md`.
- **ADR 086 — Rune InList Membership.** Promoted. `.in_list`/`.not_in_list`
  (`VmResult`-returning `RuneExpr` methods over `Vec<RuneExpr>`) → existing
  `Expr::InList`; native `negated: true`. **First post-cast cut that pushes** —
  `FullyPushed` + `OP_IN_LIST` (negated → `+OP_NOT`) when target+members are
  PBC-eligible; computed target or members → residual `EvaluateInToron`. Re-validates
  the ADR 074 frontend bound **in Rune** (non-empty + <=32, SQL-parity messages) because the
  planner's `lower_in_list` is bypassed; a guard pins `RUNE_MAX_IN_LIST_ITEMS ==
  MAX_IN_LIST_ITEMS`. Inherits binder coercion + type-mismatch + 3VL (NULL under
  `NOT IN` excluded). Reconciles the 082 scope guard (discipline #4).
  `rune_engine.rs`-localized; no IR/binder/executor/PBC change. Genuine 3-of-3 (Codex
  caught the planner-bypass; Claude conceded + adopted Codex's type-correct fixture +
  Gemini's split parity guard). Migration: `plans/impl/086_rune_in_list.md`.

- **ADR 087 — Nominal Type Identity Spine.** Promoted. **Substrate leap** (first IR +
  binder cut since the Rune run), piloted via Rune. **Doctrine: a database
  `PhantomData<T>`** — logical identity is a sidecar over physical Arrow layout. A
  `LogicalTypeId(u32)` (NAKED=0) sidecar travels beside physical `DataType` in
  `BoundExprProgram.node_logical_types`; `Expr::LogicalAscription` ascribes identity at
  **bind time only** (no `BoundExprNode`, no executor branch, no PBC opcode — it
  disappears into metadata); the **binder becomes the type checker**, rejecting
  physically-equal-but-logically-distinct operands in comparison / `IN` / equi-join via
  **one shared `nominal_compatible` helper** (no policy drift). **Strict rule:** non-naked
  vs naked-**literal** OK (`customer_id = 7` viable); non-naked vs naked-**non-literal**
  → `NominalTypeMismatch` (one-side-cast bypass blocked — the escape hatch is
  both-sides-explicit). `cast(type_i64())` erases the marker (naked); **implicit literal
  coercion preserves the node's existing identity** (Codex iter-3 subtlety). Pilot
  surface `col("id").as_logical_type::<CustomerId>()` via `rune::Params` host markers
  (no string/parser — 081/085 doctrine); static `CustomerId(1)`/`ProductId(2)` registry.
  **The `node_logical_types` sidecar ripple is the implementation bulk** — an arity
  invariant (`NodeLogicalTypeArityMismatch`) preserved through build/empty/clone/
  project/rewrite/merge is the safety net. **No Arrow-`DataType` replacement, no Wendelon
  wire, no SQL DDL, no executor/PBC change**; one nuance — the Toron-internal
  coordinator→worker plan payload gains a field (same-version Toron assumed; `serde(default)`
  can't rescue mixed versions). Behavior-additive: every all-naked query unchanged. 12
  decisions, 10 guards, 13 fixtures. Genuine 3-of-3 (Gemini conceded full-`DataType`
  replacement → sidecar + one-side-cast → strict rule; Claude verified the
  serialized-payload caveat; Codex added literal-preservation). Migration:
  `plans/impl/087_nominal_type_identity_spine.md`.

- **ADR 088 — Catalog-Carried Logical Column Identity.** Promoted. Direct successor to
  087: moves nominal identity from a query-authoring flourish to a **database contract**.
  **Doctrine: the catalog is the source of truth** — a column declares its `LogicalTypeId`
  in a reserved Arrow field-metadata key (`nilket.toron.logical_type_id`, decimal `u32`;
  missing or `"0"` → NAKED), and the binder protects *unannotated* queries by default
  (`orders.customer_id = products.id` rejects with no `.as_logical_type` in sight).
  **Carrier (Claude's iter-0 catch): metadata-resident** — read at `resolve_column`, no
  `TableBinding` parallel vector, no new arity invariant; grounded that field metadata
  rides intact through scan-pruning (`scan_output_schema`/`project_schema`) and joins
  (`join_schema`) to the binder, dropped only at `schema_for_exprs` (the `Field::new`
  rebuild = the 089 seam). `ResolvedColumn` **stays physical-only** (Codex's catch — it
  serializes inside `BoundExprNode::Column`; a logical field there would duplicate 087's
  `node_logical_types`); a binder-local `ResolvedLogicalColumn` carries identity during
  binding and the sidecar stays the sole carrier. **Decode fails closed** at
  `from_descriptor` (unknown id / non-decimal / physical mismatch → bounded catalog
  error). **One shared `reconcile_identity` helper** (Claude's iter-1 mechanism) enforces
  *add / restate / not-contradict* across both the expr path (`bind_logical_ascription`
  flips from set to reconcile-then-set, since the inner column now arrives stamped) and
  the join path (dual-source: explicit peel + catalog decode); cast-erase
  (`cast(type_i64())`) is the reinterpret escape. Reuses 087's `nominal_compatible`
  unchanged; no name heuristics; no propagation writer (089). Toron-only (`ir.rs`,
  `catalog.rs`, `binder.rs`); **no Wendelon, SQL DDL, executor/PBC, or Rune-surface
  change**; behavior-additive (no-key descriptors all-naked). 13 decisions, 11 guards, 10
  fixtures. Genuine 3-of-3 (Claude grounded the carrier + reconcile mechanism + scan-leaf
  survival; Codex caught the `ResolvedColumn` serialization; Gemini framed the
  catalog-as-source-of-truth doctrine). Migration:
  `plans/impl/088_catalog_carried_logical_column_identity.md`.

- **ADR 089 — Projection / CTAS Logical Identity Propagation.** Promoted. Direct successor
  to 088: closes the identity loop so nominal protection survives **multi-hop pipelines**.
  **Doctrine: 088 and 089 are inverse halves of one isomorphism** — identity has two
  encodings (the 087 `node_logical_types` sidecar and Arrow field metadata), and the binder
  converts between them at layer boundaries: 088 decodes *in* at `resolve_column`, 089 emits
  *out* at `schema_for_exprs` so the next hop reads it back (`SELECT customer_id AS cid` then
  a downstream `cid = product_id` rejects). The cut is a **schema-construction writer, not
  lineage inference**: one central `field_with_logical_type_metadata` (the inverse of 088's
  decoder, the *only* production emitter of the reserved key) driven by the bound **root
  sidecar** (`root_logical_type`). Computed stays naked automatically (a computed root's
  sidecar is NAKED); explicit ascription is the only computed escape. Writer call sites are
  the `Field::new` rebuilds 088 stopped at — `schema_for_exprs` (D3) + named/pruned
  projection rebuilds (D4/D5) + `infer_ctas_schema` decode-then-reemit (D7); **aggregate
  group-key preservation is automatic** (D6 — `aggregate_schema` builds groups via
  `schema_for_exprs`, `aggregate_field` untouched → outputs naked). **Round-trip invariant:**
  every emitted field
  decodes cleanly via 088 (a non-naked sidecar's physical always matches the registry) → 089
  never creates a mid-pipeline fail-closed. **CTAS `IF NOT EXISTS` keeps exact logical-schema
  equality** (Claude conceded the reconcile route — `coordinator.rs:1106` unchanged; a naked
  existing target vs an identity-bearing CTAS → `SchemaMismatch`), so idempotency depends on
  persist→decode round-trip fidelity (pinned by an idempotent-re-issue fixture). Toron-only
  (`ir.rs`, `ir/execution.rs`, `binder.rs`); **no coordinator/Wendelon/executor/PBC/SQL-DDL/
  Rune change**; behavior-additive (naked roots emit nothing). Discipline #4: 088's
  `no-new-writer` guard inverts to `adr089_logical_metadata_writer_is_centralized`
  (production-scoped; fixtures may author metadata). 12 decisions, 8 guards, 15 fixtures.
  Genuine 3-of-3 (Claude framed the isomorphism + round-trip invariant + flagged the CTAS
  equality interaction, then conceded reconcile → exact-equality and added the
  idempotent-re-issue fixture; Codex pinned exact-equality + scoped the writer guard; Gemini
  adopted the doctrine). Migration:
  `plans/impl/089_projection_ctas_logical_identity_propagation.md`.

- **ADR 090 — Persisted Logical Type Definition Substrate.** Promoted. **The arc's first
  cross-ring ADR** — a deliberate, bounded exception for one foundational concern: *durable
  type receipts*. **Doctrine: definition before use; magic enforces only with a receipt.** A
  `CAST(x AS CustomerId)` / `customer_id CustomerId` is a *use-site* presupposing a
  *definition* — and 088 persists a bare `LogicalTypeId` into descriptor metadata, so the
  definition must be exactly as durable as the descriptor. **Invariant: a descriptor may
  reference only a logical type id whose definition is durably stored in the same metadata
  authority.** Grounded: Toron holds no durable state (the catalog is a synced view of
  Wendelon descriptors), so the registry must live in Wendelon — the cross-ring exception,
  but **bounded** (a new `SchemaObjectKind::LogicalType` riding the existing schema-intent /
  `Create` / `SchemaGeneration` / sync machinery, `TenantId`-scoped). **Authority vs
  representation** (Codex's split): authority = Wendelon schema metadata (allocates the id,
  persists `{id, name, physical}`, exposes via admin/sync — never the query path);
  representation = flexible, Wendelon-**internal** (a catalog table is fine); rejected = a
  Toron-managed *ordinary* table read at bind. **Awareness (where-to-enforce, not capability):**
  090 enforces the reference invariant Toron-side (write refuses undefined ids in 091, read =
  decode fail-closed) because that's where the type-checker lives + zero new Wendelon
  descriptor-parsing — not because Wendelon is "dumb" (it already runs KLL/ApproxQuantile
  compute); the honest axis is data-plane compute vs interpreting the Toron `logical_type_id`
  metadata key (088 stores it opaquely). Codex's validate-at-authority (Wendelon enforces
  referential integrity) is a legitimate open 091 enhancement, not required by 090. Toron surface: `CREATE LOGICAL
  TYPE <Name> AS <primitive>` (a SQL extension — sqlparser lacks `CREATE DOMAIN`; `CreateType`
  is the wrong shape) → `StorageAdmin::create_logical_type` RPC; `WendelonCatalog` syncs a
  per-tenant `LogicalTypeRegistry`; **the bulk = rewiring `logical_type_spec`/088-decode to
  resolve seed ∪ synced-registry through explicit resolver context — no process-global dynamic
  registry**. Static pilot (`CustomerId=1`/`ProductId=2`) stays a
  reserved-range Rune/test bridge, disjoint from Wendelon-allocated durable ids. **SQL
  use-sites deferred to 091** (DDL column type, `CAST` + reconcile-before-erase, CTAS
  materialization); pinned by `..._still_rejects` fixtures. No executor/PBC/Rune change. 10
  decisions, 9 guards, 12 fixtures (incl. the survives-resync durability proof + the invariant
  fail-closed guard). Genuine 3-of-3 (operator design call: definition-first / Cut B /
  Wendelon-native authority / `CREATE LOGICAL TYPE`; Claude grounded the cross-ring-but-bounded
  `SchemaObjectKind` path; Codex split authority-vs-representation; Claude held minimal Wendelon
  awareness vs validate-at-authority). Migration:
  `plans/impl/090_persisted_logical_type_definition_substrate.md`.

- **ADR 091 — SQL Logical Type Use-Sites (DDL column position).** Promoted. The first honest
  SQL use-site over the 090 substrate: `CREATE TABLE orders (customer_id CustomerId)` — Int64
  field carrying the durable id via the 089 writer, protected by the 087/088 gate, **no
  hand-built Arrow metadata**. **Back to Toron-only** (consumes 090; no Wendelon change).
  **Cut: DDL column position only** — `CAST(x AS CustomerId)` + typed CTAS are 092 (coercing-
  ascription, different machinery). **Resolution-timing shift:** `SqlPlanner::plan` is sync +
  catalog-less, so `LogicalPlan::CreateTable` carries an unresolved `CreateTableSchema`
  (`CreateTableColumnType::{Physical(DataType), LogicalName(String)}`); the binder resolves
  names at **bind** (async catalog) and emits a resolved `ExecutionPlan::CreateTable`.
  **The seed-first trap (Claude's load-bearing finding, unanimous):** `LogicalTypeResolver::
  spec_by_name` is seed-first (correct for decode), so SQL DDL must use an **async registry-only**
  lookup that bypasses the seed — else `CREATE TABLE (id CustomerId)` would resolve the static
  pilot with no `CREATE LOGICAL TYPE`, making the seed "SQL truth." Two paths: decode =
  seed∪registry; DDL name = registry-only. **Write-side fail-closed:** undefined name →
  `CatalogError::LogicalTypeNotFound` before any descriptor submits (the write-half of 090's
  co-authority invariant). **Refresh-before-unknown** via 090's `list_logical_type_definitions`
  (fresh-process usable). **Create-path self-rebind goes resolver-aware** (`create_table_with_
  outcome` rebound via seed-only `from_descriptor` at `catalog.rs:636` → `with_registry`, else a
  valid create rejects its own durable-id output). `naked`/modifiers/quoted/multi-part reject;
  no executor/PBC/Rune change; no new nominal gate. Reconcile the variant/shape guards in the
  same change (the explicit 090-review lesson — don't ship a red suite). 10 decisions, 9 guards.
  Genuine 3-of-3 (Claude seeded the DDL-only cut + grounded the seed-first trap; Codex named the
  plan-shape + create-rebind catch + bind-time; Gemini converged). Migration:
  `plans/impl/091_sql_logical_type_ddl_columns.md`.

- **ADR 092 — SQL `CAST(x AS LogicalType)` (coercing ascription).** Promoted. **Completes the
  SQL authoring surface** (define → DDL column → CAST). Toron-only. The receipt doctrine at
  expression position: `CAST(x AS CustomerId)` resolves **durable-only** (091's resolver, seed
  excluded) and is a **reconciling coerce-ascription, not a silent relabel** — binds iff `T`
  durably defined ∧ physical convertible by existing semantics ∧ `reconcile(x.identity, T)`
  doesn't conflict. `CAST(product_id AS CustomerId)` rejects; `CAST(CAST(product_id AS BIGINT)
  AS CustomerId)` is the explicit override. **Reconcile before physical coercion is the whole
  feature.** Shape (converged after a load-bearing correction): transient `Expr::SqlLogicalCast
  { expr, logical_name, try_cast }` (planner-emitted, binder-consumed — never in
  `BoundExprProgram`/exec); an **async pre-resolution pass** resolves names → a `HashMap<String,
  LogicalTypeSpec>` (durable-only, refresh-on-miss; unknown → `LogicalTypeNotFound`); the **sync
  binder** reads the resolved **spec** (the seed-only-spec catch: `validate_logical_ascription_physical`
  is seed-only, so a durable id can't validate id-only — the *spec*, not the id, must reach the
  binder), reconciles against the operand identity before coercing physical via the existing
  lattice (no new kernels; same-physical incl Utf8/Boolean ascribes with no kernel).
  **`LogicalAscription` is unchanged (no coerce flag)** — Rune + 087's cast-erase escape stay
  byte-identical (Claude conceded the iter-1 coerce-flag; Codex grounded the correction). Typed
  CTAS is free via 089; **TRY_CAST deferred** (Gemini conceded; now 3-of-3 — nullability +
  non-numeric physicals are a separate call, 093). No Wendelon/executor/PBC; no new nominal gate. Reconcile
  091's `no_cast_or_typed_ctas_authoring_yet` + the cast-shape guards in-change (full suite green).
  11 decisions, 9 guards. Genuine 3-of-3. Migration: `plans/impl/092_sql_logical_cast.md`.

- **ADR 093 — SQL `TRY_CAST(x AS LogicalType)` (nullable coercing ascription).** Promoted.
  **Completes the CAST/TRY_CAST 2×2** (026 physical CAST+TRY_CAST; 092 logical CAST; 093 logical
  TRY_CAST). Toron-only, `binder.rs`-localized — the production diff is two un-gates + one branch.
  **Doctrine: try softens the *value domain*, not the *identity* contract** — physical failure →
  NULL, but a logical mismatch is a compile-time error: `TRY_CAST(product_id AS CustomerId)` still
  **rejects at bind** (`NominalTypeMismatch`, not NULL); the explicit primitive erase
  (`TRY_CAST(CAST(product_id AS BIGINT) AS CustomerId)`) is the only override — not an
  identity-laundering bypass. **Contract γ (converged 3-of-3): categorically nullable, binds
  wherever strict CAST binds** — `reconcile_identity` runs first (088, unchanged), then on every
  bound path append the existing `BoundExprNode::TryCast` (same-physical incl. `Utf8`/`Boolean`
  served by the `evaluate_try_cast` same-type passthrough — **no new kernel, no shared-gate
  widening, no routing through the identity-free physical `bind_try_cast`**); cross-physical
  non-numeric rejects == CAST. Reuses transient `SqlLogicalCast { try_cast }` (already planner-
  plumbed) + the 092 async durable spec pre-resolution — the **two** rejection sites (the
  collection prepass + the bind gate) both un-gated; resolved **spec** to the sync binder (092
  seed-only-spec trap holds). Typed CTAS materializes a **nullable** logical column free via 089's
  writer (`root_nullable` threaded; `infer_ctas_schema` preserves `is_nullable`) + survives resync;
  `SAFE_CAST` aliases. **No** Wendelon/executor/PBC/IR/Rune change; no new nominal gate; strict CAST
  + physical `bind_try_cast` byte-identical. Invert the 092 deferral assertion (inside
  `adr092_binder_resolves_sql_logical_casts_registry_only_before_sync_bind`) + flip the reject
  fixture in-change. 11 decisions, 11-gate behavior net, 6 added guards. Genuine 3-of-3 (Claude
  grounded contract γ + corrected its cost to binder-local; Codex independently reached γ + closed
  the CTAS-nullability question; Gemini ratified). Migration:
  `plans/impl/093_sql_logical_try_cast.md`.

V1 target is **"First Query with Integrity"** plus
**"Distributed Execution Correctness"**: correct analytical
results against a Wendelon-native cluster with pushdown
maximized, identity flow real, stream-shaped storage
boundary, multi-process coordinator/worker mechanics
surviving a worker SIGKILL with byte-identical normalized
output, and the first two-input relational operator working
across hash-partitioned and bounded broadcast strategies.
Multi-tenant governance, executor model upgrade, sort-merge,
and pipelined cross-stage execution
remain post-V1.

## Prioritization philosophy

Toron is the compute ring of the Nilket stack; Wendelon is
the storage ring. We control both ends. The boundary exists
for scaling dynamics and resource isolation, not because
storage and compute are independent products.

**Minimizing re-work of the long term vision.** We favor architectural 
directions that align with our far-horizon goals, avoiding dead-ends or 
designs that necessitate significant rewrites as the system evolves.

**Toron's reason for existence is smart-storage pushdown.** Every
improvement — executor model upgrade, advanced kernels, frontend
hardening — polishes a system that does its primary job along its
primary axis.

**ADR-by-ADR cadence.** Wendelon ADR 070 pattern: narrow,
evidence-driven, single-decision contracts. Each ADR earns its
scope from the prior ADR's bench evidence or a fresh code-evidence
defect; mega-scopes get split.

**Evidence drives scope.** The most informative artifact for the
next ADR is the previous ADR's bench results — or a code-evidence
silent-drop / inert-feature defect surfaced during selection.
**Evidence Pressure > Launch Pressure** when both are live: the
council picks the topic with measured signal, not the one with
the loudest backlog.

**Scale-boundary convention (adopted iter-5 of ADR 006).** Every
ADR from ADR 006 forward includes an explicit **Scale boundary**
section after Status and before Context. The decomposition makes
scope-creep into adjacent boundaries visible at review time and
sharpens ADR selection: the same operator at different scales is
a different contract.

**Ring-product placement (convention:
`plans/conventions/ring-product-placement.md`).** Classify every
candidate on the Interior/Exterior × Substrate/Composition matrix
at topic selection, and name the quadrant in the ADR's Category
line. **Interior** products (shared substrate like `tls-pki` /
`observability-http`, or coordination glue) file in this Toron
sequence acting as the workspace aggregator; **Exterior**
ring-specific products "return home" to their own ring's ADR
sequence — even when the council ran from `crates/toron/plans/wip/`.
Placement follows code-locality + server-side parentage, not
process-locality (precedent: `nilket wendelon inspect` drafted as
Toron ADR 049, relocated to Wendelon ADR 071). **Leap Rule:** a
candidate that looks like Composition but needs new interior
mechanism (e.g. multi-tenant routing's tenant-isolation logic) is
a Substrate leap — scope the interior "How" before the
user-facing "What".

**Minimize strategic rework, not ADR scope.** When two narrow
ADR shapes are otherwise equivalent, prefer the one that
preserves plausible long-term seams the noted brainstorms
depend on (e.g., `NodeRoleSet`, ring-router asymmetry, IR-arena
compatibility, `WeakControlRing` coordinator API, JIT-friendly
expression shape) and avoids likely reversal. **Evidence still
drives scope** — brainstorms remain "noted but not committed"
and do not pull near-term scope forward. An ADR that closes off
a seam should do so explicitly and with reason, not by accident.
The selection round names the touched seams; the council
ratifies the trade-off in the open.

## Forward sequence

Earlier items have firmer scope; later items are direction,
not commitment. Selection rounds include a brainstorm-seam
check per the Prioritization philosophy — most candidates
touch no seams, but the question is asked.

### ADR 093 promoted; ADR 094 candidates

ADR 093 selected and promoted **SQL `TRY_CAST(x AS LogicalType)`** — the deferred 092 sibling that
**completes the CAST/TRY_CAST 2×2**. Nullable coercing ascription: try softens physical failure to
NULL but **not** the identity contract (`TRY_CAST(product_id AS CustomerId)` still rejects at bind);
contract γ — categorically nullable, binds wherever strict CAST binds; binder-local (two un-gates +
one branch), no new kernel / shared-gate / Wendelon change; CTAS nullable-column free via 089.
Genuine 3-of-3.

**ADR 094 candidates:**

*Identity follow-ons (from ADR 093 §"What to do next"):*

- **Refinement contracts** (the next *integrity tier*; strongest lead, now fully scoped by the
  define → DDL column → CAST → TRY_CAST authoring line) — value predicates on a durable `LogicalTypeId`
  (`PersonId(Int64, value > 0)`) → static plan elision (`WHERE id = -5` → `EmptyRelation`, zero
  I/O). **Grounded as a mega-scope that must split** (no `EmptyRelation` node, no constant-folding
  pass anywhere, no predicate field on the 090 definition + cross-ring ripple). The only non-inert
  first slice is **elision-first** (`EmptyRelation` + a contradiction simplifier, Toron-only); a
  durable-predicate-persistence cross-ring ADR follows. An explicit council split call, not one ADR.
- **`DROP`/`ALTER LOGICAL TYPE`** — net-new IR variants + parsing + cross-ring DROP with referential
  integrity (can't drop a logical type a column still references).
- **PK/FK-derived identity** — "magic that follows constraints, not names": derive from `PRIMARY
  KEY`, inherit via `REFERENCES`; blocked on FK support (rejected at `sql_planner.rs:663`) + a
  canonical `schema.table.column` identity name.
- read-only **`information_schema.logical_types`** (net-new virtual-table surface);
  aggregate-output identity semantics; the optional Wendelon-validate-at-authority (090 knob);
  **`.as_naked()`** Rune helper.

*Carried Rune leads (the nominal-typing spine was prioritized over these — still live):*

- **Rune `Like`/`ILike`** — pattern predicates over existing `Expr::Like { pattern,
  negated, case_insensitive }`; **residual** (PBC-ineligible); needs pattern (literal
  `String`, per 075), case-sensitivity (`like` vs `ilike`), `.not_like` (keyword-safe).
- **Rune joins** — needs relation-alias + qualified-column ergonomics first.
- **`SIMILAR TO`/`RLIKE`/`REGEXP`**; **`CASE`/value-conditionals**; **Rune sort/limit**;
  dot-field sugar; **Wendelon typed table-mutation trace continuation**.

*Non-Rune lead:*

- **Residual minimization for factored shapes** (078 follow-on) — minimal `(R1 ∨ R2)`
  residual in `split_boolean_tree`, gated by 078's 3VL fixture.
- Carried: subquery/`UNNEST`/row-value `IN`; `CHECK`/`DEFAULT`; `ALTER`/composite-PK/
  `UNIQUE` (cross-ring schema-V2); pre-op intent audit; fragmentable bulk DML;
  JWT/OIDC/SPIFFE; gRPC/Arrow Flight/Postgres frontends.

The migration is materialized in
`plans/impl/093_sql_logical_try_cast.md`.

### ADR 092 promoted; ADR 093 candidates (HISTORICAL)

ADR 092 promoted **SQL `CAST(x AS LogicalType)`** (reconciling coerce-ascription, durable-only,
reconcile-before-coerce; TRY_CAST deferred → became ADR 093). Detail lives in
`plans/adr/092_sql_logical_cast.md` + the active-line entry above; git holds the candidate
archaeology.

### Advanced kernel graduations *(one ADR per kernel)*

Wendelon ADR 070 narrow-decision pattern. Each promotes
under its own ADR with bench evidence. Inherited from
prior roadmap, unchanged:

- **`LinearRegress` stable contract.** Numerical hardening
  (condition-number guard, centered formula, rank check)
  + stability across numeric types.
- **`ApproxQuantile` stable contract.** Real rank-based
  estimation against KLL retained values + cross-impl
  parity with Wendelon's KLL substrate.
- **`TimeBucket` stable contract.** Bucket-alignment
  semantics across timezone, duration, and pre-epoch
  inputs.

### Brainstorms and high-level vision

The far-horizon shapes the council is most excited about.
These are speculative architectures — not committed scope —
but each opens a genuinely new axis of capability the V1
substrate is being kept *compatible with*, not closed off
from. The cycle's discipline of preserving load-bearing seams
explicitly serves these futures.

- **Brainstorm 001: Pure Cranelift JIT as a First-Class
  Query Specializer.** Compile hot kernels — predicates,
  UDFs, hash-join probe loops — directly to native machine
  code. The interpretation tax that every analytical query
  pays today disappears on the inner loops that actually
  dominate wall-clock. Bare-metal throughput on the kernels
  that matter most. The seam is kept open: no V1 contract
  bakes in interpretation-only invariants. Reserved for
  post-V1 deliberation when bench evidence quantifies the
  interpretation overhead at production scale.
- **Brainstorm 002: WeakControlRing.** An in-memory Raft
  ring for HA coordinators, cluster-wide quotas, and shuffle
  barrier synchronization — *without* the persistence tax
  of NVMe / WAL that traditional Raft pays. The insight:
  control-plane state has different durability needs than
  data-plane state; a weakened ring algebra lets coordinator
  HA scale without storage coupling. Seam: ADR 003's
  `WorkerRegistry` trait + `WorkerLease` / `LeaseEpoch`
  fields are the deliberate hooks. Promotes when V1
  production exposes coordinator-HA as a binding constraint.
- **Brainstorm 003: Disaggregated Compute Roles & Unified
  Ring Algebra.** The most ambitious of the four. Break the
  symmetric Toron/Wendelon node model into specialized
  hardware roles — stateful NVMe data nodes paired with
  elastic compute-only nodes — while preserving a single
  executable, a unified ring algebra, and the operator
  contracts that work today. Three concrete visions are
  noted: hardware-optimized asymmetry, weak/strong ring
  algebra, data-broker / ToronWorker topology. Each
  unlocks a different cost-efficiency story at scale. Seams:
  `NodeRoleSet` bitmask expansion, `RingRouter` routing
  durable writes only to `NodeRole::Data`, ADR 003's
  registry/lease surfaces (shared with Brainstorm 002). The
  cycle currently passes through these seams without closing
  them off — for example, ADR 020's "GROUP BY over joins"
  candidate is parked specifically because its fragmenter
  routing decision would prematurely commit to one of the
  three placements.
- **Brainstorm 004: Memory Arenas and Custom Allocators for
  Query Planning.** AST and query-plan churn dominate
  allocation profiles in dispatch-heavy workloads. Arena
  allocation collapses the per-node `Box`/`drop` overhead
  into a single bulk free, improving cache locality on the
  hot planner path. Three sketched approaches: external
  arena crates (`bumpalo` / `typed-arena`, lifetime
  contagion), Cranelift-style index-based pool (SOA-aligned,
  no lifetime threading), domain-specific pool. Seam: IR
  type shape — `LogicalPlan`, `ExecutionPlan`, `Expr`
  currently own children via `Box`, so deeper boxing raises
  future retrofit cost. Evidence-gated by ADR 005's
  allocator-churn note; no allocator commitment lands
  without profiling, but the IR shape is kept
  arena-compatible.

The cumulative ambition: a compute ring that JITs the inner
loops, scales coordinator state without storage coupling,
disaggregates hardware roles for cost-efficiency, and
allocates query state with cache-aware discipline — all on
the same Wendelon-native substrate V1 is hardening today.
Each brainstorm is independent; the value of keeping their
seams open compounds.

**Pushdown axis** (cumulative observation, not a
brainstorm). Toron's pushdown story now covers static
predicates (ADR 002), shuffle-key affinity (ADR 003), and
*runtime* join-key filtering (ADR 006 generation plus ADR
013 storage-scan application). Future pushdown surfaces:
aggregate partial pushdown (already half-Wendelon-bound),
top-K / sort pushdown, projection-with-computed-column
pushdown. Each is its own ADR; the cumulative shape is
"Toron binds; Wendelon executes the leaves; the wire
carries results, not bytes." ADR 006 and ADR 013 together
form the runtime-pushdown substrate that V1.x runtime-
filter extensions (Candidate C above) build on. Not
numbered as a brainstorm because the content is an
existing-trajectory observation rather than a speculative
architecture — file-backed Brainstorms 001-004 are the
speculative set.

### V1.x items *(post-V1, evidence-driven)*

Items the council has explicitly deferred from V1 ADRs.
Promoted to their own ADRs when evidence drives them.

- **Non-equi joins / theta-joins.** Own ADR.
- **Sort-merge join.** ADR 010 pins the top-level `ORDER BY`
  contract and the `Sort` operator; sort-merge join builds on
  that ordering antecedent. Still its own ADR — needs the
  join-side ordering and merge-path decisions.
- **Cross-join.** Own ADR or V1.x SQL surface.
- **Cost-based build/probe swap; cost-based strategy
  selection.** Requires Wendelon stats surface and Toron
  cost model. Will land `BuildSide` field on
  `ExecutionPlan::Join` when it engages.
- **Multi-way join planning.** Requires cost model.
- **Skew detection + mitigation.** ADR 004 ships
  observation only (soft gate). Mitigation needs heavy-
  hitter detection, salt-and-replicate, dynamic
  partition-count override. Own ADR.
- **SQL join hint syntax.** `/*+ BROADCAST(t) */` and
  similar. V1.x SQL surface.
- **`USING` clause; comma-join; `CROSS JOIN`.** V1.x SQL
  sugar.
- **Floating-point join keys.** Requires canonical-
  encoding ADR (NaN, signed zero, subnormals).
- **Intra-task build / probe pipelining.** V1.x executor
  work; depends on Candidate E (executor model upgrade)
  outcome.
- **Hash-spill-to-memory demotion.** ADR 004 ships one-way
  spill; demotion is V1.x when bench shows churn benefits.
- **SQL-surface auto-rename of duplicate output column
  names.** ADR 004 requires explicit qualification.
- **Pipelined cross-stage execution.** ADR 003 ships
  strict stage barriers; pipelining adds latency wins at
  the cost of failure-recovery complexity. V1.x when the
  failure-recovery story is concrete.
- **Locality-aware fragment routing.** ADR 003 +
  ADR 004 are location-agnostic; locality requires new
  Wendelon public-API surface.
- **RDMA / RoCEv2 / SIW transport tier.** M9 seeds the
  transport-tier-negotiation shape; QUIC preferred
  through V1.
- **Operator-internal streaming end-to-end.** ADR 003
  requires stream-shaped task output; full operator-
  internal streaming is V1.x executor work (Candidate E —
  executor model upgrade — territory).
- **Higher retry counts.** ADR 003 ships `max_attempts =
  2`; future ADR raises with evidence of which failure
  modes benefit.
- **Encrypted spill governance.** Spill lifecycle beyond
  M10 seed: key rotation, AAD policy, crash recovery,
  observability, multi-query isolation. ADR 003 +
  ADR 004 share the M10 AES-256-GCM-SIV substrate without
  key rotation.
- **Full DDL propagation.** Async invalidation broadcasts,
  multi-coordinator schema consensus, descriptor
  versioning beyond fingerprint-pin.
- **Wire-protocol cross-version compat.** ADR 003 ships
  reject-on-mismatch; rolling upgrades are V1.x.
- **Decimal numeric types.** Their own ADR; pins decimal-
  specific aggregate semantics.
- **Wendelon-side crate extraction.** `wendelon-types`
  (PBC ops, identity, fingerprint), `wendelon-net`
  (transport facade), `wendelon-schema` (descriptor
  types). Defer until multi-consumer pressure (Polariton,
  Baryon, external Toron SDK) materializes.

### V2+ items

Direction, not near-term plan.

- **Replicated / disaggregated shuffle service.** ADR 003
  per-worker partition store is the V1 substrate; a
  standalone service is V2 when evidence shows producer-
  hosted partitions are the next bottleneck.
- **Direct P2P shuffle as fast path.** V2 optimization
  over the V1 pull-addressed contract.
- **Hybrid vector search.** Vector ANN with metadata
  pushdown into Wendelon. Waits for Wendelon vector-aware
  placement.
- **Visualization-as-compute IR operators.** Density
  tiles, 2D binning, quantile-per-pixel. Waits for
  Polariton's tile protocol.
- **External federation** (Postgres / S3 / BigQuery
  adapters). Explicit ADR 001 §"Deferrals" item — "one
  external adapter as a capability proof," never on the
  Wendelon-native hot path.
- **Materialized views; incremental view maintenance.**
  Requires the executor model to support both batch and
  incremental shapes; far horizon.

### Refinement plumbing *(opportunistic, no ordering)*

Small improvements that don't fit a single ADR.

- **Capability-table aggregate dispatch** replacing the
  enum (when the aggregate catalog grows beyond hand-
  listable).
- **Newtype hygiene pass** — `Symbol` / `ColumnId` instead
  of bare `String` for column refs in narrower contexts.
- **PBC catalog growth** — additional ops promoted from
  experimental to stable.
- **ADR file renaming pass** — drop the redundant
  `toron_` prefix from `plans/adr/001-003` and
  `plans/impl/001-003` filenames (ADR 004's filename
  already follows the cleaner pattern). Mechanical sed +
  cross-reference update.

## Backlog (parked, may not return)

Ideas explored across iteration rounds in `plans/wip/`
that did not earn forward placement. Kept here as a
record of what the council considered and let go. The
per-cycle detail (the ADR 003–016 era) was pruned here for
length — git history and the iter drafts in `plans/wip/`
hold it.

## What Not To Do Next

Enduring guardrails for upcoming work. Items tied to
specific shipped ADRs live in those ADRs' §"What Not To Do"
sections; this list is the cross-cutting set.

- **Don't adapt DataFusion / Ballista.** ADR 001 rejected;
  ADR 002 / 003 reinforced. The greenfield decision is
  final unless a future ADR cites concrete evidence
  reversing the surface-area-mismatch argument.
- **Don't bundle scope across contract surfaces.**
  Distributed joins, executor model upgrade, runtime
  filter pushdown, outer joins, multi-tenant governance,
  and Rune sandbox enforcement are each their own ADR.
  ADR 003 was intentionally narrow; ADR 004 inherits the
  discipline; ADR 005 stays narrow.
- **Don't ungate experimental kernels by default.** ADR
  002 requires `PlannerConfig::allow_experimental_kernels
  = false`. Each kernel graduates under its own narrow
  ADR.
- **Don't put `Result<_, String>` in any public or module-
  crossing signature.** ADR 002 G3 enforces; ADR 003 G9
  enforces for distributed surface; ADR 004 inherits.
- **Don't skip descriptor-fingerprint validation on PBC
  pushdown.** Silently-wrong-results is the failure mode
  ADR 002 paid complexity to prevent.
- **Don't extract Wendelon types into a shared crate
  before V1.x consumer pressure.** ADR 001 / 002 / 003
  import allowlist holds.
- **Don't bypass the Wendelon import allowlist.**
  Production code consumes `wendelon::schema::{pbc, *types}`,
  `wendelon_client::*`, `wendelon::network::pool::*`,
  `wendelon::storage::file_ops::*`. Forbidden:
  `wendelon::network::raft::*`, `wendelon::network::quic::*`,
  `wendelon::storage::lsm::*`, ad-hoc page readers,
  anything inside Wendelon's node / raft / transport
  internals.
- **Don't query Wendelon for ring placement.** ADR 003
  V1 is location-agnostic; ADR 004 inherits. Locality
  routing is V1.x.
- **Don't initiate P2P-push from producer to consumer.**
  ADR 003 G11 enforces. Consumers always pull via
  `PullPartition`.
- **Don't use `tokio::time::sleep` for backpressure** in
  the production worker path. ADR 003 G10 enforces. The
  per-query budget envelope is the contract.
- **Don't dispatch `LogicalPlan` to workers.** ADR 003
  G12 enforces. Coordinator binds; workers receive
  `TaskSpec`.
- **Don't let `MorselId` cross the wire.** ADR 003 G14
  enforces. Worker-local scheduling detail only.
- **Don't introduce a new IR level below `ExecutionPlan`.**
  `TaskSpec` wraps; it doesn't transform.
- **Don't conflate task-lifecycle failures with partition-
  store wire errors.** `TaskFailureKind` is 7 variants;
  `PartitionMissing` is a wire response carrying
  `PartitionMissingReason`, not a `TaskFailed` variant.
- **Don't start Stage N+1 tasks before Stage N manifests
  are accepted.** ADR 003 G13 enforces strict barrier;
  ADR 004 G18 generalizes for multi-upstream.
- **Don't promise byte-identical Arrow output without
  canonical normalization.** ADR 003 §13 normalization
  (concat → full-row sort → reslice → digest) is the
  contract; ADR 004 reuses unchanged.
- **Don't require stable worker assignment for
  determinism.** ADR 003 output parity is the contract.
- **Don't promote a kernel without a bench-evidence
  artifact.** Wendelon ADR 070 pattern — narrow ADR +
  bench results + roadmap update, every time.
- **Don't open the next ADR with a mega-scope.** One
  contract surface per ADR; bundling defers real
  decisions.
- **Don't promote stabilization watch items into roadmap
  items.** Observability metrics, architectural-test
  guards, post-merge dashboards belong in runbooks and
  impl plans. Decision-enabling measurement belongs
  inside the owning ADR.
- **Don't introduce non-pinned join algorithms.** ADR 004
  G16 enforces. Sort-merge, nested-loop, non-equi joins,
  cross join — each is its own future ADR. Outer / semi / anti
  joins now pinned by ADR 008 (eight-variant `JoinKind`); the
  underlying hash-build / probe-right algorithm is unchanged.
- **Don't reject duplicate output column names at the
  binder.** ADR 004 architectural division puts naming
  in the SQL planner, ordinals in the bound IR.
- **Don't synthesize `Exchange` in the fragmenter.** The
  planner emits explicit `Exchange` above each join /
  aggregate input; the fragmenter never adds operators.
- **Don't allow broadcast to bypass caps.** ADR 004
  pinned three caps; over-cap fails terminal; no silent
  strategy switch.
- **Don't reserve placeholder fields, message slots, or
  PBC ops for deferred features.** Convention
  `no-premature-backward-compat.md`. The future ADR adds
  the surface when it pins the contract.
- **Don't bundle ADR 003 telemetry residual into ADR 004
  feature commits.** Separate `chore:` slice (ADR 004
  council decision 19).
- **Don't conflate join-build key hashing with
  exchange-partition hashing.** ADR 003 §5 partition assignment
  uses `xxh3_64(canonical_encoded_bytes)`; ADR 005's fast-path
  key representation is operator-local. Helper names must make
  the boundary explicit ("join build key" vs "exchange partition
  hash"), never a generic "fast hash" that blurs them. ADR 005
  hard gate 14 conditional architecture guard.
- **Don't let an ADR 005 fast-path encoder lose the logical-type
  tag.** `Int32(1)` and `Int64(1)` must remain distinct
  build-side hash keys. ADR 005 hard gate 7 unit-test enforced.
- **Don't promote ADR 005 on wall-clock alone.** `build_ms` is
  the primary signal; digest equality and phase attribution are
  load-bearing. ADR 005 hard gate 16.
- **Don't soften an ADR 005 `build_ms` target miss** into a
  vague "improved" narrative. The bench artifact records the
  explicit gap; the council selects accept-narrower / scope-down
  / shelve.
- **Don't add new contract surface in ADR 005.** No new
  `MessageKind`, IR variant, `BudgetDimension`, task telemetry
  field, public API, Wendelon scan contract, or architecture
  guard (default; conditional escape only). ADR 005 hard gates
  3, 12-14.
- **Don't parallelize hash-table build inside a worker in
  ADR 005.** Sharded concurrent insertion is V1.x with its own
  determinism + budget accounting work.
- **Don't bundle runtime-plumbing investigation into ADR 005.**
  Conditional ADR 006 governed by the three-band rerun rule.
- **Don't generate ADR 006 runtime filters inside
  `execute_inner_join`.** Too late in ADR 004's stage graph;
  the probe-side shuffle has already happened. Filter
  generation lives in the build-side exchange producer wave
  (ADR 006 Decision §1).
- **Don't let ADR 006's runtime-filter hash conflate with the
  exchange-partition hash.** ADR 003 §5 partition assignment
  uses `xxh3_64(canonical_encoded_bytes)`; ADR 006's
  runtime-filter hashing is operator/cross-ring scoped.
  Helper names are semantic ("runtime filter key" vs
  "exchange partition hash"). Generalizes ADR 005's pattern.
  ADR 006 hard gate 15.
- **Don't let an ADR 006 runtime filter affect join
  correctness.** Advisory always — false positives allowed,
  false negatives forbidden. If Wendelon drops the filter for
  any reason, output is byte-identical to filter-disabled.
  ADR 006 hard gate 7 (false-positive tolerance) + hard
  gate 10 (no false negatives).
- **Don't count ADR 013 advisory-drop behavior as production
  runtime-filter application evidence.** Advisory drop is the
  safety fallback; promotion evidence needs an applied Wendelon
  filter with fewer probe rows/bytes and digest equality.
- **Don't add ADR 013 runtime-filter key columns to scan output.**
  Wendelon may fetch probe keys through
  `ColumnFetchRequirement::predicate_columns`, but PBC output is
  still governed by `output_columns`.
- **Don't put `RuntimeFilterDescriptor` into
  `TableQueryOptions`.** ADR 013 keeps options `Copy`; the
  descriptor is query payload carried by runtime-filter-specific
  table-query entry points.
- **Don't start probe-side dispatch before the ADR 006
  build-side runtime-filter barrier opens** when filters are
  enabled. ADR 006 hard gate 13 — the two-wave barrier is
  contractually load-bearing.
- **Don't deserialize ADR 006's runtime-filter descriptor as
  a Toron-private type in Wendelon.** Shared cross-ring schema
  surface only (default: extend an existing Wendelon-published
  schema crate; alternative: `nilket-shared` common module).
- **Don't multiply ADR 006 filter kinds in V1.** Bloom only.
  Exact `InList` is V1.x once the cross-ring contract is
  proven.
- **Don't add ADR 006 planner cost-model opt-in in V1.** Always
  two-wave when the filter is enabled; planner-stats opt-in
  is V1.x once stats exist.
- **Don't couple ADR 006 promotion to lockstep Wendelon-side
  landing.** Advisory invariant makes split-shipment safe.
  Toron-side shipped first; the Wendelon-enabled production row
  arrived through ADR 013.
- **Don't extend ADR 006 filter eligibility to outer / semi /
  anti / sort-merge / non-equi / broadcast joins.** Each is
  its own future ADR (V1.x). Inner + hash-partitioned only in
  V1.
- **Don't soften an ADR 006 selective-row reduction miss**
  into a vague "improved" narrative. The bench artifact
  records the explicit gap; the council selects
  accept-narrower / scope-down (narrow filter eligibility) /
  shelve.
- **Don't promote a runtime-filter ADR without a
  no-false-negative unit test.** The advisory invariant is
  load-bearing; false negatives are contract violations.
  ADR 006 hard gate 10.
- **Don't skip the non-selective guard benchmark.** The
  two-wave barrier cost is real; the guard proves it's
  bounded against ADR 005's `build_ms` baseline. ADR 006
  hard gate 19.
- **Don't skip the Scale boundary section in new ADRs**
  (convention adopted iter-5 of ADR 006). Even when the
  boundary feels obvious, naming it explicitly makes
  scope-creep into adjacent boundaries visible at review
  time. Omit only when truly N/A and say so explicitly.
- **Don't call `Context::with_default_modules()` in the Rune
  engine** (ADR 007 hard gate 19). The V1 contract is
  allow-list (`Context::new()` + explicit registration); a
  future refactor reverting to default-allow opens the
  sandbox. Source-level architecture grep guard at
  `tests/architecture_test.rs` enforces this.
- **Don't ship a fuel-only V1 Rune sandbox.** Memory
  enforcement is V1 hard requirement (ADR 007 hard gate 8). A
  memory bomb that fits within fuel can still OOM the host —
  Rune-native cap or helper-process isolation, not "deferred."
- **Don't add new `TaskFailureKind` variants for sandbox
  failures.** ADR 003 7-variant wire contract is
  architecture-guarded. Sandbox failures crossing the task wire
  map to `TaskFailureKind::TaskFailed { retryable: false,
  reason: "RuneSandbox::<label>" }`.
- **Don't let Rune sandbox failures fall through to
  `RuneRuntime`.** Use the `FrontendError::RuneSandbox`
  variant carrying the typed `RuneSandboxViolation` enum.
  `RuneRuntime` is for genuine script-logic errors only.
- **Don't expose host paths, internal VM state, or Rune
  source positions in coordinator-facing error messages**
  (ADR 007 hard gate 20). `CapabilityCategory` enum abstracts
  over raw module paths for stability and non-leakiness.
- **Don't add native functions to the Rune allow-list without
  threat-model review.** Each new function is a potential
  sandbox-escape vector. Extension is a separate narrow ADR.
- **Don't couple Rune sandbox enforcement to per-tenant
  governance.** Tenant identity is L-3 (multi-tenant
  governance) territory. ADR 007 ships per-*script* budgets;
  per-tenant aggregation is a future ADR when workload evidence
  justifies.
- **Don't treat `wall_timeout` as primary CPU accounting.**
  Fuel is primary; `tokio::time::timeout` around
  `async_complete()` is a defense-in-depth belt that fires only
  at .await yield points (Rune 0.13.4 drives the VM via a
  synchronous `vm.run()` inside `async_resume()`). Real
  preemption of a CPU-bound loop with broken fuel comes from the
  Phase 4 helper-process parent-side SIGKILL deadline, not the
  Tokio timeout. The infinite-loop hard gate (gate 7) must
  assert `FuelExhausted`, not merely `Timeout`.
- **Don't run row-level Rune UDFs in worker tasks under
  ADR 007.** Rune is frontend-scoped in V1 (script →
  `LogicalPlan` → distribution). Worker-side execution is a
  substantial future ADR.
- **Don't introduce a second `ExecutionPlan::Join` or
  `LogicalPlan::Join` physical variant** (ADR 008 hard gates
  22, 23). Per-kind physical variants duplicate the operator
  shape and weaken ADR 004's single-variant architecture guard.
  The semantic difference belongs in `JoinKind`.
- **Don't use sentinel values for null fill in outer joins.**
  Arrow's native `NullBuffer` is the canonical representation;
  sentinels break the contract and couple to downstream readers.
  ADR 008 hard gate 10.
- **Don't loosen runtime-filter eligibility beyond `Inner`.**
  ADR 006 invariant is load-bearing for outer / anti
  correctness. A Bloom filter on the build side of a left-outer
  join would drop the very rows the outer must preserve;
  `RightAnti` depends on emitting unmatched probe rows. ADR 008
  hard gate 17 enforces; a "filter + preserve" combined operator
  is the separable future design (ADR 010 Candidate C).
- **Don't allow `JoinStrategy::Broadcast` for non-Inner
  `JoinKind`.** Broadcast replicates the build side; preserved-
  side or existence variants would emit duplicate unmatched
  rows in every probe partition. ADR 008 hard gate 16 (binder
  rejection) + hard gate 18 (fragmenter defensive guard) form
  the defense-in-depth pair.
- **Don't rewrite `RightOuter` ↔ `LeftOuter` at planner time as
  a workaround for broadcast.** Strategy-dependent semantics
  blur the operator-level scale boundary. The cleaner contract
  is reject-and-defer; a strategy-rewrite ADR can follow if
  evidence supports it.
- **Don't add `EXISTS` / `NOT EXISTS` decorrelation under ADR
  008.** Subquery-planner contract; promoted as ADR 009. ADR 008
  ships the `Semi` / `Anti` operators; ADR 009 layers the
  correlated-`EXISTS` rewrite on top.
- **Don't promise rolling coordinator/worker mixed-version
  semantics for `JoinKind`.** Serde-tagged variants are additive
  but older binaries reject unknown variants. ADR 008 ships at a
  single workspace version bump; cluster upgrades are
  full-fleet, not rolling. Matches the posture from prior ADRs.
- **Don't drop `JoinKind` IR variants if `sqlparser` regresses
  a spelling.** Keep the IR variants; gate SQL lowering to
  whatever parses. Dropping IR variants breaks the serde wire
  contract and ADR 008 hard gate 25 (variant-count architecture
  guard).
- **Don't add per-kind task-telemetry counters for the new join
  kinds.** ADR 003 task-telemetry contract change worth carrying
  only when bench evidence is insufficient. Existing
  `build_rows` / `probe_rows` / `output_rows` counters are
  sufficient for V1; kind-specific aggregates surface downstream
  of the task wire if needed.
- **Don't add a subquery node to Toron's IR for `EXISTS`
  decorrelation.** ADR 009 is a targeted SQL-AST rewrite in
  `SqlPlanner::lower_select`; the logical and physical join
  contract is reused unchanged. A subquery IR node is a general
  subquery-engine decision, not ADR 009's.
- **Don't lower the whole subquery predicate through `lower_expr`
  before classifying.** ADR 009's two-scope classifier operates
  on the `sqlparser` AST so unsafe outer-reference shapes reject
  precisely; a generic lowering pass first would route a mixed
  predicate into a generic filter path. Classify, then lower only
  the accepted `CorrelatedPair` columns and `InnerResidual`
  predicates.
- **Don't invent a new unqualified-name resolution rule for
  subqueries.** ADR 009 rejects unqualified / ambiguous
  correlation rather than guessing. If the existing qualifier
  logic cannot pin one side to the outer scope and one to the
  inner scope, the rewrite is unsupported — "too narrow and
  correct" over matching a permissive engine.
- **Don't decorrelate uncorrelated `EXISTS`, `IN`, or `NOT IN`
  under ADR 009.** Uncorrelated `EXISTS` is a scalar cardinality
  check, not a semi-join; `NOT IN` carries three-valued NULL
  logic that must not be smuggled into anti-join semantics. Each
  is a named, deferred extension with its own decision matrix.
- **Don't widen the ADR 009 subquery body beyond one
  `TableFactor::Table`.** Subquery joins, aggregates, `LIMIT`,
  `DISTINCT`, set ops, and nested subqueries are unsupported and
  reject through the existing path. The classifier's
  `Unsupported` arm is the structural scope boundary.
- **Don't duplicate binder semantic checks in the SQL planner
  for ADR 009.** The planner classifies shape and builds the
  logical rewrite; the binder stays the authority for type
  compatibility, float-key rejection, computed-key rejection,
  exchange shape, and non-Inner broadcast rejection.
- **Don't let `lower_query` / `lower_select` destructure with a
  `..` rest pattern.** ADR 010's exhaustive three-bucket
  no-`..` destructure is the load-bearing clause-accounting
  guard — a `sqlparser` bump that adds a field must be a compile
  error, not a silent widening. A source-level architecture
  guard enforces it; the parser-metadata allow-list
  (`select_token`, `top_before_distinct`,
  `window_before_qualify`) is explicit.
- **Don't silently drop any top-level `Query` / `Select`
  clause.** ADR 010's invariant: every parsed field is
  lowered, rejected-when-present, or bound-and-ignored as parser
  metadata. Silently-dropped `ORDER BY` was the V1 integrity
  defect ADR 010 closed; do not reintroduce the class.
- **Don't accept bare `LIMIT` / `OFFSET` / `FETCH`.** Without an
  `ORDER BY` contract, distributed truncation is arbitrary.
  ADR 010 rejects them; deterministic top-N is
  `ORDER BY ... LIMIT n`.
- **Don't place `Sort` below `Project` or support `ORDER BY` on
  non-output / alias / computed keys.** ADR 010 keys resolve
  against the output schema only (column names + 1-based
  positive ordinals); `Sort` sits above `Project` so the
  total-order tie-break surface is exactly the output columns.
  Input-only keys are a named V1.x extension.
- **Don't ship a partial-order `ORDER BY`.** ADR 010's contract
  is a total order — user keys then every remaining output
  column, ascending `NULLS LAST`, appended at bind time. Ties
  resolved arbitrarily reopen silent nondeterminism.
- **Don't let the fragmenter infer a hidden gather, and don't
  prove the distributed gather-sort with output digest alone.**
  The planner emits the terminal `Exchange { partition_count: 1 }`
  explicitly; the post-join gather-sort is strategy-agnostic
  (hash-partitioned and broadcast) and gated by stage-shape
  inspection. ADR 010 Phase 4 is the named primary risk.
- **Don't add a new `TaskFailureKind`, `BudgetDimension`, IR
  level, or wire surface for ADR 010.** `LogicalPlan` /
  `ExecutionPlan` each gain exactly one variant (`Sort`); sort
  memory exhaustion reuses `BudgetDimension::Memory` →
  `ExecutionError::BudgetExhausted` →
  `TaskFailureKind::BudgetExhausted`. No sort spill, no
  distributed range sort in V1 — both are named V1.x ADRs.
- **Don't implement `DISTINCT` / `GROUP BY` / `HAVING` under
  ADR 010.** They loud-reject; implementing them is
  aggregate-path ADR work. ADR 010 closes their silent drop
  only.
