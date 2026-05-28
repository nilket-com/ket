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

ADRs 001–121 are promoted (implemented unless a `feat` is still pending — check
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

### Active line — Rune authoring run (079–086) + nominal-typing spine (087–093) + pushdown residual (094) + integrity & elision tier (095–098) + introspection (099) + derived-source provenance (100) + Rune LIKE/ILIKE (101) + table/column introspection (102) + derived alias columns (103) + unified alias-column rename (104) + adjacent-identity-Project coalescing (105) + derived aggregate tables (106) + information_schema.schemata (107) + SQL CASE (108) + conditional-sugar family (109) + conditional logical-identity preservation (110) + Rune CASE authoring (111) + explicit NULL literal (112) + aggregate-output logical identity (113) + contextless NULL projection (114) + composable ordered top-k (115) + unordered bounded take (116) + multiple ordered top-k barriers (117) + stream-shaped storage boundary (118) + pipelined cross-stage execution (119) + end-to-end bounded first rows (120) + general qualified relation-scope binding (121, detail retained)

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

- **ADR 094 — Predicate Pushdown Residual Minimization (factored OR / NOT).** Promoted. ADR 078's
  **explicitly-deferred** follow-on (`078:58` "no residual reconstruction/minimization this ADR"),
  on Toron's **primary axis** (pushdown). 078 kept the *whole original* predicate as the Toron
  residual after pushing a factored common term; 094 makes it minimal: `(P∧R1)∨(P∧R2)` → `pushed=P,
  residual=R1∨R2`; `NOT(P∨R)` → `pushed=¬P, residual=¬R` (= `AND of ¬unpushed-branch`). **Pure
  efficiency, zero result change** — `P ∧ minimal ≡ P ∧ original` on survivors. Binder-local
  (`split_boolean_tree` + its extractors return pushed + reconstructed residual; one new
  `combine_bound_programs_as_or`); structural-identity only (no commutation/folding/CNF);
  branch-strips-to-TRUE & extractor all-pushed → residual `None`; `NOT(AND)` out; reconstruction
  failures after a safe pushed split fall back to ADR 078's original residual; **AND-path untouched**
  (075 owns its minimal residual — clean seam). The existing
  3VL property gate (`binder.rs:3814`) auto-validates the new residual over all 27 {T,F,N}³ rows
  (extended with the D2/D3 edge shapes). Invert 078's `..._original_residual` DML guards/fixtures
  (`delete_test.rs:149`, `update_test.rs:341`) → `...plus_minimal_residual` in-change. No
  IR/SQL/PBC/Wendelon/wire/executor change. Genuine 3-of-3 after an operator-broken cross-over
  (Claude grounded 078's own-deferral provenance; Codex re-grounded + accepted the lock; Gemini's
  dual aligned). Migration: `plans/impl/094_predicate_residual_minimization.md`.

- **ADR 095 — EmptyRelation + Static Contradiction Elision.** Promoted. **Slice 1 of the refinement
  tier** the council forked over in the 094 cycle — the Toron-only elision substrate; durable
  refinement predicates are slice 2 (ADR 096). Adds `ExecutionPlan::EmptyRelation { schema }`
  (execution-IR only; **internal `TaskSpec` payload, not "no wire"** — Codex's catch) + a conservative
  binder classifier `StaticFilterClass { AlwaysPass, AlwaysReject, Unknown }` (only `AlwaysReject`
  elides). **Doctrine: a plan-shape optimization, never a truth source** — replace a `Filter` only
  when statically *always-rejecting* under `WHERE` 3VL (`{FALSE} ∪ {NULL}`, since Arrow
  `filter_record_batch` keeps `Some(true)` only). Classifier: closed (column-free) root → eval via
  `evaluate_bound_program_without_storage` on a 1-row zero-column dummy → `AlwaysReject` on
  `false`/`null`; structural `non_nullable_col IS NULL` → reject; **everything else (column ref) →
  Unknown**. **No recursive boolean short-circuit** — which by construction also **preserves eager
  errors** (a closed strict-cast that errors classifies as `Unknown`, so the query still raises it;
  Codex's catch). Closed-`NULL` **elides** (resolved 3-of-3 — column-free constant-NULL is statically
  never-TRUE, vs data-dependent Unknown). Elision is filter-local (in
  `plan_filter_with_predicate_pushdown`, before pushdown, replacing *only* the Filter), so the
  **scalar-aggregate cardinality fence** is structural — `SELECT COUNT(*) … WHERE 1=0` still emits one
  `0` row (`bound_global_aggregate`, `exec.rs:1117`). The implementation bulk is a **~15-site
  exhaustive `ExecutionPlan` walker sweep** (~4 real, ~11 trivial leaf arms; no `_` wildcard, guarded).
  SELECT + UPDATE-source covered; DELETE deferred. Logical identity survives via the carried
  `SchemaRef` (087–093, 089 path structural). **No** SQL/logical-IR/PBC/Wendelon/storage/public-wire
  change. Genuine 3-of-3 (Claude: the 15-site ripple + cardinality fence + closed-NULL-vs-data-dependent
  distinction; Codex: the `TaskSpec`-wire reality + eager-error fence + 1-row dummy-batch; Gemini
  synthesized). Migration: `plans/impl/095_empty_relation_static_contradiction_elision.md`.

- **ADR 096 — Durable Refinement Predicate Persistence + Write Enforcement.** Promoted. **Slice 2 of
  the refinement tier** — the integrity *receipt* (097 spends it on elision). **Doctrine: the receipt
  before the magic** — a `CHECK (VALUE > 0)` refinement is a valid elision basis only if stored data
  is guaranteed to satisfy it, so 096 *persists + enforces*; eliding on an unenforced declaration is a
  silent-wrong-results bug (the load-bearing **soundness reordering** Claude caught: enforce before
  elide; Gemini's persist+elide conceded). `CREATE LOGICAL TYPE <Name> AS <primitive> CHECK (VALUE >
  0)` persists a **portable, Wendelon-owned refinement AST** (`LogicalTypeRefinementPredicate` —
  comparisons over `VALUE` + `AND`; **not** `BoundExprProgram`/`ScalarValue`; float literals as
  canonical bits to keep `Eq`/`Hash`). **Complete write enforcement** across INSERT VALUES / INSERT
  SELECT / UPDATE / CTAS (a shared runtime validator reusing 095's `evaluate_bound_program_without_storage`;
  literal-only would not be a receipt). `CAST`/`TRY_CAST` stay pure physical-ascription (092/093
  byte-identical) — the storage boundary is the only enforcement point; 097's elision is scoped to
  stored-column receipts. Null orthogonal (refinement on non-null). Receipt scope = complete Toron
  write path (Toron sole writer, refinement-before-column); Wendelon authority-side
  validation carried. **No numeric suffixes/compat** — the touched Wendelon logical-type contract is **de-versioned
  in place** (`LogicalTypeDefinition`/`…Create`/`LogicalTypePhysical`/`LogicalTypeCatalog`), per the
  operator's pre-release directive (no historical installations). Cross-ring (bounded 090 re-open),
  Toron-sequence. Genuine 3-of-3 (Claude: soundness reordering + storage-only/CAST/receipt resolutions;
  Codex: portable AST + float-bits + idempotency + the de-version directive; Gemini: corrected to
  neutral-AST + complete enforcement). Migration:
  `plans/impl/096_durable_refinement_persistence_enforcement.md`.

- **ADR 097 — Refinement-Driven Read-Time Elision.** Promoted. **Slice 3 of the refinement tier — the
  payoff**, closing the arc (095 substrate → 096 receipt → 097 spend). Feeds 096's *enforced* durable
  refinement into 095's `static_filter_class` (`binder.rs:1169`) as a third `AlwaysReject` proof: a
  whole-root filter atom that contradicts a stored column's refinement → 095's `EmptyRelation` (zero
  scan, zero PBC) before pushdown. **Two soundness invariants:** (1) **durable registry, not seed** —
  thread `Catalog::logical_type_registry()` (096-synced), never the seed-only `logical_type_spec` (the
  091/092 trap); (2) **stored-scan provenance, not the sidecar** — elide only when `input` is
  `Scan { projection: None }` and the atom is a direct scan-output `Column op literal` with a durable
  refinement. A `CAST(raw AS PersonId)` column carries the sidecar (089) but is *not* a 096 receipt
  (092/093/096 D8 left casts ephemeral) → must not elide. Reasoning = a bounded **1D carrier-domain
  intersection** (interval bounds + excluded points, so `CHECK (VALUE != 0)` + `id = 0` elides; floats
  decode from 096 bits, NaN→Unknown; Bool/Utf8 equality singletons) — not an SMT solver. Whole-root
  single comparison; AND-decomposition/multi-column/pass-through-projection deferred. 3VL +
  error-conservative (structural proof; `IS NULL` stays 095's rule). **Inverts 096's
  `no_refinement_elision_yet` guard** → "elision only from durable stored-scan receipts" (discipline
  #4). Toron-only; reuses 095 `EmptyRelation`; **no** Wendelon/096-contract/PBC/executor/wire change.
  Genuine 3-of-3 (Claude grounded the durable-not-seed + provenance-not-sidecar catches; Codex
  independently grounded both + the `Scan{projection:None}` precision + the `!=` excluded-point case +
  the guard inversion; Gemini ratified). Migration: `plans/impl/097_refinement_driven_elision.md`.

- **ADR 098 — Top-Level-AND Decomposition for Static Elision.** Promoted. Widens the 095/097 elision
  substrate from whole-root-only to **compound `WHERE`** (the universal case it was missing — `id = -5
  AND active`, `1 = 0 AND active`). **Doctrine: a proof combiner, not short-circuit evaluation** —
  `A∧B` is never-TRUE if `A` is (3VL), but eliding must not skip a sibling that would *raise* under
  Toron's eager eval. So `AlwaysReject` **iff** (∃ conjunct `AlwaysReject`) ∧ (every other conjunct
  **provably error-free**) — the **eager-error fence** (Claude + Codex independent catch), the exact
  095 error-masking bug averted. Shape: an **atom/wrapper split** — extract `static_filter_atom_class`
  (the unchanged 095/097 whole-root proofs), wrap it to `collect_top_level_bound_and` (094, recurses
  nested `AND`) + `isolate_bound_subgraph` each conjunct + run the atom helper per conjunct.
  **Unifies 095 + 097** per conjunct (closed-fold, structural-null, *and* refinement — Claude's scope
  correction; not 097-only). Safe-sibling allowlist: column/literal/comparison/`IS [NOT] NULL`/nested
  `AND` (+ closed siblings iff the closed-root classifier returns `AlwaysPass`/`AlwaysReject`);
  **excluded:** `Cast`/`TryCast`/arith/`InList`/`Like`/
  `OR`/`NOT`. Inherits 097's durable-registry + `Scan { projection: None }` provenance per conjunct;
  reuses 095's `EmptyRelation`; no `AlwaysPass`/filter-removal; top-level/nested `AND` only (094 owns
  residual). **Retargets** 095's no-recursive-`AND` guard → "`AND` only in the bounded decomposition
  wrapper" (discipline #4). Toron-only; no IR/exec/wire/PBC/Wendelon change. Genuine 3-of-3 (Claude:
  unify-095+097 + the fence; Codex: independent fence + the atom/wrapper split + the guard retarget;
  Gemini ratified). Migration: `plans/impl/098_and_decomposition_static_elision.md`.

- **ADR 099 — `information_schema.logical_types` (read-only introspection).** Promoted. **The pivot off
  the 11-ADR identity/refinement arc** (087–098): makes the durable type system *inspectable* — a
  `SELECT … FROM information_schema.logical_types` lists the tenant's durable logical types
  (`logical_type_id BIGINT, name, physical` SQL-keyword, `refinement` `CHECK`-text-or-`NULL`). **The
  first read-side synthetic relation.** Selection note: Claude **proved** the on-arc lead
  (pass-through-projection) had *no SQL consumer* — `SELECT id FROM people WHERE id<0` already elides
  (`lower_select` puts Project above Filter), so its gate only blocks non-SQL-reachable shapes; operator
  locked the introspection pivot (pass-through → ADR 100). Shape (3-of-3, Codex's sharpenings): a new
  `ExecutionPlan::SystemTable { kind, schema, rows }` leaf (the 095 `EmptyRelation` precedent +
  ~15-site walker sweep), **bind-time interception** of `ScanNamed(information_schema.logical_types)`
  before `catalog.resolve_table` (no `LogicalPlan` variant; catalog access stays in the binder),
  **refresh-before-materialize** (`refreshed_logical_type_registry`, list-failure fails the query — a
  synced view must refresh, per 091/092), rows baked at bind from `specs_sorted()`. Durable-only +
  tenant-scoped (seed/`naked` excluded); **read-only namespace** (mutation/DDL *targeting*
  `information_schema.*` rejects; CTAS-*from* valid); refinement rendered from the 096 AST; composes with
  `WHERE`/projection/`ORDER BY` for free. Toron-only; **no Wendelon contract/PBC/wire/storage change**
  (refreshes through the existing catalog-sync path, then executes coordinator-local). Genuine 3-of-3.
  Migration: `plans/impl/099_information_schema_logical_types.md`.

- **ADR 100 — Derived-Source Transparent Projection Provenance.** Promoted. **Closes the 097/098
  deferral 099 named:** standalone pass-through provenance had no SQL consumer (`SELECT id FROM people
  WHERE id<0` already binds `Project(Filter(Scan))`), so 100 **pairs the consumer with the proof** —
  a narrow derived `FROM` source makes `Filter(Project(Scan))` SQL-reachable, and the refinement atom
  learns to trace through a transparent inner `Project` to the stored scan receipt. **Fork → B:** the
  planner accepts narrow single-source plain-SELECT derived tables (transparent *or* computed); a
  computed derived projection binds and runs but returns `Unknown` — transparency is an elision-proof
  property decided in the binder atom, **not** a parser property. **Two-shape receipt** (Claude's iter-1
  cut, 3-of-3): direct `Scan { projection: None }` or transparent-`Project` chain
  (`exprs.roots[ordinal]` must be `Column`, remap output→child, recurse); the projected
  `Scan { projection: Some }` receipt is **excluded as inert** — `push_down_scan_projection` runs
  *after* filter classification (`bind()` = `bind_plan` then pushdown), so it has no consumer at proof
  time. Reuses **089-propagated identity** (structural-only, no re-drive); leaves the **098 eager-error
  fence** untouched; outer projection unconstrained. Behavior payoff visible as the **inverted** guard
  (`refinement_on_non_scan_input_does_not_elide` → `refinement_through_transparent_project_elides`; its
  transparent bare-column project now elides), with `cast_ascribed_projected_column_does_not_elide`
  preserved as the non-transparent boundary. Toron-only; no IR/exec variant, PBC, executor, storage, or
  Wendelon change. Genuine 3-of-3 (Claude grounded 089-identity-propagation, the projected-scan-inert
  cut, and the confirmed retarget; Codex seeded the paired cut + the receipt resolver structure; Gemini
  ratified). Migration: `plans/impl/100_derived_source_transparent_projection_provenance.md`.

- **ADR 101 — Rune `LIKE`/`ILIKE` Pattern Predicates.** Promoted. A **pure Rune authoring admission**:
  the whole `LIKE`/`ILIKE` path already exists end-to-end for SQL (`Expr::Like` `ir.rs:528` →
  `BoundExprNode::Like` → `lower_like`/`bind_like` → Arrow `like`/`nlike`/`ilike`/`nilike` → PBC declines
  it as residual `pbc.rs:128`); Rune was the only missing authoring consumer. Adds four infallible
  `RuneExpr` methods (`like`/`not_like`/`ilike`/`not_ilike`) that build the existing `Expr::Like` via a
  `build_like` helper, registered in `create_toron_module`. **Literal-only pattern** (host `String`, per
  075); **residual-only** (no PBC); **binder owns type policy** (the existing `LIKE expects Utf8 input`
  error). Infallible `-> RuneExpr` (mirrors `logical_not`/`eq`, not `in_list`'s `VmResult` — that's the
  exception, for empty/oversized-list validation). Retargets stale surface guards: the deferred list
  (`rune_frontend.rs:700`) becomes `["not","safe_cast"]`, and the ADR 086 in-list forbidden-name guard no
  longer rejects admitted `like`/`ilike` names. Toron-only, ring-local Rune frontend; **no** SQL
  grammar/IR/binder-semantics/executor/PBC/storage/Wendelon change. Genuine 3-of-3 (Codex seeded +
  sharpened the infallible return; Claude grounded the substrate + the exact binder error + the
  `sales`-fixture rows/guard retarget sites; Gemini ratified). Migration:
  `plans/impl/101_rune_like_ilike_pattern_predicates.md`.

- **ADR 102 — `information_schema.tables` / `.columns` (authoritative).** Promoted. The 2nd/3rd read-side
  system relations, generalizing 099's `SystemTableKind` (3 finite variants; **no** virtual-table
  framework, **no** new `ExecutionPlan` variant → no walker sweep). **Fork B (operator-locked):**
  authoritative refreshed tenant descriptor listing, **not** local-cache-only — Claude grounded that the
  lazy `WendelonDescriptorRegistry` would make `information_schema.tables` mean "tables this coordinator
  touched," violating 099's refresh-before-read (D6). **The point of the ADR is the cross-ring read
  contract:** a new read-only Control-Ring RPC `list_table_schema_descriptors(tenant)` (first new
  cross-ring read surface since 090), built on Wendelon's **`latest_by_name`** index (latest-only — raw
  `descriptors_for_tenant` spans all generations → would duplicate rows). `Catalog::refreshed_table_schema_descriptors`
  mirrors 099's refresh-before-read (logical-types-then-descriptors so durable names decode; list-failure
  fails the query). `.columns` decode is reuse (`descriptor.arrow_schema()` + `decode_column_logical_type_with_resolver`
  + the closed SQL carrier renderer). Schemas: `tables`(table_schema/table_name/`BASE TABLE`), `columns`(schema/table/
  column/1-based ordinal/SQL-`data_type`/`YES`-`NO`/`logical_type_name` — the 087-098 tie-in, resolved via
  the refreshed registry, not `logical_type_name(id)` fallback). Read-only namespace; coordinator-local
  (listing at bind). **Retargets three 099 guards** — esp. `adr099_no_wendelon_pbc_wire_or_storage_surface`
  narrows to "no *execution-time* storage/PBC/worker/RPC" (102 adds a *bind-time control-plane* read).
  Toron binder/catalog + a narrow Wendelon listing RPC; no storage/executor/PBC change. Genuine 3-of-3
  (Codex seeded the authoritative-vs-cache framing + the Wendelon wire shape + the guard-retarget catch;
  Claude grounded latest-only-via-`latest_by_name` + the decode-reuse + no-walker-sweep; Gemini ratified).
  Migration: `plans/impl/102_information_schema_tables_columns.md`.

- **ADR 103 — Derived-Table Alias Column Lists (explicit-projection).** Promoted. Closes the ADR 100
  alias-column deferral: `FROM (SELECT id, note FROM people) AS p(pid, label)` **positionally renames** the
  derived subquery's outputs. **Pure `sql_planner`** — it rewrites the existing lowered
  `Project.output_names` (the rejection at `sql_planner.rs:3394` moves into `lower_derived_table`, where the
  `select` + lowered plan coexist; a new `apply_derived_alias_column_list` helper arity-checks and
  overwrites `output_names`). **Explicit-only scope (Codex's correction, operator-locked):** an explicit
  projection lowers to `Project` (plan-time arity); a wildcard (`SELECT *`) lowers to a bare plan (no
  `Project`, arity binder-known), so wildcard alias-lists **reject** with a named fence — binder-carried
  wildcard aliasing is a deferred ADR. **Exact arity** (mismatch rejects before binding). The rename is
  **transparent** for bare-column items (089 preserves identity, ADR 100's `transparent_project_child_ordinal`
  still traces the receipt → `WHERE pid < 0` over `PersonId CHECK (VALUE > 0)` still elides); a computed
  rename binds but doesn't elide. Duplicate aliases (`AS p(x, x)`) inherit existing `AmbiguousColumn`
  behavior (no new uniqueness rule). Retargets only the categorical alias-rejection assertion in
  `adr100_derived_table_source_is_narrow_not_general_subquery_support`; all other 100 fences hold. No new
  IR/exec/PBC/storage/Wendelon/`information_schema` surface. Genuine 3-of-3 (Claude seeded + the 089/100
  transparency tie-in; Codex grounded the seam + the explicit-vs-wildcard scope correction + duplicate-alias
  inheritance; Gemini ratified). Migration: `plans/impl/103_derived_table_alias_column_lists.md`.

- **ADR 104 — Unified Table Alias Column Lists at Bind.** Promoted. **Supersedes 103's planner rewrite**:
  makes the alias column list a single `LogicalPlan::Alias { columns }` concept renamed **once at bind**,
  covering base-table, explicit-derived, and wildcard-derived aliases through one mechanism. **The decisive
  grounding** (which moved the council A→B after the operator flagged a small-cut/structure-ossification
  bias): **base-table alias columns are silently dropped today** — `TableFactor::Table` never checks
  `alias.columns` and `alias_plan` drops them, so `FROM people AS p(pid)` ignores the rename — and
  base-table + wildcard both lack a plan-time `Project`, so the binder rename is **mandatory** regardless.
  Fork A (wildcard-only binder path + keep 103's planner rewrite) was two mechanisms that *still* left
  base-table broken; **B unifies all three and fixes the bug**. Shape: extend
  `LogicalPlan::Alias.columns` (mini-sweep); planner carries names for every alias + rejects type
  annotations; delete `apply_derived_alias_column_list`; arity fully unified at bind; rename = **ordinal**
  identity `Project` (not `Expr::column(name)` — robust to duplicate source names) reusing the 089
  metadata path so logical identity + the 100 receipt trace (explicit + wildcard elision both hold);
  duplicates inherit `AmbiguousColumn`. Explicit-derived gains a redundant identity `Project` (Toron has no
  project-folding) — accepted; a future general collapse pass is the right fix (out of scope). Retargets
  three 103 guards (incl. the inverted "Alias is now extended"). Toron-only IR+planner+binder; no
  exec/PBC/storage/Wendelon/`information_schema` surface. Genuine 3-of-3 (all three flipped A→B on the
  base-table grounding; Codex surfaced base-table + the ordinal-project sharpening; Claude confirmed the
  silent-ignore + no-project-folding and held fully-unified arity; Gemini conceded the planner arity
  check). Migration: `plans/impl/104_unified_alias_column_lists.md`.

- **ADR 105 — Adjacent Identity `Project` Coalescing.** Promoted. Closes 104's named follow-on: 104's
  unified bind-time rename leaves a redundant identity `Project` on explicit-derived aliases (Toron has no
  project-folding), and the pattern recurs in plain derived pass-through (`SELECT id FROM (SELECT id, note
  FROM people)`) + Rune `.project_named` chains. **The decisive grounding** (Codex, dissolving Claude's
  iter-0 Fork A/B): the post-bind optimizer `optimize_for_required` already runs on every plan and its
  `Project` arm already reconstructs the node — so the coalescer is **one helper call in an existing arm**,
  not a new walker or an alias special-case. Shape: in the `Project` arm, `collapse_adjacent_identity_project`
  folds an adjacent `Project`-over-`Project` when the parent is a **dense `0..N` positional identity** with
  **metadata-transparent** roots (physical type + nullability + durable `LogicalTypeId` match the child
  output; **names ignored** — parent schema wins); the collapsed node reuses child input + child exprs,
  keeps the parent `output_schema`; `output_remap` unchanged; bottom-up recursion collapses N-deep chains
  pairwise. Base-table + wildcard renames sit over `Scan` (sole carrier) and are **not** folded (D6).
  Conservative on bad metadata (predicate returns `false`, not error). **Efficiency only** — elision runs
  before this pass and traces chains regardless; the win is removing one per-morsel evaluator frame. No
  SQL/Rune/IR/exec-variant/PBC/storage/Wendelon/`information_schema` surface; removes a node, adds none.
  Genuine 3-of-3 (same helper shape across the latest duals; Codex dissolved the placement
  fork + added the derived-source scope regression gate; Claude corrected the rename-is-not-a-no-op
  characterization + grounded the recurrence + the correctness backbone (`output_remap` invariance, chain
  composition); Gemini converged). Migration: `plans/impl/105_adjacent_identity_project_coalescing.md`.

- **ADR 106 — Derived Aggregate Tables.** Promoted. The narrow derived-source slice: admit grouped + global
  aggregate SELECTs as derived sources (`FROM (SELECT k, COUNT(*) AS n FROM t GROUP BY k) AS p`) over the
  **existing** `Aggregate` substrate. **Pure SQL-frontend fence-lift** — `lower_select` already builds
  `Aggregate`→`Filter`(HAVING)→`Project`; the only blockers are two artificial fences. The cut: relax the
  `group_by`/`having`/aggregate-projection rejections in `validate_derived_select_shape` (keep window +
  every structural fence) and drop the `aggregate_output_catalog.is_some()` rejection in
  `lower_derived_table` (catalog feeds only `lower_query`'s ORDER BY, which stays fenced; HAVING already in
  the plan). **Decisive scope grounding (Codex): keep `lower_select`, do NOT delegate to `lower_query`** —
  the structural fences (nested-derived, joins-inside, EXISTS-decorrelation) live in
  `validate_derived_select_shape` *before* `lower_select`, so delegating would bypass them. Alias columns
  rename aggregate outputs via the 104 path (105 coalesces redundant adjacent `Project`s where a final
  projection already exists); aggregate outputs stay non-transparent for elision (the receipt walk stops at
  `Aggregate` — inherited). No
  IR/binder/executor/PBC/storage/Wendelon/`information_schema` surface. **Reconciled two inverted guards
  (discipline #4, Codex's D9 catch the others missed):** the ADR 100 narrow-derived guard drops its
  `projection_contains_aggregate_call` + `group_by_is_empty` markers (keeps the structural ones), and the
  `derived_table_source_scope_rejects_general_subquery_features` behavior test drops its two aggregate
  reject-cases. Sort/limit-in-derived (a distributed barrier+ordering cut) and joins-around-derived held as
  follow-ons. Genuine 3-of-3 (Claude leaned narrow + conceded his iter-0 schemata-inert error; Codex
  withdrew schemata, grounded the no-`lower_query` correction + the ADR 100 guard retarget; Gemini
  converged from the broad delegation). Migration: `plans/impl/106_derived_aggregate_tables.md`.

- **ADR 107 — `information_schema.schemata`.** Promoted. The 4th finite read-only system relation,
  rounding out the 099/102 ready introspection surface (logical_types + tables + columns + schemata). Lists the
  tenant's **distinct namespaces from the latest table descriptors** (`schema_name TEXT NOT NULL`),
  descriptor-derived (a `BTreeSet` over `descriptor.namespace`) — **not a schema registry** (no
  `CREATE SCHEMA`, owner, catalog, or synthetic/empty schema). **Pure 102 reuse:** a 4th `SystemTableKind`
  variant + one explicit `bind_information_schema_relation` arm over the existing
  `refreshed_table_schema_descriptors` snapshot; coordinator-local `SystemTable` execution; **no walker
  sweep** (`SystemTableKind` has no exhaustive match outside the binder), no new `ExecutionPlan` variant, no
  Wendelon RPC/storage/PBC/executor surface. Empty → zero rows; refresh-failure fails binding; read-only
  namespace (mutation rejects). Confirmed non-inert: non-default namespaces are user-creatable via two-part
  DDL (`CREATE TABLE schema.t` → `TableRef::with_schema`). **Reconciled three ADR 102 guards** (finite-family
  3→4, explicit-dispatch +schemata, coordinator-local +variant) **+ two behavior sentinels** (read-only
  test's unknown sentinel `schemata`→`views`; `system_table_executes_without_storage_scan` +schemata).
  Genuine 3-of-3 (Gemini led; Codex's iter-2 added the 3rd guard retarget + the storage-scan retarget +
  refresh-failure gate the others missed; Claude retracted his iter-0 inert claim). Migration:
  `plans/impl/107_information_schema_schemata.md`.

- **ADR 108 — SQL `CASE` (value conditionals).** Promoted. The **first conditional-expression substrate** in
  Toron (and first IR+executor leap since the 087 nominal spine). One canonical searched-form
  `Expr::Case { when_then, else_expr }`; **simple CASE desugars to searched** in the planner (one path).
  Branches unify via the existing `arithmetic_output_type` lattice (identical + numeric widening; else reject
  at bind); missing `ELSE` → synthesized typed-NULL; result `LogicalTypeId::NAKED` (uniform-branch identity
  propagation deferred). **Residual** (PBC declines; no opcode), evaluated **eager-vectorized** via Arrow
  `zip` folded in reverse (first-WHEN-true wins). **The implementation bulk is a two-sided expression-walker
  ripple** — the logical `SqlExpr`/`Expr` planner visitors (nested-aggregate/subquery/window/qualified-column/
  hidden-sort-key/structural-hash) **and** the bound `BoundExprNode` arena walkers across `ir/execution.rs` /
  `binder.rs` / `pbc.rs` / `exec.rs` (the 10th arena variant; mirror `Like`). **Documented eager/lazy
  tradeoff** (the headline semantic the operator wanted legible): SQL CASE is nominally lazy, but vectorized
  CASE computes every branch then masks — *branchless evaluation at array granularity*, where only **total**
  ops are safe to speculate. ADR 084's div0→NULL makes the common `1/x` guard safe; **hard-erroring** kernels
  (overflow, strict `CAST`) in a masked branch still fail the query — pinned by `case_divide_guard_is_safe_via_null`
  + `case_branches_eval_eagerly_masked_hard_error_fails`, and a required `evaluate_case` doc-comment. **The
  `IF` fork resolved to ADR 109** (the whole sugar family), keeping 108 the clean substrate. Retargets the 074
  + pattern-predicates + 031 guards; adds `adr108_case_is_residual_bound_expression`. No
  plan/exec-variant/Wendelon/storage/PBC-opcode surface. Genuine 3-of-3 (Gemini led the capability swing;
  Claude added the eager-vectorized/branchless semantic; Codex grounded the two-sided walker surface +
  `arithmetic_output_type` + `else_input` synthesis). Migration: `plans/impl/108_sql_case.md`.

- **ADR 109 — SQL conditional-sugar family.** Promoted. Cashes out 108's `Expr::Case` as six SQL functions —
  `COALESCE`/`NULLIF`/`IF`/`IIF`/`IFNULL`/`NVL` — **pure planner desugars** name-matched in the
  `SqlExpr::Function` arm (TIME_BUCKET precedent), **zero new IR/bound/executor/PBC**. `IF`/`IIF`→2-branch
  CASE; `COALESCE`→first-non-null nested `IS NOT NULL` CASE; `IFNULL`/`NVL`→strict-2-arg `COALESCE`;
  `NULLIF(a,b)`→`CASE WHEN a=b THEN <NULL> ELSE a`. **The one wrinkle + its fix:** `NULLIF`'s `THEN NULL`
  (and explicit-NULL value/result sugar args like `COALESCE(NULL,x)`) need a typed NULL the planner can't produce (no
  typeless `ScalarValue::Null`); reuse Toron's **existing** `Expr::Literal(ScalarValue::Utf8(None))`
  untyped-NULL placeholder (the `typed_update_null_for_target` convention, `binder.rs:1074`) via **one shared
  `bind_case` hook** — exclude placeholder result branches from `case_common_type`, reject all-placeholder,
  replace with `typed_null_for_case`. Safe because explicit `NULL` is rejected (the sentinel is unambiguous in
  a CASE result; UPDATE-staging and CASE-result paths disjoint). Eager-eval + naked identity inherited from
  108. **Boundaries (each on its own merits):** `NVL2` (3-arg, different shape), bare `SELECT NULL`,
  procedural `IF…END IF` stay out. Guards `adr109_conditional_sugar_desugars_to_case` +
  `adr109_case_binder_types_untyped_null_sentinel`; 108 guards retained. Genuine 3-of-3 (Claude grounded the
  NULLIF-isn't-planner-only catch then conceded the deferral; Codex grounded the placeholder hook + the
  explicit-NULL value/result sugar enhancement; operator steered `IFNULL`/`NVL` in — "small + fits → include"; Gemini
  aligned). Migration: `plans/impl/109_conditional_sugar_family.md`.

- **ADR 110 — Conditional Logical-Identity Preservation.** Promoted. Closes the identity-erasure hole 108/109
  opened: every CASE result was `NAKED`, so `CASE WHEN c THEN customer_id ELSE customer_id END` silently
  dropped `CustomerId` — exactly as conditionals became broadly authored. **One binder-local rule** in
  `bind_case`: compute the CASE output logical identity over the **pre-cast** branches via the literal-aware
  `nominal_compatible` (not the too-permissive `reconcile_identity`) and `set_node_logical_type` on the node
  — which propagates to the projection schema **for free** via the existing 089 path (`schema_for_exprs`
  reads `root_logical_type`). Rules (reusing 087): uniform non-naked identity with neutral nakeds (literals /
  109 placeholders / missing-ELSE) → **preserve**; mixed non-naked, or non-naked + naked-non-literal →
  **reject** (today's silent `NAKED` becomes a `NominalTypeMismatch`, the same judgment `=`/`IN` make);
  **implicit physical widening of an identity branch → reject** (escape hatch = explicit `CAST`, per 087);
  all-naked → naked. **Identity flow, not elision** (CASE is computed, not a stored-scan receipt — 108 D7
  holds). All six 109 sugars inherit by construction. Retargets `case_result_is_naked` (→ all-naked stays
  naked) + adds `adr110_case_preserves_compatible_logical_identity`. No new IR/executor/PBC/Rune/Wendelon
  surface. Genuine 3-of-3 core; the **widening sub-decision was a grounded 2-of-3 reject** (Claude + Codex)
  — Gemini's erase-to-naked resolved against because `bind_comparison` rejects non-literal physical mismatch
  and coerces literals *toward* the column, so there's no erase precedent, and 087's implicit-erasure-reject
  doctrine governs (Claude conceded Rune `.case` → Codex's identity-repair, then conceded its own iter-2
  erase → reject on this grounding; Codex grounded shared-binder-first + the reject; Gemini converged on the
  core). Migration: `plans/impl/110_conditional_logical_identity_preservation.md`.

- **ADR 111 — Rune CASE Authoring.** Promoted. **Completes the conditionals arc across both frontends** — SQL
  authors CASE directly (108), via sugar (109), with identity preserved (110); 111 lets Rune *construct the
  same node*. A **pure authoring admission** in the 080/086/101 pattern over 108's `Expr::Case`, **zero engine
  change**, `rune_engine.rs`-localized. Shape: a `RuneWhenThen { condition: Expr, result: Expr }` host carrier +
  free `when(condition, result) -> RuneWhenThen` + free `case(branches: Vec<RuneWhenThen>, else_expr) ->
  VmResult<RuneExpr>` building `Expr::Case { when_then, else_expr: Some(...) }` with **authored order preserved**,
  registered in `create_toron_module`. Searched form only (`case([when(c, t), ...], else)`); top-level
  constructor, **not** a receiver method (searched CASE has no natural receiver). **The only Rune-side check is
  the schema-free non-empty precondition** (`VmResult::panic` — the `in_list` precedent, vs infallible `LIKE`);
  everything substantive is **inherited** — Boolean-condition/type-unify/eager-vectorized eval + residual-only
  PBC from 108, and **110 logical-identity preservation** (uniform identity carries; mixed/naked-non-literal/
  identity-widening reject) by construction through `bind_case`. **Required `else_expr` in V1** (missing-ELSE
  would expose the 109 `Utf8(None)` typed-NULL placeholder before the explicit-NULL ADR); **no shortcuts**
  (simple-CASE/`if_else`/fluent builder/method alias deferred — authoring conveniences, not substrate). The arc
  landed in the right order — shared binder (110) fixed *before* the surface — so Rune CASE binds over
  already-correct semantics. Adds `adr111_rune_case_lowers_to_existing_expr_case` (pins the desugar-only nature
  + the absence of local binder/executor/PBC policy markers in the Rune engine); retargets the Rune
  surface-name discipline (admit `case`/`when`; `select`/`match`/`if`/`else`/`as`/`is`/`in`/`not`/`safe_cast`
  stay out); 108/110 CASE-policy guards reused, not restated. No SQL/IR/binder/executor/PBC/storage/Wendelon
  change. Genuine 3-of-3 (Claude + Codex held required-else/no-shortcuts/binder-owned-schema-policy; Gemini's
  iter-1 fork — optional-else/`if_else`/Rune-side Boolean check — converged to that shape in iter-2; Claude
  conceded its iter-0 infallible lean to `VmResult` on the `in_list` non-empty precedent). Migration:
  `plans/impl/111_rune_case_authoring.md`.

- **ADR 112 — Explicit `NULL` Literal.** Promoted. Retires the `Utf8(None)` sentinel hack (109's own
  `binder.rs:4873` to-do: *"Revisit … if general explicit NULL literals become supported"*) and admits explicit
  `NULL` across the SQL expression surface. **Doctrine: NULL is typed *late* by context, never by pretending to
  be Utf8.** A new **transient `Expr::UntypedNull`** (the `SqlLogicalCast`/`LogicalAscription` precedent —
  binder-consumed, **never in `BoundExprNode`**, so no executor-kernel/PBC/`NullArray`/Wendelon substrate change;
  one executor evaluator nuance is `evaluate_in_list` switching to Kleene OR for residual IN-with-NULL 3VL) lowers from
  `Value::Null` (lifting the `:3690` reject); the binder materializes it to a typed `ScalarValue::T(None)`
  wherever a **local context supplies a physical type** — designated slots (`CAST(NULL AS T)` physical **and**
  logical, `SET col = NULL`, CASE result + missing-ELSE, Boolean/null-test positions) **and** typed sibling operands
  (comparison, arithmetic, IN) — and **rejects** where no type is available, pointing at `CAST(NULL AS <type>)`
  (reject is the default arm → sound by construction; one `typed_null_for_data_type` materializer replaces the
  expression sentinel typers). **Residual-safe for free:** PBC already declines typed NULL (`emit_literal` catch-all
  → `NullLiteralUnrepresentable` `pbc.rs:231`), so `WHERE x = NULL` is 3VL residual `EvaluateInToron` with no
  Wendelon opcode; the **`NOT IN (NULL) → zero rows`** trap (`x <> NULL` is NULL ⇒ conjunction never TRUE) is a
  pinned gate. `CAST(NULL AS CustomerId)` carries identity via the existing 092/093 `reconcile(naked, T) = add`
  path; the six 109 sugars desugar through normal `lower_expr` (no placeholder); 110 identity + the all-NULL
  reject preserved (NULL branch identity-neutral by source shape). Rune `lit_null()` admits the same marker —
  `case([when(c,t)], lit_null())` delivers the 111 D5 missing-ELSE unblock. **The complete explicit-NULL cut in
  one ADR** (operator anti-granularity steer): the only deferrals are genuine boundaries — contextless `SELECT
  NULL` (needs a `DataType::Null`/`NullArray` *execution* substrate) and `NVL2` (a 109-family sugar, off-topic).
  Guards `adr112_explicit_null_is_transient_binder_consumed` / `adr112_null_sentinel_retired` /
  `adr112_untyped_null_rejects_without_context` / `adr112_null_predicates_stay_residual`; reconcile the 109/110 +
  Rune surface-name guards. No bound-arena/executor-kernel/PBC/Wendelon/storage substrate change beyond the
  `or_kleene` residual IN-list correction. Genuine 3-of-3 on the carrier +
  context-typed scope (Gemini conceded `ScalarValue::Null` → transient
  carrier + default-Utf8 → reject; Claude conceded its comparison/arithmetic *deferral* on the verified binary-op-
  contract + PBC-residual grounding — the small-cut-ossification self-check — and added the PBC-residual-mechanism
  pin + the `NOT IN (NULL)` trap gate; operator resolved `CAST(NULL AS LogicalType)` → include, Codex owning the
  impl). Migration: `plans/impl/112_explicit_null_literal.md`.

- **ADR 113 — Aggregate-Output Logical-Identity Preservation.** Promoted. Closes the **last big identity-flow
  leak** after 087–110 + 112: `aggregate_field` (`binder.rs:5688`) emitted every aggregate output NAKED, so
  `MIN(customer_id)` dropped `CustomerId`. **Doctrine: selection preserves identity, computation erases it** —
  `MIN`/`MAX` return an actual element of the input domain → carry its identity; `SUM`/`AVG`/`COUNT`/`COUNT(*)`/
  `LinearRegress`/`ApproxQuantile` compute a new value (closure isn't a declared logical-type property) → NAKED.
  Total partition over all 8 `AggregateFunction` variants. **Mechanism:** one binder helper
  `aggregate_output_logical_type(call, output_type)` reads `call.args.root_logical_type(0)`/`root_type(0)` and
  preserves under `Min`/`Max` **iff** the output physical exactly equals the input root physical (a defensive
  mirror of 110's implicit-widening reject; guaranteed by MIN/MAX's `SameAsInput` rule, `aggregate_kernel.rs:82`/
  `:92`); `aggregate_field` stamps via the existing 089 `field_with_logical_type_metadata` writer (no new
  emitter). Output physical = input physical ⇒ identity rides with no widening and the **089 round-trip invariant
  holds** by construction. Computed/cast-erased/naked inputs stay naked automatically (arg root identity is
  NAKED); `MIN(CAST(customer_id + 0 AS CustomerId))` is the explicit ascription escape. Identity flows downstream
  (projection / alias / CTAS persist-resync / join-key nominal checks) via the 088/089 decode-reemit loop; group
  keys already preserved (089 D6). **`SUM` stays naked** — physical sameness ≠ nominal identity, summing IDs is
  nonsense, and `SumNumeric` widening shows a physical-preservation rule would be partial even before the
  semantic issue (a declared-additive/"unit" logical-type property is the deferred follow-on for
  `SUM(Money) → Money`). Retargets
  `adr089_group_keys_preserve_identity_aggregate_outputs_naked` (→ group keys **and** selector aggregates
  preserve; computed/cardinality/statistical aggregates stay naked) + the `aggregate_output_stays_naked`
  behavior test; adds `adr113_min_max_preserve_aggregate_identity` +
  `adr113_aggregate_kernel_stays_physical_only`. Behavior-additive, Toron-only; **no**
  IR/executor/PBC/Wendelon/storage/kernel change. Genuine 3-of-3 on the partition; the `SUM`-naked sub-decision
  was a grounded 2-of-3 (Claude + Codex) that Gemini conceded at iter-2 → unanimous (Codex deferred the topic at
  iter-0 leaning contextless-NULL, then converged once Claude's selection-vs-computation partition answered the
  `SUM(CustomerId)` worry; the physical-equality guard + named helper came from the Codex/Gemini iter-2
  convergence). Migration: `plans/impl/113_aggregate_output_logical_identity.md`.

- **ADR 114 — Contextless `NULL` Projection Substrate.** Promoted. Finishes the NULL arc 112 named: a contextless
  *result* NULL (`SELECT NULL`, all-null `CASE`/`COALESCE`) binds to Arrow `DataType::Null` and executes as a
  `NullArray` of batch length, instead of rejecting. **Doctrine: untyped NULL stays non-executable
  (`Expr::UntypedNull`, 112); `DataType::Null` is a result type of last resort — never a coercion participant.**
  **Representation: `BoundExprNode::NullLiteral`** — a result-only bound-arena leaf (`append_null()`, type
  `DataType::Null`/nullable/NAKED), **not** `ScalarValue::Null` (operator-steered: a `ScalarValue` arm would put a
  result-only artifact on the public scalar surface — `Expr::Literal`/system rows/insert/PBC/tests — degrading
  containment to defensive rejects; the bound node makes containment **structural**, only the binder constructs it,
  and `ScalarValue` stays typed-only so 112's `!scalar_enum.contains("Null")` guard holds). **Scoping hinge:**
  materialization is opt-in at **result-projection roots only** (a `NullRootPolicy::MaterializeResultNull` path at
  SELECT `Project` including derived SELECT outputs / `RETURNING` — and **Rune `.project` inherits free** via
  `LogicalPlan::Project`); the generic `bind_expr_into_program` reject is unchanged, so `WHERE NULL`/`GROUP BY NULL`/
  `SUM(NULL)`/operand NULL keep 112's behavior (Gemini's iter-1 blanket materialization was rejected for mistyping
  exactly these). All-null `CASE` (incl. missing-ELSE) resolves to `DataType::Null` via a `materialize_null_for_output_type`
  helper that leaves `typed_null_for_data_type` (the 6 physical) narrow; the whole 109 sugar family inherits
  (`COALESCE(NULL[,NULL])`, `IFNULL`/`NVL(NULL,NULL)`, `NVL2(a,NULL,NULL)`, `NULLIF(NULL, amount)` → Null while
  `NULLIF(NULL,NULL)` still rejects). `DataType::Null` is not a coercion participant — `COALESCE(NULL,NULL) + 1`
  rejects. **CTAS boundary:** persisted columns require a declared type — `CREATE TABLE AS SELECT NULL` rejects;
  `… SELECT CAST(NULL AS BIGINT)` persists nullable `Int64` (the explicit escape). `NVL2(a,b,c)` folded in as the
  last 109 sugar straggler (pure `CASE WHEN a IS NOT NULL THEN b ELSE c` desugar; no `OP_NVL2`/bound node/Rune
  surface). Executor reuses `new_null_array`; **no** `ScalarValue::Null`/PBC opcode/Wendelon/storage carrier.
  Genuine 3-of-3 on doctrine + scope; representation operator-resolved to B (Codex + Claude leaned B for
  containment, Gemini's `ScalarValue::Null` superseded); the small-wins sweep (sugar family, `RETURNING NULL`, Rune
  `lit_null()`) inherits through the all-null-CASE + result-projection chokepoints. Migration:
  `plans/impl/114_contextless_null_projection.md`.

- **ADR 115 — Composable Ordered Top-K.** Promoted. **Pivots off the 108–114 expression-substrate spine onto the
  distributed-execution axis:** makes `ORDER BY` + `LIMIT`/`OFFSET` a composable operator inside derived sources
  and in Rune (`.sort`/`.limit`/`.offset`), reusing the existing `LogicalPlan::Sort`/`execute_sort` +
  coordinator-final single-partition gather (`Exchange{partition_count:1}`) — **no new IR/executor kernel/PBC/
  storage**. **Doctrine:** `ORDER BY` orders rows at-a-point (not durable metadata); `ORDER BY`+`LIMIT` is *ordered
  top-k* (selects rows before later operators). **Fork A (3-of-3): one global sort barrier, a capped reusable
  primitive.** **Fragmenter grounding (Claude):** `fragment_plan` is a flat single-exchange-boundary splitter
  (`==0`→1 stage, `==1`→producer+consumer, `>1`→`MultipleExchangeBoundaries`); a global sort barrier contributes
  one empty-key gather exchange, so simple sort-over-scan rides the existing `==1` split, while aggregate+sort
  reuses/generalizes the existing sort-over-aggregate exception. The V1 cap is **barrier-specific**
  (`global_sort_barrier_count > 1` → `multiple global sort barriers are not supported in V1`), not raw
  `count_exchanges > 1` and not a new bespoke shape; A→B is a contained recursive barrier-splitter rework
  (scheduler already multi-stage), so the frontend/IR/executor work is the shared non-throwaway foundation.
  **SQL/Rune asymmetry (operator-steered, D9):** the cap is
  an *implementation* cap, not Rune doctrine — SQL stays declarative (multi-barrier rejection fine indefinitely),
  but Rune `.sort().limit()` boundaries are *authorial* (meaningful, lazy) and **repeated barriers are a valid
  future Rune workflow** — Rune is the reason to expect the cap lifted, and future multi-barrier work must preserve
  authored chain boundaries (no flatten without proven equivalence); this hardens the named-primitive requirement.
  Lifts only the derived **query-tail** fence (`validate_derived_query_shape`), keeps `validate_derived_select_shape`
  + the ADR-106 no-`lower_query`-delegation correction (shared `apply_order_by_limit_tail` helper); `LIMIT` requires
  `ORDER BY` (bare-`LIMIT` cheap *take* is a deferred separate operator, **not** a `Sort` special case); Rune typed
  `RuneSortKey` + `asc`/`desc`; one barrier may feed unary consumers + coexist with a grouped-aggregate shuffle.
  Genuine 3-of-3 (Codex surfaced the B tradeoff iter-1, Claude+Gemini converged A, Codex iter-3 ratified; Claude
  grounded the barrier-count vs raw-exchange-count fragmenter reality; operator added the SQL/Rune asymmetry).
  Migration:
  `plans/impl/115_composable_ordered_top_k.md`.

- **ADR 116 — Unordered Bounded Take (cheap bare `LIMIT`).** Promoted. The operator's BigQuery-cost goal +
  Toron's reason for existence: `SELECT * FROM huge_table LIMIT 3` with **no `ORDER BY`** returns any 3 qualifying
  rows by the **cheapest** path — one bounded Wendelon request answers directly and stops when that source can
  produce `n` rows; if it underfills, the engine continues or falls back to preserve SQL cardinality.
  **A distinct operator, not a weaker sort** (`LogicalPlan::Take`/`ExecutionPlan::Take`, schema-preserving, not a
  global sort barrier, doesn't route through 115's `Sort`); a **deliberate opt-out of the 010/015
  deterministic-ordering doctrine** (no order/stability/fairness/repeatability/randomness/identity), keeping only
  the SQL **cardinality contract `min(limit, cardinality)`** (not best-effort — continue to more sources if one
  underfills). **Three layers:** (A) the `Take` operator; (B) executor early-stop = the *correctness floor*
  (coordinator-local, slice final batch, cancel upstream, `LIMIT 0` free); (C) storage pushdown `ScanRequest.row_limit`
  = the *cheap path*. **Cross-ring B is required (grounded):** `WendelonStorageScan::scan` `collect_all()`s the
  table query *before* `ScanStream::from_batches` (`storage.rs:853`), so the scan is already done by the time
  Toron streams — executor `Take` only trims; **Wendelon must honor the bound.** Narrow chain: `ScanRequest.row_limit`
  → `TableQueryOptions.row_limit` → an **explicit** field in the fixed `TableQueryRequestMeta` (`0`=absent, **not**
  `_pad`) → server decode → `handle_table_query` **post-PBC early-return** (a *success bound, not a budget* —
  count post-PBC rows, return `OK` not `QUERY_BUDGET_EXCEEDED`, don't lower `max_rows`; secondary-index honors or
  falls back). **Pushdown safety:** set `row_limit` only when post-PBC = post-storage (`Take(Scan)`/`Take(Project(Scan))`/
  `Take(Filter(fully-pushed-PBC,Scan))`); never below a residual filter or row-identity-append (→ executor `Take`).
  **`LIMIT 0`/wire-`0` trap:** short-circuit `LIMIT 0` before issuing a Wendelon request (`0` means absent). Rune
  `.take(n)` is the honest unordered surface; `.limit` stays 115 sort-attached; unordered `OFFSET` rejected.
  Genuine 3-of-3 (Codex iter-0 full design; Gemini re-drafted off a stale 114 mis-file; **operator resolved
  cross-ring → B, correcting Claude's Fork-A grounding miss — the `collect_all` materialization barrier**; Codex
  iter-2 sharpened the `LIMIT 0`/wire trap, success-bound-not-budget, secondary-index, row-identity carve-out, and
  the fragmenter single-source-first/fanout-fallback ladder). Migration: `plans/impl/116_unordered_bounded_take.md`.

- **ADR 117 — Multiple Ordered Top-K Barriers.** Promoted. Relaxes 115's single-barrier cap by **recursing the
  `global_sort_barrier` named primitive** — `fragment_plan` goes from a flat single-boundary dispatcher to a
  **recursive stage-splitter**. **Fragmenter-only** (the scheduler already runs multi-stage DAGs); reuses `Sort`/
  `execute_sort` + the empty-key single-partition gather `Exchange`; **no IR/executor/Wendelon/merge-sort/top-k-
  pushdown.** **Rune is the reason (115 D9):** `df.sort([desc(revenue)]).limit(100).sort([asc(created_at)]).limit(10)`
  = "the 10 oldest among the top 100 by revenue" — each authored `.sort().limit()` is a preserved gather+sort
  stage (no flatten/reorder/advisory-demotion); SQL gets it free if its IR is valid (no SQL rewrites). **Two
  load-bearing seams:** (1) **a threaded `FragmentIdAllocator`** — every path hard-coded `StageId::new(0)/(1)`/
  `ExchangeId::new(0)` (`fragment.rs:146`), so monotonic allocation is required for N barriers → N+ uniquely-id'd
  stages (Claude); (2) **an explicit terminal-output parameter** `FragmentTerminalOutput::{FinalResult,
  ShuffleProducer{..}}` (Codex's hinge) — because `TaskOutputBinding::ShuffleProducer` isn't just metadata (the
  worker publishes its batches *as partitions*), a subplan feeding a parent barrier is fragmented with a requested
  `ShuffleProducer` terminal and its terminal stage **wrapped in a matching `Exchange`**, not built-as-`FinalResult`-
  then-mutated; only the final stage emits `FinalResult`. Base fragmenters (scan/`==1`/aggregate/join) become
  allocator- + terminal-output-aware; **barriers compose *above* a fragmented join base but not *inside* join
  inputs** (pinned reject — the resolved fork). 115/116 (single barrier, ordered `OFFSET` per-barrier, unordered
  `Take` transparent) unchanged. Genuine 3-of-3 — Claude conceded the streaming lead (grounded: `handle_table_query`
  pre-materializes `plan.batches`, so transport streaming doesn't bound remote cost — that's a separate 2-ADR arc),
  Codex contributed the terminal-output hinge, Claude grounded the id-allocator; join fork resolved to
  compose-above. Retargets `adr115_multiple_global_sorts_reject_with_pin` → positive recursive split + strengthens
  the named-primitive guard. Migration: `plans/impl/117_multiple_ordered_topk_barriers.md`.

- **ADR 118 — Stream-Shaped Storage Boundary (Execution-Pipelined Table Query).** Promoted. The named V1
  "stream-shaped storage boundary": makes the Wendelon table-query scan **streaming end-to-end** so a
  cancelled/dropped Toron consumer **stops Wendelon before it scans all qualifying row groups** — turning 116's
  early-stop/059-060 cancellation into a *remote*-cost bound (not just a local trim), and laying the foundation for
  pipelined cross-stage execution (117's recursive fragmenter + this = the executor-model-upgrade substrate).
  **Cross-ring** (Wendelon server/protocol + `wendelon-client` + Toron bridge); **no Toron IR/SQL/Rune/PBC.**
  **The obstacle was the *metadata* path, not failure** (Toron already fails a stream cleanly): the first frame's
  `QueryResponseMeta` carries final `status`/`value_hlc`/`rows` (`encode_query_stream_batches_with_deadline`
  pre-materializes because "continuations have no status surface", `data_server.rs:13793`). **Resolution: the
  terminal-trailer status contract** — first `TABLE_QUERY_RESP` frame provisional (`status=OK` "stream opened"),
  continuation frames Arrow-only (reusing `FrameHeader.arrow_len`), **terminal frame = authoritative final meta +
  `STREAM_END`**; **`QueryResponseMeta` is NOT widened** — provisional-vs-terminal is distinguished by frame
  position + an **opt-in cursor mode** (the generic stream still rejects continuation metadata). `handle_table_query`
  refactors from "materialize `plan.batches` then respond" into an **incremental producer** that checks
  cancellation + 116 `row_limit` **between yields** (full-scan **and** secondary-index, for access-path-independent
  cost); **partial-output-then-terminal-error** is the defined contract (Toron maps a non-OK terminal → `ScanStream`
  error → query fail, no partial success; collecting wrappers discard partial → error). Toron `WendelonStorageScan`
  consumes lazily via `table_query_stream*` (no `collect_all`). Genuine 3-of-3 — **Codex retracted his iter-0
  A-only lean** (transport-de-collect alone under-delivers the cost goal); Gemini grounded the failure path, Codex
  the metadata path, Claude collapsed them into the trailer contract; the trailer shape (reuse meta, no widening)
  + secondary-index parity + opt-in cursor mode all converged. Implementation *size* (the producer refactor) is
  the named risk, not direction. Migration: `plans/impl/118_stream_shaped_storage_boundary.md`.

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

### ADR 121 promoted; ADR 122 candidates

ADR 121 selected and promoted **general qualified relation-scope binding** — scalar qualifier resolution
generalized across SELECT/WHERE/HAVING/GROUP BY/aggregate args/ORDER BY (and the existing JOIN-key
context), using the existing `ColumnRef { qualifier: Option<TableRef> }` IR (no new variants); a
binder-internal `RelationScope` sidecar carries `(qualifier, visible_column_name) → (output_ordinal,
physical_type, nullability, logical_identity)` with explicit scope propagation per plan node and
alias-hides-base-qualifier semantics, including alias-column-list renamed names under the alias
qualifier; named/actionable qualifier errors cover unknown relation, ambiguous relation, and unknown
column within relation, replacing four generic "not supported" strings plus one visible-name
qualifier gate; Rune gains `RuneDataFrame::alias` + free `qcol`. **Genuine 3-of-3 — Claude
conceded twice to Codex's grounding**: iter-0 (joins/aliases/qualifier infrastructure all landed —
[[feedback_ground_the_premise_before_sketching_scope]]) and iter-1 (wildcard expansion has no catalog
access at the SQL-planner expansion site; same lesson recursively at sub-features). The 9-site reject
surface partitions cleanly: 5 scalar sites converted (pinned by G1 — four strings gone, visible-name
qualifier shape relaxed); 4 wildcard/RETURNING sites stay rejected verbatim (pinned by G2 — the
deferred non-scope is explicit and can't be silently relaxed, including qualified RETURNING's
`expr_contains_qualified_column` helper). With 115/116/117/118/119/120/121 the execution substrate
is complete through the coordinator surface AND qualified column authoring is general at the SQL/Rune
frontends.
The next frontier is either the joins-customer side (Rune `.join(...)` direct authoring, multi-upstream
live pipelining) or the 121 named follow-ons (qualified wildcards, qualified DML RETURNING). **Scope
each candidate as a complete cut, not a ~100-line granular slice** (standing operator steer).

*121 named follow-ons (deferred non-scope, pinned by G2):*

- **Qualified wildcards (`t.*` + mixed wildcard)** — requires planner↔binder restructuring (move
  wildcard expansion into the binder where catalog access lives) or a `Wildcard` IR variant.
  Substantial; either approach is a distinct cut from "convert a reject string."
- **Qualified DML RETURNING (`INSERT … RETURNING t.id` + RETURNING wildcard)** — same machinery as
  wildcards + DML target alias semantics.
- **DML target aliases / qualified DML assignment targets** — `UPDATE t AS x SET x.col = ...` and
  similar; requires DML target alias surface that doesn't exist today.

*Compute customer frontier (now-unblocked by 121):*

- **Rune `.join(...)` direct authoring** — over the existing `LogicalPlan::Join`. 121 made qualified
  authoring general, which is the prerequisite Rune-side joins needed. Smaller cut than the V1 join
  customer itself (which 121 also unblocks).
- **Multi-upstream live pipelining** — relax 117's multi-upstream acceptance-layer barrier for the
  streamable subset, letting joins / multi-input compositions pipeline across both upstreams. **Now
  genuinely customer-ready** once Rune `.join(...)` lands (joins authorable → multi-upstream
  pipelining has live customers to exercise it; previously the [[feedback_cut_must_deliver_the_name]]
  trap of "barrier relaxation without customer evidence").

*120 V2 follow-ons (named at promotion):*

- **Bounded-buffer whole-record emit** — frame-overhead reduction for the 120 per-location emit via a
  size-bounded accumulator with periodic flush. Recovers the rebatch grouping's efficiency without
  reintroducing the post-scan barrier. Honesty boundary: any buffer makes the cost-bound's failure mode
  timing-dependent; carries its own "buffer bounded ≤ N rows AND emits within K millis" gate.
- **Configurable cursor depth** — D8's fixed `mpsc::channel(16)` becomes `QueryBudget::result_cursor_depth`.
  Probably premature until cursor backpressure shows observable cost in practice.
- **Cursor surface extended to DDL/DML returning paths** — 120 V1 was read-query-only (D6 boundary);
  RETURNING clauses + mutation-source-returning paths still take the collecting path. Extending is a
  follow-on, gated on a real consumer.
- **External SQL wire result streaming** — stream `ResultBatch` frames from the daemon as cursor batches
  arrive, including the audit-fail-closed pre-encode ordering question.

*Distributed / execution continuation (115–121 descendants):*

- **Streamable-plan coverage extension** — bounded-memory streaming shapes for `Aggregate`/`Sort` (top-k
  bounded buffers; partial+merge for aggregates) that promote them from non-streamable to live-eligible
  producers, widening 119/120's streamable subset.
- **Row-group-lazy-from-disk scan** — push the laziness deeper into Wendelon storage (per disk page rather
  than per row group) so very large row groups don't materialize. A storage-internal follow-on to 118.
- **Joins-around-derived / GROUP BY over joins** — explicitly named non-scope of 121; substantive
  next-cut on the joins surface once Rune `.join(...)` lands.

*Identity-tier continuation (087–114 descendants):*

- **Logical-type lifecycle — `DROP`/`ALTER LOGICAL TYPE`** — only `CREATE` exists; completes the logical-type
  story. Net-new IR + cross-ring DROP (bounded 090 re-open) + referential-integrity guard (can't drop a
  referenced type).
- **Declared-additive logical types** — the deferred half of 113: a "unit/additive" property so `SUM(Money) →
  Money` preserves identity. Needs a logical-type property + a `CREATE LOGICAL TYPE` extension; narrower.

*Conditionals leftovers (small, ride-along only):*

- **Rune `if_else` / simple-CASE shortcuts** (111 D9, sugar over landed `case`/`when`); masked/lazy CASE eval
  (only if 108's eager hard-error exposure bites); far-horizon `OP_CASE` pushdown. None is a standalone cut.

*Optimizer tier (105's own named follow-on):*

- **General project-fusion** — expression substitution / reorder folding / `Project(Filter(Project))`
  collapse, beyond 105's adjacent-identity case. Evidence-gated: earns a cut only if project chains persist
  after 105, and carries its own metadata / ordinal / eager-error proofs. 105's named deferrals also live
  here: **project-into-`Scan`** (base/wildcard scan-output rename folding) and standalone no-op
  identity-`Project` removal over non-`Project` children.

*Catalog introspection continuation:*

- **Further `information_schema` relations** — 107 landed `schemata` (the namespace view), closing the
  finite-family small cuts. Next are **constraints / indexes / PK-FK / views** — each gated on
  constraint/view/namespace modeling (the harder pieces), not a clean small cut like schemata was.

*Derived-source continuation (split, don't generalize):*

- **Derived-source continuation** — 106 took derived *aggregates*; the next slices are
  **sort/limit-in-derived** (a distributed barrier + ordering cut, pairs with Rune sort/limit) and
  **joins-around-derived** (needs qualifier + join-planning proof), then alias scoping / `IN` / scalar /
  correlated + decorrelation. Sequence the sub-features.
- **Projected-scan receipt** — only earns a cut if/when refinement elision is ever moved to run *after*
  `push_down_scan_projection` (then `Scan { projection: Some }` has a consumer). Deferred by ADR 100 D5.

*Identity authoring follow-ons:*

- **`DROP`/`ALTER LOGICAL TYPE`** — net-new IR variants + parsing + cross-ring DROP with referential
  integrity (can't drop a logical type a column still references).
- **PK/FK-derived identity** — "magic that follows constraints, not names": derive from `PRIMARY
  KEY`, inherit via `REFERENCES`; blocked on FK support (rejected at `sql_planner.rs:663`) + a
  canonical `schema.table.column` identity name.
- optional Wendelon-validate-at-authority (090 knob); **`.as_naked()`** Rune helper.

*Carried Rune leads (still live):*

- **Rune joins** — needs relation-alias + qualified-column ergonomics first.
- **`SIMILAR TO`/`RLIKE`/`REGEXP`** (regex pattern families, beyond 101's literal `LIKE`);
  **Rune sort/limit**; dot-field sugar; **Wendelon typed table-mutation trace continuation**. (SQL
  `CASE`/value-conditionals landed as ADR 108; Rune CASE authoring landed as ADR 111 — the conditionals arc
  is now complete across both frontends.)

*Cross-ring chore (operator-flagged):*

- **Wendelon `*V1`/`*v1` de-version sweep** — strip the version suffixes in Wendelon unless one has a
  strong reason (pre-release, no historical installs). A Wendelon-home chore (placement follows
  code-locality), needs an audit taxonomy (cosmetic suffix vs durable format label like `WDV1` vs
  compat-test fixture); not a Toron ADR.

*Carried:*

- subquery/`UNNEST`/row-value `IN`; `CHECK`/`DEFAULT`; `ALTER`/composite-PK/
  `UNIQUE` (cross-ring schema evolution); pre-op intent audit; fragmentable bulk DML;
  JWT/OIDC/SPIFFE; gRPC/Arrow Flight/Postgres frontends.

### ADR 120 promoted; ADR 121 candidates (HISTORICAL)

ADR 120 took the user-visible capstone of the 115→119 substrate arc: three hops streaming end-to-end
(Wendelon descriptor-backed whole-record per-location emit; Toron worker `drive_streamable_plan` sink
for streamable FinalResult sending `TaskBatch` frames before `TaskComplete`; coordinator
`execute_query_stream` cursor surface with spawn-driver lifetime and RAII drop-cancellation). The
bifurcated headline gates pin both halves (collecting `LIMIT 3` cheapness + caller-drop streaming
cancellation). ADR 121 followed up as the **authoring capstone**: scalar qualifier resolution
generalized across scalar `Expr::Column` positions, with Rune `alias`/`qcol` exposed,
alias-column-list names feeding relation scope, and named/actionable qualifier errors; the 9-site
reject surface partitions into 5 converted (G1: four strings gone plus visible-name qualifier gate
relaxed) and 4 stays-rejected-as-explicit-non-scope (G2). Detail in
`plans/adr/120_end_to_end_bounded_first_rows.md` +
`plans/adr/121_general_qualified_relation_scope_binding.md` + the active-line entry above.

### ADR 119 promoted; ADR 120 candidates (HISTORICAL)

ADR 119 took the consumer-side capstone of the 115→118 substrate arc: in-flight scheduler frontier +
expected-dependency readiness + explicit `ShufflePartitionRef::{Sealed,Live}` + partition store
open/append/seal/cancel lifecycle + worker streamable producer/consumer paths bypassing the existing
`Vec<RecordBatch>` barriers, with non-streamable shapes (Sort/Aggregate/Join) + full-fan-in hash shuffles
falling back to sealed manifests explicitly. Multi-upstream stages kept 117's acceptance-layer barrier
exact. ADR 120 followed up as the **user-visible capstone**: cursor surface + worker→coordinator TaskBatch
streaming + Wendelon whole-record per-location emit, closing the cost-bound chain `SELECT * FROM
huge_table LIMIT 3` → caller drop → Wendelon stops mid-scan regardless of access path. Detail in
`plans/adr/119_pipelined_cross_stage_execution.md` + `plans/adr/120_end_to_end_bounded_first_rows.md`
+ the active-line entry above.

### ADR 118 promoted; ADR 119 candidates (HISTORICAL)

ADR 118 took the named V1 "stream-shaped storage boundary": multi-frame `TABLE_QUERY_RESP` + terminal-trailer
status (no `QueryResponseMeta` widening) + Wendelon incremental production that stops on cancel/row-limit
before scanning all row groups + Toron lazy `ScanStream` — so a dropped consumer bounds *remote* cost on the
projected path. ADR 119 followed up as the **consumer-side capstone**: in-flight scheduler frontier + live
partition lifecycle + worker streamable-plan path, so cross-stage pipelining lets the 118 cost-bound chain
land end-to-end (downstream-take → live cancel → Wendelon scan stops). Detail in
`plans/adr/118_stream_shaped_storage_boundary.md` + `plans/adr/119_pipelined_cross_stage_execution.md` +
the active-line entry above.

### ADR 117 promoted; ADR 118 candidates (HISTORICAL)

ADR 117 made the fragmenter a recursive stage-splitter (multiple ordered top-k barriers). 118 took the named V1
"stream-shaped storage boundary": multi-frame `TABLE_QUERY_RESP` + terminal-trailer status (no `QueryResponseMeta`
widening) + Wendelon incremental production that stops on cancel/row-limit before scanning all row groups + Toron
lazy `ScanStream` — so a dropped consumer bounds *remote* cost (the general case 116's cancellation couldn't reach).
Codex retracted his A-only lean (transport de-collect under-delivers); the metadata path (not failure) was the
obstacle, resolved by the trailer contract. Detail in `plans/adr/118_stream_shaped_storage_boundary.md` + the
active-line entry above.

### ADR 116 promoted; ADR 117 candidates (HISTORICAL)

ADR 116 made unordered bounded take (`Take`) cheap (Wendelon `row_limit`). 117 lifted 115's single-barrier cap:
`fragment_plan` became a recursive stage-splitter over the `global_sort_barrier` primitive (threaded id allocator
+ explicit `FragmentTerminalOutput`), so authored Rune `.sort().limit()` chains fragment into correct multi-stage
DAGs (barriers compose above join bases, not inside join inputs). Claude conceded the streaming lead (grounded:
`handle_table_query` pre-materializes before responding, so streaming is a separate 2-ADR arc). Detail in
`plans/adr/117_multiple_ordered_topk_barriers.md` + the active-line entry above.

### ADR 115 promoted; ADR 116 candidates (HISTORICAL)

ADR 115 made *ordered* top-k composable (one global sort barrier). 116 took the opposite branch — **unordered
bounded take**: bare `LIMIT n` (no `ORDER BY`) → a distinct `Take` operator returning `min(n, cardinality)` rows
by the cheapest path, with Wendelon honoring a `row_limit` (post-PBC early-return via a widened fixed table-query
meta) so one source answers directly when it can produce `n` rows. Cross-ring resolved → B after grounding that
`WendelonStorageScan::scan` `collect_all()`s before Toron streams (executor early-stop alone only trims). Detail in
`plans/adr/116_unordered_bounded_take.md` + the active-line entry above.

### ADR 114 promoted; ADR 115 candidates (HISTORICAL)

ADR 114 closed the NULL arc (contextless `DataType::Null` projection). 115 pivoted off the expression-substrate
spine to the distributed axis: **composable ordered top-k** — `ORDER BY`+`LIMIT` in derived sources + Rune
`.sort`/`.limit` over the existing `Sort`/gather substrate, Fork A (one global sort barrier as a capped reusable
primitive; simple shapes reuse the existing `==1` split, aggregate+sort reuses the aggregate-sort path, and the V1
cap counts global sort barriers rather than raw exchanges). The operator set the SQL/Rune asymmetry doctrine (cap
is implementation-only; Rune authored barriers drive lifting it). Detail in
`plans/adr/115_composable_ordered_top_k.md` + the active-line entry above.

### ADR 113 promoted; ADR 114 candidates (HISTORICAL)

ADR 113 closed the identity-flow leak (`MIN`/`MAX` carry identity). 114 finished the NULL arc 112 named:
contextless result NULL (`SELECT NULL`, all-null `CASE`/`COALESCE`) materializes `DataType::Null`/`NullArray` at
result-projection roots via a result-only `BoundExprNode::NullLiteral` (operator-steered over `ScalarValue::Null`
for structural containment), scoped so non-result positions keep 112's reject; `NVL2` folded in; CTAS requires a
declared type (`CAST(NULL AS T)` escape). Detail in `plans/adr/114_contextless_null_projection.md` + the
active-line entry above.

### ADR 112 promoted; ADR 113 candidates (HISTORICAL)

ADR 112 retired the `Utf8(None)` NULL sentinel (transient `Expr::UntypedNull`, typed late). 113 closed the last
identity-flow leak: `MIN`/`MAX` aggregate outputs now carry the input column's logical identity (selection
preserves, computation erases — `SUM`/`COUNT`/`AVG`/derived stay naked), via one binder helper + the 089 writer,
zero substrate change. The `SUM`-naked sub-decision was a grounded 2-of-3 (Gemini conceded its preserve-when-not-
widened lean). Detail in `plans/adr/113_aggregate_output_logical_identity.md` + the active-line entry above.

### ADR 111 promoted; ADR 112 candidates (HISTORICAL)

ADR 111 completed the conditionals arc at the surface (Rune CASE authoring). 112 selected **explicit NULL
literal** — the conditionals/NULL-arc continuation that retired the 109 `Utf8(None)` sentinel: a transient
`Expr::UntypedNull` typed late by the binder (CAST physical+logical / assignment / CASE / Boolean/null-test /
comparison / arithmetic / IN) or rejected, with Rune `lit_null()` delivering the 111 D5 missing-ELSE unblock. The operator
steered the cut to span the *complete* explicit-NULL surface (anti-granularity), deferring only contextless
`SELECT NULL` (a `NullArray` execution substrate) and `NVL2`. Detail in `plans/adr/112_explicit_null_literal.md`
+ the active-line entry above.

### ADR 110 promoted; ADR 111 candidates (HISTORICAL)

ADR 110 closed the conditional identity-erasure hole (`bind_case` preserves uniform branch identity through
the 087–098 spine). 111 completed the conditionals arc at the surface — Rune CASE authoring as a pure
080/086/101-pattern admission over 108's `Expr::Case` (`RuneWhenThen` carrier + `when`/`case` constructors,
`VmResult`-reject empty, required else), inheriting 108 eval + 110 identity by construction, zero engine
change. The Rune `.case` lead was the operator-locked topic; explicit-NULL / `if_else` shortcuts deferred.
Detail in `plans/adr/111_rune_case_authoring.md` + the active-line entry above.

### ADR 109 promoted; ADR 110 candidates (HISTORICAL)

ADR 109 promoted the SQL conditional-sugar family (six functions desugaring onto 108's `Expr::Case`). 110
closed the identity-erasure hole the conditionals arc created — `bind_case` now preserves a uniform branch
logical identity through the 087–098 spine (reusing `nominal_compatible` + the 089 schema path), rejecting
mixed/naked-non-literal/widening cases. The widening sub-decision was a grounded 2-of-3 reject (Gemini's
erase-to-naked resolved against). Detail in `plans/adr/109_conditional_sugar_family.md` + the active-line
entry above.

### ADR 108 promoted; ADR 109 candidates (HISTORICAL)

ADR 108 promoted SQL `CASE` (the first conditional-expression substrate). 109 cashed it out as the
conditional-sugar family — `COALESCE`/`NULLIF`/`IF`/`IIF`/`IFNULL`/`NVL` as pure desugars onto `Expr::Case`,
with one `bind_case` typed-NULL placeholder hook (reusing the `typed_update_null_for_target` convention).
`IFNULL`/`NVL` were operator-steered into the same batch; `NVL2`/explicit-NULL deferred. Detail in
`plans/adr/108_sql_case.md` + the active-line entry above.

### ADR 107 promoted; ADR 108 candidates (HISTORICAL)

ADR 107 promoted `information_schema.schemata` (the 4th finite system relation), exhausting the clean
introspection small-cuts. 108 took the standing capability swing the council had teed for three cycles —
SQL `CASE`, the first conditional-expression substrate — after the introspection well ran dry. The `IF`
scope fork resolved to a 109 sugar family. Detail in `plans/adr/107_information_schema_schemata.md` + the
active-line entry above.

### ADR 106 promoted; ADR 107 candidates (HISTORICAL)

ADR 106 promoted derived aggregate tables (a narrow SQL-frontend fence-lift over the existing `Aggregate`
substrate). 107 took the ready introspection cut it carried — `information_schema.schemata`, the 4th finite
system relation — a pure 102 reuse (distinct descriptor namespaces, no new `ExecutionPlan` variant/walker/
Wendelon surface). Detail in `plans/adr/106_derived_aggregate_tables.md` + the active-line entry above.

### ADR 105 promoted; ADR 106 candidates (HISTORICAL)

ADR 105 promoted adjacent-identity-`Project` coalescing (a bounded `Project`-over-`Project` fold in the
existing `optimize_for_required` pass). 106 took the narrow derived-source slice it carried — derived
aggregate tables — a pure frontend fence-lift over the existing `Aggregate` substrate, after Codex grounded
that delegating to `lower_query` would bypass the structural derived fences. Detail in
`plans/adr/105_adjacent_identity_project_coalescing.md` + the active-line entry above.

### ADR 104 promoted; ADR 105 candidates (HISTORICAL)

ADR 104 promoted unified table alias column lists at bind (one `LogicalPlan::Alias { columns }` mechanism
across base/explicit/wildcard, superseding 103's planner rewrite, fixing the base-table silent-ignore).
105 took the optimizer-cleanup follow-on it named — adjacent-identity-`Project` coalescing — after Codex
dissolved Claude's placement fork via the existing `optimize_for_required` arm. Detail in
`plans/adr/104_unified_alias_column_lists.md` + the active-line entry above.

### ADR 103 promoted; ADR 104 candidates (HISTORICAL)

ADR 103 promoted explicit-projection derived-table alias column lists (planner `Project.output_names`
rewrite; wildcard fenced). 104 took the deferred wildcard half — but the base-table silent-ignore grounding
(+ the operator's small-cut-bias steer) widened it to a **unified** bind-time rename across base/explicit/
wildcard, superseding 103's planner path. Detail in `plans/adr/103_derived_table_alias_column_lists.md` +
the active-line entry above.

### ADR 102 promoted; ADR 103 candidates (HISTORICAL)

ADR 102 promoted authoritative `information_schema.tables`/`.columns` (Fork B: refreshed tenant descriptor
listing via a read-only Control-Ring RPC on `latest_by_name`; 099-pattern reuse, no walker sweep). 103 took
the narrowest 100 follow-on — derived-table alias column lists (explicit-projection, wildcard fenced).
Detail in `plans/adr/102_information_schema_tables_columns.md` + the active-line entry above.

### ADR 101 promoted; ADR 102 candidates (HISTORICAL)

ADR 101 promoted **Rune `LIKE`/`ILIKE`** (pure Rune authoring admission over the existing `Expr::Like`
substrate). 102 took the pre-teed introspection follow-on, resolving the (a)/(b) fork to authoritative
listing (the cross-ring read contract is the ADR). Detail in
`plans/adr/101_rune_like_ilike_pattern_predicates.md` + the active-line entry above.

### ADR 100 promoted; ADR 101 candidates (HISTORICAL)

ADR 100 promoted **Derived-Source Transparent Projection Provenance** (narrow derived `FROM` paired with
the transparent-project refinement receipt; Fork B; two-shape receipt; projected-scan excluded as inert).
101 took the smallest carried Rune cut (`LIKE`/`ILIKE`) as a palate cleanser over a fully-proven
substrate. Detail in `plans/adr/100_derived_source_transparent_projection_provenance.md` + the
active-line entry above.

### ADR 099 promoted; ADR 100 candidates (HISTORICAL)

ADR 099 promoted **`information_schema.logical_types`** (the introspection pivot off the 087–098
identity/refinement arc; first read-side synthetic relation). 100 closed the 097/098 deferral 099 named —
pass-through provenance had no SQL consumer, so 100 paired a narrow derived `FROM` source with the
transparent-projection proof. Detail in `plans/adr/099_information_schema_logical_types.md` + the
active-line entry above.

### ADR 098 promoted; ADR 099 candidates (HISTORICAL)

ADR 098 promoted **Top-Level-AND Decomposition for Static Elision** (compound-`WHERE` elision, eager-
error fence). 099 pivoted off the elision arc to `information_schema.logical_types` introspection
(Claude proved pass-through-projection had no SQL consumer; operator locked the pivot, pass-through →
100). Detail in `plans/adr/098_and_decomposition_static_elision.md` + the active-line entry above.

### ADR 097 promoted; ADR 098 candidates (HISTORICAL)

ADR 097 promoted **Refinement-Driven Read-Time Elision** (the integrity-tier payoff). 098 widened it
to compound `WHERE` via top-level-`AND` decomposition with the eager-error fence. Detail in
`plans/adr/097_refinement_driven_elision.md` + the active-line entry above.

### ADR 096 promoted; ADR 097 candidates (HISTORICAL)

ADR 096 promoted **Durable Refinement Persistence + Write Enforcement** (the integrity receipt;
portable Wendelon AST, complete write enforcement, contract de-versioned in place). 097 spent the
receipt: refinement-driven read-time elision (the payoff), closing the 095→096→097 arc. Detail in
`plans/adr/096_durable_refinement_persistence_enforcement.md` + the active-line entry above.

### ADR 095 promoted; ADR 096 candidates (HISTORICAL)

ADR 095 promoted **EmptyRelation + Static Contradiction Elision** (the Toron-only elision substrate).
096 selected slice 2 (durable refinement persistence + enforcement — the receipt), reordered before
elision (097) on the soundness catch. Detail in
`plans/adr/095_empty_relation_static_contradiction_elision.md` + the active-line entry above.

### ADR 094 promoted; ADR 095 candidates (HISTORICAL)

ADR 094 promoted **Predicate Pushdown Residual Minimization (factored OR / NOT)** (078's deferred
follow-on; minimal Toron residual, pure efficiency). 095 selected slice 1 of the refinement tier
(EmptyRelation elision substrate) over the deeper durable-predicate cut (→ 096). Detail in
`plans/adr/094_predicate_residual_minimization.md` + the active-line entry above.

### ADR 093 promoted; ADR 094 candidates (HISTORICAL)

ADR 093 promoted **SQL `TRY_CAST(x AS LogicalType)`** (nullable coercing ascription, contract γ;
completed the CAST/TRY_CAST 2×2). 094 selected the 078 residual-minimization follow-on over the
elision tier (carried → 095). Detail in `plans/adr/093_sql_logical_try_cast.md` + the active-line
entry above; git holds the candidate archaeology.

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
- **Wendelon numeric-suffix de-versioning sweep** (operator
  directive) — at pre-release with no historical
  installations, there is only "the new way": drop the
  numeric suffixes from Wendelon type/function names
  **unless a suffix has a very good reason** (e.g. genuine
  multi-version wire negotiation that actually exists).
  Behavior-neutral mechanical rename; do it as a dedicated
  chore (not bundled into a feature ADR), or let each ADR
  de-version the surface it touches (ADR 096 did the
  logical-type contract). Retarget any suffix-naming guards.

## Backlog (parked, may not return)

Ideas explored across iteration rounds in `plans/wip/`
that did not earn forward placement. Kept here as a
record of what the council considered and let go. The
per-cycle detail (the ADR 003–016 era) was pruned here for
length — git history and the iter drafts in `plans/wip/`
hold it.

## What Not To Do Next

Enduring guardrails are **enforced, not restated here.** The technical contracts live in the
`adrNNN_*` architecture-guard tests (`crates/toron/tests/architecture/`) — the durable enforcement
(council discipline #4) — and in each ADR's own §"What Not To Do". Cross-cutting process guardrails
live in the **Prioritization philosophy** (above), `plans/conventions/`, and
`plans/conventions/council.md`. The prior 81-item, per-ADR (001–016 era) list had drifted from its
cross-cutting charter into a redundant duplicate of the guards; pruned (git history holds it).
