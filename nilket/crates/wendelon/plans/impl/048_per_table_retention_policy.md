# Implementation Plan: ADR 048 — Per-Table Retention Policy

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/048_per_table_retention_policy.md`
Inherits substrate: `plans/adr/046_state_digest_substrate.md`
Inherits lifecycle composition:
`plans/adr/047_row_tombstones_append_only.md`
Bench reference: `plans/impl/046_state_digest_bench_results.md`,
`plans/impl/047_row_tombstone_bench_results.md` (when populated).

## Goal

Replace the single global MVCC retention floor (ADR 013) with
an effective retention policy resolved per `(tenant_id,
table_object_id)`, while preserving the cluster default for
legacy KV and tables that choose `InheritCluster`. Validate
ADR 046's `subtract_element` against a second consumer
(monotone, compaction-driven). Land the lifecycle algebra
that the secondary-index ADR (roadmap #8) and CDC ADR
(roadmap #10) compose against.

The implementation lets table lifecycle policy drive:

- read admission (`AS OF` reject + logical row filter);
- L1 MVCC pruning;
- L2 retention rewrites and slab retirement;
- ADR 047 row-tombstone pair reclaim;
- visible-domain `StateDigest` subtraction tests;
- audit and metrics.

Deliverables:

- New replicated catalog
  `table_lifecycle: TableLifecycleCatalog` in `SmState`.
- New control-ring mutation `SetTableRetentionPolicy` with
  monotonic `policy_generation` CAS.
- Engine surface: typed `SetTableRetentionPolicy` mutation
  (no SQL parser). User-facing DDL parsing lives in client
  tools (e.g., Toron); Wendelon receives the typed enum.
- Two-phase retention: read-time logical filter + compaction-
  time physical eviction.
- Per-row table-aware reclaim at L1 / L2 with ambiguous-
  retains rule.
- `RETENTION_FLOOR_EXCEEDED` reject for `as_of < floor`;
  multi-table all-pass admission.
- Visible-domain `StateDigest` subtract / add at retention
  eviction (no new digest domain).
- Composition with ADR 047 pair-GC under the table-specific
  floor.
- Architectural guards: no descriptor field, no fingerprint
  impact, no new digest domain, no new HRFT family, no node
  capability bit, no `format_version` bump, no slab-footer
  field.
- Golden vectors and algebraic invariants exercising
  retention against the visible-domain digest.
- Bench: L1 / L2 pruning across policy mixes; read-admission
  overhead; pair-GC composition.

## Non-goals

- No descriptor field for retention.
- No descriptor fingerprint change.
- No envelope `format_version` bump.
- No new HRFT frame family. (Retention update is a control-
  ring mutation, not a wire frame.)
- No new node capability bit.
- No `ObjectStore` trait change.
- No sidecar block. No new on-disk surface.
- No durable `StateDigest` field anywhere.
- No `digest.retention.gc.v1` domain. ADR 046 byte tag
  remains reserved for a future audit / anti-entropy
  consumer.
- No per-row TTL or predicate retention.
- No CDC retention-evicted event surface.
- No anti-entropy retention-floor coordination.
- No retention-driven compaction trigger.
- No legal-hold / freeze surface.
- No tolerant decoder for "pre-retention" lifecycle catalog
  entries. Tables predating the catalog bootstrap as
  `policy_generation = 0, retention = InheritCluster`.
- No retention-floor field in WAL, durability log entries,
  slab footers, or wire frames.
- **No PK-keyed table-specific retention.** PK-keyed tables
  reject `FixedWindow` / `Disabled` overrides and remain on
  the cluster MVCC retention path.
- **No SQL or string-form retention parser in Wendelon.**
  User-facing DDL (whatever shape Toron picks: `'90 DAYS'`,
  `INTERVAL '...'`, etc.) is the client-tool ADR's call.
  Wendelon's engine surface is the typed
  `SetTableRetentionPolicy` mutation. No reject codes for
  unit-name / negative / zero parse errors live in
  Wendelon.
- **No dry-run gate** on shrink in V1.
- **No retention-driven compaction trigger** in V1.

## Phase 0 — Contract freeze, fixtures, architectural guards

Lock the typed-mutation / read-path / compaction shapes
before touching code.

Files:

- `src/schema/units.rs` (newtype additions)
- `src/schema/lifecycle.rs` (new — policy types + catalog)
- `src/consensus/core.rs` (mutation enum)
- `src/consensus/raft_storage.rs` (state-machine apply)
- `src/storage/retention.rs` (new — resolver helpers)
- `tests/fixtures/table_retention/` (new)
- `tests/architecture_test.rs`

Work:

1. Freeze unit and policy types:
   ```rust
   #[repr(transparent)]
   pub struct RetentionWindowMillis(u64);

   #[repr(transparent)]
   pub struct PolicyGeneration(u64);

   pub enum TableRetentionPolicy {
       InheritCluster,
       Disabled,
       FixedWindow(RetentionWindowMillis),
   }

   pub struct TableLifecycleKey {
       pub tenant_id: TenantId,
       pub table_object_id: TableObjectId,
   }

   pub struct TableLifecyclePolicy {
       pub policy_generation: PolicyGeneration,
       pub retention: TableRetentionPolicy,
       pub updated_at_hlc: HybridTimestamp,
   }

   pub struct TableLifecycleCatalog {
       by_id: BTreeMap<TableLifecycleKey, TableLifecyclePolicy>,
   }
   ```
   Validation: `RetentionWindowMillis::try_new(ms)` requires
   `MIN <= ms <= MAX`:
   - `MIN_RETENTION_WINDOW_MS = 60_000` (1 minute).
   - `MAX_RETENTION_WINDOW_MS = 100 * 365 * 24 * 60 * 60 *
     1000` (100 years).

2. Freeze the control-ring mutation:
   ```rust
   pub struct SetTableRetentionPolicy {
       pub tenant_id: TenantId,
       pub table_object_id: TableObjectId,
       pub expected_policy_generation: PolicyGeneration,
       pub new_policy: TableRetentionPolicy,
       pub updated_at_hlc: HybridTimestamp,
   }
   ```
   Allocate as `WendelonRequest::SetTableRetentionPolicy`
   inside the existing control-ring mutation enum (no new
   HRFT family).

3. Freeze the effective-floor resolver signature:
   ```rust
   pub struct EffectiveRetention {
       pub policy: TableRetentionPolicy,
       pub floor: Option<HybridTimestamp>,
   }

   pub fn effective_table_retention(
       cluster_policy: &MvccRetentionPolicy,
       table_policy: Option<&TableRetentionPolicy>,
       now_ms: u64,
   ) -> EffectiveRetention;
   ```
   Pure function. `floor = None` for `Disabled` (table or
   cluster). Construction byte-for-byte equivalent to
   ADR 013:
   ```rust
   HybridTimestamp {
       physical_ms: now_ms.saturating_sub(window_ms),
       logical: 0,
       node_id: 0,
   }
   ```

4. Freeze the read-path admission helper:
   ```rust
   pub fn check_read_against_retention(
       read_hlc: HybridTimestamp,
       as_of_hlc: HybridTimestamp,
       effective: &EffectiveRetention,
   ) -> Result<(), RetentionReadError>;
   ```
   `Ok(())` if `floor.is_none()` or `as_of_hlc >= floor`.
   Else `Err(RETENTION_FLOOR_EXCEEDED { table, as_of, floor
   })`.

5. Freeze the read-path logical filter helper:
   ```rust
   pub fn row_visible_under_retention(
       row_hlc: HybridTimestamp,
       effective: &EffectiveRetention,
   ) -> bool;
   ```
   `true` if `floor.is_none()` or `row_hlc >= floor`. Used
   by scan / PBC / AGG iterators (Phase 3).

6. Freeze reject codes at the engine surface (drawn from
   the existing reject-code allocation table; no new
   family). String-parse errors (unknown units, negative
   integers, zero windows, missing quotes) are client-tool
   concerns and are **not** allocated in Wendelon. Three
   retention-specific reject codes only:
   ```rust
   pub const RETENTION_REJECTED_OUT_OF_RANGE: u16 = 0x..;
       // FixedWindow(W) with W < 1 minute or W > 100 years
       // at RetentionWindowMillis::try_new boundary.
   pub const RETENTION_REJECTED_PK_DISALLOWED: u16 = 0x..;
       // FixedWindow / Disabled against PrimaryKey table.
   pub const RETENTION_FLOOR_EXCEEDED: u16 = 0x..;
       // Read-side reject for as_of_hlc < effective_floor.
   ```
   CAS conflicts on `expected_policy_generation` are
   reported through the existing control-ring stale-
   generation conflict path (the same mechanism that
   already rejects stale schema-update mutations) — no new
   retention-named reject code is allocated. Client
   re-reads the current policy and retries.

7. Lock `StateDigest` semantics:
   - data row evict → `subtract_element(visible_row_element(t, T, k))`;
   - tombstone row evict → `add_element(visible_row_element(t, T, k))`;
   - no new domain helpers introduced.

8. Architectural guards (TODO entries; populated by later
   phases):
   - no retention field on `TableSchemaDescriptorV1`;
   - no retention in `schema_fingerprint` construction;
   - no `NodeCapability::*Retention*`;
   - no new HRFT family;
   - no slab-footer retention field;
   - no `format_version` bump;
   - no project-name prefix on new symbols;
   - no V1 / V2 suffix on new Rust types;
   - no `digest.retention.gc.v1` byte tag in source;
   - no `digest.table.row.physical.v1` /
     `digest.table.row.tombstone.v1` byte tags introduced
     here;
   - **no SQL or string-form retention parser anywhere in
     `src/`**. Wendelon receives the typed mutation only;
     user-facing DDL parsing lives in client tools (Toron).
     Architectural test scans `src/` for retention-related
     string parsing (token lists, unit lookup tables, regex
     patterns matching `RETENTION` / `'<N> <UNIT>'`).
   - retention floor never appears in WAL / durability
     log entries, slab footers, or wire frames;
   - **lifecycle catalog has no generation-keyed lookup
     API** (always-latest property — §1 of ADR);
   - **PK-keyed tables reject `FixedWindow` / `Disabled`
     policy overrides** at catalog admission. Only
     `InheritCluster` is accepted for
     `RowIdentityPolicy::PrimaryKey`.
   - no Wendelon reject codes for client-tool string
     parse failures: `RETENTION_REJECTED_BAD_UNIT`,
     `RETENTION_REJECTED_NEGATIVE`,
     `RETENTION_REJECTED_ZERO` must not appear in `src/`.
   - no background retention sweeper task; retention is
     compaction-driven only.
   - no retention filter on the write-admission path.

Fixtures under `tests/fixtures/table_retention/`. All
fixtures construct `TableRetentionPolicy` values directly
in Rust — no DDL strings; the engine surface is the typed
mutation:

- Append-only table with
  `FixedWindow(RetentionWindowMillis::try_new(90 days)?)`.
- Append-only table with `Disabled`.
- Append-only table with `InheritCluster`.
- PK-keyed table with `InheritCluster` (allowed).
- **PK-keyed table with `FixedWindow(...)` mutation
  rejects** with `RETENTION_REJECTED_PK_DISALLOWED`.
- **PK-keyed table with `Disabled` mutation rejects** with
  the same code.
- `RetentionWindowMillis::try_new` rejects below 1 minute
  and above 100 years with `RETENTION_REJECTED_OUT_OF_RANGE`.
  This is the only V1 typed-validation reject for
  out-of-range values; client tools handle string parse
  errors before they reach the engine.
- Lifecycle catalog round-trip: encode / decode preserves
  `policy_generation`, `updated_at_hlc`, all three modes.
- `effective_table_retention` golden vectors:
  - `Disabled` table → no floor regardless of cluster.
  - `InheritCluster` table → cluster floor.
  - `FixedWindow(90d)` → `now - 90d`.
- Read-admission: above floor `Ok`; below floor reject.
- Read-time logical filter: row with `__row_hlc__ < floor`
  hidden by latest read.
- Compaction with `90 DAYS` policy at `T`:
  - rows at `T-100d`, `T-50d`, `T-10d`: only `T-100d`
    evicted.
  - row tombstone pair `(data@T-100d, tombstone@T-95d)`:
    both eligible at `T` (joint drop).
  - row tombstone pair `(data@T-100d, tombstone@T-50d)`:
    only data eligible at `T`; tombstone retained.
- Multi-table read admission: table A 30d retention, table B
  90d. `AS OF T-60d` rejects (A below A's floor); `AS OF
  T-20d` admits both.
- Ambiguous-ownership: row whose `__row_table_object_id__`
  resolves to a missing catalog entry → retain.
- Policy shrink: `90d → 30d` makes `AS OF T-60d` reject
  immediately.
- Policy extend: `30d → 90d` does not restore previously
  reclaimed rows.
- Catalog bootstrap: pre-existing table without lifecycle
  entry resolves as `InheritCluster`.

Exit:

- Contract text and fixtures exist; tests fail with
  `unimplemented!()` until later phases.
- Architecture guards in place.

## Phase 1 — Control-ring policy state

Files:

- `src/consensus/core.rs`
- `src/consensus/raft_storage.rs`
- `src/network/control_server.rs`
- `src/schema/lifecycle.rs`
- `tests/control_ring_retention.rs`

Work:

1. Add `table_lifecycle: TableLifecycleCatalog` to
   `SmState`. Encode / decode through the existing snapshot
   path.

2. Add `WendelonRequest::SetTableRetentionPolicy(...)`.

3. Apply validation:
   - tenant exists and is not deleted;
   - table exists in `table_schemas.latest_by_id`;
   - if the table is `RowIdentityPolicy::PrimaryKey` and the
     `new_policy` is `FixedWindow(...)` or `Disabled`,
     reject with `RETENTION_REJECTED_PK_DISALLOWED`. PK-
     keyed tables in V1 only accept `InheritCluster`;
   - `expected_policy_generation` matches current generation
     (CAS);
   - `FixedWindow` is non-zero and within
     `RetentionWindowMillis` bounds;
   - `updated_at_hlc` is leader-generated or leader-merged.

4. Apply effect: insert / update the lifecycle entry,
   advance `policy_generation` by one. Replays are
   idempotent because the state machine is log-index
   ordered.

5. Bootstrap on table create: when `CreateTable` (or its
   equivalent) applies, the control-ring apply batch also
   emits a second `SetTableRetentionPolicy`-shaped entry
   inserting
   ```rust
   TableLifecyclePolicy {
       tenant_id, table_object_id,
       policy_generation: PolicyGeneration(0),
       retention: TableRetentionPolicy::InheritCluster,
       updated_at_hlc: <commit hlc of the schema mutation>,
   }
   ```
   into the lifecycle catalog. The descriptor-create entry
   and the lifecycle entry are submitted as **two entries
   applied atomically in the same control-ring apply
   batch** (one descriptor entry, one lifecycle entry).
   Replay-safe — catalog operations are idempotent under
   log-index ordering.

   If the client tool requests a non-default initial policy
   alongside the table create (e.g., the user's DDL
   specified `RETENTION = '90 DAYS'`), the client submits a
   `SetTableRetentionPolicy` in the same batch carrying the
   non-default policy directly instead of `InheritCluster`.
   For PK-keyed tables, only `InheritCluster` is valid in
   that second entry.

6. Bootstrap on existing tables (one-time migration): when
   the lifecycle catalog is empty for a known table at
   read / reclaim resolution time, treat the policy as
   `InheritCluster` without inserting anything. The first
   explicit `SetTableRetentionPolicy` materializes the
   entry. (No legacy decoder needed; absence is the legacy
   shape.)

7. On table drop: remove the lifecycle entry as part of
   the drop's apply effect.

8. Add control-server authorization: tenant-admin
   permission for `SetTableRetentionPolicy`. A stricter
   data-lifecycle permission is a follow-up.

Tests:

- `CreateTable` installs initial lifecycle policy with
  `policy_generation = 0`, `InheritCluster` in the same
  control-ring apply batch.
- `CreateTable + SetTableRetentionPolicy { FixedWindow }`
  in the same batch (append-only) installs `FixedWindow(...)`
  directly without an intermediate `InheritCluster` state.
- `CreateTable + SetTableRetentionPolicy { FixedWindow }`
  in the same batch (PK-keyed) rejects with
  `RETENTION_REJECTED_PK_DISALLOWED`.
- `SetTableRetentionPolicy` for an append-only table
  advances generation, persists policy.
- `SetTableRetentionPolicy { FixedWindow }` against a
  PK-keyed table rejects with
  `RETENTION_REJECTED_PK_DISALLOWED`.
- Stale `expected_policy_generation` rejects.
- Update for missing table rejects.
- Disabled / fixed / inherit round-trip through the catalog
  (append-only).
- `FixedWindow(0)` constructor rejects.
- Schema descriptor fingerprint is unchanged by policy
  update (architecture guard).
- `DropTable` removes lifecycle entry; ordering: lifecycle
  first, then descriptor; replay-safe.
- Snapshot encode / decode preserves the catalog.
- Architecture guard: lifecycle catalog API has no
  generation-keyed lookup.

Exit:

- Replicated state can store and update table retention
  policy with monotonic generations.

## Phase 2 — Effective floor resolver

Files:

- `src/storage/retention.rs`
- `src/network/data_server.rs`
- `src/storage/sweeper.rs`
- `src/storage/manager.rs`

Work:

1. Implement `effective_table_retention(cluster_policy,
   table_policy, now_ms)` per Phase 0 signature.

2. Preserve the existing global `mvcc_retention_floor_hlc()`
   for legacy KV and untyped scans.

3. Add table-aware helpers on the slab manager:
   ```rust
   fn table_retention_effective(
       &self,
       tenant_id: TenantId,
       table_object_id: TableObjectId,
       now_ms: u64,
   ) -> EffectiveRetention;
   ```
   The implementation reads from a catalog snapshot. The
   snapshot is taken once per compaction job and once per
   read plan to keep the resolver cheap.

4. Saturation: very-long retention with a young HLC
   produces floor of zero — equivalent to "no eviction" at
   that point in time.

Tests:

- `InheritCluster` follows cluster policy across modes
  (disabled, fixed, varying widths).
- `Disabled` returns no floor even when cluster is fixed.
- `FixedWindow` overrides cluster.
- Saturation: long window + young HLC = floor zero.
- Zero `FixedWindow` cannot be constructed.
- Catalog miss for known table → `InheritCluster`
  resolution.

Exit:

- Data server, sweeper, and manager can ask for table-local
  effective retention.

## Phase 3 — Read admission and logical filter

Files:

- `src/network/data_server.rs`
- `src/storage/manager.rs` (scan entry)
- `src/storage/aggregate.rs`
- `src/schema/pbc.rs`
- `src/storage/columnar.rs` (ADR 044 fetch)

Work:

1. **Phase A admission**: at every read entry point that
   accepts an `as_of_hlc`, resolve the table's
   `EffectiveRetention` once (pinned at scan start), then
   call `check_read_against_retention(...)`.

2. On admission failure: return
   `RETENTION_FLOOR_EXCEEDED { table, as_of_hlc, floor }`.
   No rows touched.

3. **Phase A logical filter — precise bound**: scan / PBC /
   AGG iterators apply
   ```text
   table_floor <= row.__row_hlc__ <= as_of_hlc
   ```
   as the visibility test. The lower bound is
   `table_floor`; the upper bound is `as_of_hlc` (or the
   read snapshot HLC for latest reads). A row below the
   floor is hidden regardless of physical presence; a row
   above `as_of_hlc` is hidden by existing MVCC. The two
   filters compose by conjunction.

4. ADR 044 column-projected fetch: include `__row_hlc__` in
   the fetch plan whenever an `EffectiveRetention` with
   `Some(floor)` applies. (The slab footer's existing
   `__row_hlc__` projection cost is already paid; this adds
   no new column.)

5. **CSB retention-completeness gate**: before committing
   to a CSB direct answer for an aggregate request, check
   the slab's row-HLC bounds against the floor:
   ```text
   slab_min_row_hlc >= floor: CSB direct answer allowed.
   slab_max_row_hlc < floor:  slab contributes zero rows;
                               CSB direct answer trivial.
   slab straddles the floor:  fall back to row-level
                               filtering. Do NOT use the
                               slab CSB.
   ```
   The slab `__row_hlc__` min / max bounds are required.
   Phase 4 / 5 verifies these are recorded in the existing
   slab summary; if not, both phases add `min_row_hlc` and
   `max_row_hlc` to the slab summary as a precondition for
   their own correctness. **Conservative fallback if Phase
   4 / 5 finds the bound unavailable on legacy slabs**:
   treat such slabs as straddling for CSB direct-answer
   purposes (forces row-level filtering, correct but
   slower). Architectural test asserts the gate is the
   only path to `csb_direct_answer` for retention-active
   tables.

6. Multi-table query admission: for any planner that
   resolves a set of `(tenant_id, table_object_id)` pairs,
   admission is the conjunction over every table. A single
   expired table rejects the whole request.

7. Latest reads (`as_of_hlc == 0`): admission always
   passes; logical filter still applies.

8. Long-running scans: the `EffectiveRetention` resolved at
   plan start is reused for every subsequent batch.

Tests:

- Table A with short retention rejects old `AS OF`.
- Table B with disabled retention accepts the same `AS OF`.
- `InheritCluster` matches existing global-window behavior.
- Latest reads (`as_of_hlc == 0`) never reject by
  retention.
- AGG request older than table floor returns
  `RETENTION_FLOOR_EXCEEDED`.
- PBC ignores rows below floor.
- Logical filter: row with `__row_hlc__ < floor` hidden on
  latest read even when physically present.
- Logical filter is consistent across scan, PBC, AGG, and
  ADR 044 fetch (same row-set returned).
- Multi-table: short-retention table A and long-retention
  table B; `AS OF` between A's floor and B's floor rejects
  the whole query.
- Long-running scan: descriptor / lifecycle policy mutates
  mid-scan; the scan continues with its plan-start
  `EffectiveRetention`.
- CSB eligibility tests continue to use tombstone /
  supersession metadata after read admission passes
  (existing ADR 047 §7 behavior).
- **CSB straddle-fallback**: aggregate over a slab whose
  row-HLC range straddles the floor falls back to row-
  level filtering (assert the CSB direct path is **not**
  taken).
- **CSB direct above floor**: aggregate over a slab whose
  earliest row HLC is above the floor takes the CSB direct
  path (assert taken).
- **CSB zero-contribution below floor**: aggregate over a
  slab whose latest row HLC is below the floor contributes
  zero (assert).

Exit:

- Reads observe per-table retention in both phases:
  admission rejection below floor, logical filter for
  rows above floor.

## Phase 4 — L1 retention pruning, table-aware

Files:

- `src/network/data_server.rs`
- `src/storage/manager.rs`
- `src/storage/core.rs`
- `src/storage/multirow.rs`

Work:

1. Change `reclaim_mvcc_retention_once(max_slabs)` to take a
   lifecycle catalog snapshot rather than a single global
   floor. The snapshot is read once per call.

2. Change `SlabManager::prune_versions_older_than` to
   evaluate each candidate record with its effective floor.
   Append-only multi-row and PK-keyed paths diverge:
   - **append-only multi-row records**: read
     `__row_table_object_id__`, resolve `(tenant_id,
     table_object_id)` table floor from the catalog snapshot;
     fall back to cluster floor only on lookup miss;
   - **single-row PK records**: stay on the existing cluster
     MVCC retention path with the existing
     `is_retention_prunable` prefix rule. **No table-specific
     floor is applied to PK-keyed rows in V1** (ADR §10);
   - **legacy KV / untyped**: cluster floor;
   - **ambiguous append-only (`__row_table_object_id__`
     malformed or missing)**: retain.

3. PK-keyed retention path is **unchanged from ADR 013**:
   prefix rule with the cluster MVCC retention floor. The
   newest version below the cluster floor is always retained.

4. Append-only retention: a data row with `row_hlc <
   table_floor` and no surviving tombstone above the floor
   is dropped.

5. Row tombstone pair-GC composition (ADR 047 §9 +
   ADR 048 §9):
   - **In-window pair**:
     - `delete_hlc < table_floor`: drop both.
     - `data_hlc < table_floor` but `delete_hlc >=
       table_floor`: keep both.
   - **Cross-window data row** (data older slab, tombstone
     current): drop data if `data_hlc < table_floor`;
     orphaned tombstone may also drop if
     `delete_hlc < table_floor`.
   - **Cross-window tombstone**: should not occur in HLC-
     ordered compaction; retain on observation.

   Ordering: retention runs first, then pair-GC on
   survivors.

6. Recompute output:
   - `has_row_tombstone_records`;
   - `tombstone_metadata`;
   - `supersession_metadata`;
   - CSB stats;
   from survivors (existing ADR 047 §9 / ADR 040 hooks).

7. Apply the visible-domain `StateDigest` reverse for each
   evicted row:
   - data row evict → `subtract_element(...)`;
   - tombstone row evict → `add_element(...)`.
   In-memory only; not persisted (Phase 6).

Tests (in `src/storage/manager.rs` unit tests):

- Append-only row `< table_floor` pruned.
- Append-only row `>= table_floor` survives.
- Disabled table never prunes.
- `InheritCluster` follows cluster floor.
- PK chain keeps newest below floor (cluster MVCC path,
  unchanged from ADR 013).
- PK-keyed slab is **not** affected by any table-specific
  override (architecture guard).
- Row tombstone pair drops only after table floor crosses
  `delete_hlc`.
- Pair-GC ordering: retention first; pair-GC second.
- Ambiguous append-only ownership retains row.
- Output footer `has_row_tombstone_records` reflects
  survivors.
- Mixed-policy slab (one append-only table 30d, one
  append-only table 90d): per-row floor applied correctly.
- Output slab records min / max `__row_hlc__` in the slab
  summary so the read-path CSB straddle gate (Phase 3) can
  evaluate retention-completeness.

Exit:

- L1 lifecycle reclaim is table-aware. Pair-GC composes
  cleanly with retention.

## Phase 5 — L2 retention rewrites, table-aware

Files:

- `src/storage/sweeper.rs`
- `src/storage/manager.rs`
- `src/storage/l2.rs`

Work:

1. Change `rewrite_tiered_slabs_for_retention_once` to pass
   the lifecycle catalog snapshot to
   `prepare_tiered_retention_rewrite`.

2. During recovered-entry classification, resolve each
   row's effective table floor.

3. Same retention vs pair-GC ordering as Phase 4.

4. Retain rows when policy lookup or table ownership is
   ambiguous.

5. Preserve existing object-store behavior:
   - rewrite partial-survivor slabs;
   - retire fully-pruned slabs through `RetireTieredSlab`;
   - CAS through Raft pointer update;
   - delete old object after committed replacement /
     retirement.

6. Recompute encrypted footer summary and stats from
   survivors. **No new footer fields.**

7. Apply visible-domain `StateDigest` reverse per evicted
   row (Phase 6 wires the helpers).

Tests:

- L2 short-retention rows rewritten out (append-only).
- L2 disabled-retention rows survive (append-only).
- Fully-pruned table slab retires.
- Mixed-policy slab rewrites with only expired rows
  removed.
- Row tombstone pair in L2 follows table floor.
- Unknown ownership retains.
- L2 PK-keyed slab uses cluster MVCC retention only;
  table-specific override absent from the rewrite.
- L2 output stats record min / max `__row_hlc__` for the
  CSB straddle gate.

Exit:

- L2 retention semantics match L1.

## Phase 6 — `StateDigest` integration: visible-domain reverse only

Files:

- `src/hash/state_digest.rs` (no new domain helpers)
- `src/storage/manager.rs` (compaction-time digest update;
  reuse helper across L1 + L2)
- `tests/state_digest_invariants.rs` (extend)

Work:

1. **No new domain constants.** Reuse
   `DOMAIN_TABLE_ROW_VISIBLE` from ADR 046.

2. **No new element-projection helpers.** Reuse
   `visible_row_element(t, T, k)` from ADR 046.

3. (Optional ergonomic helper.) Internal compaction-time
   wrapper:
   ```rust
   pub(crate) fn apply_retention_eviction_to_visible_digest(
       acc: &mut StateDigestAccumulator,
       tenant_id: TenantId,
       table_object_id: TableObjectId,
       row_key_id: Identity,
       is_tombstone: bool,
   ) {
       let element = visible_row_element(tenant_id, table_object_id, row_key_id);
       if is_tombstone {
           acc.add_element(element);    // reverse the -K admission
       } else {
           acc.subtract_element(element); // reverse the +K admission
       }
   }
   ```
   Internal-only; share between Phase 4 (L1) and Phase 5
   (L2).

4. Compaction-time digest update: at compaction output
   write, walk evicted rows and apply the reverse of their
   admission contribution. Result is **not** persisted.

5. **In-memory cache rule** (inherited from ADR 047 §10):
   any cached visible digest invalidates on compaction-
   output replacement. Cache must not be persisted to
   footer, pointer, catalog, local index, durability log,
   or wire frame.

Tests (extend `tests/state_digest_invariants.rs`):

- Pure data-row eviction: digest decreases by N
  subtractions for N rows.
- Pure tombstone-row eviction: digest increases by N
  additions for N standalone tombstones.
- Pair eviction: `(+K, -K)` admission pair, eviction
  produces `(-K, +K) = 0` net change. Digest invariant.
- Order independence: evict data first vs tombstone first
  → same final digest.
- Multi-slab equivalence: digest of survivors equals pre-
  eviction digest plus retention reverses.
- Re-eviction guard: a row already evicted is not re-
  subtracted (would over-subtract). Compaction marks
  evictions to prevent this.
- Real-multirow smoke: pack rows via the actual writer
  machinery, run compaction with a retention floor,
  decode storage metadata, compute digest, match expected.
- Tombstone-already-evicted-by-retention guard: a row
  tombstoned by ADR 047 was already subtracted at
  admission. Retention does not double-subtract; instead,
  retention of that row's tombstone marker `+K`-restores,
  and retention of the data row `-K`-removes — net zero.
  Architecture-test asserted.

Exit:

- ADR 048 validates ADR 046 `subtract_element` against a
  second consumer with monotone retention semantics. No
  durable digest placement introduced.

## Phase 7 — Bench and verification

Files:

- `benches/table_retention.rs` (new)
- `Cargo.toml` (`[[bench]]` entry)
- `plans/impl/048_table_retention_bench_results.md` (new,
  populated when the bench runs)

Bench matrix:

1. **L1 pruning** (100k and 1M rows):
   - all rows one table (single-policy);
   - mixed tables: short / long / disabled policies on the
     same slab;
   - append-only no tombstones;
   - append-only with 1% and 10% tombstones;
   - PK version chains.

2. **L2 rewrite** with mixed-policy slabs:
   - retire all (every row expired);
   - rewrite partial survivors;
   - retain all due to disabled policy;
   - retain ambiguous ownership.

3. **Read admission**:
   - latest read overhead;
   - valid `AS OF` admission;
   - expired `AS OF` reject;
   - aggregate request with CSB-eligible slab;
   - multi-table conjunction admission.

4. **Visible-domain digest cost**:
   - per-eviction subtract cost matches ADR 046 single-
     element cost within 5%;
   - total compaction-time digest cost ≤ baseline + N_evicted
     × per-element cost.

5. **Pair-GC composition**:
   - ADR 048 retention plus ADR 047 pair-GC on the same
     window: cost composes linearly (no quadratic
     interaction).

Acceptance starting points (refine after one quick smoke
run):

- Latest read admission overhead within 3% of global-
  retention path.
- L1 single-policy pruning within 5% of global-retention
  path.
- Mixed-policy pruning overhead explained by per-table
  catalog lookup count (one snapshot read amortized over
  the slab).
- No-delete append-only scan unchanged except read-
  admission lookup.
- L2 rewrite throughput not worse than 1.25× for mixed-
  policy slabs of the same byte size.
- Pair-GC + retention composed cost ≤ pair-GC cost +
  retention cost.

Bench results documented in
`plans/impl/048_table_retention_bench_results.md`. Quick
smoke is acceptable for code review; full matrix is the
ADR 048 promotion-to-shipped gate.

## Phase 8 — Metrics, audit, runbook, architectural guards

Files:

- `src/metrics/registry.rs`
- `src/audit.rs`
- `tests/metrics_smoke.rs`
- `tests/architecture_test.rs`
- `plans/ops/runbooks/table_retention.md` (new)
- `plans/ops/observability.md`

Metrics:

```text
storage_table_retention_policy_updates_total{tenant,action}
storage_table_retention_read_floor_rejects_total{tenant}
storage_table_retention_read_filtered_rows_total{tenant}
  (= rows hidden by Phase A logical filter; surfaces lag)
storage_table_retention_l1_rows_pruned_total{tenant,kind=data|tombstone}
storage_table_retention_l1_bytes_reclaimed_total{tenant}
storage_table_retention_l2_rows_pruned_total{tenant,kind=data|tombstone}
storage_table_retention_l2_bytes_reclaimed_total{tenant}
storage_table_retention_pair_drops_total{tenant}
storage_table_retention_ambiguous_rows_retained_total{tenant}
storage_table_retention_lag_seconds{tenant,table}
  (= now - earliest_unevicted_below_floor_hlc)
storage_table_retention_policy_active_tables_total{mode=inherit|disabled|fixed}
```

Audit:

- Policy update: tenant, table, before policy, after policy,
  policy_generation, updated_at_hlc, actor.
- Reclaim job: tenant, table when known, floor, pruned
  rows, reclaimed bytes, L1 / L2.
- Read-side `RETENTION_FLOOR_EXCEEDED` rejects emit a
  metric, not an audit event (volume).

Runbook:

- How table retention composes with cluster retention.
- The three modes and their semantics.
- Two-phase retention: logical (immediate) vs physical
  (eventual).
- Why extension does not restore reclaimed history.
- How to dry-run with metrics before shortening a window.
- How row tombstones reclaim once the table floor crosses
  `delete_hlc`.
- How to interpret the `ambiguous_rows_retained` metric
  (catalog rotation? recovery? real bug?).
- What disabled retention does and does not guarantee.
- Operator workflows: enable, change, disable, query
  before / after a retention change.
- Why sub-minute retention is rejected.

Architectural guards (extend `tests/architecture_test.rs`):

- No retention field on `TableSchemaDescriptorV1`.
- No retention in `schema_fingerprint` construction.
- No `NodeCapability::*Retention*`.
- No new HRFT family for retention.
- No slab-footer retention field.
- No `format_version` bump.
- No project-name prefix on new symbols.
- No V1 / V2 suffix on new Rust types.
- `RetentionWindowMillis` constructed only via `try_new`.
- `TableRetentionPolicy` has exactly three variants
  (architectural-test sanity check).
- Curve math (`use curve25519_dalek`) does not appear in
  `src/network` or `src/storage`.
- `StateDigest` symbol does not appear on storage / network
  / control-ring durable struct definitions; in-memory
  cache exception per ADR 047 §10.
- **Always-latest catalog property**: lifecycle catalog API
  exposes no `at_generation(...)` lookup; only "current"
  reads. Architectural-test scans the catalog module's
  public API.
- **PK-keyed override impossible**: catalog admission test
  asserts `SetTableRetentionPolicy { FixedWindow }` against
  PK-keyed table rejects with
  `RETENTION_REJECTED_PK_DISALLOWED`. No code path bypasses.
- **CSB straddle gate present**: aggregate planner checks
  slab row-HLC bounds against floor before committing to a
  CSB direct answer. Verified by an architecture test that
  greps the planner for the gate call.
- **Rejected-alternative surface guards:**
  - No `digest.retention.gc.v1` byte tag in source.
  - No `RetentionFloor` field in WAL / durability log
    record types.
  - No background retention sweeper task.
  - No per-row retention override field on multi-row record
    schemas.
  - No `RETENTION = '0 ...'` accepted by the parser.
  - No clamp-to-floor on read paths (hard error only).
  - No retention field in `TableSchemaDescriptorV1`
    (catch the regression where someone "simplifies" by
    moving retention onto the descriptor).
  - No `at_generation` / generation-keyed lookup API on
    `TableLifecycleCatalog`.
  - **No SQL or string-form retention parser anywhere in
    `src/`**. Wendelon is byte-code-only; user-facing DDL
    parsing belongs to client tools. The architectural
    test scans `src/` for `RETENTION` token lists,
    `'<N> <UNIT>'` regex patterns, unit-name lookup
    tables, or `parse_retention*` symbols. Any retention-
    string parsing under `src/` is a regression.
  - No `RETENTION_REJECTED_BAD_UNIT` / `_NEGATIVE` /
    `_ZERO` reject codes in source. Those are client-tool
    parse errors, not engine surface.

Exit:

- Operators can observe retention activity, audit policy
  changes, follow a runbook for common operations.

## Verification gate

Required before declaring ADR 048 shipped:

```sh
cargo test --lib retention
cargo test --lib state_digest
cargo test --test state_digest_invariants
cargo test --test architecture_test --quiet
cargo test retention --quiet
cargo test row_tombstone --quiet
cargo test storage::manager --quiet
cargo test storage::sweeper --quiet
cargo test --test control_ring_retention --quiet
cargo test --test metrics_smoke --quiet
cargo bench --bench table_retention --no-run
cargo bench --bench table_retention -- --quick
git diff --check
cargo fmt --check
```

Grep guards:

- Domain-tag well-formedness (extends ADR 046):
  ```sh
  rg -n 'b"digest\.[a-z_.]+\.v[0-9]+"' src tests benches | sort -u
  ```
  Output equals the ADR 046 set; no new domains added.

- Substrate isolation (inherited from ADR 046):
  ```sh
  rg -n 'use\s+curve25519_dalek' src/network src/storage
  ```
  Empty.

- No durable digest placement (inherited from ADR 047):
  ```sh
  rg -n 'StateDigest\b' src/network src/consensus src/storage
  ```
  Empty on persisted-field sites (in-memory accumulator
  use is allowed; the guard's intent is no durable
  `StateDigest` field on storage, control-ring, or wire
  shapes — ADR 047 §10).

- No new HRFT family for retention:
  ```sh
  rg -n 'NodeCapability::.*Retention|RetentionCapability' src tests
  ```
  Empty.

- Retention does not enter the descriptor or fingerprint:
  ```sh
  rg -n 'retention.*schema_fingerprint|schema_fingerprint.*retention' src tests
  rg -n 'retention.*TableSchemaDescriptor|TableSchemaDescriptor.*retention' src tests
  ```
  Both empty.

- No `format_version` bump on the ADR 048 diff scope:
  ```sh
  rg -n 'format_version\s*[=:]\s*[3-9]' src
  ```
  Empty.

- No version suffixes on ADR 048 Rust types:
  ```sh
  rg -n 'Retention(Policy|Duration|Window)V[0-9]|TableLifecyclePolicyV[0-9]' src tests
  ```
  Empty.

- No project-name prefix on new retention symbols (per
  `no-project-name-in-protocols.md`). The guard catches
  *new* prefix patterns like `WendelonRetention*` /
  `wendelon_retention_*`; existing umbrella enums such as
  `WendelonRequest::SetTableRetentionPolicy` are not
  project-name-prefixed retention symbols.
  ```sh
  rg -n '\bWendelonRetention[A-Z]|\bwendelon_retention[a-z_]' src tests benches
  ```
  Empty.

- **Rejected-alternative surface guards:**
  ```sh
  rg -n 'digest\.retention\.gc\.v1|retention_gc_element' src tests
  rg -n 'RetentionFloor' src/network src/consensus src/storage
  rg -n 'background_retention|retention_sweeper' src
  rg -n 'per_row_retention|row_retention_override' src tests
  rg -n 'at_generation|by_generation' src/schema/lifecycle.rs
  ```
  All five empty. These guards prevent silent re-
  introduction of the rejected audit-domain / WAL- or
  durability-log- or wire-frame-resident /
  background-sweeper / per-row-override / generation-
  keyed-lifecycle shapes (see ADR §"Rejected
  alternatives"). The WAL/durability/wire-frame guard
  covers `src/network`, `src/consensus` (Raft storage,
  the WAL host), and `src/storage` since the project does
  not split WAL / durability into separate top-level
  modules.

- **No string-form retention parser in `src/`** (Wendelon
  is byte-code-only; DDL parsing lives in client tools):
  ```sh
  rg -n "parse_retention|retention_parser|RETENTION_REJECTED_BAD_UNIT|RETENTION_REJECTED_NEGATIVE|RETENTION_REJECTED_ZERO" src
  rg -n "'\s*\\\\d+\\\\s*(SECOND|MINUTE|HOUR|DAY)" src
  rg -nP "\b(SECOND|MINUTE|HOUR|DAY)S?\b.*=" src/schema src/network
  ```
  All empty. The first guard catches function names and
  rejected-from-engine reject codes. The second catches
  regex-style retention-string matchers. The third catches
  unit-name lookup tables in schema / network code.

Convention guards:

- No `#[serde(default)]` shim on retention fields.
- No reserved enum slots for retention.
- No envelope `format_version` bump.
- No new HRFT frame family.
- No new node capability bit.
- No `ObjectStore` trait break.
- No on-disk shape change.
- Retention sits outside both `schema_fingerprint` and the
  upcoming logical / physical descriptor fingerprint split.

## V1 design summary

Engine surface decisions (within Wendelon's scope):

- Lifecycle catalog (`TableLifecycleCatalog` in `SmState`),
  not descriptor field. No generation-keyed lookup.
- Three retention modes: `InheritCluster` / `Disabled` /
  `FixedWindow(RetentionWindowMillis)`.
- `RetentionWindowMillis` bounds: 1 minute to 100 years.
- New `WendelonRequest::SetTableRetentionPolicy` mutation
  with `policy_generation` CAS.
- `CREATE TABLE` + initial lifecycle policy: two entries
  applied atomically in the same control-ring apply batch.
- Drop-table ordering: lifecycle first, descriptor second.
- Tenant-admin permission for retention updates.
- PK-keyed table-specific overrides reject; PK reclaim
  stays on the cluster MVCC retention path.
- Two-phase retention: logical row filter + admission
  reject at read; physical eviction at compaction.
- Hard error `RETENTION_FLOOR_EXCEEDED` for `as_of < floor`
  (no clamp).
- Multi-table queries require all referenced tables to
  pass admission.
- Visible-domain reversal only; no `digest.retention.gc.v1`.
- Pair-GC composition under table-specific floor; retention
  evaluates before pair-GC.
- Ambiguous-ownership retains the row.
- CSB direct-answer eligibility: slabs above floor allowed,
  slabs below floor zero-contribution, straddling slabs
  fall back to row filtering.

Out of Wendelon's scope (client-tool ADR territory):
DDL syntax (string vs `INTERVAL` form, `INHERIT`
visibility, accepted unit list), DDL parse errors,
operator UX. Wendelon receives the typed mutation only.

V1 defaults still pending (none block promotion):
- No dry-run gate for shrink (metric-based estimation).
- No retention-driven compaction trigger (existing
  triggers + `_lag_seconds` metric).
- No per-table PK observability metrics (cluster
  aggregate suffices).

## Deferred to follow-up ADRs

Mirrors ADR 048 *Deferred* section: PK-keyed row-disappear
via retention, per-column / per-partition / per-row
retention, predicate-based retention, legal hold, CDC
retention-evicted event surface, anti-entropy retention
coordination, retention-driven compaction trigger, audit /
event-history digest domain, retention-aware secondary-
index propagation.
