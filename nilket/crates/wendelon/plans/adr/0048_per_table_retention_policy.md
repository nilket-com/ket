# ADR 048: Per-Table Retention Policy

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan: `plans/impl/0048_per_table_retention_policy.md`

## Status

Accepted

## Context

ADR 013 established cluster-wide MVCC retention with a single
global floor:

```text
floor = now_hlc - cluster_window
```

That floor is too coarse for the table shapes now present:
audit / CDC sinks want long or disabled retention, telemetry
streams want short retention, OLTP-style tables want a
moderate default. ADR 047 row tombstones can only safely
reclaim payload bytes once the retention floor crosses
`delete_hlc` — a per-table contract makes that boundary
table-meaningful instead of cluster-uniform.

The next lifecycle step is roadmap item #3 (per-table
retention): the second algebra consumer after row tombstones,
completing the lifecycle validation pass before the topology
gate.

The motivating cases:

- **Compliance retention.** GDPR / SOX / HIPAA classes of
  data have a maximum retention window after which the data
  must be physically destroyed.
- **Cost retention.** Telemetry / event-stream tables grow
  unbounded; operators want a hard age cap.
- **Lifecycle algebra validation.** Retention is the second
  consumer of `subtract_element`, this time monotonic and
  compaction-driven. Pairs with row tombstones (§9
  composition).

Three pressures shape the design:

- **Algebraic composition.** Retention-evicted rows must
  reverse their admission contribution to the visible
  `StateDigest` and compose with ADR 047 row tombstones
  without double-subtracting.
- **Time-travel contract clarity.** "90 days retention" must
  mean "rows older than 90 days are not queryable, period" —
  not "rows older than 90 days are not queryable when
  compaction happens to have run." The contract has to be
  observable independent of compaction timing.
- **Schema-generation independence.** A read of schema
  generation 3 must not bypass the latest retention policy
  just because generation 3 predates a retention policy
  update.

The constraint from ADR 045 still holds: append-only rows are
immutable storage facts. The constraint from ADR 047 still
holds: visibility decisions go through the row index latest-
as-of chain plus `tombstone_metadata`, not ad-hoc filters.

## Non-goals (V1)

- **No SQL or string-form retention parser in Wendelon.**
  Wendelon is a byte-code engine. The engine surface for
  retention is the typed `SetTableRetentionPolicy`
  control-ring mutation. Client tools (Toron, etc.) own
  any user-facing DDL syntax and translate it into the
  typed mutation before submission. Wendelon does not
  parse, validate, or store string-form DDL.
- **No PK-keyed table-specific retention.** PK-keyed tables
  remain on the cluster MVCC retention path (ADR 013) until
  the PK-keyed-row-delete ADR (roadmap #6) settles
  supersession + retention semantics. Setting a table-
  specific override on a PK-keyed table rejects with
  `RETENTION_REJECTED_PK_DISALLOWED`.
- **No per-row TTL or predicate retention.** "Drop rows
  where status = 'failed' older than 7 days" is a follow-up.
- **No per-column retention.** "Drop column X after 30
  days" is a follow-up.
- **No per-partition retention.** Follow-up.
- **No legal-hold / freeze surface.** Follow-up.
- **No CDC retention-evicted event surface.** CDC ADR
  (roadmap #10) owns the cursor / event surface.
- **No retention-event audit digest domain.**
  `digest.retention.gc.v1` is **not** introduced.
  Retention uses visible-domain subtraction directly (§12).
  ADR 046 §7's deferral test (concrete reader, authentication
  boundary, invalidation rule) is not satisfied here. The
  byte tag remains reserved by ADR 046 §4 for a future audit
  / anti-entropy ADR.
- **No anti-entropy retention coordination.** Replicas may
  compact at different wall-clock times, producing transient
  visible-digest divergence. Anti-entropy reconciles after
  both compact. This ADR does not introduce a "retention
  watermark" or cross-replica retention barrier.
- **No durable `StateDigest` placement.** ADR 046 §7 +
  ADR 047 §10 hold.
- **No retention floor field in WAL, durability log
  entries, slab footers, or wire frames.** Floor is
  computed at read / compaction time, never carried as a
  stored field.
- **No envelope `format_version` bump.**
- **No descriptor format-version bump.** Retention does
  **not** live on the descriptor (§1).
- **No new HRFT frame family.** Retention update is a new
  control-ring mutation, not a wire-frame family.
- **No new node capability bit, no `ObjectStore` trait
  change, no new reject-code family.**
- **No retention floor below 1 minute.** Sub-minute
  retention has no realistic operational use and risks
  pathological compaction loops. Enforced via
  `RetentionWindowMillis::try_new` at the typed-mutation
  boundary; `FixedWindow(...)` with `ms < 60_000` rejects
  with `RETENTION_REJECTED_OUT_OF_RANGE`.
- **No synchronous retention.** Retention is compaction-
  driven physical lifecycle. Logical visibility enforces the
  contract; physical eviction is eventual.
- **No hard-delete API.** Retention is policy-driven, not
  request-driven.
- **No tolerant decoder for "pre-retention" lifecycle
  catalog entries.** Tables that predate this ADR's catalog
  bootstrap with `policy_generation = 0,
  retention = InheritCluster` at their next descriptor read.
  Per `no-premature-backward-compat.md`.
- **No descriptor-fingerprint impact.** Retention sits
  outside both logical and physical descriptor fingerprints
  by construction (§1).
- **No retention applied at write admission.** Retention is
  about historical cleanup, not write filtering.

## Decision

### 1. Retention is lifecycle policy state, not schema shape

ADR 048 adds replicated table lifecycle policy state keyed by
`(tenant_id, table_object_id)`:

```rust
pub struct TableLifecycleKey {
    pub tenant_id: TenantId,
    pub table_object_id: TableObjectId,
}

pub struct TableLifecyclePolicy {
    pub policy_generation: PolicyGeneration,
    pub retention: TableRetentionPolicy,
    pub updated_at_hlc: HybridTimestamp,
}
```

The catalog stores `BTreeMap<TableLifecycleKey,
TableLifecyclePolicy>`. Tenant and table identity belong
to the key, not the value, so policy records cannot drift
out of agreement with their map slot.

`TableLifecyclePolicy` is **not** embedded in
`TableSchemaDescriptorV1` (the existing logical descriptor),
does **not** enter `schema_fingerprint`, and is not reached
through `schema_generation` resolution. The descriptor-
fingerprint split (roadmap #7) inherits this decision: a
lifecycle policy that never belonged to the descriptor does
not have to be split out of it.

The lifecycle catalog lives in the existing control-ring
state (`SmState`, alongside `table_schemas` and the
tenant catalog). It is replicated through the existing Raft
log; no new replication primitive is introduced.

**Always-latest property.** The lifecycle catalog is read at
the current Raft state, never at any historical schema
generation. The catalog exposes no `at_generation` lookup
API. A read of `schema generation 3` decodes columns from
generation 3's descriptor but resolves retention from the
catalog's current state. An architectural test enforces
this by guarding the catalog's public surface for
generation-keyed lookups.

Why a separate catalog rather than a descriptor field: a
query's schema generation is a compatibility contract for
user columns and execution planning. Retention is current
lifecycle authority. Embedding it in historical schema
descriptors would let old schema-generation reads
accidentally apply old retention windows after the table
has changed policy. The catalog approach makes "always
latest" a structural invariant rather than a per-read-site
discipline.

### 2. Three retention modes

```rust
pub enum TableRetentionPolicy {
    InheritCluster,
    Disabled,
    FixedWindow(RetentionWindowMillis),
}

#[repr(transparent)]
pub struct RetentionWindowMillis(u64);

#[repr(transparent)]
pub struct PolicyGeneration(u64);
```

`RetentionWindowMillis::try_new(ms)` validates:

- `ms >= MIN_RETENTION_WINDOW_MS` (60_000 = 1 minute).
- `ms <= MAX_RETENTION_WINDOW_MS` (100 years in ms; well
  inside `i64` HLC arithmetic safety).

Bare `u64` is never accepted across module boundaries (per
`newtype-units.md`).

Default for new tables: `InheritCluster`. The cluster default
remains the behavior for tables that do not opt in.

`Disabled` is the explicit no-retention opt-out. It is
distinct from `InheritCluster` because the cluster policy may
itself be set; `Disabled` overrides regardless.

### 3. Engine surface (typed mutation) and client-tool DDL convention (informational)

The engine surface for retention configuration is the typed
control-ring mutation:

```rust
pub struct SetTableRetentionPolicy {
    pub tenant_id: TenantId,
    pub table_object_id: TableObjectId,
    pub expected_policy_generation: PolicyGeneration,
    pub new_policy: TableRetentionPolicy,
    pub updated_at_hlc: HybridTimestamp,
}
```

Wendelon validates only the typed mutation. The retention-
specific reject codes at the engine surface are:

- `RETENTION_REJECTED_OUT_OF_RANGE` — `FixedWindow(W)` with
  `W < 1 minute` or `W > 100 years`. Enforced at
  `RetentionWindowMillis::try_new` (the only constructor),
  so this code surfaces only when the typed value is
  malformed (e.g., a client tool with a buggy translation).
- `RETENTION_REJECTED_PK_DISALLOWED` — `FixedWindow` or
  `Disabled` against `RowIdentityPolicy::PrimaryKey` (§10).
- `RETENTION_FLOOR_EXCEEDED` — read-side, returned by the
  read entry point when `as_of_hlc < effective_floor` (§7).

CAS conflicts on `expected_policy_generation` (§5) are
**not** a new retention-named reject code; they surface
through the existing control-ring stale-generation
conflict pattern that already gates schema-update
mutations. The retention-specific code list stays at three.

There is no SQL or string-form retention parser in
Wendelon. Reject codes for malformed strings (unknown
units, negative integers, zero windows, missing quotes)
do not exist at the engine surface — those are client-tool
parse errors and live in the client tool.

**Informational reference: client-tool DDL convention.**
A client tool such as Toron may expose retention via
SQL-like syntax and translate it into the typed mutation
before submitting to Wendelon. A typical convention:

```text
CREATE TABLE events (...) WITH (RETENTION = '90 DAYS');
ALTER TABLE events SET RETENTION = '30 DAYS';
ALTER TABLE events SET RETENTION = INHERIT;
ALTER TABLE events SET RETENTION = NONE;
```

with `INHERIT → InheritCluster`, `NONE → Disabled`, and
`'<N> <UNIT>' → FixedWindow(RetentionWindowMillis(...))`
for case-insensitive `SECOND(S)`, `MINUTE(S)`, `HOUR(S)`,
`DAY(S)`. The exact syntax (e.g., string literal vs
`INTERVAL '...'` form) is the client-tool ADR's call.
This block is reproduced here so readers understand the
typical user-facing shape; the client-tool ADR is the
source of truth for parsing.

Retention changes flow through the existing control-ring
apply path as `SetTableRetentionPolicy` mutations. They
do **not** bump `schema_generation`. They bump
`policy_generation` (§5).

### 4. Effective floor resolution

For a multi-row record at `(tenant_id, table_object_id)`,
the effective retention floor is resolved at read /
reclaim time:

```text
table policy exists:
    InheritCluster -> cluster MvccRetentionPolicy floor
    Disabled       -> no floor (None)
    FixedWindow(W) -> now_ms - W

table policy missing (only legacy KV / bootstrap):
    cluster MvccRetentionPolicy floor
```

The floor itself is constructed exactly as ADR 013:

```rust
HybridTimestamp {
    physical_ms: now_ms.saturating_sub(window_ms),
    logical: 0,
    node_id: 0,
}
```

This ADR does **not** introduce a replicated global cutoff
or coordinated retention watermark. ADR 012 bounded clock
drift remains the cross-node guardrail.

### 5. Policy updates use monotonic generations

Retention updates use the `SetTableRetentionPolicy` mutation
defined in §3. CAS-style: the apply path rejects stale
`expected_policy_generation` values and advances
`policy_generation` by one. Replaying the same committed log
entry is idempotent because the state machine is log-index
ordered.

`updated_at_hlc` is leader-generated or leader-merged, not
trusted blindly from the client.

**Shrinking** (from `'90 DAYS'` to `'30 DAYS'`, or from
`Disabled` to `FixedWindow`): takes effect immediately for
read admission and future reclaim. The narrower floor
applies to `AS OF` queries from the moment the mutation
applies.

**Extending** (from `'30 DAYS'` to `'90 DAYS'`, or from
`FixedWindow` to `Disabled` / `InheritCluster` with a
longer effective window): takes effect for future retention
decisions. Already-reclaimed rows are gone permanently —
extension does not resurrect them. The audit event must make
this irreversibility explicit.

**Disabling** (`Disabled`): stops future reclaim for the
table. Does not restore prior reclaimed history.

### 6. Two-phase retention: logical at read, physical at compaction

Retention has two enforcement points:

**Phase A: logical retention (read time).** Rows with
`__row_hlc__ < effective_floor(now)` are invisible to all
reads, regardless of whether compaction has physically
evicted them. This makes the contract `"rows older than 90
days are not queryable"` independent of compaction lag.

The read-side floor is `effective_floor(read_hlc)` — a
single Duration subtraction at query plan time. For a
long-running scan, the floor is **pinned at scan start** so
the scan does not silently shift visibility mid-execution.

**Phase B: physical retention (compaction time).** When
compaction touches a slab, it evicts rows where
`__row_hlc__ < effective_floor(compaction_start_hlc)` and
applies the `StateDigest` reverse (§12). Bytes are
physically reclaimed. The physical floor lags the logical
floor by the compaction interval; this is acceptable
because Phase A enforces the contract.

For a slab whose entire row set is below the floor: the
slab is reclaimed entirely (existing compaction-output
bookkeeping; no new path).

Phase A and Phase B are independent. A row that is
physically still in storage but logically below the floor:
- not returned by any read (Phase A);
- evicted at next compaction (Phase B).

A row that is physically evicted but logically above the
floor: cannot occur (Phase B does not run ahead of Phase A's
floor).

### 7. Read admission per table; multi-table requests all-pass

For any read with `as_of_hlc != 0` against a typed table:

```text
let table_floor = effective_floor(read_hlc);
if as_of_hlc < table_floor:
    reject with RETENTION_FLOOR_EXCEEDED { table, as_of, floor }
```

For multi-table queries (joins, future): the request is
valid only if `as_of_hlc` is within **every** referenced
table's effective retention window. A single expired table
rejects the whole request. Partial answers are not produced.

Latest reads (`as_of_hlc == 0` or current-time semantics)
are always admitted; the Phase A logical filter handles
visibility.

For untyped / legacy KV scans: the existing cluster floor
remains the guard.

The reject is hard, not silent. Clamping `as_of` to the
floor would shift the query semantic; operators need to
observe the boundary and adjust deliberately.

**Precise read-side row visibility bound.** For an
append-only table with an active floor, a row is visible
iff

```text
table_floor <= __row_hlc__ <= as_of_hlc
```

Latest reads use the read-snapshot HLC as the upper bound
and the pinned `table_floor` as the lower bound. This makes
the two-phase contract precise: above-floor rows (Phase A
logical) AND at-or-before-as-of rows (existing MVCC), AND
not hidden by a tombstone (ADR 047).

### 8. Compaction-time physical reclaim is table-aware

L1 pruning and L2 retention rewrites resolve a retention
floor per recovered storage row:

```text
multi-row record:
    table_object_id := __row_table_object_id__
    floor := effective_floor(tenant_id, table_object_id)

single-row PK record:
    table_object_id := proven via existing descriptor /
                       index match used by table-aware
                       compaction
    floor := effective_floor(tenant_id, table_object_id)

legacy KV / unknown ownership:
    floor := cluster MvccRetentionPolicy floor

ambiguous (table cannot be assigned without ambiguity):
    fail closed: retain the row.
```

The "retain ambiguous" rule is load-bearing. It ensures a
record whose table identity is in flux (descriptor mutation
in progress, recovery edge cases) is never dropped under an
unintended floor. The implementation plan exposes an
`ambiguous_rows_retained` metric so operators can observe
when this rule fires.

For multi-row records, the compactor reads
`__row_table_object_id__` once per row and groups by it
before applying the floor. This avoids per-row catalog
lookups; the catalog snapshot is read once per compaction
job.

### 9. Composition with ADR 047 pair-GC

ADR 047 §9 named "the existing MVCC retention floor" as the
safety condition for tombstoned-pair joint drop. ADR 048
substitutes the **table-specific** floor:

- **In-window pair** (`(data, tombstone)` both in compaction
  window):
  - `delete_hlc < table_floor`: drop both. Visible digest
    invariant (the `+K, -K` already cancelled at admission).
  - `data_hlc < table_floor` but `delete_hlc >= table_floor`:
    keep both. Time-travel `AS OF T < delete_hlc` (and
    `T >= table_floor`) must still see the data row.
- **Cross-window data row** (data row in older slab,
  tombstone in current slab):
  - `data_hlc < table_floor` and target tombstone's
    `delete_hlc < table_floor`: data row eligible for
    eviction; tombstone may also be dropped if compactor can
    prove the data row is safely reclaimed under the same
    floor.
  - Otherwise: retain.
- **Cross-window tombstone** (tombstone in older slab, data
  row in current slab): cannot occur in HLC-ordered
  compaction; retain on observation pending audit.

All four branches preserve the ADR 047 §10 visible-digest
invariant: removing a `(data, tombstone)` pair leaves the
visible digest unchanged.

Ordering inside compaction: **retention runs first**, then
pair-GC on survivors. This avoids pair-GC making decisions
about rows that retention would already have removed.

### 10. PK-keyed retention stays on the cluster MVCC path

PK-keyed tables in V1 do **not** accept table-specific
retention overrides. The catalog rejects
`SetTableRetentionPolicy { new_policy: FixedWindow | Disabled }`
against a `RowIdentityPolicy::PrimaryKey` table with
`RETENTION_REJECTED_PK_DISALLOWED`. PK-keyed reclaim
continues to use the cluster-wide `MvccRetentionPolicy`
(ADR 013) and the existing `is_retention_prunable` prefix
rule:

- prune versions older than the cluster floor;
- always retain the newest version below the floor so reads
  at and after the floor still resolve the row;
- a row whose only version is older than the cluster floor
  is retained (never silently disappear).

Why deferred: "row fell out of the retention window" and
"row was deleted / superseded" need one shared supersession
model. PK-keyed delete is roadmap #6; mixing per-table PK
retention into V1 would risk a partial semantic that
roadmap #6 has to undo. The catalog still installs an
initial `policy_generation = 0, retention = InheritCluster`
on `CreateTable` for PK-keyed tables so the lifecycle record
is uniform across `RowIdentityPolicy` values; only the
`FixedWindow` / `Disabled` overrides are blocked.

Operators who want per-table retention on a PK-keyed table
wait for roadmap #6 (or its retention follow-up).

### 11. Append-only retention drops by `__row_hlc__`

For append-only data rows without a matching tombstone:

```text
drop when __row_hlc__ < effective_floor(compaction_start_hlc)
```

No prefix preservation rule applies (each append-only row is
its own fact, not a version of a key).

For append-only rows with a matching tombstone, §9 governs.

### 12. `StateDigest` composition: visible-domain reverse only

Retention does **not** introduce a new digest domain. It
uses ADR 046's `digest.table.row.visible.v1` directly, with
the rule:

> Retention eviction reverses the row's admission
> contribution to the visible digest.

```text
evict data row:        accumulator.subtract_element(visible_row_element(t, T, k))
evict tombstone row:   accumulator.add_element(visible_row_element(t, T, k))
```

The asymmetry: data rows contributed `+K` at admission
(ADR 046); tombstones contributed `-K` at admission
(ADR 047 §10). Eviction reverses each. The pair-GC
invariant from ADR 047 §10 carries through unchanged.

Cases:
- Eviction of `(data, tombstone)` pair: net zero digest
  change.
- Eviction of standalone data row: visible digest shrinks
  by `+K`'s removal — visible state shrinks, matching
  reality.
- Eviction of orphaned tombstone: visible digest grows by
  `-K`'s removal — restores the digest from `-K` (post-
  admission, pre-data-eviction state) to `0`.

The visible digest is **computable on demand** from slab
contents post-retention. Not persisted. ADR 046 §7 +
ADR 047 §10 hold.

In-memory cache rule (inherited from ADR 047 §10): seal-
time / recovery-time code may cache the visible digest in
memory, invalidated on compaction-output replacement. The
cache must not be persisted to footer, pointer, catalog,
local index, durability log, or wire frame.

**Why no `digest.retention.gc.v1` domain in V1.** Visible-
domain subtraction alone validates storage-state equality
after eviction; an event-history domain is only useful
when an audit / anti-entropy ADR pulls on it with a
defined reader, authentication boundary, and invalidation
rule. ADR 046 §7's deferral test is not satisfied here.
The byte tag remains reserved (ADR 046 §4) for the future
consumer.

### 13. CSB retention-completeness for direct answers

Retention changes physical row availability; it does not
change the meaning of `tombstone_metadata`,
`supersession_metadata`, or `has_row_tombstone_records`.

CSB direct-answer eligibility remains governed by ADR 040 /
ADR 042 / ADR 047 §7. The Phase A read-admission check
happens **before** CSB eligibility — once a request is
within retention, the existing CSB predicates remain the
soundness authority.

But retention introduces a new condition: a CSB built from
the full slab is only directly answerable if the slab is
**retention-complete** at the requested floor. Three cases:

- **Slab wholly above floor** (`min(__row_hlc__) >= floor`):
  CSB direct answer allowed.
- **Slab wholly below floor** (`max(__row_hlc__) < floor`):
  slab contributes zero rows.
- **Slab straddles the floor:** fall back to row-level
  filtering; do not use the slab CSB.

A CSB rebuilt at compaction from post-retention survivors is
straddle-free by construction and recovers direct-answer
eligibility on the output slab. This is the natural state for
slabs that have been compacted; only newly-written slabs
that span the floor without having been compacted yet are
in the straddle case.

Per-slab row-HLC bounds are required for the test. Most
slabs already record min / max `__row_hlc__` via existing
stats; the implementation plan's L1 / L2 retention phases
verify availability and add to the slab summary if missing
(open question §2).

A CSB built before retention runs may reference rows
subsequently evicted at compaction. Compaction recomputes
the CSB from survivors. An in-memory CSB cached between
eviction and the compaction-output handoff is stale; the
existing compaction-output invalidation hook (which already
fires on slab replacement) covers this case.

### 14. Recovery and replication

Recovery from durability log: retention does not appear in
the log. The log replays admission, not eviction. After
replay, the row index is consistent; retention has not yet
been applied (it runs at compaction). No new recovery
branch.

Replication: a replica receiving a slab reads the slab as-
is. If the slab was produced by post-retention compaction,
evictions are already physical. Otherwise the replica
applies retention at its own next compaction. Transient
visible-digest divergence (replica A compacted, B did not)
is acceptable; visible digests reconverge after both
compact.

The compaction transaction records `compaction_start_hlc`
and the lifecycle catalog snapshot. Replicas verifying a
compaction output recompute the same evictions from the
same `(compaction_start_hlc, catalog_snapshot)` inputs and
reach the same survivor set.

## Rationale

**Why a separate lifecycle catalog instead of a descriptor
field.** Schema generation is a compatibility contract for
user columns and execution planning. Retention is current
lifecycle authority. Embedding it in the descriptor would
bind retention semantics to historical schema generations,
creating cases where a read of an old generation ignores a
newer policy. Putting policy in a separate replicated
catalog lets reads use the requested schema generation for
decoding while using the latest lifecycle policy for safety.
As a bonus, the descriptor fingerprint split (roadmap #7)
does not have to unbind a field that never belonged to the
descriptor.

**Why three modes (`InheritCluster` / `Disabled` /
`FixedWindow`).** `InheritCluster` keeps the existing
cluster MVCC retention as a sane default; `Disabled` is the
explicit override for tables that must not expire (audit,
CDC); `FixedWindow` is the per-table policy. Two modes
(`Disabled` + `FixedWindow`, no inherit) would force every
new table to opt in explicitly, fighting the existing
cluster default. Four modes adds a redundant case.

**Why two-phase retention.** Logical-only would mean
storage grows forever (no physical reclaim). Physical-only
would mean queries see expired rows whenever compaction
lags. The contract operators want — "rows older than 90
days are not queryable" — needs both: logical for the
contract, physical for the storage cost.

**Why hard error on `as_of < floor` instead of clamping.**
Clamping silently shifts a query's semantic from "what
existed at T" to "what existed at floor" — that's a
different query. Operators querying historical data want to
know when the floor moved past their target, not get a
confusingly narrowed result. Hard error makes the boundary
observable. The reject code carries the floor in its payload
so clients can adjust.

**Why no new digest domain.** ADR 046 §7's deferral test
(concrete reader, authentication boundary, invalidation
rule) is not satisfied. Visible-domain subtraction alone
captures retention's effect on visible state. An event-
history domain is anti-entropy ADR territory. The substrate
already reserves the byte tag.

**Why retention applies to tombstones too.** A tombstone is
a storage fact with `__row_hlc__`. Once both the data row
and its tombstone are below the floor, both are eligible —
the visible state at the floor contains neither (the
tombstone hid the data row at admission), so storage should
reflect that. Treating tombstones specially would be
irregular.

**Why compaction-pinned (`compaction_start_hlc`) floor.**
Replicas computing different floors from local wall-clocks
would evict different rows, producing visible-state
divergence indistinguishable from anti-entropy bugs. Pinning
the floor to `compaction_start_hlc` (recorded in the
compaction transaction) makes retention reproducible.

**Why ambiguous-ownership-retains.** A row whose table
identity cannot be proven during reclaim (catalog snapshot
mid-rotation, recovery edge case) must not be dropped under
an unintended floor. Fail-closed retention is cheap (worst
case: row sticks around until next compaction) and prevents
silent data loss.

**Why shrinking is immediate, extending is forward-only.**
Retention is a promise about how far back `AS OF` reads are
supported. Shrinking narrows the promise — read admission
must enforce the new bound immediately, and physical reclaim
follows opportunistically. Extending widens the promise for
future data; previously-reclaimed bytes are gone.
Asymmetric, but matches operator intuition.

**Why a typed `SetTableRetentionPolicy` mutation bumps
`policy_generation`, not `schema_generation`.** The
descriptor is unchanged by a retention update. Bumping
`schema_generation` would force re-resolution of every
descriptor cache, write-admission validator, and execution
plan for what is fundamentally a lifecycle decision.
`policy_generation` is local to the catalog. Wendelon does
not parse DDL; the bump is driven by the typed mutation
applied through the control ring.

**Why pin the read-side floor at scan start.** A long-
running scan that re-resolves the floor mid-execution would
silently shift visibility partway through the result set.
Pinning at scan start gives a coherent snapshot. The cost is
one Duration subtraction recorded with the scan plan.

**Why defer PK-keyed table-specific retention.** "Row fell
out of the retention window" and "row was deleted /
superseded" need one shared supersession model. PK-keyed
delete is roadmap #6; mixing per-table PK retention into
V1 would risk a partial semantic that roadmap #6 has to
undo. PK-keyed reclaim continues on the cluster MVCC
retention path with the existing prefix rule (newest
version below the floor always retained). Operators
wanting per-table retention on a PK-keyed table wait for
roadmap #6 or its retention follow-up.

**Why a control-ring mutation, not a request frame.**
Retention policy is replicated cluster state. The control
ring already has the Raft log, mutation pipeline, and audit
hooks. Adding a frame family would duplicate machinery for
a low-frequency administrative operation.

## Consequences

- Reclaim planning resolves table policy per row or per
  physical compaction group; one catalog snapshot read per
  compaction job.
- Single-row PK records require careful table ownership
  resolution; if ambiguous, retention keeps the row.
- Policy updates can make older `AS OF` reads fail
  immediately, even before background reclaim has physically
  removed bytes.
- Extending a retention window is not retroactive after
  bytes have been reclaimed.
- Multi-table query admission needs a set of table retention
  checks instead of one global check.
- The control-ring state machine grows a new replicated
  catalog (`table_lifecycle`) and a new mutation
  (`SetTableRetentionPolicy`).
- Operators need per-table observability to explain why one
  table is reclaiming while another is not.
- Anti-entropy must understand "transient divergence post-
  compaction" as legitimate; the deferred anti-entropy ADR
  inherits this constraint.
- Read-side floor adds one Duration subtraction per scan
  plan; pinned at scan start.
- Logical retention means a row above the physical floor but
  below the logical floor is invisible to reads; the column
  data is still present until compaction runs (operators
  can observe this gap as the lag metric).

## Rejected alternatives

- **Embed retention in `TableSchemaDescriptor`.** Rejected.
  The variant "always read the latest descriptor for the
  retention field, regardless of the requested generation"
  works but bifurcates the descriptor's contract: some
  fields are read at the requested generation (columns),
  others at latest (retention). Every code path that
  touches retention has to remember the bifurcation. The
  catalog approach makes "always latest" a structural
  invariant — the catalog exposes no `at_generation` API,
  so there is no way to accidentally read historical
  retention.
- **Replace the cluster MVCC policy entirely.** Rejected:
  the cluster policy is a sound default and the legacy KV
  policy. Tables opt into table-specific behavior via
  `Disabled` or `FixedWindow`.
- **Use one minimum floor across all tables during
  reclaim.** Rejected: a single long-retention table would
  pin the global reclaim horizon, defeating short-retention
  tables.
- **Allow per-row TTL columns.** Rejected for V1: per-row
  TTL interacts with stats pruning, secondary indexes, CDC,
  and row-level audit in ways that need their own design.
- **Persist retention `StateDigest` in slab footers.**
  Rejected: ADR 046 §7's durable-placement rule still
  applies.
- **Introduce `digest.retention.gc.v1` now.** Rejected: no
  concrete event-history reader exists in this ADR.
  Substrate has the byte tag reserved for the future
  consumer (audit / anti-entropy).
- **Background retention sweeper independent of
  compaction.** Rejected: doubles I/O, requires a new
  background job, fragments slab layout. Compaction-time
  eviction is free given compaction is already running.
- **Wall-clock retention floor computed per node at
  compaction.** Rejected: replicas diverge unpredictably.
  `compaction_start_hlc` is reproducible.
- **Soft retention (clamp `as_of` to floor; return partial
  results).** Rejected: hides a query semantic shift.
  Operators can't tell when their query was affected.
- **Retention as a per-write attribute.** Rejected:
  doubles per-write metadata; does not compose with the
  typed `SetTableRetentionPolicy` lifecycle mutation
  (which is the engine surface for retention updates).
- **Sub-minute retention.** Rejected: pathological
  compaction-loop risk.
- **Retention applied at write admission (reject old
  rows).** Rejected: misreads what retention is for.
- **Parse SQL-like retention syntax in Wendelon.** Rejected.
  Wendelon is a byte-code engine. User-facing DDL syntax
  belongs in client tools (Toron). The engine surface is
  the typed `SetTableRetentionPolicy` mutation only. Parse
  errors for malformed strings (unknown units, negatives,
  zeros) are client-tool concerns and have no reject codes
  at the engine surface.
- **Single-phase logical-only retention (no compaction
  reclaim).** Rejected: storage cost.
- **Single-phase physical-only retention (no read filter).**
  Rejected: contract weakens to compaction-timing.
- **Separate Raft entry for default `CREATE TABLE`
  lifecycle bootstrap.** Rejected: the default
  `InheritCluster` lifecycle row is part of the schema-
  create apply effect, uses the all-zero bootstrap HLC
  sentinel, and is overwritten by the first explicit typed
  `SetTableRetentionPolicy` mutation. Non-default initial
  policy remains a typed lifecycle mutation submitted after
  create with expected generation zero.

## Resolved questions

1. **Retention lives in lifecycle catalog, not descriptor**
   (§1). Schema generation is a compatibility contract for
   user columns; retention is current authority. Embedding
   it in the descriptor would let old generation reads
   apply stale retention.

2. **Three policy modes**: `InheritCluster` /
   `Disabled` / `FixedWindow`.

3. **Two-phase retention** (§6): logical row filter +
   `as_of < floor` reject at read (precise bound
   `floor <= __row_hlc__ <= as_of_hlc`), physical
   eviction at compaction.

4. **No `digest.retention.gc.v1` domain in V1.** Visible-
   domain reversal alone (§12) validates storage-state
   equality after eviction. ADR 046 §7's deferral test is
   not satisfied here.

5. **Hard error on `as_of < floor`**
   (`RETENTION_FLOOR_EXCEEDED`), not clamp. Clamping shifts
   query semantics silently.

6. **PK-keyed table-specific retention deferred.** PK-keyed
   tables stay on the cluster MVCC retention path; table-
   specific overrides reject with
   `RETENTION_REJECTED_PK_DISALLOWED`. The PK-keyed-row-
   delete ADR (roadmap #6) settles supersession +
   retention together.

7. **Multi-table query admission requires all-pass.** A
   single expired table rejects the whole request (§7).

8. **Ambiguous-ownership retains the row** during reclaim
   (§8). Fail-closed under catalog rotation / recovery
   edge cases.

9. **`RetentionWindowMillis` bounds**: 1 minute minimum,
   100 years maximum. The upper bound exists for HLC
   arithmetic safety; operators wanting "effectively
   unbounded" use `Disabled` or `InheritCluster`.

10. **`CREATE TABLE` + initial lifecycle policy**: schema
    create initializes the default `InheritCluster`
    lifecycle row in the same state-machine apply effect.
    The implicit bootstrap row uses an all-zero HLC
    sentinel; the first explicit typed policy update
    overwrites it.

11. **Permission model for retention updates**: tenant-
    admin in V1. A stricter data-lifecycle permission is
    a follow-up.

12. **CSB direct-answer eligibility under retention**
    (§13): slabs wholly above floor allow direct CSB;
    slabs wholly below floor are zero-contribution; slabs
    straddling the floor fall back to row-level filtering.
    Requires per-slab `min(__row_hlc__)` /
    `max(__row_hlc__)` in the slab summary (open question
    §2).

13. **Drop-table ordering**: deferred until table drops are
    re-enabled. The intended ordering remains lifecycle
    entry first, then descriptor, in one replay-safe apply
    effect.

14. **`SetTableRetentionPolicy` is a new mutation** in the
    existing `WendelonRequest` enum, not a schema-mutation
    extension. Lifecycle is distinct from schema.

15. **DDL parsing is not in Wendelon.** The engine surface
    is the typed `SetTableRetentionPolicy` mutation. User-
    facing DDL syntax (string vs `INTERVAL` form, `INHERIT`
    visibility, unit lists) is a client-tool ADR concern.
    ADR §3 reproduces a typical client convention as
    informational reference only.

## Open questions

V1 picks defaults; each can be revisited in a follow-up
without on-disk surface change. None block promotion.

1. **Dry-run gate for shrink.** V1 picks no. Operators
   estimate impact via the
   `storage_table_retention_lag_seconds` and
   `_read_filtered_rows_total` metrics under the soon-to-
   be-active policy, optionally by setting a longer policy
   first as a staging point. A future ADR can add a
   shrink-time estimator if metric-based estimation proves
   insufficient.

2. **CSB retention-completeness proof shape.** V1 uses
   existing per-slab row-HLC stats (min / max
   `__row_hlc__` bounds in the slab summary). Phase 4 / 5
   verify availability and add to the slab summary if
   missing. No new footer field.

3. **Retention-driven compaction trigger.** V1 picks no.
   Existing triggers (size, age, fragmentation) plus the
   `_lag_seconds` metric let operators tune thresholds
   against retention lag. A retention-eligible-volume
   trigger can be added later if data shows operators want
   bounded lag without tuning generic compaction.

4. **Single-row PK ownership proof for observability.** V1
   PK-keyed reclaim doesn't depend on table ownership
   (cluster floor only), but per-table metrics on PK-keyed
   tables would require ownership resolution. V1 omits
   per-table PK metrics; tenants see PK reclaim under the
   cluster aggregate. The PK-keyed-delete ADR (roadmap #6)
   settles ownership proof along with table-specific PK
   retention.

## Forward-compatibility analysis

**Anti-entropy gossip (deferred).** Replicas with different
compaction states have transient visible-digest divergence
above the floor. Anti-entropy must accept this as
legitimate. The deferred ADR may pull on the reserved
`digest.retention.gc.v1` byte tag to track event-history
provenance; substrate is ready.

**Content-hash identity (roadmap #5).** Content-hash
retention is the same shape as append-only retention. The
lifecycle policy applies unchanged.

**PK-keyed row-delete (roadmap #6).** PK-keyed tables in V1
fall through to cluster MVCC PK reclaim (ADR 013); §10
defers any PK-keyed-specific table floor to roadmap #6.
The PK-keyed-delete ADR may extend retention to support
"drop entire PK chain whose latest is older than floor"
semantics; the lifecycle catalog field carries through
unchanged.

**Logical / physical descriptor fingerprint split
(roadmap #7).** Lifecycle policy sits outside both
fingerprints by construction (§1). Roadmap #7 inherits this.

**Secondary indexes (roadmap #8).** Index segments at
compaction time are rebuilt against post-retention rows.
Retention-evicted rows do not appear in the rebuilt index.
The future secondary-index ADR consumes the same lifecycle
catalog.

**CDC (roadmap #10).** Retention-evicted rows are
observable to CDC consumers as a `retention-evicted` event
class — the surface is the CDC ADR's call. ADR 048 pins
one CDC-relevant property: retention is **not** silently a
delete in CDC; consumers see a distinct event class so they
can distinguish operator intent from time policy.

**LSM compaction (roadmap #11).** Retention extends to
level merges. Each level's compaction applies retention to
its inputs.

**Tombstone-density alarms (deferred from ADR 047).**
Retention reduces long-term tombstone density by reclaiming
old `(data, tombstone)` pairs. Alarms can use this.

**Audit / event-history digest domain.** If a future audit
ADR pulls on retention-evicted-row provenance, it allocates
`digest.retention.gc.v1` (substrate already reserved at
ADR 046 §4) at that time.

## Deferred to follow-up ADRs

1. PK-keyed row-disappear via retention (interacts with
   roadmap #6).
2. Per-column retention.
3. Per-partition retention.
4. Per-row retention overrides (hold / evict-early hints).
5. Predicate-based retention.
6. Legal-hold / retention freeze surface.
7. CDC retention-evicted event surface.
8. Anti-entropy retention-floor coordination.
9. Retention-driven compaction trigger.
10. Audit event-history digest domain
    (`digest.retention.gc.v1`).
11. Retention-aware secondary-index propagation rules.
12. Tenant-level retention defaults (between cluster and
    table).

## References

- **ADR 012** — bounded clock drift (cross-node guardrail
  for floor computation).
- **ADR 013** — cluster MVCC retention (the existing
  authority that ADR 048 extends with table-specific
  policy).
- **ADR 022** — slab footer (retention does not consume any
  new footer bits).
- **ADR 040** — CSB visibility metadata (unchanged by
  retention).
- **ADR 042** — single-coherent compaction (retention runs
  inside this pass).
- **ADR 044** — column-projected fetch (retention floor
  check is one Duration subtraction at scan plan).
- **ADR 045** — append-only identity, `__row_hlc__` and
  `__row_table_object_id__` (retention's eviction key and
  ownership resolver).
- **ADR 046** — `StateDigest` substrate, `subtract_element`
  (retention's algebra).
- **ADR 047** — row-level tombstones, §9 retention-floor
  pair-GC rule (this ADR makes the per-table surface
  concrete).
- `plans/conventions/{newtype-units, no-premature-backward-
  compat, no-version-suffixes, no-project-name-in-protocols,
  capability-vs-feature-naming}.md`.
