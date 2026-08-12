# ADR 548 - CTAS staged publication and composite outcome

**Status:** Accepted and implemented 2026-08-12. Restores the replacement
withdrawn by `73afc779`.

## Context

CTAS commits a catalog mutation and a row mutation independently, in that order,
with nothing that undoes the first if the second fails. The row write is already
atomic — ADR 0542 fixed the commit unit at the parent mutation batch, and CTAS
sends one payload — so the composite has exactly three end states,
`{no table}`, `{table, no rows}`, `{table, rows}`, and no partially-populated
one. Two are defects:

- **`{table, empty}` is indistinguishable from success over an empty source.**
  Both are reachable and the legitimate one is pinned by
  `query_coordinator_empty_source_ctas_initializes_queryable_table`. Absence
  read as zero, at statement level.
- **`OR REPLACE` destroyed the target before its replacement existed.** Measured
  by restoring the withdrawn code: a statement losing a create race dropped the
  winner, created its replacement, and returned `Ok`.

`73afc779` refuses destructive replacement rather than performing it. That is a
stop-gap bought by withdrawing a capability; this record restores it.

Attribution alone cannot, because a design that only *reports* `{table, empty}`
blesses a visible partial table as legitimate, and atomic replacement would then
have to redesign that lifecycle. One state machine covers both.

### What the storage layer already provides, and what it does not

The catalog separates naming from identity: `latest_by_name` maps
`TableNameCatalogKey → TableSchemaCatalogKey`, distinct from the `by_id`
descriptor store, and `unbind_latest_name` removes a binding while the
descriptor survives. Mutation outcomes are **durable**: `MutationOutcomeStore`
(`wendelon/src/network/mutation_outcomes.rs`) is redb-backed with `OUTCOMES`,
`OUTCOME_EXPIRY` and `EXPIRED` tables.

Three things an earlier draft of this record got wrong, corrected here because
each changes the design:

1. **Unbound does not mean provisional.** `descriptors_for_tenant` ranges over
   `by_id` and returns every descriptor regardless of binding, and names are
   also removed from `latest_by_name` in ordinary drop paths. Absence from the
   name map encodes neither lifecycle nor ownership.
2. **No publication primitive exists.** Only `unbind_latest_name` is present.
   "Publication is a single map update" described a capability, not an
   operation; a compare-and-swap bind must be built.
3. **The outcome store is durable, not in-process.** The earlier draft quoted
   ADR 0542's planning-time description of an in-process `DashMap`. The shipped
   store is redb and is the starting point for the row phase, not an obstacle
   to it.

The mechanism is still composition over existing structure rather than a
cross-ring transaction. What must be added is an explicit saga ledger, a CAS
publication primitive, and a durable retirement phase.

## Decision

### 1. One composite operation id, client-supplied, with an intent fingerprint

A CTAS operation has one composite id, established before any durable action,
from which **domain-separated** sub-identities derive: catalog phase, row write,
publication, retirement. Domain separation keeps a replay of one phase from
resolving another.

**Domain separation must not disturb `ClientWriteId.client_uuid`.**
`operation_identity_is_expired` reads the UUIDv7 issuance timestamp out of those
16 bytes, and that is what makes post-retention expiry self-authenticating: once
detail and tombstone rows are both reclaimed, the timestamp is the only thing
distinguishing an old replay from a new mutation. `uuid_v7_unix_millis` returns
`None` for bytes that are not UUIDv7-shaped, so hashing a derived row id into
`client_uuid` would resolve `Unknown` instead of `Expired` and re-enable
double-apply. The row sub-identity therefore **keeps the caller's UUIDv7 as
`client_uuid` and domain-separates through reserved `sequence_no` values**, or
supplies equivalent permanent expiry evidence of its own.

**Recovery is client-driven, and that is selected here rather than deferred.**
`ToronClient::execute_with_query_id` lets a caller mint and retain the query id,
the daemon carries `frame.query_id` into the execution context, and
`mutation_operation_id` already derives the write identity deterministically
from it. `resolve_mutation(operation_query_id)` accepts the same id for later
resolution. The identity a caller can hold across a lost response therefore
already exists end to end; CTAS adopts it as the composite id.

The ledger additionally records an **intent fingerprint** over the *immutable
request intent*. Its inputs are enumerated rather than described, so two
implementations cannot disagree: the statement or request bytes, the target
logical name, the result schema, the session timezone, and the planner settings
that affect the result. It excludes every mutable catalog binding — after a
successful replacement the target resolves to the new object, so a fingerprint
including it would falsely reject a legitimate replay of the very operation that
created it. The binding observed at start is recorded separately, as the
captured prior binding in decision 2.

A composite id presented with a different intent is **refused**, not resumed:
reusing one query id for two different CTAS statements must never let the second
inherit the first's saga.

Every execution begins with a ledger lookup on the composite id, so a replay is
recognised before any phase re-runs.

### 2. A provisional object is declared by a durable saga ledger

Provisional status is **explicit state**, never inferred from an absent name
binding. The ledger durably records, per composite id: object id, intended name,
the expected prior binding captured at start, and the current phase.

The captured binding is the **complete `TableSchemaCatalogKey`** — object id
*and* schema generation — or an explicit "absent". Object id alone is
insufficient: an `ALTER` preserves the object id while advancing the generation,
so a generation-blind comparison would miss a concurrent alteration while
claiming to detect one.

The ledger is the authority for "is this object live". Every by-id consumer must
consult it — statistics, secondary indexes, migrations, retention, descriptor
sync, and ordinary object-id reads. A provisional object is **not** a tombstone:
it is an object that has never been live, which is a different thing from one
that has been retired.

### 3. The row outcome reaches publication by attestation, not inspection

`MutationOutcomeStore` lives in the data ring; the publication CAS applies in a
control-ring raft mutation. **They are separate persistence domains, and the CAS
cannot read the outcome store.** The handoff is explicit:

```text
row outcome committed in Wendelon
  → coordinator resolves it by the derived row sub-id
  → control-ring saga ledger records a RowsCommitted attestation
  → publication CAS checks that ledger phase
```

A crash between the first two steps recovers by re-resolving the derived row
sub-id; the attestation is idempotent.

**Retention must outlive the handoff, and today it does not.** Detailed outcomes
are capped at `OUTCOME_CAPACITY = 4_096` entries with a TTL clamped to at most
300 seconds, and `EXPIRED` records only that an identity aged out — never
whether its write committed. A saga stalled past that horizon would lose the
only evidence that could authorise publication, and publishing on an aged-out
identity would be publishing on an unknown.

This record therefore requires the row outcome to be **pinned until the control
ledger records the terminal attestation** — acknowledgement-based rather than
time-based — or an equivalent durable commit witness retained for the saga's
lifetime. Whichever is chosen, no saga may depend on evidence that can expire
beneath it.

### 4. Completion is explicit; an empty source is stated, never inferred

The row phase records completion as a fact. Zero rows written is not evidence of
completion, and completion is not inferred from a queryable empty table.

### 5. Publication is a compare-and-swap, not a write

A new catalog primitive binds the name atomically, and **fails** unless all
three hold in the same mutation:

- the current binding still matches what was captured at start — still absent,
  or still naming the captured old object;
- the provisional object belongs to **this** composite operation, per the
  ledger;
- the saga ledger carries the **RowsCommitted attestation** from decision 3.
  The CAS checks the control-ring ledger, not the data-ring outcome store, which
  it cannot read.

Any concurrent create, replace, alter, or drop yields a **typed conflict** and
leaves the winner untouched. Losing a race means losing the swap; nothing is
destroyed.

Catalog caches converge, so readers may briefly retain the old binding. The old
object therefore **cannot** be reclaimed at swap time.

### 6. Replacement retires the old object as a durable phase

Publication does not end the saga. The old descriptor and its storage still
exist and must be retired through the existing drop and fence lifecycle,
durably and retryably:

```text
prepared → provisioned → rows committed
                              ├─ CAS succeeds → published → retiring old → complete
                              └─ CAS conflicts → reclaim provisional → conflict
```

Readers observe the old object or the new one, never absent and never
provisional. In-flight readers of the old object drain before its storage is
reclaimed.

**Two cleanups share machinery but not semantics**, and the record keeps them
distinct:

- **Row known-not-committed** → reclaim the provisional object → terminal
  **`reclaimed`**, preserving the *row-phase* cause. Nothing was published and
  nothing was populated.
- **Rows committed, publication CAS lost** → reclaim the *populated*
  provisional object → terminal **`conflict`**, preserving the *publication*
  cause. A concurrent create, replace, alter or drop won the name.

Collapsing these would report a lost race as a failed write, and a failed write
as a lost race. Both never overwrite the winner, and never strand populated
provisional storage.

### 7. Everything that can fail without a target runs before provisioning

Source execution, materialization and validation do not need the target
descriptor, so they run **before** the provisional object exists. A failure
there leaves nothing behind; only failures after provisioning leave a resolvable
saga.

**A restart after provisioning but before row dispatch aborts the saga and
reclaims it.** The materialized payload is not durable, and re-executing the
source under the same composite id could write different rows than the caller's
statement was resolved against — a silent substitution. Unless payloads are
durably staged, which this record does not require, that state is terminal and
its remedy is a new operation.

### 8. Resolution reports each phase, with phase-appropriate causes

A caller resolving a composite id learns the catalog, row, publication and
retirement phases separately. All four share ADR 0542's commit-knowledge axis —
`committed`, `known-not-committed`, `indeterminate` — but keep **phase-specific
causes**. A row-write cause is not applied to a catalog operation.

This requires a **new or extended typed wire response**. The existing
`resolve_mutation` endpoint transports the caller's identity but returns a
row-mutation-shaped outcome; it supplies identity plumbing, not the composite
recovery surface. A four-phase response is part of this record's scope, with a
round-trip acceptance over it.

### 9. `IF NOT EXISTS` races resolve by compatibility, never by overwrite

A concurrent winner that is compatible makes the statement a success/no-op, and
the provisional object is cleaned up. An incompatible winner is a typed
conflict. The winner is never overwritten in either case.

### 10. The deadline bounds dispatch; outstanding work is bounded per activation

CTAS currently sends `deadline: None`; an operation id alone does not discharge
ADR 0542's exclusion. The contract is decided here rather than deferred:

- The caller's absolute deadline **stops the dispatch of phases not yet begun**.
  It never aborts a phase already durably committed, because abandoning a
  committed row phase would strand rows under an unpublished object.
- **That deadline is not durable today.** It is a process-local `Instant`
  reconstructed as `now + timeout`, which a restarted activation can neither
  persist nor compare. This record therefore requires one of two things, chosen
  in Stage 0: a durable deadline representation recorded in the ledger, or an
  explicit scoping of the caller deadline to the **originating activation
  only**, with every later activation bounded solely by durable attempt budgets.
  What is forbidden is carrying an `Instant` across a restart and pretending it
  still means something.
- Expiry after provisioning but before attestation does **not** blindly abort. A
  row write may be in flight, or committed but unattested. The saga first
  **resolves** the derived row sub-id, then publishes if committed, or reclaims
  the provisional object if known-not-committed. Only a saga with no dispatched
  row write aborts outright under decision 7.
- Once outstanding work exists — an unattested row outcome, an unpublished
  commit, or an unreclaimed provisional object — the saga **must reach a
  terminal state**. Each worker *activation* is bounded by an attempt budget;
  exhausting that budget records **retry debt** and releases ownership so
  another activation can take over. Exhaustion of one activation is **not** a
  terminal lifecycle state while publication or cleanup remains outstanding,
  because a saga abandoned there strands storage under an unresolvable name.
- Terminal states are `complete`, `conflict`, and `reclaimed`. Nothing else ends
  a saga.

### 11. Abandonment is proven by ownership, on a lease this record adds

A saga is abandoned when the ownership lease that held it is **provably gone** —
expired or superseded by a later epoch — and the ledger shows no phase advance
under the new owner. Elapsed time is evidence of slowness, not of abandonment,
and a long-running source must never be reclaimed beneath itself.

**The required lease does not exist and this record adds it.** Toron's current
query leases are a process-local `HashMap<QueryId, Lease>` keyed on `Instant`
inside `distributed/namespace_lifecycle.rs`, scoped to shuffle namespaces. They
are neither durable nor coordinator epochs, and they cannot survive the restart
that abandonment detection exists to handle. This record therefore introduces a
**control-ring saga ownership lease with an epoch and compare-and-swap
takeover**: a new activation claims the saga by superseding the recorded epoch,
and only the current epoch may advance a phase.

Reclamation records bounded expired-operation evidence **in the saga ledger**.
The row store's `EXPIRED` table cannot serve this: a saga that failed before the
row phase never had a row identity to age out, so its only trace must be the
ledger's own.

The bound selected by Stage 0 is concrete: the ledger admits at most **4,096
active-plus-retained sagas**, retains terminal detail for **15 minutes**, and
never evicts active work. Provisioning prunes only terminal detail older than
that horizon; if the pool is still full, it refuses with `capacity_exhausted`.
Each removal advances a durable per-tenant UUIDv7 issuance-time high-water, so
bounded retention cannot make a reclaimed operation look new again. A first
provision must therefore carry a valid caller UUIDv7 no older than the same
15-minute horizon. That is longer than Toron's 60-second server statement
timeout and 120-second client query timeout, while making the admission and
replay contract explicit. An old or malformed identity is refused as `expired`
or `invalid_operation_id`, never applied afresh.

## Stage 0

Recovery scope is no longer open — decision 1 selects client-driven recovery on
existing surfaces. What remains:

1. **By-id consumer inventory.** Enumerate every reader of `by_id` /
   `descriptors_for_tenant` / object-id lookup — statistics, secondary indexes,
   migrations, retention, descriptor sync — and state how each excludes
   provisional objects. This gates implementation: a missed consumer exposes or
   reclaims an object that was never live.
2. **The CAS primitive.** Determine where decision 5's checks can be evaluated
   inside one raft-applied mutation, and what a concurrent swap on the same name
   yields.
3. **Outcome pinning.** Determine whether `MutationOutcomeStore` can pin an
   entry until attested, or whether a separate durable commit witness is
   required, given `OUTCOME_CAPACITY` and the 300-second TTL ceiling.
4. **Retirement and drain.** Determine how in-flight readers of a retired object
   are known to have drained, using the existing fence lifecycle.
5. **Deadline durability.** Decide between a durable deadline in the ledger and
   scoping the caller deadline to the originating activation, given that the
   current deadline is a process-local `Instant`.
6. **Saga ownership lease.** Determine where a durable control-ring ownership
   epoch with CAS takeover lives, given that today's leases are process-local
   and shuffle-scoped. This is construction, not confirmation.

## Acceptance

1. **No published table is ever unpopulated.** Failures injected at
   materialization, validation, write, and publication each leave the name
   unbound or unchanged.
2. **Pre-provision failures leave nothing.** A source or validation failure
   creates no provisional object and no ledger entry.
3. **Empty source publishes and says so.** A legitimately empty result publishes
   a queryable empty table with explicit completion; an unfinished one is
   distinguishable by resolution, not by row count.
4. **Replacement never destroys the old table before the swap**, and the old
   binding resolves and returns its original rows at every injected failure
   point.
5. **Publication CAS rejects each violated precondition** — changed binding,
   foreign provisional object, uncommitted row phase — with a typed conflict,
   one fixture each, winner untouched.
6. **Provisional objects are invisible to every by-id consumer** from Stage 0's
   inventory, with one control per consumer.
7. **Abandoned sagas are reclaimed on ownership evidence**, never on elapsed
   time; bounded expired-operation evidence survives reclamation **in the saga
   ledger**, including for sagas that never reached the row phase; and a
   reclaimed id replays as expired rather than applied afresh.
8. **Retirement completes or resumes**, readers never observe an absent name
   across a swap, and reclamation waits for drain.
9. **Replay is safe** across a restart, per phase, and does not double-apply.
   A composite id re-presented with a different intent fingerprint is refused
   rather than resumed, and a restart between provisioning and row dispatch
   aborts and reclaims rather than re-running the source.
10. **Row evidence outlives the handoff.** A saga stalled past the outcome TTL
    still publishes correctly, or fails with a typed cause — never publishes on
    an aged-out identity.
11. **A lost publication CAS terminates as `conflict`.** Rows commit, a
    concurrent mutation wins the name, and the saga reclaims the populated
    provisional object and terminates as `conflict` preserving the publication
    cause — winner untouched, no populated provisional storage stranded.
12. **A known-not-committed row outcome terminates as `reclaimed`**, preserving
    the row-phase cause. One fixture each, asserting the two are not reported
    interchangeably.
13. **Ownership takeover works across a restart.** A saga whose owning epoch is
    superseded is resumed by the new owner, and only the current epoch may
    advance a phase.
14. **Activation exhaustion is not terminal.** A saga whose worker activation
    exhausts its attempt budget records retry debt, releases ownership, and is
    driven to a terminal state by a later activation.
15. **Composite resolution round-trips.** A caller resolving a composite id over
    the wire receives all four phases with phase-specific causes and the shared
    commit-knowledge axis.
16. **The refusal is lifted and its controls invert.** The three refusal
    controls from `73afc779` become replacement controls, and the guard that no
    `OR REPLACE` path calls `drop_existing_table_with_storage_admin` still holds:
    retirement uses the drop lifecycle *after* publication, never before.

## Guardrails

- No cross-ring transaction. Phases commit independently; the saga sequences
  them and the ledger resolves them.
- No visible partial table, and no lifecycle inferred from an absent name
  binding.
- No completion inferred from row count, elapsed time, or table existence.
- CTAS stops sending `operation_id: None` and `deadline: None`; ADR 0542's
  exclusion is discharged rather than restated.

## Risks

- **The provisional object is a new lifecycle state.** Stage 0 item 2 exists
  because a missed by-id consumer would expose or reclaim an object that was
  never live.
- **Reclamation can destroy an in-flight operation** if abandonment is judged by
  age. Acceptance 7 requires evidence.
- **Retirement can strand storage** if publication succeeds and retirement never
  resumes. Acceptance 8 requires resumption, not best effort.
- **Scope pressure toward a general transaction.** The saga is scoped to CTAS.

## Forward

Discharges ADR 0542's named CTAS exclusion and restores the replacement
withdrawn by `73afc779`. The CAS bind and the retirement phase are the
mechanisms an atomic `RENAME` or online rebuild would reuse; this record does
not build for them.
