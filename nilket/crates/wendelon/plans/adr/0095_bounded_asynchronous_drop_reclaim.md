# ADR 0095 - Bounded asynchronous drop reclaim

## Context

Wendelon currently performs table DROP physical reclaim synchronously inside
the DROP RPC. The handler fences the object, runs three reclaim passes with
unbounded work budgets, finalizes the drop, and only then replies. At the
PRODUCT-0137 scale this meets Toron's 60-second statement authority. The
abandoned server work continues, and retries can start concurrent reclaim over
the same mutable slabs. Live evidence showed approximately 125 GB read against
an 839 MB profile, approximately 205% CPU, and compaction warnings in the
missing-slab class.

ADR 0093 made a lost DROP response recoverable and kept the control connection
alive beyond Quinn's former idle boundary. Those guarantees hold: failures are
typed, the operation does not wedge, and no false receipt is returned. They do
not bound physical reclaim or remove it from statement authority.

The existing table-drop fence is the admission authority. It is consulted by
reads, streaming reads, writes, deletes, CDC, and Raft apply paths. Removing it
at finalize while the descriptor remains addressable would reopen stale
object-ID access. The retained outcome and a reclaim-debt record do not
currently gate admission.

The existing reclaim limits count slabs compacted, not slabs examined. A pass
can therefore inspect every nonmatching slab while reporting no budget use.
The global migrated-range tombstone pass also already has a bounded owner in
the ordinary sweeper; running an unbounded copy inside tenant DROP is
redundant.

Erasure is distributed. A table can span several partitions, ADR 0090 permits
partitioned and legacy raw-locality records to coexist permanently, migration
can leave physical bytes on former owners, and ownership can move while debt
is outstanding. A leader-local scan cannot prove cluster erasure.

## Decision

### 1. DROP completes logically before physical reclaim

The statement path becomes:

1. install the existing drop fence;
2. finalize the logical drop;
3. return success;
4. reclaim physical data asynchronously.

Finalize atomically converts the fence into a logical-drop tombstone, unbinds
the name, records the retained ADR 0093 outcome, and creates durable reclaim
debt. The tombstone has exactly the fence's admission authority and remains
until settlement. Descriptors remain available only to reclaim machinery.

A durable fence left by a crash before finalize is automatically rolled
forward to tombstone plus debt when the sweeper activates. Rollback is
rejected: the accepted DROP already made the object inaccessible, and rolling
forward is the deterministic outcome that never reopens it.

Settlement is one prevalidated Raft apply: remove debt, remove the tombstone,
call `retire_unbound_object`, and retain the terminal outcome. No debt
tombstone is introduced.

Reclaim debt is replicated state. Snapshot format moves from v4 to v5 and WAL
body format from v3 to v4, using frozen version spines, immutable fixtures, and
total upconversion. A pre-v5 fence upconverts as a fence and follows the
roll-forward rule; historical state does not invent completed reclaim.

### 2. The erasure participant set is conservative and deterministic

Finalize stamps an immutable participant set computed only from replicated
state at its log index. It is the union of:

- for partitioned records, every replica owner of every partition the table
  descriptor can place rows into;
- for legacy raw-locality records, every replica owner whose ring arc
  intersects any descriptor-derived possible locality range; and
- every `source_replicas` node from every migration-ledger entry whose range
  intersects those possible locality ranges, regardless of whether the entry
  is pending, in progress, or completed.

`table_spread_prefix_ranges` and the descriptor's locality policy define the
possible ranges. Placement and raw-locality ownership are evaluated against
the replicated membership and ring epoch closed by finalize. Completed
migrations are included because completion moves logical ownership but does
not prove that old source bytes were erased. There is no durable source-erasure
predicate today, so no migration source is excluded on that basis.

Over-inclusion is safe: a node with no matching bytes completes a bounded empty
drain and returns a receipt. Under-inclusion is the defect. The rule explicitly
covers tables containing both ADR 0090 placement semantics.

The set remains closed after finalize. Migration must filter tombstoned-object
rows and never copy them to a node outside the stamp. Filtering is row-level:
a packed record can contain live, tombstoned-object, and legacy rows together,
so skipping or copying the whole record is incorrect.

### 3. Reclaim is coordinated by bounded node RPCs

One leader-resident debt sweeper processes debts in finalize-log-index order
with concurrency one. It carries an ownership epoch; every slice request,
progress update, receipt, and settlement validates the current epoch. A
deposed sweeper cannot advance or settle work.

For every stamped participant, the leader issues reclaim-slice RPCs over the
existing mutually authenticated node transport. At RF=1 this short-circuits
locally through the same handler. The request carries:

```text
{ operation_id, finalize_log_index, ownership_epoch, phase, examined_budget }
```

It deliberately carries no cursor. Each node's durable local cursor is the
sole progress authority; a stale leader cannot supply an advanced cursor and
skip work. The response reports examined work and whether the phase is done.

When a participant completes, it durably records provenance:

```text
{ node_id, operation_id, finalize_log_index, ownership_epoch }
```

Settlement trusts the current leader's epoch-prevalidated assertion that the
complete stamped participant set has attested. Mutual transport authentication
identifies the peers during collection, but these provenance values are not
cryptographically verifiable during Raft apply. The system's guarantee is its
existing crash-fault model, not Byzantine-leader resistance. Signed,
apply-verifiable receipts are deferred hardening.

### 4. Receipts cannot outrun admitted mutations

Each node gains a per-`(tenant, table_object_id)` generation-tagged in-flight
mutation registry. An RAII guard is registered atomically with the same
lifecycle admission check that consults the fence or tombstone. It covers
leader and replica writes, deletes, streaming and bulk writes, and spawned
durable work. There is no interval in which finalize can observe zero while an
admission has passed its lifecycle check but has not registered.

Applying finalize closes the current generation. The tombstone refuses new
admissions; a node may attest only after:

1. its applied index covers the finalize log index;
2. startup mutation reconciliation is complete;
3. every durable mutation resumed during reconciliation has registered its
   guard;
4. the closed pre-finalize generation count reaches zero; and
5. all reclaim phases complete from the node's authoritative cursor.

The startup barrier is required because the registry is node-local: a restart
must not turn an unfinished durable mutation into an apparent zero count.

### 5. Progress has a durable format and bounded repair

Node-local redb table `drop_reclaim_progress_v1` is keyed by canonical
`DropOperationId` bytes. Its versioned value carries phase, phase-specific
cursor, examined total, ownership epoch, completion state, and receipt
provenance.

A cursor written under an old epoch is valid for idempotent resume, but only
the current epoch may persist progress or produce a receipt. An absent key
means not started. An undecodable value is named corruption: it is counted and
logged, a corruption marker is persisted, and the affected phase enters an
explicit reset-from-zero rebuild bounded by normal slice budgets. Missing and
malformed state never share an outcome. Future incompatible formats fork the
table name.

Receipt production performs no final unbounded scan. Completion consists of
every phase cursor reaching its end plus bounded keyed checks of the in-memory
and persistent indexes. The slice traversal itself accounts for L1 and L2.

### 6. Bounded work means bounded examined work

ADR 0095 introduces new cursor-based reclaim-slice APIs. Input is phase,
authoritative cursor, and examined-work budget; output is the new cursor,
examined count, rewritten count, and completion flag. A slice examines at most
its budget and yields. Existing `max_slabs` functions are not extended or
treated as evidence of bounded work.

The ordinary sweeper remains the sole owner of global migrated-range
tombstones. DROP no longer invokes that pass.

### 7. Reclaim and compaction exclude each other per slab

The existing per-slab lease is generalized to acquire before rewrite rather
than at compaction completion. Reclaim and compaction use the same acquisition
order and never rewrite one slab concurrently. Exclusion remains per slab, so
unrelated compaction continues. The lock-order change is grounded before
implementation and is the riskiest refactor in this cut.

Logical deletion does not immediately remove physical exposure. Tenant and
slab keys cannot be destroyed while co-resident survivors need them. Measured
drain time under concurrency one is therefore recorded as erasure-latency
policy in the implementation receipt.

## Acceptance

- **C1 — responsiveness:** a one-million-row DROP returns within statement
  authority with tombstone and debt durable. A same-name replacement is
  immediately readable while old-object debt remains.
- **C2 — restart:** a crash after prepare rolls the fence forward. A crash
  after debt creation resumes from durable progress. A crash with a
  pre-finalize durable mutation cannot produce a receipt until startup
  reconciliation registers and drains that work. Settled debt stays settled.
- **C3 — admission:** every former fence site refuses the dropped object by
  ID between finalize and settlement, with one discriminating arm per path
  class.
- **C4 — exclusion:** deterministic barriers reproduce the missing-slab class
  without the generalized lease and prove clean serialization with it.
- **C5 — bounded work:** against many nonmatching slabs, examined work never
  exceeds budget and the next slice resumes after prior work.
- **C6 — ownership:** retries spawn no extra sweeper; a leadership change
  fences the old sweeper's RPC, progress, receipt, and settlement actions.
- **C7 — truthful settlement:** every stamped participant must attest after
  finalize application, startup reconciliation, and mutation drain.
  Independent checks find no target references in memory, persistent index,
  L1, or L2. A withheld receipt blocks settlement without losing progress.
- **C8 — participant closure:** controls cover different partition and legacy
  raw-locality owners, an over-included empty participant, and pending plus
  completed migration sources. A migration after finalize filters a mixed
  packed record row-by-row, preserves live and legacy rows, copies no dropped
  row, and does not widen the stamp.
- **C9 — sole global owner:** migrated-range tombstone debt still converges
  through the bounded ordinary sweeper after the DROP-path pass is removed.

The forward exhibit is the viz cleanup workload on optiplex. It is explicitly
the RF=1 degenerate participant case; multi-node behavior is established by
C7 and C8 in-repository controls.

## Guardrails

- Fence and logical-drop tombstone admission sites have a structural census.
- Every mutation admission path has a structural census requiring the
  generation registry; recovery cannot open receipt production before its
  reconciliation barrier.
- No statement-reachable path invokes unbounded reclaim or the global
  tombstone pass.
- Participant derivation is total across partitioned and raw-locality
  placement, includes every intersecting migration source, and uses only
  replicated finalize-time state.
- Migration filters tombstoned rows within records.
- Settlement prevalidates epoch and the complete stamped set, then performs
  debt removal, tombstone removal, descriptor retirement, and outcome
  retention in one apply.
- Snapshot/WAL fixture corpora and exact-set guards cover v5/v4.

## Risks and deferrals

- The admission registry adds a per-object hot-path operation to every
  mutation; its overhead is measured in the receipt.
- Current-leader attestation is the interim trust boundary. Signed,
  apply-verifiable receipts are deferred.
- Reclaim-aware slab placement could reduce erasure latency by preventing
  dropped and surviving rows from sharing slabs.
- Concurrency above one is measurement-gated.
- Debt-drain observability and settled outcome/debt compaction remain forward
  work.

## Forward

1. Run and record the viz cleanup exhibit and measured drain time.
2. Add signed, apply-verifiable erasure receipts if Byzantine-leader
   resistance enters the threat model.
3. Evaluate reclaim-aware slab placement and higher concurrency from evidence.
4. Add viz-grade debt observability and bounded settled-state compaction.
