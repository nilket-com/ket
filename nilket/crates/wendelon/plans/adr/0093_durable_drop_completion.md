# ADR 0093 - Durable table-drop completion

## Context

A `DROP TABLE` whose response is lost wedges the table's name unrecoverably.
The failure was observed live (PRODUCT-0137 scatter-threshold evidence, four
runs on a healthy deployment) and every leg is representable in current code.

The observed trigger is Quinn's default 30-second idle timeout. Wendelon's
admin drop runs physical reclaim synchronously between fence and finalize,
before any response is sent (`run_table_schema_drop` →
`drive_table_drop_physical_reclaim(...).await` in
`network/control_server.rs`), and no constructor in `wendelon/src/network/`
installs a `quinn::TransportConfig`. ADR 0577 fixed exactly this class on
every Toron QUIC endpoint; the Wendelon control link was outside its cut. A
reclaim slower than 30 seconds kills the connection while Wendelon keeps
going and finalizes.

The wedge itself is transport-independent — any lost success response
produces it. `apply_table_drop_finalize` (`consensus/raft_storage.rs`)
unbinds the name, removes the lifecycle entry and the ADR 0087 certificate,
and consumes the drop fence — the only state keyed by `DropOperationId`.
After finalize, a retry finds the binding gone and gets
`TableAlreadyDropped` (`control_server.rs`), which Toron collapses into
`TableNotFound` (`map_wendelon_admin_error_to_storage`,
`toron/src/storage.rs`). Toron's coordinator treats
`Ok(_) | Err(TableNotFound)` as storage-complete
(`drop_existing_table_with_storage_admin`, `toron/src/coordinator.rs`) and
proceeds to the catalog, whose guard correctly refuses without the local
finalized marker (`toron/src/catalog.rs`, installed only on a successful
response via `mark_finalized_drop`). The evidence that would let a retry
recover is destroyed at finalize; the collapse then hides even the
distinction between "never existed" and "dropped, completion unproven".
The observing harness additionally saw a same-name materialization issue a
success receipt over a relation that could not be read — the wedged state
leaking into receipt integrity, the one outcome the ADR 0087/0574
certificate machinery exists to exclude.

One structural fact narrows the design: `DropOperationId` is *derived*
(tenant, table_object_id, schema_generation), not random. A caller
re-issuing the same DROP is the same operation by construction; a different
operation id means a genuinely different lifecycle event.

## Decision

**1. Finalize retains a terminal outcome, with a fixed prevalidate-then-
mutate order.** The replicated state machine gains a drop-outcome record
keyed by `(TenantId, TableObjectId)` holding the completing
`DropOperationId` and the finalize entry's log index — which is threaded
into `apply_table_drop_finalize` (the prepare and reclaim applies already
receive `entry_log_index`; finalize does not, an omission this cut ends).
The apply's contract:

- *Recognize first:* an already-recorded outcome with the identical
  `(operation_id, finalize_log_index)` returns success before any fence is
  required — a replayed finalize entry is one completion, not an error.
- *Prevalidate everything fallible:* fence presence, operation match, both
  log-index checks, a conflicting retained outcome (same table, different
  operation or index — an error), and the name-unbind precondition — all
  before any state changes.
- *Then mutate infallibly only:* fence removal, outcome insertion, name
  unbinding, lifecycle and certificate cleanup. A validation failure leaves
  the state machine byte-identical to before the entry.

"Same raft apply" alone would not exclude a partial in-memory mutation when
a late step errors. This ordering does.

**2. The snapshot format version bumps: v2 is frozen, current becomes v3.**
Adding the outcome catalog changes the `SmState` field spine, which is
exactly what ADR 0092's versioned snapshot-body machinery exists for. This
cut freezes the outgoing v2 spine as its own version module (the pattern
`snapshot_body.rs::v1` established), dispatches v2 decodes through it,
upconverts with an **empty** outcome catalog (correct, not lossy: a pre-v3
deployment has no retained outcomes to lose, only the live wedge this ADR
repairs going forward), and pins the shape with immutable v2 and current
fixtures, extending ADR 0092's format-fixture corpus controls.

**3. The drop RPC becomes same-operation idempotent.** Before descriptor
resolution, `run_table_schema_drop` consults retained outcomes: a matching
`DropOperationId` returns success — the server cannot know whether its
earlier response arrived, so completing the same operation twice is one
completion. The same table under a *different* operation id keeps returning
`TableAlreadyDropped`: completion evidence is never borrowable across
operations. `TableMissing` remains the answer for tables with neither
descriptor nor outcome.

**4. Toron stops collapsing `TableAlreadyDropped` into `TableNotFound`.**
The mapping gains a distinct storage error, so the coordinator's
tolerated-absence arm no longer swallows it. With decision 3, a
same-operation retry returns `Ok`, Toron installs its local marker, and the
catalog removal proceeds — the wedge's exit is "run the DROP again". The
distinct error surfaces only genuinely foreign completion, as a failure.

**5. Every Wendelon QUIC endpoint receives an explicit liveness policy**
(mirroring ADR 0577): a finite `max_idle_timeout` and a
`keep_alive_interval` strictly shorter than it, installed at the shared
constructor choke points in `network/tls.rs` so server configs, outbound
client configs, and `wendelon-client` connections all inherit it. The
choke-point claim is pinned by a Wendelon **architecture guard** — no
`quinn::ServerConfig`/`ClientConfig` constructed outside those constructors
— so a future endpoint cannot silently regress to Quinn defaults.

**Test seams are feature-gated, not `#[cfg(test)]`-gated.** The acceptance
controls below need a severed response and a slow reclaim, which require
hooks inside Wendelon that Toron's integration tests can arm. Plain
`#[cfg(test)]` is crate-local — invisible to a dependent crate's tests — so
the hooks compile under a **non-default** Wendelon `test-support` feature
(`cfg(any(test, feature = "test-support"))`), forwarded by a non-default
Toron feature. An architecture guard proves default production builds
cannot arm them.

**Out of scope, held deliberately:** write chunking / parent-write chaining
(ADR 037 pt 10 — the next cut); asynchronous reclaim (synchronous reclaim
inside the RPC stays; the liveness policy makes it survivable); outcome
compaction (see Risks and deferrals).

## Acceptance

Two independent claims; neither certifies the other. A green A1 with no A2
evidence is keep-alive mistaken for the integrity fix, by construction.

**A1 (trigger):** with the explicit liveness policy, a drop whose reclaim
outlasts the idle timeout completes over a live connection. Two controls at
two altitudes:

- the compressed-timescale unit pair (silence beyond idle survives with
  keep-alive, dies without — both arms), which proves the transport
  mechanism only;
- an owned end-to-end exhibit — `#[ignore]`d for wall-clock, run explicitly
  for the evidence receipt — in which the DROP admin path itself holds
  reclaim beyond 30 real seconds and the public Toron DROP still succeeds.
  The unit pair alone does not discharge A1's literal claim.

**A2 (integrity):** retained same-operation evidence recovers after *any*
lost response, independent of transport timing. Its controls sever the
response deliberately — a `test-support` seam closes the stream after
remote finalize and **before the response is emitted**; a control that
merely constructs the post-loss state through a direct client call receives
its response, severs nothing, and is rejected as acceptance:

- the first public Toron DROP observes a transport failure; the
  same-operation retry succeeds and Toron installs the local finalized
  marker;
- a different `DropOperationId` for the same table is refused, never
  treated as complete;
- the full observed sequence replayed post-fix: severed DROP → retry
  recovers → catalog removal succeeds → same-name materialization → the
  relation is **read back**, not receipt-checked.

**P2 disposition (promotion gate on closing the finding, not a footnote):**
the impl plan probes whether a materialization issued *before* recovery
still publishes a success receipt over an unreadable relation — under the
observed multi-run shape, with the coordinator/descriptor registry rebuilt
against persisted Wendelon state between the wedged DROP and the
materialize; a warm same-process `TableAlreadyExists` disproves nothing. If
it reproduces, 0093 either closes it in this cut or this record explicitly
withdraws the claim that the receipt-integrity consequence is resolved, and
the originating finding stays open until the named follow-on lands.

## Guardrails

- The Toron catalog marker guard ("missing finalized DROP marker …") is
  unchanged — the backstop this ADR gives an exit to.
- The fence machinery's same-vs-different operation discrimination is
  preserved verbatim; the retained outcome extends it past finalize rather
  than replacing it.
- New Wendelon architecture guards: the QUIC endpoint census (decision 5);
  the snapshot format-fixture extension (decision 2); and the test-support
  gate — `test-support` absent from default features, every hook site under
  `cfg(any(test, feature = "test-support"))`. No existing guard is
  inverted.
- State-machine determinism: the outcome write is a pure function of the
  applied entry and its log index — no new `WendelonRequest` variant, no
  wall-clock input — so WAL replay under ADR 0092's boundary is unaffected.

## Risks and deferrals

- **Outcome retention is unbounded in this cut.** One small record per
  dropped table, kept indefinitely and carried in every snapshot. Accepted
  knowingly; compaction (e.g. retiring outcomes older than a snapshot
  horizon once Toron-side acknowledgement exists) is deferred and named.
- **`StorageError` gains a variant**; exhaustive matches fan out
  compile-guided, but every call site is enumerated, not a preselected
  list.
- **The frozen-v2 module is a maintenance liability by design** — its byte
  fixtures fail the moment any referenced leaf encoding moves, which is the
  0092-intended signal to fork a copy, not a defect of this cut.
- **CTAS over a still-wedged name** does not route through
  `drop_existing_table_with_storage_admin` (single caller — the DROP
  statement path); materialize retires old bindings via `ctas_sagas`.
  Whether the pre-recovery shadow is reachable is the P2 probe's question,
  with the disposition fixed in Acceptance.

## Forward

1. **Write chunking** (ADR 037 pt 10): chunked INSERT…SELECT / materialize
   preserving the 65,536-row frame bound and the u16 wire sentinel — the
   sole remaining blocker on the 200k/1m scatter-threshold verdicts.
2. **Drop-outcome compaction** once Toron-side acknowledgement exists.
3. **CTAS-path receipt integrity** — mandatory before the originating
   finding closes if the P2 probe reproduces; otherwise closed by this
   cut's evidence.
4. **Failure-layer reporting**: first-failure attribution (a transport
   error naming what is a catalog-consistency problem) remains untouched.
