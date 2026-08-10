# impl 0543 — shuffle namespaces have a driven, bounded lifecycle

Implementation receipt for `plans/adr/0543_shuffle_namespace_lifecycle.md`. This cut is
lifecycle correctness, not an optimization authorized by ADR 0538's timing branches. The
measured defect was a long-lived worker retaining one namespace per query indefinitely;
production also retained cancellation tombstones forever and could strand encrypted spill
files across restart.

## Grounding corrections

Phase 0 corrected the plan before code was allowed to choose policy:

- Cancellation was already driven from six production worker-server sites; successful query
  completion had no corresponding store lifecycle. The shipped precursor repair
  (`d7960883`) separately fixed cancellation of query A closing every unsealed live partition
  belonging to other queries.
- `cancelled_queries` was both a tombstone and the write gate. Merely bounding it would have
  let a delayed task recreate an old namespace. This implementation replaces that negative
  authorization with a positive, expiring query lease on every namespace-creating operation.
- Worker-loss expiry accepted a caller-supplied duration but production owned no default. The
  worker-registry boundary now owns `DEFAULT_WORKER_LOST_AFTER = 30 s`; the lifecycle derives
  its term from that same value and checks `lease_term > lost_after + renew_cadence`.
- A lease proves authorization, not that a namespace ever existed. Only a committed namespace
  licenses a terminal receipt and the later `Reclaimed` answer.
- The encrypted spill store generated a fresh key while reusing its directory. Files from a
  prior incarnation were therefore certainly unreadable, not merely suspect.

## Protocol and coordinator lifecycle

Protocol version 2 appends four typed exchanges: `QueryLease` / `QueryLeaseAccepted` and
`RetireQuery` / `QueryRetired`, all carrying `EnvelopeIdentity::Query`. The coordinator sends
the first lease before `StartTask`, keeps a monotonic per-query worker census (including a
worker that only received a cancelled task), and renews every worker in that census for the
whole `ActiveQuery` lifetime. A renewal failure stops dispatch and remains attributable as
`WorkerLost { worker_id }`; it does not borrow the cancellation outcome.

Success, cancellation and terminal failure each issue a typed retirement after the terminal
response path. Delivery is idempotent and retries unacknowledged workers until the query lease
expires. The worker's one-second reaper supplies the lost-coordinator path: an unrenewed lease
is abandonment evidence, not an age-based guess.

Two transport repairs precede the plan rather than hiding inside this implementation. Selecting
a periodic maintenance tick against `QuicEndpoint::accept_connection()` can cancel the combined
dequeue-and-handshake future after it has consumed an incoming connection. The preceding
`fix: keep admitted QUIC handshakes outside maintenance races` commit splits
dequeue from handshake; once a peer is admitted, only shutdown can cancel it. The named control
`admitted_quic_worker_handshake_survives_maintenance_ticks` holds an already-dequeued peer across
several forced ticks. Reintroducing the tick into the handshake race fails it with
`ConnectionRefused`.

TCP exposed the corresponding terminal boundary in the 21-cell atlas: a peer closing after a
complete short acknowledgement may surface `ConnectionReset`. The preceding
`fix: classify terminal worker TCP resets by exchange phase` commit accepts it only in
`BetweenCompletedExchanges`; the same error before a terminal response remains fatal. The named
control `tcp_reset_is_clean_only_after_a_completed_terminal_exchange` injects both phases and
fails with `Err(Io(ConnectionReset))` when the completed-exchange arm is removed. Both repair
commits compile and pass independently before the ADR 0543 plan.

## Store lifecycle

`ShuffleNamespaceLifecycle` is shared by the in-memory, encrypted-spill and hybrid stores.
Production construction is managed and lease-enforcing; the explicit unmanaged constructor is
limited to direct substrate fixtures. A successful retirement closes new pulls, drains readers
already admitted, then removes state. Cancellation and failure terminate pulls immediately
with their own reasons. Superseded attempts remain until query-terminal retirement.

Receipts are capacity-bounded first (4,096 per worker, oldest first) and horizon-trimmed second
(15 minutes); each eviction path has a distinct counter. A live receipt is the sole authority
for `Reclaimed`. Once it is gone, or when no namespace was ever committed, the worker returns
the loud `NamespaceUnknown`.

Physical deletion failures remain cleanup debt and are retried. Spill files carry a
per-incarnation prefix; startup removes only provably owned files and completes before writes
are accepted. Failure to remove an owned orphan fails startup. The hybrid store has one shared
lifecycle and records one receipt only after both backends finish, preventing split residency.

## Observable boundedness

The metrics surface is aggregate and brand-clean: resident namespaces and bytes, physical
spill files, cleanup debt, receipt and tombstone counts, live records, oldest namespace age,
reclamations by the four terminal causes, receipt evictions by capacity or horizon, and startup
swept files. It carries no query, task, worker or tenant labels.

The controls cover the two baselines separately: payloads, files, accounting, tombstones and
live records return immediately after quiescence; receipts remain bounded and return after
their evidence horizon. Additional controls pin mid-pull success draining, prompt typed
cancellation/failure, long-running renewal, abandonment, stale-attempt retention, receipt
evidence, cleanup debt, restart sweep/fail-closed behavior, composite coherence, and the
coordinator census/retry protocol.

## Gate receipt

- `cargo test --workspace --no-fail-fast --quiet`: **exit 0**, no failed target.
- `cargo check --workspace --all-targets`: **exit 0**, zero warnings.
- Toron architecture: **2,109 / 0**.
- Toron library: **1,514 / 0**.
- TLS listener: **10 / 0**; observability listener: **36 / 0**.
- ADR 0538 full 21-cell timing-atlas driver: **1 / 0**, journal replay clean.
- `cargo fmt --all --check` and `git diff --check`: clean.

No live-deployment performance claim is made. The implementation establishes bounded lifecycle
semantics and makes residency externally measurable; future timing work can now run without
the standing cross-query accumulation confound.
