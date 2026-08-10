# ADR 543 - shuffle namespace lifecycle and reclamation

ADR 0538 recorded, without fixing, that shuffle state is never reclaimed in production. This
record takes it as **lifecycle correctness**, not substrate optimization: the atlas that found it
closed with a no-work verdict, and nothing here is authorized by its performance branches. A
worker that grows without bound is wrong whether or not it is slow.

## What is actually there

An earlier draft of this record claimed neither query-scoped hook had a production caller. That
was false, and the correction matters because it changes what is missing.

- **`cancel_query` is driven in production — 6 direct call sites**, all in the worker server
  (`server/mod.rs` 807, 876, 941, 960, 1038, 1079), from `StartTask` cancellation and
  `CancelTask` handling. (An earlier count of 11 was wrong: it swept in scheduler methods of the
  same name, tests, trait impls and `Hybrid` forwarding.)
- **`remove_query` has no production caller.** Every call site is `HybridPartitionStore`
  forwarding to its halves, or a test.

So the gap is not "no lifecycle at all". It is that **cancellation is wired and terminal success
is not**, and that the wired path itself does not fully reclaim:

- `cancel_query` inserts into `cancelled_queries` and **nothing ever removes from that set** — an
  unbounded tombstone accumulation, already in production, on the path that *is* driven.
- **It cancels other queries' live partitions.** The three `retain` calls filter correctly on
  `query_id`, but the live-partition loop iterates `live_partitions.values_mut()` with no such
  filter, so cancelling one query marks *every unsealed live partition on the worker* cancelled,
  wakes its waiters and closes its pacing semaphore. `values_mut()` yields no address, so there
  was nothing in scope to filter on. Consumers of an unrelated, healthy query receive
  `CancelledTaskCleanup` for partitions nobody cancelled. This is a live cross-query correctness
  defect, not a leak, and it is worth repairing ahead of the lifecycle work rather than waiting
  for it.
- `EncryptedSpillPartitionStore::remove_query` deletes spill files with
  `let _ = std::fs::remove_file(path);` — **deletion failures are discarded silently**, so bytes
  can remain on disk with nothing recording that they did.

ADR 0538 measured the consequence at sf=0.01: 0 retained bytes at the post-setup baseline,
63,432 after a single child, no reclamation at completion.

## Decision

1. **Retirement is a coordinator-issued protocol operation, and its parts are decided here.**
   A worker observes *task* completion; only the coordinator knows a query is terminal, and the
   wire carries no retirement operation today.

   - **Census.** The coordinator keeps, per query, the set of workers it has ever addressed for
     that query. The set only grows — it is never pruned while the query lives — and it
     **includes workers that only ever received a cancelled task**, because such a worker may
     hold tombstones and live records without having published output.
   - **Terminal response does not wait for cleanup.** Coupling client-visible completion to
     reclamation would trade user-facing latency for a memory property. Retirement is issued
     after the terminal response.
   - **An acknowledgement means retired, not removed.** The worker asserts it has closed the
     namespace to new readers; draining and removal proceed afterwards. Removal is reported by
     telemetry, not by the ack.
   - **Unacknowledged retirement is coordinator-side state**, retained per query until acked or
     until the query's lease (Decision 5) expires.
   - **Redelivery is idempotent and bounded**: retire is safe to receive repeatedly, is
     redelivered on a fixed schedule until acked, and stops at lease expiry — after which the
     worker's own abandonment path reclaims.

2. **Reclamation is two-phase — retire, drain, remove — and draining is scoped by terminal
   cause.** Retirement closes a namespace to new readers; draining waits for pulls already
   admitted; removal frees bytes. Collapsing these into "terminal means delete" races a consumer
   that was legitimately mid-pull.

   Draining applies to **successful** retirement. **Cancellation and terminal failure keep their
   existing behaviour**: they terminate live pulls immediately with their typed cause, because
   waiting politely for readers of a cancelled query is the opposite of cancelling it. Nor may
   the three collapse into a generic `reclaimed` outcome — that would erase the attribution a
   consumer needs to tell "your query was cancelled" from "your query finished and you were
   late".

3. **Superseded-attempt state is retained until query retirement; no attempt-scoped
   reclamation is added.** A replacement attempt keeps the **same `QueryId`** and only advances
   `AttemptId` (`task_state.rs`: `let replacement_attempt = attempt_id.next();`), so retiring a
   query namespace on supersession would delete the replacement's state. But the converse is
   equally true: a replacement may run on a *different* worker, so the old worker cannot infer
   that it was superseded, and `PartitionMissingReason::AttemptStale` is a **reason a pull
   fails, not a trigger to reclaim**. `CancelTask` cannot be borrowed for this either, because it
   invokes query-wide cancellation.

   This cut therefore holds stale attempt state until query retirement rather than inventing an
   attempt-retirement operation keyed by query/stage/task/attempt. The cost is bounded by query
   lifetime and is stated rather than hidden; a scoped attempt-retirement operation is named as
   deferred, to be taken only if measurement shows stale attempts dominate residency.

4. **A capacity-bounded terminal receipt distinguishes "reclaimed" from "never existed", and
   the loud answer is the fallback.** After removal the namespace is gone, so the distinction is
   unprovable unless the worker keeps a small record that it once existed and was reclaimed.

   A time horizon **alone is not a bound** — arbitrary query churn fills any window. The ledger
   is therefore bounded by **capacity first**: a fixed maximum number of receipts per worker,
   evicting oldest-first, with a retention horizon as a secondary trim. Both the cap and the
   horizon are stated constants, and the ledger's size is published.

   **A live receipt is the only thing that licenses `Reclaimed`.** Once a receipt is evicted the
   worker knows only that it holds no such namespace, which is not evidence that the namespace
   never existed — so the reason reported is a loud **`NamespaceUnknown`**, not a claim about
   history the worker cannot support. `Reclaimed` is the benign, attributable answer and is only
   ever given on receipt evidence. Erring loud is correct: the alternative would let a genuine
   addressing defect borrow the benign answer.

   **Stated constants.** The ledger holds at most **4,096 receipts per worker** with a
   **15-minute** retention horizon, both typed configuration with those pinned defaults — not
   implementation choices, or the implementation silently picks the evidence horizon. Capacity
   binds first and evicts oldest, the horizon trims what capacity has not; both evictions are
   counted separately so churn-driven loss of evidence is visible rather than inferred.

   An unbounded receipt set is not a fix; it is the leak wearing a different name — which is
   precisely what `cancelled_queries` is today, and that set is part of what this record bounds.

   **Bounding the tombstone requires replacing what it currently protects.** `cancelled_queries`
   is not only a record; it is a *write gate* — `publish` rejects with `CancelledQuery` while the
   entry is present. Letting it return to baseline while receipts later evict would let a
   delayed task from a cancelled or completed query recreate the namespace, which is stale
   resurrection. So authorization inverts from negative to positive: **every
   namespace-creating operation requires a live query lease** (Decision 5), issued before first
   task dispatch. A missing or expired lease rejects the write — not only the pull. The tombstone
   can then be bounded because it is no longer the thing keeping stale writers out.

5. **The backstop is an expiring query lease, and its mechanism is decided here.** A raw TTL
   cannot distinguish a lost coordinator from a legitimately long-running query, and would
   reclaim live state from the latter.

   - **The lease needs its own carrier; the existing heartbeat cannot do it.**
     `HeartbeatPayload` carries only `worker_id` and `lease_epoch` — nothing query-scoped — so
     this record adds a coordinator→worker query-lease issue/renew operation, with a stated
     cadence and term. The **coordinator issues a lease** with the query's first dispatch to each
     worker in the census and renews it on that cadence while the query is live. A long-running
     query keeps its state indefinitely, because renewal — not age — is the evidence.
   - **The term is derived from worker-loss detection, not configured beside it.** Renewal
     cadence defaults to **5 s**, and the term must satisfy
     `lease_term > lost_after + renew_cadence`. Phase 0 corrected an assumption in the draft:
     `scheduler.rs` and `worker_registry.rs` expose `lost_after` as a caller-supplied expiry
     parameter, but production had no owner for a default window. This cut establishes the
     **30 s** production default at the worker-registry boundary and derives the shuffle lease
     from that same value; the store does not invent a parallel clock. If a lease could expire
     faster than failover classifies a worker, a network partition would reclaim state the
     coordinator's recovery still needs. The equation is checked at construction so a
     configuration that violates it fails loudly rather than silently shortening the safety
     margin.
   - **Expiry is the abandonment proof**: a lease that goes unrenewed past its term is evidence
     the owner is gone, and the worker reclaims with a distinct `abandoned` cause.
   - **A restarted coordinator does not adopt orphaned queries.** Their leases simply go
     unrenewed and expire, which is the intended path rather than an edge case.
   - Reclamation under abandonment is identical in mechanism to retirement — retire, drain,
     remove — and differs only in what proved the namespace was reclaimable.

   The lease is recovery from a lost signal, never the correctness trigger, and it must never
   fire on an ordinary terminal path.

6. **Cleanup debt is retained and retried, never discarded.** Physical deletion can fail. A
   reclamation that drops the error reports success while bytes remain, which makes the boundedness
   claim unfalsifiable. Failed deletions are retained as debt, retried, and counted.

7. **"Returned to baseline" means every retained surface.** Budget-accounted bytes alone are not
   the measure. The measured surface is: resident namespaces, retained bytes, **physical spill
   files**, **lifecycle receipts**, **cancelled-query tombstones**, and **live-partition
   records**. Telemetry publishes namespace count, retained bytes, oldest resident namespace age,
   receipt-ledger size, and reclamation outcomes attributed by cause. The four causes are the
   query-terminal states this cut actually implements: quiesced success, cancellation, terminal
   failure and abandonment. Superseded attempts deliberately have no reclamation cause because
   Decision 3 retains them until query-terminal retirement. Today only per-task span fields exist
   (`pulled/left/right/published_shuffle_bytes`) and no store-level instrument at all, so
   boundedness is currently unfalsifiable from outside the process.

8. **Worker restart sweeps orphaned spill state at startup.** `EncryptedSpillPartitionStore`
   generates a **fresh encryption key** on construction and resets spill ids, while
   `create_dir_all` quietly reuses the existing directory. Every file written by the previous
   incarnation is therefore undecryptable, untracked, and permanently resident — a leak that
   *survives* restarts, unlike the in-memory one. In-flight cleanup debt is lost with the same
   event.

   Because a fresh key makes prior files unreadable by construction, they are garbage with
   certainty rather than probability: startup sweeps them before serving, and counts what it
   removed. This is preferred to durable debt recovery, which would add a persistence format to
   track files that can no longer be read anyway.

   The sweep is **fail-closed and ownership-scoped**, because spill ids restart at one and a
   stale file could otherwise collide with a freshly minted name:

   - Files are written under a **per-incarnation prefix**, so ownership is decidable from the
     name alone rather than by attempting to read a file this store cannot decrypt.
   - The sweep removes only files this store provably owns — never unrelated paths that happen
     to share the directory.
   - **Sweeping completes before the store accepts any write.** A partial sweep racing new
     writes is how a stale name collides with a new one.
   - **An owned file that cannot be deleted blocks startup.** Serving while known-orphaned bytes
     remain would make the boundedness claim false from the first moment, and a worker that
     cannot clean its own spill directory has a problem that hiding will not fix.

9. **One lifecycle, shared by every implementation.** `InMemoryPartitionStore`,
   `EncryptedSpillPartitionStore` and `HybridPartitionStore` must not each carry the state
   machine. The composite forwards to two halves that could otherwise retire, drain and remove on
   different schedules, leaving a namespace gone from one backend and resident in the other.

## Grounding

Phase 0 establishes from source and records the answers: which task states can still touch a
namespace once the coordinator considers a query terminal; whether a consumer can begin a pull
after its producer sealed; what the worker learns, if anything, when a coordinator disappears;
exactly which structures `cancel_query` leaves behind today; and whether the two backends behind
`HybridPartitionStore` can currently diverge in residency. It also records that worker heartbeat
expiry had a typed API but no production-owned default; Decision 5 supplies one shared source
rather than pretending a pre-existing configuration existed. The reproduction is ADR 0538's
measurement, re-run before the change so the baseline belongs to this record.

## Acceptance

1. **In-flight reader control, on the success path.** A consumer mid-pull on a namespace whose
   query completed **successfully** is not cut off, and removal does not occur until that pull
   completes. Fails if retirement is collapsed into immediate deletion. Paired with its
   opposite: on cancellation and on terminal failure, live pulls terminate promptly with their
   own typed cause, and a control proves those causes are not flattened into `reclaimed`.
2. **Long-lived worker returns to baseline, checked at two separate points** across repeated
   completed, cancelled and failed queries — measured on every surface in Decision 7, not budget
   bytes alone, and returning *to* baseline rather than below a threshold:
   - **immediately after quiescence**: payloads, live-partition records, spill files, budget
     accounting and cancelled-query tombstones;
   - **after the receipt horizon**: the receipt ledger, which is expected to be non-empty before
     then and must stay within its stated cap throughout.
3. **Cross-query isolation.** Cancel query A while query B holds an unsealed live partition, and
   prove B's partition is untouched — not cancelled, its waiters not woken, its pacing semaphore
   not closed — and that B completes normally. This control fails against today's code.
4. Each terminal path — success, cancellation, failure — reaches retirement independently. One
   fixture per path, no shared assertion.
5. **A superseded attempt does not retire its query, and does not reclaim early.** A control
   drives an attempt replacement and proves the replacement's state survives. It also proves the
   stale attempt's partitions **remain resident until query-terminal retirement** and are
   reclaimed then — per Decision 3, which rejects attempt-scoped reclamation because the old
   worker cannot learn that a replacement ran elsewhere.
6. A late pull reports `Reclaimed` **only** while a receipt licenses it; after eviction or
   expiry it reports the loud `NamespaceUnknown`, and a pull for an address that was never
   published reports the same loud reason. A control proves `Reclaimed` is never given without
   receipt evidence.
7. The receipt ledger and the cancelled-query tombstone set are both proved bounded under
   sustained query churn.
8. The abandonment backstop reclaims state stranded by simulated coordinator loss, **and** a
   control proves a legitimately long-running query survives well beyond the nominal cleanup age.
9. A cleanup failure is retained as debt, retried, and visible in telemetry; a control injects a
   deletion failure and proves reclamation does not report success.
10. **Worker restart with spilled state.** A worker restarted while spill files exist sweeps
   them at startup and reports the count; a control proves no undecryptable file survives, and
   that the sweep is bounded rather than a scan of unrelated paths.
11. All three implementations share the lifecycle; a control proves the composite's backends
   cannot diverge in residency for the same namespace.
12. Standard Toron, Wendelon, architecture, format and full `--no-fail-fast` gates pass.

## Guardrails

- Do not present this as atlas-authorized; ADR 0538's verdict was no-work.
- Do not delete on terminal. Terminal starts retirement; quiescence permits removal.
- Do not retire a query because an attempt was superseded.
- Do not let the backstop become the trigger, or an age stand in for abandonment.
- Do not replace an unbounded namespace set with an unbounded receipt set.
- Do not discard a cleanup error.
- Do not reimplement the state machine per backend.
- Do not let a query's cancellation touch another query's partitions.
- Do not report `Reclaimed` without a live receipt, or claim a namespace never existed.
- Do not let the query-lease term be shorter than `lost_after + renew_cadence`.
- Do not bound the tombstone without first gating writes on a live lease.
- Do not sweep a file this store cannot prove it owns, and do not serve writes mid-sweep.
- Do not treat a retention horizon as a capacity bound.

## Forward

A worker that returns to baseline makes long-running deployments honest, and removes a standing
confound from future timing work: the atlas had to hold workers resident across sixteen children
and isolate repetitions itself, precisely because reclamation did not exist. ADR 529's timing
follow-up and ADR 537's fragmented-plan fingerprint coverage sit behind this.
