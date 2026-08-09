# ADR 542 - write-path progress bounds and attributable write expiry

The ADR 0539 confirmation soak recorded silent insert-batch failures at roughly the same row
boundary that made reads fail. ADR 0541 ruled those a separate write-path finding and
deliberately did not absorb them, because one collapsed symptom had already misattributed two
mechanisms in that arc. This record takes the finding.

Grounding confirms the separation is real. INSERT routes `WendelonStorageWrite::write` →
`Client::table_write`, and that path carries none of ADR 0541's machinery: `WriteRequest` has no
deadline field, `TableWriteRequestMeta` carries neither a deadline nor a budget, and no
`QueryExecutionBudget` or `QueryClocks` is constructed anywhere along it. `DeleteRowsRequest`
has no deadline either, on its own wire path.

What the write path *does* have is better than the read path was before ADR 0540: a typed
`WriteStatus` with named constants, `PbcRejectCode` for rejects, an exactly-once cache, and a
bounded client retry loop — `DATA_RETRY_BUDGET = 3` attempts with per-cause backoffs.
Status-bearing failures already surface as `ClientError::WriteFailed` rather than collapsing
into `Io`, so this record does **not** assume ADR 0540's read-side defect repeats here.

Two properties of that exactly-once cache are load-bearing and were initially misread. It is an
`Arc<DashMap<IdempotencyKey, CachedWriteOutcome>>` — **in-process, empty at bootstrap**. It
caches durability receipts; it is not itself durable. So replay safety after a restart is not
something the cache currently provides, and any decision resting on "the idempotency cache will
absorb the retry" is resting on process lifetime.

What the path lacks is *time*. A retry budget bounds attempts, not duration: three attempts,
each server-side unbounded, is an unbounded total. A slow or wedged write has no cap, no
attributable expiry, and — as the soak observed — no surface naming why it stopped. "Three
tries" is not a bound any caller can reason about.

## Decision

1. **The caller's deadline crosses every in-scope row-mutation boundary, on ADR 0541's
   protocol rule.**
   The producers are enumerated rather than sampled: `execute_write_plan_with_source` builds a
   `WriteRequest` at four sites — INSERT, INSERT…SELECT, UPDATE, and CTAS — and production
   DELETE builds a `DeleteRowsRequest` at `coordinator.rs:2225`. (The `WriteRequest` in
   `coordinator/delete.rs` is test setup: that file's `#[cfg(test)]` begins at line 158.)

   **CTAS is excluded, deliberately and by name.** It is a catalog-plus-storage composite, so a
   durable outcome recorded on the storage side alone cannot say whether the table was created
   or replaced, whether an old table was dropped, and whether rows committed. Resolving that
   needs a durable coordinator/control-ring operation record carrying child storage identity —
   a distinct problem, with no observed failure driving it: the soak's symptom was INSERT
   batches. Half-covering CTAS would be worse than leaving it, because a bound whose expiry
   produces an unresolvable durable artefact gives the caller nothing to act on. CTAS keeps its
   current behaviour until a successor record takes it, and that gap is accepted here rather
   than discovered later.

   In scope, therefore: INSERT, INSERT…SELECT, UPDATE, and DELETE.

   The wire carries **remaining duration**, never a cross-host wall-clock timestamp, with an
   absent-sentinel stated explicitly and shared with no other field's sentinel.

2. **Retries consume the original deadline; they never mint fresh time.** This is what the
   current design gets wrong by omission: each of three attempts starts an unbounded
   server-side operation, so the retry loop multiplies exposure instead of bounding it.
   Attempts and deadline both bind, whichever expires first.

3. **Write progress is commit progress, and the unit is named before it is bounded.** Progress
   for a scan is examination; for a write it is durability advancing. Phase 0 determines and
   records the actual commit unit of a multi-row batch — whole batch, per row, or per locality —
   before any bound is designed, because retry and replay semantics both hang off it.

4. **A write outcome after expiry is one of three typed classes, and `Indeterminate` is a
   first-class answer.** A caller deadline can expire after dispatch while the server continues
   toward durable quorum, so *exact* commit knowledge is not always obtainable and a design that
   promises it would lie. The classes are **committed**, **known-not-committed**, and
   **indeterminate**. An indeterminate outcome is not a failure to report; it is the honest
   report, and it obliges the next two decisions.

   Commit knowledge is **orthogonal to expiry cause**. Every terminal outcome states both axes:
   *why it stopped* (deadline, no-progress, attempt exhaustion) and *what is durable*
   (committed, known-not-committed, indeterminate). Neither may stand in for the other — "it
   expired" does not answer "did it commit", and "indeterminate" does not answer "why did it
   stop". Collapsing the two is the same reduction ADR 0540 exists to prevent, one axis over.

5. **A named operation-identity protocol, not merely a requirement for one.** "Stable identity"
   is unimplementable until five things are fixed, so this record fixes them:

   - **Who mints it.** The *caller* (Toron) mints the operation id before first dispatch, because
     only the caller knows that attempt two is a replay of attempt one rather than a new
     statement. A server-minted id cannot distinguish them.
   - **How the caller reuses it.** The id travels in the mutation meta on every attempt of the
     same logical operation, including attempts after a caller restart, and is surfaced to the
     SQL/Rune layer so a client can present it again.
   - **Where the outcome lives.** Durably, on the storage side, keyed by that id — not the
     in-process `DashMap`, which is empty at bootstrap and therefore cannot answer any question
     posed after a restart.
   - **How resolution is queried.** A caller holding an id can ask for its outcome and receive
     `committed`, `known-not-committed`, or `indeterminate`, without re-submitting the mutation.
     Re-submission as the only resolution path is what makes duplication unavoidable today.
   - **Retention, and what reclaim means.** Durable outcomes are retained for a stated window
     and reclaimed on a stated rule; an unbounded outcome log is a second unbounded thing this
     record would be adding while removing the first. **After reclaim, replaying that operation
     id is rejected as expired — never treated as a new mutation.** Without this invariant
     bounded retention quietly reintroduces double application at the retention horizon, which
     is the exact failure the identity protocol exists to prevent, merely delayed.

6. **Abandoned server work is bounded.** A caller timeout drops its wait while local durable
   work and replica tasks may continue. Repeated expiries must not accumulate detached,
   unbounded mutation work — the same hazard ADR 0541 closed for encode by bounding the slot
   pool rather than trusting cancellation.

7. **Expiry causes stay attributable through ADR 0540's boundary.** Deadline expiry,
   no-progress expiry, and attempt exhaustion are distinct reasons end to end. This record does
   **not** pre-rule a write-side `io::Error` status extractor: write statuses already surface
   typed, and transport failures properly remain `Io`. Phase 0 determines whether any
   status-bearing `io::Error` actually reaches a caller, and only then is a downcast seam added.

## Grounding

Phase 0 answers from source, and records the answers whatever they are: the commit unit of a
batch write; what identity, if any, makes a caller replay the same operation, and where it is
durable; whether any write failure can reach a caller without a status; and what server-side
work continues after a caller stops waiting. The reproduction target is the soak's own
observation — insert batches failing silently near a few thousand rows under sampler-rate load —
reproduced before it is bounded, so the bound is shown to address the observed failure rather
than a plausible neighbour.

The write path is not the read path with different nouns. ADR 0541's `QueryClocks` may be
reusable, but only after Decision 3's progress-unit difference is stated; sharing the type
without sharing the semantics would repeat the absent-sentinel conflation that record already
paid for.

## Acceptance

1. Expiry cause and commit knowledge are **orthogonal axes**, and every outcome states both. A
   mutation exceeding the caller's deadline reports the typed deadline cause; one making no
   progress for the stall bound reports the typed no-progress cause — and each is paired
   independently with `committed`, `known-not-committed`, or `indeterminate`. One fixture per
   cause, and no fixture may let either axis erase the other: "it expired" is not an answer to
   "did it commit", and "it is indeterminate" is not an answer to "why did it stop".
2. A retry provably consumes the original deadline: a fixture drives the retry loop and requires
   the remainder measured from the first attempt's instant, never reset.
3. INSERT, INSERT…SELECT, UPDATE, and DELETE each carry the deadline, proved per path. A
   control proves CTAS is unchanged, so its exclusion is visible rather than assumed.
4. The commit unit found by Phase 0 is pinned by a test that states it.
5. All three outcome classes are reachable and distinguishable, including a fixture that yields
   `Indeterminate` rather than guessing.
6. A caller replay after an indeterminate outcome resolves to the same operation and does not
   double-apply, proved across a restart — not only within one process — and a caller can resolve
   an outcome by id *without* re-submitting the mutation. Retention reclaim is exercised, and a
   replay of a reclaimed id is rejected as expired rather than applied afresh.
7. Repeated caller expiries do not accumulate unbounded detached server work.
8. The soak's insert-batch boundary reproduces before the change and completes after it, under
   concurrent sampler-rate load.
9. Standard Wendelon, Toron, architecture, format and full `--no-fail-fast` gates pass.

## Guardrails

- Do not amend ADR 0539 or 0541; this record stands on its own evidence.
- Do not describe the in-process exactly-once cache as durable, or rest replay safety on it.
- Do not adopt attempt exhaustion as a proxy for a time bound; it is what already fails.
- Do not report an indeterminate outcome as either success or failure to satisfy a gate.
- Do not add a write-side status extractor before Phase 0 shows a status-bearing `io::Error`.
- Do not extend this record to CTAS; it is excluded by name and belongs to a successor.

## Forward

Closing this removes the last unexplained symptom from the 0539 soak, and makes
`statement_timeout` mean the same thing across scans and mutations — which is the property a
caller already believes it has.
