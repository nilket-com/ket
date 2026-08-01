# impl 535 - Global sort barrier completion

Scope: make Wendelon-backed `ORDER BY` complete through the production SQL
listener by correcting the live-shuffle liveness contract. This is an
implementation correction, not a new architecture decision: the global gather
and terminal sort already exist and are proven over fixture workers, but the
producer publication mode ignores whether its consumer can drain concurrently.
Deliverable is one `impl:` commit reviewed against the gates below.

No distributed merge-sort, sort-performance claim, planner syntax expansion,
or statement-timeout policy change rides this pair.

## Dogfood evidence (2026-08-01)

The user created `test2(cat STRING, val FLOAT32)`, inserted repeated six-row
categorical batches, and observed:

- unordered `SELECT cat, val FROM test2` completes and returns the rows;
- aggregates and `GROUP BY` complete;
- `SELECT cat, val FROM test2 ORDER BY cat` does not complete;
- the abandoned query later appears as worker connection-loss warnings rather
  than a useful SQL result.

Codex reproduced the smallest product-equivalent case against a disposable
`nilket local` profile, never the user's retained profile:

- one six-row insert (`Cat1` through `Cat6`);
- unordered select completed immediately with six rows;
- the ordered select exceeded an external 8-second bound (exit 124) while the
  server's configured statement timeout remained 60 seconds;
- cancellation identified stage 0, task 0 as the unfinished producer.

The existing process fixtures
`multiple_ordered_top_k_process_query_preserves_inner_barrier` and
`ordered_grouped_aggregate_process_digest_is_stable_across_worker_counts` both
pass. They prove the sort and fragment shape, but their fixture producer does
not create the Wendelon multi-batch pressure that exposes the liveness cycle.

## Root cause

The fragmenter lowers the global sort correctly:

1. a unary scan chain produces a one-partition, empty-key gather shuffle;
2. a single terminal stage consumes the sealed partition and runs `Sort`.

The scheduler also correctly recognizes that `Sort` is not a streamable unary
consumer and waits for the producer's sealed manifest. The contradiction is at
the producer boundary: `is_streamable_exchange_producer` consults only the
producer's own scan/filter/project/take shape, so the worker opens a paced live
partition anyway. Once more than the live chunk depth has been appended, the
producer waits for a reader to release permits. The reader cannot start until
the producer seals. Neither side can advance.

This is the streamability seam already named by ADR 529 and ADR 531: live versus
sealed publication must consult downstream drain eligibility. It is not a sort
kernel failure and must not be fixed by enlarging or disabling the live buffer.

## Contract

### 1. Publication mode is explicit

Represent shuffle publication as an explicit value carried by the dispatched
task, for example `ShufflePublishMode::{Live, Sealed}`. Exact naming may follow
the surrounding task vocabulary, but these semantics are fixed:

- `Live` means a downstream consumer is eligible to start and drain while the
  producer is running;
- `Sealed` means the producer materializes through the existing bounded
  non-live publish path and the consumer starts only from the accepted manifest;
- deserializing a task that lacks the new field defaults to `Sealed`, never
  `Live` — absence of a liveness proof fails safe.

Producer shape remains an input to the decision, not the decision itself. The
worker must not independently infer live publication from the producer plan
after the coordinator has resolved the graph.

### 2. Resolve mode from the complete stage graph

One pure coordinator/fragment-graph seam assigns publication mode before a task
is dispatched. A producer may be `Live` only when its existing producer-shape
predicate passes **and every immediate shuffle-consumer path for its published
partitions is eligible to dispatch and drain concurrently under the scheduler's
actual rules**. Any non-streamable barrier consumer makes that producer
`Sealed`; the global-sort gather is the headline case.

Do not encode `Sort` as a one-off exception. Joins, aggregates, windows,
repeats, multiple consumers, and future barriers must receive the same answer
from the shared graph predicate. A conservative `Sealed` answer is correct;
an unjustified `Live` answer is a liveness bug.

### 3. A typed liveness backstop remains

Validate the resolved graph before the first task dispatch. A `Live` producer
feeding a consumer that cannot drain concurrently returns a dedicated
`FragmentError` naming the producer stage and blocking consumer stage. Preserve
that typed origin through `QueryError` rather than erasing it into
`QueryError::Internal`, then map it to `SqlErrorCode::PlanningError` with a
concise capability message before any worker task starts. No string matching
may recover the type at the SQL boundary. Existing exhaustive fragment-error
classifiers must gain the new arm by compile force.

This is the immediate execute-or-refuse obligation made permanent: the fixed
sort graph selects `Sealed` and executes, while any future unresolved unsafe
graph refuses synchronously instead of waiting for statement timeout. Do not
globally reject `ORDER BY`; do not translate a statement timeout or client
disconnect into a fake capability refusal.

### 4. Existing live paths stay live

The current scan/filter/project/take producer feeding a genuinely streamable
single-upstream consumer must retain early dispatch and bounded live pacing.
Multi-upstream live eligibility, broadcast exclusions, attempt identity,
cancellation, retry, manifest acceptance, and partition-store accounting are
unchanged except where they consume the single resolved publication-mode seam.

## Acceptance

1. **Product-path regression:** extend the real SQL-listener fixture that owns a
   live `WendelonNode`, real `WendelonStorageScan`, Toron daemon, worker transport,
   and public Toron client. Write more separately committed row groups than the
   live partition chunk depth, then execute the user's shape:

   ```sql
   SELECT cat, val FROM test2 ORDER BY cat;
   ```

   It completes with all rows in value-discriminating order. The fixture must
   exercise the public SQL wire and must not use `toron_fixture_worker` or
   `InMemoryStorageScan` as a substitute for the real producer path. Injecting
   a client into the daemon test constructor is acceptable only when that
   client reads from the live Wendelon node and produces the real multi-batch
   table-query stream; a transport stub is not.
2. **Pressure is proven:** the regression fixture records enough producer
   batches/chunks to prove the pre-fix execution would exceed the paced live
   depth. An elapsed-time-only test is insufficient.
3. **Sort variants:** ascending and descending order are proven; one case also
   covers `LIMIT`/`OFFSET` or a projection above the sort so the existing
   global-sort tail remains intact. Duplicate keys must not lose rows.
4. **Resolved mode:** fragment/scheduler tests show the gather producer below a
   global sort is `Sealed`, while a producer below a streamable consumer remains
   `Live` and dispatches before producer completion.
5. **Backstop with positive control:** one synthetic unsafe graph is rejected by
   the typed pre-dispatch validator, and the adjacent corrected graph is
   admitted. A work log proves zero dispatches occurred on refusal.
6. **Wire compatibility:** a serialized legacy task without the new mode decodes
   as `Sealed`; round trips preserve both explicit variants.
7. **No timeout laundering:** the successful SQL fixture records no statement
   timeout/cancellation outcome for the query. A separately forced real timeout
   retains the existing `StatementTimeout` behavior and is not reclassified.
8. The established ordered process fixtures remain green, followed by the full
   Toron, Wendelon, architecture, and workspace gates.

## Guardrails

- No buffer-depth increase, unbounded queue, permit bypass, or eager consumer
  dispatch through a non-streamable operator. Those approaches hide the cycle
  or move the memory bound.
- No local-executor fallback for `ORDER BY`. The production distributed path is
  the behavior under test.
- No sort rewrite, spill-policy change, or parallel sort. Sealed publication may
  cost latency; this pair restores correctness and liveness only.
- No fixture-only proof. The old fixtures already pass and are the reason this
  escaped.
- Statement timeout remains a last-resort operational bound, not an admission
  mechanism for a statically knowable liveness error.

## Namespace guard chore

Extend the architecture guard family for the post-census TORON namespace
(numeric identifiers 528 and newer): when both
`plans/adr/NNN_slug.md` and the canonical `plans/impl/NNN_slug.md` exist, their
slugs must match exactly. Implementation-only identifiers remain valid.
Historical result/evidence companions and pre-528 naming remain untouched; the
guard must not rewrite immutable records into modern convention. Include a
synthetic mismatched pair as the positive control, and cite this plan in the
failure message.

## Review checklist

- [ ] Publication mode is explicit, safe-defaulted, and resolved once from the
      complete stage graph.
- [ ] Global-sort producers publish sealed; genuinely drainable paths remain
      live.
- [ ] The typed pre-dispatch backstop fires with zero worker dispatches and has
      a positive control.
- [ ] A live-Wendelon, public-SQL-wire test crosses the pacing depth and returns
      correctly ordered, row-complete results.
- [ ] Timeouts and client disconnects retain their honest existing classes.
- [ ] No capacity, sort-policy, local fallback, or test-fixture shortcut rides
      the fix.
- [ ] The same-number/same-slug guard is scoped to the clean post-528 namespace
      and proves it can fail.
- [ ] Full validation is green from the final tree.
