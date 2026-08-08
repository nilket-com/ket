# ADR 541 - streaming-scan progress bounds

Successor to ADR 118. That ADR delivered the stream-shaped table-query boundary — Toron
consumes `StreamingQueryResult` lazily, Wendelon emits terminal status metadata, cancellation
is wired — and it is live in the code path today. What it did not touch is the server's
execution budget: `QueryExecutionBudget` applies a fixed `max_wall_time` (250 ms default) as a
*lifetime* limit across planning and response encoding, so a legitimate, continuously
progressing full-table scan dies at a few thousand text rows. The ADR 0539 confirmation run
proved the consequence on a healthy profile: tables became unreadable through the SQL path at
roughly the fifth sampler scrape, borderline tables flapped with concurrent load, and the
dogfood viewport lost exactly the tables it exists to serve. A wall clock that cannot tell "no
progress" from "steady progress through a large table" is not a budget; it is a table-size
cap denominated in milliseconds.

## Decision

1. **Two clocks, separately owned, enforced concurrently.** They are different quantities and
   are never reduced to "the tighter of" two comparable durations:

   - *Wendelon's no-progress timeout* is sliding: it resets on each unit of progress (a batch
     encoded, a location chunk scanned) and expires only when a scan makes no progress for the
     bound's duration. A progressing full-table scan can no longer die by lifetime; a stalled
     one still dies.
   - *The caller's end-to-end deadline* is absolute and non-resetting: Toron owns it,
     propagates it in the query metadata, and Wendelon enforces it as a hard instant
     regardless of progress. Only the caller knows how long the whole query may take, because
     only the caller knows the query. Absent a caller deadline, there is no total wall-clock
     cap — totality is bounded by the caller budget or not at all.

     Protocol condition: the wire carries **remaining duration**, never a cross-host
     wall-clock timestamp. Toron computes the remainder from its original absolute deadline
     at each transmission; Wendelon derives a local monotonic deadline on receipt. Retries
     consume the original deadline — a retry's remainder is measured from the original
     instant, so retrying can never mint fresh time.

2. **No pagination.** The ADR 118 streaming contract stays as the one shape for large results;
   this ADR fixes the budget semantics inside it rather than adding a second result-transfer
   mechanism with its own consistency questions.

3. **`max_locations` and `max_rows` stop being total caps.** Today they are hard-coded
   Wendelon totals — leaving them "unchanged" would merely swap a milliseconds-denominated
   table-size ceiling for a row-denominated one. Under the streaming design they become
   bounded work-per-progress/yield limits: how much the server does between progress points,
   not how much a query may touch in total. Only a propagated caller budget may impose a
   total query cap.

4. **Every expiry is attributable.** No-progress timeout, caller-deadline expiry, and
   caller-budget exhaustion are distinct failure reasons, each surfacing through ADR 540's
   typed status boundary. Collapsing them into one indistinguishable failure is exactly the
   defect class this arc keeps finding, and it is unrepresentable here by construction.

## Grounding

Phase 0 maps every consumer of `QueryExecutionBudget.max_wall_time` — plan preparation, the
per-location loop, and `encode_query_stream_batches_with_deadline` — and establishes where
progress is observable on each. The confirmation profile's evidence pins the reproduction: a
table crossing a few thousand rows under concurrent sampler load, `query_budget_exceeded` in
the audit, ~250 ms server time per failure. The discriminating fixture drives a full-table
scan an order of magnitude past today's failure point under concurrent ingest and requires
completion; its sibling stalls a scan artificially and requires the typed budget failure.

The run also recorded silent insert-batch failures near the same boundary; Phase 0 determines
whether those share this clock or are a separate write-path finding, and names the answer
rather than absorbing it — one collapsed symptom has already misattributed two mechanisms in
this arc.

## Acceptance

1. A full-table scan at least ten times today's failure size completes under concurrent
   sampler-rate ingest, on the deployment profile.
2. An artificially stalled scan fails with the typed no-progress status within the bound.
3. A caller deadline tighter than server policy is honored; a caller deadline looser than the
   stall bound does not resurrect lifetime-style failures.
4. Each expiry cause is discriminable in a test: a stalled scan yields the no-progress
   status; an expired caller deadline yields the deadline status; an exhausted caller budget
   yields the budget status. One fixture per cause; no shared assertion.
5. The ADR 0539 deployment confirmation rerun passes under a tightened gate: create the
   secondary index explicitly; **wait until its gap-gauge series exists before the measured
   window opens — a missing series is a failed run, not an explanatory result**; sustain both
   ingest and indexed lookups through the window; span at least one induced slab rotation
   across 60-second scrapes; and record the gap wall-time fraction, the refusal-counter
   delta, and the gauge-versus-uncovered divergence. ADR 0539's own text stays immutable; its
   Acceptance 6 is met by this rerun or not at all.
6. Standard Wendelon, Toron, architecture, format and full `--no-fail-fast` gates pass.

## Guardrails

- Do not raise the 250 ms constant as the fix, alone or alongside; the defect is the clock's
  semantics, and a bigger number is a bigger table-size cap.
- Do not add pagination, offsets, or a second transfer path.
- Do not let progress-reset units be so coarse (whole-result) that the bound degenerates back
  into a lifetime, or so fine (per row) that a pathological trickle never expires.

## Forward

On success the dogfood viewport's tables stop having a size ceiling, and the ADR 0539
confirmation rerun becomes schedulable. The open question from the confirmation profile — no
secondary index was ever created for the sampler's tables, so what created the old profile's
indexes — is carried by the rerun protocol, which creates one explicitly rather than assuming
the write path does.
