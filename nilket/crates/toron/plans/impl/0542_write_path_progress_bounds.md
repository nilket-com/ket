# ADR 0542 implementation receipt — 2026-08-10

ADR 0542 is implemented and code-gated. **Acceptance 8 remains pending:** the sampler-rate
insert soak that forced this record has not been rerun on this tree, so this receipt does not
claim the ADR sealed.

## Grounded commit model

The logical commit unit is the parent mutation batch. A table batch may bifurcate into several
locality groups and those children become durable independently, but visibility advances once,
through parent finalization, only after every required group reaches durable quorum. DELETE
packs tombstones and uses the same parent boundary. Child durability therefore marks progress;
it does not turn a partial batch into a committed logical mutation.

The old exactly-once `DashMap` remains an in-process receipt cache and is still empty at
bootstrap. Restart replay and resolution do not rely on it.

## Implementation

- INSERT, INSERT…SELECT, UPDATE, and DELETE propagate the original statement deadline and one
  caller-minted UUIDv7 operation identity. Retries recompute remaining duration from that same
  absolute deadline. CTAS carries neither field and remains visibly excluded.
- Wendelon races admission, local durable progress, and replica progress against a 30-second
  no-progress clock and the caller's absolute deadline. Deadline, no-progress, and attempt
  exhaustion remain distinct typed stop causes; commit knowledge is a separate axis.
- A node-wide mutation semaphore binds before secondary-index admission, batch bifurcation, or
  DELETE classification. Its permit moves into detached local work and is released only when
  that work truly ends, so timed-out callers cannot accumulate unbounded mutation work.
- Terminal outcomes live in a tenant-scoped redb ledger. Capacity (4,096) binds before age
  (five minutes); eviction first writes independently bounded expired-id evidence (4,096 rows,
  15 minutes). UUIDv7 issuance time keeps an id permanently rejectable after both ledgers have
  reclaimed it.
- Resolution is by operation identity, never by replay. Wendelon's smart client asks every
  current Data node because the admitting write locality cannot be reconstructed from a table
  descriptor; `Present` wins, `Expired` is retained, and an unreachable node prevents a false
  `Unknown`.
- Toron's public SQL/Rune client now exposes `resolve_mutation(QueryId)`. The request carries
  only the retained operation id; tenant scope comes from the authenticated connection. The
  response preserves resolution, numeric write status, stop cause, commit knowledge, and
  DELETE counts as independent fields.

## Discriminating evidence

- Deadline and no-progress fixtures pin both outcome axes independently.
- A retry fixture derives each transmitted remainder from one original deadline.
- Producer census pins INSERT, INSERT…SELECT, UPDATE, and DELETE while proving CTAS unchanged.
- Restart fixtures resolve committed and post-dispatch-indeterminate outcomes and prove replay
  does not append again.
- Capacity eviction moves the oldest terminal row into expired evidence; an aged UUIDv7 can
  never become a fresh mutation after both bounded ledgers reclaim it.
- A paused-time semaphore fixture proves a queued expiry never enters work, the true holder
  remains charged, and the permit recovers only after that holder completes.
- The public-listener fixture executes a named INSERT, resolves it over a separate SQL exchange,
  receives `Present / Committed`, and observes exactly one row. Its mock-rail control panics if
  resolution invokes `write`, proving the API does not resubmit the statement.

## Gate status

Targeted affected-crate checks, public-client tests, Toron architecture controls, the live SQL
listener resolution fixture, Wendelon's durable-outcome controls, and the existing live storage
execution target are green. Workspace `check --all-targets` completed with zero warnings and
format is clean. A final workspace `test --no-fail-fast` attempt was aborted when its terminal
pane and process exited before producing a result; this receipt does not convert that absence
into a green claim. The complete test gate remains for the seal pass.

## Outstanding live acceptance

Acceptance 8 requires a fresh-profile deployment that first reproduces the historical silent
insert-batch boundary without these bounds, then completes beyond it under concurrent
sampler-rate load with typed stop/outcome evidence. No such deployment has been authorized or
run here. Until it is, ADR 0542 is implemented and gated, not sealed.
