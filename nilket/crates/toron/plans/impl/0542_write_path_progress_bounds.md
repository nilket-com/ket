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

## Deterministic acceptance 8 fixture plan

Status: selected plan; acceptance remains OPEN until paired executed evidence and gates pass.

Base: published C-S3b c059c3fb8e48443c2699fde0751257c10c1d25eb.
Historical parent P: cb7fdaf4d3d0e9c75dbc366b2d704a93011d5af8.
Historical implementation I: 0987e0f59f42cc0b40963e31d676bd4b1889b53c.
The landed fixture also runs on the current implementation descendant.

### Evidence and scope

The prior soak attempts remain unchanged: ac1cda18 had a parser defect,
6e04f0f5 missed the event floor, and 6df8c9a4 met the floor with no failure
signature. None discharges acceptance 8. The deterministic trigger replaces
hoping that a soak reaches an unbounded server write; it does not assert that
the old row boundary's cause has been identified.

ADR 0539 supports a separate compaction/reference-integrity hypothesis.
Ordinary row records are indexed directly during slab recovery; F-CTR-6's
counter tentative/marker split is not the same mechanism. Rotation and
compaction retirement therefore remain a separate paired boundary fixture.
Any failure at P is reported as a new finding, not relabeled as acceptance 8.

### Shared test seam

Use the existing AsyncFileOps interface and RecordingFileOps-style gate.
Add only a feature-gated SlabManager constructor accepting caller-supplied
AsyncFileOps, reachable from Toron through Wendelon's non-default test-support
feature. Apply exactly the same seam patch to P and I. Builds without that
feature retain their production construction path. Reuse existing guards for
test-support seams where applicable. A production-visible change beyond this
constructor is a STOP requiring review.

The injection holds fdatasync after append and before durability acknowledgement.
Record gate entry before sampling outcomes; do not infer commit knowledge from
the presence of appended bytes. Release for teardown is recorded separately
from the observation. Detached work is not claimed cleaned up merely because a
caller returned or the harness cutoff elapsed.

### Known duplication

The `test-support` branch in node bootstrap repeats the `SlabManagerConfig`
literal to preserve the default construction path. Future configuration fields
must be added to both literals; compiling the feature branch checks that they
remain complete.

### Paired observations

1. No injection: exact batch count and successful reads on P and I.
2. Persistent hold at P: the batch has no response at the explicit harness
   cutoff, no typed cause or commit-knowledge result, and no exhausted retry
   budget. Record cutoff/ambiguous, never pass or silent success.
3. Transient hold at I, released before expiry: committed batch with exact count.
4. Persistent hold at I: typed deadline or no-progress cause within the
   predeclared deadline plus epsilon, with the independently justified
   commit-knowledge class for post-dispatch/pre-fsync injection.
5. Force slab rotation and a compaction retirement during INSERT batches while
   concurrent COUNT(*) reads run. Execute on both P and I. At I require exact
   final counts and consistent reads; at P retain the observed outcome,
   reporting any failure separately.

Before execution, freeze fixture inputs, gate-entry proof, clock bounds,
cutoff/epsilon, exact expected typed fields, boundary-event receipts and teardown
semantics. The seam and shared inputs are byte-identical across the pair;
revision-specific outcome assertions are explicit.

### Execution and publication

Use isolated worktrees and fresh dedicated target directories for P and I.
Retain successful compiler command/artifact provenance, Compiling lines,
executable SHA, actual --list names, raw logs and source-before/after hashes.
Never reuse a base executable as the subject. The P failing observation is
external evidence, not a failing test landed on main.

Sequence: selected census and review -> this plan commit -> test source ->
pre-seal check/format/guards -> unsigned review -> signed SOURCE -> standard
gates selected for the actual changed files -> FINAL and joint FF. No full
ladder is presumed necessary for a test-file-only change; any seam/build-input
change is disclosed for gate selection. No runtime deployment or soak is granted.

Commit subjects:
- plan: adr 0542 deterministic acceptance 8
- test: adr 0542 deterministic acceptance 8

After executed acceptance and publication, update the ADR status in the later
status-closure pass. Until then item 8 remains unresolved. F-CTR-5a follows this
fixture in the selected work order.
