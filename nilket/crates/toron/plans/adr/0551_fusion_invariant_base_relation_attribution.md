# ADR 551 - Fusion-invariant base-relation attribution

**Status:** Proposed 2026-08-13. Accepting this record discharges Stage 0.

## Context

ADR 550 withdrew ADR 538's component-dominance fork because `scan` and `operator`
do not name the same scope on both arms: a base-relation read is written to
`scan_micros` only when it happens at a bare `ExecutionPlan::Scan` node, and to a
join bracket or to remainder when it happens inside a fused operator loop. No
reweighting of the banked artifact can recover a comparable partition, so the
instrumentation is the prerequisite and this record is what must be settled before
any new timing is collected.

**Stage 0 is deliberately measurement-free, and accepting this record discharges
it.** Its output is a taxonomy, a control specification, and a predeclared rerun and
dominance rule — not code. **Stage 1** implements the instrumentation and the
discriminating controls; **Stage 2** is the fresh run. Nothing here selects a
substrate family or authorizes engine optimization.

### The inventory (Stage 0 item 1, complete)

The read surface is exactly one method — `StorageScan::scan(ScanRequest) ->
ScanStream` — so an inventory of its callers is complete by construction rather
than by search. On current main:

| route | sites | writes `scan_micros` |
|---|---|---|
| `exec.rs:855` bare `ExecutionPlan::Scan` arm in `execute_bound` | 1 | **yes — the only site** |
| `exec/aggregate.rs` fused aggregate paths (4 fns) | 4 | no |
| `exec/join/streaming.rs` fused join paths (19 fns) | 24 | no |
| `server/mod.rs:4367` `drive_streamable_plan` | 1 | no |
| `coordinator/delete.rs:33` `materialize_delete_candidate_batches` | 1 | no |

Thirty-one production call sites, one of which is instrumented. Test-only routes —
`exec.rs:418` (`#[cfg(test)] fn execute`, the unbound logical executor) and eleven
sites in `storage.rs`'s test module — are excluded from timing but **not** from the
scope definition: ADR 549 established that a test-only evaluator disagreeing with
the shipped one lets tests pass against semantics the engine does not have.

### A second scope variance, independent of fusion

The inventory surfaced a route the fusion analysis did not predict. Two production
decorators implement `StorageScan` and are installed as the task executor's storage
in the worker setup path (`server/mod.rs` 339–412, 692):

- `TaskInputStorageScan` — serves pre-materialized batches for a `TableId` from an
  override map;
- `LiveInputStorageScan` — serves `LiveProducerPartitionRef` partitions through the
  `PartitionStore`.

So on the distributed arm a `scan()` call, including one at the instrumented bare
node, may be reading an **exchange partition or an injected task input rather than
a base relation**, and `scan_micros` cannot currently tell the two apart. This
record makes no claim that this occurred in any ADR 538 cell — it is a route that
exists and is production-wired, and a taxonomy that ignores it would reproduce the
same class of defect on a different axis.

### The design constraint

Adding a scan timer inside a fused operator bracket would **double-count**: the
interval would be attributed to both `scan` and the enclosing `operator`, and the
task-wall partition ADR 538 relies on would break. The new instrumentation must
produce **exclusive, non-overlapping phases**. Two admissible constructions:

1. **Subtractive** — bracket the scan interval and subtract it from the enclosing
   operator's phase at accrual, so operator time is reported net of scan.
2. **Task-local phase ledger** — a single ledger that assigns each wall interval to
   exactly one phase, with nesting resolved at record time rather than by
   convention at each call site.

The ledger is preferred: ADR 538's spill/build double-count arose from eighteen
call sites feeding one interval into two counters, and a guard that scans call
sites is weaker than a structure that cannot express the error. The choice is
Stage 1's, but either way the exclusivity property is what must be proven, not the
mechanism.

## Decision

Stage 0 commits the following before any timing is collected.

### 1. One scan scope: opening and actively polling a base-relation stream

`scan` is **the time spent opening a base-relation stream and actively polling it**:
the `await` on `StorageScan::scan`, and the time each `poll_next` is in flight
waiting for a batch to be yielded.

Two exclusions make this exact, and both are load-bearing:

- **`ScanRequest` construction is excluded.** It is predicate and projection
  preparation, not acquisition.
- **All consumer work between yielded batches is excluded.** The interval closes the
  moment a batch is yielded and reopens only when the next poll begins.

Without the second exclusion, a scope defined as "materialization" would swallow
the fused predicate evaluation, aggregate folding and join build/probe that run
between batches — reclassifying operator work as scan and inverting the very defect
ADR 550 recorded. Timing only the `scan()` call is the opposite error, reporting
near-zero everywhere; the poll-in-flight interval is the one definition that is
neither.

### 2. Classification follows the stream's origin, not its decorator

Both `TaskInputStorageScan` and `LiveInputStorageScan` **delegate to base storage on
a miss**, so the installed wrapper does not determine what a stream carries. The
decorator is not the classifier.

`ScanStream` (`storage.rs:191`) carries `schema`, `session_id`, `query_id` and
`batches` — **no provenance** — and `into_batches` (`storage.rs:227`) returns the
bare `BoxStream`, erasing the wrapper at the point of consumption. A pinned origin
is therefore a prerequisite, not a convenience.

`ScanStream` gains an **origin** — `BaseRelation`, `MaterializedExchangeInput`, or
`LiveExchangeInput` — set where the stream is constructed and **preserved through
consumption**, so the consumer can attribute without knowing who wrapped the
storage.

### 3. Exchange-input consumption lands in ADR 538's existing components

ADR 538 already defines exchange consumption as `shuffle` and decode as
`transport`. This record adds no component and resolves the mapping now:

| origin | attributed to |
|---|---|
| `BaseRelation` | `scan` |
| `MaterializedExchangeInput` | `shuffle` |
| `LiveExchangeInput` — pull and wait | `shuffle` |
| `LiveExchangeInput` — decode | `transport` |

**None of it is `scan`.** Absent this the distributed arm would count exchange
output as base-relation work, which is the ADR 550 defect on a second axis.

### 4. Coverage is structural, not enumerated

Attribution is carried by **one metered stream-consumption API that production
callers cannot bypass**. `ScanStream::into_batches` — 33 call sites today, 30 of
them in execution paths — hands out a raw `BoxStream` and is exactly the hole that
let a single instrumented node stand in for thirty-one read routes. It is replaced
by, or wrapped in, a consumption path that meters every poll and attributes it by
origin.

An enumeration guard over the 31 call sites is **secondary**, a backstop against
new routes, not the mechanism. An inventory has to be re-verified every time
someone adds a caller; an API that cannot be bypassed does not. This is the same
reasoning ADR 538's spill/build double-count taught in the other direction: a guard
that scans call sites is weaker than a structure that cannot express the error.

### 5. Join-operator time is exclusive of the scan scope

`operator` remains join build/probe/spill and is reported **net of** any scan scope
interval nested inside it. `operator` and `scan` must not overlap on any timeline.
The ledger identity that must hold exactly is

    inclusive operator = exclusive operator + nested scan

### 6. Phases stay a partition of the task wall

The per-node wall-partition veto and the missing-counterpart refusal are retained
unchanged. Eligible portions must not exceed a node's own wall, on- and off-path;
a missing local receipt is a defect, never a silent zero.

### 7. Rerun and dominance rules are predeclared

The rerun is a **fresh matched-envelope paired run** on the same protocol as
ADR 538 — 7 measured pairs, 2 discarded warmups, 21 cells with q15 excluded,
parity-fixed arm order, median paired delta and ratio as the estimators,
`distinguished` requiring strictly signed deltas. The dominance rule is unchanged:
a component leads a cell at ≥50% of its median paired delta, a branch wins at
≥⌈2/3·|S|⌉ with |S| ≥ 5, otherwise no-work.

Both are fixed here, before the numbers exist, so a branch cannot be argued into
winning afterwards.

### 8. No existing artifact is reweighted

`timing_atlas_cad04fc4.jsonl` and every earlier artifact are **not** re-derived into
the new taxonomy. They were produced under a scope that the new one replaces, and
a recomputation would inherit the defect while appearing to be new evidence. The
next authoritative component verdict requires a fresh run after the instrumentation
passes the controls below.

## Acceptance

**Accepting this plan discharges Stage 0.** Its output is the taxonomy above and
the control specification below. **Stage 1 implements** the instrumentation and
these discriminating controls; Stage 2 is the fresh run and is not authorized here.

Every control is **exact by construction** — no cost-model plan selection, no real
storage, no undeclared tolerance:

- **Plans are directly constructed**, bare and fused, rather than obtained by
  binding SQL. Plan shape is cost-model driven, so a bound plan cannot be relied on
  to be the shape the control needs.
- **A fake stream** supplies batches with a scripted yield schedule, so the work
  under test is fixed rather than measured off real I/O.
- **An injected clock** makes intervals deterministic. Assertions are equalities on
  known quantities, not tolerances around a wall-clock reading.

Each control records **fail-on-`bd3c8e59`, then pass-on-implementation**. A control
that cannot fail against the current engine is not evidence.

1. **Bare-versus-fused equivalence.** The same base read, driven through a directly
   constructed bare-`Scan` plan and a directly constructed fused plan over the same
   fake stream and clock, attributes an **identical** `scan` figure. This is the
   control that would have caught the ADR 550 defect.
2. **Ledger identity, exactly.** `inclusive operator = exclusive operator + nested
   scan`, asserted as an equality on injected-clock values, not a bound or a
   tolerance. Underflow is a reported defect; no `saturating_sub`.
3. **Consumer work is excluded from scan.** A fake stream that yields batches with a
   scripted delay between them, consumed by an operator with scripted per-batch
   work, attributes only the poll-in-flight intervals to `scan`. Increasing the
   consumer's per-batch work must leave `scan` **unchanged**.
4. **Origin classification, proven on both sides of each wrapper.** For each of
   `TaskInputStorageScan` and `LiveInputStorageScan`, two separate cases:
   **wrapper hit** — the override is served, origin is the exchange variant, `scan`
   is zero and the time appears under `shuffle`/`transport` per Decision 3; and
   **wrapper delegation** — the same wrapper misses and falls through to base
   storage, origin is `BaseRelation`, and the time appears under `scan`. Proving
   only the hit would leave the delegation path silently misclassified.
5. **Origin survives consumption.** A stream's origin is still readable after it has
   passed through the metered consumption API, so `into_batches`-style erasure
   cannot recur.
6. **Coverage is unbypassable.** A production consumption path that does not go
   through the metered API fails to compile or fails a guard. The 31-site
   enumeration guard is retained as a secondary backstop.
7. **Exclusivity.** For every task, assigned phase intervals sum to no more than the
   task wall and no interval appears in two phases — structural, not a numeric
   coincidence on one workload.
8. **Wall-partition veto and missing-counterpart refusal still fire**, proven by a
   negative control that trips each.

## Guardrails

- **No engine optimization.** This arc changes instrumentation and taxonomy only.
  No `ScanShare`, no scan reuse, no scheduler or capacity work.
- **No substrate family is selected**, and none may be selected until a fresh run
  under this taxonomy produces a verdict.
- **No artifact is reweighted or resealed**, and no earlier artifact is edited.
- **The merged scan+operator crossing from ADR 550 is not a hypothesis to confirm.**
  It was computed after the board was known; treating it as a prior would bias the
  taxonomy toward reproducing it.
- Timing is collected only after every Acceptance control passes.

## Risks

- **Directly constructed plans can drift from what the binder actually produces.**
  The controls deliberately bypass the cost model to stay exact, which means they
  prove the instrumentation correct without proving the shapes are ones the engine
  emits. That is the right trade for a discriminating control, but it makes the
  Stage 2 run — on bound plans — the only evidence that the taxonomy behaves on real
  shapes.
- **Subtraction can go negative** under clock skew or nested brackets, and a
  `saturating_sub` would mask it, as one did in ADR 538. Underflow must be a
  reported defect, not a floor at zero.
- **Widening the scan scope will move numbers with no engine change.** Cells will
  shift between `scan`, `operator` and remainder relative to the ADR 538 board.
  That is the intended effect and must not be read as a regression or a win; the
  two boards are not comparable and no delta between them means anything.

## Forward

**Stage 1** implements the origin-carrying `ScanStream`, the metered consumption
API, the ledger or subtraction, and the eight controls — each recorded
fail-on-`bd3c8e59` then pass-on-implementation. **Stage 2** is a fresh
matched-envelope run under the rules predeclared above. Only then does a component
verdict — including a reinstated no-work — become available.

ADR 538's correctness identities, paired elapsed measurements and 21-cell board
remain authoritative throughout, and nothing here reopens them.
