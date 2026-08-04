# ADR 536 - Admission-safe live publication

The post-535 execution census (`plans/impl/execution_census_post535_results_20260804.md`)
left q04 as the only cell that does not execute. Discriminating controls at sf=8 establish
the mechanism and bound the claim: uncancelled at `max_in_flight_tasks = 4` and at 5, q04
did not complete within 300 s; at 6 it completed in 13.7 s with digests equal to the local
oracle; with publication forced sealed at 4 it completed in 3.6 s, also equal. There is no
correctness divergence — only a publication mode the admission cap cannot host.

The seam is the interaction of three existing, individually correct mechanisms. ADR 123
deliberately dispatches a multi-upstream consumer against live refs when every upstream is
a streamable producer stage, covering all 8 HashPartitioned join kinds
(`scheduler.rs:648-672`); the consumer reads through `LiveInputStorageScan` rather than
waiting for sealed manifests. ADR 124 bounds unread live backlog per partition, so
`append_live` awaits capacity once the bound is reached — designed pacing, not a fault.
ADR 535 resolves publication mode once from the whole stage graph
(`fragment.rs:255-286`) and validates it before dispatch (`fragment.rs:319-362`). None of
them consults admission. The scheduler enforces one global cap for the query,
`in_flight_count < limit` (`scheduler.rs:375-380`), and a live producer holds its slot
until it seals.

q04 fragments to `1,1,4,4,1`: two single-task streamable producers feeding a four-task join
consumer stage. Its live component therefore needs six tasks resident at once. At a cap of
4 the two producers dispatch and only two consumer tasks follow; the remaining two of the
eight live partitions are never drained, so the producers block in `append_live` and no
participant can advance. At 5 one consumer task is still missing and the outcome is the
same — which is why the requirement is the whole consumer stage, not a majority of it: a
producer blocks on *any* undrained partition. The engine selected `Live` under a legal
admission cap that cannot host the live component. The envelope is the declared operating
contract, so this is an engine liveness defect, not a harness artifact.

The census that found it cannot presently verify a fix. Its eligibility counter
(`toron-tpch/src/lib.rs:4882`) counts only tasks satisfying `is_streamable_shuffle_consumer`,
so ADR 123 join consumers are invisible and the ADR 529 premise guard never arms; and the
rail cancels on the first blocked-appender receipt (`execute_census_distributed_attempt`),
which the admission-6 control proves occurs in a run that completes healthily. Both
corrections ride this ADR, because a corrected engine measured by the current rail would
still be cancelled mid-pacing and reported stalled.

## Decision

**1. Publication mode becomes admission-aware, resolved from the graph.** Extend the ADR 535
resolver with the live *residency requirement*: for a candidate live producer, the set of
tasks that must be resident together for its partitions to drain — the producer stage's own
tasks plus every task of each consumer stage that would consume it live, closed transitively
across live edges, since a live consumer that is itself a live producer extends the same
component. When that requirement exceeds `max_in_flight_tasks`, the component resolves
`Sealed`. When admission is unbounded (`None`) the requirement is trivially satisfiable.

The rule is graph- and admission-aware, never a blanket join seal. At a cap of 4 q04
resolves `Sealed`; at a proven-sufficient cap it stays `Live`. Applying the unary
`is_streamable_shuffle_consumer` predicate to the multi-upstream arm is explicitly rejected:
it would seal every hash join and retire ADR 123, which is an architecture decision this ADR
does not make and does not need.

**2. Resolver and validator consume the same predicate.** The admission-aware requirement
lives in one function used by both `resolve_shuffle_publish_modes` and
`validate_shuffle_publish_modes`, so they cannot disagree. Today they share
`consumer_supports_live_inputs_when` and are consistent; that property must survive. A graph
arriving with `Live` set on a component whose residency requirement exceeds the cap — which
the resolver will not produce, but deserialization or a future caller can — is refused
synchronously by the existing typed `FragmentError::UndrainableLiveShuffle` path, carrying
the producer stage, the blocking consumer stage, the requirement, and the cap. Refusal
happens before the first task dispatch, never as a statement timeout.

**3. The census rail learns to see what it measures.** Three corrections, each independently
testable:

- the eligibility receipt counts every task the scheduler would treat as live-eligible,
  including ADR 123 multi-upstream consumers, by consuming the scheduler's predicate rather
  than re-deriving a second, narrower one;
- stall classification requires evidence of non-progress, not a single blocked-appender
  sample: a blocked appender that persists across an observation window during which no task
  completes, no manifest is accepted, and no live chunk is released. A transient receipt in a
  progressing query is healthy and must not be classified or cancelled;
- rows gain the facts this census could not report: per-stage publication mode, upstream
  stage edges, live-eligibility per task, and the residency requirement against the cap.

**Phase 0 — blast radius, before the mechanism lands.** The current artifact stores stage
task widths but not edges, publication modes, or eligibility, so which cells run an ADR 123
consumer is not knowable from it and width-only inference would be speculation. Emit the new
fields first and enumerate the 123-shaped cells across all 22. That set is what makes the
post-fix board predictable rather than hopeful; if it is larger than expected, the mode
changes in Decision 1 are re-scoped before implementation.

## Acceptance

1. **q04 executes.** From a sealed binary at the unchanged ADR 529 envelope, q04 classifies
   `executes_identical` with digest, row count and schema fingerprint equal to the local
   oracle, and its producers resolve `Sealed` at that cap.
2. **Live is preserved where it is safe.** A shape whose residency requirement fits the cap
   still resolves `Live` and still dispatches its consumer before the producer seals. A
   discriminating test pins both directions of the same graph across the cap boundary — the
   q04 shape at cap 6 stays `Live`, at cap 4 resolves `Sealed` — so the test would fail under
   a blanket join seal and under an unconditional live path alike.
3. **Backstop with positive control.** An externally forced unsafe `Live` graph is refused
   synchronously with the typed error naming producer, consumer, requirement and cap, with a
   work log proving zero dispatches; the adjacent admissible graph is accepted.
4. **The rail can tell pacing from a stall.** A regression reproduces the admission-6
   observation — a blocked-appender receipt in a query that then completes — and the rail
   classifies it as completed, not stalled. A genuine non-progressing graph is still
   classified stalled, from persistence evidence rather than a first sample.
5. **Board.** The full census re-runs at the unchanged envelope from the sealed identity.
   Predeclared expectation: 20 `executes_identical`, q11 `executes_divergent`, q15
   `execution_rejected(BroadcastProbeNotStarLinear)`. A cell that changes publication mode
   and stays identical is a pass; a cell that regresses to a wall fails the ADR.
6. Standard full battery from the final tree including the complete `--no-fail-fast` suite
   (engine cut), and the ADR 528 census rail's exhaustive error classification gains any new
   arm by compile force.

## Guardrails

- No buffer-depth increase, unbounded queue, permit bypass, or admission-cap raise as the
  remedy. Raising the cap makes q04 pass and leaves the defect: some larger graph exceeds any
  fixed cap. The mode must adapt to the cap, not the reverse.
- No blanket seal of hash joins, and no change to ADR 123's dispatch relaxation or ADR 124's
  pacing bound. This ADR decides *when live is admissible*, not how live works.
- Sealed publication may cost latency. No timing authority is created; the census timing
  block stands until q11 is resolved, and elapsed stays diagnostic.
- No local-executor fallback, no statement-timeout laundering of a statically knowable
  admission error, and no test-fixture substitute for the real producer path.
- The census corrections must not weaken the ADR 529 premise guard into silence: a shape the
  rail cannot classify soundly still refuses loudly.

## Forward

q11's constant-key HAVING-scalar divergence is out of scope and is ADR 537; its evidence is
independent and unaffected by publication mode. Two-phase partial aggregation remains the
named future capability from ADR 531. If Phase 0 shows the 123-shaped cell set is broad, the
follow-on question is whether the fragmenter should size live components against admission at
plan time rather than downgrading them at resolution — a planning-side decision this ADR
deliberately leaves open.
