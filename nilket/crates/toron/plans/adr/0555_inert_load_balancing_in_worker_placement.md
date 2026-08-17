# ADR 555 - Inert load balancing in worker placement

**Status:** Proposed 2026-08-16. **Implemented and measured.** The engine half
landed, the paired `placement-ab` comparison was built (`31a18639`) and run, and
§7 records its result: placement changed completely, affinity survived, and
SF=8 with two workers showed **no material timing effect**. A materiality/verdict
field in the artifact is the named follow-on.

## Context

An authoritative SF=8 timing atlas run (`237fb57518e4`, no replay violations)
retained per-task worker identity for the first time. Every distributed cell's
tasks were assigned to **one real worker**, placement was stable across all seven
attempts of each cell, no nil identity appeared, and both workers were live across
the cohort. ADR 554 §3 records that observation and declines to call it engine
behaviour without this audit.

A read-only trace of the placement path explains it, and the explanation is two
mechanisms with opposite standing.

### Shuffle affinity is intentional, and it works

`live_input_worker_for_all_shuffle_inputs` places a consumer on the worker holding
its shuffle input when every input resolves through exactly one live producer to
one worker. That is data locality: it avoids fetching a shuffle output across the
network to consume it. Nothing here is defective, and this record does not touch
it.

### The fallback is written to balance load and cannot

`StaticWorkerRegistry::select_for_attempt` selects
`min_by_key(active_tasks, in_flight)` over live workers. The intent is
unmistakable — least-loaded placement.

**Its inputs are never maintained.** There is no assignment to a worker's
`resources`, `active_tasks`, or `in_flight` anywhere in the crate after
registration. `ActiveTaskCount::new(n)` with a non-zero `n` appears only in unit
tests. Every live worker therefore sits permanently at `(0, 0)`.

A total order over equal keys is no order at all: `min_by_key` returns whichever
element iteration reaches first, and `StaticWorkerRegistry` stores workers in a
`HashMap`, whose order is unspecified. **Tied production placement is decided by
map iteration order.**

### Why one worker per query, yet both workers used

A leaf scan task has no shuffle inputs, so affinity declines and the fallback
chooses. The atlas rebuilds the registry per cell, so each cell draws a fresh
`HashMap` order and its leaf lands on whichever worker that order surfaces first.
Affinity then cascades every downstream consumer onto that same worker, pinning
the query. Different cells draw different orders, so the cohort splits across both
workers while no single query ever spans them.

The tie-break is reproducible in isolation: twenty-one fresh two-entry maps with
equal keys split 15/6 by iteration order alone. The artifact's 11/10 split is the
same effect.

### Scope

`StaticWorkerRegistry` is the **only** `WorkerRegistry` implementation, and the
production daemon constructs it. This is not a harness artifact.

## Decision

### 1. Make the existing fallback observe load; do not replace the policy

The rule is already least-loaded. It is inert because nothing reports load. The
correction is to maintain `WorkerResourceState` so the existing policy can
function — not to design a new placement policy, and not to declare that work
should be spread.

**"Spread work" is not the decision.** Shuffle locality avoids network transfer,
so co-locating a query may well be the faster plan. What is wrong today is that
nothing *chose* it: a policy that intends to balance is silently deciding by hash
order. This record makes the choice real and measurable; it does not presume its
outcome.

### 2. The effective key is `(active_tasks, worker_id)`; byte `in_flight` is not consulted

`WorkerResourceState` carries two fields with different standing. `active_tasks`
is a task count with an obvious reservation point. `in_flight` is a **ByteCount**
with no producer anywhere: the scheduler's own capacity check reads the length of
its pending-futures collection, which is a task count and unrelated to this field.

Byte `in_flight` is therefore **dropped from the key** for this cut. A
permanently-zero term is not forward compatibility — it is a misleading claim
that byte load is being considered, which is this record's own defect moved one
position down the tuple.

The field is left untouched, and byte-based load becomes a separate decision once
a trustworthy estimate exists. Nothing here computes one.

`worker_id` is the final, deterministic tie-break, replacing `HashMap` iteration
order (§6) and making equal-load selection reproducible across processes.

### 3. Reservation is an owned token, not paired increment and decrement

Selection — or affinity acquisition — reserves under the registry's **write
lock** and yields an owned reservation. Completion, failure, and cancellation
consume that same reservation.

**Paired increment/decrement calls are rejected.** A cancel racing a completion
can decrement twice and underflow the counter, after which a worker looks
permanently idle and attracts every task — a balancer worse than the inert one it
replaced, and silent. An owned, exactly-once token makes double release
unrepresentable rather than merely tested for.

### 4. Load state is released on every terminal outcome

Completion, failure, and cancellation all consume the reservation. A counter that
only increments converts an inert balancer into a steadily worsening one, which
is worse than the current state because it looks like it is working.

### 5. Shuffle-local affinity keeps precedence

When `live_input_worker_for_all_shuffle_inputs` yields a worker and that worker
resolves live, it still wins. Load observation changes only what happens when
affinity declines — which is precisely the leaf-task case that pins each query
today.

### 6. No tied placement may be decided by map iteration order

Whatever tie-break exists must be explicit and stable. Iteration order over a
`HashMap` is neither, and a placement that depends on it is unreproducible across
processes for reasons unrelated to the workload.

### 7. Measure before claiming an improvement

**Placement measured 2026-08-17.** The engine half (`bf5717e0`) was run at SF=8
from a clean worktree (`bf5717e0604c`, authoritative, no replay violations)
against the baseline this record was written from (`237fb57518e4`). Both runs
carry identical plan structure, so the classes below are directly comparable.

Tasks are separated into the two classes the placement path actually
distinguishes, because a single combined figure would credit this change for
structure it never touched.

**Fallback-eligible sibling groups** — multi-task stages, where affinity declines
because a producer stage has more than one task, so `select_and_reserve` decides:

| | baseline | after |
|---|---:|---:|
| sibling groups | 224 | 224 |
| groups that spread across workers | **0 / 224** | **224 / 224** |

**Single-producer affinity links** — a lone consumer following its single
producer, where §5 requires locality to keep winning:

| | baseline | after |
|---|---:|---:|
| chain links | 77 | 77 |
| co-located | **77 / 77** | **77 / 77** |

The fallback class moves from no spreading to complete spreading; the affinity
class is untouched at 100% co-location. Had the second number moved, the cut
would have traded locality for balancing and exceeded this record.

Illustratively, q19's four stage-2 siblings — which have no affinity reason to
share a worker — split evenly across both workers, where the baseline placed all
four on one. q06 remains the one cell that never spreads, and that now reads
correctly: two tasks joined by a single-producer shuffle edge, so it is entirely
in the affinity class with no fallback-eligible siblings at all.

**That earlier run was placement evidence only.** The paired comparison this
section requires has since been run, and is recorded below.

### The paired result: placement changed completely, timing did not move

An authoritative `placement-ab` run at SF=8 (`31a18639fed1`, 21 cells, two
workers, `paired_timing_authority: true`, no replay violations, no withheld
reasons) executed both arms adjacent within each pair, seven pairs per cell.

**Placement gates, over 147 attempts per arm:**

| arm | sibling groups | spread | affinity links | co-located | live reservations left |
|---|---:|---:|---:|---:|---:|
| affinity-only | 224 | **0** | 77 | **77** | 0 |
| load-observing | 224 | **224** | 77 | **77** | 0 |

The two arms placed as intended and locality survived in both, reproducing the
earlier unpaired comparison from a paired design.

**Timing: no material effect.** With `delta = load_observing - affinity_only`, so
that a negative value means spreading was faster:

- 11 of 21 cells negative, 10 positive — no directional tendency.
- **2 of 21** cells exceed ±1% of their own baseline: q02 at +1.3% and q21 at
  +1.6%, both *slower* when spreading.
- **0 of 21** cells have a median paired delta larger than that cell's own
  pair-to-pair spread.
- **1 of 21** cells holds a consistent sign across all seven pairs.

**The analysis rule, stated because the artifact does not encode one.** Each
cell's effect was taken as the median paired delta, expressed as a percentage of
that cell's median affinity-only elapsed, and compared against the range of its
own seven paired deltas. A cell whose median is smaller than its own spread is
not distinguishable from run-to-run variation by this rule. Every cell failed
that test.

**Conclusion: at SF=8 with two workers, making the fallback observe load changed
placement completely and moved wall time not at all.** Neither outcome §7
anticipated — a speedup, or spreading proving slower and thereby vindicating
co-location — is supported. The parallelism gained appears to be offset by the
shuffle locality given up, which is precisely the trade this section named; that
balance is a property of *two* workers and this scale, and says nothing about
wider pools.

**No materiality gate exists in the artifact.** `placement_ab_cell_summary`
carries `median_paired_delta_ms` with no verdict or distinguished field, so the
null reading above is the analyst's, not the artifact's. A reader could take
q21's +377.6 ms as a finding; against a 24-second baseline with a 1,075 ms spread
across its own pairs, it is not one.

**A materiality/verdict field is a named follow-on, and must not be retrofitted
to reinterpret this run.** The raw paired results stand as measured. A later
artifact may encode a floor and a verdict; re-deriving this run's conclusion
through a rule invented afterwards would be the post-hoc weakening this arc has
refused elsewhere.



The worker-identity artifact path added in `237fb575` already reports which worker
ran each task. The locality-versus-parallelism trade-off is measured through it on
a matched SF=8 pair before any performance claim.

A run in which queries spread across workers and get **slower** is a legitimate
outcome that would vindicate co-location on its merits, and it must be reportable
as such rather than treated as a regression to be tuned away.

## Acceptance

- **Selection and reservation are atomic from the next selector's perspective.**
  Reserving after dispatch leaves a window in which two selectors observe
  identical load and recreate the tie. Asserted by selecting twice with no
  intervening completion.
- **A reservation cannot be released twice.** The type makes it unrepresentable;
  the control asserts a cancel racing a completion cannot underflow the counter.
- **Byte `in_flight` is not consulted.** Asserted as absence: two workers
  differing only in that field select identically.
- **Sequential leaf-task selection spreads across equal workers once the first
  assignment is live.** Two live workers, two successive tasks with no shuffle
  inputs: the second must not select the worker already running the first. This
  control fails against today's engine and is the record's point.
- **Every terminal outcome releases load state** — completion, failure, and
  cancellation each asserted separately, because they are three code paths and a
  shared assertion would let two of them rot.
- **Shuffle-local affinity still wins when its preconditions hold**, including
  when the affine worker is the more loaded one. Otherwise this record would have
  quietly replaced locality with balancing.
- **No tied placement is decided by map iteration order.** Asserted structurally:
  equal-load selection over the same worker set is identical across repeated
  registry constructions.
- **The measured effect is reported from the worker-identity artifact**, showing
  distinct workers per query where placement changed, before any speedup is
  claimed. **Met** for the fallback class (0/224 → 224/224) with the affinity
  class held at 77/77; the speedup measurement itself remains owed.

## Guardrails

Locality is not being removed. If a change makes shuffle-local placement lose to
a load rule, the cut has exceeded this record.

An inert balancer and an absent one are different failures. This record fixes the
first; it does not introduce a placement policy where none existed.

The artifact reports worker identity, not worker capability. Nothing here licenses
inferring why a worker was faster or slower from placement alone.

## Risks

Maintaining live load state introduces write contention on a structure currently
read-only after registration. The counters are small and per-dispatch, but this is
a real cost on the dispatch path and should be measured rather than assumed
negligible.

Spreading a query across workers converts local shuffle reads into network
transfers. That may be slower, which is why §5 measures rather than claims.

Load counters can drift if a release path is missed. The three-outcome control in
Acceptance exists because the failure mode — a worker that looks permanently busy
and stops receiving work — is silent and looks like placement preference.

## Follow-on

**A materiality/verdict field for the paired artifact.** `placement_ab_cell_summary`
reports a median paired delta and nothing about whether it means anything, so the
null reading in §7 depends on the analyst rather than the artifact. A floor and a
verdict — as ADR 538's board carries — would make a null result explicit and
survive a reader who takes a large absolute number at face value.

It applies to future runs only. The `31a18639fed1` artifact and its raw paired
results are preserved as measured.

**Whether the trade holds at wider worker pools.** §7's result is a property of
two workers at SF=8: the parallelism gained by spreading appears to be offset by
the shuffle locality given up. Nothing here establishes where that balance sits
with more workers, and this record does not speculate.

**Byte-based load**, per §2, once a trustworthy estimate exists. Nothing computes
one today.

## Forward

ADR 554's Branch B closes with the finding that its `loop_non_path_ms` was
serialized off-path sibling execution under single-worker placement. This record
addresses why that placement occurred. Whether correcting it changes the
distributed-versus-local deltas that arc measures is exactly the §5 measurement,
and is not assumed here.

Branch A — in-task unbracketed work — remains independent and untouched.
