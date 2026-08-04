# ADR 528 - Fragment-phase capability census before an execution-substrate comparison

ADR 527 closed red: the local executor has no query-wide thread control, so a thread-scaling
atlas is inexpressible on that substrate. The operator has opened the execution-substrate
question — the `prioritization.md` executor-model-upgrade thread, selected by demand signal.
Toron has exactly one other execution substrate: the distributed fragment/task path
(`fragment_plan` → `DistributedScheduler` → workers). Its scheduler is a work-conserving
frontier (`FuturesUnordered` over all stages, `scheduler.rs:223-242`), consumer stages fan out
`partition_count` concurrent tasks, and cross-stage pipelining is proven — so unlike the local
substrate, it can in principle express intra-query parallelism.

But "in principle" is not a measurement axis. The TPC-H gauge has never invoked the fragmenter:
`Phase::Fragment` has been reserved and never constructed since ADR 261
(`toron-tpch/src/telemetry.rs:7-18`), and the gauge binds with `single_partition_execution =
true` — the exact inverse of the distributed coordinator's posture (`coordinator.rs:1365-1369`).
No fragment-level coverage exists for any multi-join chain anywhere in the tree, and the
fragmenter's own reject taxonomy (`JoinInputNotExchanged`, `BroadcastProbeNotStarLinear`,
union-arm `MultipleExchangeBoundaries`, `SortBarrierInsideJoinInput`) plausibly excludes most
of q01-q22. Whether the distributed substrate can even *plan* the board's queries is unknown.

ADR 528 therefore begins where 527 did: with a bounded capability census, not a comparison.
Plan-shape only — no workers, no transport, no execution, no timing authority.

## Contract

1. **Census rail:** a new explicit toron-tpch rail runs q01-q22 through the gauge's exact
   LOAD → PLAN → BIND funnel, with one difference: the bind mirrors the coordinator's
   distributed posture (`single_partition_execution = false`; nothing else changes — all
   activated default-on knobs stay as they are). It then calls
   `fragment_plan(bound, FragmentPlanConfig::new(query_id, task_budget))` with the budget
   derived exactly as `coordinator.rs` derives it, and stops. Default rails are untouched.
2. **Phase honesty:** the rail constructs the reserved `Phase::Fragment` for the first time.
   Every `FragmentError` variant gets a typed classification arm; a fragment failure is an
   honest `blocked` with the exact variant, never `unrecognized`/`fail`.
3. **Per-cell receipt:** on success — stage count, per-stage task counts, maximum concurrent
   task width, and the count of single-task producer stages; on failure — the variant and
   message. Bind failures under the distributed posture are their own class, recorded with the
   bind reason.
4. **Fingerprint disclosure:** distributed-posture plans are legitimately different plans
   (ADR 490 admission declines by design). Census rows record their own fingerprint; no
   identity requirement against board fingerprints exists, and census rows never touch perf
   baselines or summaries.
5. **Classification vocabulary** (predeclared, receipts only):
   - `bind_rejected` — the distributed-posture bind fails;
   - `fragment_rejected(<variant>)` — `fragment_plan` returns that error;
   - `fragments_sequential` — fragments, but every stage has task width 1;
   - `fragments_parallel` — fragments with some stage width > 1.

## Predeclared capability gate

A bounded execution-substrate comparison (the next decision, not this one) is expressible only
over cells classified `fragments_parallel`: a sequential fragmentation can execute distributed
but cannot exhibit intra-query parallelism, so no multicore-latency claim about it is possible.

**If the `fragments_parallel` set is empty, ADR 528 closes red as a substrate-comparison
precondition.** The census matrix and the exact reject histogram still publish — they become
the evidence base that prioritizes fragmenter capability work. If the set is non-empty, a
follow-up ADR must resolve three named comparison blockers before any timing arm is authorized;
none of them is addressed here:

- **budget multiplication:** `TaskBudget::from_query_budget` (`budget.rs:314-323`) hands every
  task the full `memory_per_task` and the full per-query spill allowance, so K concurrent tasks
  hold K× the aggregate budget — incompatible with 527's fixed-aggregate-budget rule;
- **single-task producers:** `producer_task` (`fragment.rs:1010`) hardcodes one task per join
  input; scan-side parallelism exists only for union producers and broadcast probes, and
  production never sets `worker_count > 1`;
- **transport floor:** there is no in-process dispatcher; every task pays a TCP loopback round
  trip, a latency confound that must be priced or removed before wall-clock comparison.

## Acceptance

1. The census rail is invoked only by its explicit command; every default rail (reachability,
   correctness, perf) produces byte-identical behavior, pinned by test.
2. `FragmentError` classification is total: an exhaustive match, no fallthrough to
   `unrecognized`. `Phase::Fragment` is emitted by the census rail alone.
3. The receipt is typed, serde-backward-compatible, and tested for a fragmenting shape, a
   rejecting shape, and stage/task/width extraction.
4. The full q01-q22 matrix lands in the impl doc with per-cell classification, width facts,
   and the reject histogram.
5. Standard gates pass from the final tree. No fragmenter, planner-default, executor, or
   scheduler behavior changes.

## Guardrails

- **Fragmented ≠ executable.** The census proves plan admission only. Known execution-side
  walls (Decimal128 exchange partition keys, worker budget semantics) are out of scope and
  must not be inferred green from a `fragments_*` classification.
- Census rows carry no timing authority; any elapsed fields are diagnostic.
- No fragmenter fix rides this ADR, even a one-liner that would flip a cell — reject counts
  are the evidence for the *next* decision, and fixing mid-census corrupts the histogram.
- The distributed posture must not leak: `single_partition_execution` stays true on every
  default gauge rail, pinned by the existing ADR 490 test plus a census-sibling test.
- The frozen 3,554 ms floor and the closed epoch's receipts are not consulted; this is a
  capability question on a different substrate.

## Forward

- `fragments_parallel` non-empty → a follow-up ADR may authorize a bounded in-process
  substrate comparison over exactly that set, after deciding the three named blockers
  (budget model, producer parallelism, transport floor) — each an architecture decision with
  its own receipt.
- `fragments_parallel` empty → fragmenter capability work becomes the named arc, prioritized
  by the reject histogram (join-chain exchange admission, broadcast-star relaxation, sort
  placement), each variant's cell count its demand evidence.
- Either way, the census rail remains: a standing instrument that turns "can the distributed
  substrate plan this?" from folklore into a receipt.
