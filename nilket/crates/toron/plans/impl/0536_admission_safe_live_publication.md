# impl 0536 - Admission-safe live publication

Implementation receipt for `plans/adr/0536_admission_safe_live_publication.md`. Two sealed
commits: `afedc51c` (Phase 0 — the admission seam and observe-only reporting) and
`efeb7f24` (the mechanism). No timing authority is created; elapsed stays diagnostic.

## Phase 0 — the seam and the blast radius (`afedc51c`)

`FragmentPlanConfig` gained `max_in_flight_tasks: Option<usize>`, defaulting to `None`
(unbounded) so plans fragmented without an explicit cap keep their prior shape, and
flooring a supplied limit at one to match `DistributedScheduler::with_max_in_flight_tasks`.
It is populated from the same `QueryBudget` that configures the scheduler at every capped
site: the coordinator's `fragment_config`, `run_streaming_read_driver`, the replay-signal
path in `lib.rs`, and the execution-census preflight.

`adr536_fragment_and_scheduler_receive_the_same_admission_cap` pins the wiring. It counts
only *capped* scheduler constructions: `lib.rs` also builds a scheduler over a hand-made
single-stage plan that fragments nothing and is legitimately unbounded. The guard earned
its place on first run by failing on a genuinely unwired path.

`live_publication_report(stages, cap)` (`distributed/scheduler.rs`) is the single
observation authority. It consumes the scheduler's own `consumer_supports_live_inputs`
predicate, so a multi-upstream join consumer is counted rather than overlooked by the
narrower unary shape test, and it additionally requires every shuffle input to come from a
stage that is *actually publishing live* — a unary consumer reading a sealed producer
drains nothing concurrently and must not arm the global stall premise. A paired control on
one graph pins both directions: as resolved (all sealed) it reports zero live-eligible
consumers and zero residency; with publication forced live, two and four.

**Blast radius, sf=8, from `afedc51c`: exactly one cell.** The evidence is 21
execution-census rows plus q04's fragment-rail and refusal evidence — not a 22-row
execution artifact. Every one of the 21 rows reported zero live producer stages, zero
live-eligible consumer tasks and zero residency. q04 emitted no execution row: with
corrected eligibility the ADR 529 premise guard refused it, reporting 4 live-eligible
consumer tasks, and the fragment rail independently classified it `fragments_parallel` at
widths `1,1,4,4,1` with the same count.

Two facts the phase established that the ADR did not anticipate:

- **The census corrections are ordered, not parallel.** Corrected eligibility alone makes
  the premise guard fire on q04, and that refusal is a harness error that aborts the whole
  run — the Phase 0 census measured q01-q03 and stopped. Per-cell invocation was the
  workaround. Persistence-aware classification is what restores a total board.
- **The ADR 529 premise guard had been unreachable by construction.** With the narrower
  counter reading zero for every multi-upstream shape, its refusal branch could not be
  entered; it had never evaluated a real shape before this phase.

## Mechanism (`efeb7f24`)

`resolve_shuffle_publish_modes` now takes the cap. After the existing live fixpoint, any
live component whose residency requirement exceeds the cap is demoted to `Sealed` and the
fixpoint is re-run — iterated, because demoting one component can make a neighbouring
multi-upstream consumer ineligible in turn. Residency is the task count of a component:
the stages joined by live edges, merged when they overlap, since a live producer holds its
scheduler slot until it seals and a producer blocks on *any* undrained partition.

`validate_shuffle_publish_modes` takes the same cap and computes the same components
through the same shared helpers (`live_candidate_components`, `component_residency`), so
resolver and validator cannot disagree. A graph arriving with an over-cap live component —
which the resolver never produces, but deserialization or a future caller can — is refused
before any dispatch with the typed `FragmentError::LiveComponentExceedsAdmission`, carrying
the producer stage, the requirement and the cap. The census rail's exhaustive classifier
gained its arm by compile force.

`adr536_admission_cap_decides_live_versus_sealed_for_the_same_graph` is the discriminating
test, over a real fragmented graph rather than a hand-built one. A `LeftSemi` join — q04's
kind — publishes live at an unbounded cap, so the same plan is fragmented three ways:
unbounded (live, residency `R`), cap `R` (stays live), cap `R - 1` (resolves sealed,
residency zero). It fails under a blanket join seal and under an unconditional live path
alike.

Nothing in ADR 123's dispatch relaxation or ADR 124's pacing bound changed. Rarity of the
multi-upstream live path in TPC-H is a reachability and performance observation, not a
correctness argument against a general scheduler capability: the path is correct and useful
whenever admission fits, which the cap-6 control demonstrated directly before the fix
existed.

## Board (`efeb7f24acbc`, strict sf=8 persisted, unchanged ADR 529 envelope)

Artifact: `target/tpch/adr536_census_efeb7f24/execution_census.jsonl`.

| classification | count | cells |
|---|---:|---|
| `executes_identical` | 20 | q01-q10, q12-q14, q16-q22 |
| `executes_divergent` | 1 | q11 |
| `execution_rejected` | 1 | q15 (`BroadcastProbeNotStarLinear`, never attempted) |

The predeclared expectation was 20 / 1 / 1 and the run met it. q04 is
`executes_identical`: 5 rows on both arms, equal digest and schema fingerprint, 3.11 s
distributed — consistent with the 3.6 s forced-sealed control taken before the mechanism
existed. Its receipt reads `live_producer_stages = 0`, `live_eligible_consumer_tasks = 0`,
`live_residency_requirement = 0` against `admission_max_in_flight_tasks = 4`, so the
component resolved sealed as intended. Board-wide, no cell publishes live and no cell
exceeds the cap, so no other classification moved.

## Gates

`cargo fmt` and `fmt --check` clean; `check -p toron -p toron-tpch --all-targets` clean;
toron lib 1,480 passed; toron `architecture_test` 2,100 passed, zero failed. Wendelon
carries a pre-existing red unrelated to this work (`tls-pki` in the nilket root manifest
plus two descriptor-surface guards), independently reproduced at clean `09a6f29f`.

## Completing the acceptance contract (`310736f8`)

The first two commits delivered the mechanism but did not discharge Acceptance 3 or 4. An
earlier revision of this receipt moved Acceptance 4 to a Forward section; that was a
post-hoc weakening of a promoted contract and is withdrawn. Both gates are now met.

**Acceptance 3 — the admission backstop is its own control.** The pre-existing
zero-dispatch test covers `UndrainableLiveShuffle` and does not discharge the admission
variant. `LiveComponentExceedsAdmission` now also carries `consumer_stage`, the immediate
downstream stage that cannot be held resident alongside the producer.
`adr536_forced_over_cap_live_graph_is_refused_before_any_dispatch` forces a live graph past
the cap, asserts the typed refusal, verifies the named consumer genuinely consumes the named
producer, checks requirement and cap, proves an empty work log, then accepts the *same graph*
at a fitting cap and reaches dispatch.

**Acceptance 4 — the rail distinguishes pacing from a stall.** `StallPersistenceTracker` is
pure and driven by a caller-supplied elapsed value, so both controls are deterministic
without a cluster. It replaces cancel-on-first-sample in
`execute_census_distributed_attempt` under a declared two-second window. Controls: a blocked
appender whose backlog keeps draining is never stalled across 200 samples; cleared
backpressure starts a fresh window rather than inheriting an earlier one; an unchanging
blocked state is progressing at 1,999 ms and stalled at the 2,000 ms boundary; a first
sample is never a verdict.

**A guard inverted, and reconciled.** ADR 529's shape-based premise refusal is removed.
It asked whether a drainer *could* exist and abandoned the cell when one did — which is
what aborted the Phase 0 run. Persistence answers the question directly: a receipt now
arrives only after the shuffle stood still with an appender blocked throughout the window,
so whether some consumer was eligible no longer bears on soundness. Eligibility remains on
the row as a diagnostic. Its board-wide pin was inverted rather than deleted: the former
"zero live-eligible consumers everywhere" held only because the counter was blind to the
multi-upstream arm, and is now
`live_eligible_consumers_are_confined_to_one_board_cell`, pinning the observed set and
instructing a future reader to re-derive the blast radius rather than relax the pin.

**Blast radius, corrected for scope.** "Exactly one cell" is a property of the sf=8 board
under this envelope, not of the engine. At sf=0.01 with unbounded admission the set is q04
and q13, four live-eligible consumer tasks each. Fragment shapes are scale-dependent (ADR
528 recorded cells rejecting at sf=0.01 and fragmenting at sf=8), and the dependence runs
both ways. Any restatement of the blast radius must carry board, scale and admission cap
together.

## Final board (`310736f85fa5`)

Artifact: `target/tpch/adr536_final_census/execution_census.jsonl`. Re-run at the unchanged
envelope from the sealed final identity, now exercising the persistence-aware classifier:
**20 `executes_identical`, q11 `executes_divergent`, q15 `execution_rejected`** — the
predeclared expectation. q04 is `executes_identical` with equal digest and schema
fingerprint, 5 rows on both arms, 3.29 s. No cell publishes live under this cap, so the new
classifier is inert here by construction and the mechanism, not the rail, is what admits the
board.

Gates from the final tree: `fmt` and `fmt --check` clean; `check -p toron -p toron-tpch
--all-targets` clean; toron lib 1,481 passed; toron `architecture_test` 2,100 passed;
toron-tpch lib 80 passed; zero failures. Wendelon's pre-existing red is unrelated and
independently reproduced at clean `09a6f29f`.

## Forward

Nothing from this ADR's acceptance contract remains outstanding. q11's constant-key
HAVING-scalar divergence is ADR 537. Whether the multi-upstream live path earns its
complexity is a question for after ADR 537 and the correctness gate — it is a reachability
and performance question, and nothing here licenses touching ADR 123.
