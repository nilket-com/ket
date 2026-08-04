# ADR 453 - q18 membership-first chain: stream the mixed spilled-base pair

Repair the q18 sf≥2 engine red found by ADR 452's ladder. Grounding shows the committed
membership-first chain fails not because a spilled build cannot stream, but because the
drive's spilled-base arm is narrower than the machinery under it: it requires the
terminal build to be spilled too, while the terminal probe layer already handles every
build form. This cut widens the spilled-base arm to accept any terminal form —
delivering the chain at capacity, not a decline-to-fallback.

## The grounded failure, corrected across two probe rounds

Probed on the failing artifact (release, strict, parquet, per-decline and per-form
instrumentation, since removed):

- q18 sf=2 dies at EXECUTE with `committed membership-first chain could not stream its
  terminal stage` (`streaming.rs:9331`), profiled or not — an engine defect
  (profiling-free reproduction persisted under
  `target/tpch/adr452_ladders/q18_sf2_no_profile_probe/`). sf=1 never enters the chain
  surface, hence the scale trigger.
- The failing state is the **mixed build pair**: at sf=2 the base join's build prepares
  as `Spilled` while the terminal stage's build prepares as `PartitionedScan(16)`.
- The drive (`try_streaming_reoriented_inner_chain_scan_probe_base_to_sink`,
  `:11126`) already streams both **pure** pairs: a spilled base with a spilled terminal
  (`:11215`), and a partitioned-scan base (`:11314`). Only the mixed pair has no path —
  it falls through to the partitioned-scan match, declines, and the committed chain
  errors with no fallback.
- The first probe round read this as "the terminal stage cannot stream a spilled
  build" and chose an admission-mirror cut; the second round falsified that premise:
  `probe_reoriented_inner_join_streaming` (`:15249`) is **total over build forms** —
  InMemory, Spilled, and PartitionedScan (resident load first, per-batch build scan
  otherwise). The spilled-base arm's terminal-form restriction is narrower than the
  probe layer it calls. The admission-mirror cut is therefore rejected: it would
  disable a chain the machinery can already run, and it would need an estimate mirror
  the widened arm makes unnecessary.

## Decision

**Widen the spilled-base arm's condition from "base spilled and terminal spilled" to
"base spilled"**, leaving the loop body untouched: its only use of the terminal runtime
is `probe_reoriented_inner_join_streaming`, which totalizes over build forms. The
mixed pair then streams through the existing loop.

- The `:9331` hard error is **kept** for whatever committed state still cannot stream —
  that state must stay loud; this cut makes the observed one unreachable.
- No admission change, no new streaming form, no estimate duplication.
- Known cost caveat, accepted and recorded: a PartitionedScan terminal that fails its
  resident load re-scans the build per probe batch — a throughput hazard, never a
  correctness one, and exactly what the 452 instrument can measure later.

## Acceptance

- q18 strict parquet passes at sf=2 (the persisted 452 reproduction goes green).
- q18 **correctness** (answer-check against the independent oracle) passes at a
  spill-triggering scale — the widened path must prove output equality, not just a
  pass status.
- The 452 ladder climbs q18 through sf=4 to an honest per-rung outcome, closing 452's
  open q18 reachability question.
- The pre-existing q05 paired fixtures and the full architecture_test stay green.
- No deterministic unit test can force the mixed pair without a budget knob; the
  persisted probe artifacts plus the ladder record are the regression evidence, and
  adding a budget-driven forcing knob is noted as follow-on work, not smuggled in here.

## Guardrails and risks

- **Correctness before throughput.** The widened arm reuses proven probe machinery,
  but the digest/oracle check at the failing scale is mandatory before any green
  claim.
- **Keep the loud invariant.** Softening `:9331` into a fallback would hide future
  capability gaps; the error stays.
- **Don't re-judge sealed reds.** 452's q18 record stands; this is its fix-forward.

## Forward arc

With q18 green at sf≥2, the 452 ladder answers whether q18 ever commits the grouped
reservoir (unlocking Rail 2/3 oracle/cohort work) or seals as no-commit like q21. The
mixed-pair throughput caveat is measurable with the 452 instrument if q18 becomes a
paired-ablation cell. The ADR 446 12x spill-scope gap, query-budget realism,
`MAX_FILTER_BYTES` cap raise, ADR 132's comparator, q17 remeasurement, q18 strict
one-pass, q21 residual, q19 re-diagnosis + 438 widening, q13, Cut A (439 mirror), and
the generic O(1) LRU rewrite remain queued.
