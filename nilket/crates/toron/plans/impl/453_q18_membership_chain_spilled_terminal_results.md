# ADR 453 implementation receipt

q18 sf≥2 is repaired: the mixed spilled-base pair now streams by re-preparing the
terminal stage in materialized form. The naive arm-widening the plan first sketched was
falsified in execution and replaced within the cut.

## What execution taught (three rounds)

1. **Round one (plan's first premise, wrong):** "the terminal stage cannot stream a
   spilled build." Probing showed the drive already streams spilled×spilled; the
   failing state is the mixed pair — base `Spilled`, terminal `PartitionedScan(16)`.
2. **Round two (naive widening, falsified by wall clock):** since
   `probe_reoriented_inner_join_streaming` is total over build forms, the spilled-base
   arm was widened to accept any terminal. Correct — and quadratic: the terminal
   PartitionedScan's resident load declines at sf=2, so every probe batch re-scans and
   re-partitions the terminal build. The run held 98% of a core for 3+ hours against a
   ~7 s expectation and was killed, not completed. A pass that costs hours at sf=2 does
   not deliver the cut's name.
3. **Round three (shipped):** on detecting the mixed pair, the drive re-prepares the
   terminal stage with `stream_scan_build = false` — the prepare itself then chooses
   InMemory or Spilled, both of which probe without re-scanning — swaps it into the
   caller's runtime in place (telemetry, sink, and cleanup all see the live runtime),
   and streams through the existing spilled-base loop. If the materialized re-prepare
   declines, the chain declines as before; the `:9331` loud invariant is untouched.

## Evidence

- q18 strict parquet sf=2: **pass, 6.6 s** through the chain (previously: hard error;
  under round two: killed at 3+ hours).
- q18 **correctness** (answer-check vs the independent oracle) at sf=2: **pass**
  (compare phase, 9.0 s).
- 452 ladder, q18, post-fix (`target/tpch/adr452_ladders/q18_post453/`): **complete
  no-commit through sf=4** (2.9 s / 6.6 s / 10.7 s / 15.2 s, all `no grouped-chain
  surface entered`). 452's open q18 reachability question closes: like q21, q18 never
  reaches the grouped reservoir, so the 446 bridge is definitively q05-only in the
  affordable range.
- q05 sf=4 perf child: pass, 24.5 s — consistent with its ADR 451 cohort median; the
  chain fix does not perturb the q05 fixtures.
- Full architecture_test green (2091); full `-p toron --no-fail-fast` suite green.

## Residuals

- The materialized re-prepare does the terminal build work a second time on the mixed
  pair (the discarded PartitionedScan prepare is cheap — it defers its scan — so the
  waste is one extra prepare decision, not a rebuilt build).
- No deterministic unit test forces the mixed pair without a budget knob; the persisted
  probe artifacts, the sf=2 correctness pass, and the post-fix ladder record are the
  regression evidence. A budget-forcing knob remains follow-on work.
- The sibling mixed pair (partitioned-scan base, spilled terminal), if reachable, still
  declines to `:9331`; it has never been observed and is left loud by design.
