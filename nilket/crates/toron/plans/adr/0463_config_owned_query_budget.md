# ADR 463 - config-owned query budget: explicit knobs at the daemon wiring point

Give the daemon's coordinator `QueryBudget` explicit configuration for the two
coordinates the 454–461 arc measured — `memory_per_task` and
`spill_bytes_reserved_per_query` — as optional `[worker]` config fields. Present
means the operator chose; absent means today's explicit test constant, unchanged.
No derivation formula ships in this cut: that requires evidence a formula is right,
and none exists yet.

## Context — the fork, decided

- 462 removed the silent default and documented the daemon site as the wiring point;
  the forward arc named "config-derived production budgets."
- Grounding shows *derivation* is premature: `config.worker.memory_budget_bytes`
  itself defaults to 64 MiB (`config.rs:66`), the divisor any per-task formula needs
  (target concurrency per worker) is unestablished, and
  `derive_max_concurrent_queries` runs the other direction (per-task → slots). A
  formula would be invented policy — the anti-pattern this arc refused from 454 on.
- The knob-first cut is the 454 gauge move at engine level: the same two coordinates,
  explicit and validated, meaning defined by the 455 frontier vocabulary (an operator
  setting `memory_per_task` knows the reservoir regime boundary scales linearly with
  it, spill reserve non-binding). Formula-derivation stays a named successor,
  unblocked the day a gauge can price a divisor.

## Decision

1. `ToronWorkerConfig` gains optional `query_memory_per_task_bytes` and
   `query_spill_bytes_reserved_bytes` (serde-defaulted to absent). Zero or
   unparseable values fail config load loudly — same contract as the gauge knobs.
2. A config-owned constructor (`ToronWorkerConfig::query_budget()`) builds the
   coordinator budget: start from the explicit test constant, override whichever
   coordinates are present. The daemon's construction site consumes it, replacing
   the bare `test_default()` and its follow-on comment.
3. Tests: config parse round-trip (absent, present, zero-rejected); the constructor
   unit-tested for all three shapes; an architecture pin that the daemon's
   coordinator budget flows through `query_budget()` (so the wiring point cannot
   silently regress to a constant).
4. Config example/docs updated where `memory_budget_bytes` is documented.

## Acceptance

- Absent knobs → byte-identical behavior (relied on; no perf claims).
- Present knobs → values land in the coordinator `QueryBudget`, proven by unit test.
- Loud rejection of zero/invalid; architecture pin green; full architecture_test and
  engine suite green; adr176-family allowlists extended per their mechanism if
  tripped.
- No sealed verdict re-judged; the gauge's env knobs are untouched (they configure
  the tpch harness, not the daemon).

## Guardrails and risks

- **No formula.** Any "derive from worker memory" expression is out of scope; the
  successor cut owns it with evidence.
- **Two knobs only** — the coordinates the arc measured. Widening to the rest of
  `QueryBudget` waits for a consumer with a case.
- **Config surface is public**: naming is load-bearing; `query_`-prefixed to bind
  them visibly to the per-query budget, `_bytes`-suffixed like the existing field.
- **Do not re-judge sealed verdicts.**

## Forward arc

Evidence-priced derivation (a gauge for coordinator-level budget policy) is the
successor when wanted. Behind it: ADR 132's comparator, q17 remeasurement, q21
residual, q19 re-diagnosis + 438 widening, q13, Cut A (439 mirror), and the generic
O(1) LRU rewrite remain queued.
