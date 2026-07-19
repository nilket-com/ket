# ADR 454 - gauge query-budget realism: explicit budget, measured sensitivity

Make the tpch gauge's memory budget an explicit, receipt-recorded parameter instead of a
silent unit-test constant, and measure how budget-conditioned the 450–453 reachability
and materiality conclusions are. No sealed verdict is re-judged; each already carries
the budget it ran under — this cut makes that condition visible and quantified.

## Context

Every gauge execution runs on `QueryBudget::test_default()` — 64 MiB `memory_per_task`
— through three layers of defaults:

- `Default for QueryBudget` is `test_default()` (`distributed/budget.rs:109`).
- The executor context default embeds it (`exec.rs:158`), with `with_task_budget`
  (`exec.rs:169`) as the unused override.
- The gauge builds its context as `ExecutionContext::local_test()`
  (`toron-tpch/lib.rs:3580`) and never touches the budget.

Every threshold the 450–453 arc measured is downstream of that constant: the reservoir
commits when the 64×-inflated build estimate exceeds `memory_cap × 16` = 1 GiB, so the
q05 committing scale (sf=4), the q18/q21 no-commit verdicts' "affordable range", the
spill points, and the shape of the 451 materiality win are all conditioned on a budget
chosen for unit tests. ADR 450 flagged this; 454 executes it.

## Decision — two rails

**Rail 1 — explicit, recorded gauge budget.** The gauge reads
`TORON_TPCH_MEMORY_PER_TASK` (bytes) and applies it via `with_task_budget` in
`tpch_execution_context`; unset means the unchanged 64 MiB default, so every standing
fixture and baseline stays byte-comparable. Every perf child row and summary records
`memory_per_task` and its provenance (`default` vs `explicit`). A figure without a
recorded budget is no longer produceable by the gauge.

**Rail 2 — budget-sensitivity probe at the arc's pivot cell.** One profiled q05 child
per budget rung at the fixture scale sf=4 — 64 MiB (control, must reproduce the 450
commit), 256 MiB, and 1 GiB — classified with the 450/452 taxonomy. The commit
threshold scales with the budget (cap = 16× `memory_per_task`), so the predeclared
expectation is that commitment recedes as the budget grows. Outcomes, all admissible:

- **q05 stops committing at realistic budgets** → the 446 bridge's materiality is a
  small-budget-regime result; the 451 verdict stands as sealed (it states its regime),
  and any claim of general q05 improvement must state its budget. The arc's conclusions
  become explicitly budget-scoped.
- **q05 still commits at 1 GiB** → the reservoir regime is robust across a 16× budget
  range and the arc's conclusions generalize further than the test constant suggests.

No paired cohort, no new materiality claim, no re-run of sealed cohorts — a
reachability-classification probe only, in the 450 mold.

## Acceptance

- `TORON_TPCH_MEMORY_PER_TASK` parsed (positive bytes; reject zero/garbage loud);
  applied via `with_task_budget`; unset preserves today's behavior exactly.
- Perf rows and summaries carry `memory_per_task` + provenance; a test pins the default
  provenance and the explicit-override path.
- Probe rungs executed with per-rung fresh profile dirs and cleared spill scratch;
  outcome per rung recorded with the 450 taxonomy; control rung must reproduce the 450
  sf=4 commit or the probe is inconclusive (apparatus drift, not evidence).
- Sealed verdicts are not edited; the results doc states the budget-scoping conclusion.

## Guardrails and risks

- **Do not change the default.** Silent re-baselining would orphan every artifact this
  arc produced; realism arrives as an explicit knob plus evidence, not a new constant.
- **The engine's Default-is-test_default footgun stays** — retargeting `Default` for
  the whole engine is deliberate follow-on work with its own blast radius, not a rider.
- **Budget changes what "pass" costs.** Larger budgets may shift failures between
  memory/spill walls; the probe classifies, it does not tune.
- **Do not re-judge sealed reds or wins.** 443–453 stand as written, each in its
  budget regime.

## Forward arc

If commitment recedes with budget, the reservoir arc's next question becomes "at what
realistic budget/scale frontier does the reservoir regime begin," and the 452
instrument measures along that frontier. The engine-side `Default` retarget, the ADR
446 12x spill-scope gap, `MAX_FILTER_BYTES` cap raise, ADR 132's comparator, q17
remeasurement, q21 residual, q19 re-diagnosis + 438 widening, q13, Cut A (439 mirror),
and the generic O(1) LRU rewrite remain queued.
