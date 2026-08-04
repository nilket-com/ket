# ADR 454 implementation receipt

Both rails delivered. The budget-sensitivity probe's answer is decisive: among the
probed budgets, the reservoir regime at q05 sf=4 — the regime in which the ADR 451
materiality win operates — engages only under the 64 MiB unit-test budget; the probed
256 MiB and 1 GiB regimes take the resident path.

## Rail 1 — explicit, recorded gauge budget

- `TORON_TPCH_MEMORY_PER_TASK` (positive bytes; zero/garbage refused loud at child
  start) applies via `with_task_budget` in `tpch_execution_context`; unset preserves
  the 64 MiB default exactly, keeping every standing fixture comparable.
- Every perf child row and summary now carries `memory_per_task` and
  `memory_per_task_provenance` (`default` / `explicit`; older cached rows surface as
  `unrecorded` in summaries). Verified live in both provenances.
- Pinned by `adr454_gauge_budget_defaults_and_overrides_with_provenance`.

## Rail 2 — budget-sensitivity probe (q05, sf=4, release, profiled, 450 taxonomy)

| memory_per_task | outcome | reservoir_build_rows | execute |
|---|---|---|---|
| 64 MiB (control) | **committed** | 3,885,538 | 26.7 s |
| 256 MiB | non-committing (Guard A, resident path) | — | **5.0 s** |
| 1 GiB | non-committing (Guard A, resident path) | — | **5.0 s** |

The control reproduces ADR 450's sf=4 commit exactly (same row count), so the
apparatus held still; the probe is evidence.

## The budget-scoping conclusion

This probe is reachability classification, as predeclared — not a throughput
experiment. What it establishes:

- **At both probed larger budgets (256 MiB and 1 GiB), q05 sf=4 does not enter the
  reservoir**: the chain takes the resident path (Guard A) and the 446 bridge never
  engages. The reservoir regime at sf=4 engages only under the 64 MiB test budget
  among the probed rungs.
- **ADR 451 stands as sealed**: a real, causal, within-regime result — the 446 bridge
  is material *when the reservoir regime is active*, which the probe now scopes to the
  64 MiB regime at this scale.
- The q18/q21 no-commit verdicts can only move further toward no-commit under larger
  budgets (the threshold recedes); their conclusions are budget-robust in the
  direction that matters.

Non-comparable context, recorded but not a claim: the 256 MiB/1 GiB rungs' single
profiled children finished around 5.0 s, while ADR 451's 24.3 s ON figure is an
unprofiled paired-cohort median — different instruments, different conditions. Whether
a larger-budget resident configuration actually outruns the 64 MiB reservoir regime is
a like-for-like measurement the 452 instrument can run if that ranking is ever needed;
this cut does not make it.

Gauge figures without a stated budget regime are no longer produceable, which is the
cut's lasting rail.
