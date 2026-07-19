# ADR 462 - remove the silent QueryBudget default

Delete `impl Default for QueryBudget` so no construction can silently receive the
64 MiB unit-test budget; every site states its choice. Behavior-neutral by
construction — each of the nine call sites becomes an explicit
`QueryBudget::test_default()` — with an architecture guard preventing the silent path
from returning. Deriving real budgets from configuration is a policy decision, named
as follow-on, not smuggled in.

## Context

- `Default for QueryBudget` returns `test_default()` (`distributed/budget.rs:107`)
  and reaches production: the daemon's coordinator construction (`daemon.rs:288`),
  the Wendelon descriptor-sync accountants (`catalog.rs:2565,2632`), the broadcast
  cap ceiling (`binder/join.rs:1110`), and `TenantBudgetPolicy::omitted_config`
  (`budget.rs:250`). The 454–461 arc measured how completely this constant shapes
  engine behavior; 454 deferred the engine-side fix as its own cut. This is it.
- The queue item said "retarget"; the grounded cut is **removal**. Retargeting
  `Default` to anything else would silently change every site at once — the exact
  failure mode being excised. Removal forces each site to be a visible decision.
- The daemon already receives `config.worker.memory_budget_bytes` (it flows to
  `TenantBudgetPolicy`), so a config-derived per-task budget is *available* — but
  choosing its derivation (fractions, floors, interactions with the frontier law)
  is policy with production consequences and no gauge coverage here. Named
  follow-on, with the wiring point documented at the daemon site.

## Decision

1. Delete `impl Default for QueryBudget`. `test_default()` remains the single
   explicit constructor of the test constant.
2. Rewrite all nine `QueryBudget::default()` sites as `QueryBudget::test_default()`
   — byte-identical behavior, visible provenance. The daemon and catalog sites gain
   comments naming the follow-on (config-derived budgets) and the wiring point.
3. Architecture guard (`adr462`): source-scan asserting `impl Default for
   QueryBudget` and `QueryBudget::default()` are absent from `src/` — the
   reject-string-absence pattern, so the footgun cannot quietly return.

## Acceptance

- No `Default` impl; all sites explicit; grep-clean under the guard.
- Full architecture_test and engine suite green; toron-tpch gates green (the gauge
  already stopped using the silent path in 454).
- Behavior-neutrality stated and relied upon: no perf re-measurement is claimed or
  needed; every sealed verdict's conditions are unchanged.
- The follow-on (config-derived production budgets) recorded in the forward arc
  with the daemon wiring point.

## Guardrails and risks

- **No policy rider.** Any site that "obviously should" use a real budget still
  gets `test_default()` here; changing its value is the follow-on's decision with
  its own evidence.
- **Public-API ripple**: `QueryBudget` is `pub`; removing `Default` can break
  downstream `..Default::default()` uses — the workspace compile plus suites are
  the containment, and `--all-targets` catches benches/examples (a widely
  constructed type per standing practice).
- **Do not re-judge sealed verdicts.**

## Forward arc

Config-derived production budgets (daemon: `config.worker.memory_budget_bytes` →
per-task derivation under the 455 frontier vocabulary) is the successor governance
cut. Behind it: ADR 132's comparator, q17 remeasurement, q21 residual, q19
re-diagnosis + 438 widening, q13, Cut A (439 mirror), and the generic O(1) LRU
rewrite remain queued.
