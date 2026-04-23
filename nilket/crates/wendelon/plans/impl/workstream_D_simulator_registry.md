# Workstream D Simulator Registry

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `D4`
Roadmap Source: `plans/impl/workstream_D.md`

## Purpose

This is the canonical registry for deterministic simulator scenarios.

It exists to answer one question honestly: which failure classes are covered by executable, seed-reproducible scenarios now, and which still need a better harness seam before we should claim coverage.

## Status Rules

- `live`: executable in `tests/simulator_ladder.rs` now
- `partial`: some reusable evidence exists, but not yet as a named ladder scenario
- `gap`: no credible named simulator scenario yet

## Registry

| Scenario | Status | Harness / Source | Current Evidence | Notes |
|---|---|---|---|---|
| Coverage ladder | `live` | `tests/simulator_ladder.rs` | `coverage_ladder` | Mixed counter/register/object-store/transport budget; broad smoke, not a substitute for named scenario coverage |
| Replay ordering | `live` | `tests/simulator_ladder.rs` | `replay_ordering`, `simulator_replay_ordering_scenario_is_live` | Proves committed replay stops at the durable boundary after reopen and does not pull an uncommitted WAL tail into recovered state |
| Apply-order races | `live` | `tests/simulator_ladder.rs` | `apply_order_races`, `simulator_apply_order_races_scenario_is_live` | Proves admission snapshots stay monotonic across finalize apply, snapshot install, and lease-only no-op applies that must not publish |
| Recovery after interrupted finalization | `live` | `tests/simulator_ladder.rs` | `interrupted_finalization_recovery`, `simulator_interrupted_finalization_recovery_scenario_is_live` | Seeds a durable pending-finalization record across restart and proves the bootstrap reconciler drains it into an applied finalization marker |
| Lazy-migration pressure | `live` | `tests/simulator_ladder.rs` | `lazy_migration_pressure`, `simulator_lazy_migration_pressure_scenario_is_live` | Repeated `MIGRATION_INCOMPLETE` responses force retry/backoff without topology refresh and still converge once pressure clears |
| Quota and authorization boundary behavior | `live` | `tests/simulator_ladder.rs` | `quota_authorization_boundary`, `simulator_quota_authorization_boundary_scenario_is_live` | Quota exhaustion fails closed without retry churn, and the actor permission matrix stays aligned with the product boundary |

## What This Means

`D4` now has executable coverage for:

- durable replay ordering at reopen
- admission-snapshot apply ordering and monotonicity
- startup recovery of persisted interrupted finalization work
- lazy-migration retry pressure
- quota and authorization boundaries

## Reproduction

Named simulator scenarios replay through:

```bash
WENDELON_SIM_SEED=<seed> cargo test --test simulator_ladder <scenario> -- --ignored --nocapture
```

The supported scenario list is maintained in:

- `tests/simulator_ladder.rs`
- `tests/sim/seeds/README.md`

## Exit Bar For D4

`D4` is complete when:

1. every row above is at least `live` or explicitly deferred by design
2. every `live` row has a stable named scenario plus at least one direct test proving the scenario still executes
3. the registry is the thing we update when a new deterministic failure class is discovered, not a dead appendix
