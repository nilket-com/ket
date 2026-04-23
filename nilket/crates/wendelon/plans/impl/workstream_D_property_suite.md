# Workstream D Property Suite

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `D5`
Roadmap Source: `plans/impl/workstream_D.md`

## Purpose

This is the minimum promoted property suite for `M1/M2`.

It exists to make one thing explicit: which properties we are willing to claim are enforced by executable tests now, and which tests are the gate evidence for those claims.

## Property Set

| Property | Status | Evidence | Why It Matters |
|---|---|---|---|
| No stranded quorum-durable writes | `live` | `tests/property_suite.rs::property_pending_finalization_restart_recovers_without_losing_value` | A write that survives the data branch but stalls before finalization must not remain stuck forever after restart |
| No silent loss of finalizable work | `live` | `tests/property_suite.rs::property_pending_finalization_restart_recovers_without_losing_value` | Persisted pending-finalization work must either finalize or stay visible as pending, not disappear |
| Replay reaches a legal terminal state | `live` | `tests/property_suite.rs::property_duplicate_replay_converges_to_a_legal_terminal_state` | Duplicate replay is allowed to be retryable while recovery is in flight, but it must converge to a terminal duplicate with a finalization marker |
| Bounded overload rejection instead of unbounded fan-out | `live` | `tests/property_suite.rs::property_backpressure_retries_stay_bounded_without_topology_refresh` | Overload must shed with a bounded retry loop and no topology-refresh storm |
| Register linearizability checker rejects impossible histories | `live` | `tests/property_suite.rs::property_register_linearizability_checker_accepts_legal_and_rejects_illegal_histories` | The in-repo linearizability gate used by the simulator ladder must stay trustworthy for the short histories we claim it covers |

## What D5 Adds Beyond D3 And D4

`D3` and `D4` prove specific adversarial scenarios execute.

`D5` promotes a narrower but stronger statement:

- these are the minimum properties we expect to survive refactors
- these tests are now part of the gate evidence, not just helpful coverage
- a future regression against one of these properties is a program-level failure, not a local test break

## Boundaries

This is intentionally not the full future property program.

Still out of scope for this first D5 pass:

- multi-key or multi-register linearizability
- external Elle or Jepsen-style verdict ingestion
- probabilistic long-horizon state-space exploration
- SLO soak evidence

Those belong to later `D5` expansion and `D6`.

## Reproduction

Run the promoted property suite with:

```bash
cargo test --quiet --test property_suite
```

Or run the whole workspace gate:

```bash
cargo test --quiet
```

## Exit Bar

I would treat `D5` as complete for the current milestone when:

1. every property row above is `live`
2. the tests run in ordinary CI without private harness steps
3. the scorecard points at this file as the canonical D5 artifact
