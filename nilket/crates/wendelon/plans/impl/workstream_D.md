# Workstream D: Observability, Validation, And SLO Evidence

Date: 2026-04-22
Author Role: Lead Architect
Roadmap Source: `plans/impl/roadmap.md`
Execution Source: `plans/gap/execution_20260422.md` `WS3`

## Objective

Turn correctness and scale claims into measured, repeatable evidence that an operator can actually use.

## Why Now

This workstream must run in parallel with `B` and `C`, not after them.

If instrumentation, chaos, and simulator coverage arrive later:

- regressions hide in the gap
- benchmark deltas become folklore
- the team promotes SLO language without proof

## ADR Anchors

- ADR `027`
- ADR `025`
- ADR `028`

## Scope

- metric catalog and ownership
- trace coverage for critical paths
- chaos matrix expansion
- deterministic simulator ladder expansion
- linearizability/property coverage
- soak evidence and SLO promotion rules

## Out Of Scope

- landing the underlying runtime fixes themselves
- custody/provider implementation
- operator replacement automation
- full compliance packaging

## Dependencies

- `B` and `C` for the surfaces being instrumented and stress-tested
- `A` for scorecard and artifact discipline
- `G` for audit/trace correlation shape

## Entry Criteria

- metric naming conventions are fixed enough to avoid churn
- the critical-path spans and failure scenarios are enumerated
- scorecard rows exist for the evidence this workstream will produce

## Implementation Slices

### D1. Metric Catalog And Owners

Create a metric catalog with:

- metric name
- semantic definition
- code owner
- alert/dashboard owner
- milestone where it becomes required

If a metric is not owned, it is not real.

### D2. Critical-Path Instrumentation

Add metrics and spans for:

- write branch split
- quorum acquisition
- pending-finalization backlog and replay
- WAL append and flush
- `io_uring` queue depth and errors
- connection and stream counts
- rejection and backpressure events
- migration-read proxying

### D3. Chaos Matrix Expansion

Expand from smoke to adversarial combinations:

- partition plus restart
- crash between durable quorum and finalization
- overloaded listener plus retry traffic
- follower loss during active write load
- audit sink distress where local events are fail-closed

### D4. Simulator Ladder

Add deterministic scenarios for:

- replay ordering
- apply-order races
- recovery after interrupted finalization
- lazy-migration pressure
- quota and authorization boundary behavior

### D5. Linearizability And Property Checks

Promote a minimum property suite for:

- no stranded quorum-durable writes
- no silent loss of finalizable work
- replay reaches a legal terminal state
- bounded overload rejection instead of unbounded fan-out

### D6. Soak And SLO Evidence

Define the mixed-load soak shape and do not promote `TARGET` to `COMMITTED` until it has passed.

The mixed-load shape should include:

- writes
- reads
- failover churn
- background migration or compaction pressure

## Tests And Benchmarks

- metric emission tests for required series
- trace propagation tests across critical spans
- chaos runs with preserved seeds and result summaries
- simulator runs with saved seeds and failure reproduction notes
- linearizability/property-test jobs
- soak report with fixed workload definition and duration

## Exit Criteria

- required metric families exist and are documented
- critical-path traces tell an operator where time and rejection happened
- the minimum failure-property suite passes
- chaos and simulator ladders cover the failure modes the program claims to handle
- SLO promotion is backed by soak evidence rather than aspiration

## Artifacts

- metric catalog: `plans/impl/workstream_D_metric_catalog.md`
- scorecard support: `plans/impl/scorecard.md`
- artifact registry support: `plans/impl/artifact_registry.md`
- trace-span inventory: `plans/impl/workstream_D_trace_inventory.md`
- chaos matrix: `plans/impl/workstream_D_chaos_matrix.md`
- simulator scenario registry: `plans/impl/workstream_D_simulator_registry.md`
- property suite note: `plans/impl/workstream_D_property_suite.md`
- soak specification: `plans/impl/workstream_D_soak_spec.md`
- soak report and SLO evidence package: `plans/impl/workstream_D_soak_report.md`
- dashboard and alert inventory: pending

## Current D1 Status

- `D1` metric catalog: complete
- `A-lite` dependency support for scorecard/artifact discipline: complete and canonicalized in `plans/impl/scorecard.md` and `plans/impl/artifact_registry.md`
- `D2` first pass: complete for B-path and WAL-path metrics plus trace inventory
- `D2.2`: complete for lazy-migration proxy metrics and span inventory
- `D2.3`: complete for `io_uring` queue/error metrics and internal batch trace seam
- `D2`: first-pass complete; remaining trace follow-ons are tracked in `plans/impl/workstream_D_trace_inventory.md`
- `D3`: first-pass complete; the formerly deferred isolated-leader partition-plus-restart case is now live, while the durable-quorum-to-finalization crash row remains partial until it has multi-node chaos evidence
- `D4`: complete for replay-ordering, apply-order-race, interrupted-finalization-recovery, lazy-migration-pressure, and quota/auth-boundary scenarios
- `D5`: complete for the minimum property suite and promoted CI gate
- `D6`: soak specification and report scaffold are complete; authoritative measured run is still pending

## Deferred Follow-Ons

- richer tail-based sampling guidance
- tenant-specific or workload-class-specific SLO variants
- more advanced failure-injection tooling once the core ladder stabilizes
