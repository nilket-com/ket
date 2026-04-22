# Workstream D: Observability, Validation, And SLO Evidence

Date: 2026-04-22
Author Role: Lead Architect
Source: `plans/gap/execution_20260422.md` WS3

## Mandate

Turn claims into evidence. This workstream exists because a distributed system without instrumentation and adversarial validation is just a story.

## Architectural Position

No uninstrumented critical path ships. No SLO is promoted without soak evidence.

My position is:

- metrics are part of the runtime contract, not garnish
- chaos and simulator coverage must target the failure modes we are claiming to handle
- "TARGET" SLOs remain non-contractual until the soak gates are met

## Scope

- metric surface expansion
- trace coverage for critical flows
- chaos matrix expansion
- simulator ladder expansion
- deterministic migration-pressure scenarios
- linearizability validation
- SLO promotion criteria and evidence

## Non-Goals

- implementing the hot-path fixes themselves
- rewriting runbooks and DR procedures
- CMVP lab process

## Required Metric Families

Must exist by Gate 3:

- pending finalization backlog and replay
- replica fan-out latency
- quorum acquisition latency
- WAL append and sync latency
- `io_uring` queue depth and errors
- active connections and streams
- request rejection and backpressure
- lock contention or saturation indicators where measurable

## Work Packages

### D1. Metric Catalog And Owners

Create an explicit metric catalog with:

- metric name
- source code owner
- alert owner
- dashboard placement
- release gate that depends on it

If a metric has no owner, it will rot.

### D2. Trace Critical Paths

Required trace spans:

- write branch split
- durable quorum achieved
- finalization commit and replay
- migration-read proxy path
- transport admission rejection

Tracing should tell an operator where time went, not just that time was spent.

### D3. Chaos Expansion

Expand beyond smoke scenarios into:

- partition + restart
- partition + transport overload
- disk pressure / local persistence failure
- finalization failure windows
- topology change + migration pressure

### D4. Simulator Ladder Expansion

Move from one named scenario to a real matrix that covers:

- upgrade windows
- migration pressure
- lease churn
- transport degradation
- write-path failure windows

### D5. Linearizability And Soak

Add either:

- real Jepsen integration
- or a comparably serious in-house checker

Then run weekly soaks long enough to justify SLO promotion.

## Deliverables

- metric catalog
- dashboards and alert proposals
- critical trace map
- expanded chaos suite in CI
- expanded simulator ladder
- linearizability harness or equivalent
- SLO evidence package

## Exit Criteria

- every critical failure mode from the merged execution plan is instrumented
- chaos coverage is materially beyond smoke-level
- simulator coverage is materially beyond a single scenario
- SLOs are promoted only with soak evidence attached

## Recommended Sequencing

1. metric catalog and owners
2. critical metrics and traces for WS1/WS2
3. chaos expansion
4. simulator expansion
5. linearizability
6. soak and SLO promotion

## Risks

- unbounded metric cardinality burns the observability backend
- chaos scenarios are added without real assertions
- SLOs get promoted because the team is schedule-constrained, not because the evidence is good

## Opinionated Constraints

- Do not merge a critical runtime change without its metrics.
- Do not count a chaos test that only asserts "cluster did not crash."
- Do not promote an SLO on sampled anecdotes and one lucky soak.

## Open Questions

- Do we integrate with Jepsen directly, or build a narrower in-house checker first?
- What exact soak duration and load shape are required before promoting an SLO to `COMMITTED`?
- Which lock-contention metrics are worth emitting directly versus inferring through latency histograms?
- How much trace sampling is acceptable before we lose confidence in rare failure-path observability?
