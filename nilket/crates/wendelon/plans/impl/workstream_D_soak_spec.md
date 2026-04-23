# Workstream D Soak Specification

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `D6`
Roadmap Source: `plans/impl/workstream_D.md`

## Purpose

This is the canonical soak contract for `M2/M3`.

It exists to prevent two failure modes:

- promoting SLO language from short tests and intuition
- running ad hoc load that no one can reproduce later

If a run does not match this document closely enough, it is not soak evidence.

## Promotion Rule

`TARGET` may become `COMMITTED` only after this soak shape passes on the intended hardware profile and the corresponding report is written to:

- `plans/impl/workstream_D_soak_report.md`

Until then, all latency and availability language remains provisional.

## Deployment Profiles

Two profiles matter for D6:

### Commodity Default

- `3` nodes
- `rf = 3`
- `2-4 vCPU`
- `8-16 GiB RAM`
- commodity NVMe or VPS-backed SSD
- explicit `wal_io_depth` and `slab_io_depth`
- L2 enabled with local or object-store-backed tiering

This is the baseline profile for the actual product promise.

### Premium Supported

- `3` nodes minimum
- higher-core / higher-IOPS machines
- multi-device explicit config allowed on day one

This profile is for supportability and saturation characterization, not for changing the semantic contract.

## Cluster Shape

The soak run must use a real `3`-node cluster, not three isolated single-node runs.

Required settings:

- `rf = 3`
- real Raft leadership and failover
- transport admission enabled
- post-auth request backpressure enabled
- tiering enabled so the background sweeper can create real pressure
- observability listeners enabled so metrics and traces can be collected during the run

## Workload Shape

The workload must stay mixed for the full run. Pure-write or pure-read phases do not count as the primary soak result.

Required mix:

- `40%` point writes
- `40%` point reads
- `20%` range/query traffic

Query traffic does not need to be expensive SQL. It does need to exercise the query path and any migration-merge logic the current build supports.

Keyspace rules:

- at least `100k` logical keys
- hot set plus cold tail
- repeated overwrites on a subset of keys so replay/idempotency and retention behavior stay exercised

Value sizing:

- mostly small rows
- at least one medium payload bucket
- enough write volume to force slab rotation and tiering pressure

## Required Background Pressure

The soak must include one of these continuously:

- tiering sweeper pressure
- migration pressure
- retention / compaction pressure

For the current codebase, the default D6 recommendation is:

- small enough slab sizing to guarantee rotation
- L2 enabled
- tiering sweeper active for the entire run

That gives a realistic and already-supported form of background storage pressure without inventing a separate lab-only harness.

## Failover Churn

The soak must include repeated failover churn.

Minimum requirement:

- force leader loss and re-election on a fixed cadence
- restart the displaced leader as a joiner after a bounded delay
- complete at least `6` churn events in a commodity-profile soak

Recommended cadence:

- one churn every `5 minutes`
- restart old leader within `30-60 seconds`

Pass/fail must include whether the cluster returns to healthy service after each churn event.

## Duration

Minimum durations:

- commodity default: `60 minutes`
- premium supported: `30 minutes`

Shorter runs are smoke or benchmark support, not soak evidence.

## Required Measurements

The report must capture at least:

- write latency: `p50`, `p95`, `p99`, max
- read latency: `p50`, `p95`, `p99`, max
- query latency: `p50`, `p95`, `p99`, max
- throughput by operation class
- error counts by status class
- backpressure rejection counts
- transport rejection counts
- pending-finalization backlog max and end-of-run value
- migration-proxy reads and latency if exercised
- `io_uring` queue-depth and error metrics
- leader-change count and failover recovery time per churn event

At least one trace sample set should be preserved for:

- a normal write
- a write during churn
- a read/query under background pressure

## Hard Failure Conditions

Any of these fail the soak immediately:

- silent data loss
- stranded pending-finalization backlog at end of run
- persistent degraded readiness
- repeated failover where service does not recover within the allowed window
- unbounded retry amplification or queue growth
- unexplained `INTERNAL_ERROR` bursts
- data corruption, panic loops, or fence loops

## End-Of-Run Invariants

A run only passes if all of these hold at the end:

- cluster is healthy
- no node is stuck fenced or permanently degraded
- pending-finalization backlog returns to `0`
- request rejection rates return to normal after churn windows
- no outstanding restore/rejoin debt remains
- no invariant-breaking audit gaps are observed

## Output Artifacts

Every D6 run must produce:

- `plans/impl/workstream_D_soak_report.md`
- raw benchmark and soak command lines
- environment summary
- churn timeline
- metric screenshots or exported samples
- anomaly list, even if empty

## Command Contract

The exact launcher may evolve, but the report must record the concrete commands used.

Recommended repo-local launchers:

- `scripts/soak/run_supporting_evidence.sh`
- `scripts/soak/run_commodity_soak.sh`
- `cargo run --example soak_driver -- ...`

At minimum, the D6 evidence package should include:

```bash
cargo test --quiet --test property_suite
cargo test --quiet --test chaos chaos_smoke_catalog
cargo test --quiet --test simulator_ladder
cargo bench --bench io_topology -- --mode both
```

Those commands are not the soak themselves. They are required supporting evidence around the soak run.

## What D6 Does Not Allow

D6 does not allow any of these shortcuts:

- using a single-node run as “soak”
- calling a benchmark a soak
- claiming success without churn
- claiming success without background storage pressure
- claiming success from a run that did not preserve artifacts

## Exit Bar

I would call D6 complete for the current milestone only when:

1. this soak shape has been executed on the commodity default profile
2. the report is filled with measured results, not placeholders
3. the pass/fail verdict is backed by the required metrics and churn timeline
4. any promoted SLO language points back to that report
