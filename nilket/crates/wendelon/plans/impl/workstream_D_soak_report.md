# Workstream D Soak Report

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `D6`
Spec Source: `plans/impl/workstream_D_soak_spec.md`

## Status

Current status: `template present, authoritative run pending`

This file exists so the evidence package has a canonical landing zone before the real run happens.

Today it is intentionally not claiming a passed soak.

## Environment

Fill this in for the authoritative run:

- profile: `commodity default` or `premium supported`
- node count:
- CPU / RAM per node:
- storage type:
- kernel / distro:
- Wendelon commit:
- config deltas from default:
- L2 backend:

## Workload

- duration:
- key count:
- write/read/query mix:
- value-size distribution:
- background pressure mode:
- churn cadence:

## Commands

Record the exact commands used for the run.

Required supporting commands:

```bash
cargo test --quiet --test property_suite
cargo test --quiet --test chaos chaos_smoke_catalog
cargo test --quiet --test simulator_ladder
cargo bench --bench io_topology -- --mode both
```

Soak launcher:

```bash
scripts/soak/run_commodity_soak.sh
```

## Results

Fill these with measured values:

### Throughput

- writes/sec:
- reads/sec:
- queries/sec:

### Latency

- write `p50/p95/p99/max`:
- read `p50/p95/p99/max`:
- query `p50/p95/p99/max`:

### Churn

- number of failover events:
- worst recovery time:
- average recovery time:

### Pressure

- max pending-finalization backlog:
- end-of-run pending-finalization backlog:
- max `io_uring` submission backlog:
- max `io_uring` completion backlog:
- backpressure rejections:
- transport rejections:

## Churn Timeline

Document each churn event:

| Event | Timestamp | Action | New Leader | Recovery Time | Notes |
|---|---|---|---|---|---|
| example | pending | pending | pending | pending | pending |

## Anomalies

List every anomaly observed during the run, even if the verdict remains pass.

- pending

## Verdict

Current verdict: `not yet run`

Allowed verdicts once the run exists:

- `pass`
- `pass with follow-ups`
- `fail`

## Promotion Decision

Current decision: `no SLO promotion from this template`

Promotion is allowed only after the measured run satisfies the D6 spec and the verdict is not `fail`.
