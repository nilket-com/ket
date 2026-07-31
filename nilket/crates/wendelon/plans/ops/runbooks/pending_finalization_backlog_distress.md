# Pending-Finalization Backlog Distress

Date: 2026-04-23
Owner: `H6`
Depends On: `B` `D`

## When To Use This

Use this runbook when persisted pending-finalization work is building up faster
than the cluster is draining it, or when clients are repeatedly observing
`FINALIZATION_PENDING`.

Typical symptoms:

- `wendelon_data_ring_pending_finalization_backlog` rises and stays elevated
- `wendelon_data_ring_write_outcome_total{outcome="finalization_pending"}`
  rises above the local baseline
- duplicate retries continue resolving through the pending path instead of
  converging quickly

## First Look

Check these signals first:

- `wendelon_data_ring_pending_finalization_backlog`
- `rate(wendelon_data_ring_pending_finalization_replay_total{path="reconciler"}[5m])`
- `rate(wendelon_data_ring_pending_finalization_replay_total{path="duplicate_replay"}[5m])`
- `rate(wendelon_data_ring_pending_finalization_replay_total{path="orphan_witness_import"}[5m])`
- `rate(wendelon_data_ring_write_outcome_total{outcome="finalization_pending"}[5m])`
- `wendelon_data_ring_pending_finalization_replay_latency_seconds`
- `/ready` on the current leader and any recently fenced nodes

Also inspect the trace span:

- `data_ring.pending_finalization.reconcile`

## Interpretation

The metric split matters:

- `path="reconciler"`
  - background reconciler is actively draining work
- `path="duplicate_replay"`
  - clients are retrying into pending records
- `path="orphan_witness_import"`
  - leader-change witness recovery is actively importing orphaned records

The backlog itself is the durable queue length. A non-zero backlog is not
automatically fatal. A backlog that rises without replay progress is the real
distress condition.

## Decision Tree

If backlog is non-zero but steadily falling:

- keep monitoring
- do not force leadership churn

If backlog is rising and `reconciler` replay is near zero:

- identify the current leader
- verify the leader is stable and healthy
- check whether any node is fenced or degraded with
  `degraded_reason = "LocalPersistenceDistrust"`

If `duplicate_replay` is high:

- clients are spending time on retry/replay rather than converging directly
- keep the current leader stable while the backlog drains

If `orphan_witness_import` is high:

- the cluster is likely recovering from recent leadership turnover
- avoid introducing further topology churn unless the current leader is
  actually unhealthy

If any node is fenced with `LocalPersistenceDistrust`:

- treat it as a storage-suspect node
- remove it from client traffic
- prefer replacement or rebuild over in-place trust restoration

## Mitigation Actions

Ranked by reversibility:

1. stabilize leadership and peer reachability
2. stop unnecessary restarts or rolling topology churn
3. isolate fenced or storage-suspect nodes from client traffic
4. if the active leader is the suspect node, replace it through the supported
   node-replacement flow after membership is safe

Do not:

- delete `pending_finalizations.redb` by hand
- clear pending records manually
- restart leaders repeatedly while the backlog is draining

## Success Criteria

- `wendelon_data_ring_pending_finalization_backlog` trends back to `0`
- `FINALIZATION_PENDING` write outcomes return to baseline
- replay latency and replay rates return to normal background levels

## Escalate When

Escalate if:

- backlog keeps growing for more than one reconciler interval window under
  otherwise stable leadership
- replay activity is near zero while backlog is non-zero
- clients continue to see `FINALIZATION_PENDING` during a stable-leader period
- the backlog condition coincides with `LocalPersistenceDistrust` or repeated
  failed witness imports

## References

- `plans/ops/runbooks/degraded_fenced_leader.md`
- `plans/ops/observability.md`
