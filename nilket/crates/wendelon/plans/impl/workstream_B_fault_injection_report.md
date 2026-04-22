# Workstream B Fault-Injection Report

Date: 2026-04-22
Author Role: Lead Architect
Scope: Bifurcated write correctness, replay correctness, and local-persistence distrust behavior

## Summary

Workstream B now has targeted fault seams and regressions for the critical correctness cases that were previously implicit or best-effort.

This is not the full Workstream D evidence package. It is the B-local report for the seams we actually exercised while landing `B1-B6`.

## Fault Seams Exercised

| Seam | Injection Surface | Expected Result | Anchoring Tests |
| --- | --- | --- | --- |
| Leader-local durable append failure | test-only failpoint on the local durable append task | Node trips `LocalPersistenceDistrust`, fences future writes, and surfaces degraded readiness. | `local_persistence_failure_fences_future_writes` `readiness_reporter_surfaces_local_persistence_distrust` `bootstrap_restores_local_persistence_distrust_fence` |
| Pending-intent persist failure after durable quorum | `PendingFinalizationStore::fail_next_upsert_for_test()` | Node does not claim recoverable pending without durable intent. It either commits immediately then fences, or fences immediately if immediate finalization also fails. | `write_commits_and_fences_when_pending_intent_persistence_fails` `write_fences_when_pending_intent_persistence_and_immediate_finalization_fail` |
| Control-branch interruption with durable data already present | restart/replay path after pending row survives | Restart replay resolves the pending work without re-appending payload bytes. | `pending_finalization_replays_after_restart_when_control_branch_failed` |
| Duplicate replay while finalization is still pending | duplicate request on same `write_id` | Response remains retryable and does not cross the bifurcation boundary again. | `duplicate_write_stays_retryable_while_finalization_is_pending` |
| Cache-loss / restart duplicate replay | empty in-memory cache with durable pending row still present | Server finds the durable pending row by `write_id` and replays pending semantics without re-append. | `duplicate_write_replays_persisted_pending_intent_after_cache_loss` |
| Leader turnover with orphan witness on peer | peer witness import before replay / reconciliation | New leader imports the orphan witness and finalizes instead of re-appending. | `leader_recovers_orphan_witnesses_from_peer_before_reconciliation` `fresh_leader_recovers_peer_witness_before_duplicate_write_replays` |
| Fresh-leader sweep outage during cached duplicate replay | unreachable peer introduced into sweep membership | Cached duplicate replay still succeeds; sweep barrier only applies to cache-miss paths. | `cached_duplicate_replays_without_orphan_witness_sweep` |
| Durable quorum accounting under crashy replica behavior | crashy loopback replica fixture | Only `DURABLE` acks count; append-only / lost-on-crash acks do not satisfy quorum. | `write_counts_only_durable_quorum_acks_across_replica_crash_boundary` |

## Verification Run

Commands exercised during the landing:

```bash
cargo test --quiet --lib network::pending_finalizations::tests::
cargo test --quiet --lib network::data_server::tests::duplicate_write_replays_persisted_pending_intent_after_cache_loss
cargo test --quiet --lib network::data_server::tests::pending_finalization_replays_after_restart_when_control_branch_failed
cargo test --quiet --lib
```

Latest local result:

- `cargo test --quiet --lib`
- `296 passed, 0 failed, 2 ignored`

## What This Report Covers

- No recoverable branch without durable pending intent.
- No duplicate replay that silently re-appends after restart or cache loss in the covered matrix.
- No pure-remote quorum acceptance when the leader is itself a replica.
- No cached duplicate replay outage caused by fresh-leader orphan-witness sweep failures.
- Local persistence distrust persists across restart and surfaces via readiness.

## What Remains Outside This Report

- Benchmark deltas for concurrent durability legs.
- Metric and trace coverage required by Workstream D.
- Broader chaos-matrix / simulator-ladder evidence beyond the targeted B regressions.
- A true clean leadership-transfer or abdication story beyond current shutdown-and-fence behavior.

