# Polariton soak findings — 2026-08-07, first hours

The soak's instrumentation is the dogfood itself: a sampler
(`packaging/ubuntu/polariton-telemetry-sampler.sh`, unit
`polariton-telemetry.service`) lands the deployment's own telemetry in Toron —
`polariton_process_samples`, `polariton_health_samples`, `polariton_metric_samples` —
via the production SQL surface on UDP 7742. No CSVs; review happens with SELECT. The
sampler found a real storage defect within its first hour.

## Finding 1 — LSM compaction service wedges permanently; tables become write-only

**Symptom.** `SELECT` on an ingest-heavy table fails with
`Toron Internal: task failure: Internal { reason: "storage unavailable" }` while
`INSERT` into the same table keeps returning `OK`. Simultaneously Wendelon's
compaction service enters a ~10 Hz failure loop that never exits:

```
WARN wendelon::storage::lsm_worker: LSM compaction service tick failed error=no slab with id 4 under this manager   (first occurrence, 15:22:52 CDT)
WARN wendelon::storage::lsm_worker: LSM compaction service tick failed error=secondary index backfill source slab 0 not found   (all subsequent ticks)
```

Over 1,000 occurrences in the first minutes; still looping at ~600/min hours later.

**The wedge outlives the table.** `DROP TABLE` on the wedged table succeeds and the
compaction loop continues unchanged — the broken state lives in the LSM worker, not the
catalog. Only a service restart can clear it (not yet performed; the wedged state is
left live in invocation `49c8d8e603f64a9984ad97ac79126641` for inspection).

**Two reproductions, one sufficient and one merely cumulative:**

1. *Killed client mid-ingest* (deterministic, 10/10): fire a ~50-row `INSERT` via
   `nilket toron sql` and `timeout -s KILL` the client at 10–120 ms, ten times. The
   table's reads fail from then on. A client process dying mid-request must not be able
   to wedge server-side storage.
2. *Clean sequential bulk ingest*: with the compaction loop already broken, 47
   sequential 50-row INSERT batches (single client, no kills, every exit checked) left
   the freshly recreated table read-wedged again, with 5 of 47 batches failing
   silently. Once the LSM worker is poisoned, any table whose ingest crosses into the
   compaction/backfill path appears to follow.

**Blast radius.** Small steady tables (3-row process samples, 2-row health samples per
minute) keep serving reads throughout. The failure needs the compaction path.

**Also visible in the dogfood data itself**: Wendelon's cpu_secs in
`polariton_process_samples` climbs ~6× faster than Toron's while the 10 Hz loop runs,
and journald absorbs ~600 warn lines/minute — the wedge taxes the whole soak.

**Cross-ring shape.** The trigger crosses the seam (Toron SQL ingest → Wendelon LSM),
the broken state and the fix are Wendelon's (slab lifecycle under interrupted ingest;
compaction tick that retries forever on a missing slab instead of quarantining), and
the read-path error surfaced by Toron ("storage unavailable") names no table, slab or
cause. Per convention this lands in a Toron-side grounding arc; the ADR that picks it
up should decide (a) crash-safety of ingest against client death, (b) compaction
failure quarantine versus infinite retry, (c) error attribution on the read path.

## Finding 2 — observability endpoints are mTLS by design; document the scrape

Plaintext probes of ready (7432) and metrics (7433) hang or fail; both require the
operator identity from the deployment PKI. The sampler and any future scrape must use
`--cert pki/local-operator.crt --key pki/local-operator.key`. Health (7431) is
plaintext `ok` by design. Not a defect — recorded so the next person doesn't diagnose
their curl for an hour.

## Soak viewport — example queries

```sql
-- RSS trend per process (leak review)
SELECT process, sampled_at, rss_kb FROM polariton_process_samples ORDER BY process, sampled_at;

-- health/ready latency over the soak
SELECT endpoint, sampled_at, latency_ms FROM polariton_health_samples ORDER BY sampled_at;

-- raft progress between any two scrapes
SELECT sampled_at, value FROM polariton_metric_samples
WHERE name = 'wendelon_raft_entries_applied_total' ORDER BY sampled_at;
```

The metrics table is currently read-wedged by Finding 1 (its rows continue to land);
process and health tables serve normally. Journal review scopes to the soak with
`journalctl --user -u nilket-local.service --since "2026-08-07 15:10:29"`.
