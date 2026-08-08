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
`INSERT` into the same table keeps returning `OK`. **This is a silent-data-loss shape,
and it is the operationally sharpest part of the finding**: the client is told the data
landed, and it did land — into a table nothing can read. That is worse than a write
error; every acknowledged row is unreachable and no surface says so. Simultaneously
Wendelon's compaction service enters a ~10 Hz failure loop that never exits:

```
WARN wendelon::storage::lsm_worker: LSM compaction service tick failed error=no slab with id 4 under this manager   (first occurrence, 15:22:52 CDT)
WARN wendelon::storage::lsm_worker: LSM compaction service tick failed error=secondary index backfill source slab 0 not found   (all subsequent ticks)
```

Over 1,000 occurrences in the first minutes; still looping at ~600/min hours later.

**The wedge outlives the table.** `DROP TABLE` on the wedged table succeeds and the
compaction loop continues unchanged — the broken state lives in the LSM worker, not the
catalog. Only a service restart can clear it (not yet performed; the wedged state is
left live in invocation `49c8d8e603f64a9984ad97ac79126641` for inspection).

**Three reproductions — and the third corrects the attribution of the first two.**

1. *Killed client mid-ingest*: ten kills at 10–120 ms, then the table's reads fail.
   Originally read as "client death wedges storage"; see 3 — one sequence, one wedge,
   so the kill was never isolated as the trigger.
2. *Clean sequential bulk ingest*: with the wedge already present, 47 clean 50-row
   batches left a freshly recreated table read-wedged again (5 of 47 batches also
   failing silently).
3. *Sampler-only load* (decisive, post-restart, fresh table): no kills, no bulk
   backfill — a 60-second sampler whose scrape lands ~2,300 rows every 5 minutes. The
   first scrape served reads for four minutes; **the second scrape wedged the table
   permanently, with zero compaction warnings**. Every wedge observed so far, kills
   included, is consistent with one trigger: **a table's second multi-thousand-row
   ingest cycle**. Client death is not required, and neither is the 10 Hz warn loop —
   that loop was an expression of the original incident's in-memory state, not of the
   wedge itself. The silent form is strictly worse: reads gone, writes acknowledged,
   nothing in any log.

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

## Finding 1a — restart outcome (15:35 CDT): the zombie is durable, the loop is latent

The operator snapshotted the wedged state non-destructively first (data dir, config, 20
minutes of journal; on-disk slabs at snapshot time were 2, 6 and 7, while the loop
referenced slabs 4 and 0 — dangling references to slab ids with no backing file,
presumably held in `storage_index.redb` or `pending_finalizations.redb`).

Restart observations, in order:

1. **Shutdown was clean and ordered** (Toron drained before Wendelon), with the 10 Hz
   loop still firing between the stop signal and the drain.
2. **The loop did not resume at startup** — zero compaction warnings in the new
   invocation's first minutes. Recovery neither retried nor reported anything.
3. **The wedged table is still write-only after restart.** Reads on
   `polariton_metric_samples` fail with the same `storage unavailable`.
4. **The small tables survived intact** — process and health row counts after restart
   are consistent with every tick since the soak began, downtime gap aside.
5. **The on-disk state is not inert — and that sharpens the finding.** Pre-restart
   slabs were 2, 6, 7; post-restart they are 2, 6, 8, 9 (slab 7 retired, two created)
   and both index files changed. Named confound: the sampler resumed writing after the
   restart, so the mutation cannot be attributed to recovery specifically. What it
   establishes is stronger than passive carry-forward regardless: **the dangling
   references to slabs 0 and 4 survived a period of active LSM compaction that rewrote
   the very slab set they point into**, with no component noticing that they reference
   ids which have never existed on disk in this profile. A compaction path that retires
   and creates slabs while holding an unresolvable reference is a validation gap, not
   merely a missing repair step. The clean isolation — recovery without ingest — is
   reproducible at any time from the wedge snapshot: restore the profile, start the
   service with the sampler disabled, and diff the slab set and index files.

Post-restart answers so far:

- **Ingest into the zombie table does not resurrect the loop.** A 6.5-minute watch
  spanning the sampler's scheduled scrape ticks recorded zero compaction warnings while
  INSERTs continued to land in the write-only table. The zombie is inert but incurable
  in place: reads never recover, and nothing complains.
- **A fresh table under sampler-only load wedged at its second scrape** (see
  reproduction 3 above): silently, permanently, no warn loop. The question this soak
  was restarted to answer is answered — steady modest ingest alone re-triggers it.

**Consequences for the running soak.** The sampler now defaults to a curated ~70-row
metric subset (`POLARITON_METRICS_NAME_FILTER`; set to `.` for the full scrape, which
currently doubles as an on-demand reproduction). The process-samples table accretes
~4,300 rows/day; if the trigger is row-volume-like it may wedge within a day — left
unmitigated deliberately, because whether and when it wedges measures the threshold's
shape (rows versus bytes versus flush count) with data the team can read right up to
the moment it can't.

## Finding 1c — the old binary re-ran the whole cycle organically, and the loop leaks

With no intervention beyond ordinary sampler load, the pre-fix deployment reproduced the
defect end to end a second time: routine compaction retired slab 8 (itself the replacement
installed after the first restart) while durable references still named it, and at
2026-08-07 15:52:59 the lsm_worker entered the loop again (`no slab with id 8 under this
manager`). Three escalations over the first incident, all verified live:

1. **Total telemetry-table loss.** All three tables — including the 3-row-per-minute
   process and health tables previously reported as safe — now refuse reads with `storage
   unavailable`. "Small steady tables are unaffected" was a statement about exposure time,
   not safety: one missing slab's references can span tables, which is live confirmation of
   the fence-set cardinality the ADR's Decision 2 anticipated.
2. **The retry loop leaks a slab file per tick.** 38,810 slab files exist against ~38,800
   loop warnings — each failed compaction attempt creates a replacement slab, fails against
   the missing reference, abandons the file, and repeats. Unbounded file-count growth as a
   third expression of the same defect (81 MB and climbing at the current tick rate). The
   fixed binary cannot express this: quarantine is checked at compaction entry, so a failed
   retirement is attempted once, not per tick.
3. **Readiness reports 200 throughout.** Health and ready surfaces do not consult table
   integrity, so a deployment with zero readable application tables presents as healthy —
   the same diagnosability inversion as the uncovered-gauge finding, one layer up. Worth a
   follow-on decision: whether readiness should reflect fenced/unreadable table state.

Consequence for the soak: the old-binary deployment has nothing further to teach and cannot
record the ADR 0539 declared measurements (the binary predates both new series, and its
tables no longer serve reads). The clean-profile rebuild and confirmation run — separately
authorized — are the only path to the declared baseline. The wedged profile remains live
evidence for Acceptance 5 until then.

## Finding 2 — observability endpoints are mTLS by design; document the scrape

Plaintext probes of ready (7432) and metrics (7433) hang or fail; both require the
operator identity from the deployment PKI. The sampler and any future scrape must use
`--cert pki/local-operator.crt --key pki/local-operator.key`. Health (7431) is
plaintext `ok` by design. Not a defect — recorded so the next person doesn't diagnose
their curl for an hour.

## Finding 3 — the scan wall-time budget fails whole-table reads, behind the same collapsed error (2026-08-08, clean-profile confirmation run)

**The ADR 0539 confirmation run failed its reads for a reason outside ADR 0539 entirely.**
On the fixed binary, clean profile, single never-sealed slab, zero compaction activity, zero
gaps, zero refusals, zero fences and a silent journal, `SELECT COUNT(*)` on the metrics
table began failing `storage unavailable` at the fifth scrape and never recovered. The
audit stream holds the truth the SQL error discards: `data.range_query` completions with
`status: "query_budget_exceeded"` (21 events), ~250 ms of server time each, ~563 KB of
response prepared then thrown away.

Mechanism: Toron full-table scans pull every row through Wendelon's table-query path, whose
`QueryExecutionBudget` defaults to `max_wall_time: 250ms` (with `max_locations: 4096`).
A few thousand text-heavy rows approach the wall; borderline tables flap with concurrent
load — synthetic probes put one table permanently failing at 3,000 rows while a twin table
passed at 3,100 in a quieter moment. Insert batches also fail silently near the boundary
(a probe table read back 2,950 of 3,100 acknowledged-or-not rows; the client harness
discarded which). `ClientError::Io -> StorageError::Unavailable` collapses the status
string, so the reason never reaches the operator; the 0539 cut carved only the integrity
failure out of that catch-all, and every other Wendelon query status still loses its name.

**Attribution corrections this forces.** Two earlier observations blamed on the LSM defect
were in fact this budget: reproduction 2's "clean bulk ingest re-wedged the recreated
table" (~4.4k rows) and reproduction 3's "sampler-only wedge at the second scrape" (~4.4k
rows) — both crossed the scan-time boundary, and the shared collapsed error made them
indistinguishable from the storage wedge. The LSM defect itself stands on independent
evidence — dangling references to on-disk-absent slabs, the restart-surviving durable
state, the warn loop, the per-tick slab leak, and the manager-level seam test that
reproduces retirement-without-reconciliation with no scan involved — but ADR 0539's
"second multi-thousand-row ingest cycle" trigger framing was contaminated by this defect
and should be read as: the LSM seam is real and proven; the second-cycle *read failure*
signature was the scan budget. One collapsed error string hid two failure classes, and the
first mechanism found absorbed the second's evidence.

**Also established by the run:** the fixed binary's 0539 machinery stayed clean under the
load that killed the old binary — no loop, no leak, no fences — and the declared gap-gauge
measurement could not be recorded because no secondary index was ever created on the fresh
profile (both metric families render seriesless), which leaves "what created the old
profile's secondary indexes" as an open question for the follow-on.

Decisions this puts on the table (not taken here): a scan/read-path cut — streaming or
paginated table scans versus budget policy for full-table reads; widening Acceptance 5's
attribution requirement to every Wendelon query status rather than integrity alone; and
re-running the deployment confirmation after that cut, since its acceptance cannot pass
while any table beyond a few thousand rows is unreadable through the SQL path for reasons
unrelated to storage integrity.

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

Two wedged metric tables have been dropped so far; the current one runs curated and
serves reads. Process and health tables serve normally — until the day the process
table answers the threshold question. The wedge snapshot plus reproduction 3 make the
defect reproducible on demand in about ten minutes. Journal review scopes to the soak with
`journalctl --user -u nilket-local.service --since "2026-08-07 15:10:29"`.
