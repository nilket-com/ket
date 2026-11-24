# ADR 0129 - A tenant applied after a member's last policy refresh is admitted from its applied state

## Status
Proposed (local), 2026-11-24, revision 3. Not a delivery decision.
- **Per Codex:**
  - 01a20ee0: GO for the test-only attribution of DC-285;
  - 01a20ef0: the attribution accepted for the two observed failures; GO for this narrow ADR and its implementation on the delivered `5899a8d2c`, the pair returned for delivery review, no push.
  - 01a20f4c: the move, the bounded one-tenant read, the ordered controls and the restore results accepted; delivery held for one write-accounting race: a later-sampled competing miss replaced an occupied entry's used bytes and so dropped a charge added after the earlier sample. A narrow amendment to that merge, with an ordered control on the real path.
  - 01a20f84: the ordered control accepted as closing the lost charge; delivery held for the opposite error the revision 2 merge introduced: a miss that read an applied state already holding a charged write added that charge a second time, and a write the quota had room for could be refused before the next rebuild. A narrow amendment that avoids the overlapping views instead of keeping a second ledger: the lookup and the placing of the entry are one step.
- **Base:** origin/main `5899a8d2c`.
- **Evidence** (DC285-ATTRIBUTION.md; a test-only probe on the base, 30 runs idle and 30 under load):
  - `backup_restore::an_append_only_tables_duplicates_survive_a_backup_and_restore` failed once in each 30, refused `tenant_unavailable(8)`;
  - the idle failure was the source cluster's read before the export; the loaded one was the restored target's verification read inside `restore`;
  - in both, the refusal was `enforce_read_policy` on the serving member: the tenant was absent from that member's tenant policy cache, refreshed 95.1 ms and 80.5 ms earlier (the interval is 100 ms), while the member's applied state, sampled after the refusal, held the tenant Active.
- **Not shown by the probe:** the order of the apply and the lookup, which it did not observe atomically. The controls here stage and witness that order.
- **Not claimed:** every `tenant_unavailable(8)` (an aggregate leg maps other refusals to the same status); a member that has not yet applied the tenant.
- **Held:** Section 8 measured work and its comparator (pinned to `5899a8d2c`), qualification, delivery.

## Context
- A member admits a request for a tenant from its **tenant policy cache** (`network/data_server.rs` before this ADR): the lifecycle state for reads, and the state, quota and in-flight budget for writes.
- The cache is rebuilt from a clone of the member's applied state, by a reconciler every 100 ms and by a request that finds it at least 100 ms old. Between rebuilds it answers as it stands.
- So a tenant whose creation the member applies after a rebuild is absent from the cache for up to 100 ms, although the member has it. `enforce_read_policy` and `enforce_write_policy` both turn that absence into `TENANT_UNAVAILABLE`.
- `restore` creates the tenant and then reads the restored table back to verify it. That read is refused when it reaches such a member in the window, and nothing on the path waits or retries for this status: a real restore is exposed, not only the test.

## Decision
### Scope
- **Changed:** what a member answers for a tenant that is absent from its policy cache; and what a refresh may replace.
- **Moved, unchanged:** the policy's functions (the rebuild, the reconciler, the freshness check, the lookup, the read and write admissions and the committed-write note) leave `network/data_server.rs` for `network/data_server/tenant_policy.rs`. The cache's two types move with them. The root was at its ADR 077 bound (16,287 lines); it is now 16,120 and the bound ratchets down to that. The server's handle on the cache and the write reservation stay in the root.
- **Not changed:** the hit path (one read of the cache); the refresh interval, the reconciler and its full rebuild; the lifecycle semantics (Active reads and writes; Suspended reads, and refuses writes `TENANT_SUSPENDED`; Creating, Deleting and Deleted refuse); the quota and in-flight checks; the refusal of a tenant the member has not applied; any wire or stored format. No follower catch-up, no wait, no retry in `restore`.

### D1. A miss is answered from the member's applied state
- The shared lookup (`cached_tenant_policy` in `network/data_server/tenant_policy.rs`, used by read and write admission) answers a hit from the cache as before.
- On a miss it reads the **one tenant** from the member's applied state, under the applied state's read lock (`read_state`): no clone of the state, no rebuild of the cache.
- **Absent there:** the tenant is unknown to this member. The answer is the same refusal as today, and **nothing is kept**: no negative entry, and the cache is not touched, so a stream of distinct unknown ids costs one bounded lookup each and leaves the cache as it was.
- **Present there, and the cache still without it:** its entry is placed **under that same read lock**, from what the applied state holds at that moment (its state, quota, used bytes, replication factor and the applied log index), with a mean write size of 1 as a rebuild gives a tenant it sees for the first time. It answers this call and later ones.
- **Present there, and the cache now with it** (another miss placed it meanwhile): that entry is the answer, as on a hit, with whatever this member has charged to it since. It is not rewritten.
- **Lock order:** the applied state's read lock, then the cache's synchronous lock for the one map operation. Nothing takes them in the other order: a rebuild releases the applied state before it takes the cache's lock, and the committed-write note takes the cache's lock alone. The cache's lock is never held across an await.

### D2. One view per entry, and no earlier view replaces it
- Because the read and the placing are one step under the applied state's lock, **two misses never hold different views of one tenant**. An entry is placed once, from the applied bytes at that moment, and afterwards only `note_committed_write` adds to it.
  - A write charged after the entry was placed is in the entry once (the note) and, when it is applied, in the applied bytes once; no later miss reads those applied bytes into the entry, so nothing is counted twice.
  - A miss that arrives after a charged write finds the entry and uses it, so the charge is not dropped.
- Each entry records the applied log index it was placed at.
- **A rebuild** takes its snapshot before it takes the cache's lock, so a miss can place a tenant from a later applied state meanwhile. The rebuild keeps any entry placed at a later index than its snapshot, untouched; every other entry is replaced or dropped as before. A rebuild remains the authoritative reconciliation: an entry it replaces starts again from the applied bytes, so usage corrections arrive as before.
- So `note_committed_write`, which adds a committed write's bytes to the tenant's entry and returns when there is none, finds the entry for a write admitted on a miss, and a rebuild already under way cannot remove it.
- An entry found by a later miss can be older than the applied state by up to the refresh interval in its state and quota, exactly as a hit can; the rebuild brings a later lifecycle or quota decision as it does today.

### D3. What stays refused
- A tenant the member has not applied: the member cannot admit what it does not have. A request routed to a lagging member is refused as before, and the caller's routing decides what follows.
- A tenant whose applied state does not allow the request.

## Rationale
- The cache is a copy of the applied state; its absence of a tenant is evidence only up to its last rebuild. The member holds the authoritative answer and can read one record of it cheaply.
- A shorter interval or a wait in `restore` would narrow the window and leave it. A test precondition would not cover `restore`'s own read.
- Recording the index, not a time, orders a miss against a rebuild exactly: both read the same applied state at a known position.

## Controls
On a single-node store with the reconciler stopped, so the cache changes only by a request or by the test (`network/data_server/tests/tenant_policy_miss/tests.rs`).
- **The order, staged and witnessed** for every control below: the cache is refreshed; a new tenant is created; its creation is read back as applied on this member; the cache is still that refresh, without the tenant, with more than half the interval left. A slow machine restages with a new tenant.
- **Read** (the point-read handler): the request is then issued. Asserted after it: the cache is still the same refresh, so nothing rebuilt it; the status is not `TENANT_UNAVAILABLE`; the entry is now cached. **On the base this control fails** with `TENANT_UNAVAILABLE`.
- **Write** (`table_write`): the write is `OK` (on the base, refused), and, when no rebuild has intervened, the entry carries the write's anticipated bytes and a mean write size above 1.
- **Unknown ids:** 20 rounds of 50 reads for 50 distinct tenants the member has not applied: each refused `TENANT_UNAVAILABLE`; no entry kept; the cache's size unchanged; and at least one round in which the cache was not rebuilt at all.
- **Suspended:** a tenant created and suspended inside the window reads (not `TENANT_UNAVAILABLE`) and refuses its write `TENANT_SUSPENDED`.
- **Two requests around one miss, ordered on the real path** (a test-only hold on a request that has just missed, before it reads the applied state; one resume handle per held request, in arrival order). Each is judged only when no refresh happened throughout:
  - **a miss that lands after a charged write:** a write and then, after an unrelated apply, a read both miss and are held; the write resumes, is admitted, commits and is charged (witnessed in the cache and in the applied state); the read resumes. The used bytes and the mean write size are still the write's, and with a quota one write fills the next write is refused `QUOTA_EXCEEDED`. **On revision 1 the used bytes returned to 0 and the next write was admitted.**
  - **a miss that reads an already-applied write:** a read misses and is held; a write misses, is admitted, commits and is charged, in the cache and in the applied state; the read resumes and reads that applied state. The used bytes are still the write's, once; with a quota that has room for a second write the second is admitted and the third is refused `QUOTA_EXCEEDED`. **On revision 2 the used bytes doubled and the second write was refused.**
- **An earlier view does not replace a later entry:** an admitted entry marked as read at a later index than any snapshot, with a charge on it, survives a rebuild unchanged; at its own index the rebuild replaces it with the applied bytes and keeps its mean write size.
- **Integration:** the backup and restore workflow test, unmodified, in the existing lanes.

### Not exercised
- A miss racing a rebuild on real timing (the rule is exercised by placing the index).
- A member that has not applied the tenant because it lags (one member here).
- The aggregate path's own mapping to `tenant_unavailable`.

## Consequences (The "Negatives")
- A request for a tenant the member does not have now takes the applied state's read lock once, where it took none. The lookup is one map read; it is not rate limited.
- Each cache entry is 16 bytes larger.
- A miss holds the applied state's read lock while it takes the cache's lock for one map operation.
- An entry admitted on a miss starts without the mean write size a long-lived entry would have, until writes or a rebuild set it.
- A lagging member still refuses a tenant it has not applied.

## Open Questions
- Whether DC-285's rate falls to zero with this, or other causes of the same status remain: to be measured on the pair.
- Whether the aggregate path should stop reporting other refusals as `tenant_unavailable`. Separate.
