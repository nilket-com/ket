# ADR 539 - LSM reference integrity and fail-closed table health

Polariton's first self-hosted telemetry soak found a storage-correctness failure, not a
benchmark result. A table receiving ordinary sampler traffic becomes permanently unreadable
after its second multi-thousand-row ingest cycle. `INSERT` continues to return `OK`, while
`SELECT` returns only Toron's generic `storage unavailable`. The first incident also drove
Wendelon's compaction worker into a roughly 10 Hz warning loop, but the decisive clean
sampler-only reproduction wedges silently: client death is neither necessary nor established as
the cause.

The preserved finding is
`plans/impl/polariton_soak_findings_20260807.md` (`d5e5087d`). Its snapshot records slab files
`2, 6, 7` while live failures refer to slab ids `0` and `4`; after an ordered restart, active
compaction retired slab `7`, created `8` and `9`, and rewrote both durable index files without
detecting the unreachable references. The table remains write-only across restart. This ADR
therefore treats an index or finalization reference to a missing slab as an integrity violation,
not a retryable compaction inconvenience.

## Decision

1. **The invariant is published-reference integrity.** Every record location and secondary-index
   source reference made visible to a table must resolve to an active, sealed, or tiered backing
   slab. A mutation, compaction install, recovery, or read that cannot establish this invariant
   must return a typed integrity failure carrying the affected table identity, reference kind,
   missing slab id, and origin seam. It must not be reduced to a generic unavailable error.

2. **Integrity violations fence the affected table set.** Once detected, both reads and writes
   fail closed with the same typed state for every table holding a published reference into the
   missing slab. Slab co-tenancy alone does not fence a table. No subsequent write may return
   success for a table whose published state cannot be read. This is containment, not repair: the
   ADR does not delete, rebuild, renumber, or silently discard existing durable state.

3. **Compaction failures distinguish retryable work from corruption.** A missing slab/reference
   integrity failure quarantines the affected compaction work and records one durable diagnostic
   plus bounded telemetry; the service must not retry it at every picker tick. Ordinary transient
   compaction failures retain their existing retry behavior. A restart is not a repair mechanism
   and may not clear or hide the fence.

4. **Validate before publication at every mutation seam that can create this state.** Phase 0
   maps the exact second-ingest reproducer through append/finalization, primary-key index update,
   secondary-index backfill, compaction install, and recovery. The implementation adds validation
   at each proven publication seam rather than guessing one write site from the first warning.
   The required outcome is that an acknowledged write cannot make a table unreadable; an
   unvalidated publication is not acknowledged.

5. **Toron preserves the diagnosis.** The Wendelon-to-Toron error surface carries the typed
   table-integrity failure through the SQL path. A user receives an attributable table/reference/
   slab error rather than `TaskFailureKind::Internal { reason: "storage unavailable" }`.

## Grounding and implementation sequence

### Phase 0 — deterministic reproduction and source map

Build a persisted, single-node Wendelon fixture equivalent to the sampler's clean case: a fresh
table receives two ordinary multi-thousand-row batches, with reads after each cycle. Preserve the
write ids, table identity, slab set, index/finalization state, and compaction outcomes. The test
must reproduce either the missing reference or a more precise earlier invariant break; it may not
use client termination as a prerequisite.

Trace the actual state transitions through:

- data-ring table ingest and pending-finalization completion/reconciliation;
- primary-key location publication and durable storage index persistence;
- secondary-index source-slab coverage/backfill;
- LSM compaction output installation and recovery.

The phase also establishes missing-slab-to-table cardinality from durable references and the
inline `__row_table_object_id__` format field. Snapshot inspection suggests one slab can contain
rows from more than one table, but printable column names are a starting hypothesis, not format
proof. The published affected set is precisely the tables with references into the missing slab;
tables sharing a slab without such a reference remain unrelated for containment.

Phase 0 uses the already-observed contagion discriminator rather than rediscovering it: before
restart, a fresh recreated table wedged while the original worker state was live; after restart,
a fresh table required its own second ingest cycle. The trace must decide whether an in-memory
poisoned worker can contaminate a new table, whether the corruption is per-table publication
state, or whether both mechanisms exist.

The phase publishes the first mutation that can make a published reference unresolvable. Until
then, the findings document's suspected `storage_index.redb` and `pending_finalizations.redb`
homes remain evidence targets, not a cause claim.

### Phase 1 — validation, fencing, and attribution

Introduce the typed integrity state, publication-seam validation, affected-table-set fence, bounded
compaction quarantine, and Toron error translation. All new observations are O(1) per failure;
there is no per-row validation scan on a healthy write path. Persist enough diagnostic identity to
correlate the table, reference kind, slab id, and origin seam without exposing a repair command.

### Phase 2 — proof

Run the clean two-cycle reproducer, restart/recovery, and compaction tick controls from a sealed
binary. Use the full sampler scrape only as an end-to-end deployment confirmation after the
deterministic fixture proves the mechanism.

## Acceptance

1. **Clean reproduction is discriminating.** A fresh table's second ordinary large ingest cycle
   either remains readable or produces a typed integrity failure before an `OK` response can make
   it write-only. The fixture records table identity, write ids, slab inventory, and reads after
   each cycle. Client termination is absent.
2. **No acknowledged unreadability.** A fault injected at each proven publication seam cannot
   yield `OK` for a write followed by an unreadable table. It either preserves readable prior
   state or fences the table before acknowledging the invalid state.
3. **Recovery and active compaction validate references.** A durable dangling-reference fixture
   is detected on recovery or before the next affected compaction/read. Restart does not silently
   clear the diagnostic; repeated ticks do not create an unbounded warning loop.
4. **Containment is reference-scoped and stable.** After a violation, reads and writes to every
   table with a published reference into the missing slab return the same typed integrity failure;
   tables with no references into it continue to serve, even if they share another slab with a
   fenced table. Dropping a table does not turn an undetected global dangling reference into a
   silent success.
5. **SQL attribution survives the seam.** A Toron SQL read of the fenced table reports the typed
   table/reference/slab/origin-seam diagnosis, while the old generic `storage unavailable`
   mapping remains available only for genuinely unattributable transport/storage outages.
6. **Deployment confirmation.** From a clean local profile, two full sampler-sized metric ingests
   and reads after each stay readable; the metric table does not become silently write-only.
7. Standard Wendelon, Toron, architecture, format, and full `--no-fail-fast` gates pass from the
   final tree. Existing inherited reds, if any, are named and independently reproduced.

## Guardrails

- Do not attribute the defect to killed clients, pending finalizations, `storage_index.redb`, or
  `pending_finalizations.redb` until Phase 0 locates the mutation seam.
- Do not repair by deleting index files, restarting the process, dropping the affected table, or
  rebuilding slabs silently. Those actions destroy evidence and can turn acknowledged data loss
  into an invisible operational ritual.
- Do not turn every compaction error into a permanent fence. The typed integrity class is distinct
  from existing transient retryable failures.
- Do not use sampler curation as a correctness mitigation. It limits soak blast radius only; the
  full scrape remains the deployment confirmation.

## Forward

On success, the sampler may restore its full metrics scrape and the soak can test long-lived
table health without relying on curated batches. A repair/recovery policy for already-fenced
tables is deliberately deferred: it requires a separately authorized data-loss and operator
semantics decision. The three timing-atlas overhead families remain independent, receipt-backed
performance premises; this storage-integrity cut outranks them while acknowledged rows can be
unreachable.
