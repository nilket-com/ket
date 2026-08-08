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

   Ruled amendments, post seam proof:

   - **The skip is unrepresentable.** `reconcile_secondary_index_segments_after_compaction`
     takes a required `SecondaryIndexCompactionContext` — never an `Option` whose absence
     silently reports success — and returns a proof:
     `SecondaryIndexReconciliation::{Reconciled, NothingToReconcile}` or an error. "Nothing to
     reconcile" is a proven conclusion from the catalogs and the durable segment registry, so
     it can never be confused with "skipped". Slab retirement on the compaction paths consumes
     this proof; on failure the slab is quarantined and its file preserved.
   - **A coverage gap refuses lookups; it never under-reports — and the gate is the
     invariant, not the failure record.** The non-fatal rebuild exit was justified as
     "degraded and backfillable"; scrutiny refuted the premise twice over. First, uncovered
     slabs were excluded from backfill candidacy, making the state permanent while the
     coverage-blind lookup returned success that silently omitted rows — a strictly worse
     shape than the unreadable table this ADR opened on. Second, the first fix gated refusal
     on the in-memory `uncovered_slabs` record, which empties on restart: the silent wrong
     answer resurrected for every process that never observed the failure. This arc found
     the same shape three times — a guard bound to a failure record rather than to the
     invariant — so the ruling binds the gate to the invariant: `table_lookup_by_index`
     refuses while any historical source slab lacks a durable segment, derived from the
     segment registry (rebuilt from durable rows at every open) against the live slab
     inventory. It survives restart by construction and needs no new durable field. There is
     deliberately no exemption for an open active buffer whose slab has sealed: those rows
     are index-invisible until the buffer seals, and answering through that window is the
     silently-incomplete success the gate exists to refuse.

     Retry is budget-bounded, never class-gated: every backfill failure keeps candidacy
     until a fixed attempt budget spends, then lands permanent — a structural failure burns
     its budget cheaply, and no transient-versus-structural misclassification can strand a
     transient error, which is where the expensive mistake would live. Two former marking
     sites are removed from the failure taxonomy entirely: segment deletion is lifecycle
     bookkeeping (the registry removal itself re-opens the gap and candidacy), and the
     still-active seal race is ordinary scheduling noise. The full-scan fallback stays
     ruled out as silent degradation of query cost. Controls:
     `secondary_index_lookup_refuses_while_a_source_slab_lacks_a_segment` (both silent
     shapes, no failure record involved),
     `secondary_index_lookup_still_refuses_after_reopen` (the restart hole, closed by
     construction, plus heal-and-survive), and
     `backfill_candidacy_survives_failures_until_the_budget_spends`.
   - **Positive proof replaces the negative scan on the compaction paths.** The
     snapshot-scanning `can_retire_local_slab` guard remains only on the sweep paths, where no
     reconciliation proof exists; the compaction paths retire on proof, which removes the
     O(candidates x index) scan from the hot route instead of optimizing it.
   - **Scope.** Both with-replacement compaction paths are fixed — `compact_sealed_slab_records`
     and `recluster_one_sealed_slab` (which previously retired without any reconciliation call at
     all). The tiered retirement paths (`tiered_io.rs`) are **deferred by name**: they retire
     slab files under the tiered/L2 lifecycle, a different mechanism with its own invariants,
     and are unguarded today. The follow-on must extend either proof-carrying reconciliation or
     the sweep scan to tiered retirement; until then finding 1 of the guard review is contained,
     not closed.

5. **Toron preserves the diagnosis.** The Wendelon-to-Toron error surface carries the typed
   table-integrity failure through the SQL path. A user receives an attributable table/reference/
   slab error rather than `TaskFailureKind::Internal { reason: "storage unavailable" }`.

## Grounding and implementation sequence

### Phase 0 — deterministic reproduction and source map

**Discharged.** The seam:`compact_sealed_slab_records` unlinked the retiring slab
unconditionally after a reconciliation whose result was discarded and whose context was
optional — three of four production-reachable callers passed `None`, making reconciliation a
silent no-op (`recluster_one_sealed_slab` never reconciled at all). Proven by
`prune_compaction_must_not_orphan_secondary_index_sources`, authored red with production calls
only — ordinary writes, seal, production backfill, routine retention prune — failing with the
live wedge's verbatim error, including the slab id: fresh profiles begin at slab 0. The
two-cycle sampler-sized fixture is a **smoke check, not evidence**: it is green on the wedging
base because point lookups bypass secondary coverage; promoting it requires a node-level typed
table-query executor in the fixture harness, which does not exist yet.

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
