# ADR 055 Cached-Reference Audit

Status: Phase 0 baseline complete.

Purpose: validate the no-LSM-relocation stance before the
compaction substrate starts retiring input slabs. The audit
enumerates code paths that cache `RecordLocation` and dereference it
outside the immediate lookup call.

Classification key:

- **P**: protected by a manifest-snapshot slab pin before ADR 055 can
  retire the referenced slab.
- **R**: covered by existing non-LSM relocation machinery in
  `src/storage/manager.rs`.
- **S**: sidecar replace-by-new; cached references retire with the
  sidecar.
- **U**: uncovered. Any such site blocks Phase 1.

## Findings

| Site | Classification | Evidence | ADR 055 requirement |
| --- | --- | --- | --- |
| `src/storage/manager.rs` `relocated_reads: DashMap<RecordLocation, RecordLocation>` plus `relocate_read_location` | **R** | Existing retention rewrite paths populate and dereference this map for non-LSM rewrites. | Leave untouched. LSM code must not write new entries or add an LSM relocation table. |
| `src/storage/core.rs` per-slab and manager `key_index` version chains | **P** | These are authoritative lookup indexes, not detached caches. ADR 055 reader integration must acquire pins from the manifest snapshot before dereferencing selected slabs. | Phase 3 wires ordinary read/query iterator opens to snapshot pins. |
| `src/storage/secondary_index.rs` and `src/storage/secondary_index_segment.rs` sidecar `RecordLocation` entries | **S** | ADR 052 sidecars are per source slab. ADR 055 output sidecars are rebuilt out-of-place and old sidecars retire with old slabs. | Compaction must rebuild output sidecars and never mutate input sidecars in place. |
| `src/storage/secondary_index_backfill.rs` candidate locations | **P/S** | Backfill operates against a source slab lease and persists sidecar coverage for that source slab. | Phase 5 compaction takes the write side of the source-slab lease before replacing sidecar coverage. |
| `src/network/data_server.rs` read/query response rows containing `RecordLocation` | **P** | Query/read planning materializes locations before reading records or streaming response chunks. | Phase 3 extends remote read/query response handlers to hold slab pins for the response stream lifetime. |
| `src/network/data_server.rs` CDC catchup rows materialized from storage scans | **P** | CDC catchup reads bounded storage ranges before live-tail handoff. | Phase 3/8 CDC catchup opens a manifest snapshot and holds pins until handoff. Live-tail does not pin slabs. |
| `src/storage/columnar.rs` column fetch probes using `RecordLocation` | **P/R** | Column fetch is a dereference helper for selected storage records; retained rewrite readers already use manager relocation where applicable. | Callers that select slabs from an LSM manifest snapshot must hold pins across the probe/fetch. |
| `src/storage/pk_delete.rs` latest-as-of locations | **P/R** | PK delete computes latest visible locations through storage lookup and existing retention rewrite coverage. | ADR 055 delete admission continues to dereference through normal storage manager read paths while read-side pins protect compacted slabs. |
| `src/storage/index.rs` persistent restart rows | **R** | Persistent rows mirror local storage metadata and participate in existing manager rebuild/relocation behavior. | ADR 055 manifest rebuild does not reinterpret these rows as LSM relocation state. |
| Catalog and admission-side code | **P** | No durable catalog/admission structure currently stores a detached `RecordLocation` for later lookup outside storage manager read paths. | Any future site must either acquire a manifest pin or reopen this audit before using LSM-compacted slabs. |

## Blocker Status

No **U** sites are present in the current baseline.

Phase 3 remains responsible for turning the **P** classifications into
runtime enforcement by acquiring manifest-snapshot pins for ordinary
scan/query opens, CDC catchup opens, and remote response stream
lifetimes.
