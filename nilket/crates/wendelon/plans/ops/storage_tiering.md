# Wendelon Tiered Storage

> **Status note.** This document has been rewritten to reflect the Wave 4 ADRs. The prior revision's positions are updated or superseded as follows:
>
> | Prior §  | Prior position | Superseding / amending ADR(s) | Outcome |
> |----------|----------------|-------------------------------|---------|
> | §1 Storage Engine Trait | `LocalSlabStore` + `RemoteObjectStore` ad-hoc shape | [ADR 022](../adr/022_l2_slab_format_footer_indexed_range_reads.md) | Replaced: canonical `ObjectStore` trait (one coordinated break) covers `head`, `get`, `get_range`, `put`, `put_multipart`, `delete`, `list`, `copy`. |
> | §2 Slab Geometry (1 GB slabs, 16 MB range-read chunks) | Slab = 1 GB, logical chunks = 16 MB for range reads | [ADR 022](../adr/022_l2_slab_format_footer_indexed_range_reads.md), [ADR 020](../adr/020_l2_envelope_encryption_and_dek_lifecycle.md) | Superseded: range reads use the footer-embedded offset index (per-record byte offsets into the plaintext stream); the 64 KiB unit inside the slab is the encryption-chunk size (ADR 020), not a range-read unit. Slab size is operator-tunable in `[~16 MiB, 32 GiB]` with `slab_max_bytes = 64 MiB` pilot default. |
> | §3 Slab Manifest (`#[repr(C)]` POD) | On-NVMe manifest with state enum `{Active, Sealed, Uploading, Stubbed}` | [ADR 023](../adr/023_l2_upload_lifecycle.md) | Superseded: the **authoritative** slab lifecycle lives in the Raft state machine's `tiered_slabs` table (`Pending → Uploaded → Committed → Retired → Reclaimed`, plus `Aborted → Reclaimed`). The on-NVMe manifest survives as a local *summary* that mirrors authoritative fields and records L1-only metadata (temperature, local file descriptor state, per-node L1 presence). Per-node L1 purge is **not** a replicated state — it is a local decision each replica makes after the slab reaches `Committed`. |
> | §4 Sweeper Policies 1-4 | Sealing / Eviction-Upload / Promotion / E_safe Compaction | [ADR 023](../adr/023_l2_upload_lifecycle.md), [ADR 024](../adr/024_l2_orphan_cleanup_remote_reclaim.md) | Retained in shape; Eviction/Upload now drives the ADR 023 lifecycle transitions and consumes ADR 022's slab format. Reclamation (previously implicit inside "E_safe Compaction") is pulled out into the ADR 024 four-track reclaim sweeper. |
>
> The rewrite below is the canonical operational guidance. [ADR 006](../adr/006_tiered_storage_lazy_migration.md) remains the framing ADR (L0 → L1 → L2, lazy migration); Wave 4 ADRs ([022](../adr/022_l2_slab_format_footer_indexed_range_reads.md), [023](../adr/023_l2_upload_lifecycle.md), [024](../adr/024_l2_orphan_cleanup_remote_reclaim.md)) are the authoritative sources for on-object layout, upload lifecycle, and reclaim.

Wendelon uses a three-tier storage hierarchy (L0 DRAM, L1 NVMe, L2 Object Storage) driven by an append-only slab allocator and a background Sweeper. Historical data falls out of NVMe onto S3-class object storage; topology changes do **not** trigger eager rebalancing (ADR 006); confidentiality is enforced by client-side envelope encryption (ADR 020).

---

## 1. Object-Store Trait Surface

All L2 I/O goes through the canonical `ObjectStore` trait frozen in [ADR 022 §8](../adr/022_l2_slab_format_footer_indexed_range_reads.md). The trait covers three use-case clusters:

**Object I/O.**
- `head(key)` — metadata-only fetch; used by orphan-GC triage.
- `get(key)` / `get_range(key, range)` — whole-object and byte-range fetches; range fetches power the tail-read protocol (§2.3).
- `put(key, body, condition)` — single-PUT path with If-None-Match / If-Match semantics.
- `delete(key)` — reclaim path; gated through the `ReclaimingObjectStore` façade (ADR 024 decision §7).
- `copy(src, dst, condition)` — intra-bucket copy (staging, future per-tenant bucket migration).

**Object listing.**
- `list(prefix, continuation)` — paginated listing for full-bucket orphan-GC (ADR 024 Track D). Not to be confused with `list_multipart_uploads` below.

**Multipart (new, resume, enumerate, abort).**
- `put_multipart(key)` — create a new multipart upload; returns a handle.
- `resume_multipart(key, upload_id)` — re-hydrate a handle for an upload created by a prior process or before a crash (ADR 023 §4 recovery step 2).
- `list_multipart_uploads(prefix, continuation)` — enumerate in-progress uploads (ADR 023 §4 disk-loss fallback; ADR 024 Track A orphan-multipart scan).
- `list_parts(key, upload_id, continuation)` — enumerate accepted parts for an in-progress upload (ADR 023 §4 ledger reconciliation).
- `abort_multipart(key, upload_id)` — free-function abort by id (ADR 024 Track A); equivalent to `MultipartUpload::abort` on a live handle.

Production implementations target AWS S3, GCS, and Azure Blob. The test substrate is [rustfs (ADR 014)](../adr/014_rustfs_test_backend.md); deterministic fault injection uses the mocked `ObjectStore` from [ADR 028](../adr/028_deterministic_simulator_and_asyncfileops_abstraction.md) (planned).

L1 (NVMe) I/O goes through the `AsyncFileOps` abstraction (ADR 028 planned) wrapping io_uring + `O_DIRECT` (ADR 009). The slab's byte layout is identical on L1 and L2 — L1 writes the slab; the L2 uploader streams the same bytes as one object.

---

## 2. Slab Geometry and Range Reads

### 2.1 Slab Size

`slab_max_bytes` is a cluster-global tunable with a pilot default of **64 MiB** and a hard ceiling of **32 GiB** (ADR 020 §2, AES-GCM per-key data limit). Typical production ranges are 64 MiB to 1 GiB. Smaller slabs (<16 MiB) are possible but inefficient at the object store's per-request overhead. Operators tune by measurement, not folklore.

### 2.2 Slab Layout (frozen in ADR 022)

```
magic_head (8B) | slab_header (64B) | encrypted_chunks (P bytes)
  | offset_index (I bytes) | slab_footer (256B)
  | footer_length (4B) | index_length (4B) | magic_tail (8B)
```

- **Encrypted chunks** are 64 KiB plaintext chunks with 16-byte AEAD tags, padded as a stream to 4 KiB for O_DIRECT (ADR 020 §2).
- **Offset index** is a densely packed array of 32-byte records (key_hash[16] | byte_offset[8] | byte_length[4] | flags[4]) addressing the *plaintext* byte stream. See ADR 022 §4.
- **Slab footer** carries `slab_format_version`, `cipher_id`, `slab_id`, `tenant_id`, `chunk_count`, `record_count`, `tenant_kek_id`, `wrapped_slab_dek`, and a footer HMAC keyed by the Tenant KEK. See ADR 022 §5 and ADR 020 §6.
- **Tail anchor** (16 bytes: footer_length || index_length || magic_tail) is what the reader fetches first.

### 2.3 Range-Read Protocol

Cold point read on L2 completes in ≤ 2 round trips (ADR 022 §6):

1. **Metadata RTT.** `get_range(slab, size - 64 KiB, size)`. The 64 KiB window covers the tail anchor, footer, and — for slabs with up to ~2000 records — the offset index. Slabs with larger indexes pay a second metadata RTT.
2. **Record RTT.** With the record's `byte_offset` and `byte_length` from the offset index, the reader computes the encryption-chunk range needed, issues `get_range` for those chunks, decrypts under the slab DEK (ADR 020 §2), and slices out the record.

Warm reads (offset index cached, DEK cached) complete in one RTT.

### 2.4 Logical Record Index

The **local** persistent index (redb per ADR 017) maps `(tenant_id, key_hash) → (slab_id, slab_state_hint)`. The `byte_offset` / `byte_length` are *not* duplicated in the local index — they live in the slab's offset index where they belong. The local index is the "which slab holds this key?" answer; the slab's own offset index is the "where in the slab?" answer.

---

## 3. Slab Lifecycle and State

### 3.1 Authoritative Lifecycle (Raft)

The `tiered_slabs` table in the Raft state machine is the authoritative record. Keyed by `slab_id`, each record holds: `slab_id`, `tenant_id`, `state ∈ {Pending, Uploaded, Committed, Retired, Reclaimed, Aborted}`, `target_tier`, `slab_size_bytes`, `wrapped_slab_dek_ref`, `upload_leader_node_id`, `attempt_count`, `created_hlc`, and (once uploaded) `provider_etag` and `provider_uploaded_hlc`. `Retired` adds `retired_hlc` and `retire_reason` ∈ {`compaction_retired`, `retention_expired`, `migration_old_copy`, `tenant_deleted`}. `Aborted` adds `aborted_hlc` and `abort_reason` ∈ {`provider_error_terminal`, `retry_exhausted`, `operator_abort`, `leader_transition`, `orphan_detected`}. See [ADR 023 §1](../adr/023_l2_upload_lifecycle.md) for the full state machine and [ADR 024 decision §2](../adr/024_l2_orphan_cleanup_remote_reclaim.md) for the reclaim transitions.

**Local purge is not a replicated state.** After a slab reaches `Committed`, each replica independently decides when to unlink its own L1 copy based on its own NVMe pressure. Per-replica L1 presence is tracked in the node's local redb index (§3.2) and (optionally) gossiped for reader-routing hints; it is **not** recorded in the `tiered_slabs` state.

Lifecycle states replace the prior doc's `{Active, Sealed, Uploading, Stubbed}` enum. The mapping for readers of the old doc:

| Old state | New authoritative position |
|-----------|---------------------------|
| `Active` | L1-only; no `tiered_slabs` record yet (the slab is still being written) |
| `Sealed` | L1-only; no `tiered_slabs` record yet OR `Pending` (if selected for upload) |
| `Uploading` | `Pending` in `tiered_slabs` (with a local execution-ledger entry tracking the multipart upload) |
| `Stubbed` | `Committed`, with each replica's L1 copy purged *locally* on its own cadence. "All replicas have purged L1" is observable by gossip or by absence of local entries, not by a replicated state bit. |

### 3.2 Node-Local State (Non-Authoritative)

The node-local view of a slab is stored in two places:

- **The local execution ledger** (redb, per ADR 023 §2) tracks multipart UploadId, temp-file paths, per-part ETags, and retry counters for uploads this node is driving. Non-authoritative; recoverable from the provider via `ObjectStore::list_multipart_uploads` if lost.
- **The on-NVMe slab manifest** (kept for L1 file-scan tooling and bringup) is a local mirror of the authoritative fields needed to classify a slab file on disk: `slab_id`, `tenant_id`, `slab_format_version`, `created_hlc`, `chunk_size_bytes`, `chunk_count`, `temperature` (L1-only read counter, not replicated), `local_l1_present` (boolean: this replica currently holds an L1 copy — the load-bearing source of "do I serve this from L1 or L2?"), and a pointer to the authoritative `tiered_slabs` record. It is **not** the source of truth for lifecycle state.

### 3.3 Why the Rewrite

The prior doc treated the `#[repr(C)]` `SlabManifest` as the authoritative state, including the upload state. That design failed two invariants Wave 4 restored:

- **Cluster-wide lifecycle must be replicated.** The authoritative lifecycle states (`Pending`, `Uploaded`, `Committed`, `Retired`, `Reclaimed`, `Aborted`) are cluster-wide facts that every replica needs agreement on — for quota accounting, reclaim authority, and reader routing. They live in Raft.
- **Provider-side transient artifacts (UploadId, temp files) have no meaning on other nodes.** They belong in a node-local ledger, not the replicated state machine.
- **Per-replica L1 presence is a local fact.** Replicas purge and rehydrate L1 on their own NVMe pressure curves; forcing cluster-wide agreement on local presence would either push a Raft commit per local action (too chatty) or produce a state that means different things on different replicas (a correctness hazard). The current design keeps L1 presence local and only reflects it upward through non-authoritative gossip when reader-routing benefits from the hint.

ADR 023 splits these responsibilities correctly; this section mirrors the split.

---

### 3.4 Transition Ownership (Implementation Guardrail)

The first implementation should treat lifecycle edges as **owned** by one component each. If multiple code paths can emit the same transition, the implementation will drift from the ADRs almost immediately.

| Lifecycle edge / insertion | Only component allowed to propose it | Provider-side prerequisite | Must not be coupled to |
|----------------------------|--------------------------------------|----------------------------|------------------------|
| Direct insert `Pending` | The per-slab upload selector / upload leader | Sealed L1 slab selected for upload | Any provider I/O yet |
| `Pending → Uploaded` | Upload leader | `put` returned success, or `MultipartUpload::complete` returned success | Quota accounting, local purge |
| `Pending → Aborted` / `Uploaded → Aborted` with uploader-produced reasons | Upload leader | Terminal provider failure, retry exhaustion, explicit operator abort, or leader handoff decision | Reclaim batch, synthetic orphan handling |
| `Uploaded → Committed` | Leader-side policy gate | `Uploaded` already committed in Raft | Provider I/O, local purge |
| `Committed → Retired(compaction_retired)` | E_safe compactor on the leader path | Replacement slab durably recorded in the same Raft commit | Remote delete in the same step |
| `Committed → Retired(migration_old_copy)` | Leader migration-cleanup path | New copy already `Committed`; migration lease expired | Local purge, remote delete in the same step |
| `Committed → Retired(retention_expired)` | Leader retention path | E_safe / retention eligibility established | Local purge, remote delete in the same step |
| `Committed → Retired(tenant_deleted)` | Leader tenant-delete path | Tenant state already `deleted` in Raft | Multipart abort, Track D synthetic insertion |
| `Retired → Reclaimed` | Leader reclaim sweeper | `delete(key)` succeeded | Fresh retirement decision |
| `Aborted → Reclaimed` | Leader reclaim sweeper | `abort_multipart` succeeded, or multipart residue confirmed gone | Synthetic orphan insertion (Track D does that earlier) |
| Direct insert `Aborted(orphan_detected)` | Track D full-bucket orphan GC only | Double-pass absence from the state machine for a provider object key | Any uploader failure path |
| `local_l1_present = false/true` | Replica-local purge / promotion sweeper only | Local unlink or local rehydrate succeeded | Any Raft transition |

Illegal shortcuts to assert in code and tests:

- Never `Pending → Committed`.
- Never `Committed → Aborted`.
- Never let local purge or promotion emit a Raft lifecycle transition.
- Never let Track A synthesize `orphan_detected`.
- Never let Track D reuse uploader-produced abort reasons.

### 3.5 First Implementation Test Matrix

The first implementation pass should land the following tests before optimization work:

| Scenario | Minimum assertion |
|----------|-------------------|
| Upload happy path | `Pending → Uploaded → Committed`; quota increments exactly once; later local purge flips only `local_l1_present` and emits no Raft transition |
| Multipart resume after crash (ledger intact) | Restart uses `resume_multipart` + `list_parts`, completes upload, and does not duplicate the remote object |
| Multipart recovery after ledger loss | Restart uses `list_multipart_uploads`, adopts or restarts correctly, and still reaches exactly one `Uploaded` record |
| Uploader abort path | Terminal 4xx or retry exhaustion yields uploader-produced `Aborted`; Track A later advances to `Reclaimed`; quota never increments |
| Local purge / local promotion | One replica purges and later rehydrates L1 while another does neither; authoritative state stays `Committed` throughout |
| Compaction retirement | `Committed → Retired(compaction_retired) → Reclaimed`; remote delete happens only in reclaim; quota decrements in the reclaim commit |
| Migration old-copy cleanup | New owner's copy reaches `Committed`, lease expires, old copy transitions `Retired(migration_old_copy) → Reclaimed` |
| Tenant delete reclaim | Deleted tenant's slabs transition `Retired(tenant_deleted) → Reclaimed` with zero grace and quota decrement in reclaim |
| Full-bucket orphan GC | Track D alone inserts synthetic `Aborted(orphan_detected)` and advances it to `Reclaimed` in the delete batch |
| Illegal transition suite | Reject `Pending → Committed`, `Committed → Aborted`, any direct insert except `Pending` and `Aborted(orphan_detected)`, and any Raft mutation sourced from local purge/promotion |

These are the first correctness tests, not the full matrix. If these are not green, implementation is still below the design bar regardless of how much provider integration code exists.

---

## 4. Background Sweeper Policies

The Sweeper is a low-priority per-node background task. Policies 1, 3, and 4 run on every node for its local L1 state. Policy 2 drives the ADR 023 upload lifecycle. Reclamation (what the prior doc folded into "E_safe Compaction") is pulled into the separate leader-only ADR 024 reclaim sweeper (§5).

### Policy 1 — Sealing (L1-local)

- **Trigger.** `slab.total_bytes ≥ slab_max_bytes` OR `(current_hlc - slab.min_hlc) > slab_max_age` (default 4 hours).
- **Action.** Close the O_DIRECT file descriptor for writing. The slab becomes read-only on L1. A new `Active` slab opens for the tenant's hash subspace.
- **Note.** Sealing does **not** by itself create a `tiered_slabs` record. Sealing marks L1 immutability; selection for L2 upload (Policy 2) is a separate decision.

### Policy 2 — Eviction / Upload (drives ADR 023 lifecycle)

- **Trigger.** `NVMe_utilization > nvme_high_watermark` (default 70%) AND `slab.temperature < cold_threshold`. The sweeper selects the coldest sealed slabs until utilization falls below the low watermark.
- **Action sequence** (each step is ADR 023 §1):
  1. **Propose `Pending`.** The sweeper proposes a `Pending` record to Raft, naming itself as `upload_leader_node_id`.
  2. **Upload.** `ObjectStore::put_multipart` per ADR 023 §3 (trigger: `slab_size ≥ slab_max_bytes`; default multipart ≥ 16 MiB parts clamped to `[provider_floor, 64 MiB]`). Every multipart mutation is mirrored in the local execution ledger.
  3. **Propose `Uploaded`.** On `MultipartUpload::complete` (or on 200-OK from `put` for single-PUT), propose `Uploaded` with the provider ETag.
  4. **Leader gates `Committed`.** After the configurable grace window (default 5 minutes; see ADR 023 Open Question 1), the leader transitions `Uploaded → Committed` in the same Raft commit that increments the tenant's authoritative storage-quota counter (ADR 021 §4).
  5. **Per-replica L1 purge (local, not replicated).** Once the slab is `Committed`, each replica's local purge sweeper MAY unlink its L1 copy when that node's NVMe high-water mark warrants. The unlink updates the node's on-NVMe slab manifest (`local_l1_present = false`) and the local redb index; no Raft commit is proposed, because purge is not a cluster-wide transition. Readers on the same node fall through to L2; readers elsewhere are unaffected. An L1 copy on one replica and no copy on another is a normal steady state.
- **On failure.** Retry with exponential backoff; after `max_upload_attempts` (default 10) or a terminal 4xx, propose `Pending → Aborted` (or `Uploaded → Aborted` if the failure is post-upload-but-pre-commit) with `abort_reason`. Track A of ADR 024 later advances the record to `Reclaimed` after aborting any residual multipart via `ObjectStore::abort_multipart`.
- **On node restart.** Follow ADR 023 §4 recovery: consult Raft, consult local ledger, fall back to `ObjectStore::list_multipart_uploads`. Resume a recovered upload by calling `ObjectStore::resume_multipart(key, upload_id)` and reconciling parts via `ObjectStore::list_parts`.

### Policy 3 — Promotion (L2 → L1 cache)

- **Trigger.** A `Committed` slab with `local_l1_present = false` on this node sees its `temperature` spike above `promote_threshold` (e.g., an analytical scan repeatedly touches historical data this replica has purged).
- **Action.** The sweeper reads the slab from L2 back to L1 (decrypting above the alignment layer per ADR 020 §5). The slab's local file is re-created; the on-NVMe manifest flips `local_l1_present = true`; the redb local index continues to point to the same `slab_id`. The authoritative lifecycle does **not** change — the slab remains `Committed`; only this replica's local presence is restored. Subsequent reads on this node prefer L1 per the redb index hint.
- **Eviction of promoted slabs.** Policy 2's per-replica local purge can re-purge a promoted slab if it goes cold again. Promotion and purge are node-local caching decisions, not cluster-wide lifecycle transitions.

### Policy 4 — E_safe Compaction (L1, L2)

- **Trigger.** `slab.max_hlc < global_E_safe_floor` AND `percentage_of_dead_keys > 50%`.
- **Action.** The compactor reads the live keys from the old slab (decrypting chunks lazily), appends them to the current `Active` slab for the same tenant, and retires the old slab. Retirement in Raft:
  - The old slab's `tiered_slabs` record is transitioned `Committed → Retired(retire_reason = compaction_retired)` in the same Raft commit that durably records the re-written keys in the compacted slab. `retired_hlc` is stamped on that commit.
  - ADR 024's Track C (retention / tombstone reclamation) picks up the retired slab after the reclaim grace window (default 24h) has elapsed from `retired_hlc`, deletes the L2 object, and advances the record to `Retired → Reclaimed` in a `reclamation_event` commit (ADR 024 decision §2). Tenant storage quota is decremented in the same commit (ADR 021 §4).
- **Note on the `Active` slab target.** Compaction appends into the tenant's current `Active` slab, not a fresh slab, so that small compaction output does not fragment the L2 object set. Operators with unusual compaction profiles can force compaction into its own `Active` slab via a per-tenant setting.

---

## 5. Remote Reclamation (Leader-Only)

Reclamation of L2 objects is **leader-only** and driven by the ADR 024 four-track reclaim sweeper. Followers never issue `delete` or `abort_multipart` against the provider. Every destructive action is paired with a lifecycle transition in the same `reclamation_event` Raft commit: Track A drives records from `Aborted → Reclaimed`; Tracks B and C first transition `Committed → Retired(retire_reason)` (a separate commit; see column 5) and then, after the grace window, drive `Retired → Reclaimed`; Track D synthesizes an `Aborted(abort_reason = orphan_detected)` record for the unreferenced object so its deletion is representable as `Aborted → Reclaimed`.

| Track | Trigger | Join | Provider call | Lifecycle transition in `reclamation_event` |
|-------|---------|------|---------------|---------------------------------------------|
| A — Aborted multipart | Periodic (default 6h) | `ObjectStore::list_multipart_uploads(prefix)` vs `tiered_slabs` Pending/Uploaded/Aborted | `abort_multipart(key, upload_id)` for uploads ≥ 1h old with no matching record | `Aborted(abort_reason ∈ {retry_exhausted, provider_error_terminal, operator_abort, leader_transition}) → Reclaimed` for uploader-produced failures; unreferenced multipart residue with **no** record is provider cleanup only. Track A never synthesizes `orphan_detected`. |
| B — Migration cleanup | Event: `epoch_committed` | Old-epoch ring vs new-epoch ring | `delete(key)` for old-copy slab | Pre-commit: `Committed → Retired(retire_reason = migration_old_copy)` once new-copy reaches `Committed` and migration lease expires (default 48h). Reclaim commit: `Retired → Reclaimed` |
| C — Retention / tombstone | Event: E_safe advance; tenant `deleted` | E_safe floor vs `tiered_slabs`; `deleted` tenant → all of its slabs | `delete(key)` for each eligible slab | Pre-commit: `Committed → Retired(retire_reason = retention_expired \| compaction_retired \| tenant_deleted)`. Reclaim commit: `Retired → Reclaimed` after grace (default 24h for retention/compaction; **0h** for `tenant_deleted`) |
| D — Full-bucket orphan GC | Periodic (default 72h) | Paged `list("tenant/")` vs full `tiered_slabs` | `delete(key)` after double-pass (observe-at-T0, delete-at-T0+24h) | Synthetic record inserted as `Aborted(abort_reason = orphan_detected)` → Reclaim commit advances it to `Reclaimed` |

Every delete is gated by a freshness recheck against the leader's current committed state (ADR 024 decision §3): the final delete proceeds only if the record is still `Aborted(...)` or `Retired(...)` at recheck time. Any other state (e.g., the slab was re-referenced by a concurrent compaction or migration) cancels the delete. The three-level rate budget (global 500/s; per-tenant 50/s; per-prefix 100/s, all operator-tunable) prevents provider-side throttle and cross-tenant monopoly. Tenant storage quota is decremented in the same `reclamation_event` commit that advances a record to `Reclaimed` (ADR 021 §4), matching the `Uploaded → Committed` credit.

**Archival tier reclamation is deferred.** Archival slabs reach end-of-E_safe but their L2 objects are not deleted in pilot; `plans/ops/archival_retention.md` (post-pilot) covers the archival lifecycle, including minimum-storage-duration economics.

---

## 6. Tenant Integration

- **`tenant_id` is in every durable structure.** Slab header, slab footer, AEAD AAD, `tiered_slabs` record, local redb index key, audit events (ADR 025 planned). See [ADR 021](../adr/021_tenant_aware_architecture_day_one.md).
- **Object key naming is deterministic and tenant-prefixed**: `tenant/{tenant_id_hex}/slab/{slab_id_hex}.wnd`. This enables per-tenant prefix-scoped LIST for reclaim Tracks C and D, and aligns with the future per-tenant bucket-routing phase (ADR 021 §8 phase 2).
- **Storage quota** is accounted at the `Uploaded → Committed` transition in the same Raft commit (ADR 021 §4). The unit is stored ciphertext bytes per logical replica (`plans/ops/multi_tenancy.md` §3.0). Best-effort soft-reject at admission, authoritative counting in Raft, reconciliation-interval-bounded overshoot.

---

## 7. Encryption Integration

- **Per-slab DEK.** Assigned on slab creation (ADR 020 §2); wrapped by the Tenant KEK; the wrapped form lives in the slab footer (ADR 022 §5).
- **Chunk encryption.** 64 KiB plaintext chunks, per-chunk AEAD with nonce derived from chunk ordinal, fixed 48-byte AAD (ADR 020 §2). Ciphertext stream padded to 4 KiB for O_DIRECT (ADR 020 §5).
- **Plaintext-to-ciphertext migration.** A background re-encryption sweeper (ADR 020 §7) rewrites plaintext slabs through the standard slab-creation path and retires the old copies through Policy 4 (E_safe Compaction) → ADR 024 Track C.
- **FIPS posture.** Cipher selection per ADR 019's decision tree; aws-lc-rs backend; full 140-3 certification target per ADR 026 (planned).

---

## 8. Open Items Tracked by ADRs (Not This Doc)

The following operational choices are fixed by ADRs and should not be re-litigated here:

- **Upload lifecycle states** — ADR 023 §1.
- **Multipart part-size tuning** — ADR 023 §3 (and Open Question 2 for GCS/Azure benchmark).
- **Reclaim rate budgets** — ADR 024 §4.
- **Orphan-GC cadence at very large bucket scales** — ADR 024 Open Question 1.

Changes to these belong in ADR supersession or amendment, not in this operational document.

---

## References

- [ADR 002 — Unified Hash Ring Topology](../adr/002_unified_hash_ring.md)
- [ADR 006 — Tiered Storage and Lazy Migration](../adr/006_tiered_storage_lazy_migration.md) (framing; extended by 022/023/024)
- [ADR 009 — io_uring Threading Model](../adr/009_io_uring_threading_model.md)
- [ADR 014 — rustfs Test Backend](../adr/014_rustfs_test_backend.md)
- [ADR 017 — Persistent Local Index on redb](../adr/017_persistent_local_index_redb.md)
- [ADR 019 — Cipher Selection and FIPS Posture](../adr/019_cipher_selection_and_fips_posture.md)
- [ADR 020 — L2 Envelope Encryption and DEK Lifecycle](../adr/020_l2_envelope_encryption_and_dek_lifecycle.md)
- [ADR 021 — Tenant-Aware Architecture from Day One](../adr/021_tenant_aware_architecture_day_one.md)
- [ADR 022 — L2 Slab Format and Footer-Indexed Range Reads](../adr/022_l2_slab_format_footer_indexed_range_reads.md)
- [ADR 023 — L2 Upload Lifecycle](../adr/023_l2_upload_lifecycle.md)
- [ADR 024 — L2 Orphan Cleanup and Remote Reclaim](../adr/024_l2_orphan_cleanup_remote_reclaim.md)
- [ADR 028 — Deterministic Simulator and AsyncFileOps](../adr/028_deterministic_simulator_and_asyncfileops_abstraction.md) (planned)
