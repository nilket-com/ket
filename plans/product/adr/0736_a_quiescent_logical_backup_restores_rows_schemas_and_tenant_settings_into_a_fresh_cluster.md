# ADR 0736: a quiescent logical backup restores rows, schemas and tenant settings into a fresh cluster

Status: accepted for implementation, 2026-11-15. Not delivered; the plan stays off main until its reviewed implementation follows. Consolidates DESIGN-DRAFT-06, DESIGN-ADDENDUM-07 and Codex's corrections A and B (01a1dce3). Release obligation: backup/restore; this delivers its first, bounded contract and does not close it.

## Contract (the working fidelity target relayed by the operator)
Lose the installation, restore the backup, verify the data, resume service. Restored: every row of every eligible table, every table schema, every tenant's settings that this mode can establish. Not restored, by design and documented: write outcomes, version history, idempotency continuity (an old operation id has no authority in the restored cluster; callers must drop, not replay, the old installation's unresolved work, whose outcome is unknown, not "not committed"); node, store, signer, root and key identities (all new). Member replacement and section 8 are unrelated.

## Why
ParentDecisions has no backup path: control export is refused (`export_backup_with_state`), restore refuses any cold reference and any incarnation takeover (DC-162), and a fully tiered installation has cold references. A physical route would need authority transitions this mode does not have. A logical route reads through the reader that already answers complete-or-refused at one cut, cold rows included, and restores into a new deployment that owns its own authority.

## Decision

### 1. Eligibility (whole-backup refusal, named; checked at export AND re-checked on the target at restore)
A backup is refused, with the reason recorded, when at the roster cut:
- any table has an effective retention floor (`RetentionActive`; the reader refuses floors in this mode, and `SetTableRetentionPolicy` is refused under ParentDecisions, so a source should have none; the restore checks the target has none either);
- any table's statement is refused by the reader: version bound (`TableTooLarge`), migration in progress, a child plan excluding the exporting store, or any other reader refusal (`ReaderRefused { reason }`), never mislabelled;
- any row fails `storage::content_hash::encode_row_canonical` (unsupported column type or encoding refusal) (`UnsupportedEncoding`);
- any table's canonical bytes exceed the per-table digest cap (§4) (`TableTooLargeForDigest` for bytes, `TooManyRowsForDigest` for the independent row-count limit);
- any KV data exists (§3) (`KvDataPresent`);
- any tenant is not in a live lifecycle state, or any table is mid-drop (`TableDropPrepare` is refused in this mode; a lifecycle record in a non-terminal state still refuses) (`LifecycleNotSettled`);
- any setting cannot be reproduced on the target through a supported route (§6) (`SettingNotReproducible`).

### 2. The backup hold (new replicated state and commands)
- State in the decision state: `hold: Option<BackupHold { id: u128, entered_at: u64 }>`; `entered_at` is the log index of the entry that created it.
- `BeginBackupHold { id }`: no hold → create, return the token (id, entered_at); held with the same id → no change, return the EXISTING token; held with another id → refused `HoldConflict { token }`.
- `ReleaseBackupHold { id, entered_at }`: applies only when BOTH match the current hold; otherwise refused `HoldMismatch`, state unchanged. A delayed release from an earlier hold with the same id cannot clear a later one.
- While held, refused AT APPLY (state unchanged, typed refusal): a genuinely NEW, still-eligible parent reservation (in `reserve`, AFTER recorded-parent replay and the existing identity/retirement checks, including `OutcomeUnavailable`; the hold must never turn an expired or pruned outcome into known-not-committed), and the logical mutations this mode admits: `SchemaUpdate` (table, index, logical type), `CreateTenant`, `UpdateTenantQuota`, `SuspendTenant`, `ResumeTenant`, `InstallTenantKekVersion`, `RetireTenantKekVersion`. Already refused in this mode independently: `TableDropPrepare`, `DeleteTenant`, `SetTableRetentionPolicy`, CTAS. NOT refused: Finalize, Resolve, Abort, RecordNoop, TakeOverLineage, lease grants and renewals, generation and tiering commands, so admitted work reaches its actual terminal result and storage keeps moving.
- A refused new reservation is answered WRITE_REFUSED / KNOWN_NOT_COMMITTED with reason "backup hold" (true: nothing was reserved). Control mutations return their own typed refusal.
- Settled predicate, at a linearizable cut S ≥ entered_at: no parent body Reserved; no table/index lifecycle non-terminal; the exporter waits with a declared bound (120 s) and otherwise releases with its exact token and refuses `HoldNotSettled`. A non-terminal lifecycle whose completion is itself blocked by the hold is refused immediately rather than spending 120 s waiting for impossible progress. Leases are not waited for.
- Roster cut R: the first settled cut. Each table statement's own cut K ≥ R.
- Continuity: before publication the exporter reads the hold at a fresh linearizable cut and requires the exact token; any release and re-entry changes `entered_at`.
- Orphans: the hold survives exporter death and leader change; it ends only by an authorized exact-token release (`backup release --id <id> --entered-at <index>`), both shown by `inspect`. Until then new mutations are refused: a stated outage.

### 3. KV: fail-closed, from the reader's own authority
KV writes are parents in namespace `Kv`, stored as table `DEFAULT_KV`. The check uses the SAME authority pairing the reader uses (`prove_rows`: certified-base entries from the persistent index plus retained bodies at one applied cut), validated around the check as the reader validates its read authority: refuse if any retained body is in namespace Kv (any status) or any base entry (tombstones included) names DEFAULT_KV. If the base and the bodies cannot be read under one validated authority (a generation cut moved bodies into a newer base in between), the check is repeated, never concluded "no KV".

### 4. The artifact
- Staging: an exclusively created fresh directory; refused if it exists.
- Keys: one fresh 256-bit data key per backup; wrapped under scrypt(passphrase) with FIXED parameters (log_n 17, r 8, p 1, 128 MiB) in the operator tool only; the wrap header (magic, version, KDF parameters, salt, nonce) is bounded, versioned and authenticated as associated data; restore refuses other parameters.
- Chunks: AES-256-GCM, nonce = 96-bit counter 1..=MAX_CHUNKS (overflow refused), associated data (backup id, tenant, table, schema generation, ordinal). Nonce 0 is reserved for the manifest.
- Manifest, sealed with nonce 0, published LAST (temp, fsync, rename to `MANIFEST`, directory fsync): format version, backup id, hold token, R, final continuity index, roster (tenants with settings; complete logical table descriptors including namespace/name, primary-key and row-identity policy, column metadata and referenced logical-type definitions; secondary-index definitions where reproducible, never source physical index contents), and the ordered chunk list (ordinal, tenant, table, length, SHA-256 of ciphertext, tag), per table the row count and the canonical digest.
- Canonical digest (bounded, framed): per table, rows are encoded with `encode_row_canonical` over the logical schema; the encoded rows are held in memory up to independent per-table caps of 64 MiB of canonical bytes AND 2^20 rows (whole-backup refusal beyond either; the backup owns this row-count limit independently of the reader), sorted, and hashed as SHA-256 over: domain "ket-backup-table-digest-v1", the canonical logical schema (column names, types, nullability, in order, each length-prefixed), the row count (u64), then each row as u32 length + bytes in sorted order; duplicates and nulls preserved. Working memory, separate from the KDF's 128 MiB: up to 64 MiB of canonical payload plus bounded row-container/offset/sort workspace for at most 2^20 rows, checked before append/allocation; separately bound decoded batch and chunk/manifest buffers. The reader's retained-parent version cap is not a total certified-base row bound. Per table, one table at a time. No spill files.
- Every table export requires the reader's success terminal and read check; anything else refuses the backup.

### 5. Restore
- (R1) Preflight, before touching the target: bounds on manifest and chunk sizes and counts before allocation; unwrap; authenticate the manifest; stream every chunk: hash, tag, associated data and order against the PINNED manifest; recompute each table's canonical digest from the opened rows; equal to the manifest, else refused.
- (R2) The operator forms a FRESH cluster with existing provisioning in an isolated trust domain (independently provisioned CAs; no source credentials, endpoints or bucket access), the declared profile, and does not publish endpoints or issue application credentials. Before any import effect, the tool checks that the logical target is fresh/empty apart from explicitly identified formation-created built-ins, binds to its independently provisioned target identity, and refuses a non-fresh target; it never clears or overwrites existing application data to make one.
- (R3) Roster through the public admin routes (§6), each re-checked for eligibility on the target (no retention floor).
- (R4) Rows through the ordinary public client with the importer's data-capable credential: per manifest chunk one batch, its UUIDv7 operation id minted and journalled before its first offer, re-offered only as the recovery-aware rules allow; each chunk is re-read and revalidated against the pinned manifest before import; a recorded refusal, an unknown outcome at its bound, or a batch older than the parent expiry window fails the restore.
- (R5) Verification: per table, an exact read through the importer recomputes count and canonical digest; equal, else failed.
- (R6) In the acceptance workflow, one explicitly owned new write with a fresh id and its exact read-back proves that the restored service accepts work. The normal restore command does not synthesize a row in a user table or add hidden verification objects; its completion report distinguishes verified restore from the acceptance workflow's subsequent write proof.
- (R7) Only then the operator issues application credentials and publishes endpoints. Any failure in R2..R6: the target stays unpublished and is reported failed; retry = discard it (fresh directories, fresh bucket prefix) and restore again; no resume.

### 6. Settings mapping (public routes, existing roles)
| Setting | Source of truth | Restore route | Role |
|---|---|---|---|
| tenant id, replication factor | TenantRecord | TenantAdmin `Create { tenant, quota, rf }` | tenant/cluster admin (AdministrativeAction::TenantCreate) |
| quota | TenantRecord.quota_bytes | in Create (or `UpdateQuota`) | same |
| tenant lifecycle | must be live (else refused at export) | none needed | — |
| table schema (logical) | TableSchemaCatalog | `SCHEMA_UPDATE_REQ` (Table kind), client `schema_update` | actor with TenantAdminMutation, scoped to the tenant |
| secondary indexes | catalog (Index kind) | `SCHEMA_UPDATE_REQ` (Index kind) | same; ELIGIBILITY to confirm in the plan's first step |
| logical types | catalog (LogicalType kind) | `SCHEMA_UPDATE_REQ` (LogicalType kind) | same |
| retention policy | exact source logical policy plus effective no-floor eligibility; setter refused in this mode | record and compare the exact policy against the target's formation-established policy; refuse a mismatch as SettingNotReproducible, even when both currently yield no floor | — |
| tenant KEKs, root | not copied | fresh cluster's own | — |
| suspended state | refused at export | — | — |
The plan's first implementation step confirms each route end to end in-process; a setting that cannot be reproduced refuses the backup (`SettingNotReproducible`) rather than being dropped.

### 7. Operator sequence
export: `nilket backup export --to <new dir>` (passphrase from a prompt or a file descriptor; begin hold → settled → roster → KV check → tables → continuity check → publish manifest → release with token). verify: `nilket backup verify <dir>` (R1 only). restore: `nilket backup restore <dir> --target <fresh cluster endpoint> --importer-credentials …` (R1 again, then R3..R5, then print the R7/cutover instructions; the acceptance workflow supplies its explicit R6 write probe). release orphan: `nilket backup release --id … --entered-at …`.

## What does not change

Existing application-request encodings, the reader, leases, tiering, Raft algorithms/timing and ADR 0733/0734/0735 stay as they are. New hold commands/refusals append variants without renumbering existing tags. The ParentDecisions durable log/snapshot versions and mode marker change, AND its Raft/forwarded-decision peer envelope advances to the next unused version, enforced both ways before decode; WAL format alone is not a live peer boundary. Today's versions and behavior remain unchanged and Today refuses the backup-hold surface. New operator endpoints use existing authenticated administrative admission, never an unauthenticated direct proposal. Pre-release, old ParentDecisions stores are refused rather than converted or reset; no existing store is modified. New also: the artifact format and operator commands.

## Limits, stated
First workflow only: refuses retention floors, KV data, unsupported encodings, tables over the reader's supported inventory bound, the backup's own 2^20-row bound or 64 MiB of canonical bytes, non-live tenants. Quiescent: new mutations are refused for the whole export, and indefinitely after an exporter dies until an operator releases the hold. The KDF costs 128 MiB in the operator tool. A failed restore is discarded, not resumed. No point-in-time, no partial restore, no history, no member replacement.

## Acceptance (verification map)
Pure: hold transitions (begin idempotent with token, conflict, exact-token release incl. a delayed same-id release refused); reserve answers recorded parents before the hold guard; each held control mutation refused at apply; artifact seal/open, tamper (header, chunk, manifest, order, nonce reuse), wrong passphrase, KDF parameter refusal; digest framing (duplicates, nulls, length boundaries); bounds before allocation.
In-process three-node: hold refuses new writes while an admitted parent finalizes; orphaned hold survives leader change and an exporter abort until exact release; KV check refuses with a KV key and with a deleted KV key; export of tables incl. offloaded (LocalFs) rows; each named eligibility refusal; restore into a fresh in-process cluster with no access to the source; verification; new write; failure leaves the target unpublished.
Real: ONE owning workflow on the reviewed tree: export from a running three-host cluster (B = 250 / 2000 ms) to a local artifact; restore into fresh owned directories, a fresh trust domain and a bucket/endpoint the source never used and whose credentials the source does not hold; verify; new write. One execution, reported either way.
Checks: focused selection; one whole-crate run; the one real workflow.

## First step: bounded feasibility gate

First perform the bounded feasibility gate: map the actual schema/admin admission and reducer routes, the exact non-terminal lifecycle set and the fixed-cut ReadAuthority/K_min plus active-generation validation reused by the KV check. Focused in-process route controls are permitted at this gate; it is not a request for a broad suite or live run. Index/logical-type definitions must be reproduced with valid dependency order and index rebuild/completion semantics, or their presence refuses the WHOLE backup explicitly; they are never omitted. Existing formation-created tenants/tables need explicit equality/update/refusal handling rather than blindly issuing Create. Source inspection already establishes that encode_row_canonical validates the descriptor's Arrow schema but does not encode the physical descriptor fingerprint; keep that validation while excluding physical identifiers from the digest. If this gate shows the selected fidelity needs another foundational authority/reader protocol, stop with that specific finding before expanding implementation.

## Stop points

Reviewed plan with the exact corrections → bounded feasibility gate → RED-first implementation on the existing working branch (no branch/worktree per substep) → aggregate source review → the agreed whole-crate run and one owning real workflow → delivery ruling. If supported feature eligibility is resolved by explicit whole-backup refusal as specified, continue without another generic permission stop. No live backup/restore, reset, broad checkpoint or owning real run precedes source/launch review. Keep one plan and one implementation commit; no automatic exception to acceptance is granted.
