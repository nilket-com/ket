# ADR 0119 - A ParentDecisions AppendOnly table keeps every row by its operation and position

## Status
Accepted as a plan, 2026-11-20 (Codex 01a1f911, 01a1f91d). **Implementation and delivery are separate.**

**Scope:** ordinary **named** AppendOnly tables under ParentDecisions. Out of scope: provisional/CTAS admission, `AppendOnlyContentHash`, OR REPLACE, allocator work, qualification runs.

## Context
**Toron gives every table without a PRIMARY KEY `RowIdentityPolicy::AppendOnly`** (`toron/src/sql_planner.rs:2056`), with the default table spread and MultiRow.

**Under ParentDecisions such a table is created, but never written.** The baseline on `bfebc3bc8`, on a fresh PD local profile:
- `CREATE TABLE pd_append (id BIGINT NOT NULL, label TEXT)` → `OK`;
- `INSERT … (1,'same'),(1,'same'),(2,'other')` → exit 1, `Toron StorageError: storage client error`;
- `SELECT` → 0 rows, also after a restart.

On a three-voter PD cluster the same write answers `INTERNAL_ERROR` with knowledge `INDETERMINATE`. **By source trace** (no reservation is made), that comes from the coordinator's probe, `child_versions` → `row_versions_in_order` → `UnsupportedTable` (`network/data_server/child_writes.rs:196-204`). The PD profile's retention window is 0, so no retention floor applies.

**Why AppendOnly is refused.** Today's AppendOnly identity is `(writer node, row HLC, index within the batch it is given)`. PD derives identities again at each receiver, and on subsets or reordered batches: the missing-row subset, repair answers in manifest order, recovered pages grouped by HLC, base pages. So a Today identity cannot be reproduced. **Nine PD sites assume an identity and position derived from row values, which only PrimaryKey provides:**
1. the refusal;
2. the retry digest;
3. receivers' child planning;
4. repair;
5. the reader's locality;
6. delete position;
7. certification tombstone placement;
8. base fetch;
9. recovered-row ingest.

## Decision

### D1. The identity of a PD AppendOnly row
`I = BLAKE3::derive_key("wendelon pd table append-only row v1", tenant_u64_be ‖ table_object_id[16] ‖ client_uuid[16] ‖ sequence_no_u64_be ‖ ordinal_u32_be)`. The result is a 32-byte `Identity` (`schema/units.rs:85`).
- **Inputs:** `client_uuid` and `sequence_no` are the parent key's. `ordinal` is the row's position in the original request.
- **Excluded:** no HLC, no writer, no schema generation. Schema generation and row contents stay bound by `request_digest` and `row_digest`.
- **The pinned vector:**
  - tenant 1, table `0xA0`×16, uuid `01 02 … 10`, sequence 7, ordinal 2;
  - the input (52 bytes) is `0000000000000001 a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0 0102030405060708090a0b0c0d0e0f10 0000000000000007 00000002`;
  - **I = `a65089040c14263afc3ceaa9882e978ccb058eef2e28dec28d12241d82bc12e3`.**
- **Consequences:**
  - the same key with a changed payload gives a different digest, hence `WRITE_KEY_CONFLICT`;
  - identical values at two ordinals, or identical inputs under two operation ids, give distinct identities, so both survive;
  - a retry, or a change of coordinator, reproduces the same identities.

**Today's AppendOnly rule is unchanged.**

### D2. Position: per row, from the identity
**For a PD AppendOnly row:**
- its **raw locality** is `table_spread_locality(I)`;
- its **position** is `row_position` (the partition if the table is partitioned, otherwise the raw locality) (`coordinator.rs:41-51`).

**The two are kept apart consistently:** routing uses the raw locality's replica set, and lineage and child ranges use the position.

**Which sites take the position from the stored identity:** `delete_row_locality`/`delete_row_position`, the reader's `version_locality`, certification and base fetch. They no longer re-run admission on values. PrimaryKey keeps its value-derived identity, with the same position rule.

### D3. The identity reaches the disk, index-aligned
**A PD-only admission entry** takes the identities as input, index-aligned with the batch.
- **What it derives from each identity:** the raw locality and the partition.
- **What it writes from them:** the synthetic columns (`__row_key_id__`, `__row_routing_locality__`, `__row_partition__`, `__row_hlc__`).
- **What it keeps:** all existing input validation (schema, nulls, the row cap).

**The identity vector goes through every selection the batch goes through:**
- holder and replica `persist_locked` (the sparse `missing` indices take rows and identities alike; `child_writes.rs:1043-1080`), and `bifurcate_child`;
- partial repair;
- recovered-row storage (`ingest_recovered_rows`).

**The manifest is built in one index-aligned pass,** with no per-key linear search on this path. The existing search is `child_writes.rs:384-392`.

**Base fetch for a replacement:**
- pair each verified certified entry (identity, HLC, row digest, position) with its Arrow row, in the existing page order;
- check the payload's recomputed digest and the position derived from the identity;
- apply the same HLC-group indices to rows and identities, as `replacement_recovery::ingest_page` already groups by each live entry's HLC.
- **No identity or HLC column is added to base pages.** The page budget and the staged whole-chunk check are preserved.
- **Replacement keeps exact internal identities.** Logical restore is a different contract.

### D4. Bounds and ownership
**N** is the checked sum of `child.rows`. Each child has 1..=`MAX_ROWS_PER_CHILD` rows, and **N ≤ `TABLE_WRITE_MAX_ROWS` (65,536)** is checked in `validate_claim` for PD AppendOnly claims.

**Each receiver checks:**
- ordinals distinct within the child;
- each ordinal < N;
- `I(key, ordinal)` recomputed;
- `position(I)` within the child's `[range_start, range_end]`;
- the row count equals `child.rows`;
- `manifest_content == child.content`.

**Why every ordinal of `0..N` is owned exactly once:**
- ranges are disjoint (`validate_claim`), and each ordinal has one identity and so one position, so no ordinal appears in two children;
- the counts sum to N, and ordinals are distinct per child and < N;
- **so the accepted children together hold every ordinal once.**

**Excess fan-out** refuses the whole write, as today.

### D5. Carrying ordinals and identities: the HRFT trailing section
**Child writes and replicas** carry the child's ordinals as fixed-width u32 big-endian in the frame's **trailing section**, in batch order. **Repair answers** carry the served rows' 32-byte identities there, in row order.
- **Why the trailing section:** it has its own 5 MiB cap (`network/core.rs:31`, checked before allocation). The user Arrow schema stays intact, and near-limit Arrow payloads do not grow.
- **The receiver checks before building a vector:**
  - the trailing length equals `rows × width`, computed with checked arithmetic;
  - the role, the table policy, and the presence or absence of the trailing section.
- **Any mismatch refuses.**

**The size budget is enforced before Reserve.**
- **`build_claim` computes each child's true section sizes,** without stacking extra full encodings under the applied-state read lock:
  - the write frame's Arrow rows, plus `4 × rows` trailing bytes;
  - the repair answer's Arrow rows (the bound below), plus `32 × rows` trailing bytes.
- **A child over 5 MiB in any section refuses the whole write,** typed, with no reservation.

**The repair bound, and the representations it admits** (qualified at implementation review, Codex 01a1f9ca, 01a1f9f6):
- **The bound holds only for flat columns.** A flat column is one whose value and offset bytes are the same however its rows are assembled.
  - So the server's concatenation of individually stored rows, and the write's `take` of the same rows, differ at most by one validity bitmap per column.
  - The stream is uncompressed, with the same messages and fixed-size buffer entries.
  - **Hence** `repair ≤ write + columns × pad64(ceil(rows / 8))`.
- **A named PD AppendOnly table is written only when every column is flat** and has a canonical row encoding (`storage::content_hash`). The admitted columns are therefore:
  - Boolean;
  - the signed and unsigned integers;
  - Float32 and Float64;
  - Decimal32, Decimal64, Decimal128 and Decimal256;
  - Date32 and Date64, Time32 and Time64, Timestamp, Duration and Interval;
  - Utf8, LargeUtf8, Binary and LargeBinary.
- **Excluded, and refused before any identity:**
  - by the repair bound: dictionary, view (`Utf8View`, `BinaryView`), nested (lists, structs, maps) and union columns, and run-end encoded columns;
  - by the canonical encoding: `Null`, `Float16` and `FixedSizeBinary`.
- Such tables stay refused under PD, as before this ADR. No new canonical type or dictionary representation is added here.

### D6. The version fence
**The bumps:** the PD marker to v22, the PD WAL and snapshot formats to 32, and the PD envelope (frame) version to **25**.

**The message family:** `TABLE_CHILD_WRITE`/`REPLICATE`/`FETCH` (request and response) and the base-fetch request and response join `carries_decision_envelope` (`consensus/decision_mode.rs:199-208`).
- **Exact version checks run at the request AND response boundaries** of this family, before semantic decoding. This is needed because the generic `validate_frame_version` always admits v1.
- **The checks run on disk open AND on snapshot install.**
- **Today's frames and bytes are unchanged.**

**The routes, as in ADR 0118:**
- the prior binary exports and the new binary restores into a fresh cluster;
- a prior-format store is refused with its bytes preserved.

### D7. Routing
**The client reads the per-peer `NodeCapability::ParentDecisions` bit** from cluster status, as a **routing hint** only. When it is set and the table is AppendOnly, the client previews row 0's raw locality from `I(key, 0)`.
- **Missing, stale or inconsistent bits:** fall back to Today's preview.
- **The server still decides:** it admits, or redirects to the lineage holder of `row_position(first)` (`NOT_LEADER` with a hint), within the existing `DATA_RETRY_BUDGET`.
- **What the first hop can be:** `mutation_target` prefers a remembered leader within the replica set (`client/core.rs:2145-2175`). So a correct preview reaches the right replica set, but at RF > 1 it may reach a replica other than the leased holder. **Zero redirects is asserted only when the initial selection is the holder.**

## Rationale
- **The identity is a function of the operation and the original position only,** so every receiver, retry, coordinator, repair and replacement derives the same one, and checks it against the signed content.
- **Placing by identity makes position a pure function of identity under PD.** Deletes, reads and certification then need no stored-locality lookup.
- **The fence makes both directions refuse cleanly.** Without it, layouts would be unchanged while semantics change.

## Consequences (The "Negatives")
- **A pre-0119 PD store or peer is refused:** marker, formats and envelope move. The route is prior-binary export and a fresh restore.
- **A PD AppendOnly write spreads over more children than Today's single request-locality child.** A write that needs more than 64 children, or any section over 5 MiB, is refused whole.
- **The O3 knowledge of an unrecorded refusal** (INDETERMINATE) is unchanged here.

## Acceptance (each control RED before its fix)
1. **Public (live CLI, PD local profile):** the baseline above; INSERT `OK rows=3`, all three rows read, also after a restart.
2. **Identity:** the pinned vector; a retry (one parent, one row set); a changed payload under the same operation (`WRITE_KEY_CONFLICT`); identical values under two distinct operations (both kept).
3. **A partial retry** with sparse, non-consecutive missing ordinals and equal values. The stored `__row_key_id__`, position and partition equal the manifest's; nothing is duplicated.
4. **Cross-child ordinal misuse:** an ordinal out of the range, duplicated, ≥ N, a forged identity, or a wrong or missing trailing section. Each is refused at the receiver, with nothing persisted.
5. **Partially present repair,** with byte-equal identities and digests.
6. **Replacement from a certified generation,** including a tombstone.
7. **Delete:** deleting one of two equal-valued rows by identity removes exactly one.
8. **Reads** keep the stored-key and content checks.
9. **A changed coordinator:** a redirect, or indeterminate pending authoritative resolution. No duplicate, no contradictory decision.
10. **Size:** a near-limit write is accepted; an oversize write is refused with no reservation.
11. **Routing:** a stable crossed case (the preview reaches a non-holder replica and converges); an old preview follows the hint within the budget.
12. **The fence:** both frame directions on every changed request and response path; snapshot install; the prior binary on a copy of a new store refuses with the bytes preserved; the prior-binary export and new-binary restore route.
13. **Backup and restore of duplicates** into a fresh PD cluster: the row multiset is equal.
14. **The PrimaryKey and Today contracts** stay unchanged, with their tests untouched.
