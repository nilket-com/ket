# Pre-Ship Follow-Up: Canonical `ClientWriteId` Idempotency Bytes

Companion ADRs: 045 (PK-less Append-Only Tables), idempotency
contract documented at `src/schema/core.rs:151`.

> **Status:** landed. All three sites canonicalized; the
> architecture guard was widened from the
> `"table.append-locality"`-scoped form to a global block on
> `bytemuck::bytes_of(&...write_id)` patterns; regression tests
> cover TTL/pad drift across all three derivations and across
> `find_by_write_id` (including the secondary index's open-time
> rebuild from a 32-byte to a 24-byte key shape, handled
> transparently by the existing self-heal — no store-name version
> bump was required).

## Why this exists

ADR 045 Phase 8 fixed the append-only locality preimage to hash
only the canonical idempotency identity (`client_uuid` +
`sequence_no`), per
`ClientWriteId::canonical_idempotency_bytes()`. The architecture
guard added in Phase 8 surfaced three pre-existing call sites
that hash the **full 32-byte `ClientWriteId`** (including
`idempotency_ttl_ms` and `_pad`) for retry-stable derivations:

- `src/network/pending_finalizations.rs:23-29`
  `PendingFinalizationId::for_record` — content hash of a
  pending finalization record. Hashes
  `bytemuck::bytes_of(&write_id)`.
- `src/network/pending_finalizations.rs:377-381`
  `encode_write_id_key` — redb table key for the
  `pending_finalizations_by_write_id_v1` store. Encodes the
  full `ClientWriteId`.
- `src/network/data_server.rs:9172-9181`
  `table_write_parent_identity` — derives the parent identity
  for a table write from
  `bytemuck::bytes_of(&meta.parent_write_id)`.

The documented contract at `src/schema/core.rs:151-157` says the
leader maintains the idempotency cache **keyed on
`(client_uuid, sequence_no)`**. `idempotency_ttl_ms` is a
server-clamped hint; `_pad` is alignment. Neither is part of
identity. The three call sites above are inconsistent with that
contract.

## Why this matters more for AppendOnly

For PK-keyed tables, a same-logical retry that bypasses the
in-memory idempotency cache (post-eviction or post-restart)
falls through to `find_by_write_id`. With full-bytes keying,
the lookup misses → fresh admission. But PK identity is a pure
function of the canonical PK encoding + `table_object_id`, so a
re-admitted PK write produces the **same** identity. The
duplicate is benign on the read side: the second write
deterministically supersedes the first under MVCC, and the row
count is correct.

For AppendOnly, identity is HLC-derived. A re-admitted retry
gets a fresh `commit_hlc` and therefore a **fresh, distinct
identity**. The retry is no longer a duplicate at the storage
level — it is a new row. Tenants who think they sent one event
get two. Append-only makes the existing retry-stability gap
materially worse.

## What landed

All three sites canonicalize via
`ClientWriteId::canonical_idempotency_bytes()`:

1. `PendingFinalizationId::for_record` hashes canonical bytes
   only. The pending-finalization content hash no longer
   depends on TTL / pad.
2. `encode_write_id_key` returns a 24-byte key
   (`WRITE_ID_KEY_LEN = 24`). The
   `pending_finalizations_by_write_id_v1` store name was
   **not** bumped: the store's existing open-time self-heal
   (`retain(|_,_| false)` then re-insert from the primary
   table) rebuilds the secondary index with the 24-byte key
   shape on next open. Old records remain reachable through
   the primary table (whose key, `PendingFinalizationId`, was
   not bumped).
3. `table_write_parent_identity` hashes canonical bytes only.

`find_by_write_id` and `remove`'s replacement-search relax
their full-struct `write_id` equality checks to canonical-
bytes equality so a retry with a drifted TTL still resolves
to the original pending record.

## Test surface (landed)

- `for_record_ignores_idempotency_ttl_and_pad`: same
  canonical identity, drifted TTL / pad → same
  `PendingFinalizationId`.
- `find_by_write_id_matches_across_idempotency_ttl_drift`:
  upsert under one TTL, retrieve under a drifted TTL via the
  redb secondary index.
- `table_write_parent_identity_ignores_idempotency_ttl_and_pad`:
  same canonical identity, drifted TTL / pad → same parent
  `Identity`.
- Architecture guard widened from the
  `"table.append-locality"`-scoped form to a hard ban on the
  `bytemuck::bytes_of(&...write_id)` pattern across `src/`,
  `tests/`, and `benches/` (excluding `architecture_test.rs`
  itself). Any future site that tries to hash the raw 32-byte
  struct fails the guard.

## Pre-ship status

Pre-ship correctness work for ADR 045. AppendOnly should NOT
ship before this pass lands, otherwise a TTL drift between
client retries can silently double-admit append-only events
post-restart.

## Out of scope

- Migration of existing on-disk pending-finalization records
  (Wendelon doesn't have a release with AppendOnly committed
  yet, so the on-disk surface is fluid).
- Changes to the `ClientWriteId` wire shape — the wire format
  stays. Only derivations off it canonicalize.
