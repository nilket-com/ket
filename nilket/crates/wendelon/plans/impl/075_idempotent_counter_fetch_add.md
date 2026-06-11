# Impl Plan — Wendelon ADR 075: Idempotent Atomic Counter Fetch-Add

Pairs with `plans/adr/075_idempotent_counter_fetch_add.md`. Producer-only (no Toron change). The cut adds a
`COUNTER_ADD` data-ring operation on the existing write/finalization path; the two load-bearing pieces are
the **per-key read-modify-write critical section** (D2) and **structural counter/blob type safety** (D7).
All anchors verified.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Generic KV write/read + `write_id` | `Client::write` / `read` (wendelon-client/src/lib.rs:815/1007); `WriteRequestMeta` fixed Pod |
| Per-key primary owner (routing) | `RingState::owner_of_locality` / `owner_of_identity` (network/ring.rs:192/199) |
| Per-key version chain (insertion order) | append-path `DashMap::entry(...).and_modify(insert_version_sorted)` + "different HLCs … cannot interleave" (storage/core.rs:502/581) |
| `write_id` idempotency | `pending_finalizations_by_write_id`, `canonical_idempotency_bytes`, `find_by_write_id` (network/pending_finalizations.rs) |
| Durability / group commit | `wait_durable_commit` / `run_group_commit_driver` (storage/core.rs:319/345) |
| Finalization record | `TenantFinalization` (marker + `charged_bytes`) (network/pending_finalizations.rs) |

## Phase 0 — grounding gates (resolve before locking D2/D7)

1. **Per-key RMW serialization (D2):** confirm whether the storage manager already offers an atomic
   read-compute-append per key, or whether a server-side **keyed mutation lock** must wrap read-latest →
   `checked_add` → append. Version-chain insertion order is NOT sufficient on its own (two adders can share
   `previous`). Land the critical section as `StorageEngine::counter_fetch_add` (preferred) or a keyed lock.
2. **Record kind vs key-domain (D7):** confirm whether the storage record can carry a durable `Blob |
   Counter` discriminant. If yes → typed record (preferred). If no → domain-separated counter keyspace
   (`counter-domain ‖ caller-key`) with **server-side** `key_identity` verification. Caller discipline is
   not an option.
3. **Failed-quorum visibility:** ground whether a locally-appended-but-unfinalized version can be read by a
   later primary read (which would poison `previous_value`). The conditional fixture
   `counter_fetch_add_failed_quorum_does_not_poison_later_previous_value` is required iff this is reachable.

## Phase 1 — wire + storage primitive

- `COUNTER_ADD_REQ`/`RESP` message pair beside `WRITE_REQ` (point writes stay byte-stable, LWW):
  `CounterAddRequestMeta` (write_id, client_hlc, expected_epoch, tenant_id, proxy_hop, durability,
  await_finalization, key_identity, key_len, `delta: u64`) → `CounterAddResponseMeta` (write coordinates +
  `previous_value` + `current_value`).
- `StorageEngine`/`SlabManager` `counter_fetch_add(tenant, key_identity, delta)` implementing the D2 critical
  section: read latest counter version (absent → 0), `checked_add` (overflow → reject, D5), append the new
  LE-`u64` version under the per-key guard. Reject `delta == 0`. Enforce D7 type/domain safety.

## Phase 2 — admission, durability, idempotency

- Route + admit like a point write (tenant lifecycle, quota/charged-bytes, lease gate, durable quorum,
  pending finalization, epoch).
- Typed cached mutation outcome: `CachedMutationOutcome::{ Write(...), CounterAdd { response, trailing } }`,
  keyed by `canonical_idempotency_bytes`; duplicate replay returns the original counter response without
  re-applying (D4).

## Phase 3 — client facade

- High-level `Client::counter_fetch_add(tenant, key, delta, options) -> CounterAddResponse`, mirroring
  `Client::write` by allocating a `ClientWriteId` internally and preserving it across transport retries.
  Lower-level request APIs still expose `CounterAddRequestMeta.write_id` for duplicate-replay fixtures and
  callers that intentionally own idempotency keys.
- `Client::counter_read(tenant, key) -> Option<u64>` (point read + LE-`u64` parse; absent → `None`, D6).
- If domain-separated keys: derive + verify the physical counter-domain key on both client and server.

## Phase 4 — fixtures + guards

The ADR's Integration Fixtures (headline concurrent-sum, racing-adders-don't-share-previous, idempotent
replay, overflow/zero-delta, absent-read, blob/counter cross-reject, leader-change/restart, conditional
failed-quorum) + the 10 architecture guards. The headline + racing-adders fixtures are the D2 proof; the
two cross-reject fixtures are the D7 proof.

## Verification gate

```
cargo check -p wendelon --all-targets
cargo test  -p wendelon counter            # the COUNTER_ADD unit + integration fixtures
cargo test  -p wendelon write              # ordinary point write stays byte-stable / LWW
cargo test  -p wendelon-client counter     # client facade
cargo test  -p wendelon                    # full suite
cargo fmt --all -- --check
```

## Risks

- **D2 critical section is the correctness crux** — without per-key read-compute-append serialization, the
  headline concurrent-sum fixture loses increments. Land it as a storage-manager atomic update or a keyed
  lock; do not rely on version-chain insertion order alone.
- **D7 must be structural** — a typed record kind or a server-verified counter key-domain; never caller
  discipline (it recreates the cross-writer problem).
- **Idempotency outcome typing** — squeezing counter values into `WriteResponseMeta` makes duplicate replay
  guess the shape; use the typed `CachedMutationOutcome`.
- **Charged-bytes growth** — each add appends a version; ground whether compaction of the counter chain is
  needed or the existing version-chain compaction covers it (note for the evidence pass).
