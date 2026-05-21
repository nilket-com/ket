# Impl Plan 073: Table-Query Request Cancellation Producer Contract

**Status:** Promoted, paired with
`plans/adr/073_table_query_request_cancellation.md`.
**Posture:** Wendelon-ring only. Explicit cancel primitive for
`TABLE_QUERY_REQ`; reuses the bidirectional data-ring transport; **no
Control-Ring RPC, no unidirectional stream, no transport-abstraction
change, no Toron production change.** Seven phases, dependency-ordered. The
cancellation substrate mirrors Toron 059, with `CancellationToken` (not a
hand-rolled `Notify`) per the 059 review lesson.

## Invariants carried in

- Wendelon storage paths unchanged except the table-query request path;
  `QUERY_REQ`/`QUERY_STREAM`/`AGG_REQ`/writes/CDC/admin untouched.
- `crates/nilket` runtime-free; fixture worker unchanged; new types follow
  Wendelon's `no-version-suffixes` convention. No Toron production change.

## Phase 0 — Audit (verified anchors)

- `client/core.rs`: `table_query_once` / `table_query_with_runtime_filter`
  send `TABLE_QUERY_REQ`, await `TABLE_QUERY_RESP` on one bidi stream.
  `TableQueryRequestMeta` is a **fixed ~120-byte Pod**; **`_pad[0]` is
  already taken** (`QUERY_REQUEST_FLAG_DISABLE_STATS_PRUNING`, core.rs:580) —
  so the id grows the struct, not `_pad`.
- `data_server.rs`: `handle_table_query` → `prepare_table_query_execution_
  plan_with_migration` → executes → **concatenates batches → encodes one
  response** (materialize-then-respond). `decode_table_query_request_frame_
  parts` accepts `meta.len() >= size_of::<TableQueryRequestMeta>()`. CDC's
  `client_cancel` (emit-failure→unregister) is the fallback pattern to reuse.
- Wire msg-type space: `0x1F` = `AUDIT_CHAIN_HEAD_RESP` (avoid); take
  `0x3E`/`0x3F`. `ReadStatus` enum — add `QUERY_CANCELLED` (next unused).
- `crates/wendelon/tests/architecture_test.rs` (guard home);
  `plans/conventions/no-version-suffixes.md` (no-V1 convention).

## Phase 1 — Request identity + wire (schema, client, wire) (D3/D5/D6/D7/D14)

- `TableQueryRequestId` newtype (Wendelon schema export), represented as a
  Pod-compatible `u128` in the fixed meta with validation rejecting zero;
  scoped `(TenantId, …)`. Not `ClientWriteId`, not Toron ids.
- **Grow `TableQueryRequestMeta`** to carry the id (≈120→136 bytes);
  **update the fixed-size layout guard**. No optional extension / compat
  path (Wendelon no-premature-backward-compat).
- `TABLE_QUERY_CANCEL_REQ = 0x3E`, `TABLE_QUERY_CANCEL_RESP = 0x3F` +
  payloads (req: `(TenantId, TableQueryRequestId)`; resp: `ACCEPTED |
  TERMINAL`). `ReadStatus::QUERY_CANCELLED`.

## Phase 2 — Cancellation substrate module (network/table_query_cancellation.rs) (D8/D9/D10)

Mirror Toron 059's `cancellation.rs`, with the **fixed** signal:
- `ActiveTableQueryRegistry`: `Mutex<HashMap<(TenantId,
  TableQueryRequestId), CancellationToken>>` + a bounded pending-cancel
  tombstone (`MAX` count + TTL, `VecDeque` eviction), consumed on register.
- `register((tenant,id)) -> Registration { token, guard, cancelled_before_
  start }`; RAII `guard` removes the entry on drop (all terminal paths).
- `cancel((tenant,id)) -> Accepted|Terminal`: if active → `token.cancel()` →
  Accepted; if not active but tenant-scoped → insert a bounded tombstone →
  Accepted (covers cancel-before-register and unknown ids); known terminal /
  tenant-invisible cases collapse to the oracle-safe terminal class at the
  handler.
- **Signal = `tokio_util::sync::CancellationToken`** (add `tokio-util`); a
  guard forbids a hand-rolled `Notify` loop (the 059 lost-wakeup).

## Phase 3 — data_server integration (data_server.rs) (D8/D13/D14/D15/D16)

- In `handle_table_query`: register `(tenant, request_id)` **before**
  planning; hold the `Registration` for the request lifetime (RAII removal).
- **Cooperative checks** (`token.is_cancelled()`) at the D13 boundaries:
  post-decode/pre-plan, around migration-pull, per scan range/location,
  before secondary-index fallback, before row-group materialization, before
  PBC over rebatch groups, before concat, before encode, before response
  write. On observed cancel before final emission → `TABLE_QUERY_RESP {
  status: QUERY_CANCELLED }` with **no Arrow body**; release admission /
  BufPool / scratch via RAII. No rollback.
- **Stream-drop fallback:** an emit/finish failure → `client_cancel` cleanup
  + aggregate metric (the CDC pattern), secondary to the explicit path.

## Phase 4 — Cancel handler (data_server.rs) (D11/D12)

- `TABLE_QUERY_CANCEL_REQ` handler: resolve/authenticate tenant; look up
  `(tenant, request_id)` → `cancel(...)`; reply `TABLE_QUERY_CANCEL_RESP`
  with `ACCEPTED` (active signalled or bounded tombstone recorded) or
  `TERMINAL` (known terminal / tenant-invisible) — **oracle-safe,
  non-enumerating**; unknown tenant-scoped ids may return `ACCEPTED` so the
  early-cancel race remains correct. Handled on the data ring, not the
  Control Ring.

## Phase 5 — Client API (client/core.rs) (D5/D19)

- Table-query construction generates a **fresh `TableQueryRequestId` per
  attempt** and writes it into the grown meta.
- Add a producer-only client helper `cancel_table_query_attempt(target,
  tenant_id, request_id)` for the future Toron consumer (ADR 060). **No
  Toron production change here.**

## Phase 6 — Metrics (metrics) (D17)

- Aggregate-only cancellation counters (trigger/outcome/source); **no**
  tenant/table/request-id/node/fingerprint/endpoint/SQL-text/peer labels.

## Phase 7 — Guards + fixtures + evidence + roadmap

- 22 architecture + meta guard (ADR list) in
  `crates/wendelon/tests/architecture_test.rs`, incl. the `0x3E/0x3F-not-0x1f`
  guard, the `cancellation_signal_uses_cancellation_token` guard, the
  fixed-meta-required guard, oracle-safety, RAII registry, bounded tombstone,
  producer-only, no-V1 (Wendelon convention), and the prior-Wendelon-protocol
  meta-guard.
- 19 fixtures (ADR list).
- `plans/impl/073_table_query_request_cancellation_evidence.md`:
  `architecture_test adr073`, full `architecture_test`, `cargo test -p
  wendelon` (data-server/client/wire/schema suite counts), `clippy -p
  wendelon --all-targets -- -D warnings`, `git diff --check`.
- Roadmap: 073 promoted; **next consumer = Toron ADR 060** (Toron sequence,
  cross-referenced, not owned here).

## Watch-items

1. **`CancellationToken`, not `Notify`** — the 059 review caught a
   lost-wakeup in a hand-rolled signal; a guard forbids reintroducing it.
2. **Tombstone bounded** (TTL + count, consumed on register) — no new
   unbounded map (the 055/056/059 discipline).
3. **Checkpoints at boundaries, not kernels** — don't thread the token into
   FSST/decompression; add deeper checks only on proven latency.
4. **Materialize-then-respond is the whole point** — a cancel *during* scan
   must stop before the full scan completes (fixtures 9/10/11), not just at
   the final write.
5. **Oracle safety** — unknown tenant-scoped ids can be tombstoned and return
   `ACCEPTED`; known terminal / tenant-invisible ids collapse to `TERMINAL`;
   never leak cross-tenant existence.
6. **`0x3E`/`0x3F`** — not `0x1F` (`AUDIT_CHAIN_HEAD_RESP`); guard + fixture.
7. **Fixed-meta layout guard updated** to the new size; no compat/extension
   path.
8. **Producer-only** — no Toron production change; the end-to-end claim is
   Toron ADR 060.

## Acceptance checklist

- [ ] Phase 1: `TableQueryRequestId` (Pod-compatible u128 newtype, zero
      rejected by validation); meta grown + layout guard updated; `0x3E/0x3F`
      + `ReadStatus::QUERY_CANCELLED`.
- [ ] Phase 2: registry + RAII guard + bounded tombstone + `CancellationToken`
      (no hand-rolled `Notify`).
- [ ] Phase 3: register-before-plan; checkpoints at D13 boundaries;
      `QUERY_CANCELLED` no-body on pre-emission cancel; RAII resource release;
      stream-drop fallback.
- [ ] Phase 4: oracle-safe idempotent cancel handler on the data ring.
- [ ] Phase 5: per-attempt id; producer-only client cancel helper; no Toron
      change.
- [ ] Phase 6: aggregate-only metrics.
- [ ] Phase 7: 22+meta guards, 19 fixtures, evidence + roadmap; no
      control-plane / legacy-query / Toron / audit-subsystem drift.
