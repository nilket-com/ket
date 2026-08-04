# Impl Plan 072: Table Descriptor Snapshot Producer (Control Ring RPC)

**Status:** Promoted, paired with
`plans/adr/0072_table_descriptor_snapshot_producer.md`.
**Posture:** Single plan, Wendelon-ring only. A read-only Control
Ring RPC + client helper. No consensus write, no Data Ring change,
no Toron production change. Six phases.

## Invariants carried in (must still hold at the end)

- `crates/nilket` stays runtime-free.
- New surface is brand-clean (no project-name prefix) and has no
  `*V1` type suffix (the descriptor codec helper
  `encode_table_schema_descriptor_v1` is an existing wire-format
  name, not new surface).
- Schema update/drop control paths and the data ring are
  behaviorally unchanged; prior Wendelon ADR guards keep passing.
- No new actor role, permission, SAN prefix, or break-glass scope.
  Descriptor-get authorization is a local helper whose V1 role matrix
  delegates to existing admission.

## Phase 0 — Audit (no code change)

Confirm the seams (verified at promotion):

- `MsgType` newtype in `schema/core.rs` (`TENANT_ADMIN_REQ = Self(0x1C)`,
  schema/core.rs:602); **0x3C / 0x3D are unallocated**.
- Catalog: `TableSchemaCatalogV1::latest_by_name(tenant, namespace,
  table)` (schema/table.rs:2020), read from
  `state_snapshot().table_schemas` (raft_storage.rs:1535).
- Descriptor codec: `encode_table_schema_descriptor_v1` /
  `decode_table_schema_descriptor_v1` (schema/table.rs:1490/1502).
- Control handler: `network/control_server.rs` — `ControlOperation`
  (line 74), `authorize_actor(actor, ActorPermission::…)`,
  `context.actor.claimed_tenant_id()? -> Option<TenantId>` (used at
  control_server.rs:923/1057/1557 for the existing tenant paths),
  top-level `CONTROL_REASON_*` constants (including existing
  `AUTHORIZATION_FAILED = 9` and `NOT_FOUND = 10`), and the
  `TENANT_ADMIN_REQ` no-Arrow/no-trailing rejection
  (control_server.rs:1553).
- Router: `network/router.rs`.
- Authz matrix: `network/tls.rs` — `ActorPermission::TenantAdminRead`
  satisfied by `Operator | TenantAdmin | BackupAdmin |
  ReadOnlyObserver | BreakGlassAdmin` (tls.rs:393). Toron's Wendelon
  service client is currently a no-claim `Operator`, so V1
  descriptor-get authorization can delegate to this role matrix without
  adding a role or permission. Keep the descriptor-get helper named
  locally so a later QuerySession/data-domain admission cycle can widen
  it without changing the wire API.
- Metrics: `metrics/registry.rs` (`MetricsRegistry`).

## Phase 1 — Protocol + wire meta (`schema/core.rs`)

- Add `pub const TABLE_DESCRIPTOR_GET_REQ: Self = Self(0x3C);` and
  `TABLE_DESCRIPTOR_GET_RESP: Self = Self(0x3D);` to `MsgType` (D3).
- Add `bytemuck` Pod metas (D4):
  - `TableDescriptorGetRequestMeta` — `tenant_id`, `namespace: [u8;
    128]`, `table_name: [u8; 128]` (264 bytes total). Names are
    zero-padded UTF-8; decode by trimming trailing zero bytes and then
    applying descriptor-name validation.
  - `TableDescriptorGetResponseMeta` — `accepted: u8` (0/1, not
    `bool`), `reason_code`, `leader_hint`, and `descriptor_len`
    (fixed-size; pin the byte size in a guard/test).
- Pin the existing Control Ring reason constants used by this surface:
  `GRANTED`, `NOT_LEADER`, `INVALID_CONFIG`, `AUTHORIZATION_FAILED`,
  and `NOT_FOUND`. Do not add duplicate reason discriminants.

## Phase 2 — Control handler (`network/control_server.rs`)

- Add a `ControlOperation::DescriptorGet` arm or equivalent explicit
  dispatch for 0x3C. The handler calls a local
  `authorize_descriptor_get_actor` helper after decoding the request;
  do not add a `TenantAdminRequestWire` variant.
- Handler (D5–D10):
  1. Reject Arrow/trailing payloads on the request (mirror
     `TENANT_ADMIN_REQ`, control_server.rs:1553).
  2. `authorize_descriptor_get_actor(&context.actor, request.tenant_id)`.
     V1 delegates to
     `authorize_actor(&context.actor, ActorPermission::TenantAdminRead)`
     and then applies the tenant-isolation check below.
  3. Tenant isolation: `if let Some(claimed) =
     context.actor.claimed_tenant_id()? { if claimed !=
     request.tenant_id { → CONTROL_REASON_AUTHORIZATION_FAILED } }`
     (no-claim cluster actor allowed — the Toron-`Operator` case).
  4. Decode + UTF-8/bounds-validate `namespace`/`table_name`; invalid
     → `CONTROL_REASON_INVALID_CONFIG`.
  5. Leader check: if not leader → `accepted = 0`,
     `CONTROL_REASON_NOT_LEADER`, `leader_hint` from the current Raft
     leader. (Read-only against applied SM; no log append — D7.)
  6. `state_snapshot().table_schemas.latest_by_name(tenant, ns,
     table)`:
     - found → `accepted = 1`, `CONTROL_REASON_GRANTED`,
       `descriptor_len = trailing.len()`, descriptor Postcard-encoded
       via `encode_table_schema_descriptor_v1` into the response
       **trailing section** (D10);
     - missing → `accepted = 0`, `CONTROL_REASON_NOT_FOUND`,
       `descriptor_len = 0`, no trailing payload.
  7. Increment the Phase 4 metric for the outcome.

## Phase 3 — Router (`network/router.rs`)

- Route `TABLE_DESCRIPTOR_GET_REQ` (0x3C) exactly to the control
  handler. `TABLE_DESCRIPTOR_GET_RESP` (0x3D) is a client-decoded
  response opcode, not a data-ring or raft route.

## Phase 4 — Metrics (`metrics/registry.rs`)

- Add aggregate counters (D12): `control_descriptor_get_requests_total`,
  `_success_total`, `_not_found_total`, `_not_leader_total`,
  `_authorization_failed_total`, `_invalid_request_total`. No
  per-tenant/table labels.

## Phase 5 — Client helper (`src/client/core.rs`)

- `pub async fn get_table_schema_descriptor(&self, tenant_id:
  TenantId, namespace: &str, table_name: &str) ->
  Result<Option<TableSchemaDescriptor>, ClientError>` (D11):
  - encode the fixed-size request meta (validate name lengths
    client-side);
  - send to the known control leader; on `CONTROL_REASON_NOT_LEADER`
    refresh topology from `leader_hint` and retry (mirror
    `submit_table_schema_update`);
  - on success decode the trailing descriptor via
    `decode_table_schema_descriptor_v1` after verifying
    `descriptor_len == trailing.len()`;
  - `CONTROL_REASON_NOT_FOUND` → `Ok(None)`.

## Phase 6 — Guards + fixtures + evidence + promotion

- Add the 16 architecture guards + 3 convention guards (ADR list) in
  `crates/wendelon/tests/architecture_test.rs`.
- Add the 12 required integration fixtures + the optional metrics
  fixture.
- Evidence doc
  `plans/impl/0072_table_descriptor_snapshot_producer_evidence.md`:
  commands + observed results (targeted descriptor-get tests,
  `architecture_test adr072`, full `architecture_test`,
  `cargo test -p wendelon`, `cargo clippy -p wendelon --all-targets
  -- -D warnings`).
- Roadmap: add the ADR 072 forward-sequence entry; cross-reference
  the Toron consumer (Toron ADR 053) in the Toron roadmap.

## Acceptance checklist

- [x] Phase 1 MsgType 0x3C/0x3D + fixed-Pod metas with no `bool` +
      existing reason-code mappings pinned.
- [x] Phase 2 handler: descriptor-get auth helper + tenant-isolation
      overlay + leader-only + latest_by_name + descriptor_len-pinned
      trailing-section descriptor + no Raft write.
- [x] Phase 3 router routes 0x3C request frames to control only; 0x3D
      is a client-decoded response opcode.
- [x] Phase 4 aggregate outcome metrics, no labels.
- [x] Phase 5 client helper with leader-hint retry + `Ok(None)` on
      not-found.
- [x] Phase 6 16+3 guards pass; 12 fixtures pass; schema
      update/drop + data ring unchanged; prior Wendelon guards pass;
      evidence + roadmap recorded.
