# ADR 072: Table Descriptor Snapshot Producer (Control Ring RPC)

**Status:** Promoted
**Seed:** Toron ADR 052 §D15 / roadmap "ADR 053 candidates" — live
Wendelon → Toron descriptor sync, sequenced producer-first.
**Council:** Claude, Codex, Gemini (iter-0 3-of-3 on the topic;
iter-1 settled on a dedicated `MsgType` over reusing the
`TenantAdmin` enum; authz converged on a descriptor-get helper whose
V1 role matrix delegates to the existing `TenantAdminRead` admission
for Toron's current no-claim `Operator` Wendelon identity; iter-2
unified on the fixed-Pod / trailing-section / tenant-isolation D-set).
**Category:** Wendelon storage-ring substrate ADR — a read-only
Control Ring producer that exposes the authoritative
`TableSchemaCatalogV1` by name. Exterior Substrate → Wendelon home
(drafted via the Toron operator-plane council, relocated at
promotion — ADR 071 precedent). Read-only; no consensus write; no
Toron-side change.

## Summary

Add a read-only Control Ring RPC,
`MsgType::TABLE_DESCRIPTOR_GET_REQ` (0x3C) /
`TABLE_DESCRIPTOR_GET_RESP` (0x3D), that answers

```text
(tenant_id, namespace, table_name) -> latest TableSchemaDescriptor | not-found
```

against the Raft **leader's applied state machine** catalog
(`TableSchemaCatalogV1::latest_by_name`). It is the **producer half**
of the live descriptor-sync arc that ADR 052 D15 sequenced: Wendelon
publishes the authoritative read; a later **Toron** consumer ADR
(053) handles a catalog bind-miss by fetching exactly one scoped
descriptor and merging it into its live registry in place. ADR 072
ships only the Wendelon producer + a high-level client helper — no
Toron production change.

## Motivation

ADR 051 made Toron's startup descriptors tenant-explicit but
**startup-only**; ADR 052 closed the resource-governance front and
made the descriptor-liveness sequencing explicit — Toron must not
invent a descriptor source, background sync loop, or stubbed
`DescriptorSource` before Wendelon publishes the real producer. The
gap: a table created in Wendelon after a Toron daemon starts is
invisible to Toron SQL until restart.

Wendelon already owns the authoritative state:
`TableSchemaCatalogV1::latest_by_name(tenant, namespace, table)`
(schema/table.rs:2020) returns the latest bound descriptor; a dropped
table unbinds the latest name (so a dropped table reads as
not-found, never a stale descriptor); `TableSchemaDescriptor` is
already `Serialize`/`Deserialize` and self-validating, and is the
exact object Toron uses at storage boundaries. The missing piece is
a narrow, stable, read-only producer surface.

## Decisions (14, Promotion-Locked)

**D1: Topic.** A read-only Control Ring RPC that returns the latest
authoritative `TableSchemaDescriptor` for an exact
`(tenant_id, namespace, table_name)`.

**D2: Placement.** Exterior Substrate → Wendelon home (ADR 072,
Wendelon's next number after 071). Drafted via the operator-plane
council, relocated at promotion; Toron's roadmap cross-references,
does not own it. Guards/fixtures in `crates/wendelon/tests/`.

**D3: Protocol.** New `MsgType::TABLE_DESCRIPTOR_GET_REQ` (0x3C) and
`TABLE_DESCRIPTOR_GET_RESP` (0x3D) in `schema/core.rs` (alongside
`TENANT_ADMIN_REQ` 0x1C). A dedicated MsgType, not a new
`TenantAdminRequestWire` variant — the new opcode keeps a single-
purpose request/response self-contained and avoids a `None`-bearing
field on the shared `TenantAdmin` response. The router
(`network/router.rs`) routes 0x3C requests exactly to the control
handler; 0x3D is the client-decoded response opcode.

**D4: Fixed-size Pod wire meta.** `TableDescriptorGetRequestMeta`
is a `bytemuck` Pod (264 bytes: `tenant_id` + `namespace: [u8; 128]`
+ `table_name: [u8; 128]`); `TableDescriptorGetResponseMeta` is a
fixed-size Pod carrying `accepted: u8` (0/1, not Rust `bool`),
`reason_code`, `leader_hint`, and `descriptor_len`. The request
carries no Arrow and no trailing payload (mirroring the existing
`TENANT_ADMIN_REQ` rejection); the response descriptor lives in the
frame's trailing section (D10), and `descriptor_len` must match the
trailing length on success and be zero on rejection.

**D5: Name encoding + validation.** `namespace` / `table_name` are
zero-padded UTF-8 within their 128-byte buffers; the decoder trims
trailing zero bytes before validation. The handler validates UTF-8 and
the existing descriptor-name bounds before querying the catalog. A
malformed name is rejected with `CONTROL_REASON_INVALID_CONFIG`;
`CONTROL_REASON_NOT_FOUND` is reserved for well-formed names that are
absent or dropped.

**D6: Authorization — descriptor-get helper + tenant isolation.** The
handler uses a local descriptor-get authorization helper rather than
reusing the `TenantAdminRequestWire` surface or inlining a raw
permission check into the handler. V1 of that helper delegates to
`ActorPermission::TenantAdminRead` (which Toron's current no-claim
`Operator` Wendelon identity already holds) and then enforces tenant
isolation the same way the existing tenant paths do: if
`context.actor.claimed_tenant_id()?` is `Some(t)`, require
`t == request.tenant_id`; a cluster-level actor with no tenant claim
(Toron-as-`Operator`) may target any tenant, having already done its
own ADR 049 SQL-boundary authz. Cross-tenant attempts by a
tenant-scoped principal fail with `CONTROL_REASON_AUTHORIZATION_FAILED`.
The helper name keeps descriptor-read policy local so a later
QuerySession/data-domain admission cycle can widen the V1 role matrix
without renaming the wire API or architecture guards.

**D7: Serializable-at-leader.** The lookup reads the **leader's
applied state-machine** catalog. A follower returns `accepted = 0`,
`CONTROL_REASON_NOT_LEADER`, and `leader_hint` from the current Raft
leader; the client retries to the leader. This is the freshness
floor: a bind read must not miss the just-created table that is the
liveness gap. It **bypasses the Raft log** (read-only against applied
state) — no log append, no Data Ring storage touch, idempotent.

**D8: Latest-only, exact-name.** Returns the single latest
descriptor for the name. **No** historical generations exposed to
bind; **no** bulk tenant enumeration / inventory; **no**
`table_object_id` lookup. `descriptors_for_tenant` stays an internal
helper. A bulk/snapshot export, if ever needed, is a separate ADR
with its own cursor + cardinality story.

**D9: Reason codes.** Reuse the existing Control Ring reason codes:
`CONTROL_REASON_GRANTED` on success, `CONTROL_REASON_NOT_FOUND` for an
unknown / dropped (name-unbound) table,
`CONTROL_REASON_AUTHORIZATION_FAILED` for tenant mismatch or
unauthorized actors, `CONTROL_REASON_NOT_LEADER` on a follower, and
`CONTROL_REASON_INVALID_CONFIG` for malformed request names. The ADR
adds no new reason discriminants; it pins the descriptor-get mappings
to the already-existing constants.

**D10: Full-descriptor trailing payload.** On success the **entire**
`TableSchemaDescriptor` is Postcard-encoded via the existing
`encode_table_schema_descriptor_v1` (schema/table.rs:1490) into the
response frame's trailing section — preserving every identity field
(`table_object_id`, schema generation, logical/physical
fingerprints, namespace, table name, schema, locality, layout, row
identity, evolution policy) so the Toron-side registry merge is
trivial. No Toron `TableId` (Toron-local; the consumer allocates it).
No fingerprint recompute or field rewrite.

**D11: Client helper.** `crates/wendelon` client (`src/client/core.rs`)
gains `get_table_schema_descriptor(tenant_id, namespace, table_name)
-> Result<Option<TableSchemaDescriptor>, ClientError>`, mirroring
`submit_table_schema_update`'s leader-hint retry/topology-refresh
posture and returning `Ok(None)` on `CONTROL_REASON_NOT_FOUND` (maps
cleanly to Toron's catalog miss). The client rejects success responses
whose trailing length does not equal `descriptor_len`.

**D12: Aggregate observability.** Add to the Wendelon
`MetricsRegistry` (`metrics/registry.rs`) aggregate counters only:
`control_descriptor_get_requests_total`, `_success_total`,
`_not_found_total`, `_not_leader_total`,
`_authorization_failed_total`, and `_invalid_request_total`. **No**
per-tenant or per-table labels — holds the storage-ring
low-cardinality posture.

**D13: Scope fence.** No descriptor write/create/alter/drop behavior
change; no schema-evolution policy change; no data-ring descriptor
lookup; no bulk inventory; no `table_object_id` lookup; no
cross-tenant name search; no new actor role, permission, certificate
claim, SAN prefix, or break-glass scope; no Toron production change;
`crates/nilket` stays runtime-free.

**D14: Consumer carry-forward (next ADR, not this one).** Toron
ADR 053: `Catalog::resolve_table` miss → one scoped
`get_table_schema_descriptor` → validate returned namespace/table
identity matches the request → **in-place** merge under the live
`Arc<WendelonDescriptorRegistry>` (e.g. `sync_descriptor_for_tenant`)
→ retry bind once. It must **not** swap the registry `Arc`, and
ADR 051's `from_tenant_descriptors` stays a startup constructor, not
a live-sync primitive. Conflict (same name, different Wendelon
identity) reports; never silently rewrites. Staleness / external-DROP
invalidation / `DescriptorEvolved` refresh are that ADR's problem.

## Architecture Guards (16, Promotion-Locked)

1. `adr072_descriptor_get_msg_types_are_unique` (0x3C/0x3D
   unallocated before this ADR)
2. `adr072_descriptor_get_response_meta_has_no_bool_and_pins_descriptor_len`
3. `adr072_descriptor_get_uses_descriptor_authorization_helper`
4. `adr072_descriptor_get_enforces_tenant_isolation`
5. `adr072_descriptor_get_is_leader_only_with_leader_hint`
6. `adr072_descriptor_get_bypasses_raft_log`
7. `adr072_descriptor_get_returns_latest_by_name`
8. `adr072_descriptor_get_not_found_is_control_reason_not_found`
9. `adr072_descriptor_get_tenant_mismatch_is_authorization_failed`
10. `adr072_descriptor_get_malformed_name_is_invalid_config`
11. `adr072_descriptor_get_returns_catalog_descriptor_without_mutation`
12. `adr072_descriptor_get_payload_is_full_descriptor_v1_postcard`
13. `adr072_metrics_track_descriptor_get_outcomes`
14. `adr072_no_data_ring_or_bulk_or_object_id_descriptor_lookup`
15. `adr072_schema_update_and_drop_paths_stay_unchanged`
16. `adr072_no_toron_descriptor_sync_stub`

**Convention guards:**
- `adr072_nilket_root_stays_runtime_free`
- `adr072_new_surface_is_brand_clean_and_has_no_v1_suffix`
- `adr072_prior_adr_guards_continue_to_pass` (meta-guard)

## Integration Fixtures (12 required + 1 optional)

**Required (gates promotion):**
1. `descriptor_get_leader_returns_latest_descriptor` — create/update
   through the schema path, read by name, observe the latest
   generation.
2. `descriptor_get_returns_latest_not_historical_after_update`.
3. `descriptor_get_follower_returns_not_leader_with_hint`.
4. `descriptor_get_missing_table_is_not_found` (wrong tenant /
   namespace / table, and a dropped/unbound name).
5. `descriptor_get_uses_descriptor_authorization_helper` (an
   unauthorized role is rejected; an authorized role succeeds through
   the helper).
6. `descriptor_get_cross_tenant_claim_is_authorization_failed` (a
   tenant-7-claimed principal fetching tenant 8 is rejected).
7. `descriptor_get_cluster_actor_reads_any_tenant` (no-claim
   `Operator`, the Toron case, reads tenant 7 and tenant 8).
8. `descriptor_get_overlong_or_invalid_name_is_rejected`.
9. `descriptor_get_returns_catalog_descriptor_unchanged` (round-trips
   identity fields; no fingerprint recompute).
10. `client_get_table_schema_descriptor_retries_to_leader`.
11. `client_get_table_schema_descriptor_maps_not_found_to_none`.
12. `client_get_table_schema_descriptor_rejects_descriptor_len_mismatch`.

**Optional (non-gating):**
- `descriptor_get_metrics_count_aggregate_outcomes`.

## Out of Scope

- Bulk / tenant-inventory descriptor export; `table_object_id`
  lookup; cross-tenant name search.
- Data-ring descriptor lookup; descriptor write/alter/drop change;
  schema-evolution policy change.
- The Toron-side consumer, registry merge, staleness/invalidation,
  and `DescriptorEvolved` refresh (Toron ADR 053).
- Any Toron production change; new role/permission/SAN/break-glass
  scope.

## Council History

- **Iter-0 (3-of-3 topic):** all three led the Wendelon-sequence
  descriptor producer; agreed exact-name latest-only, leader-routed
  freshness, read-only, identity-preserved, no bulk inventory.
  Authz split: Codex + Gemini `TenantAdminRead`; Claude argued
  Data-domain.
- **Iter-1:** grounding showed Toron's current Wendelon service client
  is the no-claim `Operator` role, so V1 can delegate descriptor-get
  admission to the existing `TenantAdminRead` role matrix without a
  new role or permission. The promoted contract keeps that behind a
  descriptor-get helper so future QuerySession/data-domain admission
  can widen the helper without changing the wire API. Wire shape
  settled on a dedicated `MsgType` (0x3C/0x3D) over reusing the
  `TenantAdmin` enum.
- **Iter-2 (promoted):** unified on fixed-size Pod metas, full-
  descriptor Postcard trailing payload, tenant-isolation overlay via
  `claimed_tenant_id`, serializable-at-leader bypassing the Raft log,
  and aggregate metrics.

**Settled:** authz is a descriptor-get helper whose V1 implementation
delegates to the existing `TenantAdminRead` role matrix plus
tenant-isolation. Wire is a dedicated MsgType rather than a
`TenantAdmin` enum extension.

## Promotion criteria met

1. D-set locked at 14 (3-of-3).
2. Guards 16 + 3 convention.
3. Fixtures 12 required + 1 optional.
4. Read-only; no consensus write; no Data Ring change; no Toron
   production change; `crates/nilket` runtime-free.
5. Placement: Exterior Substrate → Wendelon home (ring-product-
   placement; ADR 071 relocation precedent).
6. No-V1 / brand-clean new surface; prior Wendelon ADR guards persist.
7. Reuses the authoritative `TableSchemaCatalogV1`; adds no new
   schema state.

## Conclusion

ADR 072 gives Wendelon a narrow, authoritative, read-only descriptor
producer: leader-fresh, tenant-isolated, latest-by-name, full
descriptor on the trailing section, behind a descriptor-get
authorization helper whose V1 role matrix Toron's `Operator` identity
already satisfies. It unblocks the
Toron bind-miss consumer (ADR 053) without a stub, without a Data Ring
or write-path change, and without touching the Toron tree.

The migration is materialized in
`plans/impl/0072_table_descriptor_snapshot_producer.md`.
