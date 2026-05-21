# Wendelon ADR 073: Table-Query Request Cancellation Producer Contract

**Status:** Implemented 2026-05-21 (3-of-3 — Claude, Codex, Gemini);
evidence in `plans/impl/073_table_query_request_cancellation_evidence.md`.
**Seed:** the deferred half of **Toron ADR 059**'s honesty boundary — 059
made Toron worker cancellation real but explicitly did **not** cancel
already-issued Wendelon storage reads. This ADR is the Wendelon-side
producer primitive a later Toron consumer (ADR 060) calls.
**Council:** drafted via the cross-ring council; **placed in the Wendelon
sequence at promotion** per the ADR 072 precedent (Toron ADR 052 §D15 seeded
072; Wendelon owned it; Toron 053 consumed it). Strongest convergence of the
governance arc — 3-of-3 on the spine and the full D-set. Two settled forks:
the wire **grows the fixed `TableQueryRequestMeta` directly** (Wendelon's
no-premature-backward-compat convention — the trailing-extension idea was
withdrawn); the abort signal is **`tokio_util::sync::CancellationToken`**,
deliberately carrying the ADR 059 review lesson (no hand-rolled `Notify`
lost-wakeup). The Toron no-V1-suffix concern was a non-issue: Wendelon's own
`no-version-suffixes` convention applies (historical `*V1` names are legacy,
not license), so the no-V1 guard is retained.
**Category:** Wendelon storage-ring substrate ADR — data-ring table-query
execution lifecycle. Exterior Substrate → Wendelon home. Node-local
data-server mechanism; reuses the bidirectional data-ring transport; no
Control-Ring RPC, no Toron production change.

## Summary

Toron's `WendelonStorageScan` reads via `TABLE_QUERY_REQ`, and the data
server **executes the plan, concatenates result batches, then writes
`TABLE_QUERY_RESP`** — *materialize-then-respond*. So a cancelled Toron
query that merely drops its client stream is **not observed until the final
write**, after the whole scan already ran. ADR 073 gives Wendelon an
**explicit** cancellation primitive: a Wendelon-owned, per-attempt
`TableQueryRequestId` carried in the request meta; a data-server
**active-request registry** keyed `(TenantId, TableQueryRequestId)`; a
`TABLE_QUERY_CANCEL_REQ`/`RESP` pair on the existing bidi data ring; a
**`CancellationToken`** checked at execution boundaries that aborts the scan
**before** completing it. Best-effort, checkpoint-bounded, oracle-safe, no
rollback (reads have no side effects). Stop-on-client-drop is retained only
as a fallback cleanup/metric path. **Producer-only** — the end-to-end claim
waits for Toron ADR 060.

## Motivation

ADR 059 closed the Toron-internal half of cancellation; its honesty boundary
left already-issued Wendelon storage work running. Because the table-query
path materializes before responding, the implicit "client gone" signal fires
too late to free disk I/O / memory / CPU. Wendelon must expose an explicit
abort handle so a cancelled Toron query promptly reclaims storage-ring
resources — the symmetric completion of 059, on the ring that owns the read.

## Decisions (19, Promotion-Locked)

**D1 Placement.** Wendelon producer ADR, seeded by Toron ADR 059; Toron
consumer integration is a later Toron ADR (060). Exterior Substrate →
Wendelon home. No Nilket runtime change.

**D2 Scope.** `TABLE_QUERY_REQ` cancellation only. Out: legacy `QUERY_REQ` /
`QUERY_STREAM`, `AGG_REQ`, writes, table delete, CDC, descriptor/admin RPCs,
sweepers, compaction, SQL grammar, Toron production wiring.

**D3 Request identity.** Add `TableQueryRequestId` — a Wendelon-owned,
client-generated 128-bit newtype, scoped `(TenantId, TableQueryRequestId)`.
It is represented in the fixed Pod wire meta as a `u128`-compatible value;
zero is rejected by validation, not by using a non-Pod `NonZeroU128` field.
Bare request-id lookup is forbidden.

**D4 No cross-ring identity import.** Wendelon wire/registries carry no Toron
`QueryId`, `TaskIdentity`, SQL text, or Toron cancellation token; and do
**not** reuse `ClientWriteId` (write idempotency ≠ read cancellation).

**D5 Per-attempt semantics.** Each bounded table-query retry attempt gets a
fresh `TableQueryRequestId`; cancellation targets the in-flight attempt; a
retry uses a new id. (Stateless server tracking across retries.)

**D6 Fixed wire change.** `TableQueryRequestMeta` grows directly to include
`TableQueryRequestId` (≈120→136 bytes), and its fixed-size layout guard is
updated. **No** optional/trailing extension and **no** backward-compat
uncancellable path — Wendelon's no-premature-backward-compat convention; the
internal pre-V1 Toron↔Wendelon wire may change.

**D7 Cancel wire.** Add `TABLE_QUERY_CANCEL_REQ = 0x3E` and
`TABLE_QUERY_CANCEL_RESP = 0x3F` on the existing bidirectional data-ring
transport. **Not** `0x1F` (already `AUDIT_CHAIN_HEAD_RESP`). No Control-Ring
RPC, sidecar listener, unidirectional stream, or transport-abstraction
change. Table-query-specific names (no generic `CANCEL_REQ` implying broader
coverage).

**D8 Active registry.** The data server registers cancellable table-query
work keyed `(TenantId, TableQueryRequestId)` **before visible execution** and
removes it via **RAII** on every terminal path (success, error, cancel,
budget failure, encode failure, stream close, panic-unwind, shutdown).

**D9 Early-cancel tombstone.** A cancel can arrive before the matching query
registers (separate stream). A **bounded** (TTL + count) pending-cancel
tombstone keyed `(TenantId, TableQueryRequestId)`, **consumed on
registration** and evicted when stale, handles the race — no unbounded map
(the ADR 059 discipline).

**D10 Signal correctness.** Active-request cancellation uses
**`tokio_util::sync::CancellationToken`** — **no** hand-rolled
`Notify`/`notify_waiters()` loop that can lose wakeups (the ADR 059 review
lesson, carried forward by construction). The new `tokio-util` dependency is
justified by introducing a runtime cancellation substrate.

**D11 Idempotence.** Cancel is idempotent for active, pending,
already-cancelled, completed, missing, and tenant-invisible ids.

**D12 Oracle safety.** The cancel response is tenant-scoped and
non-enumerating: `ACCEPTED` means an active request was signalled or a bounded
tombstone was recorded; `TERMINAL` means the server can safely treat the
request as completed / already-cancelled / tenant-invisible. A tenant-scoped
unknown id may be tombstoned and return `ACCEPTED` to cover the
cancel-before-register race. Internal metrics may distinguish richer
outcomes; the **wire must not** reveal cross-tenant query existence, and
callers must not infer existence from either status.

**D13 Abort granularity.** Best-effort, checkpoint-bounded at Wendelon-owned
boundaries: post-decode/pre-plan, around migration-pull, per scan
range/location, before secondary-index fallback, before row-group
materialization, before PBC over rebatch groups, before concat, before Arrow
encode, before response write. **Not** threaded into FSST/decompression
kernels; deeper checkpoints only where a fixture proves coarse ones are too
delayed. **No durable rollback** (reads are side-effect-free).

**D14 Original response status.** Add `ReadStatus::QUERY_CANCELLED` (next
unused value). A query that observes cancel before final emission and whose
original stream is still writable returns `TABLE_QUERY_RESP` with
`QUERY_CANCELLED` and **no Arrow body**; if the stream is already gone,
cleanup + metrics run with no response.

**D15 Stream-drop fallback.** A client stream write/finish failure is treated
as client cancellation for cleanup + aggregate metrics (the CDC
`client_cancel` pattern), but it is **not** the primary prompt mechanism.

**D16 Resource release.** Admission permits, temporary buffers, BufPool-owned
frames, row-group/materialization scratch, and stream resources release
through normal RAII ownership paths after cancellation — no BufPool leak.

**D17 Metrics.** Aggregate-only. Trigger/outcome/source labels allowed;
tenant, table, request id, node id, descriptor fingerprint, endpoint, SQL
text, and peer labels forbidden.

**D18 Audit boundary.** Existing query audit may record a cancelled
completion with existing identity hygiene; the deferred audit subsystem is
**not** pulled in.

**D19 Producer-only boundary.** ADR 073 does not modify Toron production code
or claim end-to-end storage cancellation — that waits for Toron ADR 060. It
may add a client-facing cancel helper (e.g.
`cancel_table_query_attempt(target, tenant_id, request_id)`) the consumer
calls later.

**Single-plan posture; Wendelon-ring only; node-local.**

## Architecture Guards (22 + meta, Promotion-Locked)

1. `adr073_table_query_cancel_is_wendelon_home_producer_only`
2. `adr073_table_query_request_id_is_wendelon_owned_not_toron`
3. `adr073_table_query_request_id_is_required_fixed_meta`
4. `adr073_no_table_query_meta_extension_or_compat_path`
5. `adr073_table_query_cancel_msg_types_are_3e_3f_not_1f`
6. `adr073_cancel_wire_uses_data_ring_bidi_not_control_plane`
7. `adr073_no_uni_stream_sidecar_listener_or_transport_redesign`
8. `adr073_active_table_query_registry_is_tenant_scoped_raii_removed`
9. `adr073_cancel_before_register_tombstone_is_bounded_and_consumed`
10. `adr073_cancellation_signal_uses_cancellation_token`
11. `adr073_cancel_is_idempotent_for_terminal_and_missing_requests`
12. `adr073_cancel_response_is_oracle_safe`
13. `adr073_table_query_checks_cancel_at_execution_boundaries`
14. `adr073_cancelled_table_query_has_stable_read_status`
15. `adr073_stream_drop_is_fallback_not_primary_mechanism`
16. `adr073_cancel_releases_admission_buffers_and_scratch`
17. `adr073_metrics_are_aggregate_only`
18. `adr073_no_writes_cdc_admin_agg_or_legacy_query_scope`
19. `adr073_no_toron_storage_scan_consumer_claim`
20. `adr073_nilket_root_stays_runtime_free`
21. `adr073_fixture_worker_source_unchanged`
22. `adr073_new_surface_is_brand_clean_and_has_no_v1_suffix`

**Meta guard:** `adr073_prior_wendelon_protocol_and_architecture_guards_continue_to_pass`.

(The no-V1 guard is retained: Wendelon's `plans/conventions/no-version-suffixes.md`
applies to new types; historical `*V1` names are legacy, not license.)

## Integration Fixtures (19 required)

1. `table_query_request_meta_requires_nonzero_request_id`
2. `table_query_cancel_active_request_returns_query_cancelled`
3. `table_query_cancel_before_register_tombstone_is_consumed`
4. `table_query_cancel_missing_is_oracle_safe`
5. `table_query_cancel_cross_tenant_is_oracle_safe`
6. `table_query_cancel_after_completion_is_idempotent`
7. `table_query_cancel_does_not_use_audit_chain_head_msg_type`
8. `table_query_cancel_during_migration_pull_releases_resources`
9. `table_query_cancel_during_scan_stops_before_remaining_locations`
10. `table_query_cancel_during_projected_materialization_stops_at_checkpoint`
11. `table_query_cancel_before_concat_or_encode_suppresses_arrow_body`
12. `table_query_cancel_releases_admission_permit`
13. `table_query_cancel_releases_bufpool_and_scratch`
14. `table_query_cancel_signal_observes_cancel_before_wait`
15. `table_query_cancel_metrics_are_aggregate_only`
16. `table_query_retry_uses_fresh_request_id_per_attempt`
17. `stream_write_failure_records_client_cancel_cleanup`
18. `legacy_query_req_and_agg_req_are_unchanged`
19. `toron_storage_scan_does_not_claim_storage_cancel_yet`

## Out of Scope

- Toron consumer wiring (ADR 060) and any Toron production change.
- Legacy `QUERY_REQ` / `QUERY_STREAM` / `AGG_REQ`; writes / table-delete /
  descriptor-admin; CDC subscription semantics; sweepers / compaction.
- Control-Ring cancel RPC, unidirectional stream, sidecar listener, or any
  transport-abstraction change.
- Importing Toron identities/tokens; SQL grammar / textual cancel.
- The deferred audit subsystem.
- Durable / rollback cancellation (reads are side-effect-free).

## Council History

- **iter-0:** all three on the topic; Claude leaned implicit stop-on-drop,
  Codex/Gemini flagged it as insufficient.
- **iter-1:** Codex's audit was decisive — `TABLE_QUERY_REQ` materializes
  *then* responds, so implicit drop is too late; the **explicit registry +
  cancel wire is the spine**. Claude conceded; the design crystallized as
  **Toron 059 ported to Wendelon**. Claude carried forward the 059 Notify
  lost-wakeup lesson.
- **iter-2:** wire-evolution and signal forks. Claude initially read
  Wendelon's historical `*V1` types as license for a `MetaExtV1` extension;
  **corrected** — Wendelon has a `no-version-suffixes` convention, so the
  no-V1 guard stays and the meta **grows directly** (no extension).
- **iter-3 (3-of-3):** genuine full convergence — fixed-meta growth,
  `0x3E`/`0x3F` (avoiding `0x1F` audit), `CancellationToken`, per-attempt id,
  identical guard set. The strongest-aligned promotion of the arc.

## Promotion criteria met

1. D-set 19, **3-of-3** (Claude + Codex + Gemini).
2. Guards 22 + meta; new-surface no-V1 retained (Wendelon convention);
   prior Wendelon protocol/architecture guards preserved.
3. Fixtures 19.
4. Wendelon-ring; reuses the data-ring bidi transport; no Control-Ring /
   transport-abstraction change; no Toron production change.
5. Placement: Exterior Substrate → Wendelon home (072 precedent); Toron
   consumer = ADR 060, cross-referenced not owned.
6. Carries the ADR 059 cancellation lessons (CancellationToken, bounded
   tombstone, oracle-safe, RAII, no-rollback) onto the storage ring.

## Conclusion

ADR 073 gives Wendelon the explicit, checkpoint-bounded, oracle-safe
primitive to abort an in-flight `TABLE_QUERY_REQ` before it completes —
freeing disk I/O, memory, and CPU. It reuses the data-ring bidi wire, owns
its identity and registry, and carries the 059 cancellation discipline onto
the storage ring without claiming the end-to-end story, which Toron ADR 060
will complete. Migration:
`plans/impl/073_table_query_request_cancellation.md`.
