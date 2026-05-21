# ADR 054: Reactive Descriptor Invalidation

**Status:** Promoted
**Seed:** ADR 053 §D6/§"Out of Scope" — the deferred other half of
descriptor liveness: an already-cached descriptor that Wendelon later
evolved or dropped, left fail-closed by 053.
**Council:** Claude, Codex, Gemini (iter-0 split — Claude+Gemini led
Governance Phase 3, Codex led descriptor refresh; iter-1 Claude
withdrew the governance lead after verifying the evolution loop is a
permanent-failure hole and **contested bundling governance into 054**;
iter-2 dual converged the mechanism on invalidate-and-reuse-053 and
resolved the open questions; iter-3/iter-4 reached 3-of-3 on the
**split** — Gemini conceded governance → ADR 055 — and on Codex's
final D-set, which absorbed the replacement-object guard,
path-agnostic invalidation, and the distributed-identity resolution).
**Category:** Toron compute-ring substrate ADR — catalog bind/exec
descriptor liveness. Exterior Substrate → Toron home. Node-local;
consumes the Wendelon ADR 072 producer through ADR 053; no Wendelon
server change, no SQL wire change.

## Summary

When execution proves a cached descriptor stale (`DescriptorEvolved`),
Toron **invalidates** the stale local registry entry and lets ADR 053's
existing governed bind-miss sync re-fetch Wendelon's current truth on
the retry bind. **Refresh = invalidate + 053 re-sync.** No new fetch
path, no new budget, no TTL/reaper/watch/push. This closes the gap ADR
053 left open: a discovered table that later evolves currently
re-binds to the *same stale cache entry forever* → `SchemaInstability`
until daemon restart. A drop-and-recreate (different `TableObjectId`)
is **not** a transparent re-bind — the current statement fails closed
(`SchemaInstability`); a later statement binds the new object cleanly.
Governance Phase 3 is explicitly **not** here — it reverses the ADR
050/052 hard-reject posture and is ADR 055.

## Motivation

ADR 053 made a *missing* table self-heal (bind-miss → sync) but is
**insert-only**: it never refreshes an already-cached binding. The
existing SELECT retry loop (`coordinator.rs:879-894`) re-binds on
`DescriptorEvolved`, but the re-bind's `resolve_table` returns the
**same stale cached descriptor** → second `DescriptorEvolved` →
`SchemaInstability`, and the cache stays stale until restart. So a
table 053 made discoverable becomes *worse off* once it evolves. ADR
054 is the symmetric completion — 053 = miss→sync; 054 =
stale→invalidate→sync — built entirely on the 053/072 seam.

## Decisions (17, Promotion-Locked)

**D1: Placement.** Toron Exterior Substrate — catalog / coordinator /
storage error handling. No Wendelon server or SQL wire change;
`crates/nilket` runtime-free; `toron_fixture_worker.rs` unchanged.

**D2: Trigger.** The only automatic refresh trigger is execution-time
`DescriptorEvolved`. **No** TTL, watch, inventory scan, reaper, push
invalidation, or forced-refresh RPC.

**D3: Local identity.** `StorageError::DescriptorEvolved` carries the
stale local `TableId` (was a unit variant, error.rs:14).

**D4: Distributed identity.** `TaskFailureKind::DescriptorEvolved`
(error/distributed.rs:12) also carries the stale local `TableId`;
fingerprints remain diagnostic but are **not** sufficient to drive
invalidation.

**D5: Contextual mapping.** Context-free conversion from Wendelon
`ClientError::TableDescriptorEvolved` (error.rs:54) cannot supply the
table id; storage call sites map it explicitly with the in-scope
`request.table_id`.

**D6: Catalog API.** Add `Catalog::invalidate_table_descriptor(table_id)
-> Result<Option<InvalidatedDescriptor>, CatalogError>`, no silent
default, returning the removed `{ table_id, table_object_id,
fingerprint, refs }` when present. The removed **refs** + **object id**
feed the replacement guard (D12).

**D7: Registry semantics.**
`WendelonDescriptorRegistry::invalidate_descriptor_for_tenant(tenant,
table_id)` removes, under the write lock, only the scoped tenant's
`by_id`, descriptor payload, and every `by_ref` alias (qualified +
unqualified) for that table id; idempotent; **never** bumps
`next_table_id`, calls descriptor sync / `from_tenant_descriptors`,
holds the lock across an await, or touches another tenant.

**D8: Re-sync reuse.** After invalidation the coordinator re-binds
normally; the local miss enters ADR 053's single-flight, budgeted
descriptor sync. No separate refresh RPC path; no `force_refresh` flag
on `resolve_table`.

**D9: Path-agnostic invalidation.** SELECT, write, delete, and CTAS
paths all **invalidate** on `DescriptorEvolved`. Only SELECT
auto-retries.

**D10: SELECT retry bound.** SELECT keeps one evolution retry: first
`DescriptorEvolved` invalidates + re-binds; a second in the same
statement returns `SchemaInstability`.

**D11: Write posture.** Writes/deletes/CTAS invalidate the stale
descriptor before returning the error; they do **not** auto-retry
(replay can duplicate side effects). If invalidation itself fails,
surface that failure because the cache was not repaired. Otherwise the
client's next attempt binds fresh through ADR 053. (Preserves ADR 053
D11 "no write retry.")

**D12: Replacement-object guard.** If, after invalidate + re-bind, an
invalidated ref resolves to a **different** `TableObjectId` for the
same statement, fail with `SchemaInstability` — do not cross object
identity mid-statement. The new object stays registered for a *later*
statement.

**D13: Missing after invalidation.** If the old refs no longer resolve
(dropped, no replacement), the re-bind surfaces the normal
`TableNotFound`; Toron never synthesizes a stale success.

**D14: Local ids are cache handles.** A refresh may allocate a fresh
local `TableId`; Wendelon object identity is the preserved contract.
In-flight holders of the old id fail closed because the old entry is
gone (D7); the guard (D12) protects statement identity.

**D15: Observability.** Aggregate-only sibling counter
`catalog_descriptor_refresh_requests_total` with bounded `outcome`
values `invalidated`, `missing`, and `replacement`; ADR 053's sync
metrics remain the source for fetch outcomes. No tenant / table /
object / fingerprint labels. Outcomes are independent event counters,
not mutually exclusive per refresh attempt; a drop-recreate path may
record both `invalidated` and `replacement`.

**D16: Governance reuse, not extension.** Re-sync uses ADR 053's
`max_concurrent_descriptor_syncs` admission, single-flight,
waiter-no-budget rule, and `ResourceExhausted` mapping. **No** new QPS,
WFQ, queueing, borrowing, or tenant-scheduler knob.

**D17: ADR 055 handoff.** Governance Phase 3 is the next candidate,
explicitly framed as the ADR 050 D7 / ADR 052 D3-D14 hard-reject
reversal — its own cycle, not a 054 appendix.

**Single-plan posture; Toron-ring only.**

## Architecture Guards (14, Promotion-Locked)

1. `adr054_descriptor_evolved_carries_local_table_id` (local + distributed)
2. `adr054_wendelon_evolved_error_mapped_with_request_context`
3. `adr054_trigger_is_descriptor_evolved_only`
4. `adr054_invalidation_removes_by_id_payload_and_all_refs`
5. `adr054_invalidation_is_tenant_scoped_and_idempotent`
6. `adr054_select_retry_reuses_adr053_bind_miss_sync`
7. `adr054_no_force_refresh_resolve_table_flag`
8. `adr054_invalidation_is_path_agnostic_select_write_delete`
9. `adr054_writes_invalidate_but_do_not_retry`
10. `adr054_replacement_object_returns_schema_instability`
11. `adr054_second_evolution_returns_schema_instability`
12. `adr054_no_ttl_reaper_watch_inventory_or_push`
13. `adr054_no_new_budget_or_governance_knob`
14. `adr054_no_wendelon_server_or_sql_wire_change`

**Convention guards:** `adr054_nilket_root_stays_runtime_free`,
`adr054_fixture_worker_source_unchanged`,
`adr054_new_surface_is_brand_clean_and_has_no_v1_suffix`.

**Prior-guard reconciliation:** retarget ADR 053's
`descriptor_evolved_path_stays_unchanged` to the narrower invariant —
no storage/background/write *retry*, but SELECT runtime invalidation is
now allowed and pinned. ADR 049-053 meta-guard persists.

## Integration Fixtures (13 required + 1 optional)

1. `local_descriptor_evolved_carries_stale_table_id`
2. `distributed_descriptor_evolved_carries_stale_table_id`
3. `invalidate_removes_qualified_unqualified_by_id_and_payload`
4. `invalidate_is_idempotent`
5. `select_evolved_invalidates_rebinds_resyncs_once_and_succeeds`
6. `select_refresh_reuses_adr053_singleflight`
7. `select_refresh_over_budget_returns_resource_exhausted`
8. `dropped_object_after_invalidation_returns_table_not_found`
9. `drop_and_recreate_returns_schema_instability_for_current_statement`
10. `second_evolution_in_one_select_returns_schema_instability`
11. `write_descriptor_evolved_invalidates_and_returns_error_without_retry`
12. `refresh_metrics_have_no_tenant_or_table_labels`
13. `governance_knobs_absent_no_qps_wfq_queue_borrow` (split fence)

**Optional:** `concurrent_stale_selects_share_one_descriptor_get`.

## Out of Scope

- TTL / background reaper / watch / inventory / push invalidation;
  forced-refresh RPC; `force_refresh` flag on `resolve_table`.
- In-place same-object overwrite (use invalidate + re-sync).
- Write/delete auto-retry (D11).
- Governance Phase 3 — QPS, WFQ, queueing, borrowing, fairness,
  connection caps (→ ADR 055, the 050/052 hard-reject reversal).
- Any Wendelon server change; SQL wire / DDL / new SQL error code.

## Council History

- **Iter-0:** Claude+Gemini led Governance Phase 3; Codex led
  descriptor refresh.
- **Iter-1:** Claude verified the evolution loop is a permanent-failure
  hole (re-bind returns the same stale entry → `SchemaInstability`
  forever), withdrew the governance lead, and **contested the bundle** —
  WFQ reverses the ADR 050 D7 / 052 D3-D14 hard-reject exclusions and
  earns its own ADR. Codex converged to the invalidate-and-reuse-053
  mechanism (dropping his iter-0 TTL/reaper).
- **Iter-2 (dual):** mechanism settled (invalidate + reuse 053, no
  TTL/force-flag/overwrite); resolved path-agnostic invalidation,
  precise table-id, replacement guard, sibling metric; flagged the
  distributed-identity wrinkle.
- **Iter-3/4 (promoted):** 3-of-3 on the split (Gemini conceded
  governance → ADR 055); Codex's final D-set carried the
  replacement-object guard and resolved the distributed wrinkle by
  carrying the `TableId` on the task failure.

**Settled:** refresh = invalidate + reuse-053 (no new mechanism);
invalidation path-agnostic, retry SELECT-only; drop-and-recreate fails
closed; governance split out.

## Promotion criteria met

1. D-set locked at 17 (3-of-3, split included).
2. Guards 14 + 3 convention; ADR 053 evolved-path guard retargeted;
   ADR 049-053 meta-guard.
3. Fixtures 13 required + 1 optional.
4. Toron-ring; reuses 053/072; no Wendelon server / SQL wire change.
5. Placement: Exterior Substrate → Toron home.
6. Brand-clean / no-`*V1`; `toron_fixture_worker.rs` unchanged;
   `crates/nilket` runtime-free.
7. ADR 049-053 invariants preserved; insert-only 053 path now completed
   by reactive invalidation, fail-closed semantics retained.

## Conclusion

ADR 054 completes descriptor liveness symmetrically: 053 heals a
*missing* table, 054 heals a *stale* one — both by reactive pull on a
runtime signal through the one 072 RPC, never a Toron-managed mirror.
Refresh is invalidate-then-reuse-053, governed by 053's budget,
fail-closed on drop-and-recreate. Governance Phase 3 — the hard-reject
reversal — is correctly deferred to ADR 055.

The migration is materialized in
`plans/impl/054_reactive_descriptor_invalidation.md`.
