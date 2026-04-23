# Workstream E: Tenant KEK Retirement Accounting And Destructive Retirement

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `E`

## Objective

Make historical tenant-KEK retirement a measurable decision instead of a guess, then enforce destructive retirement through the same replicated evidence.

## Decision

Retirement accounting should be derived from replicated tier metadata, not ad hoc object-store scans.

That means:

- each replicated tier pointer carries `tenant_id`
- each encrypted replicated tier pointer carries `tenant_kek_id`
- background sweeps backfill missing metadata on older encrypted pointers as they touch them

This keeps restart behavior, control-plane reads, and operator decisions consistent.

## What Landed

- `TieredSlab` and `TieredSlabPointer` now carry optional `tenant_id` / `tenant_kek_id` metadata
- fresh tiering, plaintext rewrite, DEK re-wrap, retention rewrite, and recovery reconciliation all preserve that metadata
- the local persistent tier index now stores the same metadata with backward-compatible decoding for older rows
- `TenantAdmin::Get` and `TenantAdmin::List` now return:
  - `tenant_kek_accounting_complete`
  - `tenant_kek_versions[]`
  - per-version `tiered_slab_ref_count`
  - per-version `safe_to_retire`
- `TenantAdmin::RetireTenantKek { target_tenant_id, tenant_kek_id }` now commits a destructive retirement through Raft
- the state machine rejects retirement when:
  - the target KEK is still active
  - replicated tier metadata still references it
  - retirement accounting is incomplete
- post-commit success is gated on local `CryptoManager` convergence so the leader does not report success while it still holds the retired KEK in memory

## Current Contract

- `safe_to_retire = true` means:
  - the KEK version is not the active tenant KEK
  - replicated tier metadata shows zero encrypted tiered slab references to it
  - accounting is complete enough to trust the answer
- `safe_to_retire = false` means either:
  - the KEK is still active
  - one or more encrypted tiered slabs still reference it
  - accounting is incomplete because older encrypted pointers have not been backfilled yet

## Deliberate Non-Goals

- automatic deletion of historical tenant KEK versions based purely on age or timers
- formal key-age policy enforcement

Those should sit on top of this accounting layer, not be entangled with it.

## Evidence

- control-plane unit proof: `src/network/control_server.rs::tenant_kek_versions_wire_marks_old_unreferenced_keys_safe_to_retire`
- control-plane safety proof: `src/network/control_server.rs::tenant_kek_versions_wire_blocks_retirement_when_refs_or_metadata_gaps_exist`
- wire integration proof: `tests/control_ring_rpc.rs::tenant_admin_rotate_tenant_kek_commits_and_returns_new_active_version`
- state-machine destructive proof: `src/consensus/raft_storage.rs::tenant_kek_tests::retire_tenant_kek_removes_unreferenced_historical_version`
- state-machine safety proofs:
  - `src/consensus/raft_storage.rs::tenant_kek_tests::retire_tenant_kek_rejects_active_version`
  - `src/consensus/raft_storage.rs::tenant_kek_tests::retire_tenant_kek_rejects_referenced_historical_version`
- control-ring destructive proof: `tests/control_ring_rpc.rs::tenant_admin_retire_tenant_kek_removes_safe_historical_version`
