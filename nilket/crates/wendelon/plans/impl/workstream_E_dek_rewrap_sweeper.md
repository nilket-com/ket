# Workstream E6: DEK Re-Wrap Sweeper

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `E`

## Objective

Make tenant-KEK rotation converge historical tiered data instead of leaving old wrapped slab DEKs stranded under prior tenant KEK versions indefinitely.

## Decision

The storage sweeper is the right implementation point.

The pass order is now:

1. plaintext L2 rewrite
2. encrypted DEK re-wrap under the active tenant KEK
3. retention rewrite / retirement

That ordering matters:

- legacy plaintext objects must be encrypted before they can participate in DEK re-wrap
- retention rewrite should see the current encrypted pointer, not race an older tenant-KEK wrapper

## What Landed

- `SweeperConfig.max_dek_rewraps_per_pass` bounds the E6 pass independently of fresh tier uploads and plaintext rewrite
- the sweeper now detects encrypted tiered slabs whose footer `tenant_kek_id` differs from the active tenant KEK for that tenant
- if the replicated tier pointer already carries current `tenant_id` and `tenant_kek_id` metadata, the sweeper skips the L2 download entirely on that pass
- if the object footer is current but the replicated pointer metadata is missing, the sweeper commits a metadata-only backfill without re-emitting the object
- the sweeper downloads the object, decrypts it through the normal manager path, re-encrypts it through the normal tiered-object path, uploads a fresh object, verifies the payload hash, and commits the new `TieredSlab` pointer through Raft
- old objects are deleted only after the new pointer has committed and applied locally
- each successful re-wrap emits `crypto.slab_dek_rewrapped`

## Safety Properties

- the pass is bounded per scan and retryable
- the authoritative tier pointer still moves only through Raft
- stale reads continue to relocate through the manager's tiered-pointer path after the rewrite
- old tenant KEK versions remain readable until the sweeper has converged historical objects

## Operator Contract

- setting `max_dek_rewraps_per_pass = 0` pauses the background re-wrap pass
- rotation success does not mean every historical tiered object has already moved to the new tenant KEK; it means new writes use the new tenant KEK and the sweeper can now converge the backlog
- Wendelon now computes tenant-KEK retirement accounting separately; operators should only retire historical tenant KEK versions once the control surface marks them `safe_to_retire`

## Evidence

- sweeper unit proof: `src/storage/sweeper.rs::sweeper_rewraps_tiered_slabs_to_the_active_tenant_kek`
- node integration proof: `src/node.rs::tenant_kek_rotation_is_raft_committed_and_old_l2_slabs_remain_readable`

## Follow-Ons

- key-age enforcement and operator warnings for stale rotation backlog
- optional metrics for rewrap backlog depth and per-pass convergence rate
