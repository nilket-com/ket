# Wendelon Execution Workstream D — Migration, Leases, And Tiering Closure

Author: Codex
Source roadmap: `plans/gap/codex_execution_20260415.md`

## Purpose

This document decomposes Workstream D into implementation-ready packets.

Workstream D exists to close the remaining automation, authenticity, and tiering
gaps in subsystems that already have substantial groundwork in code.

## Workstream Objective

- finish migration orchestration, lease authenticity, and tiering closure work

## Entry Criteria

- Workstream B is materially complete
- Workstream C has data-ring reachability in place
- `plans/024_epoch_transition_autoseed.md` exists and is accepted

## Exit Criteria

- epoch transitions seed migrations automatically
- background migration and reclaim paths exist
- lease and transfer authenticity are enforced
- tiering is no longer limited to the current local-filesystem-shaped baseline

## Ordering

1. D.1 epoch-transition auto-seeding
2. D.2 background migration sweeper
3. D.3 tombstone-aware reclaim
4. D.4 lease signing and verification
5. D.5 transfer-token enforcement
6. D.6 client `MIGRATION_INCOMPLETE` retry
7. D.7 remote object-store backend
8. D.8 L2 rehydration policy and implementation

## Work Packets

### D.1 Epoch-transition auto-seeding

Objective:
- emit migration seeds automatically when ownership moves from `ring(E)` to `ring(E+1)`

Size:
- `S`

Exact files/modules expected to change:
- `src/consensus/core.rs`
- `src/consensus/raft_storage.rs`
- `src/network/ring.rs`
- `plans/019_lazy_migration.md`
- `tests/epoch_transition_seed.rs`

Explicit invariants that must become true:
- each reassigned range gets one deterministic seed path
- replay or retry does not duplicate logical migration state
- chained migration reconciliation continues to hold

Explicit non-goals:
- background sweeping itself
- client retry behavior
- tier rehydration policy

Dependencies:
- `plans/024_epoch_transition_autoseed.md`
- Workstream B epoch-advance reachability

Tests that must pass or be added:
- add `tests/epoch_transition_seed.rs`
- reassigned-range coverage
- idempotent replay coverage

What mini-plan, if any, governs the change:
- `plans/024_epoch_transition_autoseed.md`

What assessment gap or milestone item it closes:
- M5 auto-seeding
- gap: migration seeds are not emitted automatically on epoch transition

### D.2 Background migration sweeper

Objective:
- add the background task that drains pending migrations without manual intervention

Size:
- `M`

Exact files/modules expected to change:
- `src/network/data_server.rs`
- `src/consensus/core.rs`
- `src/consensus/raft_storage.rs`
- `tests/migration_sweeper.rs`

Explicit invariants that must become true:
- pending migration entries can drain automatically
- source failover and backoff are explicit
- background migration does not saturate the cluster without a rate limit

Explicit non-goals:
- compaction and reclaim
- client retry behavior
- remote object-store work

Dependencies:
- D.1
- Workstream C base data-ring reachability

Tests that must pass or be added:
- add `tests/migration_sweeper.rs`
- completed-drain coverage
- source-failure failover coverage

What mini-plan, if any, governs the change:
- `plans/024_epoch_transition_autoseed.md` for orchestration assumptions

What assessment gap or milestone item it closes:
- M5 migration sweeper automation

### D.3 Tombstone-aware reclaim and compaction

Objective:
- reclaim bytes after migration completion without violating time-travel or ownership guarantees

Size:
- `M`

Exact files/modules expected to change:
- `src/storage/manager.rs`
- `src/storage/core.rs`
- `src/consensus/core.rs`
- `src/storage/sweeper.rs`
- `tests/tombstone_compaction.rs`

Explicit invariants that must become true:
- migrated-away ranges can be physically reclaimed after the correct floor
- locally owned live versions are preserved
- historical reads remain correct through the new owner path

Explicit non-goals:
- remote object-store integration
- DataFusion work
- rolling-upgrade compatibility

Dependencies:
- D.2

Tests that must pass or be added:
- add `tests/tombstone_compaction.rs`
- reclaim correctness
- historical read correctness

What mini-plan, if any, governs the change:
- none beyond D.1 assumptions

What assessment gap or milestone item it closes:
- M5 tombstone-aware physical reclaim

### D.4 Lease signing and verification

Objective:
- replace zero-filled lease signatures with real verification semantics

Size:
- `M`

Exact files/modules expected to change:
- `src/schema/core.rs`
- `src/network/data_server.rs`
- `src/consensus/raft_storage.rs`
- `src/node.rs`
- `tests/lease_signing.rs`

Explicit invariants that must become true:
- forged lease descriptors do not pass validation
- verification occurs where leases are actually trusted
- key material is loaded explicitly rather than assumed

Explicit non-goals:
- rolling key management UX
- multi-tenant security policy
- TLS SAN enforcement

Dependencies:
- the config and key-material slice of Workstream E.1, which may be executed
  before the rest of M6

Tests that must pass or be added:
- add `tests/lease_signing.rs`
- valid signature path
- wrong-key and tampered-signature rejection

What mini-plan, if any, governs the change:
- add a short plan if the key-id or rotation story is not settled during implementation

What assessment gap or milestone item it closes:
- M5 signed lease authenticity
- gap: `LeaseDescriptor.signature` is currently zero-filled

### D.5 Transfer-token enforcement

Objective:
- enforce signed transfer-token validation on the leadership-transfer path

Size:
- `S`

Exact files/modules expected to change:
- `src/schema/core.rs`
- `src/consensus/raft_storage.rs`
- control-ring server module under `src/network/`
- `tests/transfer_token_verification.rs`

Explicit invariants that must become true:
- unsigned or invalid transfer tokens are rejected
- token validation is wired into the actual receive path, not just schema definitions

Explicit non-goals:
- lease acquisition policy changes
- transport identity work
- backup/restore work

Dependencies:
- D.4
- Workstream B leadership transfer path

Tests that must pass or be added:
- add `tests/transfer_token_verification.rs`
- valid and invalid token coverage

What mini-plan, if any, governs the change:
- none if lease-signing decisions already cover the token format and keys

What assessment gap or milestone item it closes:
- M5 transfer authenticity
- gap: `TransferToken` enforcement is not wired today

### D.6 Client `MIGRATION_INCOMPLETE` retry

Objective:
- let the client treat temporary migration incompleteness as a retryable state

Size:
- `S`

Exact files/modules expected to change:
- `src/client/core.rs`
- `src/schema/core.rs`
- `tests/client_migration_retry.rs`

Explicit invariants that must become true:
- temporary migration incompleteness can resolve transparently within the retry budget
- callers only see the failure once retry policy is exhausted

Explicit non-goals:
- migration seeding
- background sweeper behavior
- topology push subscription

Dependencies:
- Workstream C client API work
- D.2 preferred first

Tests that must pass or be added:
- add `tests/client_migration_retry.rs`
- retry-until-success behavior
- exhausted-budget behavior

What mini-plan, if any, governs the change:
- none

What assessment gap or milestone item it closes:
- M5 client migration retry semantics

### D.7 Remote object-store backend

Objective:
- add a real remote `ObjectStore` implementation for tiering

Size:
- `M`

Exact files/modules expected to change:
- `src/storage/l2.rs`
- `src/storage/sweeper.rs`
- `src/storage/manager.rs`
- `tests/l2_s3.rs`

Explicit invariants that must become true:
- tiering can use a remote object-store backend rather than only local filesystem semantics
- upload and fetch behavior is observable and retry-aware

Explicit non-goals:
- rehydration heuristics
- backup/restore
- multi-cloud provider abstraction beyond the initial target

Dependencies:
- none at the code level
- Workstream E observability is preferred so the path is measurable

Tests that must pass or be added:
- add `tests/l2_s3.rs`
- round-trip object-store integration coverage

What mini-plan, if any, governs the change:
- none unless backend selection remains unsettled

What assessment gap or milestone item it closes:
- M7 remote L2 backend

### D.8 L2 rehydration policy and implementation

Objective:
- move hot tiered data back toward L1 under an explicit, non-thrashing policy

Size:
- `L`

Exact files/modules expected to change:
- `src/storage/manager.rs`
- `src/storage/sweeper.rs`
- `src/storage/l2.rs`
- `tests/l2_rehydration.rs`

Explicit invariants that must become true:
- rehydration is driven by an explicit policy rather than ad hoc reads
- promoted slabs do not immediately thrash back and forth between tiers
- read behavior remains correct while promotion is happening

Explicit non-goals:
- new transport providers
- query spill control
- backup/export semantics

Dependencies:
- D.7

Tests that must pass or be added:
- add `tests/l2_rehydration.rs`
- hot-read promotion coverage
- thrash-guard behavior

What mini-plan, if any, governs the change:
- create `plans/025_l2_rehydration.md` first

What assessment gap or milestone item it closes:
- M7 rehydration and tiering hardening
