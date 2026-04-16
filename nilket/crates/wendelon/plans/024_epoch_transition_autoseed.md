# Plan: Epoch Transition Auto-Seeding for Lazy Migration

## Summary
This plan defines how migration-ledger seeding becomes an automatic part of the
epoch-transition workflow instead of an external manual step.

The recommended decision is:

- Seeding is a deterministic follow-on action of a successful epoch advance,
  driven by the leader-side Control Ring workflow after the epoch/config change
  is committed.
- Do not initiate new Raft proposals from state-machine apply code.
- Compute reassigned ranges by diffing the prior and new ring projections, then
  submit one `SeedMigration` per reassigned range using deterministic ids.
- `advance_epoch` should not report success until the seed batch has been
  submitted and committed, so the new epoch does not become externally visible
  without its migration ledger entries.

## Motivation
The migration subsystem is already substantial:

- `src/consensus/core.rs` defines `MigrationLedgerEntry`, `SeedMigration`, and
  `MigrationComplete`.
- `src/consensus/raft_storage.rs` already handles seeded entries and chained
  reconciliation correctly.
- `plans/019_lazy_migration.md` assumes seeding happens during epoch
  transitions, but no code path currently performs that orchestration.

Without automatic seeding, lazy migration correctness depends on an external
caller that does not exist. This plan makes epoch transition and migration
ledger creation one coherent workflow.

## Goals
- Define where auto-seeding happens in the epoch-advance sequence.
- Define how reassigned ranges are discovered.
- Define deterministic `migration_id` and `upstream_migration_id` behavior.
- Make the operation idempotent across retries and partial failures.

## Non-Goals
- Background migration sweeping itself.
- Tombstone-aware reclaim.
- Client retry policy for `MIGRATION_INCOMPLETE`.
- Reworking the existing ledger state-machine structures.

## Must Settle
1. **Hook location**
   Seeding is leader-driven after the epoch/config commit succeeds. Do not
   attempt to propose new `SeedMigration` entries from inside `apply_request` or
   any other state-machine apply path.

2. **Visible completion point**
   `advance_epoch` is a compound control-ring operation:
   - commit epoch/config change
   - diff `ring(E)` against `ring(E+1)`
   - submit the deterministic `SeedMigration` batch
   - wait for the batch to commit
   - then return success

3. **Diff unit**
   Seed one migration entry per reassigned contiguous hash range / vnode-owned
   range in the new ring projection, not per key and not per hot read.

4. **Deterministic identity**
   `migration_id` remains deterministic from at least:
   - `to_epoch`
   - `range_start`
   - `range_end`

   Re-running the same transition must reproduce the same identifiers.

5. **Chained migration handling**
   When a new epoch reassigns a range that is already mid-migration, the seeding
   logic must consult the existing ledger and set `upstream_migration_id`
   accordingly so the current reconciliation logic remains correct.

6. **Leader-failure reconciliation**
   If the leader fails after the epoch/config change commits but before the full
   seed batch commits, the next leader must detect that `ring(E+1)` is live
   while the expected seed set is absent or incomplete and finish submitting the
   deterministic seed batch before considering the epoch transition fully
   reconciled.

## Implementation
1. **Capture the old and new ring projections**
   The Control Ring advance path must have both `ring(E)` and `ring(E+1)` in
   scope at the leader when the config change commits.

2. **Compute the reassignment diff**
   Enumerate the ranges whose logical owner changes across the epoch boundary.

3. **Build deterministic ledger entries**
   For each reassigned range, construct a `MigrationLedgerEntry` with:
   - deterministic `migration_id`
   - `from_epoch`, `to_epoch`
   - new logical owner
   - ordered `source_replicas` from the old ring
   - pinned `migration_hlc_floor`
   - `upstream_migration_id` when a predecessor migration is still relevant

4. **Submit the seed batch**
   Propose one `SeedMigration` per entry through the Control Ring leader after
   the epoch commit succeeds.

5. **Make retries safe**
   If the leader crashes after some but not all seeds commit, retrying the same
   epoch transition must be safe because duplicate seeds are already idempotent
   in the state machine.

6. **Reconcile on leader transition**
   When a new leader comes up, it must compare the latest committed epoch
   against the expected seeded migration set. If the committed epoch's seed
   batch is missing or incomplete, the leader resubmits the deterministic batch
   and waits for it to commit.

7. **Update the migration plan doc**
   `plans/019_lazy_migration.md` should be refreshed so the documented workflow
   matches the actual compound epoch-advance sequence.

## Risks and Mitigations
- **Risk: trying to seed from state-machine apply**
  - **Mitigation:** explicitly forbid that shape in this plan. State-machine
    apply stays deterministic and side-effect free with respect to new Raft
    proposals.

- **Risk: partial seeding on leader failure**
  - **Mitigation:** use deterministic ids and idempotent `SeedMigration`
    handling so retries are safe.

- **Risk: chained migrations selecting the wrong source lineage**
  - **Mitigation:** require the seeding logic to consult the existing ledger and
    populate `upstream_migration_id` when a predecessor entry already exists.

## Timeline
- M0: approve this orchestration contract.
- Workstream B: make epoch advance externally reachable.
- Workstream D.1: implement deterministic auto-seeding.
- Workstream D.2+: layer sweeper and reclaim behavior on top of the seeded
  ledger.

## References
- `plans/gap/codex_execution_20260415.md`
- `plans/gap/codex_execution_workstream_D_20260415.md`
- `plans/019_lazy_migration.md`
- `plans/consensus/epoch_transitions.md`
- `src/consensus/core.rs`
- `src/consensus/raft_storage.rs`
