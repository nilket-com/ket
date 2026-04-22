# ADR 033: Backup, Restore, And Node-Replacement Architecture

## Status
Proposed

## Context
ADR `001` intentionally stopped Wendelon at autonomous eviction and mathematical healing. That was the right cut for keeping infrastructure provisioning out of the database binary, but it left a separate architectural surface unresolved:

- what counts as a real backup
- what counts as a real restore
- what the supported node-replacement contract is
- how commodity day-one operation differs from later automation and higher-availability uplift

The planning cycle made the current gap explicit: node-image folklore and ad hoc restore steps are not an architecture. Wendelon needs a real recovery model even if the first implementation is scripted rather than fully operator-driven.

## Decision

1. **Backups are cluster-scoped consistent cuts, not node-image anecdotes.**
   The supported backup story is a cluster-consistent cut at a stable control-log point that can restore:
   - the authoritative control state
   - the storage metadata needed to reconstitute service
   - the storage data required for that recovered control point

2. **Backup storage must be in a separate failure domain from live storage.**
   Backup artifacts must not live in the same logical failure domain as the live L2 tier they protect.

3. **Restore is a first-class supported workflow.**
   Wendelon must support:
   - restoring the control state
   - reattaching or revalidating storage state
   - validating remote-tier/object-store references required by the restored cluster view
   - verifying that restored cluster state is usable before the cluster is declared ready

4. **Node replacement is an explicit contract, not an implicit hope.**
   Wendelon itself still does not provision hardware or cloud instances. That remains outside the database boundary per ADR `001`.

   But Wendelon does require a supported replacement path:
   - either a first-party operator/controller
   - or a tightly specified external automation contract with clear inputs, outputs, and expected timing
   - and a defined sequence for removing the failed member identity, introducing the replacement node, and verifying replicated catch-up before the replacement is considered complete

5. **Day-one recovery posture is restore-to-latest-good-state, not full disaster luxury.**
   The day-one architecture requires:
   - real backup artifacts
   - real restore choreography
   - real replacement procedure
   - rehearsed runbooks that a non-author can execute successfully

   It does not require warm standby, cross-region async replication, or PITR to exist on day one.

6. **Higher-order DR features are additive.**
   Warm standby, async replication, and PITR are later uplift paths on top of the same recovery architecture. They are not separate systems.

## Rationale

- **Recovery is part of correctness, not just ops polish.** A system that can write data but not restore it has not finished its storage contract.
- **This cleanly separates Wendelon's role from infrastructure's role.** Wendelon owns the recovery choreography and state model; infrastructure owns machine creation.
- **Autonomous healing and node replacement are different layers.** ADR `001` was right to stop at eviction; this ADR is the missing contract for what happens next.
- **Commodity deployments still need a real answer.** Scripted or operator-assisted recovery is acceptable early, but it must be deliberate and documented.
- **This leaves room for operator automation later.** A first-party controller can be added without redefining what backup, restore, and replacement mean.

## Consequences (The "Negatives")

- **Recovery docs and tooling become architecture-owned artifacts.** They are no longer optional ops notes.
- **Day-one recovery may still be slower than enterprise expectations.** Accepted because the baseline contract is correct recovery, not luxury recovery.
- **Separate backup storage adds deployment burden.** Accepted because a backup that shares the live failure domain is not a credible backup.
- **Cluster-consistent backup requires coordination across control and storage state.** Accepted because per-node snapshots taken independently do not add up to a believable cluster restore.
- **Rejected alternative: node-image restore as the whole DR story.** Rejected because it is too informal and too host-shaped.
- **Rejected alternative: database-owned provisioning.** Rejected because ADR `001` already drew the correct boundary.
- **Rejected alternative: defer recovery architecture until operator work lands.** Rejected because recovery shape drives data and control-plane decisions now.

## References
- ADR 001: Boundary of Autonomous Self-Healing vs. Infrastructure Provisioning
- ADR 007: Raft Implementation Strategy
- ADR 015: Segmented WAL and Snapshot Lifecycle
- ADR 025: Audit Event Model And Tamper-Evident Hash Chain
- ADR 030: Deployment Profiles And Support Envelope
