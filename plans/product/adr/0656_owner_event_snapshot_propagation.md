# ADR 0656: owner event snapshot propagation (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Carry profile-two ownership through events and snapshots rather than losing it at a persistence boundary.

## Decision

- Preserve owner binding in event and snapshot representations.
- Keep propagation consistent with the manifest codec.
- Treat the slice as dormant beyond its stated provisioning boundary.

## Acceptance predicates

Exercise propagation through the owner/event/snapshot boundaries and refusal of inconsistent ownership.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 0c83b021778c078abf4957cffb498d876921f30d

Relevant source components include:

- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admin/manifest.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/provisioning/inventory.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/provisioning/owner_core.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/provisioning/snapshot.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/provisioning/tests/frozen_d6ce07b8.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
