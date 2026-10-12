# ADR 0657: replica install gate exact binding (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Bind replica, install, gate and command authority inputs exactly for the dormant profile-two path.

## Decision

- Preserve the declared binding across replica and install boundaries.
- Require the admission gate to use the same authority identity.
- Do not infer equality from a partial subset of binding fields.

## Acceptance predicates

Exercise exact-match acceptance and mismatched inputs, including governed reducer coverage.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 0ab73574af5b68ace6f031196c3f79253aeec873

Relevant source components include:

- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admin_install.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admission_gate.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/origin.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/provisioning/replica_anchor.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/provisioning/replica_read.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
