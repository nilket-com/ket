# ADR 0658: owner provisioning tuple export (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Export the owner-produced provisioning tuple through a defined dormant interface.

## Decision

- The owner produces the tuple from its owned authority state.
- Keep tuple shape and delivery provenance explicit.
- Do not substitute caller-assembled data for owner-produced evidence.

## Acceptance predicates

Exercise exported shapes, the owner-to-consumer delivery chain and retired-owner refusal.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 76bf521830b6544014426cf0c2fa0ef267bd6839

Relevant source components include:

- nilket/crates/wendelon/src/storage/mutation_visibility/root2/provisioning/owner_log.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/provisioning/owner_log/ops.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/provisioning/tests/mod.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/provisioning/tests/owner_tuple_export.rs
- nilket/crates/wendelon/tests/mutation_visibility_architecture_test.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
