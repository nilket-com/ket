# ADR 0651: committed prefix shape validation (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Validate the shape of a committed prefix before allowing it to serve as authority input.

## Decision

- Reject malformed or inconsistent prefix structure.
- Keep validation separate from acquisition and replay.
- A prefix-shaped container alone does not prove its contents are current or complete.

## Acceptance predicates

Exercise accepted prefix shapes and malformed, incomplete or inconsistent alternatives using the existing dormant boundary.

## Source and scope

This records the decision embodied at the original source checkpoints:

- a0eb4a77c8c425b6a984eace955146f72e80a802

Relevant source components include:

- nilket/crates/wendelon/src/storage/mutation_visibility/root2/mod.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/prefix.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/tests/mod.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/tests/prefix.rs
- nilket/crates/wendelon/tests/mutation_visibility_architecture_test.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
