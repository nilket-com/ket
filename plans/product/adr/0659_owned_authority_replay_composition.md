# ADR 0659: owned authority replay composition (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Compose authority replay with explicit ownership rather than detached replay resources.

## Decision

- Keep replay resources owned for the composed lifetime.
- Keep dormant replay separated from production guards.
- A composed replay object does not itself declare replay closure.

## Acceptance predicates

Exercise composition and teardown while guarding the dormant/production boundary.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 4fac574f1a5d4ce082dad37605d24e898e44336a

Relevant source components include:

- nilket/crates/wendelon/src/storage/mutation_visibility/root2/acquisition.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/committed.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/committed/fixture.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/mod.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/recovery.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
