# ADR 0662: governance and lease signature verification (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Verify governance and lease signatures in the dormant authority path.

## Decision

- Keep signature checking at the declared authority boundary.
- Bind verification to the represented governance or lease input.
- Do not promote an unchecked representation into accepted authority.

## Acceptance predicates

Exercise valid verification and malformed, wrong-key or otherwise inconsistent signature inputs at the existing verification seams.

## Source and scope

This records the decision embodied at the original source checkpoints:

- f8b126f62fc225c1b987d74fbf982bc5a23b107b

Relevant source components include:

- nilket/crates/wendelon/src/storage/mutation_visibility/root2/governance_command.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/lease_payload.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/mod.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/tests/governance_command.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/tests/governance_permissions.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
