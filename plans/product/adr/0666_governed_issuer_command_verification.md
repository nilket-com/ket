# ADR 0666: governed issuer command verification (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Verify dormant governance commands against the explicit issuer registry.

## Decision

- Consult the bound registry rather than accepting an arbitrary signing key.
- Bind authorization to the command being verified.
- Keep command verification separate from later recovery-role and key-epoch mechanisms.

## Acceptance predicates

Exercise registered versus unauthorized issuers and inconsistent command/signature bindings.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 976c55f88a5cc909e60763cdf1708cda78c60d26

Relevant source components include:

- nilket/crates/wendelon/src/storage/mutation_visibility/root2/governance_command.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/governance_verification.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/mod.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/replay/tests.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/replay/tests/carrier.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
