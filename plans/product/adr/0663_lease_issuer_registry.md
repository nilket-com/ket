# ADR 0663: lease issuer registry (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Represent the lease issuer registry explicitly in authority roots.

## Decision

- Carry issuer registry state with the authority root representation.
- Keep issuer lookup separate from a caller assertion of identity.
- Do not infer issuer authorization merely from the presence of a signature.

## Acceptance predicates

Exercise registry representation and issuer binding, with governed-command verification handled by its successor decision.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 8bf672173203ebb2798ca7c48471e6d1cb4ee2d0

Relevant source components include:

- nilket/crates/wendelon/src/storage/mutation_visibility/root2/command.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/incremental.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/issuer_registry.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/mod.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/origin.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
