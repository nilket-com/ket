# ADR 0670: recovery freeze and joint unfreeze (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Represent recovery freeze and joint unfreeze as explicit authority transitions.

## Decision

- Keep the frozen state observable to admission and command processing.
- Require the declared joint-unfreeze conditions rather than a unilateral state change.
- Preserve exact authority binding through the transition.

## Acceptance predicates

Exercise freeze enforcement and both accepted and refused unfreeze transitions.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 524db8a29acc236b86e970f3206a2eec6274d19f

Relevant source components include:

- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admission_gate.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/command.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/governance_verification.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/incremental.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/mod.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
