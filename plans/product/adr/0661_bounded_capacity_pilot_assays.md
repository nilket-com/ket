# ADR 0661: bounded capacity pilot assays (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Use a bounded dormant capacity pilot as an assay rather than a claim of production capacity.

## Decision

- Keep allocator tracking and pilot imports inside the authority-test boundary.
- Make bootstrap registration visible in the pilot outcome.
- Bound the pilot and preserve the dormant production boundary.

## Acceptance predicates

Exercise the pilot phases and observable outcome structure; no operating ceiling or campaign activation follows from this decision.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 3cc42d94cadbbee0abeda27c9a8471b90f5c4e91

Relevant source components include:

- nilket/crates/wendelon/src/consensus/retention/tests.rs
- nilket/crates/wendelon/src/consensus/retention/tests/g2.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/mod.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/replay/tests.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/replay/tests/g2.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
