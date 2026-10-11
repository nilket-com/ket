# ADR 0650: deterministic post root coverage (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Make post-root coverage deterministic in the dormant Root2 fixtures and architecture boundaries.

## Decision

- Define reproducible coverage cases for the root authority checks.
- Keep case construction and expected outcomes independent of scheduling.
- Do not infer production visibility or activation from fixture coverage.

## Acceptance predicates

Pin deterministic cases and refusal boundaries; the coverage mechanism does not certify an operating ceiling.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 549887b9670889ebba14a68649bd774661d586de

Relevant source components include:

- nilket/crates/wendelon/src/storage/mutation_visibility/root2/origin.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/reducer.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/tests/cover.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/tests/cover_vectors.json
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/tests/mod.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
