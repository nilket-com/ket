# ADR 0672: logical counter admission lifetime (follows wendelon adr 0102)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0102_counter_visibility_and_persisted_origin.md

## Context

Counter admission gates are owned by logical participants, not by incidental task or request lifetimes.

## Decision

- Reclaim a gate when its logical admission lifetime ends.
- Keep replica logical-participant ownership explicit.
- Treat cancellation and reclaimed admission as correctness paths rather than timeout-based cleanup.

## Acceptance predicates

Exercise cancellation/reclamation under contention and retain a negative bypass control that proves the stress test observes admission.

## Source and scope

This records the decision embodied at the original source checkpoints:

- d26bcf57b7bc9e00546145436b8237f9edd29c80
- fc261cc078387fe7d5314dbaf0e8d84503fb33f4

Relevant source components include:

- nilket/crates/wendelon/src/network/data_server/counter_visibility/tests.rs
- nilket/crates/wendelon/src/storage/counter.rs
- nilket/crates/wendelon/src/storage/counter/replica_scope_tests.rs
- nilket/crates/wendelon/src/storage/counter/write_key.rs
- nilket/crates/wendelon/tests/architecture_test.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
