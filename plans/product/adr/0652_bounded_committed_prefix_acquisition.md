# ADR 0652: bounded committed prefix acquisition (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Acquire a committed prefix through a bounded driver that validates the observed data rather than trusting a stale read.

## Decision

- Make acquisition bounds explicit.
- Keep coverage validation tied to the acquired prefix.
- Refuse inconsistent observations instead of treating a partial acquisition as complete authority.

## Acceptance predicates

Exercise mutation after the read and final-batch coverage, together with bounded refusal paths.

## Source and scope

This records the decision embodied at the original source checkpoints:

- bd99078ac76c693fbf8e463c55031a83d50dc8c0

Relevant source components include:

- nilket/crates/wendelon/src/storage/mutation_visibility/root2/acquisition.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/mod.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/tests/acquisition.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/tests/mod.rs
- nilket/crates/wendelon/tests/mutation_visibility_architecture_test.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
