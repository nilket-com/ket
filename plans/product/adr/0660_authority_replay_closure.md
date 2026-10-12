# ADR 0660: authority replay closure (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Write the recovery mark only after authority replay is complete and its closure binding has been checked.

## Decision

- Require complete history at replay closure.
- Check the replay binding before mark publication.
- Keep closure and recovery marking within the owned replay lifetime.

## Acceptance predicates

Refuse incomplete history or an inconsistent binding before writing a success mark.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 0e32df246d5e4bed38d4e87c05347d44ec88119a

Relevant source components include:

- nilket/crates/wendelon/src/storage/mutation_visibility/root2/replay.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/replay/tests.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/replay/tests/completion.rs
- nilket/crates/wendelon/tests/mutation_visibility_architecture_test.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
