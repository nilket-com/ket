# ADR 0654: administrator command envelope validation (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Check administrator command envelopes as a pure validation boundary before stateful application.

## Decision

- Keep envelope checks deterministic and free of storage mutation.
- Validate the administrator framing required by the command.
- Refuse malformed envelopes before they become authority state.

## Acceptance predicates

Use pure positive and negative envelope cases, and preserve the separation from reducer/application effects.

## Source and scope

This records the decision embodied at the original source checkpoints:

- c6e603a19ed401a3649463463417285627745ed3

Relevant source components include:

- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admin_command.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/mod.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/tests/admin_command.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/tests/admin_command_reference.py
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/tests/admin_command_vectors.json

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
