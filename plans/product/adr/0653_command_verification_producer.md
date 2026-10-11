# ADR 0653: command verification producer (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Produce command-verification input through a narrow dormant Root2 producer.

## Decision

- Bind the produced input to the command being checked.
- Keep authority input construction separate from command application.
- Do not broaden the producer into an implicit activation path.

## Acceptance predicates

Exercise command/input agreement and refusal of inconsistent bindings at the producer boundary.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 72dc079228cc68ef6e1de764b0cac68c9981ca7e

Relevant source components include:

- nilket/crates/wendelon/src/storage/mutation_visibility/root2/mod.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/origin.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/tests/mod.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/tests/verification.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/tests/verification_reference.py

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
