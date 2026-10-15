# ADR 0668: role key epochs and recovery rotation (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Track role-key epochs and require recovery authorization for rotation.

## Decision

- Keep key epoch identity part of the role authority state.
- Require the declared recovery authorization when rotating role keys.
- Do not treat a new key value alone as sufficient authorization.

## Acceptance predicates

Exercise accepted rotation and refused epoch/authorization mismatches at the role-command boundary.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 722a951bbbf2928ff53a28870ade8bb4bcc33ff4

Relevant source components include:

- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admin_command.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admin_install.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admin_statement.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admission_gate.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/command.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
