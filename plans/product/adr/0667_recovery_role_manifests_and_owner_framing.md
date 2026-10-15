# ADR 0667: recovery role manifests and owner framing (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Frame owner directories explicitly and represent recovery roles in manifests.

## Decision

- Keep owner directory framing unambiguous.
- Represent recovery-role authority in the manifest rather than an out-of-band assumption.
- Preserve the separation between framing and later role-key rotation policy.

## Acceptance predicates

Exercise manifest and owner-framing validation, including malformed or inconsistent role/owner inputs.

## Source and scope

This records the decision embodied at the original source checkpoints:

- d9f3c8b58c269bd04b30c3f4f632db2ef6b15a58

Relevant source components include:

- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admin/errors.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admin/manifest.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admin/tests/mod.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admin/tests/profile_three.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admin/tests/profile_three_reference.py

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
