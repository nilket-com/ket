# ADR 0655: administrator manifest codec (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Give profile-two administrator manifests an explicit codec instead of inferring authority from arbitrary bytes.

## Decision

- Encode and decode the declared manifest representation.
- Reject invalid encodings at the codec boundary.
- Keep profile selection explicit; later profile removal remains a separate historical change.

## Acceptance predicates

Pin codec round trips and invalid byte cases without claiming support for later formats at this checkpoint.

## Source and scope

This records the decision embodied at the original source checkpoints:

- d6ce07b8311ad23cabf14e120626a8e586334ac9

Relevant source components include:

- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admin/errors.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admin/manifest.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admin/tests/mod.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admin/tests/profile_two.rs
- nilket/crates/wendelon/src/storage/mutation_visibility/root2/admin/tests/profile_two_reference.py

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
