# ADR 0669: counter visibility and witnessed bases (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Counter visibility must exclude uncommitted values while retaining the witnessed base needed for later decisions.

## Decision

- Hide values that do not have the required committed visibility state.
- Preserve witnessed bases across counter operation handling.
- Keep tenant/origin scope and later deduplication/admission-lifetime mechanisms distinct.

## Acceptance predicates

Hold operations before commitment and verify that uncommitted changes do not replace the visible witnessed base.

## Source and scope

This records the decision embodied at the original source checkpoints:

- a5c50bd758beadeb70ad51f3e72398d3f9426091

Relevant source components include:

- nilket/crates/wendelon/src/network/data_server.rs
- nilket/crates/wendelon/src/network/data_server/counter.rs
- nilket/crates/wendelon/src/network/data_server/counter_visibility/tests.rs
- nilket/crates/wendelon/src/network/data_server/descriptor_migration.rs
- nilket/crates/wendelon/src/network/data_server/tests.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
