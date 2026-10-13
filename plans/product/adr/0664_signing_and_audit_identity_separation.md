# ADR 0664: signing and audit identity separation (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Keep development, audit and other signing identities explicit and separated.

## Decision

- Require explicit development signing keys.
- Validate identity separation rather than silently sharing signing material.
- Require audit-root verification to be anchored to the intended audit identity.

## Acceptance predicates

Refuse missing or conflicting identity configuration and an unanchored audit root; test distinct development and audit identities.

## Source and scope

This records the decision embodied at the original source checkpoints:

- d7dcfad00bb1dbe7414b5455ba9f377b3fc1e963
- 99cfa5ae923bc3fa9a26d5a803f3766ab7a10270

Relevant source components include:

- nilket/Cargo.lock
- nilket/crates/nilket/tests/cli_integration.rs
- nilket/crates/observability/Cargo.toml
- nilket/crates/observability/src/audit/mod.rs
- nilket/crates/observability/src/audit/root_trust.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
