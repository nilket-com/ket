# ADR 0671: exclusive node data directory ownership (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: /home/me/work/reviews/adr0102-f-own-1-census-r1/CONTRACT.md

## Context

A node owns an exclusive data-directory lock for the full interval in which its workers can access that directory.

## Decision

- Acquire the directory ownership boundary before allowing node use.
- Retain it through shutdown and release it explicitly.
- A forked child must not extend the ownership lifetime after the owning node closes.

## Acceptance predicates

Refuse competing owners, exercise shutdown/reopen and prove that a forked child cannot keep the lock alive.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 36cf32ddfa436abcd636d8040ee1c93786e7cd23

Relevant source components include:

- nilket/crates/nilket/src/local.rs
- nilket/crates/observability/src/audit/mod.rs
- nilket/crates/toron/tests/wendelon_storage_execution_test/acceptance8/mod.rs
- nilket/crates/wendelon/src/audit.rs
- nilket/crates/wendelon/src/cli.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
