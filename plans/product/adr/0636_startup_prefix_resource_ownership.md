# ADR 0636: startup prefix resource ownership (follows adr 0635)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: /home/me/work/reviews/cut-c-b0-plan-20261022/PLAN.md

## Context

Every resource started before complete node handoff has an owner responsible for bounded rollback.

## Decision

- Own each startup prefix until ownership is transferred to the complete node.
- Preserve the original startup error when cleanup also fails.
- Prepare the startup watchdog early and preserve failed-startup recovery; audit provider withdrawal and scoped spawning.

## Acceptance predicates

Inject failures at startup prefixes and prove bounded termination, retained primary errors and complete ownership transfer. Do not disguise a leak by detaching work.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 50882be2c5c1e06400926b352bdfac072c31e0d3
- 62857f589661864030a55ab62d3df8a21f4dc68f
- c5a4e20038024f1bc3c9c06cff9da2e23fd1536e
- 54bbc75c1e9c34672d35aac38daef36f01e3fc82

Relevant source components include:

- nilket/crates/toron/src/binder/runtime_evidence.rs
- nilket/crates/wendelon/crates/wendelon-client/tests/counter_transport/mod.rs
- nilket/crates/wendelon/src/cli.rs
- nilket/crates/wendelon/src/client/core.rs
- nilket/crates/wendelon/src/config.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
