# ADR 0631: editor startup latency (follows libaryon adr 0005)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: crates/libaryon/plans/impl/startup-latency.md

## Context

Measure engine readiness and terminal startup separately so regressions identify the boundary that became slower.

## Decision

- Use the first usable engine viewport as the headless readiness boundary.
- Use complete painted output and reflected input for the terminal milestones.
- Keep timing probes in the appropriate controlled lane, with isolated state and bounded teardown.

## Acceptance predicates

Exercise empty/file startup and terminal input; report the existing milestone intervals and enforce the recorded budgets without widening them.

## Source and scope

This records the decision embodied at the original source checkpoints:

- b4c22d712dc9fdbb9281611f3266ddbaa063d076

Relevant source components include:

- Cargo.lock
- ci/controlled-tests.txt
- ci/startup-latency-check.sh
- crates/baryon/Cargo.toml
- crates/baryon/tests/startup_latency_pty_test.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
