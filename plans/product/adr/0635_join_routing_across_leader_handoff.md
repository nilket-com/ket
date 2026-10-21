# ADR 0635: join routing across leader handoff (follows wendelon adr 0002)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: chatd01a16489-8307-7162-8e79-9546f4889d9a, archived in /home/me/work/reviews/history-cleanup-20261023/map-r5/original-design-sources/cut-ab-plan-chatd.txt

## Context

Cluster JOIN must remain routable when leadership changes during admission and handoff.

## Decision

- Accept a completed self join across the handoff boundary.
- Route to a promoted leader even when that leader is absent from the caller current epoch view.
- Keep routing correctness separate from startup resource ownership.

## Acceptance predicates

Exercise self join completion and a promoted leader missing from the old view; retries must not turn a completed join into a new incompatible admission.

## Source and scope

This records the decision embodied at the original source checkpoints:

- c5b3294476611852957fb0210d965d52a45f57f6
- 85d374804e7394cc24e2e3e332836897b0c57c29

Relevant source components include:

- nilket/crates/wendelon/src/config.rs
- nilket/crates/wendelon/src/consensus/raft_storage.rs
- nilket/crates/wendelon/src/network/control_server.rs
- nilket/crates/wendelon/src/network/control_server/completed_join_tests.rs
- nilket/crates/wendelon/src/network/control_server/join_forwarding_tests.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
