# ADR 0665: bounded root carriers and wal compatibility (follows wendelon adr 0100)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md

## Context

Carry bounded Root2 authority data through the declared WAL compatibility path.

## Decision

- Keep the Root2 carrier bounded and its framing explicit.
- Replay dormant carriers from committed entries.
- Preserve the outgoing WAL-v5 compatibility contract instead of splitting it from the carrier representation.

## Acceptance predicates

Exercise carrier bounds, committed-entry replay and frozen outgoing WAL-v5 compatibility vectors.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 669342eedfe20afc9cf183219df63963e2a1aca7
- f2f046c2acf6321f5c49d9352f670bb326bf3e12

Relevant source components include:

- nilket/crates/wendelon/src/consensus/core.rs
- nilket/crates/wendelon/src/consensus/mod.rs
- nilket/crates/wendelon/src/consensus/raft_storage.rs
- nilket/crates/wendelon/src/consensus/root2_carrier.rs
- nilket/crates/wendelon/src/consensus/root2_carrier/tests.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
