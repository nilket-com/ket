# ADR 0637: trust roots and starter signing identity (follows wendelon adr 0018)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: chatd01a164d7-2fcd-764e-bf07-8dc41b6a62eb, archived in /home/me/work/reviews/history-cleanup-20261023/map-r5/original-design-sources/cut-c-security-chatd.txt

## Context

Trust-root withdrawal and fresh starter signing identities are one coupled original security checkpoint.

## Decision

- Remove the distrusted emSign G3 root from the bundled trust surface.
- Provision fresh starter signing keys rather than treating starter material as a reusable identity.
- Keep the later Nilket CLI delegation correction at its own chronological endpoint.

## Acceptance predicates

Check bundled-root distrust and actual fresh-key CLI delegation. This record does not split the coupled root/key commit into an invented intermediate source state.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 0deda6addcf9ed47a325a14660da74f84de1e2d9
- b16dc4ef349e7546e95efb6bf61e1663b976576e

Relevant source components include:

- Cargo.lock
- crates/polariton/Cargo.toml
- crates/polariton/tests/tls_root_distrust.rs
- nilket/Cargo.lock
- nilket/crates/nilket/tests/cli_integration.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
