# ADR 0634: sidecar publication lifetime and catalog identity (follows wendelon adr 0081)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0081_secondary_index_declaration_and_lifecycle.md

## Context

A secondary-index sidecar has a full catalog identity and an ordered publication/retirement lifetime.

## Decision

- Make concurrent publication temporary names independent.
- Order deletion and retirement against publication and reclaim interrupted publication temporaries at startup.
- Use full catalog identity for persistent sidecars; preserve supported normalized aliases and rebuild corrupt coverage.

## Acceptance predicates

Exercise concurrent publication, retirement races, interrupted startup, fingerprint aliases and corrupt coverage. Keep the historical RED checkpoint separate from repairs.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 95c7145f4a508f015a2c95b75db2176cade0cb60
- d3d3a771bae31657ab9123661aecd7a2a026c5bc
- fbae3080e5d529b26843c6efc0c2e4aa07e94bac
- 6e5fbbfe8b03c5884d54897566615643939b39b7
- b99c9a0f14983ea92dd0694d527eb2efe133c6a3

Relevant source components include:

- nilket/crates/wendelon/src/network/data_server/tests.rs
- nilket/crates/wendelon/src/storage/file_ops.rs
- nilket/crates/wendelon/src/storage/manager.rs
- nilket/crates/wendelon/src/storage/manager/open.rs
- nilket/crates/wendelon/src/storage/manager/secondary_index_cleanup.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
