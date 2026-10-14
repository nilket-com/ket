# ADR 0630: tenant scoped storage identity (follows wendelon adr 0021)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0021_tenant_aware_architecture_day_one.md

## Context

Storage identities must include tenant scope at every mutation boundary, rather than relying on a caller to have selected the right tenant.

## Decision

- Scope warm mutation caches, pending persistence and pending-finalization indexes by tenant.
- Bind counter replica operations to both tenant and origin.
- Refuse a foreign-tenant indexed mutation before durable persistence; equal local identifiers do not establish equal authority.

## Acceptance predicates

Cross-tenant collisions must be refused without changing the other tenant; same-tenant operations retain their existing semantics.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 2afdb20ec6f543f10a8be7eda9dae24013b646fc
- 5bc1b69fb16810b42fb404d4391dc6e95e124c4b
- 810361cd6381b16d82f74d4f8750602631895ee5
- 8beb06b8c882312fa4ebbc40858b44532faf3054

Relevant source components include:

- nilket/crates/toron/tests/sql_listener_test.rs
- nilket/crates/wendelon/src/network/data_server.rs
- nilket/crates/wendelon/src/network/data_server/counter.rs
- nilket/crates/wendelon/src/network/data_server/mutation_protocol.rs
- nilket/crates/wendelon/src/network/data_server/tests.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
