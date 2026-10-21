# ADR 0633: retain asynchronous file resources until kernel completion (follows wendelon adr 0009)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/crates/wendelon/plans/adr/0009_io_uring_threading_model.md

## Context

An asynchronous kernel request owns the file and other resources it may still access until completion is proved.

## Decision

- Retain resource ownership across submission, cancellation and completion.
- Do not treat cancellation submission as proof that the kernel has stopped using a resource.
- Bound repeated io_uring enter attempts that make no progress.

## Acceptance predicates

Exercise delayed completion, cancellation and shutdown; resources cannot be released while a kernel request can still reference them.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 3d55647298683e1ac54f792cad329918dfeca7ed
- 0cc5447044d1ce2ec3fd5308f8ee21b47fb11740
- 374f0c43d7564dd8f0265eb1847f602537b20b7f

Relevant source components include:

- nilket/crates/wendelon/src/node.rs
- nilket/crates/wendelon/src/storage/file_ops.rs
- nilket/crates/wendelon/src/storage/io_uring.rs
- nilket/crates/wendelon/src/storage/io_uring/lifetime.rs
- nilket/crates/wendelon/src/storage/io_uring/submit_error_tests.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
