# ADR 0638: dependency advisory remediation (follows wendelon adr 0008)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: /home/me/work/reviews/cut-e-advisory-remediation-20261023/CONTRACT.md; /home/me/work/reviews/cut-e-advisory-remediation-20261023/ADDENDUM-A.md

## Context

Update the vulnerable TLS/QUIC dependency set while preserving encrypted slab compatibility under the resulting crypto implementation.

## Decision

- Keep the reviewed precise dependency changes and required transitive changes visible in both locks.
- Seal slab bodies using the separate-tag AEAD contract instead of relying on a custom buffer growth interpretation.
- Retain behavior-named format controls and backward-compatible reads of frozen encrypted data.

## Acceptance predicates

Verify both complete lockfiles, historical encrypted-format readers and both AEAD algorithm paths. Retain failure and repair checkpoints rather than claiming the earlier source already met the final predicate.

## Source and scope

This records the decision embodied at the original source checkpoints:

- e584b3b7699e80fc70a0f3b9d85923b37a5f1655
- 70748e56cc2c3fa10410e42c58e9bf73acd967a7

Relevant source components include:

- Cargo.lock
- nilket/Cargo.lock
- nilket/crates/wendelon/src/crypto.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
