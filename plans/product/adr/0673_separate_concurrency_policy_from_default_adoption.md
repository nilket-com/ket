# ADR 0673: separate concurrency policy from default adoption (follows adr 0632)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: nilket/plans/ops/test_parallelism.md, sections 1, 3 and 7; ci/README.md, Test concurrency

## Context

Land test-concurrency policy separately from changing the default execution concurrency.

## Decision

- Keep the accepted one-test-thread default during the adoption hold.
- Require closure of every registered candidate and a dedicated explicit stress lane before a default flip.
- Refuse implicitly or explicitly serial runs in a ladder intended to prove parallel adoption.

## Acceptance predicates

Guard the adoption hold and runner refusal cases; a serial pass does not satisfy the parallel adoption predicate.

## Source and scope

This records the decision embodied at the original source checkpoints:

- c316e236d581dcbabdb201a414e6d9510fd80094

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
