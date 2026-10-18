# ADR 0632: test concurrency policy and controlled lanes (follows toron adr 0629)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: /home/me/work/.agent-bridge/test-concurrency-convention-r1/test-concurrency.md; nilket/plans/ops/test_parallelism.md

## Context

Concurrent failures are defect candidates, and test resource ownership must be explicit in the runner and its controlled lanes.

## Decision

- Keep narrow controlled-test declarations and nextest groups for justified exclusive resources.
- Pass caller test environment through the cgroup wrapper into the actual command.
- Use separate cross-process and in-process concurrency evidence; an isolated pass does not close a parallel defect.

## Acceptance predicates

Guard runner environment propagation, lane declarations and feature coverage; no global serialization or timeout widening may substitute for a repair.

## Source and scope

This records the decision embodied at the original source checkpoints:

- 9ceac94efc275f86b22be32d097df091df0a2c93
- 221d34fa8b055f5e901a7bf1f2405c67407182bc
- 8f61dd2e01e6a6c8a6903ba6bba103730fadfbc1
- 17ce17f6ba36fb16fe5ba49c8cb21f7035f5a3ca

Relevant source components include:

- ci/check-windows.ps1
- ci/controlled-tests.txt
- ci/huge-file-check.sh
- ci/nilket-check.sh
- ci/startup-latency-check.sh

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
