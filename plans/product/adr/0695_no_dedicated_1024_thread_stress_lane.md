# ADR 0695: no dedicated 1024-thread stress lane (follows adr 0694)

Status: accepted design, 2026-10-30. Operator decision. Test execution policy.

## Context

ADR 0673 required a successor ladder driver with a dedicated explicit
1024-thread stress lane, and ADR 0694 kept that lane as a required, separate
driver item and called 1024 the explicit stress opt-in.

The operator ruled on 2026-10-30 that the 1024 posture was itself an
overcorrection: one test thread was a response to SSH stalling under a full
CPU meltdown, and 1024 threads was a response to that. The cpuset, CPU quota,
nice and ionice already keep the host reachable; the thread count is not the
protection.

## Decision

- No dedicated 1024-thread stress lane is required of the ladder driver. The
  open driver item from ADR 0694 is closed by this decision, not by an
  implementation.
- 1024 is no longer a named stress posture. A stress run sets an explicit test
  count chosen for what it stresses (the ladder's in-process stress steps keep
  their 12 and 8), bounded by the cpuset and quota as before.
- Unchanged: the cpuset default of ADR 0694; the explicit
  `CARGO_CGROUP_RUST_TEST_THREADS` override, which still accepts any positive
  count; the cgroup memory, task and open-file limits, which were measured for
  1024 concurrent tests and stay as headroom; the ladder v6 driver; the defect
  register's history, which records runs at 1024 as they happened.

## Acceptance predicates

1. Forward-looking prose (`ci/README.md` Test concurrency,
   `nilket/plans/ops/test_parallelism.md`, the `nilket/.config/nextest.toml`
   header, the wrapper's usage text and default comment, the policy test's
   module doc) no longer requires or names a 1024 stress lane or opt-in, and
   cites this ADR where it states the stress posture.
2. No code, limit, ladder template or test behaviour changes: the wrapper's
   behaviour and the policy test results are identical before and after.

## Source and scope

Prose only. Earlier ADR files are not edited; this record carries the
supersession of their 1024 stress-lane requirement.
