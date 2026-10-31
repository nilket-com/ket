# ADR 0694: local test runs default to the cpuset CPU count (follows adr 0673)

Status: accepted design, 2026-10-30. Operator decision; supersedes the
adoption-hold decision of ADR 0673. Test execution policy.

## Context

ADR 0673 kept `nilket/scripts/cargo-cgroup` at one test thread by default
until every entry in `nilket/plans/ops/parallel_defect_candidates.md` closed
and a ladder passed on the flip. The target itself was already designed
(`nilket/plans/ops/test_parallelism.md` section 1): the CPU count of
`CARGO_CGROUP_CPUS`, the explicit override still winning, and an explicit 1024
stress lane.

The hold has outlived its purpose and now works against the doctrine it
protected:

- A serial run is not concurrency evidence (`ci/README.md`, Test concurrency),
  yet every local `cargo-cgroup test` run is serial by default. Shared state
  inside one test process, the class the doctrine tells us to hunt, is never
  exercised there unless someone opts in.
- The portable CI lane (`ci/nilket-check.sh`) calls `cargo test` directly at
  libtest's default of one thread per CPU. Local wrapper runs are therefore
  less strict than CI.
- nextest is unaffected either way: it runs one test per process and takes its
  own concurrency from `--test-threads`. On 2026-10-30 a full default Wendelon
  nextest run through the wrapper averaged 5.5 concurrent tests on 12 CPUs
  while `RUST_TEST_THREADS=1` was set. The setting only serialises in-process
  libtest runs.
- The exit criterion couples a tooling default to closing every registered
  product defect. Keeping the default serial hides the conditions those
  defects need, which slows their closure.

The operator ruled on 2026-10-30 to flip the default now, as an explicit
exception to the plan freeze, once the W-E fix landed.

## Decision

- `cargo-cgroup` defaults `RUST_TEST_THREADS` to the number of CPUs in its
  effective cpuset (`CARGO_CGROUP_CPUS`, default `0-11`, so 12). An explicit
  `CARGO_CGROUP_RUST_TEST_THREADS` still wins; 1024 remains the explicit
  stress opt-in. A cpuset the wrapper cannot count is refused (exit 64), not
  defaulted.
- The `adoption-gate` serial scope is removed from
  `nilket/.config/serial-allowlist.txt` and from the policy test that parses
  it, so a one-thread wrapper default cannot be allowlisted again without a
  successor ADR. The blanket-serial ban of `test_parallelism.md` section 3 now
  applies to the wrapper with no exception.
- The ladder driver gets a successor, v6, whose only change from v5 r2 is the
  concurrency preflight: it accepts the cpuset default expression where v5
  accepted only a literal 1024 default, and still refuses an implicit or
  explicit serial run. The repository copies of both templates stay
  byte-identical to the reviewed v6 package. The explicit 1024 stress lane
  that ADR 0673 required of a successor driver is not part of v6; it stays a
  required, separate driver change.
- Open parallel defect candidates stay open. A failure the new default exposes
  is a defect candidate: registered with test, symptom and rate, then fixed or
  measured, never serialised, retried to green or reverted around. This keeps
  ADR 0673's refusal of serialisation as a fix.

Unchanged: nextest test-groups and the serial allowlist's `lane`, `test-group`
and `doc` scopes; the cgroup memory, task and open-file limits, which were
sized for 1024 concurrent tests and so cover a default at or below the cpuset
size; the CI lane.

## Acceptance predicates

1. With no override, the wrapper passes `RUST_TEST_THREADS` equal to the CPU
   count of its cpuset on every backend: 12 for the default `0-11`, 16 for
   `0-7,16-23`, 1 for `3`. An ambient `RUST_TEST_THREADS` does not leak in. An
   explicit override wins. A non-numeric override and an uncountable cpuset
   are refused. Guarded by `nilket/scripts/tests/test-cargo-cgroup-systemd-env.sh`.
2. The policy test (`nilket/crates/nilket/tests/test_parallelism_policy.rs`)
   refuses `adoption-gate` as a scope, and finds no unlisted serial setting in
   its pinned scope, the wrapper included.
3. Both v6 templates run at the new default with no override, and refuse
   `CARGO_CGROUP_RUST_TEST_THREADS=1` and a wrapper whose default is 1 or a
   literal 1024; the v6 package's SHA256SUMS match the repository copies.
4. A v6 ladder runs to completion on exactly the implementation candidate.
   Its acceptance is completion plus a register: every failure it reports is
   entered in `parallel_defect_candidates.md` with its rate before the
   implementation is delivered. Unlike ADR 0673, a parallel-only failure does
   not block the flip; it blocks nothing but its own closure.
5. The docs that described the hold (`ci/README.md` Test concurrency,
   `test_parallelism.md` sections 1, 3 and 7, the `nextest.toml` header, the
   wrapper's usage text) describe the new default and cite this ADR.

## Source and scope

Scope: the wrapper default, its allowlist scope, its policy and wrapper
tests, the ladder driver's concurrency preflight (v6), and the prose above. No product
code, no CI lane, no cgroup limit and no nextest configuration changes.
Earlier ADR files are not edited; this record carries the supersession.
