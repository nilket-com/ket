# ADR 0164: the Nilket lane reproduces its environment

Status: proposed 2026-09-12. Platform lane. ADR 0163's gate run put the
Nilket revision on a fresh Linux machine for the first time, and the
documented entrypoint failed there for reasons the revision did not cause:
the lane passed only once a developer's shell settings were supplied by
hand. This ADR makes the entrypoint carry them.

## Decision

### 1. The documented entrypoint is sufficient

`ci/product-check.sh nilket` reproduces the environment its tests require
on a plain Linux machine with a Rust toolchain, without a developer's shell
profile, runner script, or Git configuration. A machine that has never run
Nilket is the reference environment, not a machine that has.

### 2. Tests never read the operator's Git configuration

Temporary-repository tests see no global or system Git configuration and a
deterministic author and committer identity. Signing, hooks, templates, and
default-branch settings of whoever runs the gate do not reach them. This is
the whole of the operator's configuration, chosen deliberately over the
narrower "signing off": any global key can change a test's behavior, and
which one bites next is not knowable in advance.

### 3. The lane establishes its own process conditions

Where a test's contract depends on the process environment, the entrypoint
establishes that condition or refuses with a message naming it; it never
lets the condition silently vary with how the lane was launched. Today that
is the thread stack size the debug test profile needs and the signal
dispositions the supervisor's shutdown tests exercise.

### 4. Bigfoot validates the entrypoint as written

The gate machine runs the entrypoint with no environment supplied
externally. An external variable that makes the lane pass is evidence of a
defect in the entrypoint, not a passing lane.

## Mechanisms in this cut

`ci/nilket-check.sh` exports `RUST_MIN_STACK` at the value
`nilket/scripts/cargo-cgroup` already uses, points `GIT_CONFIG_GLOBAL` at
an empty file with `GIT_CONFIG_NOSYSTEM` set, exports a fixed
`GIT_AUTHOR_*`/`GIT_COMMITTER_*` identity, and re-executes itself with
default `SIGINT`/`SIGQUIT` dispositions when it finds them ignored, which
is what a non-interactive shell does to any `&` job. Each may be replaced
by a mechanism that keeps the decision.

## Acceptance

1. On Bigfoot, from a login with the operator's ordinary global Git
   configuration and no `RUST_MIN_STACK`, `ci/product-check.sh nilket`
   passes when launched as a background job of a non-interactive shell.
2. The same command passes from an interactive login.
3. The Ket lane of `ci/check.sh` is unchanged by this cut.

## Limits and follow-up

This records the environment the current tests need; it does not judge
whether a 64 MiB test stack or a signal-sensitive supervisor test is the
right contract. Reducing either is Nilket work under its own plan. The
cross-target lanes of `ci/check.sh` remain the open acceptance item of
ADR 0163 and are not addressed here.
