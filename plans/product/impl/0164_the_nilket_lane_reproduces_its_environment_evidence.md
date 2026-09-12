# PRODUCT-0164 implementation evidence

Status: implemented 2026-09-12. The record remains proposed.

The change is `ci/nilket-check.sh` alone. It now exports `RUST_MIN_STACK`
at the value `nilket/scripts/cargo-cgroup` uses, isolates Git configuration
for the whole lane (`GIT_CONFIG_GLOBAL` at an empty temporary file,
`GIT_CONFIG_NOSYSTEM`, fixed author and committer identity), and re-executes
itself once through perl with default `SIGINT`/`SIGQUIT` dispositions when
it finds them ignored on entry. No Nilket source, test, or fixture changed.

## Scope discovered during implementation

The three conditions were found by running the unchanged lane on Bigfoot,
a machine that had never run Nilket, against the Nilket revision in
`54e358a9`. Eleven targets died with `has overflowed its stack`; four
`toron-bench` snapshot tests failed at `git commit` because the operator's
global `commit.gpgsign=true` reached the fixture repositories; and the
supervisor's shutdown-order test could not trap `SIGINT` in its `/bin/sh`
fixtures because the lane had been launched as an `&` job of a
non-interactive shell, which POSIX requires to ignore `SIGINT` and
`SIGQUIT` for that job. Supplying each condition by hand made the lane pass: 8,145 tests with the
shutdown-order test as the sole failure, then that one target alone at 14
passed. The 13 other tests of that target were already inside the 8,145,
so the lane's true total is 8,146, the figure both acceptance runs below
report; an earlier working figure of 8,159 double-counted them. This fixed
the diagnosis before any script changed.

Git isolation is the operator's entire global and system configuration,
not signing alone: signing was the key that bit on Bigfoot, and there is
no way to know which global key bites on the next machine. The fixture
tests in `toron-bench` and `evidence.rs` already set their own author and
committer; the lane's fixed identity covers any that do not.

`bash` cannot reset a signal disposition it inherited ignored, and the
`env --default-signal` route is unavailable where `env` is the uutils
implementation (Bigfoot's is), so the reset goes through perl, which
Debian and Ubuntu install as an essential package. After the re-execution,
`SIGINT` is default in the lane's process tree; `bash` re-ignores `SIGQUIT`
for its own non-interactive use, which the tests do not exercise. The guard
against re-execution loops refuses with a message rather than running with
the condition unmet.

## Verification

Bigfoot, Ubuntu, 24 CPUs, Rust 1.98.1, warm `nilket/target/`, the plan
commit `a91b69f7` checked out with this entrypoint in place.

Acceptance 1: `nohup bash -lc 'ci/product-check.sh nilket' &` from a
non-interactive `ssh` session, with `RUST_MIN_STACK` unset and the
operator's global `commit.gpgsign=true` in place. Inspection of the live
process tree showed the re-executed script with `SIGINT` default and its
parent, the original `&` job, with `SIGINT` ignored. Result: `cargo fmt
--check` and `cargo check --workspace --all-targets` passed; the workspace
tests passed 8,146 across 179 targets with zero failures and no stack
overflow. Log: `~/ci-logs/adr-0164-nilket.log`.

Acceptance 2: the same command from a login shell on a pty (`bash -lic`),
no environment supplied. Result: exit 0; 8,146 tests passed across 179
targets with zero failures. Log: `~/ci-logs/adr-0164-nilket-interactive.log`.

Acceptance 3: `ci/check.sh` does not invoke `ci/nilket-check.sh`; the Ket
lane is untouched by this cut.
