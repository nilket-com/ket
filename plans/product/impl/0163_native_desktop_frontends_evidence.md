# PRODUCT-0163 implementation evidence

Status: implemented 2026-09-11. The record remains proposed. Acceptance
items 1 through 3 are established. Item 4 is not: every Linux lane of
`ci/check.sh` passed on the implementation revision, but the complete gate
did not pass, because its cross-target validation was blocked by the gate
machine's missing cross C toolchain.

The revision tested is `54e358a9`. This evidence file was added to that
implementation commit by amendment before publication; the amendment adds
documentation only, and the source tree it records is that revision's.

The implementation commit (`54e358a9`) is the native Windows bring-up of
Baryon and Polariton plus the SQLite plotting smoke. It established the four
decisions with today's mechanisms: MSVC builds through
`scripts/build-windows.ps1`, `cfg(unix)` gates on tests that need a Unix
facility, `--no-default-features` as the Windows Polariton head, and
`.gitattributes` with `rustfmt.toml` making a checkout byte-identical.

## Scope discovered during implementation

Five Windows defects were product code, not tests. `atomic_write` could not
replace a file the editor held open, so persistent settings never reached
`init.rn`; `Path::canonicalize` leaked verbatim `\\?\` prefixes into the Git
panel and path comparisons; the single-file Rust analysis host asserted a
literal `/tmp` as absolute and panicked the semantic reactor on every Rust
document; the reporting spool fsynced directories through `File::open`,
which Windows refuses, so reporting could not be enabled; and the spool
identified its writer with `ps`, which Windows lacks. Each is fixed with a
platform-neutral behavior, and the spool's Windows writer token checks
liveness because a terminated process stays openable while a handle to it
survives.

The checkout itself was a defect source: a `core.autocrlf=true` clone
produced CRLF, which failed fourteen fixture-reading tests, and `cargo fmt`
without a repository `rustfmt.toml` rewrote 171 files to spaces. Both are
now pinned in the repository.

Unix-only by design and gated, not fixed: the durable `SessionStore`
(`flock`), job-supervisor tests that spawn `/bin/sh`, a newline-in-filename
test, and a symlink test. The portable job supervisor therefore has no
native test coverage; a self-executing test helper is the follow-up.

## Verification

Native, on Windows 11 with Rust 1.98.1 for `x86_64-pc-windows-msvc`:
`baryon.exe` answers `--help` and `--version`; `polariton.exe` opens its
window with the scratch `SQLite :memory:` connection. The libaryon library
tests passed 1,439 with 22 ignored; Baryon passed 91 unit and 11
architecture tests; the Polariton reduced head passed 929 binary tests with
12 ignored and 129 architecture guards; the `ket` router's 2 architecture
tests passed with its Linux-only units gated. The render-evidence smoke
captured four plots from the SQLite scratch (`plot-1` and `plot-2` at
800×500 and 400×300). This establishes acceptance items 1 through 3.

Linux, on Bigfoot (24 CPUs, Ubuntu, Rust 1.98.1) against the exact commit
`54e358a9` in a fresh `target/`: `cargo fmt --check`, the Nilket user-service
packaging test, `scripts/cargo-ctrl --locked` in both the default and
`BARYON_SVP_BACKEND=blocking` configurations (general lane of 19 workers
plus the controlled lane), the workspace architecture tests, the reduced
head check, and the reduced head's 942 binary tests all passed with zero
failures. The log is `~/ci-logs/adr-0163-check.log` on Bigfoot.

The gate then stopped at `cargo check --locked --target
x86_64-pc-windows-msvc`: the build scripts of `psm`, `stacker`, `ring`, and
`aws-lc-sys` need a C compiler for the MSVC target, and Bigfoot has only the
host `cc`. The macOS lanes were not reached. This is a gate-machine gap,
and the cross-target lanes remain an acceptance requirement until a gate
machine runs them or a plan changes the requirement. The native Windows
results above are additional evidence for the same target, not a
substitute for that lane.
