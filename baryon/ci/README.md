# Home-Rolled CI

Baryon's repository-owned CI entrypoint is `ci/check.sh`. It is designed for a
Linux machine controlled by the project, not for a hosted provider workflow.

The script installs missing Rust std targets with `rustup`, then runs the local
Linux test gate, the forced blocking resolver gate, the architecture test, and
Windows/macOS cross-target `cargo check`.

Use this from whichever local runner is convenient: a shell session, a git hook,
a cron job, a systemd timer, or a small private build service. Provider-specific
workflow files such as `.github/workflows/*` should not be added unless the
project explicitly chooses that direction later.

Set `BARYON_CI_SKIP_TARGET_INSTALL=1` when the runner image already has the
required Rust targets installed and should not mutate its toolchain.

`ci/check.sh` runs the normal generated-store huge-address tests without creating
large files. On Linux, `ci/huge-file-check.sh` creates a verified sparse 200 GiB
fixture, checks its allocated-block budget before and after bounded marker reads,
and removes it through a cleanup trap. It never performs a full-file scan.

Any future destructive/full-scan variant must require `BARYON_CI_HUGE=1`; the
huge-file entrypoint refuses `--full-scan` without that opt-in.
