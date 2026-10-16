# Local CI

Native Windows builds use [`scripts/build-windows.ps1`](../scripts/build-windows.ps1).
See the [Windows build guide](../docs/windows.md) for prerequisites, optional
native tests, and the initial Polariton build without local Toron integration.

The repository contains two independent Cargo workspaces after PRODUCT-0001.
The product dispatcher preserves that isolation:

```bash
ci/product-check.sh ket
ci/product-check.sh nilket
ci/product-check.sh cross
ci/product-check.sh all
ci/product-check.sh changed origin/main
```

`changed` selects workspace checks from Git paths and adds the cross-product
contract job whenever both workspaces or `plans/product/` changed. Path
selection is a latency optimization, not the only gate: `all` remains the
scheduled/manual complete run. The cross-product job initially enforces Cargo
and dependency boundaries; the Toron dogfood pair extends that same entrypoint
with its durable launcher/client/UI acceptance test.

Set `PRODUCT_CI_DRY_RUN=1` to print the lanes selected by `changed` without
executing them.

Baryon's repository-owned CI entrypoint is `ci/check.sh`. It is designed for a
Linux machine controlled by the project, not for a hosted provider workflow.
There are deliberately no `.github` workflows or provider-specific control
files.

The script uses `scripts/cargo-ctrl` for the local Linux test gate and forced
blocking-resolver gate, followed by the architecture test and the reduced
Polariton head.

Each desktop is gated natively (ADR 0170). Linux cannot cross-check the
desktop crates: `ring`, `aws-lc-sys`, `psm`, and `stacker` compile C for
their target, which needs that platform's toolchain and SDK. On macOS, run
`ci/check-macos.sh`; on Windows, run `ci/check-windows.ps1`. The Windows
build script above remains the quicker smoke entrypoint.

`cargo-ctrl` divides the CPUs available to its process into two concurrent
lanes. The general lane receives 80% and uses that many libtest workers. The
remaining CPUs are reserved for the tests listed in `controlled-tests.txt`,
which run one-at-a-time. On Linux, `taskset` gives both lanes disjoint CPU sets,
including every Cargo, rust-analyzer, and test thread they spawn. On other
platforms the same thread limits apply without CPU affinity.

Use the scheduler directly during development:

```bash
scripts/cargo-ctrl
scripts/cargo-ctrl --filter ordinary_set
BARYON_SVP_BACKEND=blocking scripts/cargo-ctrl --locked
```

Set `CARGO_CTRL_MAIN_PERCENT` to tune the split or
`CARGO_CTRL_NO_AFFINITY=1` to diagnose affinity-specific behavior.

A manifest line is an exact library test name, or an integration test written
as `<package> --test <target> <exact test name>`. Controlled tests run with
`CARGO_CTRL_LANE=controlled`, which timing tests use to decide whether to
enforce their budgets.

## Startup latency

`ci/startup-latency-check.sh` runs the libaryon engine startup test and the
baryon pty startup test from a release build with their budgets enforced. Run it
on a quiet machine; `--report-only` measures without failing. The debug engine
test also runs in cargo-ctrl's controlled lane. See
`crates/libaryon/plans/impl/startup-latency.md`.

Use this from whichever local runner is convenient: a shell session, a git hook,
a cron job, a systemd timer, or a small private build service. `.github` and
provider-specific workflow files are intentionally outside this repository's
CI model and must not be added.

## Bigfoot

`scripts/bigfoot-ci` synchronizes the current working tree straight to
`~/work/ket` on the `bigfoot` SSH host, preserves that checkout's `.git` and
`target/` directories, and runs `cargo-ctrl` there:

```bash
scripts/bigfoot-ci --bootstrap
scripts/bigfoot-ci -- --filter ordinary_set
scripts/bigfoot-ci --full
```

The bootstrap is opt-in and installs a minimal stable Rust toolchain in the
remote user's home. Source transport is `rsync` over SSH; Bigfoot never needs a
GitHub checkout, token, runner agent, or webhook.

`ci/check.sh` runs the normal generated-store huge-address tests without creating
large files. On Linux, `ci/huge-file-check.sh` creates a verified sparse 200 GiB
fixture, checks its allocated-block budget before and after bounded marker reads,
and removes it through a cleanup trap. It never performs a full-file scan.

Any future destructive/full-scan variant must require `BARYON_CI_HUGE=1`; the
huge-file entrypoint refuses `--full-scan` without that opt-in.
