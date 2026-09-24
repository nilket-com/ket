# ADR 0170: macOS is a native desktop

Status: proposed 2026-09-23. Platform lane; extends ADR 0163 to macOS and
amends its §3 for the `ket` router. Written after the first native macOS
builds, as ADR 0163 was for Windows.

## Decision

### 1. ADR 0163 holds on macOS

macOS is a supported desktop under the same rules as every other: Baryon
and Polariton build and pass their tests from a plain checkout, with no
emulation layer. Polariton's scratch notebook stands alone (ADR 0163 §2).
The Nilket deployment stays a Linux service (ADR 0163 §3): Polariton's
`toron-local` supervision is Linux-only, and the `toron` feature provides
the remote-client path a macOS desktop would use to reach it.

### 2. `ket` process replacement is Unix, not Linux

ADR 0163 §3 kept the router Linux-only "for the same reason" as Nilket. The
reason does not carry over: `exec`, device/inode identity, and mode bits are
POSIX, so macOS gets the same transparent replacement PRODUCT-0004
specifies. The target keeps ket's PID, signals, working directory,
environment, and standard streams. Siblings resolve beside the resolved ket
executable on every Unix, so a symlinked install behaves the same whether
or not the platform reports its executable path resolved. Windows keeps
the refusing entrypoint.

### 3. Per-user state lives where the platform puts it, and tests isolate
it

Engine and session state follow macOS conventions under
`~/Library/Application Support`. Only live-window sockets differ:
`~/.polariton/run`, because macOS caps a socket path at 104 bytes and
`Application Support` leaves long user names no room. An embedder can turn
persistent command history off (`EngineConfig::persistent_history`).
Polariton's test engines opt out; libaryon's unit tests compile the
persistent load out. Integration and child-process tests that need real
state directories isolate every variable their platform reads for them,
not only the XDG ones.

## Mechanisms in this cut

These may be replaced without revisiting the decision:
Linux-only target dependencies for `toron-local`, with a `toron_local` build
cfg, so Polariton's default features build on macOS; one recursive FSEvents
watch per outermost directory for the Git observer, filtered back to the
directories a non-recursive watch would cover; a catch-up Git refresh after
watch setup on every platform; `cfg(unix)` for the job supervisor's
process-group path.

## Acceptance

1. Baryon, the `toron` Polariton head, and `ket` build natively on macOS.
2. The libaryon, Baryon, Polariton, and `ket` tests pass natively on macOS,
   including the process-group and process-replacement tests, and leave
   nothing in the user's Application Support or `~/.polariton`.
3. The Linux lanes of `ci/check.sh` pass on the same tree: formatting,
   packaging, both `cargo-ctrl` backends, the architecture test, and the
   reduced Polariton head.

Items 1 and 2 were established on 2026-09-23 on Apple silicon, and item 3 on
2026-09-24 on bigfoot. The gate's cross-target step does not pass on a Linux
host: `ring`, `aws-lc-sys`, `psm`, and `stacker` compile C in their build
scripts and need a cross toolchain and SDK the runner lacks. Its Windows
check fails first, so the macOS checks after it never ran; that is how a
macOS-only compile error went unnoticed before this port. Native macOS
builds, not a Linux cross-check, are this platform's compile evidence.
The same tree also passed natively on Windows on 2026-09-24, apart from one
named-pipe ownership test that fails only under an elevated SSH session and
passes in the interactive one.

## Limits and follow-up

A root-level `cargo build` of the whole workspace still compiles Nilket's
Linux-only crates, which the workspace takes in as path members; building
Polariton itself works with its default features. There is no `.app` bundle,
signing, or notarization yet. Native per-platform gates replace the Linux
gate's cross-target step: `ci/check-macos.sh` on macOS and
`ci/check-windows.ps1` on Windows, each run on the same commit as
`ci/check.sh`. The invalid-UTF-8 file-name fixtures fail to create on the
APFS volume used for bring-up (`EILSEQ`), so the tests that need them are
gated off macOS; the product behavior is unchanged. Neither Baryon's
interactive smoke, a Polariton GUI session, nor a live remote Toron
connection has been walked on macOS; those are validation still to do.
