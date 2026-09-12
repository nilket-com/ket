# ADR 0163: native desktop frontends

Status: proposed 2026-09-11. Platform lane. This ADR formalizes the
decision taken at the start of native Windows bring-up, that Baryon and
Polariton are native desktop applications, and records the service boundary
that the bring-up clarified. It is written after the first native builds;
its commit precedes theirs to express the dependency, not the chronology.

## Decision

### 1. Baryon and Polariton are native desktop applications

Both build, run, and pass their tests on each supported desktop from a plain
checkout. No emulation layer, container, or Unix shell is part of the
desktop product. Where a platform lacks a facility the product uses, the
product reports the missing capability explicitly; it never fails silently
or pretends the facility is present.

### 2. Polariton's scratch notebook stands alone

The SQLite `:memory:` scratch connection, SQL cells, result inspection, and
basic plotting work without Nilket present. That is the first-run surface
on every desktop, and it never requires the `toron` feature.

### 3. The Nilket deployment is a Linux service

Toron and Wendelon, as deployed by Nilket, keep their Unix process model.
Non-Linux desktops reach them over the network, or run them in a container;
there is no Windows port of the storage and supervision runtime. Whether
Toron could someday offer embedded execution is not decided here. The `ket`
router's transparent process replacement stays Linux-only under
PRODUCT-0004 for the same reason.

### 4. A checkout is byte-identical on every platform

Tests read repository files as fixtures and snapshot their sizes, so the
bytes a clone produces cannot depend on the cloning machine's settings.

## Mechanisms in this cut

These establish the decision today and may be replaced without revisiting
it: the MSVC target and `scripts/build-windows.ps1`; `cfg(unix)` gates on
tests that need a Unix facility, with platform-neutral product behavior
underneath; `--no-default-features` as the Windows Polariton head while the
`toron` feature's dependencies are Unix-only; `.gitattributes` pinning LF
and `rustfmt.toml` pinning hard tabs.

## Acceptance

1. Baryon and the reduced Polariton head build from the Windows script, and
   both binaries start on a Windows desktop.
2. The libaryon library tests, Baryon tests, Polariton binary tests, and
   the architecture guards pass natively with only `cfg(unix)` groups gated.
3. The render-evidence smoke captures plots from the SQLite scratch on
   Windows.
4. The Linux gate (`ci/check.sh`) passes on the same commit.

Items 1 through 3 were established during bring-up on 2026-09-11. Item 4
has not been run on this implementation; until it passes, this
implementation has not completed acceptance.

## Limits and follow-up

`cargo install` of Polariton on Windows needs `--no-default-features`; the
follow-up is separating portable remote Toron access from Linux-only local
supervision, so the Windows head can connect to a Nilket deployment without
carrying its supervision code. Durable saved sessions and native test
coverage for the portable job supervisor are Windows gaps, not decisions.
The interactive Baryon smoke in `docs/windows.md` has not been walked by
hand.
