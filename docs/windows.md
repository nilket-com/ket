# Native Windows builds

Start with Baryon, then build Polariton with local Toron integration disabled.
Toron and Wendelon (Nilket) remain outside this native Windows build path.
Running those services in Linux or containers is separate work.

## Prerequisites

- Rust with a native MSVC toolchain, such as `stable-x86_64-pc-windows-msvc`.
- Visual Studio or Build Tools with the C++ desktop workload and Windows SDK.
- Access to crates.io and GitHub, or a populated Cargo dependency cache. The
  pinned Ruff dependency is fetched from GitHub.

Use PowerShell from the repository root. If Rust cannot locate the MSVC linker,
use a Visual Studio Developer PowerShell matching your target architecture.

## Baryon first

```powershell
.\scripts\build-windows.ps1
.\target\x86_64-pc-windows-msvc\debug\baryon.exe --help
.\target\x86_64-pc-windows-msvc\debug\baryon.exe README.md
```

The script builds the native MSVC target explicitly and preserves `Cargo.lock`.
Pass `-Release` for an optimized binary in the target's `release` directory.
Pass `-Test` to run the engine library and frontend binary tests serially after
building. Tests use the debug profile even when the binary build uses `-Release`.
These are initial native checks; the complete Linux CI gate remains separate.
For ARM64 MSVC hosts, substitute `aarch64-pc-windows-msvc` in the output paths.

For an interactive smoke check, open a disposable file, enter Insert mode with
`i`, type text, press Escape, save with `:w`, and quit with `:q`. Reopen it and
verify the saved text. Also check arrow keys, repeated typing, resizing, and
clipboard paste in your terminal.

## Polariton next

```powershell
.\scripts\build-windows.ps1 -Product polariton
.\target\x86_64-pc-windows-msvc\debug\polariton.exe
```

This selects the reduced head, `cargo build -p polariton --no-default-features`,
with the locked dependencies and native target. A plain `cargo build -p
polariton` also works: the default `toron-local` feature's local process
supervision is Linux-only and compiles out here, leaving the remote `toron`
client. Neither establishes Windows support for remote Toron
connections.

For a plotting example that works without Toron, open
`uat/native_plot_showcase.md` and run the cells in order on the scratch SQLite
connection. See [the UAT guide](../uat/README.md) for GPU capture commands.
The relation-based `uat/plot_distribution_showcase.rn` requires Toron and does
not use the portable notebook API.

## Line endings and formatting

`.gitattributes` pins every text file to LF and `rustfmt.toml` pins hard tabs.
Both matter on Windows: a `core.autocrlf=true` clone otherwise checks out CRLF,
and engine tests that read repository files as fixtures, snapshot byte counts,
or run the bash CI scripts then fail for reasons unrelated to the code. A clone
made before `.gitattributes` existed keeps its CRLF working tree until the
files are re-checked out; deleting the tracked files and running
`git checkout -- .` is the quickest way to renormalize.

## What is Unix-only by design

- Durable saved sessions (`SessionStore`) need `flock`; `SessionStore::create`
  and `open_*` answer `UnsupportedPlatform` on Windows and the engine runs
  without checkpoints. The `SavedSession<Writable>` mutation API compiles on
  every platform so the typestate tests hold everywhere.
- The job supervisor has a portable fallback, but its tests spawn `/bin/sh`
  fixtures and are gated `cfg(unix)`. A self-executing test helper would give
  the portable supervisor native coverage.
- Polariton's live agent connections, local Toron supervision (the `toron`
  feature), saved-session listing, and the `windows`/`observe`/`act`
  subcommands are Unix-only.
- The `ket` router replaces its process with `exec`, which PRODUCT-0004 scopes
  to Linux in v1; `ket.exe` builds but answers with that refusal. Run
  `baryon.exe` and `polariton.exe` directly.

## Validation status

On 2026-09-11 Baryon built and ran natively with Rust 1.98.1 on
`x86_64-pc-windows-msvc`: `baryon.exe --help` and `--version` answer, and the
libaryon engine library tests pass with the Unix-only groups above gated off.
The native run found and fixed three Windows defects:

- `atomic_write` failed with `ERROR_ACCESS_DENIED` whenever the target was open
  in a buffer, so persistent `:set` never reached `init.rn`. `MoveFileExW`
  refuses to replace an open file; the write now falls back to `std::fs::rename`,
  whose POSIX-semantics rename replaces it.
- `Path::canonicalize` answers with a verbatim `\\?\C:\...` path, which leaked
  into the Git status panel root and never compared equal to plain paths.
  `core::canonicalize` strips the prefix and the engine uses it throughout.
- The single-file Rust analysis host asserted a literal `/tmp` as an absolute
  path, which panicked the semantic reactor for every Rust document.

Polariton built natively the same day with `-Product polariton`, opened its
iced window, and passes its binary tests (929) with the same gating. Two more
Windows defects fell out of that run:

- The reporting spool fsynced directories through `File::open`, which Windows
  refuses, so reporting could not be enabled at all. Directory sync is a no-op
  there now, as it already was in libaryon's atomic writes.
- The spool identified its writer process with `ps`, which Windows lacks, so
  every spool open failed with "cannot identify writer process". The Windows
  token is the process creation time from `GetProcessTimes`, with
  `GetExitCodeProcess` deciding liveness because a killed process stays
  openable while a handle to it survives.

The interactive smoke checks above have not been walked through by hand yet.
