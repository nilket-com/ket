# Platform Support

Begin moving Baryon from a Linux-only editor toward a deliberate multi-platform
architecture. The near-term goal is not to dilute the Linux-first design; it is
to isolate the platform-specific pieces so Windows and macOS can be added
without disturbing the editor core.

## Assessment

- The primary portability wall is `src/svp/resolver.rs`, which currently owns
  the Linux `io_uring` dependency and uses Unix file descriptors.
- The rest of the editor is much closer to portable Rust: UAST, projection,
  rendering, languages, editing, diffing, Markdown preview, settings parsing,
  and command handling are not inherently Linux-only.
- Native Windows support should target Windows 11 and use Windows I/O Rings.
  Native Windows 10 support is not a goal.
- macOS should start with a portable blocking-read resolver. `kqueue` is not a
  replacement for `io_uring` random file reads; a native `DispatchIO` backend
  can be considered later if measurements justify it.
- A portable blocking backend is useful even if Windows gets I/O Rings, because
  it gives macOS a path and provides a fallback for unsupported capabilities.

## 1. Resolver Backend Boundary

- Split the current `SvpResolver` implementation into a stable public shell and
  backend-specific implementation modules.
- Keep the public surface unchanged where possible:
  - `SvpResolver::new`
  - `register_device`
  - `request_dma`
  - `request_queue` access used by ingestion/sequential scans
  - `RequestPriority`
  - `SvpRequest`
- Move shared completion behavior into reusable helpers:
  - metric inflation
  - viewport hot-swap
  - sequential scan continuation
  - UI refresh notification
- Make `io-uring` a Linux-only dependency in `Cargo.toml`.
- Preserve Linux behavior first; this phase should be a refactor with no
  product behavior change.

## 2. Portable Blocking Backend

- Add a resolver backend built from standard `File`, `Seek`, and `Read` APIs.
- Run it on the same resolver thread boundary so the editor thread remains
  non-blocking.
- Keep the existing 64KB chunk size and request priority semantics.
- Use this backend for macOS initially and as a generic fallback.
- On Linux, allow `BARYON_SVP_BACKEND=blocking` to force this backend during
  validation while keeping `io_uring` as the product default.
- Verify large-file scrolling, metric inflation, save behavior, and search over
  source-backed documents.

## 3. Windows 11 I/O Rings Backend

- Add a Windows backend using I/O Rings rather than IOCP.
- Probe support at startup with the Windows I/O Ring capability API.
- Use Windows Build 22000 or newer as the native support floor.
- Map Baryon's existing request model to Windows I/O Ring operations:
  - open file handles per registered device
  - submit offset reads
  - carry node id, priority, buffer id, and scan continuation metadata through
    completions
  - update registry state through the shared completion helper
- Fall back to the blocking backend if capability probing fails.

## 4. macOS Backend Strategy

- Start with the portable blocking backend.
- Do not attempt to map the resolver to `kqueue`; it is not the right primitive
  for Baryon's random file-read workload.
- Consider a `DispatchIO` backend only after macOS support is otherwise working
  and measurements show the blocking backend is a real bottleneck.

## 5. Supporting Portability Work

- Replace XDG/HOME-only config path assumptions with a platform-aware settings
  path helper:
  - Linux and other XDG-style targets: `$XDG_CONFIG_HOME/baryon/settings.toml`,
    falling back to `$HOME/.config/baryon/settings.toml`
  - macOS: `$HOME/Library/Application Support/baryon/settings.toml`
  - Windows: `%APPDATA%\baryon\settings.toml`, falling back to
    `%USERPROFILE%\AppData\Roaming\baryon\settings.toml`
- Keep `~` path expansion portable, including Windows home directory
  environment variables.
- Verify `arboard` clipboard behavior on Windows and macOS.
- Verify `crossterm` alternate screen, mouse capture, bracketed paste, and raw
  mode behavior on Windows Terminal, iTerm2, and Apple Terminal.
- Treat terminal image support as best-effort. The existing `ratatui-image`
  halfblock fallback should remain the baseline for terminals without graphics
  protocol support.
- Keep dependency feature choices friendly to Linux-hosted cross-target checks.
  `sqlparser` intentionally uses `std` without `recursive-protection` so
  Windows/macOS `cargo check` does not require target C/assembly toolchains for
  `psm`.
- Keep CI provider-neutral and Linux-hosted by default. The repo-owned entrypoint
  should be a plain shell gate under `ci/`, suitable for a private Linux runner,
  cron job, systemd timer, git hook, or small home-rolled build service.
- Do not add `.github/workflows/*` or other hosted-provider config unless the
  project explicitly chooses that direction later.

## 6. Validation Plan

- Linux refactor phase:
  - `cargo test --locked --lib -- --test-threads=1`
  - `cargo test --locked --test architecture_test`
  - manual large-file smoke test
- Portable backend phase:
  - force the blocking backend on Linux with `BARYON_SVP_BACKEND=blocking` and
    run the same tests
  - verify viewport DMA, minimap, Markdown preview, save, search, and shadow
    generation behavior
- Linux-hosted cross-target phase:
  - `cargo check --locked --target x86_64-pc-windows-msvc`
  - `cargo check --locked --target x86_64-apple-darwin`
  - `cargo check --locked --target aarch64-apple-darwin`
- Home-rolled CI phase:
  - keep `ci/check.sh` as the source of truth for the Linux runner gate
  - run Linux tests with both default `io_uring` and forced blocking resolver
    backends
  - run Windows/macOS cross-target checks from Linux
  - avoid hosted-provider workflow files by default
- Settings path phase:
  - unit-test Linux/XDG, macOS Application Support, and Windows APPDATA path
    selection from synthetic environment values
  - verify the same code with Linux-hosted Windows/macOS `cargo check`
- Platform phase:
  - Windows 11: open/edit/save large files, split diff, Markdown preview,
    clipboard, mouse, and settings persistence
  - macOS: same smoke suite, with special attention to terminal image fallback
    and clipboard permissions
