# Startup latency tests: implementation findings

Companion to `startup-latency.md`. Records where the `impl:` commit departs from the plan and what
it measured.

## Delivered

- `crates/libaryon/tests/startup_latency_test.rs`: S0, S1, S2, S3 (1 MiB, 1 GiB, 16 GiB; 64 GiB with
  `BARYON_STARTUP_S3_64GIB=1`), S4 and S5 (unix), 9 samples each. Each sample is split at spawn
  returned (M2), `Resize` submitted (M2r), first usable viewport (M3) and first text (M3t); one JSON
  line per scenario and span (`spawn`, `M2-M2r`, `M2r-M3`, `M3-M3t`). The budget is asserted on
  M2r-M3, plus a spawn budget for every scenario.
- `crates/baryon/tests/startup_latency_pty_test.rs` (Linux): S1 and S2 under a 120x40 pty, M0-M4 and
  M4-M5.
- `scripts/cargo-ctrl`: manifest lines may name integration tests as
  `<package> --test <target> <exact name>`; controlled commands run with `CARGO_CTRL_LANE=controlled`.
  `ci/controlled-tests.txt` lists the engine test.
- `ci/startup-latency-check.sh`: both tests from a release build, budgets enforced (`--report-only` to
  measure only).

## Departures from the plan

1. **`libc`, not `nix`.** The pty harness needs `openpty`, `setsid` and `TIOCSCTTY`. `libc` is already a
   workspace dependency, so it adds no package and no feature selection; the lockfile gains one line.
   It is a Linux-only dev-dependency of baryon and passes the product dependency guard.
2. **Enforcement is a lane signal, not a skip.** Both tests always measure and print. They assert
   budgets only under `CARGO_CTRL_LANE=controlled` or `BARYON_STARTUP_LATENCY_ENFORCE=1`; otherwise
   they print each would-be failure and say budgets were not enforced.
3. **No `initial_viewport`, as in baryon.** A frame rendered before the resize then has the 80-column
   fallback width, so the test refuses a first viewport whose focused pane is not 100 columns wide.
   That is the check 4.1 asked for: M3 is provably resize-driven.
4. **S2 settings come from `EngineSettings`.** Production `init.rn` files call `config.set`, which
   requests a render (4.1 forbids that). S2 uses default settings plus an init script of commands,
   mappings and an option, which register without rendering, and 1,000 history lines.
5. **Startup scripts are checked, not trusted.** The first viewport must carry no status message. A
   script that fails to compile reports `Rune error: ...` there, and would otherwise look like a fast
   startup. Verified by breaking the script.
6. **The S2/S0 factor is per profile:** 5 in debug, 12 in release. Measured ratios are 1.8 and 3.6-4.1;
   release S0 is under 2 ms, where a ratio is mostly noise.
7. **The check script also runs the engine test in release**, so its release budgets are enforced
   somewhere. The plan placed only the pty test there.
8. **The pty harness answers terminal queries.** See finding 1.
9. **M4 and M5 are output-stream proxies** (Codex review). Without a terminal emulator the harness can
   prove bytes were emitted, not that a whole frame is visible. M4 is "first content-bearing paint
   observed": the fixture's first and last visible lines both drawn after `ESC[?1049h`, possibly by
   different redraws. M5 is "typed text echoed": the marker appears in output written after M4.
10. **Release budgets have a 10 ms floor** (Codex review): S0 and S5 measure 0.6-1.9 ms, where
    scheduler noise dominates. Tighten after controlled-run history.
11. **Spawn has its own budget** (25 ms debug, 10 ms release). Saved-session restore runs inside
    `EngineHandle::spawn`, so an M2r-M3 budget alone would not see an S5 regression.

## Findings

1. **Baryon makes five terminal round trips before its first frame.** The minimap, diff, UAST-graph and
   markdown-preview controllers each call `Picker::from_query_stdio()` from `Frontend::new`, which
   writes a kitty-graphics query, `ESC[c`, `ESC[16t` and `ESC[5n` and waits up to 2 s for the status
   reply; ratatui then reads the cursor position (`ESC[6n`). The harness replies as a terminal without
   sixel or kitty graphics would. A terminal that never answers costs about 2 s per picker, and the
   missing cursor reply makes baryon exit with "The cursor position could not be read within a normal
   duration". One shared picker would remove three round trips.
2. **The 1 MiB first-text anomaly reproduces and is large in debug.** M3-M3t for a sparse 1 MiB file is
   about 370 ms in debug and 37 ms in release, against 26 ms and 6.4 ms for 1 GiB (measured from M3).
   Still unexplained (plan 9.11).
3. **Open cost is linear in extents:** about 1 ms per GiB in release and 7 ms per GiB in debug (S3
   slope line).
4. **M4-M5 sits at 17-21 ms in every scenario and profile,** which looks like the frontend's input
   poll interval rather than engine work.

## Baseline (development machine, medians)

Engine, after the review split (spawn, then `Resize` submitted to first usable viewport). M2-M2r is
under 10 us everywhere.

| Scenario | debug spawn | debug M2r-M3 | release spawn | release M2r-M3 | release M3-M3t |
| --- | --- | --- | --- | --- | --- |
| S0 | 0.6 ms | 6.2 ms | 0.1 ms | 1.7 ms | n/a |
| S1 | 0.5 ms | 16.6 ms | 0.1 ms | 3.3 ms | 5.4 ms |
| S2 | 0.5 ms | 12.0 ms | 0.1 ms | 5.4 ms | n/a |
| S3 1 MiB | 0.5 ms | 11.7 ms | 0.1 ms | 2.1 ms | 37 ms |
| S3 1 GiB | 0.5 ms | 13.5 ms | 0.1 ms | 1.9 ms | 6.4 ms |
| S3 16 GiB | 0.6 ms | 127 ms | 0.1 ms | 14.6 ms | 3.6 ms |
| S4 | 0.5 ms | 20.7 ms | 0.1 ms | 9.7 ms | n/a |
| S5 | 5.8 ms | 3.6 ms | 1.0 ms | 0.6 ms | 0 (text in first frame) |

Terminal head (pty):

| Scenario | debug M0-M4 | release M0-M4 | debug M4-M5 | release M4-M5 |
| --- | --- | --- | --- | --- |
| S1 | 28 ms | 20 ms | 21 ms | 17 ms |
| S2 | 45 ms | 20 ms | 21 ms | 17 ms |
