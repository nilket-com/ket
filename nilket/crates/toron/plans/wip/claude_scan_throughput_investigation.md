# Scan throughput — investigation state and follow-up direction

**Status:** Claude council input, 2026-08-21. Untracked. Consolidates the
whole investigation for the db team and states where it goes next.
Supersedes `claude_transport_liveness_finding.md`,
`claude_scan_throughput_finding.md`, and
`claude_iter_2_scan_batching_rerun.md` as the current summary; those remain
as the record of how each conclusion was reached. Committed detail:
`plans/product/impl/0137_scatter_threshold_evidence.md`.

## Origin

A viz-side evidence slice set out to measure three scatter rendering
thresholds (50,000 / 200,000 / 1,000,000 points) that have governed
`ResolvedPlotSettings` since ADR 0007 without a benchmark behind them. It has
produced **no threshold numbers**, because rendering has never once been
reached at any threshold cardinality. What it produced instead is this.

## Settled

**Transport — fixed (TORON-0577).** Queries formerly died at exactly 30.01 s
with untyped `connection lost`, because no `idle_timeout` or
`keep_alive_interval` was configured anywhere and quinn's ~30 s default beat
both the 60 s statement timeout and the 120 s client timeout — leaving both
unreachable. Now a long query returns typed
`Toron StatementTimeout: statement timeout exceeded` at 60.01 s. The scope
widening to internal worker links was correct and beyond what the finding
identified.

**Not materialization.** A table built by ordinary
`CREATE TABLE … AS SELECT` reads at the same rate as one built by
`materialize`. PRODUCT-0137's write path is exonerated.

**Not client formatting.** Dispatching through the Arrow-IPC adapter path,
which renders no text table, is equally slow. The CLI is exonerated.

**Not response framing (WENDELON-0088).** This is the decisive negative
result. 0088 proved 5,000 Wendelon rows now arrive as two batches, and the
handoff set the criterion: if the full path rose toward ~4,500 rows/s,
per-frame daemon overhead was material. It did not.

| rows | pre-0088 | post-0088 | post rate |
|---|---|---|---|
| 2,000 | 4.38 s | 8.77 / 8.66 s | ~230 rows/s |
| 5,000 | 10.77 s | 17.69 s | ~283 rows/s |
| 13,000 | 27.50 s | 27.02 s | ~481 rows/s |

At the largest size measured, 481 rows/s against 461 rows/s before — the same
number. Response framing is not the dominant cost.

## Two tracks for the follow-up, and they are separate

**Track 1 — locate the per-row cost.** Profile the Wendelon table-query path:
per-location materialization and PBC. That is where the ~2 ms per row lives,
now that framing, materialization, and formatting are each excluded by
control. A useful target is whatever makes 50,000 rows arrive comfortably
inside the statement timeout; today that read cannot complete at all.

**Track 2 — explain the new small-result fixed latency, first.** Small
queries roughly doubled while large ones did not move: 2,000 rows measured
8.77 s and 8.66 s warm, against 4.38 s twice before 0088. The cost shape
changed from flat per-row to a large fixed component plus per-row —
consistent with a batch-accumulation threshold or timer that small results
now wait on.

This matters beyond its own size: **an unexplained fixed latency sits inside
every subsequent measurement.** Until it is accounted for, no profiling run
or benchmark on this path is stable, because the baseline itself moved for
reasons nobody has stated. It is also cheap to check — a query returning
fewer rows than the batch threshold should not pay a wait.

## Measurement caveat

Small-size numbers are tight and repeatable. Large-size numbers are not:
13,000 rows measured 27.02 s on the plain control and 35.19 s on the
materialized table in the same session. Treat anything at 13,000 rows as
±30%, and prefer the 2,000-row point for regression detection until the
variance is understood — it may be the same phenomenon as track 2.

## Standing constraint

At ~480 rows/s the 60 s statement timeout caps every query in the product
near **29,000 rows**. That is a product-wide read ceiling, not a plotting
concern:

| target | rows | projected |
|---|---|---|
| statement timeout ceiling | ~29,000 | 60 s |
| smallest scatter threshold | 50,000 | ~1.7 min |
| raster threshold | 1,000,000 | ~35 min |
| README "10M rows" claim | 10,000,000 | ~6 h |

**Visualization work remains blocked** and no chart threshold has been or
will be tuned until a 50,000-point read completes. The viz-side rerun is one
opt-in command and will be run on request:

```sh
cargo test -p polariton --bin polariton \\
  live_scatter_threshold_evidence -- --ignored --nocapture
```

## Environment for every number above

12th Gen Intel Core i7-12700K (16 threads), 61 GiB, Linux 7.0.0-29-generic;
release builds, `nilket local` against one data directory, systemd unit
stopped so no stale binary served; load average 1.57, no competing work.
Pre-0088 figures are binary `5e32f991…`, post-0088 are `b1602ac5…`.
