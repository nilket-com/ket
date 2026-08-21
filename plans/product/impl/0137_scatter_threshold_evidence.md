# Evidence slice — scatter thresholds, and what blocked measuring them

**Status:** measurement attempt, 2026-08-21. Not a record and not a plan. It
reports what one real workflow does end to end, and the two defects that stop
the intended measurement from being taken.

**Headline: the thresholds could not be measured.** Reading a materialized
table runs at roughly **460 rows per second**, and any query exceeding ~30
seconds dies on an unconfigured QUIC idle timeout before either configured
timeout can fire. The smallest threshold under test is 50,000 points, which
would need ~109 seconds of reading. The exercise is blocked on the engine,
not on the plotting layer.

## What was exercised

The fifth arc's whole chain, in one process, for the first time:

```rune
ket.range(N).project_named([
    seeded_random(lit_i64(20260821), col("ordinal")).named("x"),
    seeded_random(lit_i64(20260822), col("ordinal")).named("y"),
]).materialize("evidence_scatter_N")
```

routed **through the production compiler** (PRODUCT-0137's terminal `ket`
route), then reopened as `ket.table(...).views([scatter("x","y")])` and
rendered. The exhibit is `live_scatter_threshold_evidence` in
`crates/polariton/src/adapters/toron/mod.rs`, opt-in behind `--ignored`.

## Environment

| item | value |
|---|---|
| CPU | 12th Gen Intel Core i7-12700K, 16 threads |
| memory | 61 GiB (40 GiB free during runs) |
| kernel | Linux 7.0.0-29-generic |
| nilket binary | release, `607d653c…`, built from `4b5bc818` (ADR 0575) |
| deployment | `nilket local`, same data dir, dev-insecure KEK |
| renderer | `SvgBackend`, 1200×720, `ResolvedPlotSettings::default()` |

The binary was verified identical (same SHA-256) to the operator's
reinstalled `~/.cargo/bin/nilket`, so nothing here is a stale-build artifact.

## What works

Materialization is sound end to end. For N = 49,999:

- the terminal `ket` relation compiled with zero presentation slots;
- the receipt reported **49,999 rows written** and
  **certificate format version 1**;
- the data is correct — `SELECT x … LIMIT 1` returns `0.996570334516858`, a
  valid unit-interval `seeded_random` value;
- bounded reads return exactly the requested rows.

## Defect 1 — reads run at ~460 rows/second, linearly

| rows | wall time | rate |
|---|---|---|
| 2,000 | 4.39 s | 456 rows/s |
| 5,000 | 10.76 s | 465 rows/s |
| 13,000 | 28.19 s | 461 rows/s |

Constant ~2.2 ms per row for two `Float64` columns. No cliff, no quadratic
term — a flat per-row cost. Extrapolated against the thresholds under test:

| threshold | projected read time |
|---|---|
| 50,000 | ~1.8 min |
| 200,000 | ~7.2 min |
| 1,000,000 | ~36 min |

**Not established:** whether this is specific to materialized tables. The
deployment had no comparable non-materialized table (`demo_sales` is absent),
and the obvious control — `CREATE TABLE … AS SELECT … LIMIT 14000` — hits
defect 2 before it can produce one. Answering this is the first diagnostic
step, and it decides whether the fault is in the materialization write path's
resulting layout or in the scan path generally.

## Defect 2 — the QUIC idle timeout preempts every configured timeout

Every query over the boundary fails at **exactly 30.01 s** with
`Toron transport: connection lost`. The daemon does **not** crash: toron kept
one PID across every failure, 85 MB RSS, with 40 GiB free. Its own log shows
only `QUIC worker connection error: query IO error: connection lost`, and
Wendelon reports `sending stopped by peer` — the downstream cancel, not a
cause.

The configured timeouts are both longer and therefore **unreachable**:

| bound | value | reachable? |
|---|---|---|
| QUIC idle (unconfigured; library default) | ~30 s | fires first |
| SQL statement timeout (`toron.toml`) | 60 s | never |
| client `DEFAULT_QUERY_TIMEOUT` | 120 s | never |

Neither `idle_timeout` nor `keep_alive_interval` appears anywhere in
`crates/toron` or `crates/toron-client`, so the transport default governs. A
query that produces no packets for 30 s loses its connection regardless of
either policy.

Two consequences worth separating:

1. **No long query can ever report a timeout.** A statement that should be
   refused at 60 s reports a transport failure at 30 s instead — a typed,
   actionable error replaced by one indistinguishable from a network fault.
2. **The 14,000-row boundary was an artifact**, not a limit. 14,000 rows takes
   ~30.4 s, which straddles the cut; it passed on one binary and failed on
   another for that reason alone. Anyone bisecting this will find a "size
   limit" that does not exist.

## What this says about the thresholds

Nothing yet, and deliberately so. `scatter_max_vector_points = 50_000`,
downsample at 200,000 and raster at 1,000,000 remain **unmeasured**, and this
slice does not propose changing them. Rendering was never reached at any
threshold cardinality.

The exercise remains the right one. It cost one session and found two engine
defects that no amount of further design work would have surfaced — including
one, the unreachable timeout configuration, that is independent of
materialization entirely and affects every long query the product can issue.

## Post-fix rerun — 2026-08-21, after TORON-0577 and TORON-0578

Rerun on a release build of `5e32f991…` (from `e7b285de`), same data
directory, same machine.

**Defect 1 is fixed.** `SELECT COUNT(*)` now returns
`Toron StatementTimeout: statement timeout exceeded` at **60.01 s** — a typed
outcome at the configured bound, where it previously returned an untyped
`connection lost` at 30 s. The statement timeout is authoritative and
reachable. TORON-0577 also widened the policy to internal worker links, which
this slice had not identified.

**Defect 2 is unchanged, and is now much better characterised.**

| rows | materialized | plain CTAS control | Arrow path (no text) |
|---|---|---|---|
| 2,000 | 4.38 s | 4.38 s | 4.39 s |
| 5,000 | 10.93 s | 10.77 s | 10.92 s |
| 13,000 | 28.47 s | 27.50 s | 27.53 s |

Three controls, one conclusion. The open question from the first run is
**answered: the cost is not materialization-specific.** A table built by an
ordinary `CREATE TABLE … AS SELECT` reads at the same rate as one built by
`materialize`, so the fifth arc's write path is exonerated. And routing
through the Arrow-IPC adapter, which renders no text table, is equally slow,
so the CLI's formatting is exonerated too.

**~460 rows/s (≈2.2 ms/row) is general Toron scan throughput.**

TORON-0578 fixed a real defect — `COUNT(*)` over a `Range` panicking once
column pruning left an empty schema — but that is a different bug from this
one, and this slice never reached it: its `COUNT(*)` ran over a materialized
Wendelon table, not a range.

### What this bounds

At ~460 rows/s the 60 s statement timeout caps any query at roughly **27,000
rows**. That is the product-wide ceiling today, not a plotting concern:

| target | rows | projected |
|---|---|---|
| statement timeout ceiling | ~27,000 | 60 s |
| smallest scatter threshold | 50,000 | ~1.8 min |
| downsample threshold | 200,000 | ~7.2 min |
| raster threshold | 1,000,000 | ~36 min |
| README claim | 10,000,000 | ~6 h |

The scatter thresholds remain unmeasured and untuned. Rendering has still
never been reached at any threshold cardinality.

## Second rerun — after WENDELON-0088 (response batching)

0088 batched Wendelon table-query responses; the db-team handoff set the
criterion: if the full path rose toward ~4,500 rows/s, per-frame daemon
overhead was the material cost. Measured on binary `b1602ac5…` (from
`cb0037e9`), same machine and data directory, load average 1.57:

| rows | pre-0088 | post-0088 | post rate |
|---|---|---|---|
| 2,000 | 4.38 s | 8.77 / 8.66 s | ~230 rows/s |
| 5,000 | 10.77 s | 17.69 s | ~283 rows/s |
| 13,000 | 27.50 s | 27.02 s | ~481 rows/s |

**Decisive negative result: response framing is not the dominant cost.** At
the largest size measured the path runs at 481 rows/s against 461 before.

Two further observations, both handed to the db team:

- **Small results roughly doubled** while large ones did not move. The cost
  shape changed from flat per-row to a large fixed component plus per-row,
  consistent with a batch-accumulation threshold or timer. Until that is
  explained, no benchmark on this path is stable — the baseline moved for
  unstated reasons.
- **Large-size numbers are ±30%**: 13,000 rows measured 27.02 s on the plain
  control and 35.19 s on the materialized table in one session. The
  2,000-row point is the reliable regression detector for now.

The ceiling is therefore ~29,000 rows at the 60 s statement timeout, and a
50,000-point read still cannot complete. Thresholds remain unmeasured and
untuned.

## Recommended next steps

1. ~~**db team:** configure QUIC liveness~~ — **done**, TORON-0577.
2. ~~**db team:** establish the scan control~~ — **done here**: general, not
   materialization-specific, and not CLI formatting. Three controls agree.
3. ~~**db team:** general scan throughput~~ — narrowed further: **not
   response framing** (WENDELON-0088, decisive negative). Remaining direction
   is profiling the Wendelon table-query path — per-location materialization
   and PBC — plus a separate explanation of the post-0088 small-result fixed
   latency before any benchmark on this path is treated as stable.
4. **viz team:** rerun `live_scatter_threshold_evidence` once reads clear
   50,000 rows in reasonable time, and only then record threshold verdicts.
5. **Do not tune the scatter thresholds** until step 4 produces numbers.
