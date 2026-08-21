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

## Recommended next steps

1. **db team:** configure the QUIC idle timeout and/or a keep-alive so the
   statement timeout is reachable, and long queries fail as timeouts rather
   than transport losses. This is small and unblocks everything below.
2. **db team:** determine whether ~460 rows/s is specific to materialized
   tables or general to scans, using a non-materialized control table of
   comparable size.
3. **viz team:** rerun `live_scatter_threshold_evidence` once reads clear
   50,000 rows in reasonable time, and only then record threshold verdicts.
4. **Do not tune the scatter thresholds** until step 3 produces numbers.
