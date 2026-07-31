# ADR 473 implementation receipt

The gauge gained a `ladder` command and the arc gained a denominator. Seven
primitives, three engines, one methodology, all conditions recorded — and the
answer changes what the next engine cut should be.

## The instrument (reusable, in-repo)

`toron-tpch ladder --sf N --engines toron,duckdb,polars_sql --runs R` runs the
primitive set across engines over the **same persisted Parquet files** (DuckDB via
`read_parquet` views on those exact paths; Polars via the same source), emits one
JSONL row per (engine, primitive) into `ladder.jsonl`, and prints a ratio line per
primitive against the best observed engine. Recorded per row: engine + version,
`duckdb_threads` / `polars_max_threads`, mode, scale, data source and directory,
`memory_per_task` + provenance, runs/warmup, every sample, median, output row
count, digest, spill bytes, peak RSS, plan fingerprint, and any failure verbatim.
`TORON_TPCH_DUCKDB_THREADS` makes the reference thread count a first-class,
reproducible condition.

## Conditions for the measured pass

sf=8 (lineitem 47,989,007 rows / 6,817,318,944 logical bytes), strict, persisted
Parquet, `memory_per_task` 64 MiB explicit, spill reserve 2 GiB explicit, 5 timed
runs + 1 warmup. **All reference engines pinned to one thread**
(`TORON_TPCH_DUCKDB_THREADS=1`, `POLARS_MAX_THREADS=1`); Toron measured at 100% CPU
(single-threaded) on a 24-core box. DuckDB v1.5.4. Output row counts agree across
all three engines on every primitive.

## The ladder

| primitive | isolates | Toron | DuckDB 1t | Polars 1t | **Toron / best** |
|---|---|---:|---:|---:|---:|
| m00 | `SUM(1)` — intended scan proof | 814.6 ms | 25.0 | 0.8 | (see note) |
| m01 | `COUNT(1)` | 362.7 ms | 12.0 | 0.9 | (see note) |
| m02 | one decimal decode + sum | 1,701.8 ms | **86.0** | 259.2 | **19.8×** |
| m03 | predicate + 2nd column | 1,162.8 ms | **254.0** | 347.9 | 4.6× |
| m04 | grouping, 3 groups | 3,973.3 ms | **183.0** | 680.2 | **21.7×** |
| m05 | grouping, 12M groups | 12,378.6 ms | 3,813.0 | **2,514.9** | 4.9× |
| m06 | one equi-join | 23,266.3 ms | **1,253.0** | 2,809.0 | **18.6×** |

**m00/m01 are not floors and must not be quoted as ratios.** Polars answers both in
under a millisecond over 48M rows — metadata reads, not scans. The intended
scan-proof (`SUM(1)`) is folded by competent optimizers exactly like `COUNT(1)`;
the lesson is that no count-shaped query can serve as a scan floor. **m02 is the
real floor** — it must touch a column.

## What this says

- **The gap is in the per-row scalar path, not in the aggregate machinery.** m02
  has no grouping, no join, no filter — one decimal column decoded and summed — and
  it is already ~20× off the best reference. m04's grouping (21.7×) sits on top of
  that substrate rather than explaining it.
- **Selectivity/pushdown is Toron's best surface** (m03, 4.6×): where work can be
  avoided, it is avoided competitively.
- **High-cardinality grouping is the *least* pathological ratio** (m05, 4.9×) — the
  one primitive the 446–472 reservoir arc has been optimizing. Toron is closer to
  the field there than anywhere else on the ladder.
- **Joins are ~19× off** (m06) — the single largest absolute gap: 23.3 s versus
  1.25 s for one equi-join.
- Toron used one core throughout while the box has 24; the reference numbers are
  also single-threaded, so **the ratios above are per-core and parallelism is a
  separate, unexploited multiplier on top**.

## Consequence for the arc

Twenty ADRs of reservoir and placement work optimized the primitive where Toron is
already most competitive, while a 20× per-core gap sits in decode/accumulate and a
19× gap sits in a bare join. That is not a criticism of the sealed results — each
was honest within its cell — but the ladder is the denominator that was missing,
and it redirects the next cut.

No optimization work was performed in this cut; the ladder is an instrument, and
what it reveals is the next cut's problem.
