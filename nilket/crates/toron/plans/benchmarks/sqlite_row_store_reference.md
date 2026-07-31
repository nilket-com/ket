# Benchmark finding — SQLite as a row-store reference on the primitive ladder

A row-store B-tree reference added to the ADR 473 primitive ladder. SQLite is a
different *kind* of reference than DuckDB/Polars: single-threaded, non-vectorized,
row-oriented, B-tree storage. Where Toron lands relative to it is diagnostic — beating
it confirms columnar value; losing to it flags a path weaker than the naive baseline.

## Environment

- SQLite `3.46.1 2024-08-13` (system `/usr/bin/sqlite3`).
- Data built from the same sf=8 strict parquet used by the ladder, via DuckDB
  `v1.5.4` sqlite extension. **Not the same physical source** — SQLite reads its own
  B-tree, so this measures engine+storage, not same-data engine-only.
- Single node, single thread throughout (SQLite is inherently single-threaded; the
  DuckDB/Polars ladder numbers it is compared to were pinned to one thread; Toron
  runs single-threaded).

## Reproducible DB build (native types)

DuckDB's default sqlite export stores DECIMAL as TEXT, which would make SQLite pay
per-row text→float coercion and unfairly penalize m02/m03/m10. Coerce to native
REAL/INTEGER; dates as ISO TEXT (which sort chronologically, so the m03 predicate is
correct):

```
duckdb -c "INSTALL sqlite; LOAD sqlite; ATTACH 'tpch_sf8.sqlite' AS lite (TYPE sqlite);
CREATE TABLE lite.lineitem AS SELECT
  CAST(l_orderkey AS BIGINT) l_orderkey, CAST(l_quantity AS DOUBLE) l_quantity,
  CAST(l_extendedprice AS DOUBLE) l_extendedprice, CAST(l_discount AS DOUBLE) l_discount,
  CAST(l_shipdate AS VARCHAR) l_shipdate, CAST(l_returnflag AS VARCHAR) l_returnflag
  FROM read_parquet('.../sf8/lineitem.parquet');
CREATE TABLE lite.orders AS SELECT CAST(o_orderkey AS BIGINT) o_orderkey
  FROM read_parquet('.../sf8/orders.parquet');"
# m06 join: give SQLite its idiomatic PK index (analytical engines build ephemeral hash tables):
sqlite3 tpch_sf8.sqlite "CREATE UNIQUE INDEX idx_o ON orders(o_orderkey);"
```

Row counts verified equal to the parquet (lineitem 47,989,007; orders 12,000,000).
**The 2.3 GB DB itself is not committed** — rebuild from the command above.

## Measurement treatment

- Page cache warmed with one full `COUNT(1)` scan before timing.
- Best-of-3 warm runs (approximates warm-cache CPU-bound cost). SQLite process
  startup measured at ~0 ms, so wall ≈ query.
- m04/m05 also measured `COUNT(*)`-wrapped to separate grouping compute from
  formatting 12M output rows to text — m05 shed ~1 s that way (8,390 ms is the
  grouping compute; the raw 9,350 ms included output formatting). m04's 3-row output
  formats instantly, so its number is unaffected.

## The ladder (sf=8, ms; Toron/DuckDB/Polars from the ADR 477 ladder, single-thread)

| primitive | isolates | Toron | DuckDB | Polars | SQLite | Toron ÷ SQLite |
|---|---|---:|---:|---:|---:|---:|
| m01 | count (full scan) | 351 | 12 | 1 | 750 | 2.1× faster |
| m02 | decimal decode+sum | 428 | 81 | 243 | 1,430 | 3.3× faster |
| m03 | filter + 2nd col | 612 | 257 | 352 | 1,850 | 3.0× faster |
| m04 | group ×3 | 2,501 | 189 | 678 | 10,460 | 4.2× faster |
| m10 | two-col mul+sum | 643 | 194 | 1,289 | 2,230 | 3.5× faster |
| m05 | group ×12M | 10,671 | 3,840 | 2,481 | **8,390** | **0.79× — slower** |
| m06 | equi-join | 22,461 | 1,232 | 2,739 | **6,510** | **0.29× — slower** |

(m00/m01 are not scan floors — DuckDB/Polars answer counts from metadata; kept only
for context.)

## Caveats

1. **Not same-source.** SQLite reads a 2.3 GB B-tree; the columnar engines read
   parquet. Storage substrate differs, so this is a row-store *baseline*, not an
   apples-to-apples same-data point.
2. **m06 join strategy differs.** SQLite used a persistent unique PK index
   (idiomatic index nested-loop); the analytical engines built ephemeral hash tables.
   Not identical algorithms — but a row engine being 3.4× faster than Toron is still
   diagnostic of magnitude.

## Conclusion (scoped)

On scalar and low-cardinality aggregation Toron is 3–4× faster than SQLite — it is a
genuine columnar engine, its per-row overhead notwithstanding. The two primitives
where Toron **loses to a row-store B-tree** are m05 (high-cardinality grouping) and
m06 (join). Both build a large hash structure keyed by many distinct values.

This **suggests the distinct-key grouping/join hash substrate is the next diagnosis
target** — the path where Toron underperforms even the naive baseline. It is **not**
proof that grouping and join share one root cause; they are separate code paths
(`GroupTable` vs `SpilledInnerJoinBuild`) that happen to share a workload shape.
Whether one mechanism underlies both is a question for a profile-first diagnosis of
each, not an assumption to carry in. It also reframes the 446–472 reservoir arc:
that work made high-cardinality grouping *survive* (spill, not OOM), not run *fast* —
consistent with m05 still trailing SQLite.
