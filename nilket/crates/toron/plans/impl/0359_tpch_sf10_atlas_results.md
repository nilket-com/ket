# ADR 359 Results - TPC-H SF=10 Atlas Pilot

## Summary

ADR 359 produced an honest early red: the current gauge cannot yet run the full sf=10 atlas under the fixed 4 GB
runtime cap, because the pilot hits the cgroup before the perf rail can emit a row. The first wall is the harness data
surface, not a query operator: sf=10 currently constructs full in-memory Arrow fixtures before execution, and the
`lineitem` fixture alone is enough to OOM at the fixed cap.

This means the full `--query all` board was deliberately not launched. It would die on the first `lineitem` query and
produce less evidence than the scoped per-query probes below. The next scale cut must make sf=10 data readable/exportable
without full in-process fixture materialization before the engine substrate can be measured across all 22.

## Runtime Setup

- Built release binary first: `scripts/cargo-cgroup build -p toron-tpch --release`.
- Canonical runtime scope used the already-built `target/release/toron-tpch`.
- Fixed cap verified with user systemd: `memory.max = 4294967296`.
- DuckDB binary: `/home/me/.duckdb/cli/latest/duckdb` (`v1.5.4`).
- No `affinity` backend was used for canonical evidence.
- No engine behavior change, no cap bump.

Canonical wrapper:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryHigh=4G --property=MemoryMax=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
  target/release/toron-tpch ...
```

## Pilot Gate

All three planned pilot queries OOM-killed before any perf JSON was written.

| query | engines | result | wall clock | peak | output |
| --- | --- | --- | ---: | ---: | --- |
| q06 | toron,duckdb | cgroup `oom-kill` | 3.048s | 4 GB | no row |
| q06 | toron only | cgroup `oom-kill` | 3.071s | 4 GB | no row |
| q14 | toron,duckdb | cgroup `oom-kill` | 3.047s | 4 GB | no row |
| q21 | toron,duckdb | cgroup `oom-kill` | 3.095s | 4 GB | no row |
| q06 | duckdb only | cgroup `oom-kill` | 3.292s | 4 GB | no row |

The Toron-only q06 and DuckDB-only q06 checks isolate the cause: this is not DuckDB export after a Toron run, and it is
not a Toron executor-only wall. Both paths currently build the same sf=10 `lineitem` Arrow fixture in process before a
row can be reported.

Source grounding:

- `perf_query_spec` builds `HarnessTable` values before `run_perf_query_with_tables`.
- q01/q06 use `one_lineitem_table`, which calls `generate_strict_lineitem(scale_factor)`.
- `generate_strict_lineitem` appends full `Vec`s for all strict `lineitem` columns, then constructs one full
  `RecordBatch`.
- q14 and q21 also call `generate_strict_lineitem`; q21 builds supplier first, then lineitem.

## Scoped SF=10 Probes

After the pilot wall, the full all-query command was scoped to one-query probes. The goal was to separate the fixed-cap
runner and non-lineitem query behavior from the shared `lineitem` fixture wall.

| query | engine | status | execute_ms | peak_rss | spill_bytes | classification |
| --- | --- | --- | ---: | ---: | ---: | --- |
| q02 | toron | fail | 860.1 | 1,793,396 KiB | 0 | clean `memory_budget_exhausted`: join output materialization requested 1,778,000,000, available 1,073,741,824 |
| q02 | duckdb | pass | 59.0 | 1,094,728 KiB | 0 | DuckDB comparison reachable |
| q11 | toron | no row | n/a | 4 GB | n/a | cgroup `oom-kill` before perf row |
| q13 | toron | no row | n/a | 4 GB | n/a | cgroup `oom-kill` before perf row |
| q16 | toron | pass | 10,949.2 | 1,910,256 KiB | 124,742,528 | fixed-cap Toron pass |
| q16 | duckdb | pass | 118.0 | 1,047,288 KiB | 0 | DuckDB comparison reachable |
| q22 | toron | no row | n/a | 4 GB | n/a | cgroup `oom-kill` before perf row |

q16 live correctness also passed under the fixed cap:

```text
tpch_correctness query=q16 sf=10 mode=strict phase=compare status=pass reason=none elapsed_ms=14291.2
```

Artifacts:

- `target/tpch/perf_adr359_sf10_q02_toron/latest_raw.jsonl`
- `target/tpch/perf_adr359_sf10_q02_duckdb/latest_raw.jsonl`
- `target/tpch/perf_adr359_sf10_q16_toron/latest_raw.jsonl`
- `target/tpch/perf_adr359_sf10_q16_duckdb/latest_raw.jsonl`

## Query Family Classification

Measured pilot wall:

- q06, q14, q21: cgroup OOM before perf telemetry, 4 GB peak.

Source-projected same first wall:

- q01, q03, q04, q05, q07, q08, q09, q10, q12, q15, q17, q18, q19, q20 all construct
  `generate_strict_lineitem(scale_factor)` directly or through a shared table-family helper.
- These were not individually rerun after the pilot because the pilot already proves the full in-memory `lineitem`
  fixture is a shared earlier wall under the fixed cap.

Measured non-lineitem controls:

- q16: pass, correctness pass, DuckDB pass.
- q02: Toron clean memory wall, DuckDB pass.
- q11/q13/q22: no-row cgroup OOM under the current harness. Because no perf row was emitted, ADR 359 does not assign a
  query-operator producer. These need stage-heartbeat or fixture-streaming instrumentation before a producer can be named
  honestly.

## Constant-Memory Verdict

The sf=1 streaming substrate cannot be evaluated across the full sf=10 board yet. The first fixed-cap sf=10 wall is the
gauge's full in-memory fixture model:

- `lineitem` at sf=10 is built as one full Arrow `RecordBatch` before load/plan/bind/execute timing starts.
- DuckDB export shares the same in-memory fixture construction, so the DuckDB comparison is also blocked for
  lineitem-family queries.
- q16 demonstrates the runner and one non-lineitem query can pass at sf=10 under 4 GB.
- q02 demonstrates the rail can still emit named sf=10 walls when execution is reached.

So ADR 359's honest result is: before measuring Toron's executor constant-memory claim at sf=10, the TPC-H gauge needs a
streaming or persisted data surface. The likely next ADR is a data-surface cut, not an executor optimization: materialize
TPC-H sf=10 data out-of-scope once, then scan/export it without constructing full in-process `RecordBatch` fixtures under
the query runtime cap.

## Acceptance Notes

- Pilot-first: complete, and it blocked the full board.
- Fixed 4 GB: complete, verified by cgroup `memory.max`.
- Live-only: complete. No `matrix` or `correctness-matrix` evidence used.
- DuckDB: included where the data surface was reachable (`q02`, `q16`), and shown blocked by the same lineitem fixture
  wall for q06.
- All 22 live board: not complete because the pilot exposed a prior, shared gauge wall. This is recorded as the
  deliverable rather than hidden by a cap bump or cached matrix.
- No fixes in 359: upheld.

## Forward

1. Add a persisted or streaming TPC-H data surface for the gauge so sf=10 tables are not constructed as full in-memory
   `RecordBatch` fixtures under the query runtime cap.
2. Add stage-heartbeat or row-on-signal telemetry for pre-row cgroup kills so no-row OOMs can be classified as fixture
   generation, load, execute, digest, or export.
3. Re-run ADR 359's full board under the same 4 GB fixed cap once the data surface is measurable.
4. Keep the q02 sf=10 Toron wall as a follow-up executor wall: join output materialization requested 1.778 GB against
   the 1.073 GB output cap, while DuckDB q02 passed in 59 ms.
