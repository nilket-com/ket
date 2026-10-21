# ADR 604 - a structural census of the largest gap cells, Toron beside DuckDB: results

Run 2026-09-29 on strix, on tree `dcd78caf` (commit `c8f6fc40`, tag
`artifact/adr604-measured`), at the sealed ADR 603 anchor's coordinate:
one separated launch, five cells in sequence (q21, q09, q05, q18, q01),
one artifact each. Every cell passed every gate:
- the anchor was bound by its sealed SHA-256, and the selected
  coordinate agreed field by field;
- T2 passed with its node attribution (outer reconciliation within
  boundary) and its scan receipt;
- T2's digest matched the anchor's Toron digest, and D1's matched the
  anchor's DuckDB digest;
- D1 read back the anchor's default limit, `98.5 GiB`;
- Toron's plan correspondence is `indirect_agreement` (binder = T2 =
  anchor fingerprint), and DuckDB's is `shape_name_match`.

So all five enter the reading. **The census cannot compare scan work
across the two engines, and says so.** Its two volume measures are
different quantities. Toron's is the parquet scan ledger's raw input,
rows read before any filtering. DuckDB's is each scan operator's
`OPERATOR_CARDINALITY`, the rows it returns to its parent after
pushed-down and dynamic filters. The census did not request DuckDB's
`OPERATOR_ROWS_SCANNED` or its bytes read. What it does establish: on
q05 and q09, DuckDB's scans carry dynamic join filters (the D1 operator
tree's extra info) and return far fewer rows than Toron's scans take in
(2.7 M returned against 109.3 M read on q05, and 21.7 M against 88.1 M on
q09). Whether DuckDB physically read fewer rows, and whether Toron fails
to prune a comparable scan, is not established. So q05 and q09, 10.2 s
of the anchor's gap together, are **filter-placement and scan-output
candidates**, and the next record measures scan work directly. q01, with
no join or exchange in its plan and a sealed 4.87× wall gap, is a
**join-free core-path throughput candidate**, with its scan work and
kernel attribution unresolved here. Toron's node attribution attributes
57–100% of its time to `aggregate` in every cell, with zero join calls
in four of the five. Toron runs its scans, filters, and joins inside the
aggregation chain, so kind-level attribution cannot separate them. That
limit is reported, not worked around. All times below come from profiled
diagnostic executions: they carry profiling overhead and are not
compared with the board's medians.

## Identity and procedure

| identity | value |
|---|---|
| measured tree | `dcd78cafa9b7f5cbd8ae3fa8d4c1baad0befb5dc`, commit `c8f6fc40` (the harness change, its tests, and the amended plan; the closing commit differs in the plan documents alone) |
| executable | `381d09a750fbca7869b11348ab81792fc0d3177c1f9418f59252fa97638ef021`, manifest `target/tpch/evidence/build/adr604-measured.manifest`; recipe `9af51cbc…`, rustc 1.95.0 |
| sealed anchor | `adr603-anchor/perf_board.jsonl`, SHA-256 `d4044c40cfe517085ec5754b4de76ef647bdbaf29cd3294c325496a9249c3273`, verified by the command before use |
| DuckDB | `c478c0f4…`, v1.5.4, one thread, default limit read back as `98.5 GiB` |
| launch | token `e6ba7ba4f4e1ff49df32fe10`; launched 10:21:37 CDT, the unit 120 s later, completion 10:25:37 (`result=success exit_code=exited exit_status=0`); load 0.00 and 43 °C before, 0.85 and 42 °C after, spill scratch empty before and after |
| build host / run host | bigfoot / strix |

Executions per cell:

| name | what | executes |
|---|---|---|
| plan | the explain binder (`bind_with_explain_summary`) under the harness's planner configuration | no |
| T1 | Toron `EXPLAIN ANALYZE`, local `QueryCoordinator`; result discarded | yes |
| T2 | Toron `perf-run-one` child with node attribution and the scan ledger | yes |
| D0 | DuckDB `EXPLAIN (FORMAT JSON)` | no |
| D1 | DuckDB profiled with operator metrics, a fresh temp directory, and the read-back | yes |

## Identity per cell

| cell | T2 vs anchor Toron digest | D1 vs anchor DuckDB digest | output rows (Toron / DuckDB) | Toron correspondence | DuckDB correspondence |
|---|---|---|---|---|---|
| q21 | match | match | 100 / 100 | indirect_agreement | shape_name_match |
| q09 | match | match | 175 / 175 | indirect_agreement | shape_name_match |
| q05 | match | match | 5 / 5 | indirect_agreement | shape_name_match |
| q18 | match | match | 100 / 100 | indirect_agreement | shape_name_match |
| q01 | match | match | 4 / 4 | indirect_agreement | shape_name_match |

Cross-engine result equivalence is not proved here. Equal row counts are
reported, and are not presented as equal results.

## (a) Join structure

Toron's per-join table comes from the binder's join edges. **Every
binder estimate is `estimate_unknown`**, since the edges carry no rows at
SF8 either. T1 emitted analyze rows for q09 only (one row, which no
stable identity ties to a join, so q09's joins read `unassigned`). On
q21, q05, and q18 every join is `not_emitted`, and q01 has no joins.
Toron's per-join rows are therefore unknown in this record.

| cell | Toron joins (binder edges, probe → dim) | DuckDB joins (D1, rows out) |
|---|---|---|
| q21 | supplier→l1, l1→orders, supplier→nation, l1→l2, l1→l3 | l_suppkey=s_suppkey 588,216; l_orderkey=o_orderkey 14,631,301; s_nationkey=n_nationkey 3,205; RIGHT_SEMI 567,039; l1/l2 inner 2,350,445; RIGHT_ANTI 31,809; l1/l3 inner 1,485,115 |
| q09 | supplier→lineitem, part→lineitem, lineitem→partsupp, lineitem→orders, supplier→nation | (p,ps)=(ps,…) 2,610,723; l_orderkey=o_orderkey 2,610,723; l_partkey=p_partkey 2,610,723; ps_suppkey=s_suppkey 6,400,000; s_nationkey=n_nationkey 80,000 |
| q05 | customer→orders, orders→lineitem, (composite)→supplier, supplier→nation, nation→region | (c_nationkey,l_suppkey)=(s_…) 58,990; l_orderkey=o_orderkey 1,462,007; o_custkey=c_custkey 365,387; c_nationkey=n_nationkey 240,661; n_regionkey=r_regionkey 5 |
| q18 | orders→(grouped IN), customer→orders, orders→lineitem | l_orderkey=o_orderkey 3,507; o_custkey=c_custkey 501; SEMI o_orderkey 501 |
| q01 | none | none |

With Toron's per-join rows unknown, no join boundary here is
comparable in rows, and no 2× flag is raised from joins.

## (b) Materialization and volume

Two different measures, labelled as such:
- **Toron raw scan input:** the T2 parquet scan ledger's rows, read
  before any filtering.
- **DuckDB scan output:** the sum of D1's scan operators'
  `OPERATOR_CARDINALITY`, the rows they returned to their parents after
  pushed-down and dynamic filters. It is not DuckDB's scan work;
  `OPERATOR_ROWS_SCANNED` and bytes read were not requested.

Their quotient is therefore not a cross-engine scan-work ratio, and none
is computed.

| cell | Toron raw scan input | DuckDB scan output | DuckDB scans carrying dynamic filters | Toron spill |
|---|---:|---:|---|---:|
| q21 | 180,047,096 | 149,732,437 | 6 of 6 | 27,242,808 B |
| q09 | 88,069,057 | 21,691,880 | 4 of 6 | 0 |
| q05 | 109,338,074 | 2,689,017 | 5 of 6 | 0 |
| q18 | 158,367,021 | 109,178,013 | 1 of 4 | 0 |
| q01 | 47,989,007 | 47,313,651 | 1 of 1 | 0 |

On q05, DuckDB's largest scan returns 1,557,352 rows and on q09
3,124,730, from a lineitem of 47,989,007 rows. The D1 operator tree marks
those scans with dynamic filters. This shows where DuckDB's filters sit,
not how much it read. T2 records only the aggregate scan receipt
(scans, batches, rows, decode time, bytes), not a per-table ledger, so
per-table Toron scan work is left to the next record. Toron has had runtime-filter code since ADR 006,
including application at the storage scan boundary (ADR 013). Whether
and where it applies at this coordinate is not measured here. q01's two
figures are close numerically, but they are the same two different
measures, and are not read as matching volume.

## (c) Cost by kind, each engine against its own denominator

Toron: T2's exclusive time per node kind as a share of its execute span
(the outer reconciliation's residual in µs). DuckDB: D1's
`OPERATOR_TIMING` per kind as a share of its operator sum, beside
`LATENCY` and the signed `LATENCY − sum(OPERATOR_TIMING)`. The two
columns come from different instrumentation and denominators, and are
not read as equivalent.

| cell | Toron span ms (resid µs) | Toron by kind | DuckDB latency ms (op sum, signed resid) | DuckDB by kind |
|---|---|---|---|---|
| q21 | 8,048 (947) | aggregate 100.0% | 2,218 (2,146, +72) | join 49.5%, scan 34.9%, filter 14.4%, other 1.1% |
| q09 | 7,479 (385) | aggregate 93.4%, exchange 4.6%, scan 1.8%, filter 0.2% | 1,647 (1,603, +44) | join 54.9%, scan 39.9%, aggregate 2.8%, other 2.3% |
| q05 | 5,590 (350) | aggregate 64.9%, exchange 25.0%, scan 10.1% | 611 (588, +23) | scan 80.2%, join 19.6% |
| q18 | 6,367 (118) | aggregate 57.3%, exchange 19.4%, join 14.5%, scan 6.5%, filter 2.2% | 1,488 (1,445, +43) | aggregate 72.7%, scan 17.9%, join 7.2%, other 1.4%, filter 0.8% |
| q01 | 4,486 (109) | aggregate 100.0% | 963 (941, +22) | scan 47.0%, aggregate 46.0%, other 7.0% |

No DuckDB operator type was unmapped except `DUMMY_SCAN` on q21, which
had zero time. Within Toron's scan, the ledger's decode time is 1,013 ms
on q21, 836 on q09, 1,052 on q05, 598 on q18, and 661 on q01.

**What the Toron column can and cannot say.** Join calls are zero on
q21, q09, q05, and q01, and `aggregate` holds 57–100% of the span in
every cell. Toron executes these plans as chains whose scan, filter,
and join work runs inside the aggregation handler's exclusive span, so
node attribution by kind cannot separate them. It does separate one
thing DuckDB's one-thread tree does not have: **exchange**, at 25.0%
of Toron's span on q05, 19.4% on q18, and 4.6% on q09. The plan shapes
show hash-partitioned joins behind `Exchange(parts=4)` at partition
parallelism 1.

## (d) Divergences, and the reading

| cell | anchor gap | largest divergence the census establishes | reading |
|---|---:|---|---|
| q05 | 4,689 ms | DuckDB's scans carry dynamic join filters and return 2.7 M rows; Toron's take in 109.3 M raw. Toron spends 25.0% of its span in exchange | **filter-placement and scan-output candidate**; secondary: exchange |
| q09 | 5,479 ms | DuckDB's scans carry dynamic filters and return 21.7 M rows (lineitem 3.1 M); Toron's take in 88.1 M raw | **filter-placement and scan-output candidate** |
| q01 | 3,700 ms | no join or exchange in either plan; sealed wall gap 4.87×; scan work and kernel attribution not isolated by this profile | **join-free core-path throughput candidate** |
| q18 | 4,547 ms | Toron spends 19.4% of its span in exchange (Toron raw scan input 158.4 M; DuckDB scan output 109.2 M, a different measure) | **execution path (exchange), tentative**; secondary: mixed |
| q21 | 5,846 ms | Toron's time is one aggregation chain (100%), with a 27.2 MB spill | **mixed or unresolved** from this census; ADRs 592–602 put its time in runtime preparation and the semi and anti writes |

Summed anchor gaps by primary reading:
- filter placement and scan output (candidates): q05 + q09, 10,168 ms;
- execution path, exchange (tentative): q18, 4,547 ms, plus q05's
  secondary;
- join-free core-path throughput (candidate): q01, 3,700 ms;
- mixed or unresolved: q21, 5,846 ms.

The readings are not measurements of recoverable time and project no
gain. Every reading is a candidate for the next record's attribution.
None is established as a cause.

## Implementation and verification

Harness only; no engine code. `toron-tpch/src/census.rs` (new):
- DuckDB's operator profile parser and the declared type-to-kind mapping
  (24 types; unmapped types kept by name; the signed residual; the root
  CPU time as a cross-check);
- `boundary_reading`, with the DuckDB-zero case named apart from the 2×
  flag;
- the D0 JSON tree as `(depth, name)` and `duckdb_plan_correspondence`,
  a shape-name match;
- `toron_plan_correspondence` (binder = T2 = one anchor fingerprint);
- `toron_join_table` (binder edges, `estimate_unknown`, `not_emitted`
  and `unassigned`, nothing attached by position);
- the selected coordinate (18 fields) and `coordinate_mismatches`;
- `census_exclusions`: T2 passed, attribution and scan receipt present,
  reconciliation within boundary, both digests, and the memory
  read-back.

`lib.rs`: `structural_census_toron_plan` (the plan from the explain
binder, and T1 through a local `QueryCoordinator`). `main.rs`:
`structural-census`, which binds the anchor by SHA-256, checks the
coordinate, runs T1, T2, D0, and D1, and writes one artifact. (Review
rounds: the plan was amended before implementation, twice, after a code
read showed T1 covers only star-like join orders and after Codex
rejected using the replay-input ledger for join rows; the implementation
review added the per-join table, the sealed-anchor binding, the
depth-aware DuckDB comparison, the comparability gate, the DuckDB-zero
case, and fail-closed checks for missing attribution, scan receipts, and
empty profiles.)

Fixtures: 11 new in `census.rs` (the recorded operator tree, an unmapped
type, every mapped type, malformed profiles, the triage flag and its
DuckDB-zero case, the D0 read and its renested and reordered mismatches,
the coordinate check, the non-star join list, the Toron correspondence
cases, and the exclusion reasons including the missing receipt). Plus an
end-to-end smoke on bigfoot at SF 0.01 (q05, against a Toron-only
mini-anchor), which exercised the refusal of a wrong anchor hash and the
comparability exclusions. Ladder on bigfoot: toron-tpch library 264 (+4
ignored), binary 133, `toron-bench` 9, integration 261 (+2 ignored), no
warnings, fmt clean. The Toron crate is unchanged.

Evidence on strix under `~/work/ket/nilket/target/tpch/`:
`adr604-census/<cell>/` (`structural_census.json` for q21
`bc01fd887724…`, q09 `208d32e7f0e2…`, q05 `fd53ee96bb5e…`, q18
`b2b4672e5c18…`, and q01 `5a40c89fb3cc…`, each beside its
`t2_node_attribution.json` and `d1_profile.json`) and
`adr604-strix-host-record/` (`launch.sh` `7700cfde93ea…`, `snapshot.sh`
`005faef3f3ac…`, and `census/` with the before- and after-snapshots,
the driver logs, and the completion marker `27d127191dbe…`).

## Closure

ADR 604 closes with all five cells passing the coordinate, identity, and
receipt checks and read, and with the
census's limit stated. It placed DuckDB's dynamic join filters in the
q05 and q09 scans, and it showed those scans returning far fewer rows
than Toron's take in. It did not measure DuckDB's scan work, so it does
not establish that DuckDB reads less or that Toron fails to prune a
comparable scan. The five-cell run is not repeated (one attempt). The
next plan record measures scan work on both sides: DuckDB's
`OPERATOR_ROWS_SCANNED` and bytes read per scan, and Toron's raw rows per
table. It then locates where Toron's existing runtime filters (ADRs 006,
013, 014, 446, 537) apply at the anchor's coordinate. No paired cut on
q05 is authorized until that attribution is known. Exchange at partition
parallelism 1 (q05, q18) and q01's join-free core path are named as
candidates after it. q21's reading stays with its own records.
