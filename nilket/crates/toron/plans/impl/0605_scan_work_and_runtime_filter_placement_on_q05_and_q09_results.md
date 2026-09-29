# ADR 605 - scan work and runtime-filter placement on q05 and q09: results

Run 2026-09-29 on strix, on tree `99d2e851` (commit `d36545eb`, tag
`artifact/adr605-measured`), at the sealed ADR 603 anchor's coordinate:
one separated launch, q05 then q09. Both cells are valid and complete:
- the anchor was bound by its SHA-256, and the coordinate agreed;
- T2 and T3 passed, with digests matching the anchor's Toron digest,
  node attribution present, and their progress folds reconciled exactly
  with the parquet scan ledger's receipts;
- the plan fingerprints of T2, T3, and the anchor agree;
- D2 passed, with its digest matching the anchor's DuckDB digest, its
  `98.5 GiB` read-back, and `OPERATOR_ROWS_SCANNED` on every scan.

**The visited-row counters show no flag.** On every table the two
engines pair on, Toron visits the same rows as DuckDB (ratio 1.0) or
exactly twice as many (ratio 2.0). No ratio exceeds the predeclared 2×
threshold. DuckDB visits every row of lineitem on both cells (47,989,007
of 47,989,007): its dynamic filters reduce the rows its scans emit, not
the rows they visit, by this counter. So ADR 604's scan-output
difference is, on this evidence, **a difference in where filters apply
after the rows are read**. That is this record's reading for both cells:
emitted-rows difference only. Two observations sit beside it. **Toron
scans each build-side table twice in a row**, with identical rows and
batches: on q05, 48.1 M extra lineitem rows, 2.3 GB decoded. And **Toron
pushes a static predicate into only one scan across both cells (region
on q05)**, while DuckDB pushes q05's orders and customer predicates and
q09's part predicate. **The local runtime-filter bridge carried no filter
on any scan**: T2 and T3 differ in nothing observed, although both took
the reoriented inner-chain route. All figures come from diagnostic
executions; none is a timing or a saving.

## Identity and procedure

| identity | value |
|---|---|
| measured tree | `99d2e85137c499603b477019bfcb4d69ec26216d`, commit `d36545eb` (the harness change, its tests, and the amended plan; the closing commit differs in the plan documents alone) |
| executable | `2560c00cff645895355ad7c985606000407f3f7bc5ac960729d9084ef6e28bd4`, manifest `target/tpch/evidence/build/adr605-measured.manifest`; recipe `9af51cbc…` |
| sealed anchor | `adr603-anchor/perf_board.jsonl`, SHA-256 `d4044c40cfe517085ec5754b4de76ef647bdbaf29cd3294c325496a9249c3273` |
| launch | token `0e69033c68f0ebaa2595022d`; launched 11:04:43 CDT, the unit 120 s later, completion 11:07:32 (`result=success exit_code=exited exit_status=0`); load 0.00 and 38 °C before, 0.48 and 44 °C after; spill scratch empty before and after |
| build host / run host | bigfoot / strix |

Executions per cell:
- **T2:** a Toron `perf-run-one` child with node attribution, the
  reservoir residency profile, the scan progress stream, and the lazy
  scan ledger;
- **T3:** the same with `TORON_ABLATE_LOCAL_RUNTIME_FILTER=1`;
- **D2:** DuckDB with `OPERATOR_ROWS_SCANNED`, `OPERATOR_CARDINALITY`,
  operator metrics, `TOTAL_BYTES_READ`, and the read-back.

## The measures, as labelled

- **Rows visited:** Toron `raw_rows` (decoded Parquet batches before
  Toron's row mask) against DuckDB `OPERATOR_ROWS_SCANNED`. These are
  the closest visited-row counters, compared operationally; their
  accounting is not proven identical.
- **Rows emitted:** Toron `output_rows` against DuckDB
  `OPERATOR_CARDINALITY`. Each is read only as that engine's own
  reduction, as a fraction of its visited rows.
- **Decoded bytes:** Toron only (Arrow bytes, not file bytes).
- **File bytes per scan:** unavailable on both engines.
- **DuckDB `TOTAL_BYTES_READ`:** query-level, 4,393,629 on q05 and
  6,425,715 on q09, as reported. DuckDB documents it as file-system bytes
  read at query level. It is not per-scan bytes and not the files' total
  size, and scan work is not inferred from it; its cache and measurement
  semantics here are unresolved.
- **Scan ends:** Toron's lazy scan path emits no `scan_end`. Every Toron
  scan is marked `end_not_emitted_by_lazy_path`, and completeness rests
  on the progress fold reconciling with the ledger's receipt (q05:
  109,338,074 rows; q09: 88,069,057, in both T2 and T3).

## Per table

Toron's figures are per-table sums over its scans (scan count shown).
DuckDB has one scan per table in both cells. "Emitted fraction" is each
engine's own emitted ÷ visited.

**q05**

| table | Toron scans | Toron visited | DuckDB visited | visited ratio (reading) | Toron emitted fraction | DuckDB emitted fraction | DuckDB scan filters | T2 vs T3 |
|---|---:|---:|---:|---|---:|---:|---|---|
| lineitem | 2 | 95,978,014 | 47,989,007 | 2.00 (unflagged) | 1.000 | 0.032 | dynamic | no_observed_effect |
| orders | 1 | 12,000,000 | 12,000,000 | 1.00 (unflagged) | 1.000 | 0.031 | static + dynamic | no_observed_effect |
| customer | 1 | 1,200,000 | 1,200,000 | 1.00 (unflagged) | 1.000 | 0.567 | static + dynamic | no_observed_effect |
| supplier | 2 | 160,000 | 80,000 | 2.00 (unflagged) | 1.000 | 1.000 | none | no_observed_effect |
| nation | 2 | 50 | 25 | 2.00 (unflagged) | 1.000 | 0.200 | dynamic | no_observed_effect |
| region | 2 | 10 | 5 | 2.00 (unflagged) | 0.200 | 0.200 | static | no_observed_effect |

**q09**

| table | Toron scans | Toron visited | DuckDB visited | visited ratio (reading) | Toron emitted fraction | DuckDB emitted fraction | DuckDB scan filters | T2 vs T3 |
|---|---:|---:|---:|---|---:|---:|---|---|
| lineitem | 1 | 47,989,007 | 47,989,007 | 1.00 (unflagged) | 1.000 | 0.065 | dynamic | no_observed_effect |
| orders | 2 | 24,000,000 | 12,000,000 | 2.00 (unflagged) | 1.000 | 1.000 | none | no_observed_effect |
| partsupp | 2 | 12,800,000 | 6,400,000 | 2.00 (unflagged) | 1.000 | 1.000 | dynamic | no_observed_effect |
| part | 2 | 3,200,000 | 1,600,000 | 2.00 (unflagged) | 1.000 | 0.054 | static | no_observed_effect |
| supplier | 1 | 80,000 | 80,000 | 1.00 (unflagged) | 1.000 | 1.000 | dynamic | no_observed_effect |
| nation | 2 | 50 | 25 | 2.00 (unflagged) | 1.000 | 1.000 | none | no_observed_effect |

**Toron's scans.** On both cells Toron scans each build-side table twice
in immediate succession (scan ids 1–2, 3–4, 5–6, 7–8), with identical
rows, batches, and decoded bytes. The chain's base table is scanned once
(orders on q05, lineitem on q09). No Toron scan carried a runtime filter.
Only region on q05 carried a pushed predicate. The paired shape matches
the partition sizing pass ADR 601 describes ahead of the reoriented
chain's build write, but this record does not verify that the first scan
of each pair is that pass. On q05 the second lineitem scan is 47,989,007
rows and 2,306 MB decoded, and the doubled tables add 48,069,037 rows in all; on q09 they add
20,000,025 rows.

## Route and bridge evidence

Both cells took the `reoriented_inner_chain_grouped` route in T2 and T3.
The reservoir residency profile was not written in either run, and is
recorded as absent. No scan carried a runtime filter in T2, and T2's and
T3's emitted rows are equal on every table: `no_observed_effect`
everywhere, under full identity (plan fingerprints, digests, per-table
scan multiplicity, and visited rows all agree). The bridge was not attached to any observed scan (no scan request
carried `runtime_filter_present`), and ablation had no observed effect.
Whether the bridge created a filter that was not attached, and why it was
not attached, is unresolved: there is no counter, and the residency
profile was absent.

## The reading

| cell | anchor gap | reading | beside it |
|---|---:|---|---|
| q05 | 4,689 ms | **emitted-rows difference only**: no visited-rows flag; DuckDB emits 3.2% of lineitem, 3.1% of orders, and 56.7% of customer; Toron emits 100% of each | Toron scans lineitem, supplier, nation, and region twice; Toron pushes no predicate into orders or customer, while DuckDB does; no bridge effect observed |
| q09 | 5,479 ms | **emitted-rows difference only**: no visited-rows flag; DuckDB emits 6.5% of lineitem and 5.4% of part; Toron emits 100% | Toron scans part, partsupp, orders, and nation twice; Toron pushes no predicate into part, while DuckDB does; no bridge effect observed |

No visited-rows flag stands, so neither cell points at reader-side
pruning. The candidates for the next plan are:
- **filter placement at Toron's scans:** q05's orders date range and
  q09's part predicate are pushed into DuckDB's scans and not into
  Toron's, and DuckDB also applies join-derived filters at its scans;
- **the doubled build-side scans:** 48.1 M extra rows on q05 and
  20.0 M on q09.

Neither is a measurement of recoverable time, and no gain is projected.

## Implementation and verification

Harness only; no engine code.
- `toron-tpch/src/lib.rs`: `scan_start` progress events gain
  `runtime_filter_present`. The storage scan is otherwise unchanged; a
  lazy-path `scan_end` was drafted, then reverted to leave the scan
  untouched.
- `census.rs`:
  - `DUCKDB_SCAN_WORK_PROFILE_SETTINGS`, and the optional
    `operator_rows_scanned` and `total_bytes_read` fields;
  - `duckdb_scan_table`: the file stem if it is a TPC-H table that
    agrees with the projected prefix, else the prefix alone, else
    unpaired;
  - `fold_scan_progress`, fail-closed: numeric batch fields, one start,
    at most one end, no batch after an end or before a start, and lazy
    scans marked;
  - `reconcile_progress_with_receipt`;
  - `toron_table_work` and `duckdb_table_work`, the latter refused
    without scans or without the counter on every scan;
  - `visited_reading` and `visited_comparison` (the reading and ratio
    both withheld unless the comparison is valid);
  - `ablation_readings`, under identity.
- `main.rs`: `census_prelude`, factored out of `structural-census`; and
  `scan-work-census` (T2, T3, and D2, named exclusions, a valid scan
  comparison, and a complete cell).

Review rounds:
- The plan was amended before implementation closure, to use the
  progress stream with ledger reconciliation and the file-stem table
  identity.
- The implementation review made the progress fold and DuckDB's table
  work fail closed, added the T3 exclusion, and withheld the ratio with
  the reading.
- Codex corrected its own requirement that every scan end, once the lazy
  path was found to emit no end.

Fixtures: 6 new in `census.rs`, plus the malformed-progress, conflicting
file identity, and missing-counter cases added to them. Ladder on
bigfoot: toron-tpch library 270 (+4 ignored), binary 133, `toron-bench`
9, integration 261 (+2 ignored), no warnings, fmt clean. The Toron crate
is unchanged. There was an end-to-end smoke at SF 0.01 on q05 against a
Toron-only mini-anchor.

Evidence on strix under `~/work/ket/nilket/target/tpch/`:
- `adr605-census/q05/scan_work_census.json` `47e11a2e2304…` and
  `adr605-census/q09/scan_work_census.json` `56cd9032360d…`, each with
  T2's and T3's progress streams and node attribution and D2's profile;
- `adr605-strix-host-record/`: `launch.sh` `e966f696f8b0…`,
  `snapshot.sh` `2ec05fc93770…`, and `census/` with the before- and
  after-snapshots, the driver logs, and the completion marker
  `83f2e2e717b8…`.

## Closure

ADR 605 closes with both cells valid and complete. By the closest
visited-row counters, both engines report the full table row count on
the tables Toron scans once, and Toron's own counters show it reading
its build-side tables twice. DuckDB's
large scan-output reduction on q05 and q09 comes from filters it applies
within its scans after reading, static and join-derived, which Toron's
scans do not apply. Toron's local runtime-filter bridge was not attached
to any observed scan, and its ablation had no observed effect. The next
plan record chooses between the two named candidates, after a check of
the code and of which cells own each:
- filter placement at Toron's scans: a static predicate that reaches
  the scan, or a join-derived filter placed where DuckDB places it;
- the doubled build-side scans.

This record establishes recoverable time for neither. A paired cut
follows a reviewed one-token plan once the chosen mechanism is concrete.
Exchange at
partition parallelism 1 and q01's join-free core path remain named
candidates from ADR 604.
