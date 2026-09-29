# ADR 605 - scan work and runtime-filter placement on q05 and q09

**Status:** PROPOSED. Successor to ADR 604, whose census found DuckDB's
q05 and q09 scans carrying dynamic join filters and returning far fewer
rows (2.7 M and 21.7 M) than Toron's scans take in (109.3 M and 88.1 M
raw). It compared DuckDB's scan *output* with Toron's raw scan *input*,
two different measures, so it established neither that DuckDB visits
fewer rows nor that Toron fails to prune a comparable scan. The two
cells are 10.2 s of the sealed anchor's 47.8 s gap (ADR 603: Toron
63.430 s, DuckDB 15.650 s). This record measures scan work on both sides with the closest
visited-row counters each engine has, as an operational comparison whose
accounting is not proven identical, and locates where Toron's runtime
filters apply at the anchor's coordinate. It proposes no cut.


## Context

**What each engine can measure per scan** (DuckDB probed on bigfoot
with the staged 1.5.4; Toron read from `toron-tpch/src/lib.rs`):

| measure | Toron | DuckDB 1.5.4 |
|---|---|---|
| rows visited by the scan | parquet scan ledger `raw_rows` per batch, per scan (`ParquetScanBatchLedger`), with the scan's `table_name` | `OPERATOR_ROWS_SCANNED`, per scan operator |
| rows emitted after the scan's own filters | ledger `output_rows` (after any runtime-filter mask and pushed predicate) | `OPERATOR_CARDINALITY`, per scan operator |
| filters at the scan | whether a runtime-filter descriptor and a pushed predicate were present (the ledger has `pbc_predicate_bytes`; no runtime-filter field) | `Filters` and `Dynamic Filters` in the scan's extra info |
| decoded bytes | ledger `raw_bytes` (decoded Arrow bytes, not file bytes) | not reported |
| file bytes read | not recorded | `TOTAL_BYTES_READ`, query-level only (no per-operator bytes metric exists; `OPERATOR_BYTES_READ` is rejected) |

In the probe, a scan whose dynamic filter was `optional: g IN (…)` read
all 4,000,000 rows (`operator_rows_scanned`) and returned 3,884,000
(`operator_cardinality`). A dynamic filter in the extra info shows where
a filter sits, not whether it reduced the rows visited.

**Where Toron's runtime filters can apply at this coordinate** (from a
code trace):
- The distributed runtime filters (ADRs 006, 013, 014, 537) are built by
  the fragmenter and scheduler. The anchor's coordinate binds with
  `single_partition_execution` and never fragments, so they do not run.
- The only local producer is ADR 446's resident-base bridge
  (`exec/join/streaming.rs`,
  `try_process_reoriented_inner_chain_grouped_partitioned_scan_reservoir`).
  It fires only when:
  - the grouped aggregate takes the reoriented inner-chain route (every
    join Inner and hash-partitioned);
  - the first stage builds from a partitioned scan with more than one
    partition and group;
  - the key is a single Int32 or Int64 column.
- The bridge filters the **build**-side scan (for example lineitem into
  the reservoir) with the resident probe base's keys. It is not the
  classic arrangement, in which the build side filters the probe side.
- It is applied after Parquet decode, as a row mask
  (`apply_harness_runtime_filter`). No row group or page is skipped, so
  it cannot reduce bytes read. ADR 013 put row-group skipping out of
  scope.
- It is on by default, and `TORON_ABLATE_LOCAL_RUNTIME_FILTER` turns it
  off.
- No counter records a filter produced, applied, or pruned. The evidence
  is indirect: the route recorded in node attribution, the grouped
  reservoir residency profile's terminal, and the ledger's `raw_rows`
  against `output_rows`.
- ADR 446 saw the bridge active on q05 at SF10, with reservoir input
  falling from 59,986,052 rows to 17,893,398. That run's timings were
  later withdrawn, but the row counts are not in question.
- On q09, the partsupp join has a two-column key and cannot be the
  filtered stage. Whether another stage can be is not decidable from the
  code.

**Pairing scans by table.** Toron's scan records name each scan's
table. A DuckDB scan is assigned to a table by its single file
(`Filename(s)` in its extra info, for example `…/lineitem.parquet`), when
that file's stem is a TPC-H table and its projected columns, if any,
carry the same table's prefix. Otherwise it is assigned by its projected
columns alone, when every prefix names one table (`l_` lineitem, `o_`
orders, `c_` customer, `s_` supplier, `n_` nation, `r_` region, `p_`
part, `ps_` partsupp). A scan that resolves to no table, or whose file
and columns disagree, is reported unpaired. (Amended before
implementation closure: the file key was found in DuckDB's extra info.)

**Completeness.** A missing `OPERATOR_ROWS_SCANNED` on any DuckDB scan,
or a profile with no scan operator, refuses DuckDB's table work rather
than reading zero. T3's failure is its own named exclusion. The
visited-rows comparison is labelled valid when T2 and D2 pass, and the
cell is labelled complete only when T3 passes too.


## Decision

**A diagnostic scan-work census for q05 and q09 at the sealed anchor's
coordinate: rows visited and rows emitted, per table, on both engines;
Toron's route and bridge evidence; and a Toron execution with the bridge
ablated, to see what the bridge itself changes.**

*The harness change: `scan-work-census --query <q>`.* It reuses ADR
604's binding: the sealed anchor by SHA-256, the selected coordinate
field by field, and the digest checks. Executions, each named:

- **T2: the Toron profiled execution,** a `perf-run-one` child with node
  attribution, the grouped reservoir residency profile, and two scan
  records, both diagnostic-only and both leaving the timed path's scan
  model unchanged. (Amended before implementation closure: the per-table
  and per-scan work comes from the scan progress stream,
  `TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH`, rather than a new ledger
  field.)
  - The progress stream carries `scan_start` (table, pushed predicate,
    and now whether a runtime filter was present), one `batch` per batch
    (rows visited, rows emitted, decoded bytes), and `scan_end` on the
    eager path. It is folded fail-closed: numeric batch fields, one start
    per scan, no batch or end after an end, no batch before a start. The
    lazy path emits no `scan_end`, so a scan without one is marked
    `end_not_emitted_by_lazy_path`, not refused.
  - The lazy parquet scan ledger (`TORON_TPCH_PARQUET_SCAN_LEDGER`) runs
    alongside, and its receipt is reconciled with the fold on batches,
    raw rows, and raw bytes. Both describe the batches the execution
    consumed. A disagreement withholds the cell's visited-row reading.
- **T3: the same execution with `TORON_ABLATE_LOCAL_RUNTIME_FILTER=1`.**
  Same measures. T3's digest must also match the anchor's, or the cell
  is reported and left out.
- **D2: the DuckDB profiled execution,** with `OPERATOR_ROWS_SCANNED`,
  `OPERATOR_CARDINALITY`, `OPERATOR_TIMING`, the operator type and name,
  extra info (static and dynamic filters, files read, projections),
  `LATENCY`, and `TOTAL_BYTES_READ`. Also the read-back and a result
  digest checked against the anchor's.

The census does not re-run D0, T1, or the plan binder; ADR 604's plan
evidence stands.

*The per-table table, per cell, with every measure labelled.* For each
table the cell reads, each engine's raw values are kept:
- Toron's rows visited and emitted, in T2 and in T3;
- DuckDB's rows visited and emitted in D2, with its static and dynamic
  filters, files read, and projections.

**Rows visited is compared as an operational comparison**, not a proven
identity: Toron's `raw_rows` (decoded Parquet batches before Toron's row
mask) and DuckDB's `OPERATOR_ROWS_SCANNED` are the closest visited-row
counters the two engines have, and their internal accounting is not
shown to be the same. Rows emitted is compared only as each engine's own
reduction (visited to emitted), never across engines as scan work.
Decoded bytes (Toron only) and DuckDB's query-level bytes read are
reported as they are. **File bytes read per scan is named unavailable on
both sides.**

*Pairing scans.* The unit compared across engines is the **per-table
sum**, labelled as a sum. An individual scan is paired with the other
engine's only through a stable table instance plus a predicate and
projection signature, where both sides expose one. Otherwise each scan is
shown unpaired. Scans are never paired by ordinal, and matching scan
counts on a table do not by themselves pair its scans.

*The triage flag, predeclared.* For each table pair (per-table sums),
the results report both visited-row counts and their ratio. A ratio
above 2× in either direction is a **triage flag**; a smaller difference
is reported without one. The flag names where to look, not a cause, and
it has no savings interpretation. Where DuckDB visits fewer rows, the
mechanism is not asserted. It could be file, row-group, or page pruning,
or accounting at another point. Its scan extra info, files read, and
query-level bytes are used only as corroboration, and the exact mechanism
stays unknown without a row-group or page receipt.

*Route and bridge evidence, per cell.* From T2:
- the node-attribution route (whether the reoriented inner-chain route
  was taken);
- the residency profile's terminal (whether the partitioned-scan
  reservoir was admitted, or why not);
- the reservoir's input rows.

The bridge's **ablation effect** is read from T2 against T3, and only
when the two runs agree on identity: the same plan fingerprint, digests
matching the anchor's, the same scan multiplicity per table, and the
same visited rows per table. Under that identity, a table whose emitted
rows are lower in T2 than in T3 shows an ablation effect, and equal
emitted rows read as `no_observed_effect`. Neither is a counter of the
bridge being applied or not: the bridge may have run and admitted every
row. If any identity check fails, the cell's bridge placement is
`unresolved`.

*The reading, declared before the runs.* For each of q05 and q09:
- **Visited-rows flag, DuckDB fewer:** a triage flag stands at a table
  pair with DuckDB visiting more than 2× fewer rows, by the operational
  comparison, with the mechanism unknown. It points the next record at
  reader-side pruning or accounting as a candidate: what reaches Toron's
  Parquet reader.
- **Visited-rows flag, Toron fewer:** reported as a flagged difference
  in the other direction. It does not point to a Toron reader cut.
- **Emitted-rows difference only:** no visited-rows flag stands at a
  table, and each engine's own per-table fraction of visited rows emitted
  (with both counts shown) differs, DuckDB's being smaller. The
  difference is then where each engine's filter applies after reading,
  with the ablation effect and route evidence beside it. Emitted counts
  are never compared across engines as scan work.
- **Unresolved:** the pairing, the identity checks, or the evidence do
  not separate the two.

The reading names candidates and projects no gain. The next plan record
chooses a scoped change from them.

*Procedure.* Build on bigfoot, stage and hash-verify on strix, and run
q05 then q09 in one separated launch, each cell with a fresh output
directory and a fresh DuckDB temp directory. The artifact's header
records the coordinate. No timed artifact runs in the window, and the
sealed anchor remains the campaign's measure.


## Acceptance

- `scan-work-census` fixtured:
  - the per-table fold of the ledger (lazy entries, one batch each,
    folded by table);
  - DuckDB scans paired to tables by column prefix, with an unpaired
    case;
  - the per-table sum as the compared unit, with individual scans
    unpaired unless a table instance and a predicate and projection
    signature pair them, never by ordinal;
  - the triage flag at a ratio above 2× in either direction, and no flag
    below it;
  - the ablation effect from T2 against T3 (`ablation_effect`,
    `no_observed_effect`, and `unresolved` when an identity check
    fails);
  - the anchor binding, the digest checks for T2, T3, and D2, and the
    fail-closed exclusions carried from ADR 604.
- The ledger's per-table field present only when the ledger is on; the
  timed path's rows and fields unchanged, asserted.
- An end-to-end smoke on bigfoot at SF 0.01 on q05.
- On strix, two artifacts, q05 and q09, under the separated launch; each
  cell's digests checked, and a failing cell reported and left out.
- The results record:
  - the per-table table with every measure labelled;
  - the route and bridge evidence;
  - the reading per cell;
  - the unavailable measures named;
  - the candidates for the next plan.
- The ladder green on bigfoot (Toron and toron-tpch, fmt clean) on the
  measured tree.
- Plan commit, then one implementation, results, and closure commit.


## Guardrails

- Harness only: no engine code changes. The ablation uses the existing
  environment switch.
- Rows visited is compared across engines only as per-table sums, as an
  operational comparison; rows emitted and bytes are never read as
  cross-engine scan work; a flag is never read as a cause or a saving.
- No cut is proposed or measured here.
- The sealed anchor stays the campaign's measure. Build on bigfoot, run
  on strix under the separated launch, one attempt.


## Risks

- DuckDB's `OPERATOR_ROWS_SCANNED` may count rows in row groups it
  opened rather than rows decoded. Its documentation defines it as rows
  scanned; the probe's full-table case matched the table size, and a
  pruning case is checked in the smoke.
- The column-prefix pairing is TPC-H specific. It is declared and
  fixtured, and an unresolvable scan is reported unpaired.
- The bridge's evidence is indirect. The T2 and T3 difference is an
  ablation effect, not a counter, and it depends on the ablation
  changing only the bridge; the identity checks guard that.
- File bytes read stay unavailable per scan on both engines. A
  statistics-pruning candidate would need its own instrumentation.


## Forward

- The next plan record takes the reading's candidate: a filter that
  reaches Toron's Parquet reader (row-group skipping by statistics or by
  a runtime filter), or a change to where Toron's bridge applies, scoped
  to one cell and measured under the paired rule.
- Exchange at partition parallelism 1 (q05, q18) and q01's join-free
  core path remain named candidates from ADR 604.
