# ADR 606 - q05's reoriented chain: the cap check's batches reused for the build: results

Run 2026-09-29 on strix, at the sealed ADR 603 anchor's coordinate: one
paired attempt on q05 (ADR 597's driver, ADR 595's separated launch),
then the mechanism check. **Retained.** The treatment's median is
4,735.0 ms against control's 5,170.6 ms: a median paired gain of
**444.7 ms (8.6%)**, against a required 249.6 ms (`2 × range_control`,
range 124.8 ms), with every pair strictly positive (430.2 to 539.7 ms).
Every row's digest equals the anchor's q05 Toron digest. The token
stays `Reuse`.

The mechanism check confirms the scan-count mechanism the plan named;
the 444.7 ms is the paired token's result, not attributed from it. With
the treatment executable, q05 scans lineitem, supplier, nation, and region
**once each instead of twice**. Lineitem's rows visited fall from
95,978,014 to 47,989,007, equal to DuckDB's `OPERATOR_ROWS_SCANNED`. The
digest, the plan fingerprint, and the route are unchanged. The
comparison is against ADR 605's census on a pre-606 executable, not
this pair's control.

**An observed tradeoff:** the treatment's peak RSS is 1.14 GiB (15%)
above control's on every measured row (9,167,336–9,167,888 KiB against
7,968,656–7,969,248 KiB). The plan predicted a difference could appear,
since scan 1's batches are held through the repartition. The pair alone
does not establish that as the cause, and no rule applies to it. The
plan asserts no equality.

This is a q05 cell measurement, not a board. The q05 Toron median it
starts from is 2.2% below the anchor's (5,170.6 against 5,285.7 ms).
No new 22-cell total is measured or projected.


## Identity and procedure

| identity | value |
|---|---|
| control | commit `1e187602`, tree `2e0c2aab64edc826a251c24858c1b094c023ea8a`, tag `artifact/adr606-control`; executable `bd72eec2c373434aefdd91b4f945dfd92f390c11f43fac73643b28748c3b77a7` |
| treatment | commit `85ff3958`, tree `64a8100b6c2acd4e4a3baad5db2741459f0ec28f`, tag `artifact/adr606-treatment`; executable `889fa4891020c53a59bd5a2bcb26273caf3d5f180a49818aecaa8f0248e8a2d8` |
| difference | one line in `exec/join/streaming.rs`: `WRAPPED_CAP_CHECK_BATCHES` `Discard` → `Reuse` |
| recipe | `9af51cbc…`, both arms built on bigfoot, staged on strix, `sha256sum -c` passed |
| sealed anchor | `target/tpch/adr603-anchor/perf_board.jsonl.evidence.sealed`; artifact SHA-256 `d4044c40cfe517085ec5754b4de76ef647bdbaf29cd3294c325496a9249c3273`; bound cell q05 Toron, median 5,285.7 ms |
| coordinate | SF8 strict parquet, task memory and spill reserve 2 GiB each (explicit), partition parallelism 1, affinity `0-15`, dataset `fnv1a64:0fc5c06d19c5ed23` |
| ladders (bigfoot, both arms) | toron 5,492 passed, 0 failed, 3 ignored; toron-tpch 674 passed, 0 failed, 6 ignored |
| build host / run host | bigfoot / strix |

The arm commits were rebuilt once before the pair, with the driver
extension below folded into control. The earlier arm commits (`0976759c`,
`3675a40f`; executables `56bb73ca…`, `c96a8233…`) were used only for the
first receipt gate and not measured. The tags now point at the rebuilt
commits.

### The driver extension

ADR 597's driver was q21-specific: the child's `--query`, the sealed
cell it bound, and the query its validator required. As the plan
allowed, it now takes `--query`, defaulting to q21:
- `adr592_load_sealed_baseline` and `adr592_bind_sealed_baseline` bind
  the named query's Toron cell. The baseline gains a `query` field in
  its JSON. Its `q21_toron_execute_ms_median` and `q21_toron_digest`
  fields keep their ADR 592 names and hold the named query's cell.
- `Adr592ChildExpectation` gains `query`, and the child validator
  requires `row.query` to equal it.
- `run_adr597_child` passes the query to the child.
- A non-q21 query without `--sealed-manifest` is refused, since the
  pre-592 fallback digest is q21's.

Pairing, gate, and rule are unchanged. For q21 the execution and
validation coordinate is unchanged, and the receipt format gains the
`query` field. One test was added:
`adr606_the_named_query_binds_its_own_cell_and_refuses_other_rows`.
The driver's `kind` string still reads
`adr597_q21_exchange_skip_paired_cut_separated_launch`. On this run
`sealed_baseline.query` is `q05`.

## The pre-pair receipt gate

Run on strix against the rebuilt executables, one q05 execution per arm,
with `TORON_REORIENTED_CAP_CHECK_RECEIPT_PATH` set
(`adr606-strix-host-record/receipt-gate-v2`). Both runs passed, with
digest and plan fingerprint equal to the anchor's. Each arm wrote four
receipts, identical except for form and `reused`:

| replay | build child | decision | control path | partitions | reused (Discard / Reuse) | equivalence |
|---|---|---|---|---|---|---|
| 1 | Scan lineitem (TableId 1) | materialized_batches | bound_exchange | 4 | false / true | passed |
| 2 | Scan supplier (TableId 5) | materialized_batches | bound_exchange | 4 | false / true | passed |
| 3 | Scan nation (TableId 4) | materialized_batches | bound_exchange | 4 | false / true | passed |
| 4 | Filter (FullyPushed) region (TableId 6) | materialized_batches | bound_exchange | 4 | false / true | passed |

Every duplicated stage is a `MaterializedBatches` decision on the
`bound_exchange` path, so the pair's delta is attributable to these four
stages. The receipt runs' timings include the equivalence replays and
are not measurements.

## The pair

Token `adr606-a666cf47f065`, launched 12:26:39 CDT. The unit started at
12:28:39 and completion arrived 12:30:19
(`result=success exit_code=exited exit_status=0`). The separated launch
was verified: first child 128.1 s after the launcher's logout, and no
session events in the measurement window. The start gate admitted at
load 0.04 and 35 °C (bounds 0.3 and 45 °C) after a 5.0 s settle.
Control variability ratio 0.024, within the 0.04 bound.

| pair | order | control ms | treatment ms | gain ms | gain |
|---|---|---|---|---|---|
| 0 | C, T | 5,149.8 | 4,718.5 | 431.3 | 8.4% |
| 1 | T, C | 5,167.5 | 4,737.3 | 430.2 | 8.3% |
| 2 | C, T | 5,170.6 | 4,719.7 | 450.9 | 8.7% |
| 3 | T, C | 5,197.0 | 4,752.2 | 444.7 | 8.6% |
| 4 | C, T | 5,274.6 | 4,735.0 | 539.7 | 10.2% |
| **median** | | **5,170.6** | **4,735.0** | **444.7** | **8.6%** |

Warmups: control 5,355.4 ms, treatment 4,844.1 ms. Treatment range
33.7 ms. Every row passed ADR 592's validator: its arm's executable, the
anchor's q05 digest, and the coordinate.

**Host record.** The pair's `snapshot.sh` was copied from ADR 601 and
still named ADR 601's paths. Its `join_spill_scratch_entries` and
`output_directory_exists_before_run` lines describe `adr601-join-spill`
and `adr601-mechanism-check`, not this run, and are **unusable**. They
are left as recorded. The driver's own checks cover the actual paths:
- `run_lifecycle_decomposition_driver` refuses an existing output
  directory before creating it;
- `run_lifecycle_decomposition_driver_in` checks `TORON_JOIN_SPILL_DIR`
  (`adr606-join-spill`) is empty before the run and again after the
  children.

The run completed through both checks, and the scratch was empty
afterwards (0 entries). The census launch used a corrected snapshot
(`snapshot-census.sh`).

## The mechanism check

ADR 605's `scan-work-census` on q05 against the treatment executable
(token `adr606m-e3f2b280c6a1`, launched 12:31:08 CDT, completion 12:33:29,
success; load 0.31 and 43 °C before; scratch empty, output fresh). The
cell is complete, with no exclusions:
- the anchor was bound by its SHA-256;
- T2 and T3 passed, with digests equal to the anchor's;
- the progress folds reconciled with the scan ledger (`scan_work_valid`);
- plan fingerprint `762bb764b20a1c60` agrees across T2, T3, and the
  anchor;
- D2 passed.

The comparison column is ADR 605's census of the same cell (executable
`2560c00c…`, the pre-606 tree), not this record's control executable.

| table | Toron scans, 605 → 606 | rows visited, 605 → 606 | decoded bytes, 605 → 606 | DuckDB rows visited |
|---|---|---|---|---|
| lineitem | 2 → **1** | 95,978,014 → 47,989,007 | 4,611,697,856 → 2,305,848,928 | 47,989,007 |
| supplier | 2 → **1** | 160,000 → 80,000 | 2,625,280 → 1,312,640 | 80,000 |
| nation | 2 → **1** | 50 → 25 | 2,144 → 1,072 | 25 |
| region | 2 → **1** | 10 → 5 | 688 → 344 | 5 |
| orders | 1 → 1 | 12,000,000 → 12,000,000 | unchanged | 12,000,000 |
| customer | 1 → 1 | 1,200,000 → 1,200,000 | unchanged | 1,200,000 |

The parquet scan receipt for T2 goes from 13,354 batch scans to 7,483,
raw bytes from 4,874,069,296 to 2,566,906,312, rows from 109,338,074 to
61,269,037, and decode time from 1,056,048 µs to 637,627 µs.
- The route is `reoriented_inner_chain_grouped` on both.
- Region's scan is still the one with a pushed predicate.
- The runtime-filter bridge still carries no filter (`ablation`
  `no_observed_effect` on every table), as in ADR 605.
- The decisions per stage are those in the receipt gate above.

As the plan stated, the reused stages' `execute_bound_node` attribution
no longer fires under `Reuse`, and that time moves into the chain's prep
span. No rule reads it.

On visited rows Toron now matches DuckDB on every q05 table. What
remains is where the filters apply: DuckDB emits 3.2% of lineitem's rows
from its scan, and Toron emits them all. That is ADR 605's
emitted-rows difference, and this record does not touch it.

## Consequence

- The token stays `Reuse` in the closing commit, which is the treatment
  tree with this results record.
- The retention covers q05 at this coordinate. The same wrapped-arm path
  applies wherever the reoriented chain's cap check runs under an
  `Exchange`. Other cells were not measured, and q09 is the next
  candidate ADR 605 names.
- Peak RSS rose 1.14 GiB on q05. At the 2 GiB task budget the result is
  unchanged, and neither arm spilled on any row (`spill_bytes` 0, no
  join-spill observations). A later record that widens the reuse
  should report peak RSS again.
