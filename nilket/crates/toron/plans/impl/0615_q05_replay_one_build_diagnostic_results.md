# ADR 615 - q05's replay-1 build: diagnostic results

Run 2026-09-30 on strix, at ADR 613's coordinate, with the instrumented
executable built from `da72e637`. It is a diagnostic: there is no token,
no pair, no cut, and no saving is projected.

**Where q05's replay-1 build spends its time.** Replay 1's `prep_build`
takes a median of **3,681.7 ms** (range 3,667.8–3,689.9) over five
timed runs. It divides into four sub-stages and a residual, with per-run
identity:

| sub-stage | what it is | median ms | range |
|---|---|---|---|
| **`prep_exchange`** | repartitioning the materialized build batches into 4 partitions (`exchange_input_batches`, the Reuse path) | **1,645.5** | 1,643.1–1,659.7 |
| `prep_scan_decision` | the cap check: scanning, decoding, and materializing `lineitem` | 927.0 | 924.7–933.8 |
| `prep_table_build` | `build_in_memory_join_rows`: the join table | 776.6 | 775.0–796.6 |
| `prep_input_materialize` | `concat_or_empty` and `key_arrays` | 241.5 | 241.0–241.8 |
| residual | the rest of `prep_build` | 77.9 | 77.8–79.1 |

The build input is **all of `lineitem`**:
- 47,989,007 rows, with raw and emitted scan bytes both 2,305,848,928;
- no filter at the scan (it emits every row);
- exchanged into 4 partitions.

The table built from it:
- key plan `int64`;
- 47,989,007 build rows under **12,000,000 unique keys**;
- 10,284,969 duplicate keys, and 35,989,007 overflow rows;
- at most 7 rows per key;
- 480,000,000 bytes of row lists.

Replays 2 to 4 (`supplier`, `nation`, and `region` filtered) take 7.0,
0.1, and 0.1 ms in all.

**The largest sub-stage is the exchange** (1,645.5 ms, 45% of the span).
That is the repartitioning of 48 M rows into four partitions, at a
coordinate whose partition-work parallelism is 1. Next come the scan
(927 ms) and the table build (777 ms). This is observed; no mechanism
beyond what the spans show is claimed, and no saving is projected.


## Identity

| item | value |
|---|---|
| instrumented tree | `4095d86e8d322bfac29d0a9690f8733de538915d`, commit `da72e637` (branch `adr615-instrument`); code equal to main plus the instrument |
| executable | `fd7c99c434bdd74f83692c0b50068e94c3a5a80e0c2837714b508b1ce0ecb5e8`, manifest `adr615-instrumented.manifest` (`acd5dd72…`), staged and hash-verified |
| ladder (bigfoot, committed tree, `--no-fail-fast`) | toron 5,515 passed (2 new fixtures), 0 failed, 3 ignored; toron-tpch 674 passed, 0 failed, 6 ignored; fmt clean; release build warning-free |
| comparison executable (sweep) | ADR 613's, `9a84d5c4…` |
| scripts | `adr615_run.sh` (SHA-256 `ad386efc…`) and `adr615_read.py` (`6e2234f4…`), with the read-out in `readout.txt` (`625bb131…`), under `target/tpch/adr615-diagnostic/` |
| host | load 0.00 before the runs; the spill scratch was empty after every run |

**Read-out provenance.** A first capture, now `readout-v0.txt`
(`856be980…`), is a Python traceback, not the read-out. It was run from
the diagnostic directory, where the script's relative paths do not
resolve, and it was cited in error in a first draft of this record.
The read-out cited above was produced by running the script from the
repository's `nilket` directory. The script was strengthened in review
(the prior version is `adr615_read-v0.py`, `1154dad1…`):
- the sweep's overall result also requires every added `prep_build`
  refinement to have passed;
- each timed run must show all four expected sub-stages on every
  replay.

The measurements were not re-run. The same saved artifacts were read.

## The instrument

- **Node attribution.** A replay-scoped refinement now finds its parent
  by name and replay at any level, and the match must be unique. An
  ambiguous parent fails closed (`parent_stage_ambiguous`). Existing
  two-level refinements are unchanged. Two fixtures cover it:
  - a third level reconciles to its refinement parent;
  - an ambiguous parent fails closed.
- **The timers.** A `PrepAttribution`, holding the handle and the
  telemetry, is created only in the reoriented chain's function, from
  its stage handle. It is `None` when attribution is off, so no timer
  code runs. It is threaded to four points, and every other caller
  passes `None`:
  - `prep_scan_decision`, around the decision call inside the wrapped
    helper;
  - `prep_exchange`, around its reuse branch;
  - `prep_input_materialize`, around `concat_or_empty` and
    `key_arrays`;
  - `prep_table_build`, around exactly the `build_micros` interval.
- **The plan's fixture amendment** (reviewed before the build; it rides
  in this commit's plan). The armed-chain unit fixtures were replaced by
  two things:
  - the five armed q05 runs;
  - a 22-cell armed compatibility sweep.

  In-process tests cannot arm attribution, and no existing chain
  fixture is known to reach the wrapped Reuse-and-Exchange path.

## Preflight (untimed)

One q05 run carried the cap-check receipt, the scan progress stream and
ledger, and the join-build profile. The receipt's equivalence check
re-executes each build, so the preflight's timing is not used. Its gate
passed.

Per stage:
- replay 1, `lineitem` (TableId 1);
- replay 2, `supplier` (5);
- replay 3, `nation` (4);
- replay 4, `region` (6), `Filter` `FullyPushed`.

Each is `materialized_batches` on `bound_exchange`, with 4 partitions,
reused, and equivalence passed.

The preflight's scan progress shows each build-side table scanned
twice: `lineitem`, `supplier`, `nation`, and `region`. `lineitem` is
scans 1 and 2, each 47,989,007 rows and 2,305,848,928 bytes. The second
scan of each is the **receipt's own equivalence check**, which
re-executes the stage's build (`execute_bound_node`). That is why the
receipt is confined to this untimed preflight. ADR 606's census, run
without the receipt, shows each build table scanned once. `customer`
and `orders` are scanned once each. The build input's facts below come
from the first scan of each pair.

**Bytes after the exchange are not measured**, and are not estimated.
The scan's emitted bytes are measured, as above.

## The compatibility sweep

One armed run of each of the 22 cells with each executable, ADR 613's
and the instrumented one. For every cell:
- both digests and plan fingerprints equal ADR 613's sealed cells;
- the invocation routes and topology are identical;
- the strict and top-level reconciliation statuses are identical;
- the failed, missing, and non-`prep_build` refinement sets are
  identical.

The only difference is the added `prep_build` refinements, all passed:
q03 (1), q05 (4), q07 (4), q08 (6), and q09 (4). No latency comparison
is made from the sweep.

## The five timed q05 runs

Each run was gated:
- the digest and plan fingerprint equal ADR 613's q05 cell;
- the cap-check receipt was unset (recorded);
- `refinements_reconciled` is true;
- all four replays have a `prep_build`;
- for every replay, the sub-stages plus the refinement residual equal
  `prep_build` exactly, with the residual non-negative.

**All five passed.** Every sub-stage was present on every replay, since
q05's four stages all take the Reuse path, so no optional child is
absent.

The runs' `execute_ms` ranged over 4,797.0–4,823.1 ms. That compares
with ADR 614's diagnostic median of 4,956.4 ms and ADR 613's board
median of 4,752.0 ms. The differences are observed, and not apportioned
between timer overhead, host drift, and anything else.

The per-replay sub-stage table is above. Each displayed value is its own
five-run median, so the displayed sub-stage medians need not sum to the
displayed 3,681.7 ms `prep_build` median. The exact identity holds per
run. **Two readings of the table
build agree:** `prep_table_build`'s median of 776.6 ms and the
join-build profile's `build_micros` median of 776.6 ms time the same
interval.

**The grouped-chain profile** (`TORON_GROUPED_CHAIN_PROFILE_DIR`) wrote
`status: unsupported_path` for this route (committed branch
`scan_probe_base`). It supplies no per-stage rows here, and none are
reported from it.

## Consequence

- **No cut is proposed.** The observed owners inside q05's replay-1
  build are:
  1. the exchange of the 48 M-row build input into 4 partitions,
     1,645.5 ms;
  2. the scan and materialization of all of `lineitem`, 927.0 ms;
  3. the table build over 48 M rows and 12 M keys, 776.6 ms.
- **Two questions for the next record,** stated as questions:
  - Is the build-side exchange necessary at partition-work parallelism
    1? ADR 604 had already observed `Exchange(parts=4)` at parallelism
    1 as a candidate.
  - Should `lineitem`, the largest table, be the build side of this
    join? DuckDB builds on the smaller inputs and probes `lineitem`, as
    context only.

  Either becomes a one-token cut only through its own plan, with a
  mechanism and a pair.
- The instrument stays. It is behaviour-neutral, confirmed by the
  sweep, and it now reports `prep_build` sub-stages wherever the
  reoriented chain builds with a handle.
