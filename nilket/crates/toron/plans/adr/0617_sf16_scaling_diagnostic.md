# ADR 617 - SF16 scaling diagnostic

**Status:** PROPOSED. Successor to ADR 616. **A diagnostic record: no
token, no pair, no cut, no saving projected.** Sealed SF8 at ADR 613's
coordinate stays the campaign target. Nothing here changes that
coordinate. SF16 artifacts are unsealed diagnostics.

Since ADR 604, the campaign's evidence has come from one scale. At one
scale, the ratio to DuckDB mixes several kinds of cost:
- cost that grows with the input;
- cost fixed per query or per stage;
- cost that changes at a size boundary: a build crossing an admission
  cap, a spill starting, an estimate changing a route.

A second scale adds evidence about which kinds dominate each gap.
**Two scales do not separate them uniquely.** A mixture of costs,
changed selectivity, group or key cardinality, allocation and cache
behaviour, or a changed plan can each produce the same ratio. So a
reading here is a candidate, supported or not by the counts and
transitions reported beside it, or it is unresolved.


## Context

**The data.** The SF16 strict dataset was generated on bigfoot on
2026-09-30, with ADR 613's measured executable (`9a84d5c4…`,
`toron-tpch prepare --data-source parquet --mode strict --sf 16`). It
took 1 min 17 s, with a peak RSS of 645 MB.

| item | value |
|---|---|
| path | `target/tpch/data/strict/sf16/`, the same absolute path on strix |
| manifest fingerprint | `fnv1a64:32703e0082d64670` |
| format, chunk rows | 2, 65,536, as for SF8 |
| `lineitem` | 95,988,640 rows, 3.81 GB (SF8: 47,989,007) |
| `orders` | 24,000,000 rows (×2) |
| `customer` | 2,400,000 rows (×2) |
| `partsupp` | 12,800,000 rows (×2) |
| `part` | 3,200,000 rows (×2) |
| `supplier` | 160,000 rows (×2) |
| `nation`, `region` | 25 and 5 rows, as for SF8 |
| total | 6.3 GB |

`lineitem` is ×2.0002, since the generator draws 1 to 7 line items per
order.

**What SF8 already shows for the pilot cells.** These are ADR 613's
board medians. ADR 616 has since changed q05 and q09: its paired
treatment medians were 3,404.6 and 6,720.6 ms. Those are paired
figures, not board cells.

| cell | Toron ms | DuckDB ms | Toron spill bytes | owner at SF8 (ADR 614/615/616) |
|---|---|---|---|---|
| q21 | 7,972.7 | 2,205.0 | 27,242,808 | `runtime_prep` 3.7 s (unrefined); inner replays 3.5 s |
| q09 | 6,978.4 | 1,629.0 | 0 | `runtime_prep` 1.7 s; `suffix_probe` replay 2 1.2 s |
| q18 | 5,976.7 | 1,479.0 | 0 | aggregate 3.5 s and exchange 1.1 s, no stage vocabulary |
| q05 | 4,752.0 | 591.0 | 0 | the replay-1 build (2.3 s after ADR 616) |
| q01 | 3,352.1 | 957.0 | 0 | no joins: the grouped-fold control |


## Decision

*The cells.* q01, q05, q09, q18, and q21:
- q01 has no joins: the control;
- q05 is the exchange and build path;
- q09 and q21 are the join and spill paths;
- q18 is the aggregate and exchange path.

*The coordinate.* ADR 613's, except for the scale:
- strict parquet;
- task memory 2 GiB and spill reserve 2 GiB, **kept at 2 GiB** so that
  budget boundaries show;
- partition parallelism 1, DuckDB threads 1, affinity 0-15.

A later panel with memory in proportion to scale is its own record.

*The executable.* Built once from **this record's plan commit**, whose
`src` is verified identical to ADR 616's closing commit (`3a782d97`), so
the manifest's tree is the reviewed plan's tree. The build goes
through `toron-bench` (recipe `9af51cbc…`, DuckDB `c478c0f4…`), and the
executable is staged and hash-verified. SF8 is re-measured on it,
not read from ADR 613's board, since ADR 616 changed q05 and q09.

*The timed runs: the runner and the row contract.* Every timed sample
goes through `toron-bench run --manifest <this record's manifest> --
perf`, the parent path. It takes an explicit `--sf`, and it gives peak
RSS with the parent-high-water label.
- Toron cannot use `perf-run-one` for this: that path has no parent
  wait4 RSS (the ADR 612 precedent).
- DuckDB cannot use it at all: `perf-run-one` rejects DuckDB
  (`main.rs`).

Each block is one cell, one engine, one scale:
`perf --engines <engine> --query <cell> --mode strict --sf <8|16>
--data-source parquet --data-dir target/tpch/data --runs 1 --warmup 1
--output-dir <fresh directory>`.

So each timed sample is preceded by its own untimed execution of the
same query under the same engine and scale. **That warm-up is the
cache control.** It is a best-effort warm-cache comparison, not a
guarantee of equal cache state: eviction, host activity, query buffers,
and different projections all remain possible.

*The order, declared before launch.* Seven rounds. Each round runs all
20 blocks (5 cells × 2 engines × 2 scales), so every cell, engine, and
scale gets 7 timed samples. Within round r (1 to 7):
- **cell order** is q01, q05, q09, q18, q21, rotated left by r − 1;
- **per cell, scale order** is SF8 then SF16 in odd rounds, and SF16
  then SF8 in even rounds;
- **per scale, engine order** is Toron then DuckDB in rounds 1, 2, 5, 6,
  and DuckDB then Toron in rounds 3, 4, 7.

Every row is retained, including failures. The run script and its full
expanded order are recorded before launch.

*The row fields, by engine.*
- **Toron:**
  - `execute_ms`, status, the Arrow digest, and the plan fingerprint;
  - `spill_bytes` and join-spill observations, labelled observed no
    spill, spilling, or unreadable (as in ADR 616);
  - peak RSS with its label.
- **DuckDB:**
  - its timed duration, status, and `duckdb_result_sha256`;
  - peak RSS with its label.

  Toron's spill fields are not read for DuckDB.

A missing field is recorded as missing, never as a default.

*The armed runs: untimed, three kinds, never mixed.*
1. **Stage-attributed Toron runs:** one per cell per scale, with node
   attribution, the join-build profile, and the scan ledger. **No
   cap-check receipt.** Its environment's absence is recorded, as for
   every timed run.
2. **The q05 cap-check receipt:** a third, separate untimed preflight
   per scale. Its equivalence path re-executes the build, so it is never
   combined with the attribution run. It gives the build estimate
   against the cap, and the path the estimate implies.
3. **DuckDB operator profiles:** one per cell per scale, with
   `OPERATOR_ROWS_SCANNED` requested explicitly. That is ADR 605's
   `DUCKDB_SCAN_WORK_PROFILE_SETTINGS`, not ADR 604's ordinary profile,
   which did not collect it.
   - The scan-work census binds to a sealed anchor, which SF16 lacks.
     So where it cannot be used, the profile is taken with the staged
     DuckDB executable directly, under the same settings, memory limit,
     thread pin, and a fresh temp directory, by a recorded script.
   - Each profile's result digest is bound to that scale's timed DuckDB
     digests.

*Identity, per scale.* **Cross-scale digests are not compared.** At
each scale:
- **each engine's timed digests are stable** across its seven samples;
- **Toron's answer is checked** by the harness's `correctness` command
  on the persisted data, at SF16 and at SF8, the same method at both;
- **the DuckDB profile's digest** equals that scale's timed DuckDB
  digests;
- a cross-engine equality check is claimed only where the two digests
  are defined over the same encoding. Otherwise it is named as
  unavailable.

**The armed runs are gated too.** Each stage-attributed Toron run and
each q05 receipt preflight must match its own cell and scale's passing
timed digest and plan fingerprint. A missing profile or receipt, or a
failed reconciliation, is named. Timed Toron plan fingerprints must be
stable within each cell and scale. Fingerprints may differ across
scales, and a difference is reported as a transition.

A cell that fails its identity at a scale gets no timing reading at that
scale. The failure is reported, and it becomes a correctness record
ahead of any perf record.

*The report, per cell.* For each engine, SF8 against SF16:
- the median and range of the seven samples, and the ratio SF16/SF8;
- **a normalized rate, per engine, with its own counter.** The rate
  uses the counter's semantics, named:
  - Toron: the ledger's decoded raw rows;
  - DuckDB: `OPERATOR_ROWS_SCANNED`.

  Scan multiplicity and filtering are shown. Each engine's SF16/SF8
  rate ratio is compared. **No cross-engine throughput is inferred
  from the absolute rates:** the counters are operational, not
  identical work (ADR 605);
- the Toron/DuckDB gap at each scale, and how it moves;
- **counts beside the ratios,** where available:
  - input, output, build, and key counts;
  - group counts;
- **transitions, only where corroborated,** each named from its
  evidence:
  - route and plan fingerprint;
  - admission and the estimate-implied build path;
  - spill bytes and status;
  - peak RSS with its label;
- **for Toron, per stage,** where attribution reconciles: each stage's
  SF16/SF8 ratio. These come from **one armed run per scale**, so they
  are single-run diagnostic observations, not stable estimates. Stages
  are paired across scales only where the invocation route and the
  stage names and ordinal topology correspond at both scales (ADR 614's
  correspondence discipline). A changed or unmatched stage is shown
  separately, not paired by position.

*The reading.* No categorical thresholds on a ratio. A cell's gap is
described as a **candidate** reading, with its supporting counts and
transitions, or as **unresolved**. It is unresolved when:
- spread overlaps between the scales;
- identity fails;
- the evidence admits more than one reading.

No mechanism is claimed past what the stages, profiles, and receipts
show, and no saving is projected.

*Budget.* Each block has an operational timeout of 20 minutes. A block
that times out is recorded as an incomplete result, not as a runtime.
The timeout terminates and reaps the block's whole descendant process
group. Before any later block starts, the script verifies that no query
child remains and that the spill scratch is empty. Incomplete
artifacts are kept. **If cleanup or scratch isolation cannot be
verified, the campaign stops:** the one-process-at-a-time coordinate
depends on it.

*SF32.* A separate follow-up record, with its own reviewed scope and
budget, after the SF16 results.


## Acceptance

- The SF16 dataset on strix, with its manifest fingerprint and every
  table's file hash matching bigfoot's.
- One executable, built from the plan commit (`src` identical to
  `3a782d97`), staged and hash-verified.
- The declared order run to completion, with every row retained.
- For each of the five cells, at both scales, for both engines:
  - the armed runs, the q05 receipt preflights, and the profiles;
  - the identity checks;
  - the report above.
- Or, for any part not established, the named reason.
- Plan commit, then one implementation commit with the results and the
  closure.


## Guardrails

- A diagnostic: no token, no cut, no saving projected.
- **SF16 timings never enter the sealed SF8 board** or its ratio. The
  board's checks require SF8, and they are unchanged.
- No ADR 588 or 597 treatment verdict or variability bar: the scale
  changes the input and the answer.
- Armed runs and profiles are never read as timed latencies.
- Cross-scale digests are not an identity gate.
- No strix launch while another record is measuring.


## Risks

- **q05's SF16 build estimate.** Doubling ADR 616's receipt (5.76 GB)
  gives about 11.5 GB, below the 32 GiB cap that receipt observed. That
  is an extrapolation. The SF16 receipt reports the estimate itself.
- **q21 spills at SF8.** At SF16 its spill may grow or change path. That
  is a finding, not a failure, unless the run is refused.
- **Historical RSS values** from ADR 613's board are not carried into
  this record's comparisons. Only this record's parent-high-water rows
  are compared.
