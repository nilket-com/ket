# ADR 367 - scan-output streaming into non-join consumers (q06 canary, measured family coverage)

## Context

The ADR 366 atlas's top sf=10 wall: **`scan output accumulator` x12** (q01/q04/q06/q07/q08/q09/q13/q15/q17/q18/q20/q22),
all ~1 GB just-over. ADR 361 streamed the *join* consumers; these 12 materialize a scan before a **non-join consumer**
(q06's is scan -> filter -> global aggregate). Stream the scan into its consumer, **q06 first**. The claim is **not
"fix all 12"** - it is **"q06 canary + measured coverage across the 12."** If q06's mechanism only covers a subset,
that is still a good cut because it names the next subset precisely.

## Decision

### Phase 0 (load-bearing) - the coverage contract: classify the 12 by consumer shape
Before any implementation, classify each of the 12 scan-output rows by consumer shape. **This table is the coverage
contract** - the cut claims exactly the buckets its mechanism is measured to cover.

- **B1 - Scan -> Filter/Project -> GlobalAggregate** (q06 likely here; the canary).
- **B2 - Scan -> Filter/Project -> GroupedAggregate.**
- **B3 - Scan -> Filter/Project -> Exchange/Join/...** where scan streaming alone **may just move the wall** to a
  downstream materialization (not a green without re-measure).
- **B4 - any scan edge whose consumer needs row identity / runtime filters / order semantics beyond q06** - distinct
  enough that q06's fix does not transfer; named, not forced.

### The cut
- **q06 first (B1):** route q06's scan as a `ScanStream` into the global-aggregate fold (extend the existing
  collapsing-global fold / the scan-edge), bypassing `materialize_scan_stream -> Vec`. Reuse 361's shared
  `bound_scan_request` (ScanRequest parity).
- **Extend only to the buckets the mechanism is measured to cover** (likely B1, possibly B2). B3/B4 and any uncovered
  cell are **named as the next subset**, not speculatively rewritten.
- Topology-named; no qNN in core; no general scan-child recognizer.

## Acceptance
- **q06 sf=10: green** (`pass@execute`) - `scan output accumulator` gone - **and persisted correctness green**
  (`correctness --query q06 --sf 10 --data-source parquet`, ADR 365, `pass@compare`); or a named next wall (332).
- **The Phase-0 coverage-contract table** (12 rows -> B1/B2/B3/B4) is recorded.
- **Measured family coverage:** after the cut, re-run all 12 live at sf=10; record green / advanced / unchanged per
  row. The cut's claim is the **measured** covered set, not the contract's optimistic max.
- **Persisted correctness on every new pass row** (not reach alone); **strict (sf=0.01/sf=1) + dev parity** preserved.
- **ScanRequest parity** (361 shared builder); **no cap bump**; **`scripts/cargo-cgroup test -p toron --no-fail-fast`
  mandatory** (this touches scan execution - the 361 lesson); de-branded core; behavior-neutral at sf=1; q14/q16
  greens unchanged.

## Guardrails
- **Coverage contract first** - classify the 12 by consumer shape before implementing; claim only measured coverage.
- **q06 canary; extend only as measured** - not a general streaming rewrite; B3/B4/uncovered named as the next subset.
- **B3 honesty** - streaming a scan into an exchange/join may relocate the wall; do not call such a cell green without
  a live re-measure showing the producer is actually gone.
- **B4 fence** - consumers needing row identity / runtime filters / order semantics beyond q06 are out of scope, named.
- **ScanRequest parity; persisted correctness on pass rows; no cap bump; --no-fail-fast mandatory; de-branded.**

## Risks
- **Wall-moving (B3)** - scan streaming into a join/exchange just shifts to a downstream accumulator; the live
  re-measure catches it (advanced, not green).
- **Overclaiming "all 12"** - the coverage contract + the measured re-run bound the claim to what's proven.
- **Silent answer change** - the 361 shared builder + persisted correctness (365) + dev parity.
- **Engine-cut regression** - `--no-fail-fast` mandatory (the empty-batch/schema family stays green-guarded).
- **Cap-bump temptation** - rejected; the 1 GB accumulator budget is unchanged; cells reduce by streaming or stay named.

## Forward arc
367 (q06 + the measured-covered scan-output buckets) -> the named next subset (remaining scan-output cells, by bucket)
-> **streamed inner join build spill writer x4** (q03/q05/q10/q12, reuse 363 projection / 364 partition) -> q02
join-output, q21 existence-set singletons -> q11 oracle-guard (small harness), q19 time-wall (separate). The 366
ranking drives the order; highest-count producer first, each claim measured.
