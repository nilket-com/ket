# ADR 368 - scan-to-grouped-fold streaming (q01 canary, measured B2 coverage)

## Context

The next scan-output-family cut. ADR 367 streamed B1 (scan -> global-aggregate fold), greening q06 (1/12). **B2 =
grouped aggregate**: the same scan-to-fold idea through **grouped state**, through the exchange where present. **q01 is
the canary** - the simplest `Scan -> Filter -> Exchange -> GroupedAggregate` member of the 367 coverage table. Claim
only **measured** coverage, like 367; and if the exchange output still materializes, this is a **wall-mover, not a green
claim**.

## Decision

### Mechanism
Compose the existing primitives: the scan `ScanStream` (367 / shared `bound_scan_request`) **feeds exchange
partitioning**, then the **grouped fold consumes the partitioned batches** (`fold_bound_grouped_aggregate_batch`,
aggregate.rs:573), bypassing `materialize_scan_stream -> Vec`. No bare scan -> (exchange ->) grouped-fold streaming
exists today (the grouped streaming paths are all join-input); this adds it. The grouped fold state is bounded by group
count (tiny for q01: ~6 groups).

### Phase 0 - the B2 coverage contract (classify before implementing)
Classify the named B2 cells by exact grouped consumer shape - this table is the coverage contract:
- **B2-direct:** Scan -> Filter/Project -> GroupedAggregate (no exchange).
- **B2-exchange:** Scan -> Filter/Project -> Exchange(group-key) -> GroupedAggregate (q01).
- **B2-mixed:** join + grouped (likely already covered by join-grouped paths, or out of B2 scope).

### The gate (honesty axis - operator)
The green path is partition-and-fold: scan batches are partitioned in materialized-exchange order and folded without a
returned exchange `Vec`. If the implementation can only stream into the exchange but the **exchange output still
materializes**, the cell **advances to a named exchange wall** - the cut is a **wall-mover for that cell, not a green
claim**. A cell is green only if the live sf=10 re-measure shows the scan-output (and exchange) producer is gone.

### The cut
q01 first (scan -> exchange -> grouped fold). Extend to the B2 cells the mechanism is **measured** to cover; B2-mixed
and any cell that stops at a named exchange wall are recorded (covered-vs-wall-moved), not overclaimed. Reuse the shared
`bound_scan_request`; topology-named; no qNN in core; no general grouped recognizer.

## Acceptance
- **q01 sf=10: green** (`pass@execute`) + **persisted correctness** (`correctness --query q01 --sf 10 --data-source
  parquet`, ADR 365, `pass@compare`); **OR** a named next wall (332) - if the exchange output materializes, q01 is
  recorded as **advanced (wall-moved), not green**.
- **B2 coverage-contract table** recorded; **measured B2 coverage** (re-run the B2 set live, per-cell green / advanced /
  unchanged); claim only the measured cells.
- **Correctness:** persisted sf=10 `pass@compare` for any new green row; small-scale (sf=0.01/sf=1) strict + dev parity
  for the grouped-fold streaming. The parity test must be Float64-sensitive and verify the streamed exchange preserves
  the same per-partition row visitation order and final group output order as the materialized exchange path.
- **ScanRequest parity** (shared builder); **no cap bump**; **`scripts/cargo-cgroup test -p toron --no-fail-fast`
  required** (engine cut); de-branded core; behavior-neutral at sf=1; q06/q14/q16 greens unchanged.

## Guardrails
- **Coverage contract first; claim only measured** - classify B2 by exact shape before implementing.
- **Wall-mover gate** - exchange-still-materializes => advanced-not-green; only the live re-measure proves green.
- **q01 canary; extend only as measured** - no general grouped recognizer.
- **ScanRequest parity; persisted correctness on new green rows; no cap bump; --no-fail-fast required; de-branded.**

## Risks
- **Exchange shuffle is the real wall** - then q01 advances (named exchange wall), and B2-exchange is a wall-mover
  cluster; the cut still succeeds by naming the exchange as the next common producer.
- **Overclaim** - the coverage contract + live re-measure bound the claim to measured cells.
- **Silent answer change** - shared builder + persisted correctness (365) + dev parity.
- **Engine-cut regression** - `--no-fail-fast` required (the empty-batch/schema family stays green-guarded).

## Forward arc
368 (q01 + measured B2, or B2-exchange named as a wall-mover surfacing the exchange) -> B3 (scan -> join/exchange) -> B4
(identity/runtime-filter/order) -> remaining scan-output cells -> **streamed inner join build spill writer x4**
(q03/q05/q10/q12, reuse 363/364) -> q02/q21 singletons -> q11 oracle-guard, q19 time-wall. If 368 surfaces the exchange
as a common wall, exchange-output streaming becomes the next high-leverage producer; the atlas/re-measure decides.
