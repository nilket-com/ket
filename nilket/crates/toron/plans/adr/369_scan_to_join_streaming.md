# ADR 369 - scan-to-join streaming (q13 canary, measured B3 coverage)

## Context

The next scan-output-family cut. 367 cleared B1 (q06), 368 cleared B2-exchange (q01/q15/q20). **B3 = scan -> join ->
group/agg** - the remaining 8 scan-output reds (q04/q07/q08/q09/q13/q17/q18/q22). Stream the scan into the join build/
probe; **join-output relocation is a successful named wall-move, not a failed cut**. q13 canary. Claim only **measured**
coverage, like 367/368.

## Decision

### Phase 0 - the B3 coverage contract (classify before implementing)
Classify the 8 into explicit sub-buckets - this table is the coverage contract:
- **B3-left-outer-simple:** scan -> LeftOuter(+residual) -> grouped/double-group (**q13**, the canary).
- **B3-inner-chain:** scan -> multi-table inner join chain -> group (q07/q08/q09-class).
- **B3-correlated:** scan -> correlated-scalar recombine (q17).
- **B3-semi/anti-or-exists:** members routing through existence/correlation rather than a plain join chain
  (q04/q18/q22, per the map).
The cut claims only the sub-buckets it is **measured** to cover; q13 first.

### Mechanism
Stream the scan(s) into the join build/probe (reuse the shared `bound_scan_request` scan stream + the existing keyed
join machinery + 368's grouped fold), bypassing `materialize_scan_stream -> Vec`. For q13: scan -> keyed
LeftOuter(+NOT-LIKE residual) -> grouped fold -> re-group.

### The gate (tight green-definition - operator)
- **"scan-output producer gone" is necessary but NOT sufficient.**
- **Green** requires: scan-output producer gone **AND** no downstream producer of any kind (join-output
  867/969/2612/2684/2920, join build/spill resident, exchange accumulator, aggregate accumulator, or other named
  producer) **AND** persisted sf=10 correctness.
- If streaming the scan moves the wall to a **named downstream producer**, that is a **successful B3 wall-mover** - the
  cell is recorded advanced-not-green. If join-output recurs, it is **named as the next high-leverage target**
  (join-output streaming, the q02 family); if another producer recurs, that producer owns the next cut.

### The cut
q13 first; extend to the measured-covered B3 sub-buckets; wall-moved cells recorded against their named join-output
producer. Reuse the shared builder; topology-named; no qNN in core; no general join recognizer.

## Acceptance
- **q13 sf=10: green** = scan-output gone **and** no join-output producer **and** persisted correctness
  (`correctness --query q13 --sf 10 --data-source parquet`, ADR 365, `pass@compare`); **OR** advanced to a named
  downstream wall (332), recorded wall-moved-not-green.
- **B3 coverage-contract table** (8 cells -> the 4 sub-buckets) recorded; **measured B3 coverage** (re-run the 8 live,
  per-cell green / advanced / unchanged); claim only the measured cells.
- **Correctness:** persisted sf=10 `pass@compare` for any new green row; small-scale strict + dev parity for the
  scan->join->group streaming (LeftOuter + residual + double-group semantics).
- **ScanRequest parity** (shared builder); **no cap bump**; **`--no-fail-fast` required** (engine cut); de-branded;
  q01/q06/q14/q15/q16/q20 greens unchanged.

## Guardrails
- **Coverage contract first; claim only measured** - the 4 sub-buckets classified before implementing.
- **Green is gated tight** - scan-output-gone is necessary; green also requires no downstream producer + persisted
  correctness. Wall-moved-to-a-named-producer is a success, not green.
- **q13 canary; extend only as measured** - no general join recognizer.
- **Residual/LeftOuter/double-group correctness** - residual per-candidate + null-fill + both group levels (strict +
  dev parity).
- **ScanRequest parity; persisted correctness on new green rows; no cap bump; --no-fail-fast; de-branded.**

## Risks
- **Join-output is the real wall** - then B3 cells advance to a named join-output producer; the cut succeeds by naming
  join-output streaming (the q02 family) as the next high-leverage target.
- **Another downstream producer is the real wall** - build/spill resident, exchange, or aggregate accumulation is
  recorded as the named next producer instead of forcing the join-output hypothesis.
- **Overclaim** - the coverage contract + tight green-definition + live re-measure bound the claim.
- **Silent answer change** - shared builder + persisted correctness + dev parity (residual/LeftOuter/double-group).
- **Engine-cut regression** - `--no-fail-fast` required (the empty-batch/schema family stays green-guarded).

## Forward arc
369 (q13 + measured B3) -> if join-output recurs, **join-output streaming** (the q02 family, next high-leverage
producer) -> B4 (identity/runtime-filter/order) -> remaining scan-output -> **streamed inner join build spill writer x4**
(q03/q05/q10/q12, reuse 363/364) -> q21 existence-set -> q11 oracle-guard, q19 time-wall. The re-measure decides whether
B3 is a green cluster or surfaces join-output as the next common wall.
