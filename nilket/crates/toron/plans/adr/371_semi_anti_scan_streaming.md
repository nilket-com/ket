# ADR 371 - B3-semi/anti scan-output miss (q04 canary, diagnostic-first)

## Context

The next scan-output sub-bucket: **B3-semi/anti (q04/q18/q22)**, q04 canary. All route through the 345 semi/anti
existence path (`StreamingSemiAntiExistence`, `extract_streaming_inner_prefix_semi_anti_chain` streaming.rs:1127),
which already streams and spills - yet all three are `scan output accumulator` reds at sf=10 (366 atlas), the scan
materializing **before** the existence path. So like 370, **diagnostic-first**: name why the scan materializes, remove
that exact miss. The expected wall-move target is q21's `semi/anti existence-set spill writer` - if q04/q18/q22 converge
there, the deferred q21 existence-set cut becomes a **common-wall cut, not q21 momentum**. Claim only measured.

## Decision

### Phase 0 (diagnostic-first) - name the q04 miss before code
1. **Producer:** q04's exact `scan output accumulator` producer at sf=10 (which scan / TableId; bound plan + temporary
   trace, not the generic name).
2. **Miss class:** **recognizer miss** (q04 never enters the 345 existence path) **vs matched-but-scan-materialized**
   (the 370 pattern: a downstream/probe scan prepared via the ordinary path).
3. **Plan-shape compare:** q04 persisted-Parquet (360) plan vs the in-memory plan 345 covered.
4. Record the miss reason first. **No code until named.** The fix is valid only if it removes that measured miss.

### The producer table (the coverage contract, measured)
Record for every cell - this table forces the q18/q22 claim to stay measured and makes the q21 convergence obvious:

| query | semi/anti kind | scan-output producer before | miss class | producer after | coverage verdict |
| --- | --- | --- | --- | --- | --- |
| q04 | semi (EXISTS) | scan output accumulator | (Phase 0) | (re-measure) | canary |
| q18 | semi (IN) | scan output accumulator | (Phase 0) | (re-measure) | covered-if-shared |
| q22 | anti (NOT EXISTS) | scan output accumulator | (Phase 0) | (re-measure) | covered-if-shared |

### Coverage (q18/q22 only if they share the measured miss)
q18/q22 are coverage **only if their measured miss matches q04's**. Otherwise **371 claims q04 only and records the
split** (q18/q22 -> their own diagnosed miss, named follow-up).

### The fix
Per the diagnosis: widen the existence recognizer to q04's shape, or stream the named uncovered scan-edge - reusing the
shared `bound_scan_request` + the existing 345 existence path. Topology-named; no qNN in core; no general recognizer.

### The gate (tight green-definition, 369/370 axis)
- **Green is NOT required.** Green requires scan-output (and any downstream producer) **gone** AND **persisted sf=10
  correctness**.
- **Moving from scan-output to a named `semi/anti existence-set spill writer` wall is a successful wall-move**
  (advanced-not-green) - the "producer after" column records it.
- **If q04/q18/q22 converge on the q21 existence-set producer, the deferred q21 cut becomes a common-wall cut** (4 cells
  share the producer), re-prioritized by measurement, not q21 momentum.

## Acceptance
- **The q04 miss reason recorded first** (recognizer-miss vs matched-but-scan-materialized + the shape evidence); the
  fix valid only if it removes that measured miss.
- **The producer table** filled live (before/miss-class/after/verdict per cell).
- **q04 sf=10:** green (scan-output + downstream gone + persisted correctness `correctness --query q04 --sf 10
  --data-source parquet`, 365 `pass@compare`); OR advanced to a named downstream wall (332), recorded in "producer
  after".
- **q18/q22:** covered only if they share q04's measured miss; else **q04-only + recorded split**.
- **Correctness:** persisted sf=10 `pass@compare` for any new green; small-scale strict + dev parity for the semi/anti
  existence streaming (EXISTS/IN/NOT-EXISTS semantics).
- **ScanRequest parity** (shared builder); **no cap bump**; **`--no-fail-fast` required**; de-branded; prior greens
  (q01/q06/q13/q14/q15/q16/q20) unchanged; **no q07/q17/q19** (time cluster, separate arc).

## Guardrails
- **Diagnostic-first** - the miss reason precedes code; the existence path already streams, no redundant stream.
- **Producer table measured** - the q18/q22 verdict comes from the live re-measure, not assumption.
- **Tight green-definition** - persisted correctness required; wall-moved-to-existence-set-spill is a success.
- **q04 canary; q18/q22 only if shared; else split.**
- **ScanRequest parity; no cap bump; --no-fail-fast; de-branded; no general recognizer; no time-cluster cells.**

## Risks
- **Wall-moves to the existence-set spill** - the expected/successful outcome; the producer table records it, and
  convergence with q21 re-prioritizes the deferred existence-set cut as a common-wall cut.
- **Miss not shared by q18/q22** - q04-only claim + recorded split (semi/IN vs anti may differ; q18's grouped-having IN
  is more complex).
- **Overclaim** - the producer table + tight green-definition + live re-measure bound the claim.
- **Silent answer change** - shared builder + persisted correctness + dev parity (EXISTS/IN/NOT-EXISTS).
- **Engine-cut regression** - `--no-fail-fast` required.

## Forward arc
371 (q04, + q18/q22 if shared, or split) -> if the existence-set spill recurs across q04/q18/q22/q21, the **q21
existence-set spill-volume** cut (now a common-wall cut; reuse 348/363 projection) -> remaining scan-output (q08/q09
separate inner-chain miss) -> **streamed inner join build spill writer x4** (q03/q05/q10/q12, reuse 363/364) ->
join-output (q02 family) -> q11 oracle-guard -> the **time/perf arc** (q07/q17/q19, vs DuckDB). The re-measure drives
the order.
