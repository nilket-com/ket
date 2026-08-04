# ADR 370 - B3-inner-chain scan-output miss (q07 canary, diagnostic-first)

## Context

The next scan-output sub-bucket: **B3-inner-chain (q07/q08/q09)**, q07 canary. The reoriented-inner-chain path
(343 q07, 357 q08/q09) **already streams its scans** via `bound_scan_request` + `storage.scan` (streaming.rs:1900/1958/
2023/2154/2215) - yet q07 still hits `scan output accumulator` at sf=10. So **this is NOT "add scan streaming"**; it is
**diagnostic-first**: find why q07 doesn't reach the already-streaming path, then remove that exact miss. No q17 (a
time/no-row concern). Claim only measured.

## Decision

### Diagnostic-first (the first deliverable is the miss reason)
Before any fix:
1. **Identify the exact `scan output accumulator` producer** for q07 at sf=10 (which scan materializes; bound plan +
   temporary trace, not the generic producer name).
2. **Prove the miss class:** did `extract_reoriented_inner_chain` (streaming.rs:178) **fail to match** q07's plan, or
   **match but leave one scan edge materialized**?
3. **Compare plan shapes:** the persisted-Parquet (ADR 360) plan vs the in-memory plan 343/357 covered - is the miss a
   persisted-path plan difference?
4. **Only then** widen the recognizer to q07's shape, or stream the specific uncovered edge. Do not reshape the
   persisted data surface merely to satisfy the recognizer; change the data surface only if Phase 0 proves the persisted
   plan is itself wrong.

The implementation is **valid only if it removes the measured miss** from step 1/2 - not a speculative stream that
already exists.

### Coverage (q08/q09 only if they share the miss)
After q07, classify whether q08/q09 share q07's diagnosed miss (the 357 generalization) or differ. **If the miss is not
shared, 370 claims only q07 and records the split** (q08/q09 -> their own diagnosed miss, a named follow-up).

### The fix
Per the diagnosis: widen `extract_reoriented_inner_chain` so q07 reaches the existing streamed path, or stream the
named uncovered scan-edge. If the persisted plan has legitimate wrappers or shape differences, the executor recognizer
handles them; the persisted data surface is changed only for a proven data-surface bug. Reuse the shared
`bound_scan_request` + the existing reoriented-inner-chain streaming; topology-named; no qNN in core; no general
recognizer.

### The gate (tight green-definition, 369 axis)
Green requires the `scan output accumulator` (and any downstream producer) **gone** AND **persisted sf=10 correctness**.
A wall moved to a named downstream producer (join-output 867/969/2612/2684/2920, build/spill resident, exchange,
aggregate) is a **successful wall-move** - advanced-not-green, the recurring producer named.

## Acceptance
- **The miss reason is recorded first** (which scan / match-fail vs uncovered-edge / persisted-vs-in-memory shape) - the
  primary deliverable; the fix is valid only if it removes that measured miss.
- **q07 sf=10: green** (scan-output + downstream gone + persisted correctness `correctness --query q07 --sf 10
  --data-source parquet`, 365 `pass@compare`); OR advanced to a named downstream wall (332).
- **q08/q09:** covered only if they share q07's miss (measured); otherwise **370 claims q07 only and records the split**.
- **Correctness:** persisted sf=10 `pass@compare` for any new green; small-scale strict + dev parity for the
  inner-chain streaming.
- **ScanRequest parity** (shared builder); **no cap bump**; **`--no-fail-fast` required**; de-branded; prior greens
  (q01/q06/q13/q14/q15/q16/q20) unchanged; **no q17**.

## Guardrails
- **Diagnostic-first** - the miss reason precedes the fix; no redundant stream (the path already uses storage.scan).
- **Valid only if it removes the measured miss** - the fix is tied to the step-1/2 diagnosis.
- **Tight green-definition** - persisted correctness required; wall-moved-to-named-producer is a success.
- **q08/q09 only if shared; else q07-only + split recorded.**
- **ScanRequest parity; no cap bump; --no-fail-fast; de-branded; no q17; no general recognizer.**

## Risks
- **Redundant-stream trap** - framing it as "add streaming" when the path streams; the diagnostic-first decision
  prevents it.
- **Miss not shared by q08/q09** - 370 honestly claims q07 only + records the split (not forced coverage).
- **Wall-moves to a downstream producer** - named (advanced-not-green); surfaces the next target.
- **Silent answer change** - shared builder + persisted correctness + dev parity.
- **Engine-cut regression** - `--no-fail-fast` required.

## Forward arc
370 (q07, + q08/q09 if shared, or the split) -> B3-semi/anti (q04/q18/q22, the 345 existence path) -> q17/q19 time-wall
investigation -> remaining scan-output -> **streamed inner join build spill writer x4** (q03/q05/q10/q12, reuse 363/364)
-> join-output (q02 family if it recurred) -> q21 existence-set -> q11 oracle-guard. The re-measure drives the order.
