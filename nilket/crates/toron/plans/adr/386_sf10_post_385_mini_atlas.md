# ADR 386 - SF=10 post-385 mini-atlas

## Status
Proposed.

## Context

ADR 385 changed the board materially: q19 moved from a 33m/no-row time anomaly to a persisted sf=10
correctness-green row, q03 advanced to a scan-output accumulator wall, and the 374-383 perf peels changed the
partitioned spilled-inner family enough that the old 366 ranking is stale for those cells.

The next cut should not be picked from stale intuition. This ADR runs a focused live sf=10 re-measure over the cells
that the perf arc touched, then ranks the next producer from current evidence. It is a mini-atlas, not a full 22-query
atlas and not an engine change.

## Decision

Run a **measurement-only** sf=10 mini-atlas over the changed/time-cluster cells:

- partitioned spilled-inner family: q03, q05, q10, q12;
- time/perf cells touched or diagnosed by 374-385: q04, q07, q17, q19, q21;
- memory-tail control: q08, to validate that the generic streamed-inner wiring in ADR 385 did not silently move the
  carried-forward inner-chain memory-tail class;
- runner anchors: q06 and q14, because q06 is the simple scan-output canary and q14 is a known persisted green.

The results doc may include q01/q11/q13/q15/q16/q20 and q02/q08/q09/q18/q22 as carried-forward rows only if they are
clearly provenance-labeled. A carried-forward row cannot outrank a freshly measured row unless the ADR explicitly reruns
it.

## Evidence Rules

- Use only live `perf` / `perf-run-one` rows from persisted Parquet for the mini-atlas target set.
- Use live persisted correctness for every new Toron `pass` row.
- Never use `matrix` or `correctness-matrix`; those are cache renders.
- Keep fixed 4 GiB measurement scope:
  - `MemoryMax=4G`
  - `MemoryHigh=4G`
  - no budget or cap changes
- Per-query isolation is required. A q07/q17 timeout must not erase q03/q19 evidence.

## Phase 0 - Runner Sanity

Before the mini-atlas:

1. Confirm the ADR 360 persisted dataset exists:
   `target/tpch/data/strict/sf10/manifest.json`.
2. Build release outside the capped runtime scope.
3. Smoke two anchors:
   - q14 should pass and correctness-verify;
   - q06 should classify according to the current engine state, with producer/bytes if red.
4. Confirm the raw rows include status, reason, diagnostic message, execute time, spill bytes, plan fingerprint, and
   engine.

If the runner cannot produce isolated rows for a target query, record the harness reason and do not replace it with a
cached row.

## Phase 1 - Focused Board

Run the target set live at sf=10, strict, release, persisted Parquet, fixed 4 GiB:

```text
q03 q04 q05 q06 q07 q08 q10 q12 q14 q17 q19 q21
```

Default to one run, no warmup. Use a bounded wall timeout per Toron query so a time-wall cell is classified as
time/no-row rather than blocking the atlas indefinitely. DuckDB is useful for gap context but does not block Toron wall
classification; if the combined engine runner is fragile, run Toron and DuckDB separately.

For every target query, record:

- Toron status/reason;
- producer, requested bytes, and available bytes for budget failures;
- execute_ms, spill bytes, and plan fingerprint;
- whether a pass row has live persisted correctness;
- DuckDB execute_ms/version if collected;
- no-row timeout duration for time walls.

## Classification

Classify only from live evidence:

- `green_correct`: Toron pass plus live persisted correctness.
- `memory_wall`: budget producer/dimension/bytes from the row.
- `spill_wall`: spill producer/bytes from the row.
- `time_no_row`: bounded timeout with stable/no budget row.
- `correctness_gap`: pass/reach row whose persisted oracle is unavailable or rejects for a policy reason.
- `harness_gap`: runner/data problem, not an engine classification.

If a row moved from the 366 class, record both the old class and the new class. A wall move is a valid result.

## Acceptance

- Every target query has a live sf=10 Toron row or an explicit no-row/harness artifact.
- Every target pass row has live persisted correctness provenance.
- q19 is re-confirmed as green or any regression is named immediately.
- q03/q05/q10/q12 are classified from current evidence, not from pre-385 results.
- q08 is rerun as a memory-tail control. If it remains in its carried-forward scan-output class, the remaining
  carried-forward memory-tail rows stay provenance-labeled; if it moves, the mini-atlas must either remeasure the
  sibling memory-tail cells or explicitly mark them unvalidated.
- q07/q17/q21 are classified with bounded timeout/producer evidence rather than left as vague "slow".
- The results doc ranks the next cut from the mini-atlas target set and marks carried-forward rows separately.
- No engine behavior change, no cap bump, no cached matrix evidence.

## Guardrails

- This ADR measures only. Do not fold a fix into the mini-atlas.
- Do not call carried-forward rows fresh evidence.
- Do not hide timeouts as failures without timeout duration and last observed artifact.
- Do not let DuckDB instability or schema mismatch block Toron classification.

## Forward Arc

ADR 386 produces the current post-385 ranking. The next ADR should target the highest-leverage freshly measured producer:
possibly q03's scan-output wall, the remaining q07/q17/q21 time cluster, q12's partition-group residual, or a memory-tail
cell if the mini-atlas justifies pulling it forward.
