# ADR 366 - TPC-H SF=10 atlas redux

## Status
Proposed.

## Context

ADR 359 tried to measure the sf=10 board and correctly stopped at the gauge data-surface wall: full in-memory
`lineitem` fixture generation OOMed before the engine could be measured. ADR 360 replaced that with a persisted Parquet
data surface, and ADRs 361-364 used q21 as a canary to peel the first scan/materialization terms:

- q06 reaches sf=10 from persisted Parquet but is a confirmed engine red at `scan output accumulator`
  (`memory_budget_exhausted`), 24,448 bytes over the 1 GiB accumulator cap.
- q14 reaches sf=10 after scan-output streaming and is live correctness-green.
- q21 no longer hits the streamed inner-build resident wall; it now reaches a distinct
  `semi/anti existence-set spill writer` `spill_budget_exhausted` wall.
- q17 still has a named persisted correctness policy gap at sf=10 (`q17_sf10_oracle_policy_gap`).

The q21 canary has served its purpose. The lever kit now includes scan streaming, filtered scan-edge widening,
spill-column projection, and partitioned streamed inner-build resident bounding. The high-leverage next step is to
measure the whole sf=10 board and identify which remaining walls are common, rather than continuing to peel q21 alone.

## Decision

Run a **live sf=10 atlas redux** over all 22 TPC-H queries using the persisted Parquet surface. ADR 365 must land first
so Toron pass rows can be checked against the independent oracles on the same persisted data surface. This ADR is
measure-and-classify only: no engine fixes, no cap bump, no q21-specific work.

### Fixed 4 GB cap

Keep the same fixed runtime cap used by the sf=1 and sf=10 work:

- `MemoryMax=4G`
- `MemoryHigh=4G`
- no budget constant changes
- release binary built before entering the capped measurement scope

A query that needs more memory is an atlas result, not a reason to move the bar.

### Persisted Parquet only

The canonical sf=10 data source is the ADR 360 dataset:

- `--data-source parquet`
- `--data-dir target/tpch/data`

Do not use the old in-memory fixture path for the canonical sf=10 atlas. If a command would silently fall back to
in-memory fixtures, reject that run and record the harness gap.

### Per-query isolation

Run per query, not as one doomed all-query process. Each query gets its own fixed-cap `systemd-run` scope and output
directory. This prevents one cgroup kill or no-row failure from destroying the rest of the board.

DuckDB runs against the same Parquet files. Prefer per-query `--engines toron,duckdb` if the perf parent survives all
child failures cleanly; otherwise split Toron and DuckDB per query and merge the rows in the results doc.

### Live-only, never cached

No `matrix` or `correctness-matrix` row counts as sf=10 evidence. Those commands are cache renders and previously hid
the q08/q09 live gap. ADR 366 uses live perf rows and live correctness rows only. q06 is the latest proof: a prior
sf=10 green assumption became a live red once ADR 365 made persisted correctness available.

### Correctness provenance

For Toron pass rows, run live correctness through ADR 365's persisted correctness path. If a query still lacks a usable
persisted correctness route, record that as an atlas blocker rather than pretending a cached or sf=0.01 correctness
result proves sf=10 correctness.

DuckDB row/schema sanity stays in the perf rail. Full cross-engine value-digest equivalence remains a follow-up unless
it already exists by implementation time.

## Phase 0 - runner and artifact sanity

1. Verify the ADR 360 dataset exists and matches the manifest:
   `target/tpch/data/strict/sf10/manifest.json`.
2. Build the release binary outside the capped runtime scope.
3. Smoke the fixed-cap runner on known anchors:
   - q06: expected named `memory_budget_exhausted` at `scan output accumulator` (confirmed seed red).
   - q14: expected Toron pass.
   - q21: expected named `spill_budget_exhausted` at the semi/anti existence-set spill writer unless later work changed
     it.
   - q17 correctness: expected `q17_sf10_oracle_policy_gap`, not a cached lower-scale correctness row.
4. Confirm output rows include engine, query, sf, mode, status, reason, diagnostic_message, execute_ms, peak RSS, spill
   bytes, plan fingerprint, and flags.
5. Decide whether a combined `--engines toron,duckdb` per-query invocation is robust enough. If not, use one invocation
   per query per engine.

## Phase 1 - full live board

Run all 22 queries at sf=10, strict mode, release, persisted Parquet, fixed 4 GB. Start with `--runs 1 --warmup 0` for
the atlas. Increase runs only after the first board confirms runtime is tractable.

For each query, record:

- Toron status and reason.
- diagnostic producer and requested/available bytes for red rows.
- peak RSS and spill bytes.
- execute_ms and plan fingerprint.
- DuckDB status, execute_ms, peak RSS, version, flags.
- whether Toron correctness was checked live at sf=10, and if not, why.

## Classification taxonomy

Classify each Toron red row into one of:

- scan-output materialization
- join-output materialization
- exchange-output materialization
- upstream child materialization
- spill-volume
- spill-resident index / writer resident state
- semi/anti existence-set spill
- aggregate/group-state memory
- time wall
- correctness drift
- no-row cgroup kill, with last known phase if available
- harness/correctness-oracle gap

Every classification must cite live evidence: the perf row, `diagnostic_message`, producer/bytes, or an explicit no-row
artifact. Structural predictions can size the next probe, but cannot classify an unreached wall as measured.

## Acceptance

- All 22 queries measured live at sf=10 under fixed 4 GB from persisted Parquet, or every missing row has an explicit
  runner/harness reason.
- DuckDB included against the same persisted files, with flags recorded rather than hidden.
- Every Toron red is named by producer/dimension/bytes where telemetry exists, or explicitly marked no-row with the
  last known phase.
- Toron pass rows have live sf=10 correctness provenance through ADR 365's persisted correctness path.
- The results doc records the common-wall ranking: which producers recur across the board and therefore deserve the
  next high-leverage ADR.
- No engine behavior change, no budget/cap bump, no cached matrix evidence.

## Guardrails

- Measure first. Do not fold a fix into this ADR.
- Use persisted Parquet only for canonical sf=10 data.
- Keep per-query isolation so a single OOM cannot erase the board.
- Preserve q21's lesson: a named next wall is success for the atlas; do not force green.
- Keep DuckDB as a reference, but do not let DuckDB instability block Toron wall classification.

## Forward arc

ADR 366 names the shared sf=10 walls. The next ADR should target the highest-leverage repeated producer, not the next
q21-only term unless the atlas proves q21's term is the common wall. Likely candidates are existence-set spill volume,
scan/exchange materialization, or spill-resident state, but the atlas decides.

## Why toron-home

This is the TPC-H gauge measuring `crates/toron` under the perf rail. No Wendelon or distributed storage surface is in
scope.
