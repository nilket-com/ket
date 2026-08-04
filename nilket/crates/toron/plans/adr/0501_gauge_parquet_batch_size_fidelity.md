# ADR 501 - gauge parquet batch size: a Parquet-optimal proxy for production's large-batch contract

ADR 500 established that the tpch gauge reads parquet in the reader's default **1,024-row batches**,
imposing per-batch poll/span/adapter framework that **no production scan path pays** — production
backends emit one whole morsel-sliced batch (`scan_in_memory_batch` → `ScanStream::from_batches(…,
vec![batch])`), and distributed task inputs inherit that large shape. The m04 sweep showed this
inflates measured execute by ~27% at 1,024 vs the parquet optimum. This ADR decides whether and how
to correct the gauge coordinate — **it is a benchmark-fidelity change, not an engine change.**

## Grounded boundary

- The overhead is a **gauge artifact**: it exists only on the `ParquetHarnessScan` path, via the
  parquet reader default. Production emits large batches and pays ~0 per-batch framework.
- The m04 batch-size sweep (ADR 500) is a **U-curve**: 8,192 is the parquet sweet spot (−27% vs
  1,024); 65,536 is *worse* (1,200 ms) because parquet's own decode degrades at large batches. So the
  gauge **cannot** replicate production's one-giant-batch — parquet has its own cost there.
- Therefore ~8,192 is a *proxy*, not production-equal: the closest a parquet reader gets to the
  production large-batch contract while staying on parquet's efficient decode range.

## Decision (scope of the change)

Adopt a **single ladder-wide** parquet batch size (~8,192) as a **dedicated gauge coordinate change
across the full primitive ladder**, not an m04-only follow-up. It is labeled exactly:
**"Parquet-optimal proxy for production's large-batch contract"** — explicitly *not* production-equal.
The `TORON_TPCH_PARQUET_BATCH_SIZE` env probe (preserved by ADR 500) is the coordinate knob; the
change sets its default. **No engine change; no per-query special-casing; gauge-only.**

## Gate

**Phase 1 — confirm 8,192 is a sound ladder-wide coordinate.** One coordinate must serve all rungs
for comparability, so before committing 8,192, sweep the *full* ladder (not just m04) across a few
sizes (e.g. 1,024 / 4,096 / 8,192 / 16,384) and confirm 8,192 does not **pessimize any rung** vs the
current 1,024. Aggregates (m04-shaped) benefit most; bare scans (m00/m07/m08) and joins (m06) may
have different curves — 8,192 must be at least neutral for each, or the coordinate is not sound.

**Phase 2 — re-pin with an explicit before/after.** Re-pin the ladder at the new coordinate and
publish **old (1,024) vs new (8,192) per rung**. Any ranking movement is a **gauge-coordinate effect,
not an engine change**, and must be presented as such — no rung's re-ranking is treated as a real
engine delta. Correctness (digest, output cardinality, spill) is invariant per rung by construction
(batch size does not change results).

## Non-goals

- Not production-equal — a Parquet-optimal *proxy* only.
- Not an engine optimization and not m04-scoped; the m04 residual is resolved (ADR 500, gauge artifact).
- Not a per-primitive batch-size tuning exercise — one coordinate for ladder comparability.

## Forward

Once re-pinned, the ladder's absolute numbers shift down for framework-heavy rungs; the DuckDB/Polars
references (which read the same parquet) should be swept at the same coordinate so the comparison
stays apples-to-apples. The env probe remains available for future fidelity sweeps.
