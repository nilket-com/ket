# ADR 0541 confirmation run — 2026-08-09

Split verdict. **Acceptance 1 passed. Acceptance 5 failed** — not waived — on two independent
counts: no supported production path can declare the required secondary index, *and* the window
produced no slab rotation. Either alone is disqualifying. ADR 0541 remains unsealed and ADR
0539's Acceptance 6 remains unmet, both pending Wendelon ADR 0081.

## Apparatus

| Property | Value |
| --- | --- |
| Binary | built from `29dac7e9`, release, `nilket/target/release/nilket` |
| Profile | `~/.local/share/nilket/confirm0541`, clean at start, preserved as scan evidence |
| Sampler | started **before** ingest, full filter `POLARITON_METRICS_NAME_FILTER=.` |
| Scrape cadence | 60 s, with `METRICS_EVERY_TICKS=1` overriding the default `5` |
| Prior deployment | drained in order and preserved unchanged (`confirm0539`) |

The `METRICS_EVERY_TICKS` override is recorded because the default would have made
"multiple 60-second scrapes" mean five-minute ones.

## Acceptance 1 — passed

A full-table scan an order of magnitude past the historical failure size completed under
concurrent sampler-rate ingest.

| Measurement | Result |
| --- | --- |
| Rows at measurement | 45,450 |
| Historical failure band (old binary) | ~3,000–4,400 rows, `storage unavailable` at ~250 ms server time |
| Ratio | **10.3×** |
| `SELECT COUNT(*)` full scan | 4.50 s, completed |
| Filtered scan `WHERE value > 0` | 4.20 s, completed |
| `GROUP BY name` full scan, 12 groups | 4.30 s, completed |
| Concurrent ingest during scans | yes — sampler running throughout |
| Distinct 60-second scrapes | 56 |
| Typed expiries (`QUERY_STALLED`, `QUERY_DEADLINE_EXCEEDED`, `QUERY_BUDGET_EXCEEDED`) | 0 |
| `storage unavailable` occurrences | 0 |
| Deployment warnings | 1, the expected `dev-insecure` KEK custody notice |

An earlier reading at 4,127 rows completed in 0.50 s — inside the exact band where the old
binary failed. The milliseconds-denominated table-size cap is gone.

One recorded operator error: an initial `GROUP BY metric` returned `BindingError: unknown
column: metric` in 0.10 s. That was a wrong column name in the query, not an engine failure;
it is noted so the timing table is not read as containing a failure it does not contain.

## Acceptance 5 — failed

Two independent disqualifications. First, no supported production path declares a secondary
index, so the required gap-gauge series can never appear on a clean profile; per the run's own
rule, a missing required series is a failed run rather than an explanatory result. Second, the
window produced no slab rotation (see below). The substrate is not absent — the control-ring
carrier and durable lifecycle both exist — but no producer or lifecycle driver does.

| Surface searched | Result |
| --- | --- |
| Toron SQL | no index DDL; `SELECT/EXPLAIN/CREATE TABLE/INSERT/UPDATE/DELETE/DROP TABLE` only |
| Rune (primary interface) | Toron source contains zero references to `secondary_index` |
| Wendelon control ring | carrier **exists** — ADR 052 routes index mutations through `SchemaUpdateEnvelope` / `SchemaObjectKind::Index` — but no supported producer |
| Backfill / drop-GC drivers | `run_secondary_index_backfill_once`, `gc_secondary_index_segments_for_dropping_index`: **test callers only** |
| Wendelon CLI / typed client | no index verb, no client method |
| `SecondaryIndexDescriptor` | constructed only in benches and test modules |

Confirmed empirically: `SELECT COUNT(*) … WHERE name LIKE '%secondary_index%'` returned **0**
across all 56 scrapes.

Consequently unmeasured, and recorded as unmeasured rather than as zero: gap wall-time
fraction, refusal-counter delta, gauge-versus-uncovered divergence, and indexed lookups.

No test-only constructor or profile surgery was used to manufacture an index.

## Also not satisfied

**No slab rotation occurred.** The profile ended with a single slab
(`slab_0000000000000000.wnd`); 45,450 rows did not reach the rotation threshold. The protocol
required sustaining ingest across an induced rotation, so that element is unmet and is not
claimed. A future rerun needs either a longer window or an explicitly induced rotation.

## Provenance question — answered

Nothing in production creates a secondary index. Whatever the wedged soak profile held did
not arrive through a supported path. This closes the open question carried since the previous
confirmation run and is the evidence base for Wendelon ADR 0081.

## Disposition

- ADR 0541 Acceptance 1: **passed**, on the evidence above.
- ADR 0541 Acceptance 5: **failed** on two independent counts (no declaration path; no slab rotation), both recorded, not waived.
- ADR 0541: **not sealed**.
- ADR 0539 Acceptance 6: **unmet**.
- Next: Wendelon ADR 0081 supplies the declaration surface and lifecycle drivers; the index-capable rerun starts on
  a clean profile and must satisfy the complete 0539/0541 sequence.
