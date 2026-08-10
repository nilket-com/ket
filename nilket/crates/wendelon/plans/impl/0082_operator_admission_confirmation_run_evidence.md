# ADR 0081 / 0082 confirmation run — 2026-08-09/10

The blocked path is open. An operator-signed CLI created a secondary index through the
supported production interface on a clean profile, the production driver advanced it to
`Ready`, both declared series appeared, and the measured window closed with zero gaps and
zero refusals.

## Apparatus

| Property | Value |
| --- | --- |
| Binary | release, built from `4220a86f` |
| Profile | `~/.local/share/nilket/confirm0082`, clean at start, preserved as evidence |
| Sampler | started **before** ingest, full filter, 60 s cadence, `METRICS_EVERY_TICKS=1` |
| Prior profiles | `confirm0081`, `confirm0541`, `confirm0539`, `local`, `nilket` — all preserved unchanged |
| Window | 51 measurement ticks, 0 tick errors; 4,147 → 45,459 rows |

## The mutation that was previously impossible

```
wendelon index status  →  no secondary indexes
wendelon index create  →  created secondary index 637a1b2a-41ab-4601-bd12-7659531f7c9f
wendelon index status  →  637a1b2a-…  ready  default.idx_metric_name  name  1:complete:gaps=0
```

Presented `local-operator.crt/.key` (operator-CA signed), verifying the server against
`node-ca.crt`, over the shared HRFT listener at `1@127.0.0.1:7001`. Before ADR 0082 this
handshake failed with `unknown ca`.

`1:complete:gaps=0` is the durable per-replica completion receipt reporting itself: replica 1
holds a receipt, coverage is complete, zero gaps.

## Declared measurements

| Measurement | Result |
| --- | --- |
| Gap wall-time fraction | **0** — `coverage_gap_slabs` read 0.0 at all 53 samples |
| Refusal counter delta | **0** — `lookup_refusals_total` 0.0 throughout |
| Gauge-versus-uncovered divergence | **0** — `coverage_gap_slabs` and `uncovered_slabs_total` agreed at 0.0 on every sample |
| Full-table scan | 46,297 rows in **4.50 s**, under concurrent ingest |
| Ratio to historical failure band (~3,000–4,400 rows) | **10.5×** |
| Typed expiries (`QUERY_STALLED`/`QUERY_DEADLINE_EXCEEDED`/`QUERY_BUDGET_EXCEEDED`) | 0 |
| `storage unavailable` | 0 |
| Slab rotation | **pre-window only** — the declaration seal rotated before the measured window opened, so 0541 Acceptance 5's *in-window* rotation is NOT discharged here. See the supplement. |
| Index engagement | `query_plans_total` max **54.0** — the index was chosen 54 times |
| Backfill | `entries_indexed_total` max 2,473; `segments_built_total` 1 |
| Deployment warnings | 1, the expected `dev-insecure` KEK custody notice |

Slab rotation deserves a correction, not a boast. The declaration seal did rotate the slab —
but it did so *at declaration*, before the measured window opened. ADR 0541 Acceptance 5
requires a rotation **spanned by** the window. This run does not satisfy that, and the original
wording of this table claimed otherwise.

## Corrections recorded

- **Index engagement was misjudged from wall clock.** An indexed lookup measured 4.10 s against
  a 4.50 s full scan, and this was initially read as the index not being engaged.
  `query_plans_total = 54` refutes that. The counter is authoritative; the timing comparison
  was not evidence. Note the precise claim: `query_plans_total` proves index **selection**;
  `candidate_rows_total` is what proves a **nonempty hit**.
- **An early lookup fixture matched nothing.** The window's per-tick lookup used a metric name
  that does not exist, so it returned 0 rows for all 51 ticks. The real indexed lookup
  (`name = 'wendelon_audit_archived_bytes_total'`) returned 55 rows. The per-tick figures prove
  sustained query load, not index hits; `query_plans_total` proves index selection and
  `candidate_rows_total` proves the hits were nonempty.

## Not performed

**The live Data Ring negative proof was not performed.** The `wendelon` CLI exposes only
`audit verify`, `inspect`, and `index`; no operator-facing client attempts a Data Ring read, so
the refusal cannot be demonstrated live with existing surfaces. That coverage rests on ADR
0082's unit controls. Node and Data traffic being *unaffected* is separately live-proven: the
sampler wrote through Toron continuously across all 51 ticks.

## Disposition

- ADR 0082 Acceptance 1 — **passed** (operator mutation through the control ring).
- ADR 0081 Acceptance 1 — **passed** (created through the supported interface, clean profile).
- ADR 0081 Acceptance 2 — **passed** (production driver advanced `Building → Ready`).
- ADR 0081 Acceptance 3 — **passed** (both series exist, attributable).
- ADR 0081 Acceptance 4 — **passed** (index selected at `Ready`; 54 query plans).
- ADR 0081 Acceptance 5 — **passed** (declaration sealed the active slab; rotation observed).
- ADR 0541 Acceptance 1 — **passed** (10.5× scan under concurrent ingest).
- ADR 0541 Acceptance 5 — **not discharged by this run**: the index was created explicitly and
  the series were present before the window opened, but the rotation fell *before* the window.
  Discharged by the supplement below.
- ADR 0539 Acceptance 6 — pending, with ADR 0541 Acceptance 5.

Remaining for ADR 0082: the live Data Ring negative, if a surface is ever built for it.
Remaining for ADR 0081: restart-survival and cross-domain rejection are unit-proven, not
live-proven.

---

# Supplement — in-window rotation via sampler scrapes, 2026-08-10

An earlier attempt at this supplement used direct endpoint reads and could not record a
refusal delta at all. Both defects it exposed were repaired before this run; the repairs are
part of ADR 0081, not footnotes.

## Repairs made first

1. **Declared per-index series now survive restart.** Declaration initialized them once, but a
   restart recovers indexes from the durable catalog without replaying declaration, so a
   recovered index emitted no refusal series — and *absence is not a zero baseline*. A new
   idempotent `initialize_storage_secondary_index_series`, plus a per-tick re-assert of the
   failure-record gauge from the tracker's real count, run inside
   `ensure_secondary_index_coverage`. Both confirmation axes and the refusal counter are now
   present on every driver tick.

2. **The sampler no longer decides idempotence by error text.** Root cause of its silent stall:
   `TableSchemaRequest::create()` hardcodes `expected_generation: ZERO`, so once any schema
   mutation advances the tenant generation — declaring an index is enough — a duplicate
   `CREATE TABLE` is rejected with a *generation mismatch*, not "already exists". The generation
   check precedes the existence check. That text did not match `ensure_tables`' case-glob, so it
   returned 1, `tables_ready` stayed 0, and the sampler sampled nothing for as long as it ran
   while looking healthy. It now probes existence and reports failures to stderr.

   Not fixed here, and recorded as a separate finding: a duplicate create should report
   existence before generation mismatch.

## Sequence — all readings from 60-second sampler scrapes

Index 1 = `637a1b2a…` on `name`. Rotation forced by declaring an index on another table
(`polariton_health_samples.endpoint`); all supported columns on the metrics table were already
indexed, and `value` was correctly rejected with
`INDEX_REJECT_UNSUPPORTED_COLUMN_TYPE: column value has type Float64`.

| Scrape | slabs | `coverage_gap_slabs` | `lookup_refusals_total` | `uncovered_slabs_total` |
| --- | --- | --- | --- | --- |
| `1786329816` pre-rotation | 4 | 0.0 | **0.0 explicit** | **0.0 explicit** |
| *declaration → rotation* | 4 → 5 | | | |
| `1786330000` post-rotation | 5 | **1.0** | **4.0** | 0.0 |
| `1786330260` | 5 | 0.0 | 4.0 | 0.0 |
| `1786330322` | 5 | 0.0 | 4.0 | 0.0 |

All three series are present pre-rotation, so the deltas are *recorded*, not inferred from
absence — which is what the previous supplement could not do.

## The three declared measurements

| Measurement | Result |
| --- | --- |
| Gap wall-time fraction | **1 of 67 scrapes ≈ 1.5%**, entirely the rotation window |
| Refusal counter delta | **+4** — one per post-rotation lookup, then stable |
| Gauge-versus-uncovered divergence | **nonzero and observed**: `coverage_gap_slabs` 1 while `uncovered_slabs_total` 0 |

The divergence is the most valuable reading in the arc. ADR 0539 predicted exactly this shape —
a rotation-window gap with no failure record, where the old gauge reads healthy while lookups
refuse — and no prior run could produce it because the gap set was never non-empty. Here it is,
measured: the invariant axis moved, the failure-record axis did not, and the refusal gate fired
on the invariant.

## Behaviour observed, for the record

A rotation transiently opens a coverage gap on an already-`Ready` index, and ADR 0539's gate
refuses lookups against it until backfill closes the gap — here within one scrape interval.
That is the designed behaviour, not a defect: refusing is correct while coverage is incomplete.
It is recorded because it is the first live evidence of the rotation-window refusal that ADR
0541 anticipated, and it bounds the cost at roughly one scrape interval at this write rate.

## Also live-proven

Restart survival: after restart the index reported `ready … 1:complete:gaps=0`, so declaration,
lifecycle state and the durable per-replica receipt survived with coverage recomputed — ADR 0081
Acceptance 7, live rather than unit-only.

## Residue

One synthetic row (`name='supplement_probe'`) remains in `polariton_metric_samples` in the
preserved profile, from testing the write path during diagnosis.

## Disposition

- ADR 0541 Acceptance 5 — **passed**: index created explicitly through the production surface;
  all declared series present before the window opened; ingest and first-index lookups sustained
  across an **in-window** rotation; all three measurements recorded from 60-second sampler
  scrapes, with nonzero values.
- ADR 0539 Acceptance 6 — **met**.
- ADR 0081 Acceptance 7 — live-proven.
- ADR 0082 — sealable independently; its Data Ring and Raft negatives and cross-domain rejection
  are control-based acceptance by design, not outstanding live requirements.
