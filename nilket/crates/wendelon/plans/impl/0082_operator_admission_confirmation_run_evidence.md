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
| Slab rotation | **yes** — 2 slabs; the ADR 0081 declaration seal forced it |
| Index engagement | `query_plans_total` max **54.0** — the index was chosen 54 times |
| Backfill | `entries_indexed_total` max 2,473; `segments_built_total` 1 |
| Deployment warnings | 1, the expected `dev-insecure` KEK custody notice |

Slab rotation deserves note: the previous run never rotated at 45,450 rows. Here it came free,
because declaration seals the active slab as one admission cut.

## Corrections recorded

- **Index engagement was misjudged from wall clock.** An indexed lookup measured 4.10 s against
  a 4.50 s full scan, and this was initially read as the index not being engaged.
  `query_plans_total = 54` refutes that. The counter is authoritative; the timing comparison
  was not evidence.
- **An early lookup fixture matched nothing.** The window's per-tick lookup used a metric name
  that does not exist, so it returned 0 rows for all 51 ticks. The real indexed lookup
  (`name = 'wendelon_audit_archived_bytes_total'`) returned 55 rows. The per-tick figures prove
  sustained query load, not index hits; `query_plans_total` proves the hits.

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
- ADR 0541 Acceptance 5 — **passed** (index created explicitly; series present before the
  window opened; ingest and lookups sustained across rotation and 51 scrapes; all declared
  measurements recorded).
- ADR 0539 Acceptance 6 — **met** by this rerun.

Remaining for ADR 0082: the live Data Ring negative, if a surface is ever built for it.
Remaining for ADR 0081: restart-survival and cross-domain rejection are unit-proven, not
live-proven.
