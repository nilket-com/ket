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

# Supplement — in-window rotation, 2026-08-10

Closes ADR 0541 Acceptance 5's in-window rotation condition, which the run above did not meet.
The preserved `confirm0082` profile was restarted rather than re-run for fifty minutes.

Measurements are read **directly from the metrics endpoint** with the operator certificate,
which is the authoritative source; the sampler table is only a copy of it.

## Restart survival (bonus, live)

After restart the first index reported `ready … 1:complete:gaps=0` — declaration, lifecycle
state and the durable per-replica receipt all survived, with coverage recomputed. ADR 0081
Acceptance 7 is therefore live-proven, not only unit-proven.

## Sequence and readings — index 1 (`637a1b2a…`, on `name`)

| Point | `query_plans_total{mode="indexed"}` | `candidate_rows_total` | `coverage_gap_slabs` | `uncovered_slabs_total` |
| --- | --- | --- | --- | --- |
| Pre-rotation baseline | 10 | 9 | 0 | 0 |
| After 4 pre-rotation lookups | **14** (+4) | **21** (+12) | 0 | 0 |
| *Second index created → slabs 2 → 3, in-window rotation* | | | | |
| After 4 post-rotation lookups | **18** (+4) | **249** (+228) | **0** | **0** |

`entries_indexed_total` for index 1 reached 45,505 with `segments_built_total` 1 and a single
backfill of 1.035 s, i.e. the index was rebuilt across the new slab and kept serving.

The delta shape is the point: `query_plans_total` moves exactly with the lookup count (+4 each
time), proving **selection**; `candidate_rows_total` moves by much more, proving the selections
produced **nonempty hits**. Neither number alone would show both.

## Index 1's three declared measurements, spanning the rotation

| Measurement | Result |
| --- | --- |
| Gap wall-time fraction | **0** — `coverage_gap_slabs` 0 before and after the rotation |
| Refusal counter delta | **0** |
| Gauge-versus-uncovered divergence | **0** — both axes 0, i.e. they agree |

## Two honest gaps in this supplement

- **Index 1 emits no `lookup_refusals_total` series**; index 2 emits it explicitly at 0. Both
  are consistent with zero refusals, but absence and an explicit zero are not the same
  observation, and the refusal figure for index 1 is inferred from absence rather than read.
- **The restarted sampler produced zero scrapes** in ~12 minutes despite the process looping,
  the metrics endpoint returning HTTP 200 with 250 KB, the ready endpoint returning 200, and a
  direct `INSERT` succeeding. `ensure_tables` tolerates the real "table already exists" text, so
  that is not the cause. Unexplained, and recorded as a defect to chase: it means the sampler
  cannot resume against an existing profile, which is why every prior run needed a fresh one.
  It did not affect this supplement, which read metrics directly.

One synthetic row (`name='supplement_probe'`) was inserted into `polariton_metric_samples`
while testing the write path; it remains in the preserved profile.

## Disposition after the supplement

- ADR 0541 Acceptance 5 — **passed**: index created explicitly through the production surface,
  series present before the window opened, ingest and first-index lookups sustained **across an
  in-window rotation**, all three declared measurements recorded.
- ADR 0539 Acceptance 6 — **met**.
- ADR 0081 Acceptance 7 — live-proven by the restart above.
