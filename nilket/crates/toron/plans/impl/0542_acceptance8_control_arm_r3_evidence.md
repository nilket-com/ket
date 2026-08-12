# ADR 0542 Acceptance 8 — control arm r3, profile `soak0542ctl-r3`, 2026-08-11 CDT / 2026-08-12 UTC

**Result: the control arm ran to the declared event floor and observed no
signature. Acceptance 8 remains UNRESOLVED. The treatment arm was not run and no
lifecycle event was induced.**

This run satisfies the event floor under its ruled definition, so unlike
`soak0542ctl-r2` it is not discarded. It is a valid negative: under this
apparatus, the historical insert-batch boundary did not appear.

A valid negative is **not** evidence that the defect does not exist, and it does
not discharge Acceptance 8. Acceptance 8 requires the boundary to reproduce
before the change; it did not, so there is nothing for the treatment arm to be
measured against.

## Apparatus

Identical to r2. No parameter was changed except the termination rule.

| Property | Value |
| --- | --- |
| Revision | `cb7fdaf4`, verified `= 0987e0f5^` |
| Build | `--release`, `nilket 0.1.0` |
| Profile | `soak0542ctl-r3`, fresh, verified from the process command line — not the readiness banner |
| Sampler | 60 s sleep-after-work, `METRICS_EVERY_TICKS=1`, `NAME_FILTER=.`, gated at **1,646 rows written before ingest** |
| Probe | 50-row batches, nine recorded checkpoints, three `COUNT(*)` each |
| Window | `03:39:13Z` → `04:04:43Z` |

## Termination, per the r3 ruling

Both conditions required, checked every 30 s:

| Condition | Required | Achieved |
| --- | --- | --- |
| Elapsed after 3,100 acknowledged | ≥ 1,500 s | **1,511 s** |
| Completed full sampler scrapes after 3,100 | ≥ 20 | **23** |

Scrapes were counted from **distinct scrape timestamps** —
`COUNT(DISTINCT sampled_at) WHERE sampled_at > watermark`, with the watermark
being `MAX(sampled_at)` captured at the instant the boundary was reached
(`1786505948`). Neither row volume nor elapsed time was used as a proxy, because
the sampler sleeps 60 s *after* completing its work and its cadence is therefore
strictly slower than one scrape per minute.

## Measurements

| Measurement | Result |
| --- | --- |
| Batch responses | 62 batches, **all exit 0** — no rejected batch, no acknowledgement ambiguity |
| `COUNT(*)` reads | 171, **0 unparsed** |
| Observations | 57 |
| Termination checks | 47 |
| max `silent_loss` | **0** |
| max `readable_regression` | **0** |
| Observations with any positive signature | **0** |
| Checkpoints | 2200, 2400, 2600, 2800, 2850, 2900, 2950, 3000, 3100 — acknowledged equals readable at every one |
| Aborts | none; neither the stuck-counter nor the hard-cap guard fired |

## Two apparatus defects caught by gating, before ingest

Both would have produced a confident wrong result had they not been gated live.

1. **The scrape-count query was invalid.**
   `SELECT COUNT(*) FROM (SELECT sampled_at FROM … GROUP BY sampled_at) s` is
   refused: *"GROUP BY requires at least one aggregate call"*. The termination
   counter would have read 0 for the whole run, and the run would have burned an
   hour before the stuck-counter guard fired.
2. **Its obvious rewrite fails worse, and is a Toron defect.**
   `SELECT COUNT(*) FROM (SELECT sampled_at, COUNT(*) AS n FROM … GROUP BY
   sampled_at) s` returns an **internal** error:
   `compute error: Invalid argument error: must either specify a row count or at
   least one column`. A planner refusal on an unsupported shape is fine; an
   internal compute error on a legal one is not. Recorded here as a repro; it is
   unrelated to ADR 0542 and is not pursued in this cut.

`COUNT(DISTINCT sampled_at)` works and is what r3 used.

## Disposition

- **Acceptance 8: unresolved.** ADR 0542 remains implemented and gated, not
  sealed.
- Treatment arm (`0987e0f5`): **not run**.
- No rotation, compaction, or index-declaration event was induced. There is no
  evidence the historical probe included one, and inducing it would change the
  frozen apparatus.
- Profiles preserved: `soak0542ctl` (parser-failed), `soak0542ctl-r2`
  (discarded under the floor), `soak0542ctl-r3` (this run). Pre-existing
  profiles untouched; `local` remains 2026-08-08 00:45.

## What a next attempt would require

A forced rotation/compaction experiment needs a **new predeclared diagnostic
protocol**, not a widened reading of `e6ff0986`. The standing hypotheses — that
the historical loss required accumulated LSM state, or a concurrent lifecycle
event of the kind that made ADR 0539's divergence appear only when a rotation
was forced inside the measured window — remain untested and are recorded as
hypotheses, not findings.
