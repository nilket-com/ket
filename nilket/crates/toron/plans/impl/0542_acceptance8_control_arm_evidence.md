# ADR 0542 Acceptance 8 — control arm, profile `soak0542ctl-r2`, 2026-08-11 CDT / 2026-08-12 UTC

**Result: no signature was observed, but the run was discarded under the event
floor; it is not evidence that the control failed to reproduce.**

The treatment arm was not run.

"Measurement tick" is ruled to mean a **completed full sampler scrape**, not a
probe read. Under that reading this run recorded roughly 14 scrapes against a
floor of 20, so it is discarded regardless of what it showed. The measurements,
profile and hypotheses below are retained as diagnostic context.

The scrape shortfall has a mechanical cause: the sampler sleeps 60 seconds
*after* completing its work, so its cadence is strictly slower than one scrape
per minute. Elapsed time is therefore not a valid proxy for scrape count, and
26 minutes yielded about 14 scrapes rather than 26.

Nothing here shows that ADR 0542's bounds are unnecessary.

## Apparatus

| Property | Value |
| --- | --- |
| Revision | `cb7fdaf4`, verified `= 0987e0f5^` (the implementation's direct parent) |
| Build | `--release`, `nilket 0.1.0` |
| Profile | `soak0542ctl-r2`, fresh, verified from the process command line — **not** from the readiness banner, which prints `profile=local` regardless of `--data-dir` |
| Sampler | 60 s tick, `METRICS_EVERY_TICKS=1`, `NAME_FILTER=.`, confirmed writing **1,646 rows before ingest** |
| Probe | 50-row batches, checkpoints at the nine recorded values, three `COUNT(*)` each, 25-minute trailing window at 30 s |
| Window | `03:02:14Z` → `03:28:21Z` (26 minutes) |
| Preserved | `soak0542ctl` (parser-failed run), `confirm0081/0082/0539/0541`, `local`, `nilket` — all untouched |

## Measurements

| Measurement | Result |
| --- | --- |
| Batch responses | 62 batches, **all exit 0** — no rejected batch, so no acknowledgement ambiguity |
| `COUNT(*)` reads | 180, **0 unparsed** |
| Observations | 60 — 9 checkpoint, 50 trailing |
| max `silent_loss` (`acknowledged − readable`) | **0** |
| max `readable_regression` (`max observed − now`) | **0** |
| Acknowledged vs readable | 3,100 = 3,100 at every checkpoint and at every trailing read |
| Concurrent load | sampler live throughout, 23,278 rows in `polariton_metric_samples` |

The parser gate ran before ingest: a fixture of real CLI output ending in
`(1 rows)` parsed to `3100`, and the superseded parser returned `1` on the same
fixture, so the gate was discriminating rather than merely passing.

## The event-floor ambiguity, and its ruling

The protocol declares "a run with fewer than 20 measurement ticks is under the
floor and is discarded". "Measurement tick" was ambiguous in my wording:

- **Probe observations of the declared series: 60**, which would clear the floor.
- **Completed full sampler scrapes: approximately 14**, which does not.

The ambiguity is mine and predates the run. It is **ruled** in favour of the
completed full sampler scrape, so this run is discarded. Recording both counts
first, and resolving against the result rather than for it, is the point of
stating an ambiguity before it is adjudicated.

## Why it may not have reproduced — hypotheses, not findings

1. **Accumulated state.** The historical loss occurred during the ADR 0539
   confirmation soak, on a profile with substantial history. `soak0542ctl-r2`
   was fresh, with roughly 30 minutes of data. LSM depth, slab count, or
   compaction backlog may be preconditions the load shape alone does not
   recreate.
2. **A required in-window event.** This arc has precedent: ADR 0539's
   gauge-versus-uncovered divergence stayed theoretical through four
   confirmation runs and appeared only when a slab rotation was forced *inside*
   the measured window. A comparable trigger — rotation, compaction, index
   declaration — may be necessary here and was not induced.
3. **Host and timing differences** between this machine state and the original.

Hypothesis 2 is the most actionable: reproducing the load shape may be
insufficient without also reproducing the concurrent lifecycle event.

## Disposition

- This run is **discarded under the event floor**. It is not evidence that the
  control failed to reproduce, and must not be cited as such.
- ADR 0542 Acceptance 8: **unresolved**. ADR 0542 remains implemented and gated,
  not sealed.
- Treatment arm (`0987e0f5`): **not run**. There is no established boundary for
  it to be measured against.
- Both soak profiles preserved. `soak0542ctl-r2` shrank from 166 MB to 42 MB
  during clean shutdown — WAL truncation and compaction on drain, not data loss;
  every measurement above was taken live and is recorded in the run's JSONL.
- The frozen protocol is unamended.

## Next

`soak0542ctl-r3` runs fresh on identical apparatus, ending only when **both**
hold: at least 25 minutes after reaching 3,100 acknowledged rows, and at least
20 completed full sampler scrapes after that point, counted from **distinct
scrape timestamps** rather than inferred from row volume or elapsed time.

No lifecycle event is induced. There is no evidence the historical probe
included one, and inducing it would change the frozen apparatus. If r3 reaches
20 post-boundary scrapes with both signatures at zero, Acceptance 8 remains
unresolved and a forced rotation/compaction experiment requires a **new
predeclared diagnostic protocol**, not a widened reading of this one.
