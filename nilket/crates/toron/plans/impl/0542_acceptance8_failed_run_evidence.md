# ADR 0542 Acceptance 8 — failed run, profile `soak0542ctl`, 2026-08-11

**This run produced no evidence in either direction.** It is recorded because it
produced a *confirmatory* false positive, which is the failure mode most likely
to be believed. The frozen protocol (`e6ff0986`) is not amended by this record.

## What failed

The probe's `COUNT(*)` reader parsed the client's output with:

```sh
grep -oE '[0-9]+' | tail -1
```

Client output ends with a row-count line:

```text
+-------+
| count |
+-------+
| 3100  |
+-------+
(1 rows)
```

The last integer in every response is therefore the `1` from `(1 rows)`, not the
count. Every read returned 1, for the whole run.

## What it claimed

```text
SIGNATURE at attempted=3100 acked=3100 readable=1 silent=3099 regression=0
```

Read literally, the control arm reproduced the historical boundary
catastrophically — 3,099 rows acknowledged and unreadable. That is the result
the control arm was *expected* to produce, so the artifact agreed with the
hypothesis. An instrument error that disagrees gets investigated; one that
agrees gets believed. This one would have discharged Acceptance 8's control arm
on a `tail -1`.

## What was actually true

Re-read with a corrected parser (`^\| *[0-9]+ *\|$`, first data row):

| Observation | Value |
| --- | --- |
| `budget_probe3` acknowledged | 3,100 |
| `budget_probe3` readable | 3,100, three agreeing reads |
| Batch responses | 62 batches, **all exit 0** — no rejected batch, so no acknowledgement ambiguity |
| Follow-up reads | 20 reads over 10 minutes, `readable = 3100`, `readable_regression = 0` throughout |
| Concurrent load | sampler live throughout, 32,430 rows in `polariton_metric_samples` |

So at the end of this run the deployment sat at exactly the state the historical
run occupied *before* its later read returned 2,950.

**This is diagnostic context, not Acceptance 8 evidence.** The declared
per-checkpoint `rows_readable` series was never validly measured, so this run
supports neither "the control reproduces" nor "the control fails to reproduce".

## Disposition

- Profile `soak0542ctl` is **preserved unchanged** as the record of this run. It
  is not deleted and not reused.
- The clean control arm runs as `soak0542ctl-r2`.
- The parser is fixed and now gated: a fixture of real CLI output ending in
  `(1 rows)` must parse to `3100`, and the gate additionally asserts the **old**
  parser returns `1` on the same fixture, so the gate is discriminating rather
  than merely passing.
- The trailing window stays at 25 minutes. The historical latency is recoverable
  from session history: 3,100 passed at `06:07:50.198Z` and the 2,950 result
  landed at `06:08:21.008Z` — a regression appearing within **30.81 seconds**.
  25 minutes covers that with wide margin and supplies the required 20
  one-minute ticks.

## Carried forward

Two profile-targeting near-misses occurred before ingest and are worth stating,
because both would have produced confident wrong results:

1. The probe initially exported `NILKET_DATA_DIR`, which does not exist.
   `resolve_profile_root` reads an explicit `--data-dir`, else
   `XDG_DATA_HOME/nilket/local`, else `$HOME/.local/share/nilket/local`. The
   export was inert and every statement would have written into the preserved
   `local` profile. `nilket toron sql` is a **network client**; there is no
   client-side data directory, and the profile under test is whichever
   deployment holds the endpoint.
2. Startup prints `LOCAL_READY v=1 profile=local` even when `--data-dir` points
   elsewhere. That is a mode label, not the directory. **The readiness banner is
   not evidence of which profile is in use.** Confirm from the process command
   line and the target directory's contents.
