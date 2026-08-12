# ADR 0542 Acceptance 8 — soak protocol

Predeclared and committed before any observation. Nothing here may be revised
after a run begins; a run that cannot satisfy a declared condition is a **failed
run**, recorded as such, not a reason to relax the condition.

## What Acceptance 8 requires

> The soak's insert-batch boundary reproduces before the change and completes
> after it, under concurrent sampler-rate load.

A **paired** claim. A single healthy run is consistent both with "the bounds
fixed it" and with "the boundary does not reproduce on this host under this
load" — the two conclusions this acceptance exists to separate.

| Arm | Revision | Expectation |
| --- | --- | --- |
| Control | `cb7fdaf4` — the direct parent of the implementation | the boundary **reproduces** |
| Treatment | `0987e0f5` — the ADR 0542 implementation | ingest **completes beyond** the control boundary |
| Validation (optional) | current `main` | confirms the result survives 47+ later commits; **not** a substitute for the treatment arm |

Control and treatment differ by exactly one commit, which is what makes the
comparison causal. Current `main` carries ADRs 0083/0546/0547 and much else, so
it cannot isolate 0542's change and is recorded separately if run at all.

**If the control arm does not reproduce the boundary, Acceptance 8 is not
discharged.** That is reported as an honest red — the historical finding failed
to reproduce under this apparatus — and is never converted into a pass by
pointing at a healthy treatment arm.

## Load, reproducing the historical shape

Two concurrent actors, identical across arms.

**1. Full Polariton sampler**

| Setting | Value |
| --- | --- |
| Tick | 60 s |
| `POLARITON_METRICS_NAME_FILTER` | `.` |
| `POLARITON_TELEMETRY_METRICS_EVERY_TICKS` | `1` |
| Volume | ~2,300 metric rows per scrape |
| Batch | 50 rows per `INSERT` |

**2. Burst boundary probe**

```sql
CREATE TABLE budget_probe3 (id BIGINT, pad TEXT)
```

Sequential `(id, 'x')`, 50 rows per `INSERT`, no delay beyond CLI latency.
Checkpoints at **2,200 / 2,400 / 2,600 / 2,800 / 2,850 / 2,900 / 2,950 / 3,000 /
3,100** rows, three `COUNT(*)` reads at each.

**Every batch response is recorded** — exit status and output. The original
harness discarded both, which is why the historical run cannot say which batch
was acknowledged. That gap is the reason this protocol exists in its present
form.

## The discriminating axes

The historical failure is *silent*: batches acknowledged, rows not readable. Two
signatures, both declared:

```text
silent_loss        = rows_acknowledged − rows_readable
readable_regression = max(rows_readable observed) − rows_readable now
```

`rows_attempted` is recorded **separately** and is never used in either
signature. `rows_attempted − rows_readable` would classify an explicitly
rejected batch as silent loss, which is the opposite of the phenomenon being
measured.

`readable_regression` is declared because it is what actually happened: three
reads at 3,100 all agreed, and a **later** read returned 2,950. Readable count
was non-monotone. A detector that only compares acknowledged against readable at
the moment of insert would have missed the recorded event; any positive
`readable_regression` is the signature.

## Series, and which arm can carry them

Harness-derived, **both arms**, initialized to zero before ingest:

- `rows_attempted`, `rows_acknowledged`, `rows_readable`
- the two derived signatures above
- per-batch response status and text

Treatment arm **only**, captured from mutation responses and resolution — not
from telemetry, which does not expose them:

- stop cause: deadline versus no-progress
- commit knowledge: `committed`, `known-not-committed`, `indeterminate`

**The control revision predates these fields and cannot emit them. Their absence
on the control arm is expected and is not a failed run.** The "a missing series
is a failed run" rule applies only to series the arm is capable of producing —
that rule exists to stop absence being read as zero, not to require a revision
to report fields it does not have.

Detached-work outstanding is **removed from the live declaration**. No metric
exposes it, and Acceptance 7 is already discharged by fixture. Adding
instrumentation is a prerequisite to measuring it live, not something to assume
mid-run.

`wendelon_data_ring_write_outcome_total` exists on both arms and is recorded as
context, not as a signature.

## Declared outcomes

**Control passes** when either signature goes positive without a surfaced error.
The row count at first positive value is recorded as `B_ctl`, together with the
checkpoint at which it appeared and the responses of the batches spanning it.

**Treatment passes** when, under identical load, ingest proceeds to at least
**3 × B_ctl** with both signatures at zero throughout, and every write that
stopped reports a typed cause paired with a commit-knowledge value. A stop
reporting one axis without the other fails Acceptance 1 and therefore this run.

Both arms report the control-arm median and the event floor. A run with fewer
than 20 measurement ticks is under the floor and is discarded regardless of what
it showed.

## Explicitly not seal criteria

Quantile exercise is **supplemental** and only meaningful on the optional
current-`main` arm, since `0987e0f5` predates ADR 546. If that arm is run,
record whether the 1,024-population cap is reached and refuses typed, memory
charged per admitted population against the 5,601-slot accounting, and that one
execution's fragments agree while separate executions may differ within the rank
bound.

**None of this alters Acceptance 8's pass/fail.** A quantile misbehaviour is a
new finding with its own disposition — neither a soak failure nor a soak pass.

## Stop conditions

- Sampler not confirmed writing before ingest → abort, failed run.
- A harness-derived series absent or uninitialized at first tick → abort, failed
  run.
- Control fails to reproduce the boundary → Acceptance 8 not discharged; report
  and stop, without claiming the treatment arm.
- Fewer than 20 measurement ticks → discard.
- A live local deployment on the fixed port wedges preflight; confirm none is
  running before either arm.

## Evidence to preserve

Both profiles unchanged (`soak0542ctl`, `soak0542trt`, plus `soak0542main` if
the validation arm runs), both build hashes, every batch response, the full
per-tick series, `B_ctl` with its checkpoint, and raw sampler output. The
receipt states the control-arm median and the event floor beside every claim.
Existing profiles — `confirm0082`, `confirm0081`, `confirm0541`, `confirm0539`,
`local`, `nilket` — are untouched.
