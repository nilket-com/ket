# ADR 451 implementation receipt

The durable journal, config-locked resume, and even-runs guard are implemented and
tested. The release-only paired cohort ran to completion as one detached parent and
produced the arc's first admissible, causal throughput result for the sf=4 committing
shape.

## Release cohort verdict (admissible)

Receipt: `target/tpch/adr449_cohort_sf4_release/latest_summary.jsonl`, `profile:
release`, journal 21/21 lines (1 warmup + 20 timed), `--sf 4 --runs 20 --warmup 1`.

- **Gate zero passed in-run**: OFF reservoir 23,996,604 rows (== oracle expectation), ON
  3,885,538 rows, strictly less; output digests identical across arms. The arms are
  distinct and output-equivalent.
- **Pairing**: `(arm, run_index)` complete and contiguous; **10/10 pairs positive**
  (OFF − ON range 7.2–9.5 s, no sign flips).
- **HL OFF−ON shift: 8,411 ms**, 95% interval **7,745–8,914 ms**.
- **Dual criterion**: interval excludes zero (detectable) and the point estimate clears
  the predeclared 10%-of-baseline-median floor (3,269 ms) — **material**.
- Review fix, same cut: the emitter originally computed the floor from the pooled cohort
  median (2,870 ms) rather than the OFF/control median the rule predeclares. Corrected to
  the OFF-arm median; the receipt now emits `baseline_off_median_ms` and
  `materiality_floor_ms`. The verdict is unchanged — 8,411 ms clears both floors — and
  `adr451_materiality_baseline_is_off_arm_median_not_pooled` pins the rule with a shift
  placed between the two thresholds (material under pooled, immaterial under OFF).
  The durable release receipt was then re-emitted under the corrected rule via a
  zero-child resume of the completed journal: no timed children re-ran, samples are
  byte-identical, gate zero re-executed live and passed with identical digests. The
  original receipt is preserved as `latest_summary.pre_reemission.jsonl` and the
  re-emission is marked by `RECEIPT_REEMISSION.md` in the cohort directory.
- Arm medians: OFF 32,690 ms, ON 24,279 ms — a **25.7% q05 wall reduction**.

**Scope**: this is a causal result for the selected sf=4 reservoir-committing shape — the
446 bridge earns its place there. It is *not* a new general q05 baseline; the summary's
pooled-spread `unstable` flag is mechanical for a two-arm cohort and the baseline gate
correctly declined to record one.

## Debug cohort (protocol corroboration only)

A prior full sweep at the same configuration ran on a debug binary
(`target/tpch/adr449_cohort_sf4_durable`, flagged `debug_build`). It is retained solely
as corroboration that the protocol survives end-to-end — gate zero passed, 21/21 journal
lines, 10/10 positive pairs, `material` receipt emitted in the mandatory order. Its
effect size is not evidence and is not compared causally to the release result.

## Durability hardening

- `adr449_paired_samples.jsonl`: each completed child appended and fsynced as it lands.
- `adr449_paired_run.json`: manifest pins query/mode/scale/data source/data dir/runs/
  warmup; resume requires an identical manifest, refuses a mismatch, refuses a journal
  without a manifest. `--paired-ablation` requires an even `--runs`.
- Pinned by `adr449_paired_journal_is_durable_and_resume_config_locked`.
- Predeclared residual for any future resume: the manifest locks the dataset path, not
  its fingerprint; hardening to fingerprint-match against both manifest and gate-zero
  record precedes any actual resume. Neither cohort here was resumed.
