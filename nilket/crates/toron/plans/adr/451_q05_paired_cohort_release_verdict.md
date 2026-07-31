# ADR 451 - q05 paired cohort: durable execution, release-only verdict

Execute the ADR 449 paired ablation at the ADR 450 fixture scale (sf=4) as one detached,
durably-journaled release-build cohort, and seal its HL/dual-criterion verdict. Only a
release-profile cohort is admissible; any debug-profile sweep is protocol corroboration,
never evidence.

## Context

449 built the instrument (gate zero, pairing rails, sign fixture, admissibility order) and
450 selected the sole committing fixture scale, sf=4. What remained was purely execution —
and execution is where this arc has repeatedly lost cohorts: a session-bound parent dies
with the session, and the harness accumulated all samples in memory and wrote only at the
end, so any interruption of a multi-hour serial sweep cost everything. A first full sweep
also ran on a debug binary, which the summary honestly flagged (`debug_build`) — a
completed protocol run, not an admissible performance verdict.

## Decision — three rails

**Rail 1 — durable incremental journal.** Every completed child (warmup included) is
appended and fsynced to `adr449_paired_samples.jsonl` in the cohort's output directory as
it finishes. An interruption costs the current child, not the cohort.

**Rail 2 — config-locked resume.** A manifest (`adr449_paired_run.json`) pins the cohort's
query, mode, scale, data source, data dir, run count, and warmup count at first launch.
Resume replays only journal rows under a byte-identical manifest; a mismatch refuses with
an instruction to use a fresh directory — never a silent cross-config merge. A journal
without its manifest is ambiguous and refused. Paired runs require an even `--runs` count
(20 → the predeclared 10 pairs); an odd count cannot form complete pairs and is rejected
up front. Known residual, predeclared before any resume is attempted: the manifest locks
the dataset *path*, not its fingerprint. Any future resume must first harden the check to
require the dataset fingerprint to match both the journal manifest and the gate-zero
record that authorized the cohort; an uninterrupted run never consults the manifest and is
internally consistent regardless.

**Rail 3 — release-only admissibility.** The verdict cohort runs a release binary as a
single detached parent (decoupled from the session, on-disk log), with 449 gate zero as
its in-run precondition. A debug-profile sweep, however complete, is retained solely as
protocol corroboration: it proves the journal, pairing, estimator, and receipt emitter
end-to-end, and its receipt says so via the `debug_build` flag. Its effect size is not
compared causally against the release result; the release result alone is the evidence.

## Acceptance

- Journal: each completed child fsynced as it lands; exact-config restart resumes;
  changed scale/dataset/run-count/warmup refuses; journal-without-manifest refuses.
  Pinned by `adr449_paired_journal_is_durable_and_resume_config_locked`.
- Even-`--runs` requirement on `--paired-ablation`.
- Verdict cohort: release profile, single detached parent, gate zero passed in-run,
  21/21 journal lines, `(arm, run_index)` pairing complete, receipt in 449's mandatory
  order, dual-criterion verdict stated with the predeclared 10% materiality rule.
- The debug cohort's artifacts are preserved and labelled corroboration-only.

## Guardrails and risks

- **A completed run is not an admissible run.** The profile flag decides; the first sweep
  of this cut completed perfectly and proved nothing about throughput.
- **No mid-flight contract changes.** The resume manifest is never mutated under a live
  run; the fingerprint hardening is a precondition of the *next* resume, not a patch to a
  running cohort.
- **Pooled-spread `unstable` is mechanical in a two-arm cohort** — the pool mixes two
  genuinely different distributions. The paired estimator is the predeclared instrument;
  the baseline-recording gate correctly declines to bank an ablation run as a plain perf
  baseline.
- **Do not re-judge sealed reds.** 443/444/446/448/449 stand.

## Forward arc

**Material** → the 446 bridge earns its place; the paired instrument plus durable journal
becomes the template for other spill-heavy cells (q18, q21).

**Immaterial or undetectable** → reservoir volume is not the q05 wall; chain reorientation
(demoted) is re-examined under the same paired instrument.

The ADR 446 12x spill-scope gap remains open. Query-budget realism (`memory_per_task` via
`test_default()`), `MAX_FILTER_BYTES` cap raise, ADR 132's comparator, q17 remeasurement,
q18 strict one-pass, q21 residual, q19 re-diagnosis + 438 widening, q13, Cut A (439
mirror), and the generic O(1) LRU rewrite remain queued.
