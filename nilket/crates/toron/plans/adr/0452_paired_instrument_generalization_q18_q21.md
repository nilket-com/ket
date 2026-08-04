# ADR 452 - paired instrument generalization: q18/q21 reachability, then cohorts

Generalize the 449/450/451 apparatus beyond q05: probe whether q18 and q21 ever commit
the grouped-reservoir path, and run the paired ablation only where they do, behind a
query-shaped gate zero. Ladder first; a query that never commits is a sealed result, not
a failure. The dataset-fingerprint resume hardening predeclared in 451 lands before any
new cohort.

## Context

451 produced the arc's first admissible causal result: the 446 runtime-filter bridge is
material for q05 at its committing shape (HL 8.4 s, 25.7% wall reduction at sf=4). Its
forward arc names the template's next cells: the spill-heavy q18 and q21. The switch is
query-generic (`TORON_ABLATE_LOCAL_RUNTIME_FILTER`, `exec/join/streaming.rs:20`), but
the instrument is q05-locked in three places:

- `--paired-ablation` refuses any query but q05 (`main.rs:419`).
- The gate-zero OFF expectation is `q05_reservoir_full_input_cardinality` — unfiltered
  lineitem, an equality 449 explicitly scoped to q05's build side. Reusing it for a
  query whose build side is filtered would wrongly fail (or worse, wrongly pass) gate
  zero.
- The 450 scale ladder (`run_adr450_scale_ladder`) has no query parameter; committing
  scale is known only for q05.

Whether the bridge is reachable at all for q18/q21 is unknown. Measuring before proving
reachability is the arc's recurring failure; the ladder is the cheap gate.

## Decision — four rails, strictly ordered

**Rail 0 — fingerprint-locked resume (451's predeclared debt).** The paired-run manifest
gains the dataset fingerprint; resume requires it to match both the manifest and the
gate-zero record that authorized the cohort. A manifest without the fingerprint field
(pre-452) is refused for resume with the fresh-directory instruction — never
grandfathered. Lands before any 452 cohort launches.

**Rail 1 — query-parametric ladder.** The 450 ladder takes the query as a parameter and
runs for q18 and q21 with the unchanged outcome taxonomy (committed /
non-committing-by-diagnostic / hard error / inconclusive) and per-rung isolation rails.
Ascend to the q05 affordability bound (sf=4 rungs first; climbing past it is an explicit
affordability decision, not a default). Per query, the smallest committing rung becomes
that query's sole fixture scale. **No commit across the ladder is a predeclared sealed
result**: the bridge is inert for that query and its cohort never runs.

**Rail 2 — query-shaped gate-zero oracle, derived per committing query.** 449's rule
generalizes, not its constant: the OFF expectation is that query's actual reservoir
build-side input cardinality, derived independently of the profiled run (from the strict
generators, with the query's build-side filter mirrored by an oracle where the build
side is not a bare table). The receipt states the equality's reason for that query, as
449's q05 record does. The profiled OFF count is the measurement, never its own
expectation; a hard-coded count at any scale is the defect this rail exists to prevent.
ON strictly less remains the distinctness half. Gate-zero failure for a query seals that
query's arm as a switch-defect or non-applicability report — no cohort.

**Rail 3 — paired cohorts, unchanged protocol.** For each query passing gate zero:
release-only, single detached parent, even runs (20 → 10 pairs), durable fsynced
journal, fingerprint-locked manifest, gate zero in-run, receipt in 449's mandatory
order, materiality floored at 10% of the OFF-arm median with `baseline_off_median_ms`
and `materiality_floor_ms` emitted. Debug sweeps are protocol corroboration only.

## Predeclared outcomes, all admissible per query

- **Material** — the bridge generalizes beyond q05; the instrument is a template in fact.
- **Immaterial or undetectable** — the reservoir commits there but its volume is not
  that query's wall; the per-query wall question reopens with clean data.
- **Never commits / gate zero fails** — the bridge is inert or inapplicable for that
  query; sealed as a reachability result. q05 remains the bridge's only proven cell.

A mixed outcome (one query material, the other not) is expected and fine; the queries
seal independently.

## Acceptance

- Rail 0: fingerprint in manifest; resume refuses missing/mismatched fingerprint against
  either the manifest or the gate-zero record; pre-452 manifests refused. Tested.
- Rail 1: ladder takes a query; q18/q21 ladders run with per-rung isolation; outcome
  recorded per rung with which diagnostic; no-commit seals without a cohort. The q05
  classifier tests keep passing unchanged.
- Rail 2: per-query OFF expectation derived, never constant; a test fails if a q05-shaped
  (bare-lineitem) expectation is applied to a query whose build side is filtered; the
  equality's reason appears in the receipt.
- Rail 3: cohort receipts carry the 451-corrected materiality fields; `--paired-ablation`
  accepts exactly the queries with a committing rung and a passed gate zero.
- No timing figure from any 452 cohort is comparable to pre-gate baselines.

## Guardrails and risks

- **Ladder before oracle before cohort.** Building the oracle for a query that never
  commits is wasted; running a cohort before gate zero is the 448-era mistake.
- **The oracle equality is query-shaped.** Copying q05's unfiltered-lineitem equality to
  q18/q21 is the exact mis-fire 449 warned about.
- **Affordability is explicit.** A committing rung near or past sf=4 hands the room a
  cost decision with a number, not a default multi-hour cohort.
- **The wrapper cannot run the heavy oracle tests** — 10 GB debug footprints wedge under
  the cgroup memory ceiling; run the built test binary directly for those.
- **Do not re-judge sealed reds.** 443/444/446/448/449 and 451's verdict stand.

## Forward arc

Material anywhere beyond q05 → the instrument graduates to a standing rail for
spill-heavy cells; q17 remeasurement and q13 become candidates under the same protocol.
Inert everywhere beyond q05 → the bridge is a q05-shaped win, and the per-query wall
work (q18 strict one-pass, q21 residual) proceeds on its own merits.

The ADR 446 12x spill-scope gap remains open. Query-budget realism (`memory_per_task`
via `test_default()`), `MAX_FILTER_BYTES` cap raise, ADR 132's comparator, q17
remeasurement, q18 strict one-pass, q21 residual, q19 re-diagnosis + 438 widening, q13,
Cut A (439 mirror), and the generic O(1) LRU rewrite remain queued.
