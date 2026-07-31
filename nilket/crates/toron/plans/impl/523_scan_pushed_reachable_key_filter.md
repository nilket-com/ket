# impl 523 - scan-pushed reachable-key filter plan

Fuse the 16,042-key ASIA-supplier membership into the lineitem scan's existing decode, under
a ~460.5 ms total overhead allowance derived from ADR 522's ledger. Phase 0 benchmarks the
probe cost first and has immediate-close authority; mechanism design happens only if the
number fits.

## Immutable inputs

- tree: post-522-red main (`3332a9a6` lineage), all activated knobs default on;
- fixed materiality floor: 3,554 ms; budgets 2 GiB / 2 GiB explicit; format-2 full-hash
  dataset;
- inherited evidence (ADR 522, immutable): dataset-twin gross ceiling +4,524.5 ms with chain
  shape and predicted volumes preserved (build 9,623,900; suffix-1 emissions 1,460,247);
  twin-only scan saving ~510 ms; branch-semi net 2,415-3,565 ms (closed red); the SQL twin's
  shape-loss hazard (chain recognition must survive);
- measured bases: m08 = 265.4 ms (Int64 decode floor), m06 = 1,784.0 ms (48M probes vs a
  12M-key build) at the ADR 518 census coordinate;
- predeclared allowance: **~460.5 ms** total filter + acquisition overhead; exceed → close
  red, return to q09.

## Phase 0 - probe-cost benchmark (no mechanism code)

Temporary ladder probe: rebind M06_SQL to
`SELECT COUNT(l_orderkey) AS row_count FROM lineitem INNER JOIN supplier ON l_suppkey =
s_suppkey` (48M probes, 80,000-key build, every row matches — pure probe cost). Five
interleaved `ladder --runs 1` invocations at the coordinate alternating with the sealed
binary's unpatched ladder are unnecessary — the comparison is internal to the patched binary:
patched m06 vs m08 within the same ladder runs. Protocol: build the probe binary, run
`ladder --runs 5` once (medians from one invocation, the ladder's own repetition), record
patched-m06 and m08 medians, remove the probe with a targeted edit.

Marginal probe cost = patched-m06 − m08 − one m08-class Int64 decode (~265 ms). Stated
overstatements (real build 5x smaller; fused filter skips join scaffolding) make this a fair
upper bound. **Decision: marginal ≤ ~460.5 ms proceeds; above closes red immediately.**
Results land in this document either way.

## Steps 1-2 - mechanism and tests (contingent)

`PlannerConfig::scan_pushed_key_filter`, compiled default **false**; gauge env
`TORON_TPCH_SCAN_PUSHED_KEY_FILTER`, loud contract, row provenance, knob test. The filter
derives the key set once from the chain's prepared supplier-domain inputs (no cloned joins),
rides the lineitem scan decode as a null-safe, no-false-negative row filter, and leaves chain
recognition, keys, intents, exchanges, and schemas untouched. Structural battery (engagement
with intact route; declines; knob-off byte-identity; single derivation), semantic battery
(NULL keys pass through; duplicates; empty/all-filtered sources), architecture
reconciliations.

## Step 3 - census, correctness, engagement evidence

Both arms, q01-q22 + m00-m10, exact coordinate, sealed binary. q05 evidence: suffix build
47,989,007 → ~9,623,900; suffix-1 emissions 7,283,512 → ~1,460,247; digests identical;
strict correctness both arms on fingerprint-changed cells; q19 sentinel; regression cohorts
for engaged cells beyond q05.

## Steps 4-7 - seal, gates, cohort, receipt

Standard protocol: cohesive seal, full gate battery with direct exit statuses, push, release
build; q05 paired cohort (20 runs / 10 pairs, plan-distinct, floor 3,554 ms, Hodges-Lehmann +
95% Wilcoxon); activation per ADR 472's rule with default flip, knob retarget, full gates,
default-arm smokes; both-way verdicts.

## Repository gates

The standard council seal and changed-surface blocks via `scripts/cargo-cgroup`, direct exit
statuses preserved, gates re-run after any code change made in response.
