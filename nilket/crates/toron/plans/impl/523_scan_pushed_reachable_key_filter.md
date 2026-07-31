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

## Phase 0 result — closed red: the probe cost alone exceeds the allowance

The m06 probe (rebound to `lineitem INNER JOIN supplier ON l_suppkey = s_suppkey`, with the
cell's table registration following; first attempt failed loudly on `TableNotFound: supplier`
and was corrected) measured at the exact coordinate, 5 runs + 1 warmup:

| primitive | median | samples |
|---|---:|---|
| patched m06 (48M probes vs 80,000-key build, every row matches) | **1,316.3 ms** | 1,337.4 / 1,316.3 / 1,328.8 / 1,312.4 / 1,315.4 |
| m08 (`COUNT(l_orderkey)` decode floor) | 272.9 ms | 272.9 / 271.9 / 274.6 / 270.5 / 273.0 |
| m00 (scan proof, unchanged) | 238.4 ms | — |

Marginal probe cost = 1,316.3 − 272.9 − 265.4 = **778.0 ms**, against the ~460.5 ms
allowance — an overrun of ~69% before any acquisition or filter-fusion overhead is even
considered. The stated overstatements (16,042-key build 5x smaller than 80,000; fused filter
skipping join scaffolding) would need to recover more than 40% of the probe cost to fit; the
predeclared rule does not speculate on that recovery. **ADR 523 closes red here.** No
mechanism code, no knob; the probe was reverted with targeted edits and the tree is clean.

The structural ledger across ADRs 522-523 now bounds the whole reachable-key family at q05:
any mechanism that tests 48M keys per traversal — semi node or scan-fused — pays 0.8-1.5 s
against a total allowance of ~0.46 s. The family is closed at this cell, not just these two
shapes. What remains of the banked +4,524.5 ms ceiling requires a mechanism that avoids
touching all 48M rows entirely (reorientation is parked with no floor-clearing path; nothing
else is currently named). Per the predeclared routing, the board returns to **q09**.

Phase 0 artifacts: `target/tpch/adr523_p0_ladder{,2}/` (first run carries the honest
TableNotFound failure row; the second is the evidence).
