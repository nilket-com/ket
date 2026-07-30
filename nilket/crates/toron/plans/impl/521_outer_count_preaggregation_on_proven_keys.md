# impl 521 - outer count preaggregation on proven keys plan

The reopened ADR 519 cycle: the same six-gate transformation, uniqueness now resolved
exclusively through the ADR 520 proof accessor, the Arrow field contract reproduced explicitly
and pinned structurally, and the census digest gate standing between the rewrite and its
cohort. ADR 519's red record is referenced, never rewritten. No Phase 0: the twin economics
(+5,602.13 ms at the exact coordinate) are inherited as immutable evidence.

## Immutable inputs

- tree: post-520 main (`8c7cda9e` lineage); dataset: strict sf=8 persisted Parquet, manifest
  format 2, full-hash loads, `customer [c_custkey]` VerifiedSnapshot proof present (bound to
  schema `fnv1a64:47fee385c7b4099d`, file `fnv1a64:562c67ef2a64d53c`);
- fixed materiality floor: 3,554 ms (inherited, not recomputed); budgets 2 GiB / 2 GiB explicit;
- inherited evidence (ADR 519, immutable): the transformation theorem; the twin —
  8,937.96 → 3,335.83 ms, spill 5,131,808 → 0, RSS ~650 → ~344 MiB, join → 76 ms, orders
  aggregation ~3.18 s; the twin's digest divergence (correct rows, matching schema
  fingerprint, different Arrow IPC digest) as the standing warning; the prototype's finding
  that uniqueness was the only blocking gate; q13 off-arm identity `32c7b15a1c5a244e` with
  8.9-9.4 s wall and `left_outer_grouped` owning 8.90 s;
- ADR 520 surfaces, read-only: the proof accessor as the sole uniqueness path.

## Step 1 - the rewrite and control

`PlannerConfig::outer_count_preaggregation`, compiled default **false** at the seal; gauge env
`TORON_TPCH_OUTER_COUNT_PREAGGREGATION`, loud "0"/"1" contract, row fields with provenance,
knob-contract test beside the prior ones.

At the grouped-aggregate bind site, gated by the knob and all six conditions: rebuild as
`Project[group key, COALESCE(cnt, 0)]` over `L left_outer Aggregate[k_R; COUNT(col_R)](R)`.
The uniqueness condition calls the ADR 520 proof accessor with the preserved join key's
ordered columns and accepts only a proof-tier match; `primary_key_columns` and optimizer-fact
NDV are not consulted. The projection's output field is constructed from the original
aggregate's Arrow field — name, type, nullability — never re-derived. Anything unproven
declines; knob-off is byte-identical. Pass body beside the scalar-reduction family per the
ADR 176 root discipline.

## Step 2 - tests

Structural (bind receipts, the prototype's shapes as permanent tests): engagement against a
proof-bearing catalog with the field contract asserted at bind (name, type, nullability —
before any execution test); per-gate declines (join kind, residual ON, null-safe keys,
non-key/extra group keys, `COUNT(*)`/left-column/`SUM`, nondeterministic right filter);
proof-only pins (`primary_key_columns`-only declines, estimated-NDV-only declines, mismatched
proof column order declines); knob-off byte-identity.

Semantic (execution, both arms identical): unmatched preserved rows → 0, duplicate right
matches, NULLs in the counted column, NULL join keys both sides, empty and all-filtered right
sides; engaged fixture asserts result and schema/nullability identity between arms.

Architecture: reconcile any guards scanning the touched surfaces; name retargets in the
receipt.

## Step 3 - census (digest-gated), correctness, and engagement evidence

Both arms, q01-q22 and m00-m10, exact coordinate, sealed binary, format-2 full-hash dataset.
**Gate: digests and output schema fingerprints identical on every cell, or the cycle stops
here with a report — no cohort runs.** Engaged set is empirical. Required q13 evidence:
`left_outer_grouped`'s ~8.9 s stage absent on-arm; spill 5,131,808 → 0; the orders
preaggregation and collapsed join as the new envelope; fingerprint-changed cells run strict
correctness in both arms; q19 sentinel.

## Steps 4-7 - seal, gates, cohorts, receipt

Standard protocol: cohesive seal, full gate battery with direct exit statuses, push, release
build; q13 paired cohort (20 runs / 10 adjacent pairs, plan-distinct, floor 3,554 ms,
Hodges-Lehmann + 95% Wilcoxon from the persisted journal) plus regression-check cohorts for
any other engaged cell; activation per ADR 472's rule, default flip + knob-test retarget +
full gates + default-arm smokes for the engaged set and q19 from the flipped tree; both-way
verdict reporting.

## Repository gates

The standard council seal and changed-surface blocks via `scripts/cargo-cgroup`, direct exit
statuses preserved, gates re-run after any code change made in response.
