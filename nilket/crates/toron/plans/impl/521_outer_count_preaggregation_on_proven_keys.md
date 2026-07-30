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

## Measured outcome

### Seal and semantic evidence

Steps 1-2 sealed and pushed as `2c78c305`. The rewrite consumes only ADR 520's ordered
proof-tier accessor; the fixtures prove that primary-key-only, estimated-NDV-only, and
wrong-order evidence all decline. Seven execution fixtures pin the original aggregate field
contract through parent projection pruning and cover unmatched rows, all-filtered input,
duplicate matches, NULL counted values, NULL keys, and every six-gate decline. One real defect
was caught before the seal: the new async bind hook overflowed the deep-chain fixture until its
future was boxed at the recursive boundary. The permanent breadth-cap test now covers that
path.

The seal battery passed with direct exit statuses: 1,456 Toron lib tests, 2,095 architecture
tests, the complete `--no-fail-fast` integration and toron-tpch suites, all three
`--all-targets` checks, fmt, and `git diff --check`. Architecture reconciliations add the carved
module and changed-surface fixture, move ADR 176's binder-root bound 6,230 → 6,275 with its
justification, and enforce that this module cannot consult primary-key or NDV evidence.

### Census, correctness, and attribution gate

Both arms used the sealed release binary, the ADR 520 manifest-format-2 sf=8 dataset under
`full_hash`, and explicit 2 GiB memory/spill budgets. The q-series engaged exactly q13:

| fact | control | candidate |
|---|---:|---:|
| plan fingerprint | `32c7b15a1c5a244e` | `4ddb353ff791ff65` |
| one-run execute | 9,387.779 ms | 3,333.286 ms |
| spill | 5,131,808 B | 0 B |
| peak RSS | 650,396 KiB | 350,496 KiB |
| output | 44 rows, schema `fnv1a64:06933e705bc02cb3` | identical |

All 22 q-series digests, schemas, row counts, and statuses match; the other 21 plans are
identical. All 11 ladder cells are invariant on plan, digest, row count, and spill. Strict sf=8
correctness passes for q13 and the q19 sentinel in both arms.

The exact-coordinate node pair corroborates the mechanism: `left_outer_grouped` disappears;
aggregate ownership falls 9,257.4 → 3,169.8 ms; its survivor is the reconciled 3,158.7 ms
`scan_grouped` orders preaggregation; spill falls 5,131,808 → 0 and RSS by about 298 MiB. Thus
the inherited twin's field-contract warning is resolved by the rewrite itself, and the win is
volume removal rather than spill relocation.

One discarded scratch census used obsolete budget variable names and consequently reported the
compiled 64 MiB / 512 MiB defaults. It was stopped before completion and is not evidence. Every
number above comes from the rerun whose rows state both 2 GiB budgets with `explicit`
provenance.

### Locked q13 cohort and activation

The plan-distinct gate passed with one digest and schema across arms. Ten adjacent pairs from
the persisted journal all report control spill 5,131,808 B and candidate spill 0 B:

| statistic | value |
|---|---:|
| Hodges-Lehmann point | **+5,985.221 ms** |
| two-sided 95% Wilcoxon interval | **[+5,964.995, +6,002.108] ms** |
| fixed materiality floor | 3,554 ms |
| verdict | **activate** |

The interval lower bound alone is 1.68x the inherited floor. Pair differences (control minus
candidate, ms) are 5,959.960; 5,947.890; 5,963.224; 6,021.288; 5,982.101; 5,988.342;
6,015.280; 6,002.108; 5,974.805; 6,001.411. All 20 children pass with one 44-row digest,
schema fingerprint, and the two predeclared plan fingerprints.

Per ADR 472's rule, the sole engaged cell activates with no census regression. This receipt
flips `outer_count_preaggregation` to compiled default true, retargets the knob-contract
default arm, and re-runs the complete seal battery plus default-arm strict sf=8 q13/q19 smokes
from a fresh release build.

The post-521 owner is now the roughly 3.16 s orders preaggregation — below the fixed floor by
itself and a third tenant of the grouped-fold family after q18 and q20. Wendelon's declared
`EnforcedConstraint` tier remains an independent cross-ring follow-on; this rewrite continues
to accept proof-tier evidence only.

| evidence | artifact |
|---|---|
| q-series censuses | `target/tpch/adr521_route_census_{off,on}_v2_2c78c305/` |
| primitive ladder | `target/tpch/adr521_ladder_census_{off,on}_2c78c305/` |
| exact-coordinate attribution | `target/tpch/adr521_q13_attr_{off,on}_2c78c305/` |
| locked cohort | `target/tpch/adr521_q13_paired_2c78c305/` |
