# impl 515 - membership-first composition plan

One declining tree-surgery recognizer at the binder's Join arm, commuting an author-pinned
membership `LeftSemi` to the single source branch of its complete key tuple. No join-order
change, no new machinery below the binder.

## Immutable inputs

- control tree: post-ADR-514 main (`8936df4b` lineage), all 511/512/514 knobs default on;
- fixed materiality floor: 3,554 ms (inherited, not recomputed);
- exact dataset: strict sf=8 persisted Parquet with `full_hash`; budgets 2 GiB / 2 GiB explicit;
- grounded anchors: q18 fingerprint `1c3870f3f1f9af20`; root `LeftSemi` `PinnedByAuthor` over the
  spine `(customer ⋈ orders) ⋈ lineitem`; join node 12,972.1 ms carrying 989,777,032 B on the
  generic spilled-hash path (`join_spill_observations` empty); membership body fold 2,552.3 ms
  (`scan_grouped`, spill-free); qualifying keys 501 of 12,000,000; post-semi lineitem volume
  3,507 rows; peak RSS 10.77 GiB; reference shape semi-first onto orders (DuckDB EXPLAIN);
  ADR 322 built the pinned membership shape, ADR 423's forward arc names this cut.

## Step 1 - recognizer and control

`PlannerConfig::membership_first_composition: bool`, compiled default **false** at the seal.
Gauge env `TORON_TPCH_MEMBERSHIP_FIRST`, loud "0"/"1" contract, row fields
`membership_first_composition` + provenance, knob-contract test beside the 511/512 ones.

Extend `binder/scalar_reduction.rs` (the reducer-below family's module) with the commute:

- hook: the `LogicalPlan::Join` bind arm, `kind == LeftSemi`, plain equality, pinned strategy
  intent, knob on — normalize-then-re-enter like the arm's existing ON-residual path;
- gates, all fail-closed to a byte-identical plan: membership subtree over the conservative
  whitelist only (scan, filter, project, alias, distinct, aggregate, exchange — anything else
  declines); every outer-side semi key a plain column owned exclusively by exactly one spine
  leaf branch (scope probes, ambiguity declines); all keys the same branch; every crossed join
  `Inner` with plain equality (outer/anti/cross/null-safe on the path decline);
- rewrite: replace the source branch `S` with `LeftSemi(S, body)` carrying the identical `on`
  tuple, the identical membership subtree (moved, not cloned), and the identical pinned intent;
  the spine above is byte-identical; re-enter `bind_plan_with_trace` on the rewritten tree;
- termination: the rewritten root is Inner (the semi arm does not re-enter); the relocated
  semi's outer side is a single branch, so the recognizer declines it structurally on re-entry.

## Step 2 - tests

Equivalence fixtures in `tests/scalar_reduction_test.rs` (execution, both arms, identical
results), proving filter-before-fanout equals filter-after-composite:

- duplicate source keys with spine fanout (one orders-class row matching several lineitem-class
  rows survives or dies atomically in both shapes); duplicate body keys;
- null keys on the source side and in the body; keys in the body absent from the source and
  vice versa; a two-key membership tuple;
- the q18 shape at fixture scale: knob on binds the semi below the spine on the source branch
  with `PinnedByAuthor` intact and a single semi receipt (moved, not cloned — structural
  equality of the membership subtree); knob off reproduces today's root-semi plan.

Decline pins (plan byte-identical, knob on): an outer join on the path; anti and cross joins on
the path; null-safe keys; a key tuple split across two branches; a body referencing an outer
relation; an unpinned semi.

Architecture: reconcile module/line-bound guards; name every retarget in the receipt.

## Step 3 - census, correctness, and the direct volume evidence

Both arms, q01-q22 and m00-m10, exact coordinate, sealed binary. The census determines the
engaged set; every fingerprint-changed cell runs its strict correctness rail in both arms
(q18's rail at its supported coordinate; q19 sentinel regardless).

Direct evidence required before any cohort row, from the sealed binary at the exact coordinate:

- q18's candidate row: spill 989,777,032 → (approximately) zero, digest/schema/rows identical,
  peak RSS collapsed with the volume;
- node attribution per arm: the join node's spill and time collapse; the membership body fold
  survives at its ~2.5 s;
- the volume invariants pinned at fixture level (membership output stays the body's key set;
  downstream volume equals the post-semi join volume) and corroborated at sf=8 by the
  data-counted 501 / 3,507 against the observed route facts.

## Steps 4-7 - seal, gates, cohort, receipt

The standard protocol: cohesive seal of steps 1-3, full gate battery with direct exit statuses,
push, release build from the pushed identity, then:

```bash
TORON_TPCH_MEMORY_PER_TASK=2147483648 \
TORON_TPCH_SPILL_BYTES_RESERVED=2147483648 \
target/release/toron-tpch perf \
  --query q18 --mode strict --sf 8 --data-source parquet --data-dir target/tpch/data \
  --engines toron --runs 20 --warmup 1 \
  --paired-ablation \
  --paired-env TORON_TPCH_MEMBERSHIP_FIRST \
  --paired-on 1 --paired-off 0 \
  --paired-gate plan-distinct \
  --paired-materiality-floor-ms 3554 \
  --output-dir target/tpch/adr515_q18_paired_<build>
```

20 child rows = 10 adjacent pairs; Hodges-Lehmann point and two-sided 95% Wilcoxon interval from
the persisted journal; the standard verdict table; no post-hoc extension or floor negotiation.
Receipt flips the compiled default iff `activate` and no engaged cell prices a material
regression (ADR 472's rule), then re-runs the full seal gate and default-arm smokes for q18 and
q19 from the flipped tree.

## Repository gates

The standard council seal and changed-surface blocks via `scripts/cargo-cgroup`, direct exit
statuses preserved, gates re-run after any code change made in response.
