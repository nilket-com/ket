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

## Measured outcome

### Seal and evidence tiers

Steps 1-2 sealed and pushed as `1ee7d952af82`; every gate green with direct exit statuses, and
the ADR 176 binder-root bound moved 6,170 → 6,190 with its justification sentence. All evidence
below is from the release binary built from that clean identity.

The pre-cohort readout followed the operator's three tiers. **Structural/mechanism gate (held in
full):** the census engaged exactly one cell — q18, fingerprint `1c3870f3f1f9af20` →
`46f9b6171129eaef` — with digests identical on all 22 q-cells and 11 ladder cells and the ladder
plan-invariant; strict correctness green in both arms for q18 and the q19 sentinel; the
989,777,032 B spill **fully eliminated** (zero, not approximately zero); the invocation receipts
show exactly one membership body fold per arm (`scan_grouped`, ~2.75 s both arms — moved, never
duplicated); the 501-key / 3,507-row volume invariants are corroborated by the zeroed spill and
equal digests, and pinned at fixture level. **Explanatory:** join node 13,394.7 → 1,506.1 ms,
exchange 3,530.5 → 1,421.5 ms, peak RSS 10.3 → 3.9 GiB. **One named residual for the successor:**
the outer fold (`uncovered_generic_fold`) costs 2,276.1 ms in the commuted shape versus 0.3 ms at
the root-semi shape — pure cost, not semantics (digests equal) — the commuted spine's outer
aggregate arrives by a path the grouped-fold recognizers do not cover.

One reachability finding recorded: a joined membership body is unreachable from the SQL `IN`
path (the frontend rejects the shape), so the body whitelist is defense-in-depth for other
frontends; the outer-join-on-path decline and both engagement directions (source branch and
fanout branch) are pinned by tests.

| evidence | artifact |
|---|---|
| censuses, both arms | `target/tpch/adr515_{route,ladder}_census_{off,on}_1ee7d952/` |
| evidence diagnostics, both arms | `target/tpch/adr515_q18_diag_{off,on}_1ee7d952/` |
| locked 10-pair cohort | `target/tpch/adr515_q18_paired_1ee7d952/` |

### Locked paired cohort

| pair | candidate | control | control - candidate |
|---:|---:|---:|---:|
| 1 | 8,064.255 ms | 20,818.110 ms | 12,753.854 ms |
| 2 | 8,028.850 ms | 20,127.792 ms | 12,098.942 ms |
| 3 | 8,085.611 ms | 21,034.297 ms | 12,948.686 ms |
| 4 | 8,672.312 ms | 20,801.033 ms | 12,128.721 ms |
| 5 | 8,655.637 ms | 21,099.827 ms | 12,444.190 ms |
| 6 | 8,695.204 ms | 21,255.712 ms | 12,560.509 ms |
| 7 | 8,682.852 ms | 21,312.033 ms | 12,629.180 ms |
| 8 | 8,206.967 ms | 20,871.185 ms | 12,664.218 ms |
| 9 | 8,631.517 ms | 21,099.508 ms | 12,467.991 ms |
| 10 | 8,748.679 ms | 21,042.152 ms | 12,293.472 ms |

```text
Hodges-Lehmann point        = 12,514.250 ms
two-sided 95% interval      = [12,293.472, 12,696.438] ms
fixed materiality floor     = 3,554 ms
interval excludes zero      = true
point reaches floor         = true (3.52x; the interval lower bound alone is 3.46x)
activate                    = true
```

### Activation

One guard retarget rides this receipt, named per the council discipline: ADR 322's
`has_q18_grouped_membership_keyed_join` pinned the semi's composite output schema (`c_name`,
`l_orderkey`), which was the pre-515 position, not the invariant. It now pins the
position-agnostic invariant — a keyed `LeftSemi` with the grouped body as its build and
`o_orderkey` on the preserved side — which holds in both arms.

Per ADR 472's rule: `activate`, with q18 the only engaged cell and no plan, digest, or route
change anywhere else. This receipt flips `membership_first_composition` to compiled default
true, retargets the gauge knob-contract default arm, and re-runs the full seal gate plus
default-arm strict correctness smokes for q18 and q19 from the flipped tree. q18 moves from
~21.0 s to ~8.4 s with spill 990 MB → 0 and RSS 10.3 → 3.9 GiB. The remaining q18 envelope is
the membership body fold (~2.75 s, an ADR 508 compute-fold premise) and the commuted shape's
2.28 s outer-fold residual — both named, neither claimed here.
