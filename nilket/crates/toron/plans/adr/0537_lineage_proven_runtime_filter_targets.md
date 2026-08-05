# ADR 537 - Lineage-proven runtime-filter targets

q11 is the last `executes_divergent` cell on the census board: 7,508 local rows against 0
distributed at sf=8, reproducing in about three seconds at sf=0.01 in memory mode. The cause
is proven, and it is not in the aggregate, the join, the HAVING predicate, the decimal
multiply, or the shuffle payload — every one of those faithfully propagated an emptiness that
originated earlier.

**The chain.** `join_having_scalar_subplans` (`sql_planner.rs:5028-5097`) lowers a HAVING
scalar subquery to an Inner HashPartitioned join keyed on a synthetic literal
`Int32(1)` projected on both sides. The scheduler treats that join as runtime-filter eligible,
builds a filter from the outer branch, and installs it on the *scalar* branch's base scans.
Those scans then return zero rows, so `SUM` over an empty input is NULL — correct SQL — and
`sum > NULL` drops every row.

Boundary evidence, local against distributed: scalar-branch base scans `1/100/8000` against
**`0/0/0`**; aggregate input 400 against **0**; raw SUM populated against **NULL**; scaled
projection populated against **NULL**; join input right side 0 nulls against **1 null**. The
distributed scans that return zero are exactly the ones carrying a runtime filter, while the
same three table identities scanned without one in the same run return full data.

**The eligibility predicate proves nothing about lineage.**
`runtime_filter_join_eligible` (`fragment.rs:1620`) checks only that the join is Inner, that
each side has one key, and that both key types are supported and equal. It never asks whether
the probe key descends from a physical column. `runtime_filter_descriptor_for_probe`
(`scheduler.rs:976-1006`) then *reconstructs* a target after the fact: it takes
`partition_key.single_column_root()`'s **post-projection ordinal** and pairs it with
`first_scan_fingerprint(&spec.plan)` — the first base scan appearing in that task's plan.
Those two facts come from different schemas. For q11 the ordinal denotes
`__having_scalar_inner_key_0`, a synthetic literal with no base-column meaning, and the
fingerprint denotes an unrelated scan. The filter is well-formed and semantically meaningless,
so it rejects everything.

**A configuration defect kept this hidden and would prevent testing the fix.** `FragmentPlanConfig::new`
defaults `runtime_filters_enabled` to true (`fragment.rs:51`). The census preflight fragments
once, but the coordinator re-fragments for execution (`coordinator.rs:1171`) through configs
that never propagate the flag — and there are two such paths, `fragment_config` and the inline
config in `run_streaming_read_driver`. Disabling runtime filters therefore changed the
preflight graph while execution ran filtered, which produced two arms that looked like a
refutation and cost two withdrawn conclusions during grounding. There is no independent writer
of `ExecutionContext::runtime_filter`; the scheduler injects the descriptor into the Scan
binding (`scheduler.rs:788`) and the worker copies that binding (`server/mod.rs:1224`).

## Decision

**1. A runtime filter targets a proven physical column, or it is not built.** Trace the probe
join key through projections to a specific physical scan column, and carry the resulting proof
— `{scan descriptor fingerprint, physical column ordinal, key type}` — as one value from
fragmentation into scheduling. Build and install a filter only when that proof exists.

The proof travels with the plan; it is never reconstructed later from
`first_scan_fingerprint` plus a post-projection ordinal. That pairing is the defect, not an
implementation detail of it: it combines two facts drawn from different schemas and cannot be
made safe by tightening either one.

A synthetic or literal join key yields no target, therefore no filter. The query stays
executable — a runtime filter is an optimization, and its absence costs scan volume, never
correctness.

**2. Runtime-filter mode is derived once, from one shared configuration path.** Preflight,
streaming execution and ordinary coordinator execution must all obtain
`runtime_filters_enabled` from the same derivation. This rides ADR 537 rather than a separate
cleanup because Acceptance 4 is untestable without it: today a caller that disables runtime
filters still gets them at execution, so no honest control over this mechanism can be written.

Rejected alternative: refuse runtime filters for constant-key joins specifically. That fixes
q11 and leaves the class open — any join whose key lacks base-column lineage is exposed to the
same reconstruction. The proof-carrying target closes the class, and it is why Acceptance 2
tests the predicate directly rather than only through q11.

## Acceptance

1. **q11.** No runtime-filter dependency edge on the scalar stage, no runtime-filter descriptor
   on its worker scan, and byte-identical distributed output against the local oracle — equal
   digest, row count and schema fingerprint.
2. **Same-typed literal-key join is ineligible.** A join whose keys are literals of an
   identical supported type builds no filter. This case passes today's type-only predicate and
   must fail the new one, so the test discriminates the decision rather than restating it.
3. **Genuine base-column join stays eligible** and receives the proven scan fingerprint and
   physical ordinal — not a reconstructed pair. This is the positive control that prevents the
   cut from degenerating into a blanket disable.
   **The probe must defeat reconstruction by construction:** use a shape with more than one
   base scan where the proven target is *not* the first scan in the plan, and a reordered
   projection where the physical column ordinal *differs* from the post-projection ordinal.
   Assert the descriptor carries the proven scan's fingerprint and the physical ordinal. Under
   today's `first_scan_fingerprint` plus `single_column_root` reconstruction this test fails on
   both axes, so it cannot pass unless the target genuinely travelled with the plan — and it
   will catch any later reintroduction of that pairing, however disguised.
4. **Runtime-filter-disabled execution is coherent.** With runtime filters disabled, every
   fragmentation path omits both the dependency edge and the worker scan descriptor. A test
   pins all three paths, since two of them silently ignored the flag before this ADR.
5. **Board.** The full census re-runs at the unchanged ADR 529 envelope from a sealed identity:
   **21 `executes_identical`, q15 `execution_rejected(BroadcastProbeNotStarLinear)` as the sole
   typed fragment rejection**, and no other cell changing classification.
6. Standard full battery from the final tree including the complete `--no-fail-fast` suite
   (engine cut).

## Guardrails

- No blanket runtime-filter disable, and no change to filter construction for joins whose
  probe key has real column lineage. Their descriptors must be identical to today's except
  that the target is carried rather than rebuilt.
- No change to `join_having_scalar_subplans`' lowering. The synthetic literal key is a valid
  plan; the defect is filtering on it.
- No timing authority. Removing a filter from a scan that was returning zero rows will change
  scan volume; the ADR 529 timing block stands until the board is clean, and elapsed stays
  diagnostic.
- No statement-timeout or capability-refusal path for an unproven target: absence of proof
  means no filter, silently and correctly, not an error.

## Forward

Closing this cell makes the identical set complete apart from q15's inherited ADR 528
fragmenter wall, which **reopens the ADR 529 timing follow-up** — the comparison precondition
that has blocked every timing claim since the execution census began.

Fragmented-plan fingerprint coverage is deliberately *not* in scope. The distributed plan
fingerprint identifies the bound execution plan and not runtime-filter edges or scheduler
metadata, so two materially different fragmented graphs share a fingerprint; that is recorded
as its own reference item and needs its own decision.
