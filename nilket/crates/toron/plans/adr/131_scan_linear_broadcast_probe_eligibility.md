# ADR 131: Scan-linear Broadcast Probe Eligibility

**Status:** Promoted. **Genuine 3-of-3 (Claude, Codex, Gemini).** Corrects ADR
130's over-strict probe-side fence — which silently regressed ADR 129 — by
splitting build eligibility (a *fact* predicate) from probe eligibility (a
*plan-shape* predicate): the probe may be **scan-linear** (`Scan` / `Filter` /
`Project`), restoring the filtered/projected-fact star-schema path while keeping
nested-join / aggregate / sort / take / exchange / limited-scan probes fenced to
hash. Binder-local; no substrate change.

**Category:** Toron interior — optimizer eligibility refinement (binder-local).
**Predecessors:** 129 (optimizer fact spine; build = single base relation + hard
byte bound; probe = any plan; single decision seam), 130 (tiny-right broadcast via
operand swap + restoring `Project`; introduced the both-sides single-base probe
fence this ADR corrects).
**Migration:** `plans/impl/131_scan_linear_broadcast_probe_eligibility.md`.

## Context

ADR 129 introduced the first auto-broadcast rule: an unhinted INNER join infers
broadcast when the **build** side is a single base relation with a hard
`byte_upper_bound` below the configured threshold; the **probe** (the big fact)
could be any plan. ADR 130 added the tiny-right case via a binder-local operand
swap + a restoring `Project` that preserves declared output order.

ADR 130 also added an over-strict probe predicate. `choose_auto_join_strategy`
now requires **both** `left_facts.origin` and `right_facts.origin` to be
`SingleBaseRelation` before either side can be a broadcast build
(`binder.rs:3794-3795`, `RightInputNotSingleBaseRelation`). So `tiny_dim JOIN
(fact WHERE …)` — a tiny base build with a *filtered* fact probe — falls back to
hash, even though the build is still tiny and the probe work is row-local. This
silently narrowed 129 (no test pinned 129's looser reach, so the suite stayed
green — the classic "passes for the wrong reason"). The check is absent from the
129 feat (`56a5bc8`) and present at HEAD: 130-introduced.

`PlanOrigin::Composite` (`binder.rs:320`) cannot express the safe boundary — it
conflates `Filter(Scan)`/`Project(Scan)` with nested joins, aggregates, sorts,
takes, and exchanges. So probe eligibility needs a dedicated plan-shape walk, not
a `PlanOrigin` reuse.

## Doctrine — build eligibility is a fact predicate; probe eligibility is a plan-shape predicate

> The build side still needs a hard proof that one base relation is small enough
> to replicate. The probe side need not be a base relation — but it must be
> **scan-linear**: a scan, optionally wrapped in row-local `Filter`/`Project`,
> with no global, repartitioning, or multi-input operator. This is grounded in the
> distributed substrate: the broadcast consumer reads the probe via a *share-scan*
> (`fragment_broadcast_join_plan` embeds the probe bare and the consumer task
> binds `TaskInputBinding::Scan { share: Some(ScanShare { .. }) }`,
> `fragment.rs:504`, `fragment.rs:526-533`), so each worker applies the probe to
> its own morsel. Only
> row-local, partition-preserving work is correct there:
> - `Aggregate` becomes a per-morsel partial aggregate, not the global result;
> - `Sort` / `Take` / a limited `Scan { row_limit: Some(_) }` become per-worker
>   order/limit, not global;
> - `Exchange` is its own repartition, structurally wrong under a broadcast probe;
> - a nested `Join` is **not** recursively fragmented into the consumer stage
>   (130's real hazard — this is the multi-way-ordering boundary, deferred);
> - `EmptyRelation` / `SystemTable` are not scan boundaries (a `SystemTable` would
>   be materialized independently per consumer task and duplicate rows).
>
> This is not multi-way star-join ordering (nested-join probes stay hash) and not
> smaller-build dominance (130's left-wins tie stands until the comparator ADR).

## Decisions

**D1 — Split build and probe eligibility.** Build eligibility (per side,
unchanged from 129/130): `JoinStrategyIntent::Auto`, `JoinKind::Inner`, effective
auto-broadcast threshold configured, the build side has
`PlanOrigin::SingleBaseRelation` AND a hard `byte_upper_bound < threshold`. Probe
eligibility (opposite side) is no longer `origin == SingleBaseRelation`; it is
`broadcast_probe_is_scan_linear(&plan)` over the already-bound child
`ExecutionPlan`. Replaces the unconditional `right_facts.origin != SingleBaseRelation`
reject at `binder.rs:3794-3795`.

**D2 — `broadcast_probe_is_scan_linear(&ExecutionPlan) -> bool`,** a private
binder helper, an **exhaustive** recursive walk (no `_` wildcard — the compiler
forces a conscious eligibility decision on any future `ExecutionPlan` variant):

```rust
fn broadcast_probe_is_scan_linear(plan: &ExecutionPlan) -> bool {
    match plan {
        ExecutionPlan::Scan { row_limit, .. } => row_limit.is_none(),
        ExecutionPlan::Filter { input, .. }
        | ExecutionPlan::Project { input, .. } => broadcast_probe_is_scan_linear(input),
        ExecutionPlan::EmptyRelation { .. }
        | ExecutionPlan::SystemTable { .. }
        | ExecutionPlan::Aggregate { .. }
        | ExecutionPlan::Exchange { .. }
        | ExecutionPlan::Sort { .. }
        | ExecutionPlan::Take { .. }
        | ExecutionPlan::Join { .. }
        | ExecutionPlan::CreateTable { .. }
        | ExecutionPlan::CreateLogicalType { .. }
        | ExecutionPlan::CreateTableAsSelect { .. }
        | ExecutionPlan::DropTable { .. }
        | ExecutionPlan::Delete { .. }
        | ExecutionPlan::Update { .. }
        | ExecutionPlan::Insert { .. }
        | ExecutionPlan::InsertSelect { .. } => false,
    }
}
```

Notes (grounded): a limited `Scan { row_limit: Some(_) }` is rejected (a pushed
`Take`/`LIMIT` becomes per-worker under share-scan, `execution.rs:207`).
`ExecutionPlan` has **no `Alias` variant** — SQL/Rune aliases are resolved before
this surface — so there is no alias arm. `EmptyRelation`/`SystemTable` are
excluded this cut (not needed to restore the regressed path; `SystemTable` would
duplicate per consumer task).

**D3 — Preserve ADR 130 physical mechanics.** Right-build selection still uses the
binder-local operand swap, paired-key swap, the existing left-build broadcast
shape, and the restoring `Project`. No new `JoinStrategy`, no `ExecutionPlan::Join`
build-side field, no executor / fragmenter / scheduler / wire change.

**D4 — Keep left-wins tie.** If both build candidates are eligible, left still
wins. Smaller-build dominance is the next ADR (132), not part of this correction.

**D5 — Single strategy seam.** `bind_join` computes two scan-linear booleans from
the bound child plans and passes them into `choose_join_strategy` via a private
`JoinInputOptimization { facts, probe_scan_linear }` struct. `bind_join` does not
inspect byte bounds, thresholds, or choose a strategy — all selection stays in
`choose_auto_join_strategy` (preserves `adr129_strategy_decision_is_single_helper`).
Add `HashByDefaultReason::ProbeNotScanLinear` when a tiny build exists but the
opposite side fails scan-linear.

**D6 — Retarget the ADR 130 guard honestly (discipline #4).** ADR 130's
`adr130_decision_helper_single_seam` currently pins the unconditional
right-origin probe check that 131 intentionally removes. **Retarget** it — do not
silently delete the assertion — so it proves build-side origin/byte checks remain
inside the helper and probe eligibility is scan-linear, not origin-based, while
keeping its proofs that the helper consumes both inputs, checks both build sides,
and preserves `BroadcastBuildSide::{Left, Right}` / `JoinPhysicalPlacement::{Natural, SwappedRightBuild}`.

## Behavior gates
- `left_tiny_with_filtered_probe_infers_broadcast` — `dim JOIN (fact WHERE …)` →
  natural left-build broadcast. **Fails on `main`** (130 regressed it). Headline.
- `right_tiny_with_projected_probe_infers_broadcast` — projected fact probe + tiny
  right dim → swapped right-build broadcast, declared output order preserved.
- `filtered_probe_broadcast_byte_identical_to_hash_over_tcp` — distributed digest
  + schema/order parity vs the hash baseline (extends 130's harness).
- `probe_containing_join_stays_hash` — the not-multi-way boundary.
- `probe_with_aggregate_stays_hash` — scan-linear ≠ arbitrary join-free.
- `probe_with_take_or_limited_scan_stays_hash` — per-worker limit not silently
  accepted.
- Regressions green: 129 left-build, all 130 right-build, unknown/large facts
  hash, non-INNER hash, pinned hints win/fail-loud, base-to-base both sides,
  125/128 digest/cap.

## Architecture guards
- `adr131_probe_eligibility_is_scan_linear_execution_plan_walk` — the helper is a
  recursive `ExecutionPlan` walk accepting only `Scan` with `row_limit.is_none()`,
  `Filter`, `Project`.
- `adr131_scan_linear_excludes_global_repartition_and_nested_shapes` — `Join`,
  `Aggregate`, `Sort`, `Take`, `Exchange`, `SystemTable`, `EmptyRelation`, and
  DDL/DML variants are rejected.
- `adr131_build_side_still_requires_single_base_hard_bytes` — origin + byte-bound
  remain build-side checks in `choose_auto_join_strategy`.
- `adr131_strategy_decision_remains_single_helper` — `bind_join` passes probe
  booleans but reads no `byte_upper_bound` / `auto_broadcast_max_build_bytes`.
- `adr131_no_executor_fragmenter_scheduler_wire_or_wendelon_change` — placement
  stays binder-local.
- Retargeted `adr130_decision_helper_single_seam` (D6) + unchanged 129 guards green.

## Non-goals
No smaller-build dominance / cost weigh (132). No nested-join probe / multi-way
star-join ordering (133+). No composite build side / selectivity propagation. No
Wendelon-backed stats producer. No optimizer `EXPLAIN`. No `PlanOrigin` / executor
/ fragmenter / scheduler / wire / config change.

## Council history
iter-0 forked 3 ways — Claude: probe-fence correction (the F1 from the 130
review); Codex: smaller-build comparator; Gemini: multi-way star-join ordering.
iter-1 grounding resolved the sequence (fix the eligibility predicate first) and
narrowed the safe set: Gemini's multi-way overclaimed the current fragmenter
(nested probes aren't recursively staged), Claude's "join-free" over-accepted
aggregate/sort/take under share-scan. iter-2 converged on the scan-linear middle.
iter-3 settled the precision points (3-of-3): limited scans rejected
(`row_limit: None` only); no `Alias` arm (variant doesn't exist); exhaustive
match; `EmptyRelation`/`SystemTable` excluded; the 130 single-seam guard
**retargeted**, not deleted. The three-way grounding narrowed the safe set twice —
the council converging *downward* to the grounded-correct predicate.

## Follow-ons
**ADR 132** — smaller-build dominance with a named left-biased margin (the first
real cost weigh, on the now-honest predicate). **ADR 133+** — multi-way star-join
ordering (recursive fragmentation of a nested broadcast probe — real substrate
work). Later — Wendelon-backed live table-size facts (flip dark-launch to
default-on); optimizer `EXPLAIN` over the reason receipts (now able to report
`ProbeNotScanLinear`).

## Promotion criteria
Filtered/projected probes infer broadcast (fails on `main` first);
nested/global/limited probes stay hash; filtered-probe broadcast byte-identical to
hash; build-side single-base + hard-byte intact; 125/128/129/130 gates green; the
five `adr131_*` guards + retargeted `adr130_decision_helper_single_seam` +
unchanged 129 guards hold; `cargo test -p toron`, `--test architecture_test
adr129 adr130 adr131`, `cargo check -p toron --all-targets`, `git diff --check`
all green.
