# ADR 132: Smaller-Build Dominance Comparator

**Status:** Promoted. **Genuine 3-of-3 (Claude, Codex, Gemini).** The first real
**cost comparison** in the CBO arc: when *both* join inputs are eligible broadcast
builds, choose the materially-smaller build instead of always-left. Binder-local;
reuses 130's right-build swap and 131's scan-linear probe predicate unchanged. No
substrate change.

**Category:** Toron interior — optimizer cost comparison (binder-local).
**Predecessors:** 129 (optimizer fact spine; single-side threshold), 130
(right-build swap + restoring `Project`; left-wins tie), 131 (scan-linear probe
eligibility — made the predicate honest so two *legal* candidates can be compared).
**Migration:** `plans/impl/132_smaller_build_dominance_comparator.md`.

## Context

129 chose broadcast only when a build input was known tiny (single-side
threshold). 130 added right-side build authoring (operand swap + restoring
`Project`). 131 constrained inferred broadcast to scan-linear probes. That left one
artifact from the deliberately small first cut: when **both** inputs are
broadcast-eligible, the planner always tries the left build first
(`both_sides_tiny_prefers_left_build`, `binder.rs:7509`). Left-bias is the right
default for ties, but not when the right side is materially smaller — in the
star-schema shape this arc is opening, both sides may be below the conservative
threshold, and choosing the smaller build is the primitive cost decision we
actually mean to make.

## Doctrine — eligibility is proven first; cost is weighed only among the eligible
> 129–131 each answered "is THIS side a legal tiny build?" 132 is the first
> **two-sided** question: "both are legal — which is the better build?" A rule may
> compare costs only after eligibility is independently proven; eligibility and
> cost-dominance are separate predicates. The comparison is the degenerate base
> case of CBO — two *hard* `ByteCount` upper bounds already on
> `PlanOptimizerFacts`. No cardinality model, no selectivity, no new facts. It is
> **left-biased**: right-build carries 130's permanent restoring `Project`, so the
> smaller-right side wins only when *materially* smaller, not merely smaller —
> `min(bytes)` is not a complete cost function and we do not pretend it is.

## Decisions

**D1 — Dominance only among already-eligible builds.** A side is a *viable build*
iff it already passes 129/131: `Auto`, `Inner`, `origin == SingleBaseRelation`,
hard `byte_upper_bound < threshold`, AND the **opposite** side is
`broadcast_probe_is_scan_linear`. The comparator runs only when **both** sides are
viable; only-one-viable is unchanged (129 left / 130 right); neither-viable falls
through the existing 131 hash-reason ladder unchanged.

**D2 — Materiality margin = named INTEGER factor, exact `u128` math.** Prefer
right-build only when right is at least 2× smaller than left:
```rust
const AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR: u64 = 2;
fn right_build_materially_smaller(left_bytes: ByteCount, right_bytes: ByteCount) -> bool {
    u128::from(right_bytes.get()) * u128::from(AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR)
        <= u128::from(left_bytes.get())
}
```
`u128` multiplication is exact — no float rounding, no `u64` overflow/saturation
edge case. **No float** (avoids sloppy `as f64` casts on byte counts). **No config
knob** (129/130 discipline) — a named constant, architecture-guarded local to
`binder.rs`. The value 2× is the one deliberately-tunable policy number; a later
ADR can pick a milder exact ratio on evidence — a one-line change, no shape change.

**D3 — Left-biased hysteresis.** Exact ties, near-ties, and "right smaller but not
materially" all keep left-build (Natural). Only a material right win flips to
`SwappedRightBuild`. Subsumes 130's left-wins tie as the limit of one honest rule.

**D4 — Enrich the decision receipt (private).** Extend
`JoinStrategyDecisionReason::BroadcastInferred` with
`rejected_side_bytes: Option<ByteCount>` — the byte bound of the *losing* eligible
candidate (`None` when only one side was viable; `Some(other)` when the comparator
chose between two). Carries the magnitude a future `EXPLAIN` needs. Binder-private
— **no SQL/Rune EXPLAIN surface, no wire change** this cut. No `BuildChoiceReason`
enum: the reason is derivable from `build_side` + the two byte values, and one
field is the smaller cut.

**D5 — Reuse 130/131 mechanics verbatim.** Right-build is still the binder-local
operand+key swap + restoring `Project`; left-build is still Natural. The comparator
only chooses *which* placement; it constructs no new physical shape. No new
`JoinStrategy`, no `ExecutionPlan::Join` field, no executor/fragmenter/scheduler/
wire/Wendelon/`PlanOrigin` change.

**D6 — Single seam.** The comparator is a private helper called inside
`choose_auto_join_strategy`. `bind_join` still passes only facts+probe bools and
reads no bytes/threshold (preserves `adr129/131_strategy_decision_*_single_helper`).

## Behavior gates
- `both_sides_eligible_right_materially_smaller_prefers_right_build` — left=3072,
  right=1024, threshold 4096 → `1024*2=2048 <= 3072` → right-build swap + restoring
  projection; `rejected_side_bytes == Some(3072)`. **Fails on `main`** (always-left).
- `both_sides_eligible_near_tie_keeps_left_build` — left=1536, right=1024 →
  `2048 > 1536` → left (Natural); `rejected_side_bytes == Some(1024)`.
- `both_sides_tiny_equal_keeps_left_build` — **retarget**
  `both_sides_tiny_prefers_left_build` (1024/1024) → still left, now proving the
  margin (not first-wins); `rejected_side_bytes == Some(1024)`.
- `sole_eligible_side_reports_no_rejected_bytes` — left-only / right-only tiny →
  that side; `rejected_side_bytes == None`.
- `dominance_right_build_byte_identical_to_hash_over_tcp` — a comparator-selected
  right-build is digest+order-identical to hash (path newly reachable via the
  comparator; reuses the 130/131 harness).
- Regressions green: 129 left-only, 130 right-only + output-order parity, 131
  filtered/projected probe + scan-linear fences, pinned hints fail-loud, 125/128.

## Architecture guards
- `adr132_dominance_comparator_in_single_helper` — comparator in
  `choose_auto_join_strategy` (or a private fn it calls), never in `bind_join`.
- `adr132_dominance_is_integer_ratio_over_hard_bytes` — uses the named
  `AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR` `u64` constant with `u128` integer math
  through `ByteCount::get()`; **no `f32`/`f64`**, no `row_upper_bound`, no
  selectivity.
- `adr132_left_biased_hysteresis` — ties/near-ties resolve left (retargets 130's
  left-wins-tie coverage rather than deleting it).
- `adr132_broadcast_inferred_receipt_carries_rejected_side_bytes` —
  `BroadcastInferred` carries `rejected_side_bytes: Option<ByteCount>`.
- `adr132_no_config_or_public_explain_surface` — no new `PlannerConfig` field; no
  public EXPLAIN rendering of the receipt; constant `binder.rs`-local.
- `adr132_no_executor_fragmenter_scheduler_wire_or_wendelon_change`.
- 129/130/131 guards stay green (single-seam, scan-linear walk, right-build rewrite).

## Non-goals
No hash-vs-broadcast cost model (only build-side choice among eligible). No
multi-way ordering (133+). No selectivity / row-cardinality (no model exists). No
Wendelon facts producer. No `EXPLAIN` surface (D4 only stages the receipt). No
config knob. No float margin. No second magic constant (ratio+absolute-delta hybrid
deferred unless evidence demands).

## Council history
iter-0: 3-of-3 on the topic (Codex's deferred-from-131 dominance pick; Gemini and
Claude independently same). iter-1: forks = margin form/value + receipt. Claude
held **integer** on grounding (`ByteCount: u64`); conceded the receipt to Gemini's
`rejected_side_bytes`. iter-2: **Gemini conceded the float→integer** ("I fully
adopt the Codex/Claude grounding"); Codex contributed the exact `u128` multiply
(over `saturating_mul`); all three landed identical. The form/receipt split is the
discipline in action — *hold* on grounding (float-on-bytes), *concede* on the
genuinely-better peer shape ([[feedback_concession_cycle_ground_not_headcount]]).

## Follow-ons
133+ multi-way star-join ordering (recursive probe fragmentation — real substrate
work). Then Wendelon-backed live facts (flip dark-launch default-on) and optimizer
`EXPLAIN` over the now-richer receipts — 132 makes EXPLAIN worth building (≥2
comparison-bearing decisions, and `rejected_side_bytes` gives it the magnitudes).

## Promotion criteria
Both-eligible material-right-win infers right-build (fails on `main` first);
near-tie/equal keep left; sole-eligible reports `None` rejected bytes; right-build
output byte-identical to hash; 129/130/131/125/128 gates green; the six `adr132_*`
guards + retargeted tie coverage + 129/130/131 guards hold; `cargo test -p toron`,
`cargo test -p toron --test architecture_test -- adr129 adr130 adr131 adr132`,
`cargo check -p toron --all-targets`, `git diff --check` all green.
