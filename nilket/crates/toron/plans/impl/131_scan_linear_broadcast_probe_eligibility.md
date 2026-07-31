# Impl Plan — ADR 131: Scan-linear Broadcast Probe Eligibility

**Migration for** `plans/adr/131_scan_linear_broadcast_probe_eligibility.md`.
**Scope:** `crates/toron/src/binder.rs` (new `broadcast_probe_is_scan_linear` +
`choose_auto_join_strategy` predicate split + `JoinInputOptimization` seam),
architecture guards, behavior tests.
**Non-scope:** executor, fragmenter, scheduler, wire, Wendelon, config,
`PlanOrigin`, `JoinStrategy`, `ExecutionPlan::Join`, ADR 130's swap/projection.

## Pre-flight anchors (grounded)
| Concern | Anchor |
|---|---|
| both-sides fence to split | `binder.rs:3794-3795` (`RightInputNotSingleBaseRelation`) |
| left-build / right-build branches | `binder.rs:3797-3809` / `:3810-3822` |
| why not PlanOrigin (Filter→Composite) | `binder.rs:320` |
| decision seam (pinned vs auto) | `choose_join_strategy` → `choose_auto_join_strategy` (`binder.rs:3760`/`:3779`) |
| bound child plans in bind_join | `left_plan.plan` / `right_plan.plan` (`ExecutionPlan`); `bind_join` `binder.rs:1075` |
| ExecutionPlan variants (exhaustiveness) | `ir/execution.rs` enum — 18: Scan, Filter, Project + EmptyRelation, SystemTable, Aggregate, Exchange, Sort, Take, Join, CreateTable, CreateLogicalType, CreateTableAsSelect, DropTable, Delete, Update, Insert, InsertSelect. **No `Alias` variant.** |
| Scan carries row_limit | `execution.rs:207` (`row_limit: Option<usize>`) |
| share-scan probe (why scan-linear) | `fragment_broadcast_join_plan` embeds probe bare (`fragment.rs:504`); consumer inputs bind `TaskInputBinding::Scan { share }` (`fragment.rs:526-533`) |
| not-reusable LogicalPlan walk | `sql_planner.rs:5805` (`plan_contains_join`, wrong layer) |
| F1 is 130-introduced | absent `56a5bc8:binder.rs`; present at HEAD |
| 130 guard to retarget | `adr130_decision_helper_single_seam` (pins the obsolete right-origin check) |

## Phase 0 — failing gates first
- `left_tiny_with_filtered_probe_infers_broadcast` must **fail on `main`** (tiny
  `customers` left, filtered `orders` right; expected `Broadcast`/Natural after,
  hash before — proves the regression).
- `probe_with_aggregate_stays_hash` passes before AND after (pins scan-linear ≠
  join-free).
- `probe_with_take_or_limited_scan_stays_hash` (per-worker-limit hazard).
- `probe_containing_join_stays_hash` stays green (130 fence preserved).

## Phase 1 — `broadcast_probe_is_scan_linear`
Add the private helper from ADR §D2 near the other join decision helpers.
**Exhaustive** match over all 18 `ExecutionPlan` variants (no `_` wildcard):
`Scan { row_limit, .. } => row_limit.is_none()`; `Filter|Project => recurse(input)`;
all other 15 => `false`. Do **not** add an `Alias` arm (no such variant — would
not compile). Grep first for any existing `ExecutionPlan` scan/join walk (expect
none; `plan_contains_join` is `LogicalPlan`-typed).

## Phase 2 — thread eligibility through the seam
Add `#[derive(Debug, Clone, Copy)] struct JoinInputOptimization { facts:
PlanOptimizerFacts, probe_scan_linear: bool }`. In `bind_join`, after binding both
children, build `left_input`/`right_input` (facts from `*_plan.facts`,
`probe_scan_linear` from `broadcast_probe_is_scan_linear(&*_plan.plan)`) and pass
them into `choose_join_strategy`. Pinned decisions ignore the booleans. No
byte/threshold logic in `bind_join`.

## Phase 3 — split build/probe candidate logic
In `choose_auto_join_strategy`, remove the unconditional `*_facts.origin !=
SingleBaseRelation` rejects. Evaluate each side's **build** eligibility (origin +
hard bytes < threshold) independently, gated on the **opposite** side's
`probe_scan_linear`:
```rust
if left build-eligible && right.probe_scan_linear  => broadcast_left  (Natural)
else if right build-eligible && left.probe_scan_linear => broadcast_right (SwappedRightBuild)
else hash(reason)
```
Left checked first → left-wins tie (130). Add
`HashByDefaultReason::ProbeNotScanLinear` for the tiny-build-but-probe-not-linear
case; keep missing/above-threshold reasons deterministic.

## Phase 4 — guards (add + retarget)
Add the five `adr131_*` guards (ADR §"Architecture guards"). **Retarget**
`adr130_decision_helper_single_seam`: remove assertions requiring the obsolete
unconditional `right_facts.origin != SingleBaseRelation`; keep the assertions that
the helper consumes both input facts, checks both build sides, and preserves
`BroadcastBuildSide` / `JoinPhysicalPlacement`. Retarget explicitly — never a
silent deletion.

## Phase 5 — verification
`cargo test -p toron` (targeted: the four Phase-0 gates + the two positive
broadcast gates + `filtered_probe_broadcast_byte_identical_to_hash_over_tcp`) ·
`cargo test -p toron --test architecture_test adr129 adr130 adr131` · `cargo check
-p toron --all-targets` (catches an E0063 if a new struct field hit a
widely-constructed type) · `git diff --check`. The 129/130 left/right-build
digest+cap gates stay green — this widens probe acceptance only.

## Risk register
- **Too-broad probe (the corrected bug):** Aggregate/Sort/Take/Exchange under
  share-scan are not row-local; the exhaustive match + `probe_with_aggregate_stays_hash`
  are the backstop.
- **Limited-scan leak:** a pushed `Take` surfaces as `Scan { row_limit: Some(_) }`;
  the helper must reject it (`row_limit.is_none()`).
- **No `Alias` arm:** matching a nonexistent variant fails to compile — omit it.
- **Seam drift:** `bind_join` may compute booleans but must not read byte facts or
  thresholds (`adr131_strategy_decision_remains_single_helper`).
- **Guard inversion hidden as deletion:** retarget the 130 guard explicitly
  (discipline #4 — reconcile inverted guards).
- **Right-build output drift:** swapped right-build probes still need 130's restore
  projection + digest/order parity.

## Acceptance
Filtered/projected scan probes infer broadcast (left + right);
nested/global/limited probes stay hash; right-build output byte-identical to hash;
build single-base + hard-byte intact; 125/128/129/130 gates green; no
executor/fragmenter/scheduler/wire/PlanOrigin change; the five `adr131_*` guards +
retargeted `adr130_decision_helper_single_seam` + unchanged 129 guards green.
