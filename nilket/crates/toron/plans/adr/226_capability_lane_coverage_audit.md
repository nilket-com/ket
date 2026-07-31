# Toron ADR 226: Capability-Lane Coverage Audit

**Status:** Promoted. **Migration:** `plans/impl/226_capability_lane_coverage_audit.md`.
**Category:** Toron optimizer instrumentation — a **negative-result audit** that proves ADR
225's `capability_limited` class contains no contractable memo opportunity today, and installs
the permanent tripwire that any future `admissible` row must clear. **Docs + tests + guards
only.** No production logic change: `bounded_admission_candidate_contract` stays an all-zero
stub; **zero `admissible` rows**; no memo engine / actuation / candidate generator; no new
decline reason; no decision-matrix change (stays 140/220); no `information_schema` / schema /
durable-key / Wendelon change.

## Why this cut exists

The first ADR 226 (exact-two-probe star contract) was promoted, implemented, and **falsified
at code review**: the `admissible` row it lit was reachable only by *seeded* decision rows; a
real two-probe galaxy commits `galaxy_root reordered` because graph recognition already
handles the shape. The plan commit was reset and the implementation dropped.

The lesson is load-bearing: a memo-admission contract is not justified by a recognizer-local
decline or a seeded row — it needs a **production home**. 225's `capability_limited` class was
designed as "where memo earns its keep," on the argument "the recognizer declines entirely →
no candidate is generated." That grounded only the *star* recognizer's decline; it never
checked whether the decline reaches the durable **decision lane**, or whether a *sibling*
recognizer commits a reorder for the same shape. This cut closes both gaps by auditing every
capability reason against a production-home test — and records the result, which is negative.

## The production-home test (promotion-locked predicate)

A capability decline reason is contractable **only if all five hold**:

1. A **real query** (not a seeded row) commits a durable decision-lane `Declined(reason)` for
   a specific `(decision_kind, provenance)`.
2. The row is **pure `capability_limited`** under the 225 blocker split (no margin/hard mix).
3. **No sibling recognizer** already handles the shape and commits a reorder/keep.
4. The missing alternatives are **finite, semantics-preserving, and not blocked by a physical
   execution fence or a typed authorization boundary**.
5. A **behavioral gate** drives `optimizer_memo_admission` to the contract row from that real
   query, with **no synthetic decision writes**.

## The audit (grounded; anchors on this `main`, HEAD = adr 225 feat)

| reason | production home? | why it fails | behavioral gate asserts |
|---|---|---|---|
| `MultipleProbeRelations` | **No** (fails #1, #3) | emitted only in the star source `analyze()` (join_order.rs:2405), which feeds render/EXPLAIN and counterfactual receipts (binder.rs:270/431), **never** `accrue_star_order_decline`. Real conformed multi-fact shapes are graph-family: they commit `galaxy_root` and re-root through `reroot_galaxy_join_tree`. | a real conformed galaxy commits `galaxy_root`, not a star decline |
| `MixedStrategyStar` | **Yes** (fails #4) | reaches the lane via `bind_local_star_actuation` declines (join_order.rs:4801/4834) → `FlatStar`/`SnowflakeRoot`. But ADR 193 **already actuates** the cost-optimal broadcast-first/hash-last order when authorized; the residual declines are **typed-authorization boundaries** (`HashOnlyCompositeProbe` branch / non-clean mixed). The candidate order is known, not missing. | a real decline binds to a **valid plan** and is authorization-fenced |
| `ProbeNotScanLinear` | **Yes** (fails #4) | reaches the lane via `accrue_star_order_decline(StarCandidate, …)` (join_order.rs:4217) on `!broadcast_probe_is_scan_linear(probe_plan)` — a **physical execution fence**; binding falls back to the valid hash plan. A generator cannot make a non-scan-linear probe scan-linear. | a real decline **falls back to a valid hash plan** |

Hard declines (`NonInnerEquiJoin`, `MissingRelationLabel`, `DuplicateDimension`,
`MissingEstimate`) are out of scope — 225 already classifies them non-memo (no candidate
exists / no estimate to choose with).

## Decisive finding

**No capability decline passes the production-home test.** The `capability_limited` class is
real as a *classification* but **empty as a memo opportunity**: the existing optimizer
families (graph galaxy/bridge/chain recognition + star actuation + the ADR 193 fragmenter
broadcast-first/hash-last order) already generate every candidate the optimizable shapes
admit; the residual capability pressure is authorization- or physically-fenced.
Exact-two-probe was the most promising reason and it failed on graph coverage. No remaining
reason is contractable.

## Decisions (promotion-locked)

**D1 — Record the negative as canonical.** This ADR carries the production-home test and the
per-reason audit (no-home / authorization-fence / physical-fence) with code anchors, so a
future ADR cannot re-attempt a redundant contract without re-running it.

**D2 — Contract stays all-zero; zero `admissible` is absolute.**
`bounded_admission_candidate_contract` returns `bounded_candidate_count = 0` for every input
(confirmed on HEAD — the falsified nonzero version was dropped with the reset); no row renders
`admissible`.

**D3 — Three real-query behavioral gates** prove the audit, each a real bind through the
normal production bind path with no seeded decisions: a conformed galaxy commits
`galaxy_root` (not a star decline); a `MixedStrategyStar` decline binds to a valid plan and is
authorization-fenced; a `ProbeNotScanLinear` decline falls back to a valid hash plan. The
decision-lane assertions read `information_schema.optimizer_decisions` after `bind` /
`bind_with_trace`; EXPLAIN receipts may be used only as secondary shape evidence, never as the
primary production-home proof.

**D4 — Tripwire.** `adr226_no_capability_decline_is_currently_contractable` locks the all-zero
contract. Any future ADR that returns a nonzero contract must retarget this guard, and the
retarget must ship a **real-query production-home fixture** (criterion #5) — never again a
seeded-decision `admissible` row. The three D3 gates are the demonstrated methodology.

**D5 — Retarget prior "future bounded contract" language.** 223/224/225's forward-looking
"a later bounded contract makes `admissible` reachable" becomes "contract only after
production-home proof." Capability rows are **not** automatically candidates.

**D6 — No source logic / durable / matrix change.** Docs + tests + guards only. Explicitly
**not** reintroducing the falsified exact-two split: no `TwoProbeRelations`, decision matrix
stays 140/220, no schema/table/durable-key change.

## Honesty gates (discriminating)

- `memo_audit_multiple_probe_relations_commits_graph_decision_not_star_decline` — real
  conformed galaxy commits `galaxy_root` under normal bind; the star
  `MultipleProbeRelations` decline never reaches the durable lane.
- `memo_audit_mixed_strategy_star_is_authorization_fence_not_missing_candidate` — real decline
  exists in the decision lane, binds to a valid plan, fails only typed authorization.
- `memo_audit_probe_not_scan_linear_is_physical_fence_not_missing_candidate` — real decline
  exists in the decision lane, binding falls back to a valid hash plan.
- `memo_admission_still_renders_zero_admissible` (absolute).
- `bounded_admission_candidate_contract_returns_zero_for_all_inputs`.

## Architecture guards (promotion-locked)

- `adr226_no_capability_decline_is_currently_contractable`
- `adr226_memo_admission_still_renders_zero_admissible`
- `adr226_future_admissible_row_requires_production_home_proof` (the all-zero contract is the
  enforced lock; the production-home fixture is the documented retarget bar)
- `adr226_capability_audit_makes_no_source_logic_or_matrix_change`

**Reconciled prior guards (named):**
- ADR 223/224/225 "future bounded contract" language/guards → **production-home-gated**
  (capability rows are not automatically candidates; the zero-admissible guards stay absolute).
- ADR 216 decision matrix → **unchanged at 140/220** (no new decline reason).
- ADR 220/222 conflict + window surfaces → unchanged.
- ADR 221/197 no-memo fences → unchanged (still no engine; the readout/stub names persist).
- ADR 217 cockpit / 225 blocker split → unchanged.

## Consensus (transparent)

3-of-3, and unanimous on a consequential negative. Claude's code review falsified the first
226 (seeded-only `admissible`; graph already covers the shape) and proposed the capability-
lane audit. Codex contributed the five-criterion **production-home test** (subsuming Claude's
four-point audit and adding the behavioral-gate enforcement — the exact catch that killed the
first cut) and the **"no home" vs "home but fenced"** distinction. Gemini synthesized the
real-query honesty gates and the absolute zero-admissible discipline. All three reached the
same three FAIL verdicts independently and the same conclusion: close the memo arc honestly
rather than hunt a first contract after the most promising candidate proved redundant. No
residual divergence.

## Forward arc

```text
225 blocker-class readout
-> 226 capability-lane coverage audit (this; the lane is empty; permanent tripwire)
-> rotation away from generic memo admission — no memo cut unless a new production-home
   fixture appears. Next plan-changing work is non-memo: a physicalization / fragmenter
   follow-on for the scan-linearity or mixed-strategy fence, or the next receipt-gated topic.
```

The optimizer-evidence arc (207→225) is complete and fully instrumented; its enduring value
is this grounded answer to "should we build a memo engine?" — *not now, here is the per-reason
proof, and here is the tripwire if a genuinely uncovered shape ever appears.*

## Deferred (named)

Any memo engine / candidate generator / actuation / selection / `admissible` row; generic
Cascades search; memo arena; candidate memoization; the exact-two-probe split (falsified);
physicalization for `ProbeNotScanLinear`; an authorization path for `MixedStrategyStar`;
mixed-chain pushdown; deployment elasticity signal; window-key GC; SQL parenthesized join
factors.
