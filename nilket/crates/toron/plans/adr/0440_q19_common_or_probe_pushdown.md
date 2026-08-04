# ADR 440 - Factor q19's common OR conjuncts and push them into the probe child

## Context

**439's evidence set the target.** The primitive-index cut took q19 to 71.99 s but left D1 dominant
(D1/root **0.5626**, `probe_lookup_select`/root **0.3204**, D2 ~**0.196**). The wall is still join
probe/output, and M — not lookup speed — is the remaining lever.

**q19's residual is `(A) OR (B) OR (C)`**, each arm AND-ing a distinct brand/container/size/quantity plus
the **common, lineitem-only** `l_shipinstruct = 'DELIVER IN PERSON'` and `l_shipmode IN ('AIR','AIR
REG')`. The 438 census proved these are **physically triplicated, not CSE-shared** (nodes 26/61/97 and
31/66/102). ADR 319 lifted the bridging partkey equality but **deliberately left common non-key conjuncts
in the residual "for a later optimizer cut."** This is that cut.

**Exact selectivity receipt (falsifiable thesis):** lineitem rows / current M = **59,986,052**; rows
passing the common ship filter = **2,141,904**; retained **3.57067%**; expected **M reduction 96.43%**.
Pushing the ship conjuncts before the probe attacks `probe_lookup_select` + `arrow_materialize` (D1) and
the OR-eval (D2) at once.

## Decision — extract the common OR conjuncts in the binder, route them into the probe child

**One owner type keeps the extraction aligned.** Replace the current
`extract_common_or_bridging_keys` tuple return (`Option<(Vec<(Expr,Expr)>, Option<Expr>)>`,
`outer_join_demotion.rs`) with:
```
CommonOrExtraction { keys, left_common, right_common, residual_or }
```
so bridging keys, side-routed common predicates, and the reduced residual stay structurally coherent.

**Fenced to ADR 319's admitted shape.** Common-conjunct extraction runs **only when a common bridging key
already exists across every OR arm** (`keys` non-empty). An OR with no common bridging key remains
**byte-identical** — this rewrite never touches arbitrary ORs.

**Duplicate/empty semantics (explicit — no general simplifier exists):** intersect conjuncts **globally**
across all arms; extract **one structural occurrence** per common conjunct into `left_common`/
`right_common`; remove **one matching occurrence from every arm**. If any reduced arm becomes empty, the
OR is fully captured by the pushed conjuncts → **`residual_or = None`**.

**Routing + seam (already built).** Route each `left_common`/`right_common` conjunct via
`push_into_cross_child` (`outer_join_demotion.rs:1223`, the existing top-level side-routing), producing a
`Filter<Scan>` (or `Exchange<Filter<Scan>>`) probe child. `decompose_streaming_scan_probe_input`
(`streaming.rs:1941`) already consumes that shape — including `FilterDisposition::FullyPushed`
(`:1959-1960`) and exchange wrapping (`:1968, 1993`). **No executor change.** The above-join
`probe_prefilter_predicate` is not the path.

**Structural matcher (no general Expr equality).** Add a **typed, exhaustive recursive equality** over
`Expr`: float **bit** equality, timestamp **units**, Decimal **precision/scale**, cast **types**, and
**every variant field**, exhaustive `match` (new variant ⇒ compile error). Hashing may **accelerate**
candidate lookup but **cannot be the equality gate**. A conjunct is factored only if it is common to every
arm and passes **both** `expr.is_deterministic()` (`ir.rs:863`) **and** `guard_filter_pushdown_safe(expr)`
(`outer_join_demotion.rs:1893`) and is side-local to one cross child.

## Correctness

- **SQL three-valued equivalence.** `(x∧A)∨(x∧B)∨(x∧C) ≡ x∧(A∨B∨C)` in Kleene 3VL; pushing `x` as a
  `WHERE` pre-filter is sound because `x` is a **necessary condition** of the whole OR (a conjunct of every
  arm) — a row with `x` not TRUE satisfies no arm. Pinned by a 3VL test (NULL `x`, NULL arms).
- **Only-if-in-every-arm; duplicate/reordered safe.** A conjunct in some but not all arms is not common.
  Order-insensitive over an arm's AND set; one occurrence removed per arm.
- **`is_deterministic()` + `guard_filter_pushdown_safe()`** both required — `is_deterministic` is always
  true today but preserves the contract for future expression volatility.

## Acceptance

- **Physical shape.** The bound plan's probe child is `Filter<Scan>` **or** `Exchange<Filter<Scan>>`
  (including `FullyPushed` disposition); the ship predicates live **below the probe exchange / at the scan
  boundary**. `residual_or` retains the per-arm brand/container/size/quantity parts.
- **Plan fingerprint** changes from `65c3c2c0b0a99063` (the *plan* fingerprint — a rewrite must move it),
  then stable across all **seven** runs. Arrow IPC digest, schema, row count, revenue value, and
  **51,471,552** spill bytes **invariant** across all seven.
- **Exact M → S reconciliation:** **M = 2,141,904** (not "M fell" — early application proof) → **S =
  1,134**; oracle `171+382+581 = 1,134`.
- **Wall shrinks — measured, ADR 356 dynamic band.** Baseline **439's 71,987.808 ms, spread 0.011899**;
  band = `max(0.10, 2 × max(0.011899, current_spread))`, **minimum 10%**. Unprofiled ADR 356-shaped strict
  sf=10 q19 (1 warmup + 7 measured) must clear the band on median execute wall; **≤0.20 stability, one
  retained retry, never average.** Sub-band → honest-red → revise/remove.
- **Survival:** one strict sf=10 q19 persisted-Parquet **pass@compare under 4 GiB/no-swap**
  (`TasksMax=512`, `RuntimeMaxSec=600`, `join_spill` fresh/empty before and after).
- **Corroboration (not the gate):** five fresh 438-ledger children showing D1 and `probe_lookup_select`
  fall in both absolute micros and share vs. the 439 receipt.
- **No-common-key ORs byte-identical; no query branding.** `exec.rs` 7,499; join root 3,713.

## Guardrails and risks

- **Correctness core** is the structural matcher + only-if-in-every-arm + one-occurrence-per-arm + empty→
  `residual_or=None`; the 3VL/missing/duplicate/reordered tests are its proof.
- **No executor change is locked.** The seam (`decompose_streaming_scan_probe_input`) already consumes
  every relevant shape; **if the bound plan fails to reach that seam, stop and revise the ADR — never add
  executor work.**
- **Band honesty** — the 437 precedent; a below-10% wall does not seal despite the 96.43% M-reduction
  thesis (the full lineitem scan still happens; measure the net).

## Forward arc

Second q19 mechanism. After it, re-diagnose: if D1 finally drops below D2, 439's residual-program
factoring-for-dedup applies to the reduced OR; otherwise the next lever is `arrow_materialize`.

## Deferred (carried)

- **Cut A (439 unconditional mirror)** after generic-accounting parity — standalone `refactor:`.
- **q17 remeasurement** — the cheap stale-baseline gate.
- **q05/q18/q21 throughput diagnosis** — required before any becomes a mechanism.
