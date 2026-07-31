# ADR 334 - name the memory-accounting escape path

## Status
Proposed.

## Context

The memory arc's second cut, the sequel to ADR 333. **First deliverable: NAME the allocation that escapes the
memory budget guard - not fix it.** ADR 333 proved six cells (q07/q14/q17/q18/q19/q21) OOM-kill at sf=1 with no
`BudgetExhausted{Memory}` row, while q04/q12/q20 clean-fail. This ADR grounds *which* allocation reaches the cgroup
ceiling without the guard firing, and stops at the named path + evidence. The fix is a scoped follow-on.

### Proof baked in - gen fits, execute escapes accounting (ADR 333 + Codex containment)
- **q18** uses `q03_tables()` *exactly*; q03 fits under 4G, q18 OOMs -> the kill is **execute allocation**, not gen.
- **q14/q17/q19** use the lineitem+part family; q08/q09 post-331 ran keyed supersets successfully, so gen fits.
  If Phase 0 needs direct same-cap evidence, run a cheap generation/known-superset control before tracing, but do
  not reopen ADR 333's classification unless that control contradicts containment.
- **q21** is contained by q05's table family; q05 fits under 4G -> gen fits.
- Therefore ADR 333's six cells are **real execute-time guard-coverage gaps**, not a generation artifact.

### Grounding done - the naive hypothesis is already refined
"The hash-join build has no accounting" is **wrong** - the build *is* budget-aware:
- `build_in_memory_join_rows(..., memory_cap = task_budget.memory.get(), ...)` (exec.rs:3027/3359).
- pre-check `estimated_build_hash_bytes(&left_key_arrays, rows) > task_budget.memory.get()` routes away from the
  in-memory build (exec.rs:2985-2988); the same predicate is wrapped by `join_build_memory_fits`
  (exec.rs:3788-3789) for per-row checks.
- per-row check: the loop bails `Ok(None)` when `inserted_rows` would exceed `memory_cap`.
- the budget machinery is real: `BudgetTracker::reserve(BudgetDimension::Memory, amount)` lives in
  `distributed/budget.rs:1403`, and executor paths also emit typed
  `ExecutionError::BudgetExhausted { dimension: Memory, ... }` directly. Phase 0 must name which exact producer
  created q04/q12/q20's clean rows instead of assuming it was `reserve`.

So the build is budget-aware yet OOMs with no row: the accounting is **inaccurate or mistimed, not absent.** That is
the question this ADR names - precisely *where*.

## Decision - a two-sided, evidence-separated trace that names the escape (or refutes the hypothesis)

The leading hypothesis is an under-counted or mistimed hash-join build, **but the plan must not assume it.** Phase 0
produces evidence that **separates six candidate sites** so the named mechanism is earned, not guessed:
1. **precheck estimate** - `estimated_build_hash_bytes` passes (estimate <= cap) but is wrong.
2. **`HashMap::with_capacity(rows)`** (exec.rs:3362) - up-front bucket allocation for *all* rows, before the per-row
   guard.
3. **per-row growth** - the incremental inserts / `BuildRowList` Vecs exceed the cap between checks.
4. **retained / concatenated Arrow batches** - child-output batches plus `concat_or_empty` left/right batches are
   held live during the build (not in the estimate).
5. **spilled fallback build map** - either the precheck or the per-row `Ok(None)` route enters
   `execute_spilled_hash_join`, which builds an in-memory `HashMap<Vec<u8>, Vec<u32>>` (exec.rs:3807-3823) before
   `SpilledJoinBuild::write` reaches its spill/memory checks.
6. **spilled writer resident state** - `SpilledJoinBuild::write_inner` checks spill bytes and resident memory only
   after pages/index state are produced (exec.rs:5366-5431).
(plus the refutation branch: the OOM is a **different operator** entirely - a wider build, an exchange, a sort.)

### The two sides (with teeth)
- **Accounted (q04/q12/q20):** name the **exact producing site** of `BudgetExhausted{Memory}` (file:line - the
  `reserve(Memory)` call, explicit executor estimate, spill-writer resident-memory check, or other typed
  `ExecutionError::BudgetExhausted` producer). Not "the accounted side" - the literal site. This gives the
  contrast teeth.
- **Escaping (the six):** instrument **one representative first - q18** (because `q18_tables() == q03_tables()` is
  the cleanest gen-fit proof) and separate the six sites at the OOM point. Instrument before large allocations
  (`HashMap::with_capacity`, spill fallback build-map construction, and spill writer page/index construction), not
  only after them, because the process may be killed before a post-allocation print. **Broaden to
  q07/q14/q17/q19/q21 only after q18's mechanism is confirmed** (then verify each shares it, or note where it
  differs).

## Acceptance - exactly one of (no fix)
- **Named allocation mechanism** - one of the six sites (or a different named operator), with the separating
  evidence (estimate vs actual, with_capacity bytes, per-row growth, live Arrow bytes, fallback build map, spill
  writer resident state) and the q04/q12/q20 exact producing site for contrast - ready for a scoped fix ADR; **or**
- **Hypothesis refuted** - the hash-join build is not the escape; the actual escaping path is named with evidence.
- **No fix in this ADR.** The wall becomes observable in-engine; making it fit is the next, scoped cut.

## Risks
- **Don't assume the estimate** - it is the *leading* hypothesis, not the conclusion; the six-way separation is
  mandatory so a different site (with_capacity / Arrow retention / spill fallback / spill writer / other operator)
  is not mis-attributed.
- **One representative first** - q18 before the rest; broadening before confirming wastes sf=1 runs and risks
  bucketing distinct mechanisms together.
- **Contrast needs the exact site** - "accounted side" is not enough; the q04/q12/q20 `BudgetExhausted{Memory}`
  producer must be named file:line or the gap isn't pinned.
- **Probe discipline** - measure + name; no mid-grounding fix (the ADR 333 order).

## Forward arc
The named mechanism scopes the fix ADR (e.g. "count BuildRowList + live Arrow batches in
`estimated_build_hash_bytes`", "reserve before `with_capacity`", "account the pre-spill fallback build map", or
"route the fallback through a typed memory budget check"). Once the six fail cleanly + named, the cost-bound
streaming/spill arc can be sized against observable walls. Make the wall observable first, then make it fit.

## Why toron-home
The budget guard + the join build are `crates/toron` (exec.rs, distributed/budget.rs); the probe drives them via
`toron-tpch` reach at sf=1. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. Grounding refined the hypothesis (the build is budget-aware, so the escape is
inaccurate/mistimed accounting, not absent). Operator guardrails folded: separate the six candidate sites with
evidence (don't assume the estimate); name q04/q12/q20's exact `BudgetExhausted{Memory}` producing site; instrument
q18 first (cleanest gen-fit); acceptance is named-mechanism-or-refuted, no fix. The title stays broad so a refutation
(a different operator) doesn't leave it misnamed.
