# ADR 334 - Implementation plan: name the memory-accounting escape path

Pairs with `plans/adr/334_memory_accounting_escape_path.md`. An **engine grounding** in `crates/toron`
(exec.rs / distributed/budget.rs), driven via `toron-tpch` reach at sf=1. **Deliverable: name the allocation that
escapes the budget guard (or refute the hash-join hypothesis) - no fix.** Release build, cgroup capped, summary-not-
dump.

## Phase 0 - the two-sided, evidence-separated trace

### Side A - accounted: name q04/q12/q20's exact `BudgetExhausted{Memory}` producer
- Trace where q04/q12/q20's clean `memory_budget_exhausted@execute` actually originates: the `reserve(Memory)` call
  or the executor estimate/path that emits `ExecutionError::BudgetExhausted { dimension: Memory, ... }` directly.
  **Record file:line** (not "the accounted side").
- This is the contrast: the same machinery the six bypass. Confirm whether it's the build pre-check or a different
  operator/path (repeat, sort, window, spilled join writer, semi-join, exchange, or another executor site).

### Side B - escaping: instrument q18 first (cleanest gen-fit), separate the six sites
- q18 is the representative: `q18_tables() == q03_tables()` and q03 fits under 4G, so any q18 OOM is execute, full
  stop. Build release; run q18 strict @ sf=1 under the cgroup cap with a one-shot `eprintln` (summary-not-dump) at
  the join build site that emits before large allocations and at/near the OOM:
  - **(1) precheck estimate** - `estimated_build_hash_bytes(key_arrays, rows)` vs `memory_cap`; did
    the direct precheck at exec.rs:2985-2988 route to spilled join, or did it enter the in-memory path?
  - **(2) with_capacity** - `rows` and the estimated `HashMap::with_capacity(rows)` footprint (bucket bytes for all
    rows). Print immediately before this allocation.
  - **(3) per-row growth** - `inserted_rows` reached + the running `BuildRowList` byte estimate at OOM.
  - **(4) retained / concatenated Arrow batches** - live child-output batches plus `concat_or_empty` left/right batch
    bytes held during the build (not in the estimate).
  - **(5) spilled fallback build map** - did the direct precheck or per-row `Ok(None)` enter
    `execute_spilled_hash_join`? If so, print before and during the pre-spill `HashMap<Vec<u8>, Vec<u32>>` build
    (exec.rs:3807-3823), and record whether this path has any memory reservation/check before the map is complete.
  - **(6) spilled writer resident state** - if the run reaches `SpilledJoinBuild::write_inner`, record page bytes,
    index bytes, writer peak, `resident_without_cache`, and whether the typed memory check at exec.rs:5426-5431
    fires before the cgroup kill.
- Attribute the OOM to **one** site (or to a different operator -> refutation). Estimate-vs-actual is the lead, but
  the spill fallback pre-write map is an equally plausible "budget-aware but mistimed" escape until q18 proves
  otherwise.

### Broaden only after q18 confirms
- Once q18's mechanism is named, run q07/q14/q17/q19/q21 @ sf=1 and confirm each shares it (same site fires) or note
  where it differs. Do **not** instrument all six up front.

## D2 - record (the deliverable)
- The **named mechanism** (one of the six sites, or a different operator) with the separating evidence; **or** the
  **refutation** with the actual path named.
- q04/q12/q20's **exact producing site** (file:line) for contrast.
- q18's confirmed mechanism + which of q07/q14/q17/q19/q21 share it.

## Tests
- This is a grounding probe - no engine assertion lands here (the fix is the next ADR). The instrumentation
  `eprintln` is removed before seal (kept only as a recorded probe command in the results doc, per the ADR 333
  pattern). Existing `toron-tpch` + `toron` suites stay green.

## Phase 3 - name the next arc (no fix)
- Results doc states the named mechanism -> the scoped fix ADR (e.g. "count BuildRowList + live Arrow batches in
  `estimated_build_hash_bytes`" / "reserve before `with_capacity`" / "account the pre-spill fallback build map" /
  "route fallback through a typed memory budget check"), **or** the refutation -> the actual operator's fix.
  **No fix in this ADR.**

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron-tpch
git diff --check        # the probe eprintln is removed; no instrumentation left in source
# required probe evidence (release, cgroup capped, q18 first then broaden; expected nonzero while OOM is diagnosed):
scripts/cargo-cgroup build -p toron-tpch --release
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=300s \
  /bin/sh -lc 'target/release/toron-tpch run --query q18 --mode strict --sf 1' || true
# then q07/q14/q17/q19/q21 with the same cap once q18's mechanism is confirmed
```

## Results doc
`crates/toron/plans/impl/334_memory_accounting_escape_path_results.md`: the named escape mechanism (or refutation)
with the six-site separating evidence; q04/q12/q20's exact `BudgetExhausted{Memory}` producing site (file:line);
q18's confirmed mechanism + the broaden check across q07/q14/q17/q19/q21; the scoped fix ADR named. The gen-fits
proof restated. No engine fix; sf=0.01 boards unchanged.

## Risk / blast radius
- **No assumption** - six-site separation mandatory; the estimate is the lead, not the verdict.
- **q18 first** - one representative before the rest; broaden only on confirmation.
- **Exact contrast site** - q04/q12/q20 producer named file:line, not "accounted side".
- **Probe only** - `eprintln` removed before seal; no engine change; the fix is the next ADR.

## Seal
`impl: adr 334 memory accounting escape path`. Do not commit wip drafts.
