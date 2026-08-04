# ADR 333 - Implementation plan: memory-family scale re-measure (release + diagnostic retention)

Pairs with `plans/adr/0333_memory_family_scale_remeasure.md`. A **measurement probe** in `toron-tpch` - no engine
cut. Re-measures q07/q14/q17/q18/q19/q21 @ sf=1 on the **release** build with **ADR 332 diagnostic retention** and
classifies each cell, naming the memory wall(s) for the next arc. Built binary, capped, mechanical classification.

## Phase 0 - build once, set the caps
- `scripts/cargo-cgroup build -p toron-tpch --release` once -> run `target/release/toron-tpch ...` directly under
  the cgroup memory + wall caps (a `cargo-cgroup run` rebuild looks like a hang - ADR 282).
- Caps (ADR 330, fixed up front so classification is mechanical): `MemoryMax=4G`, `RuntimeMaxSec=300s` @ sf=1.
- Run the six probes sequentially, not in parallel, so each query gets an isolated service result/RSS/wall reading.

## D1 - the re-measure (the six cells)
- For each q in {q07,q14,q17,q18,q19,q21}: `target/release/toron-tpch run --query qNN --mode strict --sf 1` under
  the cgroup memory ceiling + wall timeout. If the harness emits a row, capture `status`, `reason`, the
  **diagnostic_message** (ADR 332), RSS, and wall. If the service is killed before a row is emitted, record the
  systemd result/kill signal, peak RSS, wall, and the absence of a row.
- q17 note: its oracle is pinned at sf=0.02 (reach-only at scale) - this is a **reach** probe (`run`), so q17 @ sf=1
  reach is in scope; correctness stays reach-only.

Probe command shape (required evidence; direct release binary, capped, sequential). Keep running after non-zero
service exits; fail rows and kills are expected evidence, not probe failure:
```
for q in q07 q14 q17 q18 q19 q21; do
  systemd-run --user --same-dir --wait --collect --pipe \
    -p MemoryMax=4G -p MemoryAccounting=yes -p RuntimeMaxSec=300s \
    target/release/toron-tpch run --query "$q" --mode strict --sf 1 || true
done
```

## D2 - classify each cell (fixed rules)
- **now-pass** - finishes within the memory and wall caps, `status=pass`.
- **clean `memory_budget_exhausted`** - `status=fail` with `reason=memory_budget_exhausted` (the
  `diagnostic_message` contains `BudgetExhausted` and `dimension: Memory`).
- **OOM-guard-gap** - the process is OOM/cgroup-killed with **no** `memory_budget_exhausted` row (no clean guard
  fire). Record the kill signal + peak/last RSS as available.
- **time** - wall cap exceeded (killed), not a memory ceiling hit.
- **other row-fail** - a row is emitted with a non-memory reason (including `unrecognized` with a diagnostic
  message). Record the reason + diagnostic and route it separately; do not force it into the memory family.
- Row evidence wins over process status: a `memory_budget_exhausted` row is a clean guard fail even though the
  process exits non-zero. If no row is emitted, classify from the systemd result (`oom-kill`/memory ceiling vs
  `timeout`/`RuntimeMaxSec`).
- Record the diagnostic_message verbatim per row-emitted failing cell (so the operator shape - hash-join build /
  aggregate state / sort - is visible for scoping the streaming/spill arc). For killed cells, record that no
  diagnostic row was emitted.

## D3 - the memory atlas (the deliverable)
- Assemble `(query @ sf=1) -> {now-pass / clean memory_budget_exhausted / OOM-guard-gap / time / other row-fail}`
  with per-cell RSS + wall + named reason + diagnostic_message when present, or service result + no-row evidence
  when killed. Record in the results doc. Honest - greens and walls.
- Update the ADR 330 atlas cells for these six (debug oom-kill -> the release-measured classification).

## Tests
- This is a probe, not a feature - no new engine assertions. The existing `toron-tpch` suite stays green (the ADR 332
  retention + classifier tests already cover the telemetry path). If any cell is **now-pass**, note whether a keyed/
  reach canary is warranted (only if it is a durable green worth guarding - decided from the measurement, not
  pre-committed).

## Phase 3 - read the split, name the next arc (no fix here)
- Summarize: how many free wins; how many clean `memory_budget_exhausted`; how many OOM-guard-gaps; any time walls;
  any other row-fails.
- Route (results doc): OOM-guard-gap -> "extend budget-guard coverage" ADR first; clean `memory_budget_exhausted` ->
  "cost-bound streaming/spill" ADR, scoped to the cells + their operator shapes; time -> perf arc; other row-fail ->
  route by emitted reason/diagnostic. **Do not fix any wall in this ADR.**

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix   # 22/22 @ sf=0.01 unchanged
scripts/cargo-cgroup run -p toron-tpch -- matrix               # the named cells render
git diff --check
# required probe evidence (built release binary, capped, sequential; expected nonzero exits for fail rows):
scripts/cargo-cgroup build -p toron-tpch --release
for q in q07 q14 q17 q18 q19 q21; do
  systemd-run --user --same-dir --wait --collect --pipe \
    -p MemoryMax=4G -p MemoryAccounting=yes -p RuntimeMaxSec=300s \
    target/release/toron-tpch run --query "$q" --mode strict --sf 1 || true
done
```

## Results doc
`crates/toron/plans/impl/0333_memory_family_scale_remeasure_results.md`: the memory atlas for the six cells (release +
retention) with per-cell RSS + wall + named reason + diagnostic_message when present, or service result + no-row
evidence when killed; the split counts (free wins / clean budget-fail / OOM-guard-gap / time / other row-fail);
the next fix arc(s) named (guard-coverage first if any OOM-gap, then streaming/spill, scoped to the cells; time or
other row-fail routed separately); the ADR 330 atlas update; sf=0.01 boards unchanged.

## Risk / blast radius
- **Probe only** - measure + name + classify; no mid-probe fix; honest cells.
- **Cost** - sf=1 x 6 on release; caps mandatory; built binary direct; no blind sweep.
- **Classification honesty** - fixed rules (D2); OOM-without-guard-fire is a coverage gap, a `time` cell is a perf
  wall, and an other row-fail routes by its own diagnostic - none is mis-bucketed as a streaming gap.
- **No engine change** - sf=0.01 boards + the q04/q12/q20 named cells stay as they are.

## Seal
`impl: adr 333 memory family scale remeasure`. Do not commit wip drafts.
