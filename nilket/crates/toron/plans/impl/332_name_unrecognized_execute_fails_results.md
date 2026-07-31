# ADR 332 - Results: name q04/q12/q20 `unrecognized` execute failures

Pairs with `332_name_unrecognized_execute_fails.md`.

## Outcome

q04/q12/q20 at `sf=1` no longer render the soft `unrecognized` bucket. The shared raw message is
`BudgetExhausted { dimension: Memory, ... }`, classified as `memory_budget_exhausted`.

This is a named scale/cost-bound wall, not an unsupported SQL surface, so the reason is deliberately **not** in
`RECOGNIZED_BLOCKERS`; the rows remain `fail(memory_budget_exhausted@execute)`.

## Phase 0 raw messages

Release binary, cgroup capped (`MemoryMax=4G`, `RuntimeMaxSec=300s`):

| query | status | raw diagnostic message |
| --- | --- | --- |
| q04 | `fail(memory_budget_exhausted@execute)` | `BudgetExhausted { dimension: Memory, requested: ByteCount(121631060), available: ByteCount(67108864) }` |
| q12 | `fail(memory_budget_exhausted@execute)` | `BudgetExhausted { dimension: Memory, requested: ByteCount(121631060), available: ByteCount(67108864) }` |
| q20 | `fail(memory_budget_exhausted@execute)` | `BudgetExhausted { dimension: Memory, requested: ByteCount(72131060), available: ByteCount(67108864) }` |

q04/q12 share the same requested byte count; q20 is smaller but crosses the same 64 MiB per-task memory budget.

## Implementation

- `StageError` now retains `message` as well as `phase` and `reason`.
- `StageError` constructors classify and store the same rendered string, avoiding evidence/classifier drift.
- Reachability `Attempt` JSONL emits a conditional `diagnostic_message` field on failure rows. Successful rows omit
  it, and `matrix` rendering ignores the extra field.
- `reason_for_error` maps `BudgetExhausted` + `dimension: Memory` to `memory_budget_exhausted`.
- `memory_budget_exhausted` stays out of `RECOGNIZED_BLOCKERS`, so `status_for_reason` returns `Fail`.
- The ADR 262 architecture guard was retargeted from the old inline `reason_for_error(&format!(...))` shape to the
  new invariant: the run path classifies from, and retains, the same engine message.

## q02 later-scale note

Existing q02 `sf=10/100` rows in `target/tpch/reachability.jsonl` still show `unrecognized`, but they predate
diagnostic-message retention and have no raw message to compare. ADR 332 does **not** open the sf=10+ arc; q02
should be re-run under the new diagnostic path when that arc starts.

## Board

Latest release/cgroup probes:

```text
q04 sf=1 strict: fail(memory_budget_exhausted@execute), 2077.5 ms, peak 2.9G
q12 sf=1 strict: fail(memory_budget_exhausted@execute), 2323.5 ms, peak 3.4G
q20 sf=1 strict: fail(memory_budget_exhausted@execute), 1472.1 ms, peak 2.1G
```

`matrix` renders the latest strict cells as:

```text
q04 dev=pass@execute strict=fail(memory_budget_exhausted@execute)
q12 dev=pass@execute strict=fail(memory_budget_exhausted@execute)
q20 dev=pass@execute strict=fail(memory_budget_exhausted@execute)
```

## Route

Next named cut: memory-budget / spill / streaming for scale execution. The concrete first wall is a 64 MiB per-task
memory ceiling breach inside execution, not a bind/planner unsupported-feature wall.

## Verification

```text
scripts/cargo-cgroup test -p toron-tpch adr332
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup build -p toron-tpch --release
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=RuntimeMaxSec=300s /bin/sh -lc 'target/release/toron-tpch run --query q04 --mode strict --sf 1'
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=RuntimeMaxSec=300s /bin/sh -lc 'target/release/toron-tpch run --query q12 --mode strict --sf 1'
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=RuntimeMaxSec=300s /bin/sh -lc 'target/release/toron-tpch run --query q20 --mode strict --sf 1'
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q04 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q12 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q20 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```

All passed, except the three release/cgroup `sf=1` probes intentionally exit nonzero because they are named fail
rows. Their output is the measured artifact above.

`correctness-matrix` renders all 22 strict cells as `pass@compare`. `matrix` renders q04/q12/q20 strict as
`fail(memory_budget_exhausted@execute)`. q02 still renders an older later-scale `fail(unrecognized@execute)` row
from before diagnostic retention; ADR 332 records that as out-of-scope evidence for the future sf=10+ arc.
