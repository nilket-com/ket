# ADR 443 impl - q05 grouped-chain exclusive-span diagnosis ledger

Diagnosis-only. Off-by-default. Query-neutral. No engine behavior change; disabled parity is a gate.
The ADR 429 residency profiler (`TORON_GROUPED_RESERVOIR_RESIDENCY_PROFILE_PATH`, schema 1) is
**untouched** — the new ledger coexists on its own surface.

**Locked env var:** `TORON_GROUPED_CHAIN_PROFILE_DIR`. Constructed only after the grouped-chain shape
is admitted; absent/empty ⇒ no state, sink, or clock; atomic tmp-then-rename publication with loud
propagated failures.

## Phase 0 — reconfirm, map, and extend the oracle (evidence, before any hook)

1. **Live path current.** One strict sf=10 run under the exact 356 profile confirms the owner chain:
   `try_streaming_reoriented_inner_chain_grouped_aggregate` (`streaming.rs:9506`, root owner) →
   `try_streaming_reoriented_inner_chain_generic_base_grouped` (`streaming.rs:11793`, base
   accumulation) → `try_process_reoriented_inner_chain_grouped_partitioned_scan_reservoir`
   (`streaming.rs:11967`, reservoir sub-owner). Fingerprint/digest/schema/rows and the
   5,353,985,328-byte spill signature refreshed on the current binary. Drift ⇒ stop and revise.
2. **Stage map.** Record the physical stages with row/byte boundaries (runtime prep/build + its spill,
   base accumulation, reservoir write, base concat, reservoir read/probe, suffix, fold, finalize).
   The map fixes the ledger leaf anchors and the oracle extension — leaves and identities follow the
   map, not the plan text.
3. **Oracle extension.** Extend `strict_q5_oracle` (`toron-tpch/src/lib.rs:5798`) — or add a sibling
   census beside `Q5Oracle` — with exactly the independent stage counters the map requires (checked
   arithmetic, dense/frugal parity where applicable, boundary tests; the 441 oracle-counter pattern).
   Predeclared identities today: final `selected_rows` and `groups` only.
4. One qualitative external `perf` child (cpu_core cycles, dwarf callgraph) — outside every gate.

## Root + leaves

`root_span_micros` = top-level owner, **post-shape-admission → aggregate finalization + runtime
cleanup**. Outer Sort outside-root; `outside_root = execute_ms − root`, separate line, never in the
residual.

Nine role-named leaves (accumulated `Instant` sub-spans; no leaf encloses another; overlap rejected):

| leaf | anchor (finalized by the Phase-0 map) |
|---|---|
| `runtime_prep_build_micros` | pre-reservoir runtime preparation/build incl. its spill write |
| `base_accumulation_micros` | the `:11793` accumulation |
| `reservoir_partition_spill_write_micros` | reservoir write phase |
| `base_concat_micros` | reservoir-input concat |
| `reservoir_read_probe_micros` | reservoir read/decode + probe |
| `suffix_join_micros` | post-reservoir suffix stages |
| `grouped_fold_micros` | aggregate fold |
| `finalization_micros` | grouped finalization |
| `cleanup_micros` | teardown |

**Residual is derived, not a leaf:** `residual = root − Σ(nine leaves)`, **≤ 10%**
(widen-instrument-never-move). Storage scan/read counters and spill bytes separate from elapsed.

## Reconciliation

- **Two-domain spill (delta form):** record `task_spill_at_root_start` and `task_spill_at_root_end`;
  assert `end − start = runtime/build domain + reservoir domain` **and** that the delta equals the
  run's final total (5,353,985,328-shaped; reservoir ≈ 5,270,315,856 per ADR 430, earlier domain owns
  the 83,669,472 delta — exact values refreshed in Phase 0, then pinned for the cohort). Delta form
  removes absolute-counter ambiguity; a nonzero `task_spill_at_root_start` or post-root spill growth
  is itself a loud finding.
- **Row flow:** existing oracle finals (`selected_rows`, `groups`) exact 5/5; Phase-0-mapped
  intermediate counters exact 5/5 once the extension lands. No invented intermediate identities.

## Stability harness (ledger only)

Five fresh strict-sf10 children, 356 envelope: **same dominant leaf 5/5**, and the **dominant leaf's
normalized share spread ≤ 0.25**; all nine leaf shares reported; residual ≤ 10% each; result tuple
invariant. (No domain rollup — the discriminator maps each leaf to one mechanism.) **The cohort wall
distribution (min/median/max/spread) is recorded as first-class evidence** — the stability picture the
future ≥23.86% ADR 444 band rests on. The perf child stays qualitative.

**Isolation:** no q17 (or any other) measurement runs concurrently with a q05 profiling child on this
host; q17 remeasurement is temporally separated.

## Off-by-default parity (gate)

Var absent/empty ⇒ profiler `None`; zero counters/timers/spans/held state — disabled-parity test
asserts byte-identical output/digest/schema/rows/spill vs. a no-profiler build, **and that the 429
residency profiler's behavior and schema-1 receipts are unchanged** (both-enabled and both-disabled
combinations covered).

## Survival control

One persisted strict sf=10 q05 `pass@compare` under 4 GiB/no-swap (`TasksMax=512`,
`RuntimeMaxSec=600`), scratch fresh/empty before and after — the 430 contract stays green.

## Seal gate (council.md, via scripts/cargo-cgroup, after fmt)

```
scripts/cargo-cgroup fmt  -p toron -- --check
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
```

- `exec.rs` 7,499; join-root ratchet holds; box new held state if the grouped-chain async body grows
  (deep-recursion trap).
- Architecture guard: ledger env-gated + post-admission construction (fence-before-construct order
  check in the sliced owner), exclusive-leaf surface, atomic publication, **429 surface untouched**,
  no query literal.
- Focused tests: disabled/parity, matrix/residual arithmetic, overlap/outside-root rejection,
  delta-form two-domain spill mismatch loud failure, atomic publication failure, oracle-extension
  counters.

## Implementation contingencies

- Leaf anchors are finalized by the Phase-0 stage map; a stage the map reveals that the leaf table
  above does not cover gets its own leaf (widen before measuring, not after a >10% residual).
- If the Phase-0 mapping shows the live chain no longer matches the 9506→11793→11967 shape, stop and
  revise the ADR.
- A leaf measured-small across all five children is kept, not dropped.
- **No mechanism** — any write/read-side reservoir change belongs to ADR 444 after the ledger names
  the dominant leaf.
