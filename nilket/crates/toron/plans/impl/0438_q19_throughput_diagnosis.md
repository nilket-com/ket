# ADR 438 impl - q19 exclusive-span diagnosis ledger

Diagnosis-only. Off-by-default. No engine behavior change; disabled-parity is a gate.

**Locked env var:** `TORON_GLOBAL_FOLD_JOIN_FILTER_PROFILE_DIR` (contains no query number).

## Phase 0 — reconfirm dispatch + structural census (evidence, before any hook)

One strict sf=10 child under the exact 356 envelope, matching fingerprint `65c3c2c0b0a99063` /
51,471,552 spill bytes, establishes:

1. **Live `try_streaming_collapsing_global_aggregate` branch** (`aggregate.rs:428`) — in-memory
   collapsing vs. streamed-spilled vs. fallback `execute_hash_join_into_global_aggregate`
   (`exec/join.rs:449`). Leaf-span timers go **only** on the live branch.
2. **Build/probe orientation** — expected `part` = build, `lineitem` = probe; confirm via the join build
   profile (`exec/join/build_profile.rs`), do not assume.
3. **Replay ID + fingerprint** — pinned in the results doc so ledger children are provably the same run.
4. **Residual-program census** — walk the bound residual DAG: report **physical node count** and
   **per-node reference multiplicity**, so we know whether the common shipmode/shipinstruct conjuncts are
   physically repeated or CSE-shared. Eager cost is **one evaluation per physical node per batch** — the
   eval loop is `evaluate_bound_program_without_storage` (`expr.rs:374`: `outputs =
   vec![None; program.nodes.len()]`, one pass over `program.nodes`), **not** `evaluate_case`.

The Phase-0 branch identity + census are recorded in the results doc and cited by every hook site.

## The exclusive-span ledger (Cohort B, perf disabled)

Env-gated profiler, constructed only when the var is present — the `GenericGroupedFoldProfile` pattern
(`aggregate/group_table.rs`, `from_lookup` → `None` on absent/empty).

### One explicit root span

- `root_span_micros` = **entry-to-return of the Phase-0-confirmed fused path** (not the harness
  `execute_ms` boundary).
- The engine emits `root_span_micros`; the harness/results composition reports
  `outside_root_overhead_micros = execute_ms − root_span_micros` as a **separate line**, never folded into
  the residual. The subtraction is checked; `root_span_micros > execute_ms` invalidates the receipt.

### Eight mutually exclusive leaf spans

Accumulated micros over `Instant` sub-spans; **no leaf encloses another**; placed on the live branch:

| leaf | role |
|---|---|
| `probe_lookup_select_micros` | hash-key compute + bucket selection in the probe |
| `arrow_materialize_micros` | Arrow output materialization / interleave **after pages are available** |
| `predicate_eval_filter_micros` | residual `join_filter_predicate` eval + mask apply (`streaming.rs:8103`) |
| `decimal_expr_micros` | `evaluate_bound_single_root(&call.args)` for `l_extendedprice*(1-l_discount)` (`aggregate.rs:776`) |
| `numeric_fold_micros` | `numeric_value` + `add_sum_value` row fold (`aggregate.rs:779-780`) |
| `scan_poll_decode_micros` | scan polling + Arrow decode of input batches |
| `spill_page_read_micros` | spilled-output **page read / decode** |
| `build_spill_write_micros` | build-side index insert + spill encode-write |

**Disjointness guarantee for the D1/D4 overlap the prior iter got wrong:** spilled-output page reads are
owned by `spill_page_read_micros` (D4); `arrow_materialize_micros` (D1) starts only **after** pages are
available. Whenever profiling is enabled, profiler state rejects entering a leaf while another leaf is
open. Overlap, underflow, and counter overflow are loud profile errors in the release diagnostic, not
debug-only assertions.

### Domain roll-up

- **D1** = `probe_lookup_select_micros + arrow_materialize_micros` (selection + post-read materialization)
- **D2** = `predicate_eval_filter_micros`
- **D3** = `decimal_expr_micros + numeric_fold_micros`
- **D4** = `scan_poll_decode_micros + spill_page_read_micros + build_spill_write_micros`
  (scan, page read/decode, build/spill)

### In-root residual (predeclared ceiling)

- `residual_micros = root_span_micros − Σ(eight leaves)`, with checked addition/subtraction; arithmetic
  failure invalidates the receipt loudly.
- **`residual_micros ≤ 0.10 × root_span_micros`.** Over-ceiling ⇒ **add a leaf where the loop is
  unmeasured, rerun** — the ceiling never moves.
- `probe_micros` (`task.rs:175`) retained as a **coarse outer cross-check** only, never as D1.

### M → S reconciliation counters

- `predicate_input_rows` = **M**.
- `predicate_output_rows` = `sum_argument_rows` = `numeric_fold_rows` = **S** (asserted equal).
- Results doc asserts `S == oracle.qualifying_rows` (`Q19Oracle`, `toron-tpch/src/lib.rs:12683`) and,
  independently, `sum(oracle.branch_hits) == oracle.qualifying_rows` (`lib.rs:12764`). **Engine collects
  no `[b0,b1,b2]`.**

## Off-by-default parity (gate)

- Var absent/empty ⇒ profiler `None`; **zero** counters/timers/spans/held state — a disabled-parity test
  compares the same implementation with profiling absent versus an explicit disabled control and asserts
  exact output/digest/schema/rows/spill and existing-telemetry parity.
- Enabled ⇒ JSON snapshot per fold under `target/tpch/adr438/`, `schema_version` tagged, tmp-then-rename
  publish (`group_table.rs:1510`).

## Two-cohort stability harness

- **Cohort A** — 5 fresh strict-sf10 children, external perf, **ledger disabled**: names the perf cycle
  category, cross-seed stable.
- **Cohort B** — 5 fresh strict-sf10 children, **ledger enabled, perf disabled**: exclusive D1..D4 +
  dominant-leaf attribution.
- **Elapsed times are never compared across cohorts** (perf and the ledger perturb timing differently).
- **Cohort A gate:** same dominant sampled perf category in all five and
  `(max−min)/median ≤ 0.25` on its normalized share.
- **Cohort B gate:** same dominant ledger domain and dominant exclusive leaf in all five;
  `(max−min)/median ≤ 0.25` on normalized D1..D4 and the dominant-leaf share; in-root residual ≤ 10%
  each. The 356 fingerprint/spill tuple is invariant across all five children in both cohorts. `max` is
  reported, never gated.

## Seal gate (per council.md, via scripts/cargo-cgroup, after fmt)

```
scripts/cargo-cgroup fmt  -p toron -- --check
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test                       # source-scanning guards
scripts/cargo-cgroup test -p toron --no-fail-fast                                 # whole integration suite
scripts/cargo-cgroup test -p toron-tpch                                           # reachability gauge
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast   # Polars-reference (recent diagnosis cuts)
git diff --check
```

Plus, since the ledger adds held state and a new env surface:
- `exec.rs` line ratchet unchanged (ledger lives in the aggregate/join modules).
- Box new held state if the fused async body grows (the deep-recursion stack-overflow trap: enlarging a
  recursive async fn body overflows the deep-recursion tests — extract to separate futures, wrap deep
  tests in a 64 MB stack).
- Architecture guard: profiler is env-gated and **carries no query number** (no `q19` literal in engine
  source/guards).
- Disabled-parity + release-path exclusive-timer-state tests in `-p toron --lib`.

## Implementation contingencies

- Exact leaf-span insertion lines are fixed **after** the Phase-0 live-branch confirmation (streamed-
  spilled vs. in-memory); the eight leaves are named by role and anchored then.
- If the census shows the common conjuncts are CSE-shared (physical multiplicity 1, not 3), D2's
  repeated-work hypothesis weakens and the dominant-leaf result — not the AST — decides the 439 mechanism.
- If any single D4 subleaf is negligible across all five Cohort-B children, it is reported as measured-
  small and kept (not dropped) so the partition stays complete.
