# impl 448 - executable q05 ablation rails

Paired with `adr/0448_executable_q05_ablation_rails.md`. **No performance claim.**
Implement the rails in the order below; Rail 3 before Rail 1 produces a confounded
number with a more expensive instrument attached.

## Seams

| seam | file:line | role |
|---|---|---|
| **the composition asymmetry** | `toron-tpch/src/lib.rs:1688-1692` | `if !runtime_filter_present { append_projection_to_pbc(...) }` |
| filter-path column set | `lib.rs:1697-1716` | `pbc ∪ projected ∪ filter_keys`, inflate to source, then filter |
| post-filter projection | `lib.rs:1834-1846` | `filtered.project(...)` — already runs on the filter path |
| **root-span spill** | `grouped_chain_profile.rs:768` | `task_spill_at_root_end − task_spill_at_root_start` |
| role reconciliation | `grouped_chain_profile.rs:771-774` | `reservoir + runtime_build == task_spill_delta` |
| **whole-query spill** | `main.rs:2412`, `:2535` | `spill_bytes` / `spill_bytes_max` — the ADR 356 baseline's unit |
| estimator (exists) | `main.rs:2570-2638` | `MATERIALITY_FLOOR`, `PairedShift`, `paired_median_shift` |
| binary override (exists) | `main.rs:959-971`, used `:2244` | `TORON_TPCH_TORON_BIN` → `toron_binary()` |
| child spawn | `main.rs:2243` `run_perf_child_args` | builds a `Command` — the per-child `.env()` point |
| the bridge to toggle | `toron/src/exec/join/streaming.rs:12394+` | `local_runtime_filter_shape_is_admitted` / `build_local_terminal_runtime_filter` |
| env precedent | `exec/join/*` | **all** `TORON_*` there are profile paths, not mechanism toggles |

## Grounding steps before the diff

1. **Is `filter_keys ⊆ projected` an invariant or a q05 coincidence?** Rail 1's
   "no widening" claim depends on it. If it is a coincidence, the control must read the
   filter-only columns anyway (Rail 1's general rule) — ground before scoping.
2. **Does one post-filter projection suffice for the control arm?** The filter path
   already projects at `lib.rs:1834-1846`. If moving the control onto the same point is
   sufficient, Rail 1 is largely a deletion of the `runtime_filter_present` condition
   plus a shared union — not a rewrite.
3. **Can the parent set a per-child env var?** `run_perf_child_args` (`:2243`) builds a
   `Command`; `.env()` per spawn is the clean path. **Never `std::env::set_var`** — it
   races and would corrupt the arm assignment.
4. **What advertises "corrected harness capability"?** Rail 3's cross-tree probe needs a
   real signal from the candidate binary, not a version string convention.

## Rail 1 — shared scan composition

Refactor the persisted-Parquet harness so the filter and control construct one
source-column union, inflate as needed, apply the optional runtime mask, and perform one
post-filter projection. The implementation **must not** use `runtime_filter_present` to
decide whether an `OP_PROJECT` is appended. Add fixtures for both
`filter_keys ⊆ projected` and filter-only columns; assert identical read columns and
output schema.

Unconditionally appending the projection **double-projects the filtered path** and fails
as `project index 8 out of bounds, max field 4` — ADR 447's excluded first attempt. One
union, one projection point.

## Rail 2 — typed spill accounting

Introduce explicit scope-bearing values for whole-query spill totals and grouped-root
deltas. The ratio helper accepts only matching scopes and tests that a mismatched pair is
rejected. Preserve role reconciliation and report the ADR 446 mismatch as open.

## Rail 3 — same-binary paired driver

Read a per-child ablation arm at spawn time with `Command::env`, defaulting to
filter-on. Generate a deterministic A,B,A,B schedule, persist
`{run_index, arm, execute_ms}`, and feed paired differences to the existing estimator.
The binary override path must reject a comparison binary lacking the corrected harness
capability; **no remembered operator state may substitute for the probe.**

Add an architecture guard pinning the default-on behavior and limiting the mechanism
variable to the admission site. Keep all profile-path environment variables unchanged.

## Tests

**Rail 1 (discriminating):**

- `adr448_scan_composition_is_invariant_to_filter_presence` — same fixture with and
  without a descriptor; identical Parquet column set, emitted schema, and projection
  placement. **Fails today**, and fails if `append_projection_to_pbc` is re-conditioned
  on filter presence.
- `adr448_control_arm_reads_the_same_columns_as_the_treatment_arm` — fixture where
  `filter_keys ⊄ projected`; the control still reads the filter-only columns. Pins the
  general rule; fails if the control gets an I/O advantage.
- `adr448_filtered_path_projects_exactly_once` — pins the double-projection trap;
  reproduces `project index 8 out of bounds, max field 4` if a second projection returns.

**Rail 2:**

- `adr448_spill_scopes_are_distinct_types` — a whole-query total and a root-span delta
  are not interchangeable.
- `adr448_ratio_across_mismatched_scopes_is_refused` — **the ADR 446 defect encoded as a
  test.**
- Existing role reconciliation (`grouped_chain_profile.rs:771-774`) stays green
  unmodified.

**Rail 3:**

- `adr448_kill_switch_defaults_to_filter_on` — unset → bridge active. The guard against
  a feature flag.
- `adr448_driver_alternates_arms_per_child` — asserts `A,B,A,B,…` and that arm and run
  index are recorded per sample.
- `adr448_cross_tree_refuses_an_uncorrected_harness` — a binary without the capability is
  rejected, not silently paired.

**Correctness:** strict q05 sf=10 digest, schema, and row count stable with the switch in
**both** positions.

**Architecture guard:**

- `adr448_runtime_filter_kill_switch_is_an_ablation_instrument` — the switch defaults to
  on, is read only at the admission site, and no `exec/join` mechanism reads a `TORON_*`
  var other than profile paths and this one.

Avoid the string `monotonic` in any test or source name (contains `tonic`; trips
`adr043_no_grpc_protobuf_or_arrow_flight`, which source-scans `src` and `tests`). Use
"strictly increasing".

## Evidence boundary

Run correctness-only receipts for both arms. **Do not report a wall, spill, or
materiality result** until a later ADR runs the paired cohort after all three rails
pass.

The receipt must state that all existing q05 timings predate Rail 1 and are
non-comparable to anything measured after it — including ADR 447's 110,158.488554 ms
median — and that the ADR 446 12x spill gap remains open, with the scope mismatch named
as a partial explanation only.

Clear `target/tpch/join_spill` before any sweep — post-430 the gauge spills there, not
`/tmp`; stale scratch breaks sf=10 runs.

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

This cut spans `toron` (kill switch) **and** `toron-tpch` (harness, driver), so both `-p`
legs are load-bearing. Run the FULL `architecture_test` after `fmt` — the guards
source-scan and `fmt` mutates the scanned text. Record gates green only after observing
them green, never from a prior session's summary.
