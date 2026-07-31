# impl 449 - paired q05 ablation of the 446 bridge, gate-zero gated

Paired with `adr/449_paired_q05_ablation_gate_zero.md`. Four rails; the cohort is contingent on gate
zero. Lands in `toron-tpch` (driver, gate-zero runner, pairing) — no engine change; 448's
switch is reused.

## Seams (grounded)

| seam | file:line | role |
|---|---|---|
| ablation switch | `toron/src/exec/join/streaming.rs:20,29,12433` | `TORON_ABLATE_LOCAL_RUNTIME_FILTER`, default-on conjunct |
| arm driver | `toron-tpch/src/main.rs:2907` | `paired_ablation_arm(run_index) = run_index % 2 == 1` |
| per-child env | `main.rs:922-927` | `run_perf_child_args_with_env` sets the ablation env per child |
| row arm tag | `main.rs:932` | `row["ablation_arm"] = on|off` |
| sample emission | `main.rs:2495-2506,2636` | `execute_ms_samples` with `ablation_arm`, `run_index` |
| estimator (unwired) | `main.rs:2709` | `paired_median_shift(diffs)` — called only from tests |
| profile env | `grouped_chain_profile.rs:12` | `TORON_GROUPED_CHAIN_PROFILE_DIR` |
| profile-id counter | `grouped_chain_profile.rs:16,361` | process-global `AtomicU64` from 0 → first file `0000` |
| profile filename | `grouped_chain_profile.rs:1191` | `grouped_chain_{profile_id:04}.json` |
| the identity field | `grouped_chain_profile.rs:916` | `reservoir_build_rows` in the persisted snapshot |
| the cardinality oracle | `toron-tpch/src/lib.rs:907` | `generate_strict_lineitem(sf)` — the exact scanned batch; `.num_rows()` is full cardinality at any scale |

## Grounding steps before the diff

1. **Does the perf child accept a profile-dir arg, or only the env?** Gate zero needs to
   set `TORON_GROUPED_CHAIN_PROFILE_DIR` per child. `run_perf_child_args_with_env`
   (`:922`) already sets one env var; confirm it can carry two (ablation + profile dir) or
   generalize it to a set. **Do not** use `std::env::set_var` in the parent — it races.
2. **Is q05 exactly one grouped-chain profile?** The reservoir join is one admitted path,
   so one `from_env` → one `0000.json`. Confirm on the real run; the "exactly one file"
   assertion depends on it and is also the check that catches a second profile if the plan
   shape changes.
2a. **Does `generate_strict_lineitem(sf).num_rows()` equal OFF `reservoir_build_rows` on
    the real run?** This is the load-bearing equality. Verify it holds at sf=1 in gate zero
    before trusting it at sf=10 — if q05's build side is not exactly full lineitem (e.g. an
    upstream filter reaches it), the oracle expectation is wrong and the gate must be
    redefined against the actual build input, not lineitem cardinality.
3. **Does profiling perturb the arm identities?** Gate zero runs profiled; the cohort runs
   unprofiled (profiling adds ledger overhead that would confound timing). Confirm
   `reservoir_build_rows` is identical profiled vs not — it is a row count, not a timing,
   so it must be. If it differs, the profiler is not inert and that is its own defect.

## Cut

**Rail 1 — gate-zero runner.** A `--gate-zero` mode (or a distinct subcommand) that:
- creates two fresh empty temp dirs, one per arm;
- spawns the q05 child twice, ON and OFF, each with `TORON_GROUPED_CHAIN_PROFILE_DIR` set
  to its dir and `TORON_ABLATE_LOCAL_RUNTIME_FILTER` set accordingly;
- asserts each dir holds **exactly one** `grouped_chain_*.json`, parses `0000`, and reads
  `reservoir_build_rows`;
- computes `expected_off = generate_strict_lineitem(scale_factor).num_rows()` — the
  oracle-derived full cardinality, **scale-parametric, never hard-coded** — and asserts
  `off_rows == expected_off` with `on_rows < off_rows`. The equality holds *because q05's
  reservoir build side is unfiltered lineitem*; the helper is named
  `q05_reservoir_full_input_cardinality(sf)` so the q05-shaped assumption is explicit and a
  future query with a filtered build side cannot silently reuse it;
- emits a gate-zero record: `{on_rows, off_rows, expected_off, off_matches_expected,
  on_strictly_less}`. Fails loud if either identity is violated.

**Rail 2 — pairing (in `perf_summary` or a dedicated reducer).**

```rust
// pair by (arm, run_index), never by array position
fn paired_ablation_differences(samples: &[Sample]) -> Result<Vec<f64>, String>;
// - reject if any row's ablation_arm != parity(run_index)
// - require run indices 0..2n contiguous, each present exactly once
// - diff = off_ms - on_ms, on[k] with off[k] in run-index order
```

Feed the result to `paired_median_shift` (`:2709`) — this is the wiring 448 left absent.

**Rail 3 — sign.** `diff = off - on` in `paired_ablation_differences`. A helping filter →
OFF slower → positive diff → positive HL point estimate → detectable-and-material in the
improvement direction.

**Rail 4 — admissibility.** The cohort receipt emitter refuses to produce an HL/timing
block unless a gate-zero record is present and both identities passed. Encoded, not
documented.

**Nothing else.** No engine change. No re-judgement. The cohort runs only if gate zero
passes.

## Tests

**Rail 1 (gate zero):**
- `adr449_gate_zero_requires_reduced_on_arm` — fixture profiles: OFF full cardinality, ON
  reduced → passes. **Both arms full cardinality (switch is a no-op) → fails.** This is the
  test that makes the whole gate meaningful.
- `adr449_gate_zero_expectation_is_oracle_derived` — the expected OFF count equals
  `generate_strict_lineitem(sf).num_rows()` at sf ∈ {0.01, 1, 10}, and **is not** the
  hard-coded 59,986,052 at any scale but 10. Pins the operator's refinement: the gate
  stays valid when the scale changes.
- `adr449_gate_zero_rejects_multiple_profiles` — a dir with `0000` and `0001` → rejected,
  not first-picked.

**Rail 2 (pairing):**
- `adr449_pairing_rejects_a_dropped_child` — run indices `{0,1,2,4,5}` (3 missing) →
  rejected, not silently realigned.
- `adr449_pairing_rejects_arm_parity_mismatch` — a row tagged `on` at an odd index →
  rejected.
- `adr449_pairing_is_by_run_index_not_array_order` — shuffled sample array, correct pairs
  by `(arm, run_index)`.

**Rail 3 (sign):**
- `adr449_estimator_sign_is_off_minus_on` — fixture OFF>ON → HL point estimate > 0.
  **Fails if the subtraction is inverted.**

**Rail 4 (admissibility):**
- `adr449_cohort_receipt_requires_gate_zero` — a receipt without a passed gate-zero record
  cannot carry an HL block.

**Correctness:** q05 sf=1 digest/schema/rows stable in both arms (reconfirms 448).

Avoid the string `monotonic` in any test or source name (contains `tonic`; trips
`adr043_no_grpc_protobuf_or_arrow_flight`, which source-scans `src` and `tests`).

## Evidence to produce

- **Gate zero first, always.** The profiled ON/OFF `reservoir_build_rows` pair with both
  identities, at the top of the receipt.
- **If gate zero passes:** the n=10 unprofiled paired cohort on the post-448 tree —
  samples with arm and run index, HL point estimate, Wilcoxon interval, dual-criterion
  verdict. Scope order recorded.
- **If gate zero fails:** the ADR seals as a switch-defect report; no cohort, no timing.
- Every q05 figure explicitly non-comparable to pre-Rail-1 baselines (447's
  110,158.488554 ms included).

Clear `target/tpch/join_spill` before any sweep — post-430 the gauge spills there, not
`/tmp`; stale scratch breaks sf=10 runs. Gate-zero temp dirs are separate from spill
scratch; clear both.

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

The cut lands in `toron-tpch`; those legs are load-bearing. Run the FULL
`architecture_test` after `fmt`. Record gates green only after observing them green.
