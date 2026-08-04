# impl 450 - q05 committing-scale ladder probe

Paired with `adr/0450_q05_committing_scale_ladder.md`. Lands in `toron-tpch` — no engine
change; reuses 448's profiling and 449's gate-zero machinery.

## Seams (grounded)

| seam | file:line | role |
|---|---|---|
| resident/reservoir fence | `streaming.rs:15693`-region | `resident_baseline + loaded_resident <= loaded_cap` decides the path |
| retained cap | `exec/join.rs:56,3669` | `memory_cap × 16` = 1 GiB |
| build-estimate safety | `streaming.rs:10` | `× 64` inflation |
| commit guard A | `streaming.rs:10038` | resident: generic grouped path never entered |
| commit guard B | `streaming.rs:12222` | generic grouped entered, partitioned-scan reservoir declined (`Ok(false)`) |
| the bridge | `streaming.rs:12433` | inside the fn that returns false at guard B |
| reservoir profile field | `grouped_chain_profile.rs:916` | `reservoir_build_rows` — present only on commit |
| profile filename | `grouped_chain_profile.rs:1191` | `grouped_chain_{profile_id:04}.json` |
| scale acceptance | `lib.rs:453,10562` | q05 unconstrained; any finite positive sf |
| dataset prep | `lib.rs:2442` | `prepare_persisted_tpch_dataset(scale_factor, …)` |
| runtime-filter projection repair | `lib.rs:1724-1725,1799-1804,1908-1939` | full-reader batches must bypass reduced-column inflation |
| 449 gate-zero fixture scale | (449 impl) | the literal this cut replaces with the selected rung |

## Grounding steps before the diff

1. **Can guard B (`:12222`) actually fire for q05 at some scale, or only guard A?** If q05
   only ever hits guard A (resident) below the commit threshold and commits cleanly above
   it, guard B is unreachable for q05 and the taxonomy's third state is dead — still worth
   handling, but confirm which diagnostics are live before writing tests that assume both.
2. **Does the child surface the diagnostic string on stderr/exit, or only as a Kernel
   error swallowed by the harness?** The ladder classifies by matching the exact message;
   confirm it reaches the parent (the 449 gate-zero run saw it, so it should — verify the
   channel).
3. **What is 449's current fixture-scale literal?** Rail 5 replaces it with a read from the
   selected rung. Find every site that hard-codes a q05 ablation scale.

## Cut

**Rail 1 — ladder runner.** A `--committing-scale-ladder` mode: an ascending sequence of
scales; for each, prepare the scale-matched dataset, spawn one profiled ON child, classify
the outcome, and stop on the first `committed`. Emit a per-rung record and a final
`{selected_scale}` (or `{no_commit}` / `{cost_too_high}`).

**Rail 2 — classifier.** Exact three-way:

```rust
enum RungOutcome {
    Committed { reservoir_build_rows: u64, execute_ms: f64 },
    NonCommitting { diagnostic: CommitDiagnostic, execute_ms: f64 }, // GuardA | GuardB
    // no Resident-catch-all: everything else is Err
}
// committed   := exactly one grouped_chain_*.json with reservoir profile present
// GuardA      := child error message == the :10038 string
// GuardB      := child error message == the :12222 string
// otherwise   => Err (hard) — never classified as non-committing
```

The message match is against the **exact** strings at `:10038` / `:12222`; any other
failure (spawn, panic, non-zero exit, different Kernel message, missing or duplicate
profile) is a hard error that aborts the probe.

**Rail 3 — per-rung isolation.** Fresh temp dirs for profile and spill per rung; assert
empty before the child; dataset prepared for that exact scale and fingerprint-checked. No
reuse across rungs.

**Rail 4 — sf=10 untrusted.** sf=10 is included as the ladder's upper bound but flagged
`trusted: false` until a rung there returns `Committed` with a real profile. A timeout or
prior qualitative divergence does not set `trusted`.

**Rail 5 — single source of truth.** Emit the selected scale to a stable location (a
constant or a small emitted record) that 449's gate-zero fixture reads. Replace the 449
literal.

**Harness completion.** The runtime-filter persisted scan uses the same source-column union as the
control arm, including filter keys, and both arms inflate that reduced batch to the source schema
before filtering/output projection. This keeps source ordinals addressable without reading all
columns in one arm. The repair travels with the ladder because the earlier full-reader/reduced-
inflater mismatch blocked the first committing-region rung. No engine change, no paired cohort, no
filter-effect measurement.

## Tests

- `adr450_classifier_guard_b_is_non_committing_not_hard_error` — a fixture child exiting
  with the `:12222` string classifies as `NonCommitting{GuardB}`, **climb**, not abort.
  Pins the fix-1 correction.
- `adr450_classifier_unknown_failure_is_hard_error` — a fabricated unknown Kernel message
  is `Err`, **never** non-committing. Pins the no-catch-all-resident rule: an unknown
  failure must not masquerade as a legitimate non-commit.
- `adr450_ladder_stops_on_first_committed` — a fixture ladder committing at rung k does not
  run k+1.
- `adr450_rung_dirs_are_fresh_and_scale_matched` — a reused/non-empty profile dir is
  rejected; a dataset whose fingerprint mismatches the rung scale is rejected.
- `adr450_sf10_is_untrusted_until_profiled` — sf=10 with only a timeout is `trusted:false`;
  with a real reservoir profile it flips true.
- `adr450_449_reads_selected_scale_not_a_literal` — 449's gate-zero fixture scale resolves
  from the emitted source; a grep-guard asserts no q05 ablation scale literal remains
  downstream.

Avoid the string `monotonic` in any test or source name (contains `tonic`; trips
`adr043_no_grpc_protobuf_or_arrow_flight`, source-scanning `src` and `tests`). Use "climb"/
"ascending".

## Evidence to produce

- The per-rung ladder record: scale, outcome, which diagnostic, runtime, and (on commit)
  `reservoir_build_rows`.
- The selected committing scale, **or** an explicit `no_commit` / `cost_too_high` finding.
- If committed: a note that 449 gate zero must rerun at the selected scale before any
  cohort, and that this cut makes no filter-effect claim.
- sf=10's trust status stated explicitly.

Clear `target/tpch/join_spill` and all rung temp dirs before and after the probe.

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

Lands in `toron-tpch`; those legs are load-bearing. Run the FULL `architecture_test` after
`fmt`. Record gates green only after observing them green.
