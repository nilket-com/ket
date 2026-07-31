# ADR 448 - executable q05 ablation rails

Make the ADR 446 ablation possible to run and possible to interpret. This is an
instrumentation/ablation-enablement cut: **no wall or spill attribution is made.**

## Context

After ADR 447 the agreed next step was a paired A/B via `TORON_TPCH_TORON_BIN`.
Grounding says it is neither executable nor interpretable today.

**The driver does not exist.** ADR 447 shipped two of three pieces: the estimator
(`paired_median_shift` `main.rs:2583`, `PairedShift` `:2573`, `MATERIALITY_FLOOR`
`:2570`, dual criterion `:2638`) and the binary override (`toron_binary()` `:959`,
consumed by `run_perf_child_args` `:2244`). Nothing alternates A and B — the override
is a single process-global env var read at child spawn, so a sweep runs entirely on one
binary. 447's receipt says "no comparison-tree paired cohort was run here", which reads
as unrun; it is unbuilt.

**The comparison is structurally confounded.** The harness's scan composition is
conditioned on filter presence (`toron-tpch/src/lib.rs:1688-1692`):

```rust
if let Some(projected) = &projected_columns {
    if !runtime_filter_present {
        append_projection_to_pbc(program, projected)?;   // ONLY when no filter
    }
}
```

Filter-on and filter-off take different projection paths and different Parquet column
selections. An A/B on filter presence therefore compares two scan compositions, not one
mechanism — cross-tree **or** same-binary. Pairing controls for drift; it does not
control for taking two different code paths. This is the failure that invalidated ADR
446's performance evidence one cut ago.

**The spill figures are not in the same units.** `task_spill_delta`
(`grouped_chain_profile.rs:768`) is `task_spill_at_root_end − task_spill_at_root_start`
— the grouped-chain root span only, reconciled against `reservoir + runtime_build`
roles (`:771-774`). `spill_bytes` (`main.rs:2412`, `:2535`) is the gauge's whole-query
total, and is the ADR 356 baseline's unit. ADR 446 reported
`task_spill_delta = 278,762,448` against the baseline's whole-query `5,353,985,328` —
**a root-span delta divided by a whole-query total**. Its 94.79% was never a
like-for-like ratio: a second reason that evidence is withdrawn, independent of the
harness confound, and one its invalidation note does not state.

**The projection placement is a live trap, not a hypothetical.** Appending the
projection unconditionally would double-project the filtered path — PBC narrows to four
columns and the post-filter `filtered.project(...)` (`lib.rs:1834-1846`) then applies
source ordinals to a four-field batch. ADR 447's receipt already records exactly this as
its excluded first attempt: `project index 8 out of bounds, max field 4`.

## Decision

Three rails land in order.

1. **Composition invariance.** Both filter-on and filter-off scans read the same
   source-column union and apply the same post-filter projection. The runtime mask is
   the only treatment difference. Do not append a projection opcode only on one arm, and
   do not project twice. For q05 the filter key is among the projected columns; for
   general shapes the control reads and then discards any filter-only columns so it
   receives no I/O advantage.
2. **Spill scope typing.** Root-span deltas and whole-query totals remain distinct typed
   values. Ratios across scopes are rejected; the ADR 446 gap is recorded as unresolved
   rather than explained by arithmetic.
3. **Paired driver.** Add a same-binary per-child kill switch, defaulting to filter-on,
   and an A,B,A,B schedule that sets the arm on each child spawn. Cross-tree override is
   accepted only when both binaries advertise the corrected harness capability.

The kill switch is an ablation instrument, not a production feature flag. The default
and its admission-site scope are architecture-guarded. No benchmark baseline moves and
no mechanism result is reported by ADR 448.

**Order is mandatory.** Rail 3 before Rail 1 produces another confident number from a
confounded comparison — the mistake this arc has now made twice.

## Acceptance

- Filter-on/off fixtures have identical source read columns, emitted schema, and
  projection placement; a filter-only column fixture proves the control reads the same
  union.
- Scope-labelled spill values cannot form a cross-scope ratio.
- The unset kill switch selects filter-on; the driver records arm and run index and
  alternates deterministically.
- An uncorrected comparison binary is refused.
- q05 digest, schema, and row count match in both arms.

All existing q05 timings predate Rail 1 and are non-comparable to later runs. The prior
invalidated ADR 446 performance evidence remains withdrawn.

## Guardrails and risks

- **The kill switch must not become a feature flag.** Every `TORON_*` variable in
  `exec/join` today is a profile path — off-by-default diagnostics that change no
  behavior. A mechanism toggle is a new class: it changes **live** behavior. It is
  justified only as an ablation instrument, defaults to the shipped behavior, and is
  pinned by guard at the admission site. An ablation instrument that drifts into a
  config knob is how a shipped mechanism gets silently disabled in production.
- **Rail 1 changes measured behavior.** Making composition invariant moves q05's numbers
  again. That is expected and is **not** a speed result. Every existing q05 figure —
  including ADR 447's 110,158.488554 ms median — predates it.
- **Do not project twice.** The unconditional-append shape fails as
  `project index 8 out of bounds, max field 4` (ADR 447's excluded attempt). One union,
  one post-filter projection point.
- **The control must not read less than the treatment.** When `filter_keys ⊄ projected`,
  a control that skips the filter-only columns measures I/O, not the mechanism.
- **Fourth consecutive apparatus cut** (444, 447, 448; 446 was a mechanism whose
  evidence apparatus destroyed). Three produced no throughput. That is the strongest
  argument against this cut and it belongs in the record. The counter: the agreed
  experiment is currently unrunnable and would produce a confident number from a
  confounded comparison.
- **Do not re-judge sealed reds.** 443/444/446 stand. 446's performance evidence is
  already withdrawn; nothing here reclassifies its verdict.

## Forward arc

**The clean ablation of the ADR 446 bridge** is what all three rails exist to enable. It
remains unrun, and after this cut becomes runnable for the first time. That paired
cohort is the ADR 449 candidate.

**Chain reorientation** stays demoted — its justification rested on the withdrawn 446
null.

**q05's wall regime** — ADR 447's corrected-harness cohort shows the left-skew anomaly
does **not** reproduce (IQR/median 0.034; min 3.20% below median, max 5.05% above). The
fast outlier that set the 23.86% band looks like a property of the old measurement
rather than the query. Worth stating once the band is revisited — and note the band's
own construction is superseded by ADR 447.

## Deferred (named)

- The ADR 446 12x spill gap — open; scope mismatch is a partial explanation only.
- Query-budget realism — `memory_per_task: 64 MiB` reaching production via
  `impl Default for QueryBudget { fn default() -> Self { Self::test_default() } }`
  (`distributed/budget.rs:95,107-110`).
- Chain reorientation (demoted).
- `MAX_FILTER_BYTES` cap raise (1 MiB → ~2.7 MiB).
- A size-based build-side choice for spill-eligible joins — ADR 132's comparator never
  runs for non-broadcast pairs (`adr/132:3-6`).
- q17 remeasurement — itself left-skewed (+4.56) in the pre-447 baseline.
- q18 strict one-pass; q21 residual; q19 re-diagnosis + 438-profiler widening; q13 fresh
  diagnosis; Cut A (439 mirror); generic O(1) LRU rewrite.
