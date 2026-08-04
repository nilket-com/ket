# impl 507 — the attribution contract, grounded and pinned

ADR 507 step 1 (define the contract) and step 2 (reconcile q21). This receipt records the grounding
that shaped the contract and the determination that resolved q21's residual. The contract itself is
code: `crates/toron/src/exec/join/stage_attribution.rs`, pinned by tests against q21's real profile.

## Grounding: the coverage map is worse than the board implied, and there are already two vocabularies

The board named one covered cell (q21). The truth is sharper. Toron has **two** stage-attribution
instruments, not one:

- `ChainStageProfiler` (`TORON_CHAIN_STAGE_PROFILE_PATH`) — the reoriented grouped-chain path
  (`try_streaming_inner_prefix_semi_anti_scan_probe_base`). Stages: base_scan, inner_replay_0..2,
  semi/anti, grouped_fold.
- `LocalJoinPhaseAccumulator` (`TORON_LOCAL_JOIN_PHASE_PROFILE_PATH`) — the fused inner-join global
  aggregate path (`try_streaming_collapsing_global_aggregate`). Phases: build, probe_scan, probe_key,
  output_materialize, aggregate_fold.

Probing all seven candidates at sf=8 against **both** env vars:

| cell | chain | localjoin |
|---|---|---|
| q21 | emitted | — |
| q02, q08, q09, q17, q18, q20 | — | — |

**One of seven cells has any stage attribution**, from one of two vocabularies tied to two different
lifecycles. The dormant `LocalJoinPhaseAccumulator` covers none of the seven — it is a third lifecycle
model that happens not to fire here. This is exactly the risk ADR 507 exists to contain: the contract
must reconcile (or explicitly declare non-comparable) vocabularies born on different lifecycles, not
copy one across boundaries it does not fit. The convergence that *is* trustworthy: both instruments,
independently, emit named stages + a declared total + an explicit unattributed residual. That shared
shape is the contract's spine.

## Two things the grounding forced into the contract

**Time reconciles in nested bands.** The chain profiler's stage timers sum to its *drive* span, not
to `execute_ms`. q21: stages 10.68 s ⊆ drive 11.14 s (0.45 s in-drive residual) ⊆ execute ~16.9 s.
The ~5.75 s drive→execute band (build, load, finalize) is real work the single-profiler view never
named. The contract models `stage_sum ≤ drive ≤ execute` explicitly so "reconcile with execute"
cannot silently mean "reconcile with drive." A profiler holds only stage+drive (it does not know
execute), so it reconciles the inner band; the outer band is the reader's, joined from the perf row.

**Byte counters come in three kinds, and only one sums.** From q21's real fields:

| kind | reconciles by | q21 fields |
|---|---|---|
| flow | sum | `build_spill_bytes`, `probe_spill_bytes` |
| peak | max (never sum) | `max_index_bytes`, `max_pinned_build_bytes` |
| subset-of-wider-scope | subset ≤ whole; remainder attributed elsewhere | replay-1 spill vs whole-query spill |

The peak distinction is not cosmetic: adding the two q21 peaks would invent 157 MB of footprint that
never existed at once. The contract returns peaks as a distinct type so a peak can never be handed to
a flow reconciler.

## Step 2 determination: q21's 536 bytes are a scoping difference, not unattributed I/O

`build_spill_bytes` (24,317,328) + `probe_spill_bytes` (873,395,520) = 897,712,848; whole-query
`spill_bytes` = 897,713,384; remainder **536 bytes**. Source resolves which kind this is:

- Both per-side counters and the whole-query counter ultimately sum `page.ciphertext_len` from
  `SpilledInnerJoinBuild::spill_bytes()` — the *same physical quantity*.
- But their **scopes differ**. `streaming.rs` does `task.counters.spill_bytes += probe_spill_bytes`
  *after* the profiler's final flush, on top of spill already accumulated by other reservoirs. The
  whole-query counter is task scope over every reservoir and replay; the profiler's `deferred_replay_1`
  is single-replay reservoir scope.

They were never required to sum. 536 bytes on an exact per-page integer sum of 897 MB is not a summing
error; it is spill outside replay 1. The contract models this as `reconcile_subset_of_wider_scope`:
replay-1 spill is a proper subset of whole-query spill, remainder = 536 bytes attributed elsewhere by
design. **Declared non-summing, whole-query is a superset** — pinned as an executable test, not prose.

Because the whole-query total is assembled after the profiler's last flush, this reconciliation is
architecturally reader-side (profile ⋈ perf row), which is why the flow/subset reconcilers have no
in-engine caller yet: they are the step-3 reader's, defined and pinned now.

## What landed

- `stage_attribution.rs`: the contract as pure reconciliation arithmetic — `TimeBands` (three nested
  bands), `reconcile_stage_within_drive` (the profiler's inner-band entry), `FlowByteDomain`,
  `reconcile_subset_of_wider_scope`, `reconcile_peak_domain`, and a typed `AttributionError` that
  refuses inverted bands and over-attribution rather than clamping. 8 tests, pinned against q21's
  emitted numbers.
- `ChainStageProfiler` retrofit: its time residual now comes from `reconcile_stage_within_drive`
  (contract-computed, not hand-rolled), and it emits a reconciled `residency_peak` through
  `reconcile_peak_domain`. Verified on a live sf=8 q21 run: stage_sum + residual == drive exactly, and
  `residency_peak` reports the max (134 MiB), never the sum. Existing fields unchanged; the profile is
  additive.
- The module is query-neutral (ADR 350 shape-naming; the q21 label lives here in `plans/` only) and
  registered in the ADR 176 executor-split allowlist beside its sibling profilers.

## Forward (unchanged from ADR 507)

Step 3 pilots the contract on q17 and q20 — both non-chain, one per mechanism class — which requires
instrumenting two uncovered paths and building the reader that joins profile with perf row (the home
of the flow/subset reconcilers). Step 4 prices overhead and requires both-axis reconciliation within
declared boundaries. No optimization mechanism is authorized until the attribution is ranked.
