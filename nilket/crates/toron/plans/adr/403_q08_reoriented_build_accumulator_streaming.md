# ADR 403 - q08 reoriented join build accumulator: diagnostic + first streaming-completion cut

## Context

401 and 402 closed q03's SQL/Rune *logical-shape* gap (filter placement, then grouped-aggregate exchange placement;
SQL/Rune now `1.00x`, byte-identical plans). What remains is the *execution/substrate* gap: q03 SQL runs ~400 ms with
the exact shape Rune runs, while Polars streams it in ~102 ms and DuckDB in ~40 ms. That gap - and the sf=10 heavy
cells - is the Thread 3 big rock: the execution-model / streaming-completion change.

q08 is the right *entry* into that thread because it fails **fast and on the execution side**: at sf=10 it hits
`memory_budget_exhausted: reoriented join build accumulator` (requested ~1.8 GiB / available ~1 GiB) in about 1.23s
(ADR 398). It fails on **memory**, not time, so iteration is cheap - unlike the >360s time cells.

**Grounded premise (not "invent spilling"):** Toron already has a streamed, spilling/partitioning reoriented
scan-build runtime. `prepare_reoriented_inner_join_runtime_with_streamed_scan_build` (`exec/join/streaming.rs:8019`)
produces `ReorientedInnerJoinBuild::Spilled` or `::PartitionedScan` (`streaming.rs:38`, `:8127`, `:8191`) - it does not
materialize the whole build. The wall comes from the **fallback materialized path**: when the eligibility gate
`stream_scan_build && decompose_streaming_scan_probe_input(stage.build).is_some()` is false (`streaming.rs:8036`), the
build is `execute_bound(stage.build)` -> `concat_or_empty` -> `estimated_build_hash_bytes`, and over the memory cap it
raises the named wall (`streaming.rs:8207-8236`). The cap is
`join_output_materialization_memory_cap(task_budget.memory)` - the ~1 GiB **task budget**, not the 4 GB cgroup.

So the first question is **not** "how do we spill." It is: **why did q08's overflowing build miss the existing streamed
scan-build eligibility?**

## Decision

### Phase 0 - classify q08's build side (diagnostic, before any code)
1. **Which replay/stage overflows?** Identify the specific q08 join stage whose build hits the accumulator wall.
2. **Was streamed scan-build requested?** Record whether the failing stage entered
   `prepare_reoriented_inner_join_runtime_with_streamed_scan_build` (`stream_scan_build=true`) or the plain
   materialized entry point (`stream_scan_build=false`). A miss can be a **call-site decision miss** even when the build
   shape is decomposable.
3. **Is that build scan-shaped or derived?** Does `decompose_streaming_scan_probe_input(stage.build)` return `None`
   because the build is (a) a scan/exchange shape that *should* be streamed-scan-build eligible but is fenced out (an
   **eligibility miss**), or (b) a **derived/intermediate join output** (a join feeding this join's build) that the
   scan-build path legitimately cannot decompose?
4. **Did the planner pick the wrong (larger) build side?** "Reoriented" means the orientation was already flipped; Phase
   0 confirms whether the overflowing side is the genuinely-smaller build or an upstream build-side-selection problem.
5. **Census the fence:** what shapes `decompose_streaming_scan_probe_input` accepts vs rejects, and which of q08's join
   builds are scan-eligible vs derived.

Record the classification (stage, runtime entry point, scan-eligible-vs-derived verdict, orientation) in the results doc
**before** code.

### Phase 1 - the fork
- **(A1) Call-site miss** - the overflowing build is scan-decomposable, but the stage was routed through the
  materialized entry point. Extend the existing scan-build decision so this shape requests the streamed runtime, using a
  generic plan-shape / measured-size condition - no q08/table/query branch.
- **(A2) Eligibility miss** - the overflowing build is a scan/exchange shape that should stream, and streamed scan-build
  was requested, but the decomposer/fence rejected it. Extend the **existing** streamed scan-build path (loosen the
  `decompose_streaming_scan_probe_input` fence just enough), **shape-fenced and generic** - no q08/table/query branch -
  so that build takes `PartitionedScan`/`Spilled` instead of materializing.
- **(B) Genuine derived-build** - the overflowing build is a derived/intermediate join output the scan-build path
  cannot decompose. **Do not hack a q08-only spill path.** Seal Phase 0 as the evidence/spec for the bigger
  execution-model cut - intermediate/derived join-build spilling / streaming completion - and name it as the follow-on
  ADR. This is the honest "the big rock is big" outcome.

The fork is settled by Phase 0's runtime-entry + `decompose_streaming_scan_probe_input` verdict, not guessed up front.

## Acceptance
- **Phase 0 recorded:** the failing replay/stage, the runtime entry point (`stream_scan_build` true/false), the
  scan-eligible-vs-derived classification (with the `decompose_streaming_scan_probe_input` verdict), and the
  build-side-orientation finding - before code.
- **Either (A1/A2)** the shape-fenced streamed-scan-build routing/eligibility extension, **or (B)** a sealed diagnostic + named
  derived-build-spilling follow-on (no q08 hack).
- **Reach-board honesty:** under (A1/A2), q08 moves **off** `reoriented join build accumulator` - it either **passes**
  (persisted correctness `pass@compare`) **or advances to a new, explicitly named and reproduced wall**. Advancing off
  the accumulator with a named next wall is accepted progress; a silent same-wall re-fail is not.
- **Generic / shape-fenced:** no q08/table/query branch in core execution code; the fence is a plan-shape property,
  reusing the existing streamed scan-build runtime (no parallel spill path).
- **No cap bump:** the fix is execution (stream/spill the build), **not** raising `task_budget.memory` or the cgroup;
  runs stay under the ADR 398 scope (4 GB cgroup, ~1 GiB task budget).
- **Controls:** q19 and q04 sf=10 stay green (no regression); q19 is the working-spill reference (it already spills
  ~51 MB and passes). q03/q05/q10/q12 checked **only if cheap** - they are slow sf=10 windows and do not block this cut.
- **Full engine gate:** `--no-fail-fast` both crates; de-branded; no committed `.perf.data`.
- **Scope honesty:** q08 is the driver and the *first brick* of streaming-completion; this is **not** a claim that the
  >360s time cells are fixed. Under (B), the big rock explicitly remains.

## Guardrails
- **Reuse, don't reinvent** - extend the existing streamed scan-build runtime; no second spill path.
- **Diagnostic-first** - the call-site-miss vs decomposer-miss vs derived-build fork is decided by Phase 0 evidence, not
  asserted.
- **Derived-build -> stop-line** - if the build cannot use scan-build spill, defer to the named bigger cut; no q08-only
  heuristic.
- **Shape-fence generically** - the eligibility extension keys on a plan-shape property, not on q08.
- **No cap bump** - fixing memory pressure by enlarging the budget is out of bounds; the point is completion within it.
- **Fast controls only** - q19/q04 are the no-regress gate; slow windows are optional so iteration stays cheap.

## Risks
- **Derived-build (likely for an 8-table join)** -> (B): a legitimate outcome that scopes the bigger ADR, not a failure.
- **Routing/fence loosening breaks a streamed-path invariant** -> tight shape-fence; gate with the existing streamed-path
  tests + q19/q04 controls + q08 correctness.
- **Build-side orientation is the real problem** -> Phase 0 flags it; may be a cheaper upstream planner fix instead.
- **New wall after advancing** -> accepted if named + reproduced (reach-board move), not banked as a pass.

## Forward Arc
Under (A1/A2), q08 completing turns attention to the next heavy cells and the >360s **time** wall - the other half of
streaming-completion. Under (B), the intermediate/derived join-build spilling ADR is the named big-rock cut. Either
way this is the first concrete brick of Thread 3, with the Polars/DuckDB substrate gap (identical-shape q03 at ~4x/~10x)
as the horizon. Article-grade throughout.

## Grounding evidence (file:line)
Streamed scan-build runtime + spill/partition variants: `exec/join/streaming.rs:38`
(`ReorientedInnerJoinBuild::{InMemory,Spilled,PartitionedScan}`), `:8019`
(`prepare_reoriented_inner_join_runtime_with_streamed_scan_build`), `:8127`/`:8191` (PartitionedScan/Spilled
construction). Runtime call sites include streamed and materialized entries (`:7008` vs `:7014`; `:7511` for a
streamed-only semi/anti chain). Eligibility gate: `streaming.rs:8036`
(`stream_scan_build && decompose_streaming_scan_probe_input(stage.build).is_some()`). Fallback materialized path + wall:
`:8207` (`execute_bound(stage.build)`) -> `:8217` (`concat_or_empty`) -> `:8228` (`estimated_build_hash_bytes`) ->
`:8229-8236` / `:8246-8252` (`budget_exhausted(Memory, "reoriented join build accumulator", ...)`); cap =
`join_output_materialization_memory_cap(task_budget.memory)` (`:8219-8220`). q08 wall + fast-fail and the q19 working-
spill / q04 green controls: ADR 398 sf=10 board (`memory_budget_exhausted`, ~1.8 GiB / ~1 GiB, ~1.23s; q19 spill
~51 MB pass; q04 green).
