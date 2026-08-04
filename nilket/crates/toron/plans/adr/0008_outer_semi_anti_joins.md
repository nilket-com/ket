# ADR 008: Toron Outer / Semi / Anti Joins

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion implementation plan:**
`plans/impl/0008_outer_semi_anti_joins.md`

## Status

Promoted to `plans/adr/`. Implementation pending. Bench-results
artifact pending Phase 6 execution at
`plans/impl/0008_outer_semi_anti_joins_bench_results.md`.

Builds on three WIP iteration rounds across the council
(`plans/wip/{claude,codex,gemini}_{adr,impl}_008*.md`). Three-way
final convergence reached by iter-3; iteration artifacts retired
(git holds the narrative). Codex's `JoinOutputRow` row-selection
intermediate, both-sided semi/anti framing, broadcast
reject-outright posture, and verification-commands discipline;
Gemini's ANSI null-key phrasing and execution-state framing
(build-side bitset + after-pass for left-preserving variants);
Claude's scale-boundary convention adherence, single
`JoinOutputRow` materializer argument, `LogicalPlan::Join`
single-variant guard, mixed-binary-support contract framing, and
SQL-dialect fallback discipline are the load-bearing antecedents.

This ADR is a **narrow operator-level surface extension** that
closes the analytical-SQL "join width gap" recorded in the
post-ADR-007 roadmap candidate A. Eight-variant `JoinKind` (Inner
plus seven promoted kinds) extends the existing inner-equi-join
substrate from ADR 004 / ADR 005 / ADR 006 without new join
algorithms, new wire surface, new `TaskFailureKind` variants, or
runtime-filter eligibility changes.

## Goal

Extend the existing inner-equi-join substrate to cover the seven
remaining standard SQL join kinds — `LeftOuter`, `RightOuter`,
`FullOuter`, `LeftSemi`, `RightSemi`, `LeftAnti`, `RightAnti` —
without changing the cross-ring contract, the `TaskFailureKind`
shape, or the build-left / probe-right algorithm chosen by ADR
004 / ADR 005. ADR 008 pins **null-fill semantics on the
preserved side, existence-based filtering on semi/anti,
output-schema nullability adjustments, and
hash-partitioned-only distributed execution for non-Inner
kinds**. After ADR 008, the analytical-SQL "join width gap" is
closed at the operator level for the post-ADR-007 launch surface.

## Scale boundary

**Primary:** operator-level (single-operator surface extension
inside `crates/toron/src/exec.rs::execute_inner_join` →
`execute_hash_join` and peers, the `JoinKind` enum in
`crates/toron/src/ir.rs`, and the SQL surface in
`crates/toron/src/sql_planner.rs` + `crates/toron/src/binder.rs`).

**Secondary:** wire (additive `JoinKind` variants are serialized
inside `ExecutionPlan`). Serde-additive but **not**
backward-tolerant: receivers on older binaries reject unknown
variants. ADR 008 ships at a single workspace version bump.

**Not in scope:**
- No `TaskFailureKind` variant changes (preserves ADR 003
  7-variant guard).
- No `MessageKind` extension, no transport-protocol change.
- No runtime-filter eligibility extension — V1 stays Inner-only
  per ADR 006.
- No Wendelon `PartitionManifest` / `ScanRequest` change.
- No new `BudgetDimension` or telemetry counter.
- No `EXISTS` / `NOT EXISTS` decorrelation. Subquery-planner
  contract, future ADR.
- No new join algorithm (no sort-merge, no nested-loop, no
  non-equi, no cross, no `USING` / natural).
- No new `JoinStrategy` variant.
- No second `ExecutionPlan::Join` or `LogicalPlan::Join`
  physical variant.
- No rolling coordinator/worker mixed-version semantics. ADR 008
  promotes at a single workspace bump; clusters update together.

## V1 contract summary

- **`JoinKind` extends to 8 variants** (`Inner` plus seven
  promoted kinds). `LogicalPlan::Join` and `ExecutionPlan::Join`
  stay single-variant. `JoinStrategy` stays exactly
  `{ HashPartitioned, Broadcast }`.
- **Build-left / probe-right algorithm preserved** from ADR 004 /
  ADR 005. No new join algorithm; kind-specific logic lives in
  the probe loop and the materializer.
- **Hash-partitioned only for non-Inner.** Binder rejects
  `Broadcast` for `kind != JoinKind::Inner`. Fragmenter has a
  belt-and-braces guard against hand-built plans.
- **Runtime filters stay Inner-only** per ADR 006. Architecture
  guard pinned.
- **ANSI SQL `NULL != NULL` semantics strictly preserved.** Null
  keys never match. Preserved null-key rows emit per join-kind
  rules.
- **Duplicate semantics SQL-compatible.** Inner/outer preserve
  Cartesian multiplicity; semi/anti emit each preserved-side row
  at most once.
- **Output-schema nullability adjusted at bind time** via the
  single `join_schema(kind, left, right)` helper.
- **Arrow native null buffers** for null-fill; never sentinel
  values.
- **Spill semantics match in-memory semantics for every kind.**
  Memory budget is not a semantic switch.
- **No new wire surface, no `TaskFailureKind` variant, no new
  telemetry field, no Wendelon scan-contract change.**
- **Mixed old/new binary support is explicitly not promised.**
  Serde-tagged `JoinKind` variants are additive but older
  binaries reject unknown variants. Cluster upgrades are
  full-fleet, not rolling.

## Context

`JoinKind` lives at `crates/toron/src/ir.rs:117` and is currently
single-variant `{ Inner }`. The execution path at
`crates/toron/src/exec.rs:1226` carries the kind through but does
nothing with it (`match kind { JoinKind::Inner => {} }`).
Distributed planning at `crates/toron/src/distributed/fragment.rs:439`
gates runtime-filter emission on `JoinKind::Inner`. SQL planning at
`crates/toron/src/sql_planner.rs:84` and binding at
`crates/toron/src/binder.rs:174` always produce `Inner`. The
spilled-inner-join path at `execute_spilled_inner_join` does not
track build-side unmatched rows.

ADR 007 landed the L-2 launch blocker (Rune sandbox enforcement,
process-internal safety). The remaining launch-driven gap is SQL
surface width: outer joins are routine in analytical SQL
(left-outer dimension lookups, full-outer diff reports) and
semi/anti are routine for existence filtering (the SQL forms
`SEMI JOIN` / `ANTI JOIN` that `sqlparser` 0.54 exposes). The
roadmap pinned this as ADR 008 candidate A.

## Decision

### 1. `JoinKind` enum extension

```rust
pub enum JoinKind {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
}
```

`Copy + Eq + Hash + Serialize + Deserialize`. Externally-tagged
serde — each variant is a stable string on the wire.

`ExecutionPlan::Join` remains the single physical join variant.
`LogicalPlan::Join` remains the single logical join variant.
`JoinStrategy` remains exactly `{ HashPartitioned, Broadcast }`.

### 2. Equi-join predicate contract preserved

ADR 008 does not expand predicate expressiveness. Binder
continues to reject:
- non-equality `ON` predicates;
- computed join keys;
- key type mismatch;
- floating-point join keys;
- malformed hash-exchange shapes.

Inherited from ADR 004.

### 3. Output-schema nullability adjustment

Per-kind promotion via a single helper `join_schema(kind, left,
right) -> Schema`:

| Kind         | Output schema                                              |
|--------------|------------------------------------------------------------|
| `Inner`      | left fields ++ right fields, original nullability          |
| `LeftOuter`  | left original; right forced nullable                       |
| `RightOuter` | left forced nullable; right original                       |
| `FullOuter`  | both sides forced nullable                                 |
| `LeftSemi`   | left schema only, original nullability                     |
| `LeftAnti`   | left schema only, original nullability                     |
| `RightSemi`  | right schema only, original nullability                    |
| `RightAnti`  | right schema only, original nullability                    |

Computed once at bind time; execution receives the adjusted
schema.

### 4. Null-fill semantics (outer joins)

For each preserved side, emit a row per build-row or probe-row
that finds no match, with the other side's columns filled by
**Arrow's native `NullBuffer`** (not a sentinel value). The
output `RecordBatch` carries the correct null bitmap per column.

`FullOuter` is the union of left-outer and right-outer behavior:
unmatched build rows with null right, plus unmatched probe rows
with null left.

### 5. Existence-based filtering (semi/anti)

Build-left / probe-right preserved. The build hash is the same
machinery as inner joins.

- `LeftSemi`: emit each left row at most once iff at least one
  right match exists. Driven by the build-side matched bitmap
  after probe. Output = left schema.
- `LeftAnti`: emit each left row once iff no right match exists.
  Driven by the build-side matched bitmap after probe. Output =
  left schema.
- `RightSemi`: emit each right row at most once iff at least one
  left match exists. Decided during probe — when a probe row
  finds at least one match, emit it once. Output = right schema.
- `RightAnti`: emit each right row once iff no left match
  exists. Decided during probe — when a probe row finds no
  match, emit it. Output = right schema.

ANSI SQL `NULL != NULL` semantics strictly preserved across all
eight kinds.

### 6. Distributed strategy compatibility

`HashPartitioned`:
- All seven non-Inner kinds are correct under hash-partitioning
  when the partition key is the join key on both sides (already
  the `JoinStrategy::HashPartitioned` invariant).
- Matching keys are co-located; unmatched rows on either side
  stay local to their partition. Each partition emits its
  unmatched rows independently; the union of partition outputs
  is the global join output.

`Broadcast`:
- **Inner-only in V1.** `validate_strategy_shape(kind, strategy)`
  rejects `JoinStrategy::Broadcast` when `kind != JoinKind::Inner`.
- Reason: broadcast replicates the build side. Any preserved-side
  or existence variant would emit unmatched rows independently in
  every probe partition, creating duplicates. A coordinator-level
  reduction would fix it but blurs the operator-level scale
  boundary.
- The SQL planner defaults to `HashPartitioned` for non-Inner
  kinds; the binder rejection (gate 16) is a belt-and-braces
  guard. The fragmenter has a defense-in-depth check (gate 18)
  returning a typed `FragmentError` if a non-Inner broadcast
  plan reaches it.

### 7. Runtime-filter eligibility unchanged

ADR 006's runtime-filter gate at `fragment.rs:439`
(`matches!(join.kind, JoinKind::Inner)`) is **not** loosened in
V1. Architecture test pins this.

Reason: a Bloom runtime filter on the build side of a left-outer
join would eliminate probe rows lacking a build-side match — but
those are exactly the rows the outer join must preserve with null
fill. `RightAnti` depends on emitting unmatched probe rows, which
runtime filters could incorrectly drop. A "filter + preserve"
combined operator is a separable design and a future
evidence-driven ADR.

### 8. Execution-path rename + selection intermediate

Rename `execute_inner_join` → `execute_hash_join` at
`exec.rs:1221`. The probe phase returns a kind-specific selection
of rows; a shared materializer builds the output batch.

```rust
enum JoinOutputRow {
    Paired { left: u32, right: u32 },
    LeftFillNull { left: u32 },     // outer-left or full-outer
    RightFillNull { right: u32 },   // outer-right or full-outer
    LeftPreserved { left: u32 },    // LeftSemi / LeftAnti
    RightPreserved { right: u32 },  // RightSemi / RightAnti
}

fn materialize_join_selection(
    kind: JoinKind,
    left_batch: &RecordBatch,
    right_batch: &RecordBatch,
    selection: &[JoinOutputRow],
    output_schema: &SchemaRef,
) -> Result<RecordBatch, ExecutionError>
```

`JoinOutputRow` is private to `exec.rs`. The materializer uses
Arrow's `new_null_array` + `take` to build absent-side columns.
The build-left / probe-right algorithm is unchanged.

Per-kind probe-loop dispatch:

| Kind         | Probe-time emission                            | After-probe pass                       |
|--------------|------------------------------------------------|----------------------------------------|
| `Inner`      | `Paired` only                                  | none                                   |
| `LeftOuter`  | `Paired`; update build-side bitmap             | `LeftFillNull` for unmatched build     |
| `RightOuter` | `Paired`; `RightFillNull` for no-match probe   | none                                   |
| `FullOuter`  | `Paired`; bitmap; `RightFillNull` for no-match | `LeftFillNull` for unmatched build     |
| `LeftSemi`   | bitmap update only                             | `LeftPreserved` for matched build      |
| `LeftAnti`   | bitmap update only                             | `LeftPreserved` for unmatched build    |
| `RightSemi`  | `RightPreserved` once per matched probe row    | none                                   |
| `RightAnti`  | `RightPreserved` for no-match probe            | none                                   |

`Right*` kinds are entirely probe-time-decided. `Left*` kinds use
the build-side bitmap with an after-pass. Build side is never
swapped — ADR 004 build-left convention preserved.

### 9. SQL surface

`sql_planner.rs` maps `sqlparser` join operators directly:

- `JoinOperator::Inner(On(_))` → `JoinKind::Inner` (existing).
- `JoinOperator::LeftOuter(On(_))` → `JoinKind::LeftOuter`.
- `JoinOperator::RightOuter(On(_))` → `JoinKind::RightOuter`.
- `JoinOperator::FullOuter(On(_))` → `JoinKind::FullOuter`.
- `JoinOperator::Semi(On(_))` / `LeftSemi(On(_))` →
  `JoinKind::LeftSemi`.
- `JoinOperator::RightSemi(On(_))` → `JoinKind::RightSemi`.
- `JoinOperator::Anti(On(_))` / `LeftAnti(On(_))` →
  `JoinKind::LeftAnti`.
- `JoinOperator::RightAnti(On(_))` → `JoinKind::RightAnti`.

`JoinConstraint::On(...)` only — `USING`, natural joins, comma
joins, and cross joins remain rejected. Bare `Semi(_)` and
`Anti(_)` normalize to the `Left*` form per `sqlparser`
convention.

`EXISTS` / `NOT EXISTS` decorrelation is out of scope. Bare
`EXISTS` predicates remain unsupported — they hit the current
unsupported-expression rejection path in `sql_planner.rs`,
unchanged by ADR 008. A subquery-planner ADR is the right home
for `EXISTS` support.

**Dialect fallback.** If a future `sqlparser` bump regresses one
of the semi/anti spellings, keep the IR/executor variants; gate
SQL lowering to whatever the current dialect parses. Don't drop
IR variants because one spelling is absent — would break the
serde wire contract and the architecture guard.

### 10. Task wire stability

Failures crossing the task wire use the existing
`TaskFailureKind::TaskFailed { retryable, reason }`. No new
variant. ADR 003 7-variant architecture guard preserved.

### 11. Spill-path matched-build tracking

The in-memory build keeps a `matched: Vec<u64>` (bitset) over
build rows. Outer-join probe sets the bit on each match; the
after-pass emits unmatched build rows with null-filled probe
side. The spilled path keeps the same bitmap per spill partition.
Memory cost: 1 bit per build row, well below the
`TaskBudget::memory` envelope.

The spilled path reuses the ADR 005 build-side-hash-page spill
format **unchanged** — ADR 008 does not persist `JoinOutputRow`
vectors to disk. Semantic parity is the contract: after a
spilled build page is loaded and probed, the probe emits the
same `JoinOutputRow` selection representation as the in-memory
path, and the shared `materialize_join_selection` helper turns
it into output. Memory budget is **not** a semantic switch — a
forced spill produces byte-identical normalized output to the
in-memory path for every kind.

## Hard gates

### Correctness (1-13)

1. `inner_join_unchanged_under_kind_rename` — renamed
   `execute_hash_join` produces byte-identical output on every
   M7 inner-join fixture.
2. `left_outer_emits_null_filled_unmatched_left_rows`.
3. `right_outer_emits_null_filled_unmatched_right_rows`.
4. `full_outer_emits_union_with_unmatched_both_sides`.
5. `left_semi_emits_each_left_row_at_most_once_when_matched`.
6. `right_semi_emits_each_right_row_at_most_once_when_matched`.
7. `left_anti_emits_each_left_row_once_when_no_match`.
8. `right_anti_emits_each_right_row_once_when_no_match`.
9. `null_keys_never_match_on_either_side` — for all 8 kinds.
10. `outer_join_uses_arrow_null_buffer_not_sentinel`.
11. `duplicate_keys_preserve_multiplicity_inner_and_outer` —
    Cartesian for inner/outer; one-per-input for semi/anti.
12. `empty_left_empty_right_both_empty_for_all_kinds`.
13. `composite_key_fixture_covers_outer_and_semi_anti`.

### Distributed (14-18)

14. `hash_partitioned_full_outer_correct_across_partitions` —
    multi-partition full-outer matches single-partition baseline
    digest.
15. `hash_partitioned_left_outer_left_anti_distributed_digest_parity`.
16. `binder_rejects_broadcast_for_every_non_inner_kind` — table
    test over all 7 non-Inner kinds × `Broadcast`.
17. `runtime_filter_eligibility_stays_inner_only` —
    `runtime_filter_join_eligible` returns false for every
    non-Inner kind.
18. `fragment_broadcast_non_inner_returns_fragment_error` — a
    hand-built execution plan with `Broadcast` + non-Inner kind
    reaching the fragmenter (binder rejection bypassed) returns a
    typed `FragmentError`, never silently-incorrect tasks.
    Defense-in-depth pair with gate 16.

### Spill (19-20)

19. `spilled_output_matches_in_memory_digest_for_every_kind` —
    forced spill via low `TaskBudget::memory` produces identical
    normalized output to in-memory for all 8 kinds.
20. `spill_cleanup_runs_on_success_and_error_for_all_kinds` —
    spill files removed regardless of join kind or completion
    state.

### Surface stability (21-26)

21. `task_failure_kind_seven_variants` — ADR 003 guard.
22. `execution_plan_keeps_one_join_variant` — ADR 004 guard.
23. `logical_plan_keeps_one_join_variant` — new, pairs with 22.
24. `join_strategy_keeps_two_variants` — ADR 004 guard.
25. `join_kind_eight_variants_pinned` — new architecture guard.
26. `message_kind_and_wire_surface_unchanged` — no new
    `MessageKind`, `TaskFailureKind`, `BudgetDimension`,
    telemetry field, or Wendelon scan-contract change.

### Performance (27-28)

27. **Inner-join regression within ±10%** —
    `distributed_inner_equi_join` quick rows match post-ADR-006
    baseline within 10%.
28. **Per-kind quick-row matrix captured.** Evidence, not a
    promotion blocker, unless a kind shows >3× inner wall time
    on equivalent cardinalities.

## Rationale

### Why eight kinds, one ADR

Outer / semi / anti share the build-left / probe-right substrate:
the build hash, the probe iteration, the spill machinery, the
matched bitmap. Kind-specific logic is concentrated in the
probe-side selection switch and the materializer's null-fill
helper. Eight kinds, one operator, one materializer is one
decision; splitting into separate ADRs would duplicate the
contract framing without adding evidence-driven decisions.

Right-side semi/anti is included because `sqlparser` 0.54 exposes
both directions natively and the build-left / probe-right engine
supports both with identical infrastructure (`RightSemi` and
`RightAnti` decide at probe time; `LeftSemi` and `LeftAnti` decide
via the build-side matched bitmap after probe). Excluding
right-side would fragment the SQL surface without simplifying the
implementation.

### Why null buffers, not sentinels

Arrow's native null mask is the canonical representation. A
sentinel approach would require per-type sentinel choices, break
upstream consumers that already use Arrow null masks, and add
hidden coupling between operator output and downstream readers.
ADR 008 stays inside the Arrow contract.

### Why no runtime-filter pushdown for non-Inner

A Bloom runtime filter on the build side of a left-outer join
would eliminate probe rows that don't match the build — but those
are exactly the rows the outer join must preserve with null fill.
`RightAnti` depends on emitting unmatched probe rows, which
runtime filters could incorrectly drop. The "filter + preserve"
combined operator is nontrivial and orthogonal to the
null-semantics decision ADR 008 is making.

### Why no `TaskFailureKind` variant

There is no new failure class. The eight join kinds use the
existing build/probe machinery, the existing spill machinery, and
the existing budget envelope. A `JoinKind`-related failure is
either an invalid plan (caught at bind time, surfaces as
`FrontendError`) or a runtime resource failure (already mapped to
`BudgetExhausted` / `TaskFailed`). The 7-variant guard stays
load-bearing.

### Why `Broadcast` rejected outright (no rewrite) for non-Inner

Broadcast joins replicate the build side to every probe-partition
worker. For inner joins, that's safe — every probe partition
sees the full build, matched rows are produced partition-locally
without duplicates. For any preserved-side or existence variant,
broadcast leads to duplicate emission of unmatched rows. A
planner-time rewrite (sides-swap for right-outer, fallback for
full-outer) is possible but introduces strategy-dependent
semantics that blur the operator-level scale boundary. Cleaner
contract: binder rejects `Broadcast` for `kind != Inner`. A
strategy-rewrite ADR can follow if evidence supports it.

### Why no `EXISTS` / `NOT EXISTS` decorrelation in ADR 008

`EXISTS` decorrelation is a subquery-planner contract. It needs
its own decision matrix: which patterns are decorrelated, how
does the planner detect them, how does the binder fold them into
the `Semi` / `Anti` operator. ADR 008 ships the `Semi` / `Anti`
operators; a later subquery-planner ADR can leverage them.

### Why single `JoinOutputRow` row-selection intermediate

The alternative — per-kind probe loops with per-kind output
construction — duplicates null-fill code across paths and makes
the in-memory / spilled selection logic divergent (each kind
reasoning about emission separately on both paths). Centralizing
on `JoinOutputRow` means one selection/materializer contract: the
probe phase becomes a switch that picks the right `JoinOutputRow`
variant, the materializer is the single place for null-buffer
construction, and the spilled path emits the same selection
representation after lookup (no spill-format change — the ADR 005
build-side-hash-page format is reused unchanged). Slightly more
intermediate allocation than ad hoc; correctness is much easier
to argue. The ±10% perf gate accommodates this cost.

### Why architecture-guard `JoinKind` variant count

Counts `JoinKind` variants at exactly the ADR 008 set. Catches
drive-by expansion of the join surface that hasn't been pinned
through an ADR. Same pattern as ADR 003's `TaskFailureKind`
guard.

### Why mixed binary support not promised

`JoinKind` is serialized inside `ExecutionPlan` task payloads.
Externally-tagged serde adds variants as new strings on the wire;
older binaries deserialize unknown variants as errors. Promising
rolling mixed-version semantics would require either a
version-negotiation layer (cross-ring scope) or a fallback
variant on the wire (changes wire semantics for every receiver).
The right shape is the existing posture from prior ADRs:
full-fleet upgrades at promotion.

### Verification commands at promotion

```bash
cargo fmt -p toron --check
cargo check --workspace
cargo test -p toron --no-fail-fast
cargo test -p toron --test architecture_test --no-fail-fast
cargo test -p toron --test distributed_process_test --no-fail-fast
cargo test -p toron --test wendelon_storage_execution_test --no-fail-fast
cargo test -p toron --test runtime_filter_pushdown_test --no-fail-fast
cargo test -p toron --test hash_join_fast_path_parity_test --no-fail-fast
cargo test -p toron --test join_kind_test --no-fail-fast
cargo test -p toron --test hash_join_kinds_test --no-fail-fast
cargo test -p toron --test hash_join_spilled_kinds_test --no-fail-fast
cargo test --workspace --no-fail-fast
cargo bench -p toron --bench distributed_inner_equi_join --no-run
cargo bench -p toron --bench distributed_inner_equi_join -- --quick --distributed-inner-equi-join-only
cargo bench -p toron --bench distributed_inner_equi_join -- --quick --kind=left-outer --kind=full-outer --kind=left-semi --kind=left-anti
git diff --check
```

## Open questions

- **`sqlparser` 0.54 token exhaustiveness.** Does the SQL parser
  actually emit all eight `JoinOperator` shapes for every dialect
  we ship? Pinned to Phase 0 audit (P0.1). Fallback documented in
  §9.

## What Not To Do

- Don't introduce a second `ExecutionPlan::Join` or
  `LogicalPlan::Join` physical variant. Per-kind physical
  variants duplicate the operator shape and weaken ADR 004's
  single-variant architecture guard. The semantic difference
  belongs in `JoinKind`.
- Don't use sentinel values for null fill. Arrow's native
  `NullBuffer` is the canonical representation; sentinels break
  the contract and couple to downstream readers.
- Don't loosen runtime-filter eligibility beyond `Inner`. ADR 006
  invariant is load-bearing for outer / anti correctness.
  Architecture guard 17 enforces.
- Don't add a new `TaskFailureKind` variant for join semantics.
  ADR 003 7-variant guard stays. Join-related runtime failures
  crossing the task wire map to existing
  `TaskFailed { retryable, reason }`.
- Don't allow `Broadcast` for non-Inner kinds. Binder rejection
  (gate 16) + fragmenter defense (gate 18) form the
  defense-in-depth pair.
- Don't rewrite `RightOuter` ↔ `LeftOuter` at planner time as a
  workaround for broadcast. Strategy-dependent semantics blur the
  operator-level scale boundary. Separate ADR if evidence
  supports.
- Don't include `EXISTS` / `NOT EXISTS` decorrelation in ADR 008.
  Subquery-planner contract; separate narrow ADR.
- Don't promise rolling coordinator/worker mixed-version
  semantics for `JoinKind`. Full-fleet upgrade is the right
  shape; serde-tagged variant additions are not backward-
  tolerant.
- Don't drop IR variants if `sqlparser` regresses a spelling.
  Keep the IR variants; gate SQL lowering to whatever parses.
  Dropping IR variants breaks the serde wire contract and the
  architecture guard.
- Don't add per-kind task-telemetry counters in V1. Existing
  `build_rows` / `probe_rows` / `output_rows` counters are
  sufficient; surface kind-specific aggregates downstream if
  needed.
- Don't add a new `JoinStrategy` variant. The two-strategy
  contract from ADR 004 is preserved.
- Don't expand predicate expressiveness in ADR 008. The
  equi-join contract from ADR 004 (no non-equi, no computed
  keys, no float keys, no `USING`, no natural, no cross) stays.

## Rejected alternatives

- **Six kinds (drop `RightSemi` / `RightAnti`).** Fragments the
  SQL surface without simplifying the implementation.
- **Seven separate ADRs, one per kind.** Duplicate contract
  framing across kinds that share the same substrate.
- **Broadcast rewrite for non-Inner kinds.** Strategy-dependent
  semantics blur the operator-level scale boundary.
- **`EXISTS` / `NOT EXISTS` decorrelation in ADR 008.**
  Subquery-planner contract; separate narrow ADR.
- **Sentinel-based null fill.** Breaks the Arrow contract and
  couples to downstream readers.
- **Loosen runtime-filter eligibility.** Conflates
  null-preservation correctness with pushdown correctness; future
  evidence-driven ADR.
- **New `TaskFailureKind::JoinSemantics` variant.** No new
  failure class exists.
- **Second `ExecutionPlan::Join` or `LogicalPlan::Join` physical
  variant.** Per-kind physical variants duplicate the operator
  shape and weaken ADR 004's architecture guard.
- **Per-kind probe loops without a row-selection intermediate.**
  Duplicates null-fill code; makes spill divergent.
- **Rolling coordinator/worker mixed-version semantics for
  `JoinKind`.** Would require wire-level version negotiation or
  a serde fallback variant.
- **`JoinKind::Cross` / `JoinKind::AsOf`.** Out of scope.
- **Per-kind task-telemetry counters.** Existing counters
  sufficient.

## Council convergence summary

Three-way agreement at iter-3:

| Decision                                              | Status      |
|-------------------------------------------------------|-------------|
| Eight `JoinKind` variants                             | converged   |
| Build-left / probe-right algorithm preserved          | converged   |
| Hash-partitioned only for non-Inner                   | converged   |
| Broadcast for non-Inner: reject outright              | converged   |
| Runtime-filter eligibility stays Inner-only           | converged   |
| ANSI null-key semantics                               | converged   |
| Output-schema nullability promotion at bind time      | converged   |
| Arrow native `NullBuffer` over sentinels              | converged   |
| `JoinOutputRow` row-selection intermediate            | converged   |
| Single materializer with `new_null_array` + `take`    | converged   |
| Spill semantics match in-memory                       | converged   |
| No new `TaskFailureKind` / `MessageKind` / telemetry  | converged   |
| Mixed binary support not promised                     | converged   |
| `LogicalPlan::Join` + `ExecutionPlan::Join` 1-variant | converged   |
| `JoinKind` variant-count architecture guard           | converged   |
| Inner-join regression gate ±10%                       | converged   |
| Bench-harness shape: extend with `--kind`             | converged   |
| Bench-results artifact at promotion                   | converged   |
| Single-bump cluster upgrade, no rolling               | converged   |
| `EXISTS` decorrelation deferred                       | converged   |

Remaining items at promotion are pin-level Phase 0 audit work
(P0.1 sqlparser token check, P0.3 bitmap storage representation),
recorded in the impl plan.
