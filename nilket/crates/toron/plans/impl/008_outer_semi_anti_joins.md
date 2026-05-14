# Implementation Plan: ADR 008 — Outer / Semi / Anti Joins

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion ADR:**
`plans/adr/008_outer_semi_anti_joins.md`
**Bench-results artifact:** pending Phase 6 execution at
`plans/impl/008_outer_semi_anti_joins_bench_results.md`

## Status

Promoted to `plans/impl/`. Implementation pending.

Built across three WIP iteration rounds with the AI Swarm Council
(`plans/wip/{claude,codex,gemini}_impl_008*.md`). Three-way
final convergence reached by iter-3.

## Substrate

- `crates/toron/src/ir.rs:117` — `JoinKind` enum (1 variant
  today, extends to 8). `LogicalPlan::Join` and
  `ExecutionPlan::Join` both stay single-variant.
- `crates/toron/src/exec.rs:1221` — `execute_inner_join`, renamed
  + branched in Phase 2.
- `crates/toron/src/exec.rs` — `execute_spilled_inner_join`,
  renamed + extended in Phase 3.
- `crates/toron/src/binder.rs:174` — `bind_join` builder.
  `validate_strategy_shape` gains the `kind` parameter; new
  `join_schema(kind, left, right)` helper.
- `crates/toron/src/sql_planner.rs:84` — SQL `JOIN` decoder.
- `crates/toron/src/distributed/fragment.rs:431-439` —
  runtime-filter eligibility kept Inner-only; belt-and-braces
  fragmentation guard added.
- `crates/toron/tests/architecture_test.rs` — guards for
  `TaskFailureKind` variant count (preserved),
  `ExecutionPlan::Join` shape (preserved), new
  `LogicalPlan::Join` single-variant guard, new `JoinKind`
  variant-count guard.
- `crates/toron/tests/distributed_inner_equi_join_test.rs` —
  inner-join regression surface.
- `crates/toron/benches/distributed_inner_equi_join.rs` —
  extended with `--kind` quick-row option.

## Goal

Ship the 8-variant `JoinKind` contract from ADR 008 with: correct
null-fill semantics for outer joins, correct existence semantics
for semi/anti (both directions), output-schema nullability
promotion, broadcast-rejection for non-Inner kinds, and zero
semantic regression on the inner-join path. No cross-ring
contract changes. No `TaskFailureKind` extension. No `EXISTS`
decorrelation. M7 inner-join byte-identity preserved; bench
within ±10% of post-ADR-006 baseline.

## Phases

### Phase 0 — Audit + golden matrix

Eight sub-pins, each producing a concrete decision recorded in
this doc before code lands:

- **P0.1: `sqlparser` 0.54 token inventory.** Confirm exact
  variant names: `JoinOperator::LeftOuter`, `RightOuter`,
  `FullOuter`, `Semi`, `LeftSemi`, `RightSemi`, `Anti`,
  `LeftAnti`, `RightAnti`. Confirm bare `Semi(_)` / `Anti(_)`
  normalize to `LeftSemi` / `LeftAnti` in our planner.
- **P0.2: `JoinKind` variant naming.** Confirm
  `LeftOuter`/`RightOuter`/`FullOuter`/`LeftSemi`/`RightSemi`/`LeftAnti`/`RightAnti`.
  Stable from V1 — serde-tagged enum, pinned by architecture
  guard.
- **P0.3: Build-side matched-bitmap storage.** Pin
  representation (`Vec<u64>` vs an existing bitset helper). Pin
  survival across spill round-trips. Memory budget assertion:
  1 bit per build row.
- **P0.4: `JoinOutputRow` shape.** Pin the enum shape used by
  probe + materializer. Confirm the spilled path reuses the
  existing ADR 005 build-side-hash-page spill format unchanged
  — ADR 008 does not persist `JoinOutputRow` vectors to disk;
  the spilled path emits the same selection representation after
  lookup (semantic parity, not format change). If Phase 3 finds
  a case that genuinely needs selection persistence, that
  becomes an explicit pin + hard gate added here.
- **P0.5: Output-schema nullability table.** Pin the per-kind
  promotion rule (ADR §3) as `join_schema(kind, left, right)`.
  Identify all binder + ir call sites that need updating.
- **P0.6: Golden fixture matrix.** Enumerate test fixtures used
  across Phases 2-4:
  - duplicate left keys, duplicate right keys;
  - unmatched left rows, unmatched right rows;
  - null left keys, null right keys;
  - empty-left, empty-right, both-empty;
  - composite-key fixture.
  Expected row counts per join kind tabulated.
- **P0.7: M7 inner-join fixture inventory.** List every test
  that constructs `JoinKind::Inner` and verify each is unchanged
  after the rename + executor refactor.
- **P0.8: Broadcast-rejection error shape.** Pin which existing
  `BindingError` variant carries the rejection (or whether a
  new typed variant is justified). Default: reuse
  `BindingError::UnsupportedExpression` with a stable diagnostic
  message; add a new variant only if tests would be ambiguous.

Phase 0 produces eight `// P0.x:` comments in this doc, then a
single commit `plan: adr 008 outer / semi / anti joins`.

### Phase 1 — IR, binder, SQL surface

Files:
- `crates/toron/src/ir.rs`
- `crates/toron/src/binder.rs`
- `crates/toron/src/sql_planner.rs`
- `crates/toron/tests/architecture_test.rs`

Work items:

- **A. `JoinKind` extension.** Add the seven new variants. Keep
  `Copy + Eq + Hash + Serialize + Deserialize`. Externally-tagged
  serde stays the default.

- **B. `join_schema(kind, left, right)` helper.** Per the ADR §3
  table. Pure function; called once at bind time. Includes a
  private `nullable_schema(schema: &Schema) -> Vec<Field>` helper
  that preserves names + types and forces `nullable = true`.

- **C. `validate_strategy_shape(kind, strategy, ...)` extension.**
  Add `kind` argument. Reject `Broadcast` when
  `kind != JoinKind::Inner` using the P0.8 error variant.

- **D. SQL planner outer/semi/anti decode.** Extend
  `sql_planner.rs:84` per ADR §9 mapping table.
  `JoinConstraint::On(_)` only. The planner continues to emit
  explicit `HashPartitioned` exchanges on both sides for
  non-Inner kinds.

- **E. Architecture guard updates.**
  - New guard `architecture_guard_join_kind_eight_variants` —
    greps `ir.rs` for the `JoinKind` block, asserts exactly 8
    variants + stable label list.
  - New guard
    `architecture_guard_logical_plan_join_single_variant` —
    pairs with the existing `ExecutionPlan::Join` single-variant
    guard.
  - Existing guards stay green:
    `task_failure_kind_keeps_partition_store_errors_separate`
    (7 variants), `execution_plan_join_single_variant` (1
    variant), `join_strategy_two_variants`.

- **F. `EXISTS` decorrelation untouched.** `EXISTS` / `NOT
  EXISTS` predicates remain on the current unsupported-
  expression rejection path in `sql_planner.rs` — there is no
  scalar-subquery lowering today and ADR 008 adds none. No
  rewrite to `Semi` / `Anti` in Phase 1.

Tests (new file `tests/join_kind_test.rs`):
- `join_kind_serde_round_trips_all_eight_variants`.
- `output_schema_nullability_table_for_each_kind` — exhaustive
  table assertion.
- `bind_join_left_outer_schema_marks_right_side_nullable`.
- `bind_join_right_outer_schema_marks_left_side_nullable`.
- `bind_join_full_outer_marks_both_sides_nullable`.
- `bind_join_semi_anti_schema_exposes_preserved_side_only` —
  one test per direction.
- `bind_join_rejects_broadcast_for_every_non_inner_kind` — table
  test over all 7 non-Inner kinds + `Broadcast`. **Hard gate 16.**
- `sql_left_outer_join_lowers_to_left_outer_kind`.
- `sql_right_outer_join_lowers_to_right_outer_kind`.
- `sql_full_outer_join_lowers_to_full_outer_kind`.
- `sql_left_semi_join_lowers_to_left_semi_kind`.
- `sql_right_semi_join_lowers_to_right_semi_kind`.
- `sql_left_anti_join_lowers_to_left_anti_kind`.
- `sql_right_anti_join_lowers_to_right_anti_kind`.
- `sql_bare_semi_normalizes_to_left_semi`.
- `sql_bare_anti_normalizes_to_left_anti`.
- `sql_join_rejects_using_natural_cross_for_all_promoted_kinds`.
- `sql_exists_predicate_remains_unsupported` — pins that bare
  `EXISTS` / `NOT EXISTS` still hits the unsupported-expression
  rejection path; ADR 008 does not change it.
- `architecture_guard_join_kind_eight_variants` — **Hard gate
  25.**
- `architecture_guard_logical_plan_join_single_variant` — **Hard
  gate 23.**

Gate: `cargo test -p toron join_kind`.

### Phase 2 — Executor selection model

Files:
- `crates/toron/src/exec.rs`

Work items:

- **A. Rename `execute_inner_join` →  `execute_hash_join`.** No
  signature change for the rename itself.

- **B. `JoinOutputRow` enum + materializer.** Per ADR §8.
  Private to `exec.rs`; not exposed beyond the operator.

  ```rust
  enum JoinOutputRow {
      Paired { left: u32, right: u32 },
      LeftFillNull { left: u32 },
      RightFillNull { right: u32 },
      LeftPreserved { left: u32 },
      RightPreserved { right: u32 },
  }

  fn materialize_join_selection(
      kind: JoinKind,
      left_batch: &RecordBatch,
      right_batch: &RecordBatch,
      selection: &[JoinOutputRow],
      output_schema: &SchemaRef,
  ) -> Result<RecordBatch, ExecutionError>
  ```

  Null-filled absent-side columns use `new_null_array` + `take`,
  not per-type sentinel construction.

- **C. Inner-join byte-identity test (FIRST).** Same M7 fixtures
  as current inner-join tests run against the refactored
  `execute_hash_join`. Output digest matches the pre-refactor
  digest. **Hard gate 1.** This is the first test that runs in
  Phase 2 — if it fails, the refactor is reverted before
  proceeding.

- **D. Probe-side kind dispatch** (ADR §8 table):

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

  `Right*` kinds are entirely probe-time-decided — no after-pass.
  `Left*` kinds use the build-side bitmap with an after-pass.
  Build side never swapped; ADR 004 convention preserved.

- **E. Build-side matched bitmap allocation.** `Vec<u64>` sized
  to `left_batch.num_rows()`. Allocated only for kinds that need
  it: `LeftOuter`, `FullOuter`, `LeftSemi`, `LeftAnti`. Other
  kinds skip the allocation.

Tests (new file `tests/hash_join_kinds_test.rs`):
- `inner_join_byte_identical_after_executor_refactor` — **Hard
  gate 1.**
- `left_outer_emits_null_filled_unmatched_left` — **Hard gate
  2.**
- `right_outer_emits_null_filled_unmatched_right` — **Hard gate
  3.**
- `full_outer_emits_union_with_unmatched_both_sides` — **Hard
  gate 4.**
- `left_semi_emits_each_left_at_most_once` — **Hard gate 5.**
- `right_semi_emits_each_right_at_most_once` — **Hard gate 6.**
- `left_anti_emits_unmatched_left_once` — **Hard gate 7.**
- `right_anti_emits_unmatched_right_once` — **Hard gate 8.**
- `null_keys_never_match_for_all_kinds` — **Hard gate 9.**
- `outer_join_uses_arrow_null_buffer_not_sentinel` — **Hard gate
  10.**
- `duplicate_keys_inner_outer_multiplicity_preserved` — **Hard
  gate 11a.**
- `duplicate_keys_semi_anti_one_row_per_input` — **Hard gate
  11b.**
- `empty_left_for_each_kind` — **Hard gate 12a.**
- `empty_right_for_each_kind` — **Hard gate 12b.**
- `both_empty_for_each_kind` — **Hard gate 12c.**
- `composite_key_left_outer` — **Hard gate 13a.**
- `composite_key_left_semi` — **Hard gate 13b.**

Gate: `cargo test -p toron hash_join_kinds`.

### Phase 3 — Spilled path extension

Files:
- `crates/toron/src/exec.rs`

Work items:

- **A. Rename spilled helpers.** `execute_spilled_inner_join` →
  `execute_spilled_hash_join`. Spill format is the existing
  ADR 005 build-side-hash-page format, unchanged (see P0.4).

- **B. Per-partition matched bitmap.** Each spilled build
  partition keeps a matched bitmap during probe. After probe
  completes for a build partition, scan the bitmap to emit
  unmatched build rows for `LeftOuter`, `FullOuter`, `LeftAnti`;
  emit matched build rows for `LeftSemi`.

- **C. Shared selection representation after spill lookup.** The
  spilled path does not persist `JoinOutputRow` vectors — it
  keeps the ADR 005 build-side-hash-page spill format. After a
  spilled build page is loaded and probed, the probe emits the
  same `JoinOutputRow` representation as the in-memory path, and
  the shared `materialize_join_selection` helper turns it into
  output. Semantic parity, not a spill-format change.

- **D. Cleanup invariant.** Spill files removed on every
  termination path (success, error, drop). Existing cleanup
  pattern from ADR 005 preserved; new kinds don't introduce a
  new cleanup branch.

Tests (new file `tests/hash_join_spilled_kinds_test.rs`):
- `spilled_left_outer_matches_in_memory_digest`.
- `spilled_right_outer_matches_in_memory_digest`.
- `spilled_full_outer_matches_in_memory_digest`.
- `spilled_left_semi_matches_in_memory_digest`.
- `spilled_right_semi_matches_in_memory_digest`.
- `spilled_left_anti_matches_in_memory_digest`.
- `spilled_right_anti_matches_in_memory_digest`.
- `spilled_inner_matches_in_memory_digest` (regression).
- All eight collapse to **Hard gate 19.**
- `spill_cleanup_runs_on_success_for_all_kinds` — **Hard gate
  20a.**
- `spill_cleanup_runs_on_error_for_all_kinds` — **Hard gate
  20b.**

Gate: `cargo test -p toron hash_join_spilled_kinds`.

### Phase 4 — Distributed verification

Files:
- `crates/toron/src/distributed/fragment.rs` — runtime-filter
  eligibility (unchanged) + defensive fragmentation guard.
- `crates/toron/tests/distributed_process_test.rs` — distributed
  digest parity for non-Inner kinds.
- `crates/toron/tests/runtime_filter_pushdown_test.rs` —
  non-Inner runtime-filter ineligibility.

Work items:

- **A. Runtime-filter eligibility test.** Table test:
  `runtime_filter_join_eligible` returns false for every
  non-Inner `JoinKind`. **Hard gate 17.**

- **B. Hash-partitioned distributed digest parity.** Distributed
  process tests for `LeftOuter`, `LeftSemi`, `LeftAnti`,
  `FullOuter` — multi-worker hash-partitioned execution produces
  digest equal to single-process baseline. **Hard gates 14, 15.**

  Right-side variants covered by unit/integration tests; the
  build-left convention means right-side correctness in the
  distributed setting follows from the same hash-partition
  invariant.

- **C. Belt-and-braces fragmentation guard.** If a hand-built
  execution plan reaches the fragmenter with `Broadcast` +
  non-Inner kind (binder rejection bypassed), return a typed
  `FragmentError`, never silently-incorrect tasks. Defense in
  depth, paired with the binder rejection (gate 16).

Tests:
- `runtime_filter_eligible_false_for_every_non_inner_kind` —
  **Hard gate 17.**
- `fragment_broadcast_non_inner_returns_fragment_error` — **Hard
  gate 18.**
- `distributed_left_outer_matches_single_process_digest` —
  **Hard gate 15a.**
- `distributed_left_anti_matches_single_process_digest` —
  **Hard gate 15b.**
- `distributed_full_outer_matches_single_partition_baseline` —
  **Hard gate 14.**

Gate:
```bash
cargo test -p toron --test distributed_process_test --test runtime_filter_pushdown_test
```

### Phase 5 — Architecture + wire stability guards

Files:
- `crates/toron/tests/architecture_test.rs`

Work items:

- **A. Existing guards stay green:**
  - `task_failure_kind_keeps_partition_store_errors_separate`
    (7 variants) — **Hard gate 21.**
  - `execution_plan_join_single_variant` — **Hard gate 22.**
  - `join_strategy_two_variants` — **Hard gate 24.**
  - `message_kind_unchanged` and wire-shape guards — **Hard gate
    26.**
- **B. New guards landed in Phase 1E** —
  `architecture_guard_logical_plan_join_single_variant`
  (**Hard gate 23**) and
  `architecture_guard_join_kind_eight_variants`
  (**Hard gate 25**).

Gate: `cargo test -p toron --test architecture_test`.

### Phase 6 — Performance verification + promotion

Files:
- `crates/toron/benches/distributed_inner_equi_join.rs` — extend
  with `--kind` quick-row option. **Preserve every existing
  inner-join row label** (load-bearing in CI scripts).
- `crates/toron/plans/impl/008_outer_semi_anti_joins_bench_results.md`
  — promotion artifact.

Work items:

- **A. Inner-join regression check.** Run
  `cargo bench -p toron --bench distributed_inner_equi_join --
  --quick --distributed-inner-equi-join-only`. Compare to the
  post-ADR-006 baseline. **Within ±10%.** **Hard gate 27.**

- **B. Per-kind quick rows.** Extend the bench with
  `--kind=left-outer --kind=full-outer --kind=left-semi
  --kind=left-anti` quick rows over identical cardinalities to
  inner. Record wall time, row count, canonical digest, task
  telemetry.

- **C. Promotion artifact.**
  `008_outer_semi_anti_joins_bench_results.md` captures bench
  output, baseline comparison, ±10% inner-join regression check,
  and the per-kind quick-row matrix. **Hard gate 28**:
  promotion-blocker only if a kind exposes pathological behavior
  (>3× inner wall time on equivalent cardinalities).

Gate: full bench run + `cargo test --workspace --no-fail-fast`.

## Verification commands at promotion

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

## Risks + mitigations

- **R1: Inner-join byte-identity regression from the
  `JoinOutputRow` refactor.** Most invasive change.
  *Mitigation:* P0.7 fixture inventory is exhaustive; gate 1 is
  the first test that runs in Phase 2; if it fails, the refactor
  is reverted before proceeding to other kinds.

- **R2: `serde` variant drift on the wire.** Externally-tagged
  enums reject unknown variants on read.
  *Mitigation:* ADR 008 ships at a single workspace version
  bump. Architecture guard 25 pins the variant list.

- **R3: Outer-join correctness under hash-partitioning.**
  Unmatched build/probe rows must surface per-partition without
  cross-partition deduplication.
  *Mitigation:* hash-partition invariant (matching keys
  co-located) guarantees this — gates 14, 15 assert it.

- **R4: Broadcast non-Inner reaches the fragmenter through a
  hand-built plan.**
  *Mitigation:* Phase 4C adds a fragmentation-level guard
  returning a typed `FragmentError` (gate 18). Defense in depth.

- **R5: Spilled build-bitmap survival.**
  *Mitigation:* P0.3 pins the storage; gate 19 verifies in-memory
  and spilled produce identical multisets for every kind.

- **R6: Null semantics divergence between semi/anti and SQL.**
  *Mitigation:* gate 9 pins this with fixtures that have null
  keys on both sides; ANSI `NULL != NULL` strictly preserved.

- **R7: Output-schema nullability drift.**
  *Mitigation:* gates 2-4 (executor output), gate 10 (Arrow null
  buffer present), Phase 1B (`join_schema` single source of
  truth).

- **R8: `EXISTS` rewrite scope creep.**
  *Mitigation:* ADR §rationale documents this is a
  subquery-planner contract; scope boundary explicitly excludes.

- **R9: Runtime-filter loosening pressure.**
  *Mitigation:* ADR §rationale documents the correctness
  obstacle; gate 17 enforces unchanged eligibility.

- **R10: Cross-ring contract drift.**
  *Mitigation:* scale-boundary section explicit about
  "secondary: wire" classification; gate 26 asserts no other
  transport surface changes.

- **R11: Bench fixture cardinality mismatch.**
  *Mitigation:* P6 bench fixtures pin both fully-matched and
  partially-matched cardinalities at the same total cardinality
  as inner.

- **R12: `FullOuter` + spilled + multi-partition correctness.**
  Composition of three branches.
  *Mitigation:* `spilled_full_outer_matches_in_memory_digest`
  (Phase 3) and
  `distributed_full_outer_matches_single_partition_baseline`
  (Phase 4) are independent oracles.

- **R13: `JoinOutputRow` allocation overhead.**
  *Mitigation:* gate 27 (±10% inner-join regression) is the
  guardrail. If the bench shows >10% regression, options are
  (a) preallocate selection buffer per probe partition,
  (b) inline the materializer for `Inner` if necessary.
  Reversible refactor.

- **R14: SQL planner emits broadcast for non-Inner.**
  *Mitigation:* gate 16 (binder rejection) catches this
  regardless of planner-level decisions; binder is the
  authoritative gate.

- **R15: `sqlparser` 0.54 dialect regression on semi/anti
  spellings.** Future dialect bump drops a variant.
  *Mitigation:* ADR §9 dialect fallback — keep the IR variants;
  gate SQL lowering to whatever parses; don't remove IR variants
  because a spelling is absent.

## Definition of done

- Phase 0 audit complete: eight `// P0.x:` decisions recorded in
  this doc, plus the planning commit.
- Phases 1-5 complete: hard gates 1-26 passing.
- Phase 6 complete: bench artifact landed at
  `crates/toron/plans/impl/008_outer_semi_anti_joins_bench_results.md`,
  inner-join regression within ±10% (gate 27), per-kind
  quick-row matrix shows no pathological behavior (gate 28).
- All verification commands above green.
- Documentation: ADR 008 (`plans/adr/008_outer_semi_anti_joins.md`)
  and impl plan (`plans/impl/008_outer_semi_anti_joins.md`) land
  at promotion; bench-results artifact alongside.
- Roadmap (`plans/wip/roadmap.md`) updated: ADR 008 marked
  Promoted + implemented; ADR 009 candidates reordered with
  "Outer / semi / anti joins" removed from the candidate list.

## Files touched (expected)

- `crates/toron/src/ir.rs` — `JoinKind` extension.
- `crates/toron/src/binder.rs` — `bind_join` + `join_schema` +
  `validate_strategy_shape` extensions.
- `crates/toron/src/exec.rs` — `execute_hash_join` (renamed),
  `JoinOutputRow`, `materialize_join_selection`, spilled
  variants.
- `crates/toron/src/sql_planner.rs` — outer/semi/anti operator
  decode.
- `crates/toron/src/distributed/fragment.rs` — defensive
  fragmentation-time guard (Phase 4C).
- `crates/toron/tests/architecture_test.rs` — two new guards
  (`join_kind_eight_variants`,
  `logical_plan_join_single_variant`).
- `crates/toron/tests/join_kind_test.rs` — new.
- `crates/toron/tests/hash_join_kinds_test.rs` — new.
- `crates/toron/tests/hash_join_spilled_kinds_test.rs` — new.
- `crates/toron/tests/distributed_process_test.rs` — added
  parity tests.
- `crates/toron/tests/runtime_filter_pushdown_test.rs` —
  non-Inner ineligibility table test.
- `crates/toron/benches/distributed_inner_equi_join.rs` —
  per-kind quick rows.
- `crates/toron/plans/impl/008_outer_semi_anti_joins_bench_results.md`
  — promotion artifact.

## Files not expected to change

- `crates/toron/src/wire.rs`
- `crates/toron/src/distributed/task.rs`
- `crates/toron/src/distributed/budget.rs`
- `crates/toron/src/error/distributed.rs` (`TaskFailureKind`
  shape preserved)
- `crates/toron/src/server/mod.rs` (no new failure-mapping path)
- Wendelon crates.

## Promotion / fallback

Promote if all 26 semantic/distributed/spill/surface gates pass,
gate 27 (±10% inner-join regression) holds, and gate 28 (per-kind
quick-row matrix) shows no pathological behavior (>3× inner wall
time on equivalent cardinalities).

If spill semantics become the only blocker, do **not** ship a
memory-cap-dependent partial implementation (memory budget would
become a semantic switch). Either finish Phase 3 or scope the
ADR down in a follow-up.

If broadcast non-Inner pressure appears during review, defer it.
The correct shape is a separate ADR with a matched-build
reduction proof or strategy-specific preservation argument.

If `EXISTS` decorrelation pressure appears during review, hold
the boundary. ADR 008 ships `Semi` / `Anti` operators; the
subquery-planner ADR can leverage them.

If `sqlparser` dialect support for semi/anti is inconsistent at
implementation time, keep the IR/executor variants and gate SQL
lowering to the variants the current dialect parses. **Do not
remove IR variants solely because one dialect spelling is
absent** — the spillover from a removed IR variant breaks the
serde wire contract and the architecture guard.

If gate 27 fails by >10%, R13 mitigations are reversible; ADR
008 promotion does not block on second-order tuning.
