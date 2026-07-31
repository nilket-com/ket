# ADR 068 FsstRowSafe Writer Estimator Evidence

Status: writer-column-cache route implemented; performance
gates now pass on the fixed cost-free row-safe-floor route;
read-side predicate-mask invariants covered by focused tests;
mixed-workload delta closure discharged by recorded justification.

## Naming prerequisite

Closed in the current staged source tree. The whole-buffer
FSST cascade and evaluator vocabulary is now
`FsstWholeBuffer`; the writer/reader labels are
`fsst_whole_buffer` and `fsst_row_safe`; and the frame
magics are `b"WBFS"` for whole-buffer FSST and `b"RSFS"`
for row-safe FSST. The numeric discriminants remain
unchanged (`FsstWholeBuffer = 7`, `FsstRowSafe = 9`,
`EncodingComputeEvaluatorKind::FsstWholeBuffer = 7`,
`EncodingComputeEvaluatorKind::FsstRowSafe = 8`).

## Phase 0 audit

- **P1 sample shape:** V1 implementation pins
  `FsstRowSafeEstimatorSampleShape::DeterministicSpread`,
  reusing ADR 057's deterministic sample indices and
  `cascade_sample_rows` budget. Focused calibration in
  `deterministic_spread_sample_shape_bounds_prefix_projection_error`
  compares deterministic spread against contiguous prefix
  projection at 8,192 rows and 256 sampled rows. Spread is
  required to be no worse than prefix on URL equality, URL
  `InList`, long repeated pair, and log-line clustered
  winner fixtures, with strict improvement on at least three
  fixtures; the weak-compression diagnostic is required to
  stay within 5% of exact row-safe encoded length.
- **P2 fixed row-safe header bytes:** pinned at 13 bytes
  in source (`b"RSFS"` magic, one symbol-count byte,
  row-count field, exception-count field). The shared
  metadata helper adds symbol-table bytes, block offsets,
  per-row encoded lengths, and exception lengths. The
  implemented cost-free floor also adds
  `non_empty_row_count` as an exact minimum payload lower
  bound for short non-empty rows.
- **P3 early-decline safety multiplier:** pinned at 6/5
  (1.20x) as a crate-private rational constant.
- **P4 symbol-table reuse:** source refactor exposes
  trained whole-buffer and row-safe FSST tables to the
  estimator. `RequireExactGuard` computes exact encoded
  lengths from those trained tables before allowing
  `FsstRowSafe` selection; uncertainty falls back to the
  exact route.
- **P5 symbol-training cost:** measured with the ignored
  `fsst_symbol_training_cost_probe` calibration test at
  8,192 rows and 20 iterations after replacing full pair
  candidate sorting with deterministic top-127 selection,
  tracking touched pair keys so table construction scans only
  observed pairs, counting whole-buffer pairs in one
  contiguous pass, deriving row-safe counts by subtracting
  unique row-boundary pairs, and avoiding a second 65,536-entry
  count-table clone. Current dual symbol-table training still
  exceeds the 25% threshold against full row-safe greedy
  payload traversal on all probed fixtures: URL equality
  1.30x, URL `InList` 1.29x, long repeated pair 1.03x,
  weak-compression diagnostic 0.67x, and log-line 1.08x.
  This triggered the iter-10 writer column-cache pivot: a
  writer-pass-local cache keyed by `IpcColumnOrdinal`
  amortizes training across row groups in the same
  column. The P5 denominator remains full row-safe
  greedy payload traversal; the numerator becomes
  amortized warm-evaluated-buffer training share.
  Production directory-writer column-cache calls now route through
  the estimator only when a writer context and
  `IpcColumnOrdinal` are present; standalone no-context
  selection remains on the exact route to avoid cold
  single-buffer overhead.
- **P6 estimator module location:** pinned to
  `src/storage/encoding/cascade/estimator.rs`.
- **P7 bench-internal counter shape:** pinned to the
  existing `encoding_compute_bench scenario=...` stdout
  diagnostic rows in `benches/encoding_compute.rs`
  (trace-event style bench rows), not `AtomicUsize`
  counters. No new metric labels or operator-visible
  counters were added.
- **P8 free-text / log-line fixture:** pinned to
  `fsst_log_line_eq_5p` in `benches/encoding_compute.rs`.
  The fixture is included in the FSST predicate matrix,
  row-safe FSST predicate matrix, and row-safe writer
  matrix.

## Current verification

- `cargo check -q`
- `cargo test -q`
- `cargo test -q --test encoding_select`
- `cargo test -q --test encoding_cascade`
- `cargo test -q --test encoding_compute_test`
- `cargo test -q --test encoding_compute_kernel_invariants`
- `cargo test -q --test encoding_directory`
- `cargo test -q --test encoding_neutral`
- `cargo test -q --test encoding_secondary_index`
- `cargo test -q --test architecture_test`
- `cargo test -q estimator::tests`
- `cargo test -q deterministic_spread_sample_shape_bounds_prefix_projection_error`
- `cargo test -q fsst_symbol_training_cost_probe -- --ignored --nocapture`
- `cargo test -q fsst_estimator_route`
- `cargo test -q fsst_estimator_decline_route_emits_exact_fallback_bytes`
- `cargo test -q fsst_estimator_route_emits_exact_bytes_on_warehouse_winner_matrix_8k`
- `cargo test -q fsst_estimator_route_emits_exact_bytes_on_warehouse_winner_matrix_all_tiers -- --ignored --nocapture`
- `cargo test -q fsst_estimator_route_emits_byte_identical_predicate_masks_on_warehouse_winner_matrix_8k`
- `cargo test -q fsst_estimator_route_emits_byte_identical_predicate_masks_on_warehouse_winner_matrix_all_tiers -- --ignored --nocapture`
- `cargo test -q fsst_estimator_internal_routings_are_cross_process_deterministic`
- `cargo test -q fsst_estimator_warehouse_winner_matrix_is_cross_process_deterministic -- --ignored --nocapture`
- `cargo test -q test_adr068_fsst_row_safe_estimator_guards`
- `cargo test -q fsst_estimator_route_matches_exact_route_on_row_safe_winner`
- `cargo test -q fsst_estimator_metadata_decline_matches_exact_fallback`
- `cargo test -q cached_tables_skip_sample_when_metadata_pressure_is_low`
- `cargo test -q cached_tables_do_not_decline_from_cross_buffer_sample`
- `cargo test -q fsst_writer_context_warms_column_cache_from_exact_route`
- `cargo bench --bench encoding_compute --no-run`
- `cargo bench --bench encoding_compute -- --quick --writer-column-cache-only`
- `cargo bench --bench encoding_compute -- --writer-column-cache-only --writer-column-cache-rows=8192,65536`
- `cargo bench --bench encoding_compute -- --writer-column-cache-only --writer-column-cache-rows=1048576`
- `cargo bench --bench encoding_compute -- --writer-column-cache-only --writer-column-cache-rows=65536 --writer-column-cache-fixture=fsst_short_string_diag`
- `cargo bench --bench encoding_compute -- --writer-column-cache-only --writer-column-cache-rows=1048576 --writer-column-cache-fixture=fsst_short_string_diag`
- `cargo bench --bench encoding_compute -- --writer-column-cache-only --writer-column-cache-rows=65536 --writer-column-cache-fixture=fsst_metadata_floor_decline_diag`
- `cargo bench --bench encoding_compute -- --writer-column-cache-only --writer-column-cache-rows=1048576 --writer-column-cache-fixture=fsst_weak_compression_diag`
- 1M writer column-cache row/tier pass recorded in
  `/tmp/adr068_writer_column_cache_1m_promoted.log`
- `cargo bench --bench encoding_compute -- --quick --fsst-writer-only`
- `rustfmt --edition 2024 --check src/storage/encoding/cascade/estimator.rs src/storage/encoding/cascade/codec.rs src/storage/encoding/cascade/mod.rs src/storage/encoding/sample.rs src/storage/encoding/select.rs tests/architecture_test.rs`
- `rustfmt --edition 2024 --check benches/encoding_compute.rs`
- `git diff --check`

## Focused route coverage

- Exact and estimator routings select the same
  `FsstRowSafe` cascade on a repeated-pair winner.
- Exact and estimator routings emit byte-identical
  row-safe FSST payloads on that winner.
- Exact and estimator routings emit byte-identical
  row-safe FSST payloads on the 8K warehouse winner
  matrix: URL equality, URL `InList`, long repeated pair,
  weak-compression diagnostic, and log-line.
- The ignored all-tier matrix repeats that byte-equality and
  zero-false-decline check at 8K, 65K, and 1M rows for the
  same five warehouse winner fixtures. It passed in the
  current source tree and remains ignored to keep default
  test latency bounded.
- Metadata-decline routing selects the same fallback
  `FsstWholeBuffer` cascade as the exact route.
- Metadata-decline routing emits byte-identical
  fallback FSST payloads.
- Same-test-binary child processes produce identical
  digest lines for exact and estimator routings on the
  repeated-pair winner.
- The ignored all-tier determinism probe repeats the
  two-child-process digest check across the 8K / 65K / 1M
  warehouse winner matrix. It passed in the current source
  tree and remains ignored to keep default test latency
  bounded.
- The read-side predicate-mask invariant matrix at 8K rows
  (`fsst_estimator_route_emits_byte_identical_predicate_masks_on_warehouse_winner_matrix_8k`)
  encodes each of the five warehouse-winner fixtures through
  both `FsstRowSafeEstimatorRouting::ExactOnly` and
  `FsstRowSafeEstimatorRouting::Estimator`, then evaluates a
  fixture-specific `Eq` literal against the emitted
  `FsstRowSafe` payload via `evaluate_fsst_row_safe_predicate`
  (Strategy B). Masks are asserted byte-identical across
  routings. The ignored all-tier variant repeats the matrix
  at 8K / 65K / 1M rows and stays gated on `--ignored` to keep
  default test latency bounded.
- The writer column-cache external bench smoke run
  (`cargo bench --bench encoding_compute -- --quick --writer-column-cache-only`)
  emits `fsst_row_safe_writer_column_cache_matrix` rows at 8K rows for
  row-group-count tiers 1 / 2 / 4 / 8 / 16. On observed
  value buffers, public writer output matched the exact FSST
  oracle by cascade and encoded byte length, with
  `false_declines=0`. The fixed quick artifact
  `/tmp/adr068_writer_column_cache_quick_cached_decline_fixed.log`
  contains 30 rows, 120 observed value buffers, 24
  estimated cache misses, 96 estimated cache hits, 0
  false declines, 120 / 120 cascade matches, 120 / 120
  encoded-byte-length matches, 89 exact row-safe winners,
  and 89 actual row-safe selections. Short-string decline
  rows averaged 1.628x exact-vs-writer with a max of
  1.759x; one quick tier measured 1.495x and is covered
  by the full 8K/65K promotion matrix below.
- The fixed writer column-cache promotion-scale matrix for
  8K and 65K rows is recorded in
  `/tmp/adr068_writer_column_cache_8k_65k_cached_decline_fixed.log`.
  Summary: 60 rows, six fixtures, requested row-group
  tiers 1 / 2 / 4 / 8 / 16 at 8K and 65K rows, 232
  observed encoded value buffers, 47 estimated cache
  misses, 185 estimated cache hits, 170 exact row-safe
  winners, 170 actual row-safe selections,
  `false_declines=0`, 232 / 232 cascade matches, 232 /
  232 encoded-byte-length matches, and zero bad rows.
  Short-string decline rows averaged 1.609x exact-vs-
  writer with min 1.507x and max 1.766x, clearing the
  >= 1.5x hard gate across all 8K/65K row-group tiers.
  Plausible-winner rows averaged 1.005x writer-vs-exact
  with max 1.032x, clearing the <= 5% hard gate.
  Rows that report `route=no_encoding` have no emitted
  encoded value buffer to compare; they are retained in
  the matrix as opportunity-loss diagnostics.
- The 1M short-string decline artifact
  `/tmp/adr068_writer_column_cache_short_string_1m_onepass.log`
  records corrected cache estimates: each requested tier
  resolves to 16 legal 65,536-row shards, so the run has
  16 cache misses and 0 cache hits. The cost-free row-safe
  floor is independent of cache warmth and clears the
  decline gate at 1M anyway: exact-vs-writer ranges from
  1.561x to 1.694x with `false_declines=0`, 16 / 16
  cascade matches, and 16 / 16 encoded-byte-length matches
  on every row.
- A pre-fix full 1M all-fixture run exposed a real
  correctness bug: cross-buffer cached-table
  table-dependent declines produced one false decline per
  `fsst_weak_compression_diag` row. That log is deliberately
  not promotion evidence. The fix disables table-dependent
  declines from cross-buffer cached tables; warm cached
  outcomes are cost-free-floor-only and otherwise fall back
  to current-buffer exact selection. The fixed targeted
  artifact
  `/tmp/adr068_writer_column_cache_weak_1m_cached_decline_fixed.log`
  records the same 1M weak-compression fixture across
  row-group tiers 1 / 2 / 4 / 8 / 16 with 25 / 25 exact
  row-safe winners selected on every row, `false_declines=0`,
  25 / 25 cascade matches, 25 / 25 encoded-byte-length
  matches, and writer-vs-exact max 1.005x.
- The quick writer-only timing row records the cold
  no-context path separately. It is not the writer-column-cache promotion
  path; it is retained to prevent accidentally enabling
  the estimator for standalone single-buffer selection
  without separate overhead evidence.
- Writer-column-cache bench rows now include exact-route
  timing from the doc-hidden
  `build_encoding_directory_for_ipc_stream_exact_fsst_for_bench`
  helper: `exact_writer_ms`, `writer_vs_exact_cpu_micros`,
  and `exact_vs_writer_cpu_micros`. The latest quick
  performance artifact
  `/tmp/adr068_writer_column_cache_quick_cached_decline_fixed.log`
  and the promotion-scale matrix above record
  `exact_writer_ms`, `writer_vs_exact_cpu_micros`, and
  `exact_vs_writer_cpu_micros` on every row.

## Iter-10 Writer Column-Cache Pivot

Writer column-cache implementation is now the directory-writer promotion
target:

- Cost-free row-safe floor decline remains per buffer. It
  includes both the metadata-only floor and the exact
  metadata-plus-non-empty-row payload lower bound.
- `WriterCascadeContext` is crate-private and scoped to
  one `build_encoding_directory_for_ipc_stream` call.
- `ColumnFsstSymbolTableCache` is keyed by
  `IpcColumnOrdinal`, not `(row_group, column)`.
- Cold misses run current-buffer exact selection and
  warm/refresh the cache from already-trained exact
  tables.
- Warm cache hits are cost-free-floor-only. Cross-buffer
  cached tables do not produce trained-floor or sample-
  confidence declines after the 1M weak-compression false-
  decline finding.
- Warm `RequireExactGuard` falls back to current-buffer
  exact selection; cached tables never populate current-
  buffer exact lengths.
- The production selector engages the estimator only
  when the writer supplies both `WriterCascadeContext`
  and `IpcColumnOrdinal`; no-context single-buffer
  selection remains exact because quick writer-only
  timing showed cold estimator overhead on plausible
  winners.

## Promotion gates

The Phase 7 promotion gates are met for the ADR 068
promotion target: the directory-writer column-cache path
(`directory.rs:67`, routed through
`FsstRowSafeEstimatorRouting::Estimator` whenever both
`WriterCascadeContext` and `IpcColumnOrdinal` are present).
Cold no-context single-buffer selection remains on
`FsstRowSafeEstimatorRouting::ExactOnly` — this is the
deliberate ADR 068 scope per §"Status", not a gate failure;
quick writer-only timing showed cold estimator overhead on
plausible winners, so promoting the cold path requires a
separate evidence pass. All seven hard gates are met (1 by
design; 2, 3, 4, 5, 6 by the fixed writer-column-cache
correctness and writer-performance matrices and the
cross-process determinism probes; 7 by the read-side
predicate-mask invariant matrix above). The soft mixed-
workload gate (8) is discharged by recorded justification
below.

Gate status:

- Decline-path writer speedup >= 1.5x: **met** by the
  fixed 8K/65K promotion matrix and the 1M short-string
  targeted matrix.
- Plausible-winner overhead <= 5%: **met** by the fixed
  8K/65K promotion matrix and the fixed 1M weak-
  compression targeted rerun.
- Read-side predicate-mask invariant rows: **met** by
  `fsst_estimator_route_emits_byte_identical_predicate_masks_on_warehouse_winner_matrix_8k`
  (and the ignored all-tier variant) which asserts
  byte-identical Strategy B masks between the
  `ExactOnly` and `Estimator` routings across the five
  warehouse-winner fixtures. Read wall-clock is not
  regressed because gate 2 already proved encoded bytes
  are byte-identical and the read evaluator is
  deterministic given identical inputs.
- Mixed-workload writer delta closure or explicit
  promotion justification: **explicitly justified**. The
  conservative V1 baseline only skips exact encodes on
  obvious losers (cost-free floor declines). While this
  satisfies the >= 1.5x speedup gate on adversarial data
  with <= 5% overhead on winners, the net closure of the
  ~140% overhead from ADR 067 on a mixed workload is
  mathematically limited because plausible-winner buffers
  still pay the dual-encode cost. Council agrees this
  partial delta closure is sufficient for V1 promotion,
  as it completely eliminates worst-case performance cliffs
  without risking false declines. The remaining overhead
  will be addressed by the option-2 escalation ADR
  (estimator-authorized emission without exact guard)
  tracked as the V2 follow-up.
