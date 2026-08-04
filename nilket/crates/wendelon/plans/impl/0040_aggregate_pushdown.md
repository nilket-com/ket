# Implementation plan: ADR 040 — Aggregate Pushdown Protocol

Companion to `plans/adr/0040_aggregate_pushdown.md`.

## Goal

Implement ADR 040 as the aggregate pushdown protocol — sibling to
the predicate bytecode. Concretely:

- Accept bounded aggregate plans from Toron via a new wire frame.
- Use the predicate bytecode only as an optional row-admission
  filter.
- Reuse ADR 038 range narrowing and ADR 039 stats pruning during
  execution.
- Coordinator-node fan-out: merge peer partials, return one
  envelope to Toron.
- Add the `tombstone_metadata` field to TieredSlab pointers so
  CSB short-circuit is sound under MVCC.
- Advertise the path through
  `NodeCapability::PartialAggregatePushdown` (bit 1; ADR 039's
  `SlabStatsPruning` is bit 0).

## Non-goals (V1)

- No grouped aggregation.
- No exact `COUNT(DISTINCT ...)`.
- No float, decimal-SUM, decimal-AVG, or nested-type aggregate
  inputs. (Decimal MIN/MAX is allowed.)
- No CSB-derived aggregate short-circuit *without* the
  visibility-completeness proof (the proof is part of V1).
- No String/Binary `quantile_kll` (deferred).
- No SQL/Rune parsing inside Wendelon.

## Phase 0 — Contract freeze + visibility-flag prep

Lock everything that goes on the wire and extend ADR 039's
TieredSlab pointer with the dependency this ADR needs.

Outputs:

- ADR moves from "Ready to ratify" to "Proposed" / "Ratified" in
  `plans/adr/0040_aggregate_pushdown.md`.
- Wire envelope byte layouts (ADR 040 §7–9) frozen.
- V1 aggregate catalog frozen (§3) — 10 distinct tags.
- `AGG_REJECT_*` enumeration frozen (§20).
- KLL serialization byte layout frozen (§9 tag 0x0A).
- HLL serialization matches ADR 039 §14 byte-for-byte.

Visibility-flag prep:

- Add tri-state `tombstone_metadata: TombstoneMetadata` to the
  TieredSlab pointer schema in `src/storage/index.rs`.
- States:
  - `Untracked`: pre-metadata pointer; CSB aggregate direct
    answers are ineligible.
  - `None`: tracked pointer with no committed tombstone.
  - `Earliest(HybridTimestamp)`: tracked pointer with the
    earliest committed tombstone HLC.
- Default `None` for freshly sealed slabs; default decode for
  absent serialized metadata is `Untracked`.
- Tombstone-write hook: when a tombstone is committed against a
  row in slab S at HLC T, update `S.tombstone_metadata` to retain
  the *earliest* tombstone timestamp. `Untracked` remains
  untracked until compaction/rewrite produces a tracked pointer.

Decisions to make at Phase 0:

- KLL library choice: vendored crate vs. hand-rolled. Lean
  vendored if a maintained crate exposes the level array
  directly without forcing a wrapper for byte-format pinning.
- Wire envelope encoding: `postcard` for the envelope (passes
  embedded predicate bytes opaquely) + handcrafted big-endian
  for per-aggregate `state` (golden tests need byte-layout
  determinism).
- Bench gate thresholds (fixture-anchored).

## Phase 1 — Protocol and schema types

Likely files:

- `src/schema/aggregate.rs` (new module).
- `plans/schema/aggregate_ast.md` (new doc, parallel to
  `compute_ast.md`).
- `src/schema/mod.rs`.
- `src/schema/units.rs`.
- `src/schema/capabilities.rs` (or wherever `NodeCapability`
  lives).
- `src/network/transport.rs`.
- `src/storage/index.rs` (TieredSlab pointer extension from
  Phase 0).
- `tests/capability_negotiation.rs`.
- `tests/support/capability_harness.rs`.

Work:

- Add `AggregateRequest` / `AggregateResponse` envelope types
  per ADR 040 §7–8 with a real `format_version: u16` field.
- Add `AggregateExpression`, `AggregatePartial`,
  `AggregateState` enums per §3 / §9 with explicit
  signed/unsigned variants for sum and average-parts.
- Add newtypes per `plans/conventions/newtype-units.md`:
  `AggregateRequestId`, `AggregateExpressionIndex`, `KllK`,
  `LocationCount`, `RowCount`, `DurationMillis`, `ByteCount`,
  `PartialStateLength`, `OverflowFlag`. Reuse `HllPrecision`,
  `TenantId`, `TableObjectId`, `SchemaFingerprint`, `Locality`,
  `HybridTimestamp`.
- Add `NodeCapability::PartialAggregatePushdown = 1`.
- Add stable `AGG_REJECT_*` discriminants per ADR 040 §20.
- Add `PredicateBytecode` alias for the `PbcProgram` type
  (per ADR 041's protocol-naming).
- Extend topology snapshot serialization tests for the new bit.
- TieredSlab pointer with `tombstone_metadata = Earliest(t)`
  round-trips through Raft state and snapshot.

Tests:

- Round-trip for request / response metadata.
- Reject-code discriminants are stable (numeric pinning).
- Capability bit round-trips through cluster status.
- Unknown capability bits remain preserved by
  `NodeCapabilitySet`.
- Aggregate request with > 64 expressions rejects with
  `AGG_REJECT_TOO_MANY_AGGREGATES`.

Exit:

- Aggregate protocol types compile, validate, and round-trip.
- `tombstone_metadata` is plumbed into the TieredSlab schema.
- No executor behavior exists yet.

## Phase 2 — Accumulator and sketch library

Likely files:

- `src/storage/aggregate.rs` (new, parallel to `stats.rs`).
- `src/storage/sketch.rs` (new, factored shared HLL/KLL home).
- Optional helper extraction from `src/storage/stats.rs`.

Work:

- Implement exact accumulator structs and merges:
  - `count_star`, `count_non_null` (both u64).
  - `signed_sum` (i128 with overflow flag).
  - `unsigned_sum` (u128 with overflow flag).
  - `signed_avg` (i128 sum, u64 count, overflow flag).
  - `unsigned_avg` (u128 sum, u64 count, overflow flag).
  - `min`, `max` over §15 canonical bytes (truncation-aware for
    Utf8/Binary).
- Factor ADR 039 HLL into a reusable `HllRegisters` type in
  `sketch.rs`; CSB and aggregate-protocol HLL are distinct
  newtype wrappers over the same inner type. Default precision
  p = 12, range 10..=14 (matches ADR 039).
- Implement KLL sketch state and deterministic compaction.
  Compaction seed = `BLAKE3(tenant_id || table_object_id ||
  request_id)[0..4]`. Default `k = 200`, range 64..=2048. KLL
  inputs restricted to `Int{8..64}`, `UInt{8..64}`, `Timestamp`,
  `Date32`, `Date64`.
- Implement merge for every accumulator. Merge associativity and
  commutativity tests live with the type.
- Add expression validation against descriptor at the named
  `schema_generation`.

Tests:

- Null handling: `count_non_null`, `*_sum`, `*_avg`, `min`,
  `max` all ignore nulls; empty-input returns identity / `None`
  sentinel.
- Signed widening: `i32::MAX × 1B rows` accumulates cleanly into
  `i128`.
- Unsigned widening: same shape with `u32::MAX` and `u128`.
- Overflow detection: overflow flag set; merge of overflow-
  flagged partial poisons the merged result with
  `AGG_NUMERIC_OVERFLOW`.
- Merge associativity for every aggregate via property tests.
- HLL: estimate within standard error on cardinalities 1, 10,
  100, 1k, 100k, 1M.
- KLL: rank-error within bound on synthetic distributions
  (uniform, Zipf, bimodal). With deterministic compaction seed,
  merge is bit-exact across permutations.
- KLL parameter `k` outside 64..=2048 rejects with
  `AGG_REJECT_SKETCH_PARAMETER_INVALID`.
- KLL on a Utf8 column rejects with
  `AGG_REJECT_TYPE_UNSUPPORTED`.
- Property tests vs. Arrow kernel oracles for supported types.
- Scalar-reference oracle tests mirroring ADR 035's
  predicate-bytecode parity model.

Exit:

- Local in-memory `RecordBatch` can be aggregated without
  touching networking, slabs, or the predicate executor.

## Phase 3 — Local single-node aggregate execution

Likely files:

- `src/network/data_server.rs`.
- `src/storage/manager.rs`.
- `src/storage/aggregate.rs`.
- `tests/aggregate_pushdown.rs` (new integration test).

Work:

- Add a local execution function parallel to the existing
  table-query planning path.
- Reuse admission checks from table queries: epoch staleness,
  linearizable `E_safe`, retention floor, schema generation,
  descriptor lookup.
- Compile optional predicate program once per request via the
  ADR 035 vectorized executor.
- Reuse `locality_ranges` decomposition from ADR 038.
- Reuse ADR 039 stats pruning during slab admission — aggregate
  scans inherit the existing `pbc_slab_stats_pruned_total`
  counter naturally because the predicate filter drives the
  existing pruning path.
- For each admitted batch: predicate filter → fold survivors
  into accumulators → discard rows. Single pass.
- Track `max_hlc`, `rows_scanned`, `rows_admitted`,
  `slabs_consulted`, `slabs_pruned_by_csb` in
  `AggregateDiagnostics`.

Tests:

- `count_star` over a typed table equals row-query count.
- `count_non_null` ignores nulls.
- `signed_sum` / `unsigned_sum` / `signed_avg` / `unsigned_avg`
  match a row-query Toron oracle.
- `min` / `max` over signed int, unsigned int, timestamp, date,
  string, binary, Boolean columns match row-query oracle.
- A predicate that ADR 039 can disprove prunes the tiered slab
  before row materialization (verify via stats counter).
- Unsupported aggregate type returns
  `AGG_REJECT_TYPE_UNSUPPORTED`, no panic.
- Invalid embedded predicate program returns
  `AGG_REJECT_PROGRAM_INVALID`.
- Empty result set: `min`/`max`/`*_sum` return identity
  sentinels (`state_length = 0` per §9); `*_avg` returns
  sum=0/count=0; `approx_distinct` estimates 0.
- Approx aggregate in exact-only session returns
  `AGG_REJECT_APPROX_NOT_ALLOWED`.

Exit:

- A single node executes aggregate plans locally and returns a
  merged response envelope.

## Phase 4 — RPC and coordinator merge

Likely files:

- `src/network/data_server.rs`.
- `src/network/data_client.rs`.
- `src/network/core.rs`.
- `src/schema/core.rs`.
- `tests/control_ring_rpc.rs` if message ids project there.
- `tests/aggregate_pushdown.rs`.

Work:

- Add aggregate request / response message types to the data
  path.
- Coordinator fans out subrequests over selected
  `locality_ranges`.
- Before sending a subrequest to a peer, coordinator checks
  peer's advertised `NodeCapabilitySet` for
  `PartialAggregatePushdown`.
- Planning-time fallback: if any peer lacks the capability, the
  coordinator returns `AGG_REJECT_CAPABILITY_UNAVAILABLE` to
  Toron without dispatching. The coordinator does not silently
  materialize rows.
- Coordinator merges peer partials by `expression_index`.
- Cross-bind validation before merge: every peer partial must
  carry matching `request_id`, `table_object_id`,
  `schema_generation`, `schema_fingerprint`, aggregate-
  expression digest, and `as_of_hlc`. Mismatch →
  `AGG_REJECT_PARTIAL_CROSSBIND_FAILED`.
- Carry `max_hlc` ceiling across merged peers.

Tests:

- Two-node aggregate query returns the same result as a
  row-query oracle.
- Mixed-capability cluster: coordinator with capability talks to
  peer without; coordinator returns
  `AGG_REJECT_CAPABILITY_UNAVAILABLE` and increments
  `agg_skipped_no_capability_total`.
- Mismatched `schema_fingerprint` in a peer partial rejects via
  cross-bind.
- Mismatched aggregate-expression digest rejects.
- Remote timeout / budget exhaustion respects existing query
  error style.

Exit:

- Aggregate pushdown works across data nodes with mixed-cluster
  coverage.

## Phase 5 — CSB short-circuit (with visibility proof)

Likely files:

- `src/network/data_server.rs`.
- `src/storage/manager.rs`.
- `src/storage/stats.rs`.
- `src/storage/aggregate.rs`.
- `tests/aggregate_pushdown.rs`.

Work:

- Add a predicate-classification helper that classifies the
  embedded predicate against a slab CSB as: disjoint (already
  pruned), non-restrictive (every row satisfies), inconclusive
  (must scan).
- Add a `slab_visibility_complete(slab, as_of_hlc)` helper:
  returns true iff `slab.seal_hlc ≤ as_of_hlc` AND
  `slab.tombstone_metadata` is tracked and either `None` or
  `Earliest(t) where t > as_of_hlc`.
- For `count_star`, `count_non_null`, `min`, `max`: when the
  aggregate is in this set AND the predicate is non-restrictive
  (or absent) AND visibility is complete, answer from CSB
  without decrypting chunks.
- Increment `agg_csb_short_circuit_total` for each slab answered
  from CSB; carry the count in
  `AggregateDiagnostics.slabs_answered_from_csb`.
- HLL CSB and KLL never short-circuit (per ADR 040 §5).

Tests:

- No-predicate `count_star` over an append-only slab (visibility
  complete) short-circuits and returns the row count from CSB
  without chunk reads.
- Same query over a slab with `Earliest(t) where t <= as_of_hlc`
  does *not* short-circuit; falls through to scan.
- Non-restrictive predicate over a visibility-complete slab
  short-circuits.
- Overlapping predicate scans rows.
- Disjoint predicate is handled by ADR 039 pruning before
  short-circuit logic fires.
- CSB-derived answer matches the row-scan oracle for every
  eligible aggregate.
- Concurrent tombstone writes correctly update
  `tombstone_metadata` (CAS test).
- A slab with `tombstone_metadata = Untracked` newly migrated
  from pre-040 state: short-circuit conservatively disabled until
  a fresh sealed slab carries the field.

Exit:

- The first chunk-decryption-free aggregate path is live, behind
  explicit proof checks. `MIN(event_time)` over a cold
  append-only table reads slab tails only.

## Phase 6 — Sketch aggregates wired end-to-end

Likely files:

- `src/storage/sketch.rs`.
- `src/storage/aggregate.rs`.
- `tests/aggregate_pushdown.rs`.
- `tests/property_suite.rs`.

Work:

- Wire `approx_distinct` (HLL) through validation, execution,
  merge, and response serialization.
- Wire `quantile_kll` through validation, execution, merge, and
  response serialization.
- Per-request `max_partial_state_bytes` budget enforcement;
  exceeding budget at execution time rejects with
  `AGG_REJECT_PARTIAL_STATE_TOO_LARGE`.

Tests:

- HLL estimates within standard error on deterministic fixtures.
- HLL merge equivalent to single-pass HLL over union of inputs.
- KLL rank error within bound for q ∈ {0.5, 0.9, 0.99, 0.999}
  on fixtures of size 1k / 1M / 100M.
- KLL deterministic merge across permutations (bit-exact under
  the seeded compaction).
- Sketch byte budget rejection returns stable code.

Exit:

- Approximate aggregates work end-to-end. `PushdownApprox`
  becomes load-bearing.

## Phase 7 — Metrics and documentation

Likely files:

- `src/metrics/registry.rs`.
- `src/network/data_server.rs`.
- `src/storage/aggregate.rs`.
- `plans/schema/aggregate_ast.md`.
- `plans/adr/0040_aggregate_pushdown.md` (final ADR home).
- `plans/wip/roadmap-post-038.md` (status table update).

Metrics work:

- Add per ADR 040 §25:

  ```
  counter   agg_request_total
  counter   agg_request_rejected_total{reason}
  counter   agg_rows_scanned_total
  counter   agg_rows_admitted_total
  counter   agg_rows_filtered_out_total
  counter   agg_partials_merged_total
  counter   agg_csb_short_circuit_total
  counter   agg_csb_short_circuit_skipped_tombstone_metadata_untracked_total
  counter   agg_int_overflow_total
  counter   agg_skipped_no_capability_total
  histogram agg_partial_bytes
  histogram agg_exec_micros
  histogram agg_merge_micros
  ```

- The `agg_skipped_no_capability_total` counter supersedes ADR
  039's forward-reference placeholder
  `pbc_partial_aggregate_skipped_no_capability_total`. Remove
  the placeholder registration.
- Carry internal diagnostics in the response envelope per ADR
  040 §8 (including `slabs_answered_from_csb`).
- Keep ADR 039 stats counters unchanged and reused.

Documentation work:

- Write `aggregate_ast.md` with: aggregate catalog (10 tags),
  wire envelope byte layouts, numerical-semantics commitments
  (ADR 040 §10–15), merge function definitions, KLL `k` spec,
  HLL precision spec, `AGG_REJECT_*` enumeration.
- Cross-link from ADR 036 (capability surface), ADR 039 (CSB
  pruning + `tombstone_metadata` field), and roadmap.
- Pin a Rust reference implementation of every merge function in
  the `wendelon-client` crate so Toron's planner integration
  tests can pull it in.

Tests:

- Prometheus render includes new metrics.
- Each metric increments at exactly one observable site.
- Numeric overflow path increments overflow counter and sets
  response status.
- Mixed-capability path increments skipped-no-capability
  counter.
- CSB short-circuit increments its counter.

Exit:

- Operators can distinguish rows scanned, rows admitted, partial
  merge cost, CSB short-circuit value, capability fallback, and
  numeric overflow.
- `aggregate_ast.md` is sufficient for a future Toron planner
  integrator to implement the client side.

## Phase 8 — Benchmarks, soak, capability enablement

Likely files:

- `benches/aggregate_pushdown.rs` (new).
- `examples/soak_driver.rs`.
- `tests/simulator_ladder.rs` if appropriate.
- `src/node.rs`.
- `src/network/factory.rs`.

Bench work:

- Row-query `count(*)` vs. aggregate `count_star` →
  network-bytes ratio.
- Aggregate path with and without ADR 039 stats pruning.
- Aggregate path with and without CSB short-circuit
  (no-predicate vs. predicate that the helper classifies
  non-restrictive vs. inconclusive).
- HLL state size and merge cost.
- KLL state size and merge cost; rank-error vs. `k`.
- Coordinator fan-out under varying peer count.

Bench gates (pinned at Phase 0, verified here):

- `count_star_no_predicate` over 1B rows: ≥ 100× network-bytes
  reduction vs. row-shipping baseline.
- `count_star_csb_short_circuit` over a cold tiered slab:
  materially faster than the full-scan path.
- `signed_sum` over predicate-pruned slabs: end-to-end latency
  within 2× of predicate-only filter pass on the same fixture.
- KLL rank error ≤ 1.5% at q = 0.99 with default k = 200 on the
  uniform fixture.
- HLL estimate error ≤ 1.5% at p = 12.

Capability enablement work:

- Add advertised capability wiring in `node.rs` /
  `network/factory.rs`.
- Gate advertisement on implementation readiness; default-on
  once bench gates pass.
- Add release-note style note in the final ADR.

Exit:

- `PartialAggregatePushdown` is safe to advertise in normal test
  clusters.
- Performance and state-size data are recorded for post-040
  watch-list comparison.

## Coordination with ADR 041 (project-name decoupling)

ADR 040 and ADR 041 ratify in the same merge per
`plans/wip/merge_plan_040.md`. AGG uses brand-clean naming
(`AGG`, `agg_*`, `AGG_REJECT_*`); references to the predicate
protocol use the post-rename `PBC` vocabulary. The
`PredicateBytecode` alias in Phase 1 points at `PbcProgram` for
consistency with ADR 041's renames in `src/schema/pbc.rs`.

## Suggested review order

1. Protocol / schema types + `tombstone_metadata` field
   (Phase 0–1).
2. Accumulator and sketch library with oracle tests (Phase 2).
3. Local single-node execution (Phase 3).
4. Coordinator fan-out and cross-bind merge (Phase 4).
5. CSB short-circuit (Phase 5).
6. Sketches wired end-to-end (Phase 6).
7. Metrics, docs, benches, capability advertisement
   (Phases 7–8).

The accumulator library should land before any RPC work. It
gives the council a small, testable unit where the hard
numerical semantics (overflow, KLL determinism, HLL register
merge) are visible without networking noise.

## Post-040 watch list

Mirrors ADR 039's post-merge watch. Once 040 ships:

- `agg_request_total` vs. row-shipping baseline for the same
  query class → leverage measurement.
- `agg_csb_short_circuit_total` / `agg_request_total` ratio →
  CSB short-circuit value in real workloads. Per-tenant variance
  reveals which tenants benefit (append-only) and which don't
  (high tombstone churn).
- `agg_csb_short_circuit_skipped_tombstone_metadata_untracked_total`
  → pre-tri-state pointer backlog; should decay as compaction or
  rewrite refreshes old slabs.
- `agg_skipped_no_capability_total` → rolling-upgrade
  visibility.
- `agg_int_overflow_total` → workload-shape signal; zero is the
  goal.
- `agg_partial_bytes` distribution → KLL sizing pressure;
  informs whether default `k = 200` holds.
- `agg_merge_micros` → coordinator fan-out cost under peer-count
  variation, plus CSB tombstone safety-net scan amplification.

## Open implementation questions

1. **Module location.** `src/schema/aggregate.rs` parallels
   `src/schema/pbc.rs`. The executor side likely belongs under
   `src/storage/aggregate.rs`. Decide at Phase 0 / Phase 1
   boundary.

2. **KLL library choice.** Vendored crate vs. hand-rolled.
   Decide at Phase 2 start. Lean vendored if a maintained crate
   exposes the level array directly without forcing a wrapper
   for byte-format pinning.

3. **HLL deduplication between `stats.rs` and `aggregate.rs`.**
   Most likely outcome: shared inner `HllRegisters` type in
   `src/storage/sketch.rs`, two newtype wrappers on top.

4. **Wire envelope encoding strategy.** `postcard` for the
   request envelope (passes embedded predicate bytes opaquely)
   + handcrafted big-endian for per-aggregate `state` (golden
   tests need byte-layout determinism).

5. **`tombstone_metadata` field migration story.** Newly sealed
   slabs populate `TombstoneMetadata::None`. Pre-040 sealed slabs
   decode as `Untracked`; CSB short-circuit is conservatively
   disabled for them. Natural compaction gradually populates the
   field.

6. **Tombstone-write atomicity for `tombstone_metadata`.** The
   shipped migration tombstone path records
   `MigrationComplete.completed_at_hlc` onto affected tiered
   pointers through the state-machine apply observer. The CSB
   direct-answer path also consults committed migration
   tombstones before trusting pointer metadata, so the
   apply-to-pointer window fails closed.

7. **Integration with ADR 037's `TableSpreadProjection`.** Verify
   the existing arc-fan-out code composes with aggregate request
   distribution; needs an explicit two-arc test.

8. **Bench gates threshold at Phase 0.** Final values need a
   fixture-anchored measurement during Phase 0; pin then.
