# ADR 040: Aggregate Pushdown Protocol

## Status

Accepted

## Context

ADRs 035–039 closed the per-row pushdown story. The next leverage
tier is **collapse the bytes that have to leave storage at all**.
A `SELECT COUNT(*)` against a billion-row table should return one
merged number per query; a `SELECT approx_quantile(latency,
0.99)` should return a small sketch.

ADR 036 §10 reserved this design space as a sibling protocol —
not a predicate-bytecode opcode extension. Stack discipline
(accumulators outlive batches) and numerical-semantics surface
(overflow, sketch error, merge associativity, cross-binding)
both expand the predicate protocol's contract surface unboundedly
if mixed in.

This ADR defines the sibling protocol. Following the convention
`plans/conventions/no-project-name-in-protocols.md`, the protocol
uses functional naming throughout: acronym `AGG`, frame types
`AggregateRequest` / `AggregateResponse`, reject prefix
`AGG_REJECT_*`, metric prefix `agg_*`, module path
`src/schema/aggregate.rs`. The capability variant
`NodeCapability::PartialAggregatePushdown` is brand-clean.
References to the predicate bytecode use `PBC` per ADR 041's
project-name decoupling.

## Decision

Wendelon adds the **aggregate pushdown protocol** (`AGG`) as a
sibling protocol to the predicate bytecode (PBC). It shares the
Toron–Wendelon query session, transport, capability negotiation,
deadlines, materialization budgets, audit, and observability
machinery. It carries a distinct request payload, validator,
executor, and partial-result wire envelope.

V1 covers **ungrouped (global) aggregates over predicate-filtered
Arrow batches**, with deterministic integer arithmetic and KLL
quantile sketches. The Toron-visible surface is a single ADR 036
capability bit (`PartialAggregatePushdown`) plus stable explain
counters. The node Toron contacts is the *aggregate coordinator*:
it fans out to peer storage nodes, merges partials, and returns
one merged envelope.

CSB drives both predicate pruning during slab admission AND a
narrow direct-answer short-circuit for `count_star`,
`count_non_null`, `min`, and `max` under a strict
visibility-and-nonrestrictive-predicate proof condition (§5).
The proof depends on a small ADR 039 TieredSlab pointer
extension amended by this ADR:
`first_tombstone_hlc: Option<HybridTimestamp>` (§"ADR 039
amendment").

### Scope and shape

1. **Sibling protocol, not predicate-bytecode extension.** AGG
   requests travel in distinct frame types alongside PBC
   requests. The two protocols share wire transport, security
   envelope, and capability handshake; they do **not** share a
   stack or instruction set.

2. **Ungrouped only in V1.** A request returns one partial per
   declared aggregate per storage node, not a per-group hash
   table. `GROUP BY` is the obvious follow-up; this ADR
   deliberately does not pre-allocate spec slots for it.

3. **V1 aggregate catalog.**

   ```
   tag   name              input types                        partial state
   ----  ----------------  --------------------------------  ----------------------
   0x01  count_star        (none)                            u64
   0x02  count_non_null    any descriptor column             u64
   0x03  signed_sum        Int{8,16,32,64}                   i128 + overflow_flag
   0x04  unsigned_sum      UInt{8,16,32,64}                  u128 + overflow_flag
   0x05  min               §15-allowlisted column            canonical bytes + flag
   0x06  max               §15-allowlisted column            canonical bytes + flag
   0x07  signed_avg        Int{8,16,32,64}                   i128 sum + u64 count
                                                              + overflow_flag
   0x08  unsigned_avg      UInt{8,16,32,64}                  u128 sum + u64 count
                                                              + overflow_flag
   0x09  approx_distinct   §15-allowlisted column            HLL register array
   0x0A  quantile_kll      Int{8..64} | UInt{8..64} |        KLL sketch state
                            Timestamp | Date32 | Date64
   ```

   "Any descriptor column" for `count_non_null` means the column
   resolves at the pinned schema generation; it does not require
   byte-comparable encoding. `count_non_null` only needs the
   column's validity bitmap or CSB null/non-null counts.

   §15-allowlisted column refers to ADR 039 §15: the canonical
   byte-comparable encoding allowlist (signed/unsigned ints,
   timestamps, dates, Boolean, Utf8/Binary with prefix
   truncation). For `min`/`max` and `approx_distinct` the
   allowlist applies as-is. For `quantile_kll` the type set is
   narrowed to numeric and time domains: ADR 039's
   prefix-truncation for Utf8/Binary makes "rank within prefix-
   truncated space" a different semantic than "rank within full
   strings"; defer.

   Boolean is in `min`/`max`'s allowlist (encoded as 0x00/0x01;
   `false < true`).

   The byte-tag table above is the wire-format authority. As an
   illustration for Rust implementers, the catalog corresponds to
   the following enum shape:

   ```rust
   enum AggregateFunction {
       CountAll,
       CountNonNull { column: ColumnOrdinal },
       SignedSum { column: ColumnOrdinal },
       UnsignedSum { column: ColumnOrdinal },
       SignedAverageParts { column: ColumnOrdinal },
       UnsignedAverageParts { column: ColumnOrdinal },
       Min { column: ColumnOrdinal },
       Max { column: ColumnOrdinal },
       ApproxDistinct { column: ColumnOrdinal, precision: HllPrecision },
       Quantile { column: ColumnOrdinal, sketch: QuantileSketch },
   }

   enum QuantileSketch {
       Kll { k: KllCompactionK },
   }
   ```

   The byte tags pin wire-format identity; the enum is the
   in-memory representation. The two are kept 1:1 by validation
   tests; neither is the master.

   **Explicitly not in V1**:
   - Float aggregates (`Float32`, `Float64`).
   - Decimal aggregates, including Decimal MIN/MAX. ADR 039 §15
     deliberately gives Decimal opaque CSB entries and does not
     pin a byte-comparable Decimal encoding in V1.
   - Exact `COUNT(DISTINCT col)`.
   - String/Binary quantiles.
   - Variance, stddev, correlation, covariance, `percentile_cont`.
   - User-defined aggregate functions.
   - Grouped aggregation (`GROUP BY`).

4. **AGG composes with PBC.** A request carries an *optional*
   PBC predicate program (the row-level filter) and a list of
   aggregate ops. The storage node runs the filter over each
   batch, then folds surviving rows into accumulators. Single
   pass, no intermediate row materialization. A request with no
   filter aggregates over the entire admitted slab set.

5. **CSB-derived short-circuit (V1, with proof condition).** A
   storage node may answer `count_star`, `count_non_null`,
   `min`, or `max` for a slab from the CSB alone — without
   decrypting any chunks — when **all** of the following hold:

   1. The aggregate is in {`count_star`, `count_non_null`,
      `min`, `max`} and its column (where applicable) carries
      the relevant always-on stat in the CSB (ADR 039 §5).
   2. The slab is **visibility-complete at `as_of_hlc`**:
      `slab.seal_hlc ≤ as_of_hlc` AND
      (`slab.first_tombstone_hlc` is `None` OR > `as_of_hlc`).
      The `first_tombstone_hlc` field amends ADR 039's
      TieredSlab pointer; see §"ADR 039 amendment".
   3. The PBC predicate (if any) is **provably non-restrictive
      for the slab**: every row in the slab satisfies the
      predicate. This is *not* ADR 039's pruning proof, which
      answers the opposite question ("no row can match"). ADR
      040 defines a separate full-coverage classifier. The V1
      implementation may start conservatively with only the empty
      predicate classified as full coverage; more predicate
      shapes may be added only with tests proving the "every row
      matches" direction.

   When all three hold, the slab contributes to the partial
   without decryption. Counter `agg_csb_short_circuit_total`
   increments.

   This is the warehouse-class win: a cold-storage
   `MIN(event_time)` query against a billion-row append-only
   table reads one slab tail per slab (16 KiB) and answers the
   aggregate from the CSB locator — no chunk decryption, no row
   materialization. For append-only event tables (logs, audit
   trails, time-series) tombstones never apply, so every sealed
   slab is visibility-complete at any `as_of_hlc ≥ seal_hlc`.

   **Why this is sound under MVCC.** ADR 039 CSB stats are
   slab-write-time. For *predicate pruning* ("no row in this
   slab can match"), they're sound regardless of MVCC because
   tombstoning only ever removes rows. For *aggregate answers*,
   soundness requires that the visible row set at `as_of_hlc`
   equals the slab-write-time row set — i.e., no tombstones have
   hidden any rows. The visibility-complete check enforces
   exactly that.

   **Approx aggregates do not short-circuit.** `approx_distinct`
   never short-circuits from CSB HLL (ADR 039 pinned CSB HLL as
   selectivity-only). `quantile_kll` similarly never short-
   circuits — CSB doesn't carry quantile sketches.

6. **Approximate aggregates are tagged `PushdownApprox` at
   planning time.** ADR 036 §3's `PushdownApprox` targetability
   label, idle since the contract was written, becomes
   load-bearing here. `quantile_kll` and `approx_distinct` carry
   documented error bounds; Toron's planner is responsible for
   either surfacing the approximation (`approx_*` user
   functions) or refusing to push down a query that demands
   exact semantics.

### Wire envelope

7. **Request envelope.** An `AggregateRequest` frame:

   ```
   field                         type                  notes
   ---------------------------   -------------------   ----------------------------
   format_version                u16 BE                currently 1
   request_id                    AggregateRequestId    Toron correlation
   tenant_id                     TenantId              routed; AAD-bound
   table_object_id               TableObjectId         descriptor identity
   schema_generation             u64 BE                pinned per ADR 037
   schema_fingerprint            SchemaFingerprint     pinned per ADR 039 §13
   consistency                   Consistency           linearizable | bounded
   min_hlc                       HybridTimestamp       staleness floor
   as_of_hlc                     HybridTimestamp       read snapshot point
   locality_ranges               Vec<LocalityRange>    Toron hint, may be pre-narrowed
   predicate_program             Option<PbcProgram>    optional row filter
   aggregate_count               u16 BE                bounded ≤ 64
   aggregates                    AggregateExpression[] see below
   budgets                       AggregateBudgets      see below
   ```

   `PbcProgram` is the predicate-bytecode type from ADRs
   005/035/036.

   `locality_ranges` is not trusted as a proof. Toron may
   pre-narrow it from query predicates, but the coordinator still
   intersects it with descriptor-derived and predicate-derived
   scan ranges before peer fan-out. The second step is a safety
   intersection plus ring partitioning, not an attempt to change
   query semantics.

   `AggregateExpression`:

   ```
   field                  type        notes
   --------------------   --------    -------------------------------------
   tag                    u8          one of §3's catalog tags
   column_ordinal         u16 BE      0xFFFF for count_star
   parameter_block_len    u16 BE      0 unless tag uses parameters
   parameter_block        bytes       tag-specific (HLL precision, KLL k)
   ```

   `AggregateBudgets`:

   ```
   max_locations             LocationCount
   max_rows                  RowCount
   max_wall_time_millis      DurationMillis
   max_partial_state_bytes   ByteCount
   ```

   All fields are newtypes per
   `plans/conventions/newtype-units.md`.

8. **Response envelope.** One `AggregateResponse` per request:

   ```
   field                       type                     notes
   -------------------------   ----------------------   -------------------
   format_version              u16 BE                   must match request
   request_id                  AggregateRequestId       echo of request
   status                      AggregateStatus          success | partial |
                                                         rejected | overflow
   schema_generation           u64 BE                   cross-bind
   schema_fingerprint          SchemaFingerprint        cross-bind
   max_hlc                     HybridTimestamp          staleness ceiling
   partial_count               u16 BE                   == request.aggregate_count
   partials                    AggregatePartial[]       one per expression
   diagnostics                 AggregateDiagnostics     see below
   ```

   `AggregatePartial`:

   ```
   field                       type        notes
   -------------------------   --------    -----------------------------------
   expression_index            u16 BE      position in request.aggregates
   tag                         u8          must match request expression tag
   state_length                u32 BE      bytes of accumulator state
   state                       bytes       tag-dependent, see §9
   ```

   `AggregateDiagnostics`:

   ```
   slabs_consulted                  u64 BE
   slabs_pruned_by_csb              u64 BE
   slabs_answered_from_csb          u64 BE
   slabs_stats_unavailable          u64 BE
   rows_scanned                     u64 BE
   rows_admitted                    u64 BE
   rows_filtered_out                u64 BE
   peer_partials_merged             u32 BE
   peer_partial_bytes               u64 BE
   merge_micros                     u64 BE
   ```

9. **Per-aggregate partial-state encoding.** Frozen at V1 so
   golden byte-layout tests can pin the wire format:

   ```
   tag   state encoding
   ----  ------------------------------------------------------------
   0x01  count_star:        u64 BE
   0x02  count_non_null:    u64 BE
   0x03  signed_sum:        i128 BE (sign-bit-flipped two's complement,
                            byte-comparable per ADR 039 §15) ‖
                            overflow_flag u8
                            empty (zero non-null rows): state_length = 0
   0x04  unsigned_sum:      u128 BE ‖ overflow_flag u8
                            empty: state_length = 0
   0x05  min:               u32 BE len ‖ canonical bytes
                            (per ADR 039 §15) ‖ truncated_flag u8
                            empty: state_length = 0
   0x06  max:               same as min
   0x07  signed_avg:        i128 BE sum ‖ u64 BE count ‖
                            overflow_flag u8
                            empty: state_length = 0
   0x08  unsigned_avg:      u128 BE sum ‖ u64 BE count ‖
                            overflow_flag u8
                            empty: state_length = 0
   0x09  approx_distinct:   u8 precision ‖ register_array
                            (1 << precision bytes)
   0x0A  quantile_kll:      u8 arrow_type_tag (per ADR 039 Appendix B) ‖
                            u32 BE k_parameter ‖
                            u64 BE total_item_count ‖
                            u32 BE level_count ‖
                            (u32 BE level_item_count ‖ canonical_bytes[])* ‖
                            u32 BE compaction_seed
   ```

   `level_item_count` is an item count, not a byte length. The
   byte width of each retained item is inferred from
   `arrow_type_tag`. V1 KLL excludes variable-width Utf8/Binary,
   so the parser never needs per-item length prefixes; any future
   variable-width KLL state must use a new tag or versioned state
   body.

   `compaction_seed` for KLL is derived as
   `BLAKE3(tenant_id || table_object_id || request_id)[0..4]`
   so merges across peers are bit-exact reproducible. The input
   components are fixed-width (`TenantId`, 16-byte
   `TableObjectId`, `AggregateRequestId`), so raw concatenation
   is collision-safe for seed-domain framing.

   The HLL register array layout matches ADR 039 §14
   byte-for-byte so the same register-merge code path serves
   both. This is implementation reuse, not protocol coupling.

   `overflow_flag` semantics: when local accumulation saturates
   `i128`/`u128`, the flag is set; on merge, an overflow-flagged
   partial poisons the merged result to `AGG_NUMERIC_OVERFLOW`.
   Overflow is accumulated as a local flag and propagated through
   merge; storage does not wrap and does not need to abort at the
   first overflowing row.

   SUM `state_length = 0` is the sole wire signal for "zero
   non-null rows." Non-null counts for SUM are local accumulator
   bookkeeping used only to decide whether to emit the empty
   encoding; they are not serialized and are not merge inputs.

   MIN/MAX merge compares canonical bytes lexicographically. If
   one candidate wins, its `truncated_flag` is carried. If two
   candidates compare byte-equal, the merged `truncated_flag` is
   the OR of both flags. OR is safe for prefix truncation because
   byte-equal truncated candidates describe value ranges bounded by
   the same canonical prefix. A final `truncated_flag = 1` means
   the returned bytes are a prefix-bound, not an exact scalar; the
   response status is `partial`, and Toron must fall back to the
   row-query path when exact SQL/Rune semantics are required.

   The corresponding in-memory state enum (illustrative,
   non-authoritative; the wire format above is canonical):

   ```rust
   enum AggregateState {
       Count { value: u64 },
       SignedSum { sum: i128, non_null_count_local: u64, overflow: bool },
       UnsignedSum { sum: u128, non_null_count_local: u64, overflow: bool },
       MinMax { value: Option<Vec<u8>>, arrow_type_tag: u8, truncated: bool },
       SignedAverageParts { sum: i128, count: u64, overflow: bool },
       UnsignedAverageParts { sum: u128, count: u64, overflow: bool },
       Hll { precision: HllPrecision, registers: Vec<u8> },
       Kll { sketch: Vec<u8> },
       Empty,
   }
   ```

### Numerical semantics

10. **Integer SUM is bit-exact and order-independent.** Signed
    integer inputs accumulate into `i128`; unsigned into `u128`.
    Overflow sets a poison flag that merges into response status
    `AGG_NUMERIC_OVERFLOW`; values never wrap around.

11. **MIN, MAX, COUNT, AVG are bit-exact.** No floating-point
    arithmetic in the V1 hot path. AVG returns a `(sum, count)`
    pair; Toron divides.

12. **Boolean MIN/MAX = `false < true`.** Encoded as 0x00/0x01;
    no special-case logic.

13. **Quantile sketches use KLL with deterministic compaction.**
    Default `k = 200` (≈ 4 KiB serialized state at 1B input
    items, ≈ 1% rank error). Per-request override range:
    `k ∈ 64..=2048`. The serialization in §9 tag 0x0A pins level
    layout, item count, and compaction seed so merges across
    peers are bit-exact reproducible.

14. **HLL is a per-query sketch, independent of CSB HLL.**
    Default precision p = 12, range `p ∈ 10..=14` — same range
    as ADR 039 so the `HllPrecision` newtype and validation are
    reused.

15. **Approximation budgets are documented per aggregate.**

### Storage execution path

16. **Single-pass filter-then-fold.** For each admitted slab:
    1. Either short-circuit from CSB (per §5) or decrypt chunks
       per ADR 022.
    2. Run the optional PBC program over each `RecordBatch`.
    3. Fold surviving rows into per-aggregate accumulators.
    4. After the last batch, serialize accumulator state.

    No intermediate materialization of filtered rows.

17. **Coordinator-node fan-out merge.** The node Toron contacts
    is the *aggregate coordinator*. It:
    1. Validates schema, capability, aggregate expressions,
       PBC program, and budgets.
    2. Intersects `locality_ranges` with descriptor/PBC-derived
       scan ranges and partitions the result by ring primary.
    3. Dispatches subrequests to peer storage nodes that
       advertise `PartialAggregatePushdown`.
    4. Executes its own subranges directly.
    5. Merges peer partials by `expression_index`.
    6. Returns one merged envelope to Toron.

    Every remote partial must carry the same `request_id`,
    `table_object_id`, `schema_generation`,
    `schema_fingerprint`, aggregate-expression digest, and
    `as_of_hlc`. Mismatches reject before merge with
    `AGG_REJECT_PARTIAL_CROSSBIND_FAILED`.

    V1 does not return `partial` for peer timeout or partial
    fan-out. Missing, failed, or capability-mismatched peers fail
    closed with a rejected response (`DEADLINE_EXCEEDED` or
    `CAPABILITY_UNAVAILABLE`). `partial` is reserved for complete
    fan-out where every selected storage node responded but at
    least one final value is semantically incomplete (currently:
    prefix-truncated MIN/MAX).

18. **Per-storage-node merge first, then coordinator merge.** A
    storage node servicing multiple slabs merges its per-slab
    partials before responding. Merge is associative and
    commutative for every aggregate in §3.

### Validation

19. **Request validation rules** (coordinator, before any
    execution):
    - `format_version` is supported (currently 1).
    - `aggregate_count ≤ 64`.
    - Each aggregate's `column_ordinal` resolves in the
      descriptor at the named `schema_generation`.
    - Each aggregate's `tag` is in §3's catalog.
    - The named column's Arrow type is in the aggregate's
      input-type set.
    - Parameter blocks parse cleanly per their tag.
    - The optional PBC program passes ADR 035 / 036 validation.
    - `as_of_hlc ≥ min_hlc`.
    - `budgets.max_locations ≤ MAX_PEER_FANOUT`.
    - Toron's session-level exact-only planning flag (not an AGG
      wire field) does not conflict with approximate aggregates;
      if Toron cannot surface approximate semantics, it must not
      construct an AGG request with approximate tags. Storage
      validators may enforce the same check for internal callers
      — otherwise
      `AGG_REJECT_APPROX_NOT_ALLOWED`.

20. **Reject and status codes.** Stable across the version
    family; new codes append, never renumber. Prefix
    `AGG_REJECT_*`:

    ```
    Admission / planning rejections:
    AGG_REJECT_FORMAT_VERSION             unknown format_version
    AGG_REJECT_UNSUPPORTED_FUNCTION       tag not in V1 catalog
    AGG_REJECT_TOO_MANY_AGGREGATES        aggregate_count > 64
    AGG_REJECT_COLUMN_UNKNOWN             column_ordinal absent from
                                           descriptor at schema_generation
    AGG_REJECT_TYPE_UNSUPPORTED           column type not allowed for tag
    AGG_REJECT_SKETCH_PARAMETER_INVALID   HLL p / KLL k out of range
    AGG_REJECT_APPROX_NOT_ALLOWED         approx aggregate in
                                           exact-only session
    AGG_REJECT_PROGRAM_INVALID            embedded PBC predicate invalid
    AGG_REJECT_SCHEMA_STALE               schema_generation behind ring
    AGG_REJECT_SCHEMA_UNKNOWN             table_object_id not in
                                           descriptor cache
    AGG_REJECT_BUDGET_EXCEEDED            request budget over system caps
    AGG_REJECT_DEADLINE_EXCEEDED          query deadline crossed
    AGG_REJECT_CAPABILITY_UNAVAILABLE     chosen peer lacks capability
    AGG_REJECT_PARTIAL_CROSSBIND_FAILED   peer partial crossbind mismatch

    Execution-time statuses:
    AGG_REJECT_PARTIAL_STATE_TOO_LARGE    accumulator state exceeded
                                           per-request byte budget
    AGG_NUMERIC_OVERFLOW                  i128/u128 sum overflowed
                                           (carried in response status)
    ```

### Capability negotiation

21. **Single capability bit: `PartialAggregatePushdown`.** Per
    ADR 036's "Capability negotiation contract":

    ```rust
    enum NodeCapability {
        SlabStatsPruning         = 0,  // ADR 039
        PartialAggregatePushdown = 1,  // ADR 040
    }
    ```

    - **Advertise rule:** unconditionally advertised by any node
      that has the AGG validator, accumulator library, local
      executor, and merge RPC compiled in.
    - **Dispatcher-check rule:** before dispatching a subrequest
      to a peer, the coordinator verifies the peer advertises
      `PartialAggregatePushdown`.
    - **Fallback behavior:** at planning time, if the coordinator
      sees that any peer in the selected `locality_ranges` lacks
      the capability, it returns
      `AGG_REJECT_CAPABILITY_UNAVAILABLE` to Toron without
      dispatching. Toron uses the existing row-query path. The
      coordinator does *not* silently materialize rows on
      Toron's behalf — fallback policy lives in Toron's planner
      where the user's query intent lives. Counter:
      `agg_skipped_no_capability_total`.
    - **Required tests:** topology snapshot round-trip; mixed-
      version dispatcher test using the shared harness from
      `tests/support/capability_harness.rs`.

    Per `plans/conventions/no-version-suffixes.md`, the Rust
    enum variant is `NodeCapability::PartialAggregatePushdown`.

    **Sketches share the bit.** A node either has the AGG
    protocol (including HLL and KLL states) or it does not.

### Planner contract

22. **Toron lowers aggregate query nodes to `AggregateRequest`
    frames.** Per ADR 036, every Toron-IR expression node
    carries a targetability label:
    - `count(*)`, `count(col)`, `sum(int_col)`, `sum(uint_col)`,
      `min(col)`, `max(col)`, `avg(int_col)`, `avg(uint_col)`
      are `PushdownExact` for supported columns.
    - `approx_quantile(col, q)`, `approx_distinct(col)` are
      `PushdownApprox`.
    - `sum(float_col)`, `sum(decimal_col)`, `avg(float_col)`,
      `avg(decimal_col)`, exact `count_distinct(col)`, `stddev`,
      `variance`, `corr` are `ToronOnly`.
    - Any aggregate above a `GROUP BY` clause is `ToronOnly`
      in V1.

23. **Mixed pushdown is the typical shape.** A query like
    `SELECT SUM(amount) FROM events WHERE region = 'us-west' AND
    event_time > '2026-04-01'` lowers to a single
    `AggregateRequest`: predicate filter on `region` and
    `event_time`, single `signed_sum` aggregate on `amount`. One
    round-trip from Toron.

24. **Per-query session diagnostic flag.** Toron may set
    `aggregate_pushdown_disabled: bool` per query for diagnostic
    / repro runs. Defaults off when the capability bit is
    present. Mirrors ADR 039's `slab_stats_pruning_disabled`.

### Observability

25. **Stable counters and histograms.** Metric prefix is
    `agg_*`:

    ```
    agg_request_total
    agg_request_rejected_total{reason}
    agg_rows_scanned_total
    agg_rows_admitted_total
    agg_rows_filtered_out_total
    agg_partials_merged_total
    agg_csb_short_circuit_total
    agg_int_overflow_total
    agg_skipped_no_capability_total
    agg_partial_bytes (histogram)
    agg_exec_micros   (histogram)
    agg_merge_micros  (histogram)
    ```

    ADR 039 stats counters remain the source of truth for slab
    pruning *during* AGG execution: an AGG request consumes ADR
    039 pruning naturally because the predicate filter drives
    the existing pruning path.

### Naming hygiene

26. **No project-name leakage in protocol surfaces.** Per the
    convention `plans/conventions/no-project-name-in-protocols.md`:
    - Acronym: `AGG`, not `WAP` / `WendelonAggregate`.
    - Reject prefix: `AGG_REJECT_*`.
    - Metric prefix: `agg_*`.
    - Module: `src/schema/aggregate.rs`.
    - Frame types: `AggregateRequest` / `AggregateResponse`.
    - Per-query flag: `aggregate_pushdown_disabled`.
    - Capability variant: `PartialAggregatePushdown`
      (already brand-clean).

    References to the predicate bytecode use `PBC` per ADR 041's
    project-name decoupling.

## ADR 039 amendment: `first_tombstone_hlc` field

V1's CSB short-circuit (§5) requires a small extension to ADR
039's TieredSlab pointer schema. This ADR amends ADR 039 in
place; ownership of the field is ADR 039's storage pointer
contract, while ADR 040 is the first consumer.

```rust
// added to TieredSlab pointer (control-ring state):
first_tombstone_hlc: Option<HybridTimestamp>
```

Semantics: when a tombstone record at HLC `T` is committed
against a row in slab `S`, the storage layer atomically updates
the slab's field to retain the *earliest* tombstone timestamp:

```rust
S.first_tombstone_hlc = match S.first_tombstone_hlc {
    None         => Some(T),
    Some(prev_t) => Some(min(prev_t, T)),
};
```

(This is *not* the default `Ord` behavior on `Option<T>` — that
treats `None < Some(_)`, which is the wrong direction for "no
tombstones means latest possible." The match above captures the
intended semantics: `None` means "no tombstones yet"; any
`Some(t)` means "earliest tombstone is at HLC `t`.")

Default `None` for a freshly sealed slab.

The CSB short-circuit visibility check is:

```
slab.seal_hlc ≤ as_of_hlc
&& slab.first_tombstone_hlc.is_none_or(|t| t > as_of_hlc)
```

Cost: ~16 bytes per TieredSlab pointer and one CAS per tombstone
write. Both negligible relative to the optimization they unlock
(chunk-decryption-free MIN/MAX/COUNT for append-only and
steady-state reads).

Pre-amendment sealed slabs have no encoded field; decode defaults
to `None`. CSB short-circuit treats those slabs conservatively as
"not eligible" until natural compaction or rewrite produces a
pointer under the amended schema.

## Rationale

**Why a sibling protocol rather than predicate-bytecode opcode
extension.** Stack-discipline mismatch (accumulators outlive
batches) and numerical-semantics surface (overflow, sketch
error, merge associativity, cross-binding) both expand the
predicate protocol's contract surface unboundedly if mixed in.
Splitting the protocols keeps each contract narrow.

**Why ungrouped only in V1.** GROUP BY would balloon the spec.
Once V1 ships and Toron's planner integration is exercised, the
grouped follow-up becomes a focused ADR.

**Why explicit signed/unsigned tags.** Each tag has one fixed
wire-format shape (modulo `state_length = 0` for empty), no
runtime dispatch on state bytes. Validation is direct. Adding
new types later (e.g., `Decimal`) means appending new tag
numbers, not adding discriminator values within an existing
tag — append-only extension matches the catalog-evolution rule.

**Why integer SUM into `i128`/`u128` with poisoned overflow.** At
warehouse scale, `i64` SUM overflows are plausible. A sticky
overflow flag that poisons the merged response is louder than
modular wrap while still letting every storage node finish its
local fold and return diagnostics.

**Why no float aggregates in V1.** ADR 039 §15 deferred float
canonical encoding. Float SUM has its own design surface (Kahan
vs. naive vs. ordered merge; NaN propagation; infinity handling)
that should land *together with* float canonical encoding for
stats. Two linked deferrals are cleaner than independently
rotting halves.

**Why no decimal aggregates in V1.** ADR 039 intentionally
omits Decimal canonical byte-comparable encoding. Decimal MIN/MAX
can join once Decimal encoding and scale-normalization semantics
are pinned with golden tests; Decimal SUM/AVG also need overflow
and scale rules.

**Why KLL, not t-digest, for quantiles.** V1 rejects float
inputs; t-digest's centroids are floats; KLL operates on ordered
byte representations directly via ADR 039 §15's canonical
encoding. KLL has provable rank-error bounds; t-digest's are
empirical.

**Why KLL is restricted to numeric/time domains in V1.** ADR
039's prefix-truncation for Utf8/Binary makes "rank within
prefix-truncated space" a different semantic than "rank within
full strings"; defer.

**Why per-query HLL for correctness, separate from CSB HLL.**
Pre-filter HLL over-counts filtered-out rows. The aggregate
protocol's HLL is the correctness path; the CSB HLL is
selectivity-only.

**Why CSB short-circuit is included with proof condition.** The
optimization is too valuable to drop (`MIN(event_time)` over a
billion-row append-only table reads one slab tail per slab
instead of every chunk) and the proof shape is cheap (one
`Option<HybridTimestamp>` field, one CAS per tombstone). The
proof condition closes the soundness gap: short-circuit is
allowed exactly when slab-write-time stats equal visibility-at-
`as_of_hlc` stats.

**Why the coordinator merges, not Toron.** Coordinator merge
keeps merge logic single-source-of-truth in Wendelon. Toron sees
one envelope.

**Why composing PBC + AGG in one request.** Dominant query
shape is predicate-then-aggregate; two round-trips throw away
the leverage.

**Why `agg_*` metric namespace, not unified with `pbc_*`.**
Sibling protocol with its own validator, dispatcher, and
accumulator states. Operators get clean capability-fallback
diagnosis under `agg_*`; predicate-protocol signals stay under
`pbc_*`.

**Why single capability bit covers exact + approximate.** A
node either has the AGG protocol or it doesn't. Sketch-specific
gating is a follow-up if a real rollout demands it.

**Why brand-clean naming.** Following the
`no-project-name-in-protocols` convention removes brand leakage
at the protocol layer. Wire formats, reject codes, and metric
prefixes outlive the brand; coupling them ossifies the brand
into a stability contract it doesn't deserve.

## Consequences (the negatives)

- **Toron carries new compiler obligations.** Per-aggregate
  targetability labels, aggregate-expression digest computation,
  capability-aware planner fallback, approximate-aggregate
  user-surface decisions.
- **`GROUP BY` queries get no leverage in V1.**
- **No float or decimal SUM/AVG.**
- **`first_tombstone_hlc` adds tombstone-write cost.** Each
  tombstone write does a CAS on the slab pointer's
  `first_tombstone_hlc` field. Negligible at warehouse write
  rates, but it's not free.
- **CSB short-circuit's narrow soundness window.** Tables with
  active tombstone churn won't see the optimization; only
  append-only and steady-state reads. The visibility check
  itself is cheap, but the optimization payoff is
  workload-shaped.
- **The coordinator becomes a fan-out hot spot.**
- **`AGG_NUMERIC_OVERFLOW` as a poisoned response status is
  operationally noisy.** A legitimate query against a
  malformed-data tenant could fail closed. Modular wrap was
  rejected as silently incorrect.
- **The protocol's wire format is a new compatibility surface.**
  Future aggregates extend the catalog by appending tag bytes;
  renumbering forbidden.
- **KLL parameter `k` is per-request, not per-column.** A
  workload that wants high-accuracy quantiles on every query
  has to set `k` explicitly each time.
- **64-aggregate cap.** Queries with more than 64 aggregates
  fall through to per-aggregate sequential pushdown.
- **Cluster mid-rolling-upgrade pays the row-shipping cost.**
- **Two metric namespaces (`agg_*` + `pbc_*`) mean operators
  must learn both.** Trade vs. unified namespace;
  brand-decoupling outweighs the dashboard-convenience argument.

## Rejected alternatives

Architecture:

- **Predicate-bytecode opcode extensions for aggregates.** Mixes
  stack discipline with state-machine semantics; expands the
  predicate-protocol contract surface unboundedly.
- **Toron-side cross-node merge.** Splits merge logic across two
  code bases; coordinator merge keeps it single-source-of-truth.
- **Aggregate-only requests (no embedded predicate filter).**
  Dominant query shape is predicate-then-aggregate; two
  round-trips throw away the leverage.

V1 scope:

- **`GROUP BY` in V1.** Doubles the design surface.
- **Float aggregates in V1.** Pinned to land with float
  canonical encoding.
- **Decimal aggregates in V1.** Same scope-discipline reasoning;
  Decimal MIN/MAX joins after ADR 039 pins Decimal canonical
  encoding.
- **Exact `COUNT(DISTINCT)` in V1.** Sound implementation blows
  V1 scope; `approx_distinct` is the V1 path.
- **String/Binary `quantile_kll` in V1.** Prefix-truncation
  semantics complicate the sketch.

Numerical:

- **Modular-wrap integer overflow.** Silently incorrect.
- **Pre-divide AVG on the storage node.** Forces storage to
  choose rounding semantics; better to let Toron decide.

Sketch choices:

- **t-digest for quantiles.** Float-centroid representation is
  awkward when V1 rejects float inputs.
- **Reuse CSB HLL as the correctness path for
  `COUNT(DISTINCT)`.** Pre-filter HLL over-counts
  filtered-out rows.

Wire format:

- **Single tag with state-level discriminator** for `sum` and
  `average_parts`. Explicit signed/unsigned split is direct;
  catalog grows by appending tag numbers, not by widening
  discriminator values.
- **Pre-reserve aggregate tag numbers for hypothetical future
  aggregates.** Per
  `plans/conventions/no-premature-backward-compat.md`.

CSB short-circuit:

- **CSB short-circuit without proof condition.** Unsound under
  MVCC.
- **Full deferral of CSB short-circuit.** The proof shape is
  cheap and the optimization is too valuable to defer.

Capability:

- **Separate capability bit for approximate aggregates.** A
  node either has the AGG protocol or it doesn't.
- **Coordinator silently emits row-fan-out on capability
  fallback.** Fallback policy lives in Toron's planner where
  the user's query intent lives.

Naming:

- **Brand-prefixed protocol acronym `WAP`.** Bakes brand into
  wire format; violates `no-project-name-in-protocols`.
- **Brand-prefixed reject codes `WAP_REJECT_*`.** Same reason.
- **`AGG_REJECT_CAPABILITY_MISSING`.** `_UNAVAILABLE` reads
  better at the user-facing rejection level and matches
  established distributed-systems vocabulary (gRPC,
  HTTP 503).
- **Unified `pbc_aggregate_*` metric namespace.** Reinforces a
  single-namespace-for-multiple-protocols pattern; sibling
  protocol gets its own clean prefix.
- **`agg_rows_aggregated_total` metric.** Redundant with
  `agg_rows_admitted_total` after the predicate filter.

## Open questions

1. **Default KLL `k`.** k = 200 is the V1 default; tune from
   observability data once shipped.

2. **Boolean MIN/MAX in user-facing SQL.** §12 commits to
   `false < true` at the protocol level. Whether Toron's SQL
   surface exposes `MIN(bool_col)` natively or rewrites to
   `BOOL_OR` / `BOOL_AND` is a Toron-side decision.

3. **Capability-fallback retry policy.** When Toron receives
   `AGG_REJECT_CAPABILITY_UNAVAILABLE`, does it retry against a
   different coordinator or fall straight to row pushdown?
   Toron-planner decision; this ADR commits to "fall straight
   to row pushdown" as the default but leaves Toron room to
   refine.

Implementation-time questions (KLL library choice, HLL code
deduplication between `stats.rs` and `aggregate.rs`, wire
encoding strategy, bench gate thresholds, tombstone-write
atomicity strategy) are tracked alongside the implementation PR.

## Deferred to follow-up ADRs

1. **Grouped aggregation (`GROUP BY`).** Per-slab cardinality
   caps, hash-table strategies, group-key canonical encoding,
   spill behavior.

2. **Float canonical encoding + float aggregates.** ADR 039 §15
   already defers float encoding. Float SUM/AVG with
   well-defined merge semantics (Kahan? ordered? naive?) and
   NaN propagation rules joins this ADR.

3. **Decimal aggregates.** Pinned Decimal canonical
   byte-comparable encoding, then `Decimal256` overflow and scale
   contract for SUM/AVG.

4. **String/Binary quantiles.** Either drop ADR 039's
   prefix-truncation for KLL inputs (separate full-byte stat),
   or invent a "rank within prefix space" semantic.

5. **Exact `COUNT(DISTINCT)`.** Either materialize all distinct
   values per slab and merge globally (memory-bounded variants
   only), or accept the linear-counting tail when approximate
   distinct is good enough.

## References

- ADR 005: Decoupling Compute via Pushdown Bytecode (PBC; the
  predicate protocol).
- ADR 035: Vectorized PBC executor.
- ADR 036: PBC pushdown contract — §10 reserved this ADR's
  design space; this ADR adds `PartialAggregatePushdown`
  capability bit.
- ADR 037: Tenant Table Schemas — column ordinals,
  `schema_generation`, MVCC visibility model.
- ADR 038: Column-Aware Locality V1 (Z-Order).
- ADR 039: Slab-Footer Column Statistics V1 — CSB drives
  predicate pruning during AGG execution; AGG V1 extends the
  TieredSlab pointer with `first_tombstone_hlc` to enable CSB
  short-circuit under MVCC.
- ADR 041: Project-name decoupling from internal protocols and
  types.
- `plans/conventions/no-project-name-in-protocols.md`.
- `plans/conventions/no-version-suffixes.md`.
- `plans/conventions/no-premature-backward-compat.md`.
- `plans/conventions/capability-vs-feature-naming.md`.
- `plans/conventions/newtype-units.md`.
- ADR 042 (planned): Analytical multi-row fragments.
- Future: Grouped Aggregate Pushdown; Float Canonical
  Encoding.
