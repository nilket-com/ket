# ADR 039: Slab-Footer Column Statistics V1

## Status

Ratified.

## Context

ADR 035 gave Wendelon a vectorized PBC executor. ADR 037 made tenant
tables real with declared columns. ADR 038 closed the column-aware-
locality half: range queries on declared clustering columns now map
to a small, contiguous arc set on the unified ring.

The next compounding leverage is **skip the slab without reading any
data bytes.** A predicate `event_time > '2026-04-01'` against a
billion-row table should consult a per-slab `min`/`max` and eliminate
99% of cold-storage scan before any range-GET fires. This is the
warehouse-class pruning property that composes multiplicatively with
ADR 038's locality narrowing.

The slab-format substrate already exists. ADR 022 froze the L2 slab
layout: `slab_header` (64 B) → encrypted chunks → `offset_index`
(variable) → `slab_footer` (256 B, fixed) → tail anchor. ADR 022's
footer is deliberately too narrow for stats. ADR 039 introduces the
missing layer: an encrypted **column statistics block** between
`encrypted_chunks` and `offset_index`, located via repurposed
reserved bytes in the existing `slab_footer`. The block is
DEK-encrypted (column min/max and small-set dictionaries are
data-derived and leak distributions in a way `offset_index` does
not), and the unwrap cost is amortized across all subsequent reads
of the slab.

The design surface the post-038 roadmap names for ADR 039:

1. Which stats to maintain by default vs. opt-in via descriptor.
2. Bloom filter bit budget per slab and false-positive target.
3. Dictionary encoding threshold (cardinality cutoff).
4. Compaction interaction — stats are recomputed; lineage or merged?
5. Lazy stats computation vs. eager.
6. Toron planner contract — extends ADR 036.

Each gets an explicit decision below.

## Decision

Wendelon adds a DEK-encrypted column-statistics block (CSB) to the
slab format, gated behind `slab_format_version = 2`. Statistics are
computed eagerly at slab write time, recomputed (not merged) at
compaction, and consulted internally by Wendelon's storage layer
before any record bytes are read. The Toron-visible surface is a
single ADR 036 capability bit plus stable explain counters; no new
wire RPC names slab IDs.

### Scope and compatibility

1. **Two stat tiers: always-on and opt-in.**
   - **Always-on** (no descriptor flag, no opt-out): `min`, `max`,
     `null_count`, `non_null_count`, per column; `row_count` per slab.
   - **Opt-in via descriptor:** `bloom_filter` (per column),
     `hll_distinct` (per column), `small_set` dictionary (per column).
   - **Deferred entirely:** quantile sketches (t-digest, KLL),
     column-encoding dictionaries, custom histograms. Quantile
     sketches are reserved for the partial-aggregate sibling protocol
     (ADR 040). *Column-encoding* dictionaries (storage representation)
     overlap with Arrow's native `DictionaryArray` and should not
     duplicate at the slab layer; the `small_set` opt-in below is a
     *stats-block enumeration* for pruning, distinct from column
     encoding.

2. **Slab format version bumps to 2.** Format-v1 slabs continue to
   exist and read normally; planners that ask for stats on a v1 slab
   receive `STATS_UNAVAILABLE` and fall through to full scan. Writers
   produce v2 slabs going forward; ADR 022 readers gracefully reject
   v2 slabs (refuse-to-decode is the existing v1 behavior for unknown
   versions). A separate operational migration upgrades v1 slabs to
   v2 via offline compaction (Phase 11 below).

3. **No retroactive stats computation in V1.** A slab written without
   stats stays without stats until a compaction-driven rewrite. Lazy
   in-place stat addition is rejected (decision §12 below).

4. **Stats are mandatory for typed-table slabs under v2.** Legacy
   `{key, value}` KV slabs (ADR 037 §1) remain v1 unless an explicit
   migration upgrades them. Typed-table v2 slabs always carry the
   always-on tier; the opt-in tier follows the descriptor.

### Stats catalog

5. **Always-on stat semantics.**
   - `min` / `max`: per-column, type-typed, computed by Arrow comparison
     kernels appropriate to the column's `DataType`, then re-encoded
     into the canonical byte-comparable form pinned in §15. Null values
     do not participate. For columns with all-null rows in the slab,
     both fields are absent; `stat_flags` bits 0/1 (`has_min`/`has_max`)
     indicate absence.
   - `null_count`: u64 count of null values in the column over this slab.
   - `non_null_count`: u64 count of non-null values in the column over
     this slab. Redundant with `row_count - null_count` but stored
     explicitly: `non_null_count == 0` lets the planner short-circuit
     any `IS NOT NULL` predicate without consulting min/max, and
     mismatch with `row_count - null_count` is a cheap detectable
     stats-build bug at write time.
   - `row_count`: u64; identical for every column in the slab (slab
     property), but stored in the stats block header for reader
     convenience and redundancy with `slab_footer.record_count`.
   - **Opaque-tagged entries.** Any column whose type does not have a
     pinned canonical byte-comparable encoding under §15 — currently
     `Float32`, `Float64`, `Decimal128`, `Decimal256`, and any nested
     type (list/struct/map) — still gets a CSB entry, but with
     `arrow_type_tag = 0x0E` (`OpaqueUnsupported`, see Appendix B).
     The entry carries only `has_null_count = 1` and
     `has_non_null_count = 1` from the Arrow validity bitmap (cheap);
     no min/max, no bloom, no HLL, no small-set, no truncation flags.
     An explicit opaque entry is preferable to omission: it tells the
     planner the writer knew about the column and chose not to stat
     it (rather than the column being absent from the descriptor at
     write time). Opt-in stat policies (bloom/HLL/small-set) that
     name an opaque-typed column still fail descriptor validation
     with `PBC_REJECT_STATS_UNSUPPORTED_TYPE`; only the always-on
     null/non-null counts are produced.

6. **Bloom filter (opt-in).** SipHash-1-3 with two seeds, k = 7 hash
   functions, default bit budget = `ceil(row_count × 9.6)` bits per
   column (≈ 1% false-positive rate at full slab capacity per the
   classic m/n = 9.6 sizing). Descriptor can override `bits_per_row`
   in `1..=32`; the validator caps the per-slab bloom-filter total
   at 16 MiB to bound footer size. Bloom filters apply to equality
   predicates (`col = X`, `col IN (...)`).

7. **HLL distinct count (opt-in).** Default precision p = 12 (4096
   registers, ≈ 1.5% standard error, 4096 bytes per column).
   Descriptor override range: `p ∈ 10..=14` (1 KiB to 16 KiB per
   column). HLL is intended for selectivity estimation by the
   planner, **never for correctness pruning** — approximate distinct
   cannot prove a value absent. User-visible `COUNT(DISTINCT ...)`
   goes through ADR 040's partial-aggregate protocol when it lands.

8. **Small-set dictionary (opt-in).** When a column's distinct
   cardinality on a slab is below a configured cap, the stats block
   stores the canonical-encoded sorted set of distinct non-null
   values. Default cap: 4096 distinct values, 32 KiB total bytes.
   Builder emits `Unknown` (no set) and falls back to bloom filter
   (if also opt-in) when the cap is exceeded. Small-set dictionaries
   are **zero false-positive, zero false-negative** for equality and
   IN-list pruning when present. They compose well with bloom filters:
   small sets prune exactly until cardinality blows past the cap;
   bloom filters take over from there.

9. **Stat type tagging is Arrow-typed; string/binary stats carry a
   truncation flag.** Min/max are stored as length-prefixed bytes in
   the column's canonical byte-comparable encoding (§15). Readers
   cross-check the stat type tag against the descriptor's column type
   at the named `schema_generation`; mismatch fails closed. For
   `Utf8`/`LargeUtf8`/`Binary`/`LargeBinary`, min/max bytes are
   stored truncated to a per-column prefix length (default 64 bytes);
   a truncation flag accompanies each value. The planner may skip a
   slab only when the predicate is provably outside the *expanded*
   range — i.e., `predicate_value > stored_max` requires `stored_max`
   not be truncated, or the comparison reduces to a strict prefix
   that excludes any extension. Conservative; preserves correctness;
   may miss prune opportunities on long strings.

### Slab layout extension

10. **New section: `column_stats_block` (CSB), variable length,
    encrypted under the slab DEK.** Inserted between
    `encrypted_chunk_stream` and `offset_index`. Stats are encrypted
    because column min/max and small-set dictionaries leak
    data-derived distributions, materially more than `offset_index`'s
    record-size metadata. The DEK unwrap cost on planner consultation
    amortizes against all subsequent reads of the slab. Stats AAD
    binds `tenant_id || slab_id || stats_format_version ||
    schema_generation || stats_block_hash128` in a separate nonce
    domain from chunk AAD (ADR 020 §2). The full AAD construction
    appears in the §"Slab footer extension diagram" below. Slab byte
    layout under format-v2:

    ```
    offset                                    field
    ----------------------------------------  -------------------------
    0                                         magic_head (8 B)
    8                                         slab_header (SH = 64 B)
    8 + SH                                    encrypted_chunk_stream (P)
    8 + SH + P                                column_stats_block (CSB)  -- NEW, encrypted
    8 + SH + P + CSB                          offset_index (I)
    8 + SH + P + CSB + I                      slab_footer (SF = 256 B)
    8 + SH + P + CSB + I + SF                 footer_length (4 B)
    ... + 4                                   index_length (4 B)
    ... + 8                                   magic_tail (8 B)
    ```

    The 16-byte tail anchor is **unchanged**. CSB sits before
    `offset_index` so a tail-first reader finds the offset_index in
    the same fetch window as today (the common-case point-read path
    is unchanged). The footer's reserved 32-byte suffix carries the
    CSB locator (decision §11).

    **HMAC scope.** The footer HMAC covers the locator fields
    (including the footer-pinned `stats_block_hash128`) but **not**
    the CSB ciphertext bytes. Stats-block integrity is the conjunction
    of (a) the footer-pinned hash, validated during the normal
    tail-fetch HMAC verification, and (b) the CSB's own AEAD tag,
    validated when the planner consults stats. This split keeps the
    point-read path independent of the stats block: a query that
    doesn't need stats neither fetches them nor pays HMAC bytes for
    them.

11. **CSB locator lives in the formerly-reserved 32 bytes of
    `slab_footer`.** ADR 022's footer reserved bytes 224..256 (must
    be 0x00 in v1). ADR 039 v2 repurposes this region:

    ```
    offset  size  field
    ------  ----  ---------------------------------------------------
    224       2   stats_format_version  (BE; 0 = no stats, 1 = ADR 039 V1)
    226       2   stats_flags           (BE bitset; see below)
    228       4   stats_block_len       (BE; ciphertext bytes incl. AEAD tag)
    232       4   stats_plaintext_len   (BE; post-decrypt size)
    236       4   stats_column_count    (BE; entry count in CSB)
    240      16   stats_block_hash128   (BLAKE3(stats ciphertext)[0..16])
    ------  ----
    total    32
    ```

    `stats_flags` bits:

    - bit 0 = `present_and_encrypted` (must be 1 in V1)
    - bit 1 = `has_small_set` (some column has a small-set dictionary)
    - bit 2 = `has_bloom` (some column has a bloom filter)
    - bit 3 = `has_hll` (some column has an HLL sketch)
    - bit 4 = `always_on_complete_for_supported` (every column in the
      §15 allowlist carries the always-on tier; must be 1 in V1)
    - bits 5..15 reserved, must be 0.

    The `stats_block_hash128` is a cache key, an integrity cross-bind
    (also folded into the CSB AEAD AAD), and a fast mismatch detector
    — not the integrity root. Integrity is the footer HMAC plus the
    AEAD tag inside the CSB ciphertext.

    A v1 reader treats `stats_format_version != 0` as an unknown
    format and either (a) refuses if the slab claims
    `slab_format_version >= 2` (correct behavior), or (b) ignores the
    suffix bytes if the slab claims `slab_format_version == 1` (the
    reserved-bytes invariant of v1 still holds because v1 writers
    never set `stats_format_version`).

12. **No lazy CSB attachment.** CSB is written exactly once at slab
    finalization, before the footer HMAC. A slab without stats is
    format-v1 by definition (or v2 with `stats_format_version = 0`,
    which the migration tool may produce transitionally); reading it
    under a stats-required path returns `STATS_UNAVAILABLE` and falls
    through to full scan. Compaction is the only way to add stats to
    an existing slab.

### CSB header and per-column block layout

13. **CSB internal layout (plaintext, post-decrypt).** Fixed header +
    per-column variable blocks. Frozen so Phase 2 can write golden
    byte-layout tests:

    ```
    offset  size  field
    ------  ----  ---------------------------------------------------
    0         8   csb_magic               b"WNDSTAT1"
    8         4   csb_format_version      u32, currently 1
    12       16   table_object_id         TableObjectId ([u8; 16])
    28        8   schema_generation       u64
    36       32   schema_fingerprint      BLAKE3 of canonical
                                          descriptor at schema_generation
    68        8   row_count               u64 (mirrors footer.record_count)
    76        4   column_block_count      u32
    80        4   reserved                must be 0
    ------  ----
    fixed    84 bytes header

    per column block (one per column entry; one entry per supported
    column, plus one entry per opaque-tagged column):
      column_ordinal         (u16, ordinal in the descriptor)
      arrow_type_tag         (u8, see Appendix B)
      stat_flags_hi          (u8, high byte of column-level flags;
                              reserved 0 in V1)
      stat_flags_lo          (u8, low byte of column-level flags)
      block_length           (u32, total bytes of this column's payload)
      payload                (variable; ordered min, max, null_count,
                              non_null_count, bloom, hll, small_set
                              per stat_flags)

    Column-level `stat_flags` (16 bits, big-endian — high byte first):
      bit  0  has_min
      bit  1  has_max
      bit  2  has_null_count            (always 1 in V1)
      bit  3  has_non_null_count        (always 1 in V1)
      bit  4  has_bloom
      bit  5  has_hll
      bit  6  has_small_set
      bit  7  min_truncated             (Utf8/Binary only)
      bit  8  max_truncated             (Utf8/Binary only)
      bits 9..15  reserved, must be 0
    ```

    The `csb_magic` is forensic only — the AEAD tag is the integrity
    root. No per-entry `column_name_hash`: `column_ordinal` resolves
    the column and the 32-byte `schema_fingerprint` defends against
    descriptor mutation. **All multi-byte integers in the CSB are
    big-endian, matching ADR 022 §1's slab-tail convention.** A reader
    that finds non-zero reserved bits or bytes treats the CSB as
    malformed and falls back to `STATS_UNAVAILABLE`.

14. **Per-stat payload encoding.** The payload region within each
    column block carries fields in a fixed order, present iff the
    corresponding `stat_flags` bit is set:

    ```
    order  field            present iff           encoding
    -----  ---------------  -------------------   -----------------------
    1      min              has_min               u32 BE len ‖ bytes
    2      max              has_max               u32 BE len ‖ bytes
    3      null_count       has_null_count        u64 BE
    4      non_null_count   has_non_null_count    u64 BE
    5      bloom_filter     has_bloom             u32 BE bit_count ‖
                                                   bit_array
    6      hll_distinct     has_hll               u8 precision ‖
                                                   register_array
                                                   (1 << precision bytes)
    7      small_set        has_small_set         u32 BE value_count ‖
                                                   (u32 BE len ‖ bytes)*
                                                   sorted by canonical
                                                   byte order
    ```

    All multi-byte integers in the payload are big-endian, matching
    the CSB header (§13) and ADR 022 §1's slab-tail convention.
    `min`/`max` bytes use the canonical byte-comparable encoding
    pinned in §15. For `Utf8`/`LargeUtf8`/`Binary`/`LargeBinary`, the
    bytes are the prefix-truncated form; the truncation status is in
    `stat_flags` bits 7 (`min_truncated`) and 8 (`max_truncated`).
    Opaque-tagged entries (`arrow_type_tag = 0x0E`) carry only
    `null_count` and `non_null_count`; `stat_flags` must equal
    exactly `has_null_count | has_non_null_count`. Any other flag
    bit set on an opaque entry — `has_min`, `has_max`, `has_bloom`,
    `has_hll`, `has_small_set`, or either truncation bit — is
    malformed and the reader falls back to `STATS_UNAVAILABLE`.

15. **Canonical byte-comparable encoding for stat values.** Min/max
    values, small-set entries, and bloom-filter hash inputs share one
    on-disk encoding per Arrow type, chosen so that lexicographic
    `memcmp` over the stored bytes matches the column's PBC
    comparison semantics. This makes range pruning a branch-free byte
    compare and pins the writer's output bytes so two writers on the
    same data emit identical CSBs (golden-test stable):

    ```
    Arrow type                    canonical encoding
    ----------------------------  ----------------------------------
    Int{8,16,32,64}               sign-bit-flipped big-endian
                                  two's-complement (8/16/32/64 bits)
    UInt{8,16,32,64}              big-endian
    Boolean                       0x00 false, 0x01 true
    Timestamp(unit, _)            unit-normalized signed i64,
                                  sign-bit-flipped big-endian
    Date32 / Date64               same as Timestamp (signed integer)
    Utf8 / LargeUtf8 /            raw bytes prefix-truncated to
      Binary / LargeBinary        per-column cap; truncation flag
                                  in stat_flags bits 7 and 8
    null                          counted in null_count, never
                                  encoded as min/max
    ```

    **Float, decimal, and nested types do not have canonical
    byte-comparable encodings here.** `Float32`, `Float64`,
    `Decimal128`, `Decimal256`, and any nested type (list/struct/map)
    receive opaque-tagged CSB entries per §5, carrying only
    null/non-null counts. Pinning their canonical encodings (NaN
    handling, signed-zero bias, subnormals, decimal scale
    normalization, nested-element ordering) requires semantic
    decisions that compose with PBC's comparison rules and a
    dedicated test corpus. Until those encodings are pinned, the
    opaque-tag path lets the writer be honest with the planner — the
    column is known, it just isn't statable — without inventing a
    placeholder encoding that would have to be unwound later.

16. **Per-slab CSB byte budget.** Total CSB ciphertext size is capped
    at `slab_payload_bytes / 256`. Scales with slab
    size: a 64 MiB slab gets 256 KiB cap; a 1 GiB slab gets 4 MiB cap;
    a 32 MiB slab gets 128 KiB cap. The descriptor validator estimates
    worst-case CSB size at the descriptor's declared row capacity and
    rejects descriptors that exceed the cap. At runtime, the stats
    builder emits `Unknown` for any opt-in stat that would push the
    block past the cap; always-on min/max/null/non-null counts never
    get trimmed.

    Operator-facing initial caps that fall out of these rules:

    ```
    max_stats_block_ciphertext_bytes_per_slab = slab_payload_bytes / 256
    max_small_set_values_per_column           = 4096
    max_small_set_bytes_per_column            = 32 KiB
    default_bloom_false_positive_target       = 1%
    max_bloom_bytes_per_column                = 16 KiB
    ```

    These are deliberately conservative. The cheap win is
    min/max/null. Small-set membership and bloom stats earn their way
    in through measured predicate pressure.

### Compaction interaction

17. **Compaction recomputes stats from the output rows; never merges
    inputs.** MVCC retention and tombstone removal can prune rows
    from the compaction output, which would make merged input stats
    over-inclusive (looser min/max bounds, FP-inflated bloom filters,
    HLL registers covering removed values). A single pass over the
    output rows during compaction recomputes exact stats at modest
    extra CPU cost. The compaction pass is already O(output_rows)
    for the actual data write; stat computation rides along.

    Bloom-filter OR and HLL register-wise max are valid mathematical
    merge operations, but they are deliberately not the ADR 039
    compaction invariant. The compactor already reads survivors to
    write the replacement slab; computing fresh stats from those
    survivors avoids false-positive inflation and avoids special
    cases when compaction drops rows.

18. **No per-input lineage in the CSB.** Lineage stays in the
    compaction journal (ADR 023 territory) where forensics need it.
    The CSB is the read-path hot surface and carries only the output
    slab's stats.

19. **Compaction across descriptors is rejected.** Inherits ADR 037
    §13 and ADR 038 §18: candidate slabs must share
    `(table_object_id, schema_generation, locality_policy_fingerprint)`.
    ADR 039 adds: candidate slabs must also share `schema_fingerprint`
    in the CSB header. Mismatch is a stable compaction-eligibility
    error, not a runtime decode failure.

### Planner contract

20. **Stats pruning is internal to Wendelon's storage layer.** No new
    Toron→Wendelon RPC names slab IDs or surfaces raw stats bytes.
    Wendelon already owns arc-to-slab enumeration, tiered pointer
    resolution, footer parsing, and metadata caching (ADRs 022, 037,
    038). Stats consultation runs as an additional internal filter
    inside the existing scan plan: after locality narrows the ring
    arcs and slab enumeration produces candidate slabs, but before
    chunk-stream decryption and PBC dispatch. Toron issues the same
    query plan it issues today; the prune happens invisibly.

21. **ADR 036 extends with a single capability bit and stats-pruning
    diagnostics, not a new query-response surface.** Toron-visible
    additions:

    - **Capability bit `slab_stats_pruning_v1`** advertised by
      Wendelon nodes that have ADR 039 stats support compiled in and
      enabled by configuration.
    - **Optional per-query session flag** "stats pruning may be used"
      — defaults on when the capability bit is present; can be forced
      off by Toron for diagnostic / repro runs.
    - **Stable stats-pruning counters** emitted as process metrics
      and carried in Wendelon's internal query-plan diagnostics:
      `candidate_slabs_before_stats`, `slabs_pruned_by_stats`,
      `slabs_scanned_stats_missing`, `slabs_scanned_stats_unsupported`,
      `slabs_scanned_predicate_unrepresentable`, `stats_cache_hits`,
      `stats_cache_misses`, `stats_decrypt_failed_total`.

    No predicate-level prune reasons cross the boundary as free-form
    strings; ADR 039 does not add fields to `QueryResponseMeta`.

22. **Stats are planner-level, not PBC opcodes.** ADR 035's PBC
    executor stays bytecode-pure. Stats consultation precedes
    bytecode dispatch; once a slab is admitted, the PBC program runs
    over its records as before. No new PBC opcodes for stats.

23. **Stats absence and unsupported predicates fall through to full
    scan.** A slab without stats (legacy v1, opt-out configuration,
    or descriptor-mismatch fail-closed), an unsupported predicate
    shape (range over non-comparable type, predicate referencing a
    column without stats policy coverage), or any of the failure
    modes in §"Failure modes" below all degrade to "scan the slab,"
    never to "reject the query." Toron sees only the aggregate
    counter.

24. **Selectivity hints from HLL are advisory.** Internal cost
    estimation (join ordering, batch sizing) may consult HLL register
    estimates, but no correctness decision depends on HLL accuracy.
    The HLL precision floor (`p >= 10`) bounds error to ~3% standard
    error. **HLL is never used for correctness pruning** — only
    `min`/`max` bounds, small-set membership, and bloom filter
    "definitely-not-present" checks drive prune decisions.

    **Planner-side unified distinct-count accessor.** The CSB wire
    format keeps `small_set`, `bloom`, and `hll` as independent
    slots. The storage-node accessor that wraps a parsed CSB exposes
    a single distinct-count view to the planner:

    ```rust
    enum DistinctCount {
        Exact(u64),                              // from small_set when
                                                 // present, == value_count
        Approx { estimate: u64, std_err: f64 },  // from HLL when present,
                                                 // small_set absent
        Unknown,                                 // no slot populated
    }
    ```

    Resolution order: small-set wins when present (it's exact);
    otherwise HLL when present; otherwise `Unknown`. This keeps the
    planner's cost-model code agnostic to which opt-in stats a column
    carries while preserving the rule that only `Exact` is
    correctness-eligible (e.g., for "if exact distinct ≥ N, the slab
    cannot be pruned by IN-list of size < N" reasoning). Bloom is
    *not* a distinct-count source — its only role is the
    "definitely-not-present" pruning check.

### Storage and write path

25. **Stats computation is a pass over the in-memory batch at slab
    finalization.** O(rows × cols) work; runs after the offset index
    is built and before the footer HMAC is computed. Single pass for
    `min`/`max`/`null_count`/`non_null_count`; bloom filter, HLL, and
    small-set structures hash/compare each value once during the same
    pass.

26. **Stats are mandatory for typed-table v2 slabs.** Always-on tier
    is computed for every column; opt-in tiers are computed when the
    descriptor declares them. Legacy `{key, value}` KV slabs (ADR 037
    §1) remain v1 and never carry stats unless explicitly migrated.

27. **`TieredSlab` carries stats-presence advisories.** Three
    optional fields on the tiered-slab pointer let nodes pre-decide
    whether to fetch the stats block on a tail read:

    ```rust
    stats_format_version: Option<u16>
    stats_block_len:      Option<u32>
    stats_block_hash128:  Option<[u8; 16]>
    ```

    These mirror the locator's footer-resident copy. They are
    *advertisements*: a node may use them to skip the stats fetch
    entirely (when `stats_format_version == None` or the planner has
    no representable predicate) or to validate a cache hit before
    decrypt. The slab tail remains the source of truth — a discrepancy
    between `TieredSlab` and the on-object footer fails closed by
    treating stats as `STATS_UNAVAILABLE` for that query and emitting
    a `pbc_slab_stats_pointer_drift_total` metric for the operator.
    Per-column stats are deliberately *not* replicated to the control
    ring; that would bloat Raft state and duplicate the slab's
    authoritative bytes.

### Failure modes

All of the following degrade to **scan the slab**, never to "reject
the query":

- No stats block (slab is v1, or v2 with `stats_format_version = 0`).
- `stats_block_len > min(256 KiB, slab_size / 256)` (configured cap
  exceeded; should not happen for descriptor-validated tables but
  defended against).
- Unknown `stats_format_version` (forward-compat hatch for future
  ADRs).
- AEAD authentication failure on the stats ciphertext after footer
  HMAC succeeds (suspect tampering; falls back to scan, raises
  `pbc_slab_stats_decrypt_failed_total` for ops visibility).
- `schema_fingerprint` mismatch against descriptor cache.
- Predicate references a column that has no stats (column not in the
  descriptor's stats policy or column is in the unsupported-types
  set).
- Predicate shape isn't representable as min/max/IN bounds (e.g.,
  range over a non-comparable type).
- Cap-exceeded opt-in stat (small-set or bloom too large at build
  time; CSB carries `Unknown` in that slot).

Only structural slab corruption that already fails ADR 022 read
validation (footer HMAC mismatch, magic_tail wrong) fails the read.
Stats failures are pruning-quality issues, not correctness issues.

## Descriptor shape

```rust
struct ColumnStatsPolicyV1 {
    bloom_filters: Vec<ColumnBloomSpecV1>,
    hll_distincts: Vec<ColumnHllSpecV1>,
    small_sets: Vec<ColumnSmallSetSpecV1>,
}

struct ColumnBloomSpecV1 {
    column_name: String,
    bits_per_row: BloomBitsPerRow,      // 1..=32, default 10 (≈ 1% FP)
}

struct ColumnHllSpecV1 {
    column_name: String,
    precision: HllPrecision,            // 10..=14, default 12 (~1.5% std error)
}

struct ColumnSmallSetSpecV1 {
    column_name: String,
    max_distinct_values: SmallSetValueCap, // 1..=4096, default 4096
    max_bytes: SmallSetByteCap,         // 1..=32_768, default 32_768 (32 KiB)
}
```

Embedded in `TableSchemaDescriptorV1` as a new `stats_policy:
ColumnStatsPolicyV1` field. Always-on stats are not represented in
the policy because they are not optional.

The stat-policy scalar fields are transparent serde newtypes, not raw
integers, so normal Rust construction goes through range-checked unit
constructors while the serialized descriptor remains primitive-sized.

Validation invariants:

- Each `column_name` must resolve in the table's Arrow schema.
- A column may appear in at most one `ColumnBloomSpecV1`, at most
  one `ColumnHllSpecV1`, and at most one `ColumnSmallSetSpecV1` (no
  duplicates within a stat kind; a column may appear across kinds).
- Bloom filters, HLL, and small-sets all reject columns of types not
  in §15's allowlist — i.e., `Float32`/`Float64`,
  `Decimal128`/`Decimal256`, and any nested type (list/struct/map).
  Such columns may exist in the schema but carry only opaque-tagged
  always-on entries (§5); opt-in stat policies that name them fail
  descriptor validation with `PBC_REJECT_STATS_UNSUPPORTED_TYPE`.
- HLL additionally rejects columns whose canonical-encoded values
  exceed 64 bytes (very long strings → cap at 64-byte prefix hash
  for HLL input).
- Total estimated per-slab CSB ciphertext size at the descriptor's
  declared row capacity must not exceed `slab_payload_bytes / 256`;
  it scales with the configured slab size.

## Slab footer extension diagram

Compact form (high object offset descending):

```
  high object offset
        |
        v
  [encrypted_chunk_stream]
  [stats_block_ciphertext]  <-- NEW (encrypted under DEK, dedicated AAD nonce domain)
  [offset_index]            <-- (plaintext)
  [footer_v2]               <-- (plaintext, 256 B)
   ├ bytes 0..224           <-- (unchanged from v1)
   └ bytes 224..256         <-- (CSB locator, was reserved)
  [footer_length]           <-- (4 B)
  [index_length]            <-- (4 B)
  [magic_tail]              <-- (8 B)
```

Side-by-side with v1:

```
   format v1 (ADR 022, unchanged)         format v2 (ADR 039, new)
   -------------------------------         ------------------------------
   magic_head        (8 B)                 magic_head        (8 B)
   slab_header       (64 B)                slab_header       (64 B)
   encrypted_chunks  (P)                   encrypted_chunks  (P)
                                           column_stats_block (CSB)  ← NEW (encrypted)
   offset_index      (I)                   offset_index      (I)
   slab_footer       (256 B)               slab_footer       (256 B)
                                            ├ bytes 0..224   (unchanged from v1)
                                            └ bytes 224..256 (CSB locator, was reserved)
   footer_length     (4 B)                 footer_length     (4 B)
   index_length      (4 B)                 index_length      (4 B)
   magic_tail        (8 B)                 magic_tail        (8 B)
   tail anchor: 16 B                       tail anchor: 16 B (unchanged)
```

`slab_format_version` in both `slab_header` and `slab_footer` bumps
to 2. A v1 reader seeing `slab_format_version = 2` refuses (existing
forward-incompatibility). A v2 reader seeing a v1 slab treats stats
as `STATS_UNAVAILABLE` and falls through to full scan.

The 16-byte tail anchor is **unchanged** in v2 — v2 readers find the
CSB locator in the footer suffix (bytes 224..256), which were
reserved-must-be-zero in v1. CSB sits between `encrypted_chunks` and
`offset_index` so the common-case point-read tail-fetch window
(footer + magic_tail + offset_index) is unchanged from v1; stats
consultation is a separate, planner-driven fetch that may extend the
read range backward by `stats_block_len` bytes.

```
HMAC coverage (ADR 022 §5 extended for v2):
  magic_head ‖ slab_header ‖ offset_index ‖
  slab_footer[0..192] ‖ slab_footer[224..256] ‖ footer_length ‖
  index_length ‖ magic_tail

  slab_footer[192..224] is the HMAC field itself, excluded.
  slab_footer[224..256] is the CSB locator, including the
    footer-pinned stats_block_hash128 — this 16-byte hash binds
    the stats ciphertext into the HMAC indirectly.
  stats_block_ciphertext is NOT included in the HMAC coverage —
    stats integrity comes from the footer-pinned hash plus the
    CSB AEAD tag, which the reader verifies only when consulting
    stats. Point reads that don't touch stats need not fetch the
    ciphertext to validate the footer.
  HMAC key = Tenant KEK identified by tenant_kek_id (unchanged from v1).

CSB AEAD:
  key   = slab DEK (unwrapped via ADR 020 §3 after footer HMAC verifies)
  nonce = derived from a stats-block-specific domain separator,
          distinct from chunk AEAD nonces (ADR 020 §2)
  AAD   = tenant_id ‖ slab_id ‖ stats_format_version ‖ schema_generation
  stats_block_hash128 is not AAD because it is computed over the
  ciphertext. The reader verifies the footer HMAC first, then checks
  the footer-pinned stats_block_hash128 before decrypting the CSB; the
  CSB AEAD tag then authenticates the plaintext under the slab DEK.
```

## Rationale

**Why slab-footer stats are the right next ADR.** ADR 038's
column-aware locality narrows the scan to a contiguous arc set;
stats narrow it again, this time *within* each arc, by skipping
slabs whose `min`/`max` bounds preclude the predicate. The two
compose multiplicatively: a 1000:1 locality reduction × 100:1 stats
reduction = 100,000:1 effective scan reduction on a well-clustered,
well-bounded predicate. This is the warehouse unlock.

**Why eager-only computation.** Lazy stats are a footer-mutability
problem. ADR 022 §5 makes the footer HMAC the integrity anchor for
every non-ciphertext byte; in-place stat addition would either break
HMAC validation or require re-signing the footer (rotating slab
identity). Compaction already does the right thing — it produces a
new slab with new stats — so the upgrade path for v1 slabs already
exists.

**Why mandatory always-on for typed tables.** Min/max/null_count are
cheap (a single pass over the batch already in memory) and the
compounding-leverage argument depends on every slab having them.
Making them opt-in creates a configuration where some slabs prune
and others don't, which makes planner cost models unstable.

**Why opt-in for bloom and HLL.** Bloom filters cost ~1.2 bytes per
row at 1% FP — non-trivial for high-cardinality columns. HLL costs
4 KiB per column regardless of row count. Tenants who don't run
equality-predicate workloads shouldn't pay for bloom filters; tenants
who don't need selectivity estimation shouldn't pay for HLL. Both
are workload-shaped enough to deserve descriptor-level opt-in.

**Why no dictionary encoding in V1.** Arrow already has a
first-class `DictionaryArray` type. A tenant who wants dict encoding
declares the column as `Dictionary<KeyType, ValueType>` in the
Arrow schema; the column stores the codes natively, and stats apply
normally. Building a parallel slab-footer dict would either
duplicate Arrow's representation or invent a new one — both are
worse than letting Arrow do its job.

**Why no quantile sketches in V1.** They are the sibling-protocol
surface reserved for partial aggregates. Mixing them into ADR 039
would expand the protocol surface to two: stats consultation +
partial aggregate execution. Keep them separate. Quantile sketches
land cleanly on top of ADR 040 once that ADR opens.

**Why `schema_fingerprint` in the CSB.** Compaction across mismatched
descriptors must fail closed. ADR 037's `schema_generation`
discriminates intent; the `schema_fingerprint` in the CSB
discriminates physical layout. Both are needed because a
`schema_generation` can advance via additive evolution without
changing the CSB layout — but if a tenant hand-rotates a descriptor
in a way that produces the same generation but different bytes, the
fingerprint catches it.

**Why CSB lives inside the encryption envelope.** `offset_index`
leaks record sizes — a structural property of how the slab was
written. Column min/max, small-set dictionaries, and bloom filters
leak the *distribution of user data* in ways that are materially
more sensitive (e.g., min/max on a `customer_id` column reveals the
tenant's user-ID range; small-set on a `region` column reveals which
regions are populated). The DEK unwrap cost on planner consultation
is amortized: any slab whose stats the planner consults is a slab
whose data the planner is about to read or just read; the unwrap
happens once per slab access either way. A separate AAD nonce domain
keeps stats AEAD distinct from chunk AEAD so neither tampering
vector compromises the other.

**Why the footer HMAC excludes the CSB ciphertext.** A natural
instinct is to fold the stats ciphertext into the footer HMAC for
"belt and braces." That would force every point-read tail-fetch to
also fetch the stats ciphertext just to validate the footer. The
point-read path under ADR 022 is deliberately small (one tail GET,
parse footer, stream chunks). Stats may be many MiB; binding them
into the HMAC silently grows the minimum tail fetch. The split
design — footer HMAC covers a 16-byte locator-pinned hash; CSB AEAD
authenticates the bytes when consulted — gives the same integrity
property without taxing queries that don't need stats. The AAD on
the CSB AEAD includes that same locator-pinned hash, so a swapped
CSB ciphertext fails authentication immediately; the two checks
together close the ring.

**Why no sidecar manifest.** A sidecar would be mutable, separately
listed, separately garbage-collected, and vulnerable to object-store
consistency surprises. The stats block belongs in the slab object.
A local parsed cache is fine because it is rebuildable.

**Why `TieredSlab` advertises stats presence but not stats content.**
The control ring is consensus state; replicating per-column stats
would multiply Raft footprint by the number of columns and change
every time a slab is written. The three small fields
(`stats_format_version`, `stats_block_len`, `stats_block_hash128`)
are O(20 bytes) regardless of column count and let a planner skip
the stats fetch entirely when it can prove no predicate is
representable — without that, every cold read against a stats-less
query would still pay one GET to confirm absence. The slab tail
remains authoritative; the ring is a hint.

**Why canonical byte-comparable encoding for stat values.** Range
predicates run inside the storage node on every candidate slab.
Decoding each min/max via Arrow scalar comparators on a hot path
adds branchy code (per-type dispatch) and ties stat correctness to
specific Arrow kernel versions. Pinning a byte-comparable on-disk
form (sign-flipped BE for signed ints, BE for unsigned, etc.)
collapses the comparison to a single `memcmp` and produces stable
golden bytes across writers. Float, decimal, and nested types are
allowed in schemas but produce opaque-tagged CSB entries (§5)
carrying only null/non-null counts, because their byte-comparable
forms have non-trivial edge cases (NaN, signed zero, subnormals,
scale mismatch, nested-element ordering) that need their own
pinning work. The opaque tag avoids inventing a placeholder
encoding that would have to be unwound later.

## Consequences (the negatives)

- **Slab format v1 → v2 is a real format break.** Older readers
  refuse v2; older writers produce v1. The migration story (decision
  §2) is via offline compaction, which has its own operator burden.
- **CSB inflates the tail-window range-GET.** A typed-table slab with
  bloom filters on 8 columns and HLL on 4 columns can carry a CSB of
  several MiB, pushing the tail window beyond ADR 022's 64 KiB common
  case. Cold reads pay an extra range-GET.
- **Compaction CPU rises.** Recomputing stats on compaction output
  is O(rows × stats × cols). For large slabs this is non-trivial.
  Bench gates in implementation.
- **HLL precision is a one-way commitment per descriptor.** Lowering
  precision (more error tolerance) is fine, but raising precision
  requires a rewrite migration to repopulate registers from data.
- **Stats encryption adds per-consultation DEK unwrap.** Mitigated
  by amortization across the slab's data reads (the DEK is unwrapped
  exactly once per slab access regardless of how many stat
  consultations occur), and by the local stats cache (Phase 9) that
  serves repeated consultations without re-decrypting.
- **Truncated string min/max can miss prune opportunities.** A
  predicate `col == "abcdefgh..."` against a slab with truncated
  `min = "abcd..."` can't be safely skipped; the planner must scan.
  Operators sizing the prefix-byte cap shape the trade between CSB
  size and prune coverage.

## Rejected alternatives

- **Lazy stats computation (background pass adding stats to existing
  slabs).** Rejected: incompatible with footer HMAC integrity. Would
  require a separate "stats sidecar" object and an additional read
  path; inverts ADR 022's "one slab, one object" principle.

- **Per-input lineage in the CSB.** Rejected: lineage is forensics,
  not a hot read-path concern. Compaction journal already carries
  it. Adding per-input min/max/bloom history to the slab footer
  triples the CSB size for negligible benefit.

- **Quantile sketches in V1.** Rejected: reserved for the
  partial-aggregate sibling protocol (ADR 040). Bundling here
  expands the protocol surface and conflicts with ADR 040's scope.

- **Dictionary encoding in V1.** Rejected: Arrow's `DictionaryArray`
  already provides native dict at the column level. Adding a
  slab-footer dict either duplicates Arrow's representation or
  invents a new one. If a future workload demonstrates a need for
  slab-level dict beyond Arrow's, that's a separate ADR.

- **Bloom filters always-on.** Rejected: cost is workload-shaped
  (~1.2 bytes/row at 1% FP). Mandatory bloom filters tax tenants who
  don't run equality predicates. Opt-in is the right tradeoff.

- **Min/max on a per-record-batch granularity rather than per-slab.**
  Rejected: per-batch stats would multiply the metadata size without
  improving planner pruning meaningfully (the planner already
  operates at slab granularity in the locality phase). Per-slab is
  the right scope.

- **Plaintext stats block (alongside `offset_index`).** Rejected.
  Plaintext saves a DEK unwrap per planner consultation but leaks
  data-derived distributions (min/max value ranges, small-set
  populations, bloom-derivable presence). The unwrap cost is
  amortized across subsequent reads of the same slab; the leakage is
  permanent.

- **Footer expansion (extending `slab_footer` from 256 B to 1 KiB or
  variable).** Rejected: ADR 022 fixed the footer at 256 B
  specifically to keep the tail anchor compact and parseable. Stats
  grow large enough to need their own variable-length section;
  coexisting in the fixed footer would either crowd out integrity
  fields or require another section anyway.

- **A new "stats version" field separate from
  `slab_format_version`.** Rejected: bump the slab format. Two
  version fields proliferate evolution surfaces; one is sufficient.

- **Toron-driven `slab_skip_query` RPC.** Rejected. Wendelon owns
  slab enumeration and pruning internally; Toron sees a capability
  bit and explain counters, not a wire surface that names
  physical-storage objects.

- **Footer HMAC covers stats ciphertext.** Rejected. The simpler
  integrity story (everything signed by the footer HMAC) silently
  taxes the ADR 022 point-read path: a query that touches no stats
  would still have to fetch the stats bytes to validate the footer.
  The locator-pinned-hash + per-block AEAD split achieves the same
  integrity property without that tax.

- **Replicate full per-column stats in the control ring.** Rejected.
  Per-slab × per-column stats are too large and too numerous for
  Raft state. The three-field presence advisory on `TieredSlab`
  (decision §27) carries enough metadata for planner pre-filtering;
  the slab tail remains authoritative.

- **Skip stats on `ColumnProjection` tables because locality already
  prunes.** Rejected. Locality and stats prune different layers and
  compound well.

## Implementation staging

- **Phase 0 — Contract freeze.** Lock CSB layout, descriptor shape,
  reject codes. Confirm slab_format_version bump and footer-suffix
  locator layout.
- **Phase 1 — CSB writer (always-on tier, plaintext).** Compute
  min/max/null_count/non_null_count/row_count from the in-memory
  batch at slab finalization. Encode the plaintext CSB; encryption
  added in Phase 3. Emit between encrypted_chunks and offset_index.
  Update footer HMAC coverage. Truncation-aware encoding for
  string/binary min/max.
- **Phase 2 — CSB reader (plaintext).** Parse CSB on slab open.
  Validate `schema_fingerprint` and `schema_generation` against the
  descriptor cache. Surface as `SlabStats` accessor. Golden
  byte-layout tests against the §13 fixed header and §14 payload
  encoding.
- **Phase 3 — CSB encryption.** AEAD wrapping under the slab DEK
  with a stats-block-specific nonce domain. AAD per the §"Slab
  footer extension diagram" construction. Update footer suffix
  locator (`stats_block_len`, `stats_plaintext_len`,
  `stats_block_hash128`). Differential test: encrypted CSB
  round-trips match plaintext CSB byte-for-byte.
- **Phase 4 — Bloom filter opt-in.** SipHash-1-3 with two seeds,
  k = 7. Descriptor validation for `ColumnBloomSpecV1`. Bloom filter
  computation in the writer; bloom-membership check API in the
  reader.
- **Phase 5 — Small-set dictionary opt-in.** Capped distinct-set
  builder; emits `Unknown` on cap exceeded. Sorted canonical
  encoding. Descriptor validation for `ColumnSmallSetSpecV1`.
  Membership-check API in the reader.
- **Phase 6 — HLL opt-in.** Standard HLL with linear counting for
  small cardinalities. Descriptor validation for `ColumnHllSpecV1`.
  Compute on write, expose estimate via reader API.
- **Phase 7 — Compaction recompute.** Stats are recomputed from the
  output rows during the compaction pass; never merged from inputs.
  Compaction eligibility check for `schema_fingerprint` match.
- **Phase 8 — Internal scan-plan integration.** Stats consultation
  fires inside Wendelon's existing scan plan, between slab enumeration
  and chunk-stream decryption. Predicate extraction reuses ADR 038's
  column-predicate machinery. Server-side evaluation: min/max bounds
  (truncation-aware for strings), small-set membership, bloom
  membership for equality, null-count short-circuits. No new wire
  surface; ADR 036 sees only an extra capability bit, a diagnostic
  opt-out flag, and the process/internal counters from §21. Counters:
  `pbc_slab_stats_consulted_total`,
  `pbc_slab_stats_pruned_total`, `pbc_slab_stats_missing_total`,
  `pbc_slab_stats_unsupported_total`,
  `pbc_slab_stats_predicate_unrepresentable_total`,
  `pbc_slab_stats_decrypt_failed_total`,
  `pbc_slab_stats_pointer_drift_total`.
- **Phase 9 — Stats cache.** Local parsed-stats cache keyed by
  `(slab_id, payload_hash, stats_block_hash128)`. Counters:
  `pbc_slab_stats_cache_hits_total`,
  `pbc_slab_stats_cache_misses_total`. Histograms:
  `pbc_slab_stats_build_micros`, `pbc_slab_stats_decrypt_micros`,
  `pbc_slab_stats_block_bytes`. Cache is rebuildable from slab tail;
  not authoritative.
- **Phase 10 — `TieredSlab` advisory fields.** Add the three
  optional presence-advertisement fields to the tiered-slab pointer
  schema; backfill via a no-op control-ring migration that leaves
  them `None` for legacy entries. Verify drift handling raises the
  metric.
- **Phase 11 — Format-v1 → v2 offline migration tool.** Reads v1
  slabs, recomputes stats, writes v2 slabs under a new
  `schema_generation`. Audit record per ADR 025.
- **Phase 12 — Backward compat tests.** v1 readers refuse v2.
  v2 readers handle v1 by returning `STATS_UNAVAILABLE`.
- **Phase 13 — Enable stats-driven pruning.** Capability bit
  advertised; per-tenant rollout via configuration. Metric-tracked.

## Deferred decisions

1. **Default tail-window size.** ADR 022 fixed at 64 KiB. With CSB
   present, the right default may shift up (256 KiB?) to keep the
   common-case "tail-only" read at one RTT. Tune from benches once
   real CSB sizes are measured.

2. **Bloom filter for OR/IN-list predicates.** Phase 4 ships bloom
   checks for equality only. `col IN (a, b, c)` requires k tests
   against the bloom; not technically harder, but the planner
   contract has to specify the cost model.

3. **Stale-stats refresh policy.** A slab whose data hasn't changed
   but whose descriptor's stats policy has changed (e.g., tenant
   added a bloom filter on a column post-write) carries old stats.
   Should the system run a background recompute pass, or wait for
   the next natural compaction? Defer; the natural-compaction path
   is correct, the question is whether to accelerate it.

4. **HLL serialization compatibility.** ADR 039 ships standard HLL
   with linear counting. If a future workstream wants HLL++ or
   sparse representation for low-cardinality slabs, the descriptor
   precision field can extend; the wire format adds a flag bit in
   the CSB column block. Defer.

5. **Per-tenant stats budget.** A tenant with 1000 columns and
   bloom-on-all could push a single slab's CSB toward the cap.
   The cap exists; whether to charge tenants in storage-cost
   accounting is operational, not architectural.

6. **Strict-diagnostics mode for stats failures.** Default behavior
   treats `STATS_DECRYPT_FAILED` and `FINGERPRINT_MISMATCH` as
   `STATS_UNAVAILABLE` for query correctness while raising metrics.
   A future operator/debug session flag could turn stats corruption
   into an explicit query diagnostic. Deferred pending real operator
   demand.

## Appendix A: PBC reject codes

Extends ADR 037 §"Appendix A" and ADR 038 §"Appendix A" consistently
with ADR 036's "no free-form error strings" rule:

- `PBC_REJECT_STATS_UNSUPPORTED_TYPE` — column type cannot carry
  the declared stat (e.g., bloom or small-set on Float).
- `PBC_REJECT_STATS_DUPLICATE_COLUMN` — same column declared in
  multiple specs *within the same stat kind*.
- `PBC_REJECT_STATS_COLUMN_UNKNOWN` — a stat policy entry names
  a column that is absent from the descriptor's Arrow schema at
  the named `schema_generation`.
- `PBC_REJECT_STATS_BUDGET_EXCEEDED` — descriptor's worst-case
  per-slab CSB ciphertext size at declared row capacity exceeds
  `slab_payload_bytes / 256`.
- `PBC_REJECT_STATS_PRECISION_INVALID` — HLL precision outside
  `10..=14`.
- `PBC_REJECT_STATS_BITS_INVALID` — bloom `bits_per_row` outside
  `1..=32`.
- `PBC_REJECT_STATS_SMALLSET_INVALID` — `max_distinct_values`
  outside `1..=4096` or `max_bytes` outside `1..=32_768`.
- `PBC_REJECT_STATS_FORMAT_VERSION` — slab declares
  `slab_format_version = 2` but the CSB layout is unparseable.
- `PBC_REJECT_STATS_FINGERPRINT_MISMATCH` — CSB `schema_fingerprint`
  does not match the descriptor at the named `schema_generation`.
- `PBC_REJECT_STATS_DECRYPT_FAILED` — CSB AEAD authentication failed
  after footer HMAC succeeded (suspected tampering; surfaced via
  metric `pbc_slab_stats_decrypt_failed_total` and treated as
  `STATS_UNAVAILABLE` for query correctness).

## Appendix B: Arrow type tag mapping

Reserved enum for the `arrow_type_tag` byte in each CSB column
block. Stable across the Wendelon version family; new types append,
never renumber.

```
0x00  Reserved (must not appear)
0x01  Int8
0x02  Int16
0x03  Int32
0x04  Int64
0x05  UInt8
0x06  UInt16
0x07  UInt32
0x08  UInt64
0x0E  OpaqueUnsupported        (writer-known, no canonical stats —
                                see §5; payload carries only null
                                and non-null counts)
0x10  Utf8
0x11  LargeUtf8
0x12  Binary
0x13  LargeBinary
0x20  Timestamp(Millisecond, _)
0x21  Timestamp(Microsecond, _)
0x22  Timestamp(Nanosecond, _)
0x30  Date32
0x31  Date64
0x40  Boolean
0xFF  Reserved (must not appear)
```

All other tag values (including `0x09`–`0x0D`, `0x0F`, the gaps
inside the `0x14`–`0x1F`, `0x23`–`0x2F`, `0x32`–`0x3F`,
`0x41`–`0xFE` ranges) are unassigned. A reader that encounters an
unassigned tag treats the column entry as malformed and falls back
to `STATS_UNAVAILABLE` for that column. New types append to the
mapping; tags are never renumbered.

## References

- ADR 005: Decoupling Compute via Pushdown Bytecode (PBC)
- ADR 020: L2 Envelope Encryption and DEK Lifecycle
- ADR 022: L2 Slab Format and Footer-Indexed Range Reads — this ADR
  extends its slab layout.
- ADR 023: Multipart Upload and Compaction Lifecycle — interacts
  with stats recomputation on compaction output.
- ADR 025: Audit Event Model — stats-format-version migration audits
  here.
- ADR 032: Schema Evolution and Schema-Ring Contract — descriptor
  evolution rules carry over.
- ADR 034: Toron-Wendelon Integration Contract.
- ADR 036: PBC Pushdown Contract — this ADR extends it with the
  `slab_stats_pruning_v1` capability bit and explain counters.
- ADR 037: Tenant Table Schemas, Row Identity, Table-Scoped Locality.
- ADR 038: Column-Aware Locality V1 (Z-Order) — composes with stats
  for multiplicative scan reduction.
- ADR 040 (future): Partial Aggregate Pushdown Sibling Protocol —
  quantile sketches and aggregation primitives that ADR 039
  deliberately excludes.
