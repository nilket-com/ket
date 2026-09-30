# ADR 621 - inline fixed-width keys for generic in-memory join builds

**Status:** PROPOSED. Successor to ADR 620. One token, measured by a
controlled pair. **No saving is projected, no new board total is
claimed, and no general claim is made for all fixed-width keys before
the breadth evidence.**

ADR 620 audited the scaling cell's replay-2 join (TPC-H query nine's
composite-key build of `partsupp`) at SF8:
- **The representation is verified.** It builds `partsupp` with the keys
  (`ps_suppkey`, `ps_partkey`), both Int64 and non-nullable, not
  null-safe. The key plan is `Generic`: 6,400,000 rows under 6,400,000
  distinct keys, with 0 duplicates.
- **The envelopes:** table build 789.3 ms, probe 1,228.7 ms, and
  cleanup 556.6 ms. Build plus cleanup has a per-run median of
  1,345.2 ms.
- **The synthetic paired differences:** owned − inline was 198.3 ms at
  build and 461.0 ms at drop.
- **The provisional candidate is O1, build-key storage only.**


## Context

**The storage today** (`exec/join/build_rows.rs`):
- A `Generic` plan builds `JoinBuildRowsInner::Hash(HashMap<JoinKey,
  BuildRowList, RandomState>)`. Each accepted build row's key is encoded
  by `join_key_bytes` into a new `Vec<u8>` (18 bytes for two Int64
  columns, with a 32-byte capacity).
- A distinct key keeps that buffer; a duplicate row's temporary buffer is
  freed at insertion. The table's drop frees one buffer per entry.
- **The hash input is `JoinKey`'s derived `Hash`:** the enum
  discriminant, then the `Vec<u8>` payload (its length-prefixed byte
  slice).

**It is contained** (a read-only code audit):
- **One construction point.** Every in-memory `Generic` build goes
  through `build_in_memory_join_rows_with_insert_mode`
  (`build_rows.rs:219`), and chooses its representation at lines
  227–231. Spilled builds use their own maps (`spilled_hash.rs`,
  `spill.rs`), not `JoinBuildRows`.
- **The readers:**
  - `insert`, `get(&JoinKey)` (and the `get_int32`/`get_int64`
    fallbacks), `values`, `len`, `capacity`, `accounted_bytes`, and
    `profile_shape`;
  - the build profile, via `values` and `len`;
  - `accounted_bytes` at `streaming.rs:4887` and `:4922`.

  Only `accounted_bytes` reads the stored keys, for their capacity.
  Unmatched-build tracking is a bitmap over build rows, independent of
  the table. There is no custom `Drop`, and no key iteration.
- **The `Generic` lookups:**
  - `join.rs:2616`, the main probe (inner and outer kinds);
  - `streaming.rs:15773`, the left-outer probe;
  - `streaming.rs:25945`, the scalar rows;
  - `streaming.rs:27801`, semi/anti existence.

**The encoder's non-null widths** (`exec.rs` `encode_partition_key_value`:
a tag byte, then the payload):

| width | types |
|---|---|
| 2 | Boolean, Int8, UInt8 |
| 3 | Int16, UInt16 |
| 5 | Int32, UInt32, Float32, Date32 |
| 9 | Int64, UInt64, Float64, and Timestamp in Millisecond, Microsecond, or Nanosecond **with timezone "UTC" only** |
| 17 | Decimal128 |
| variable | Utf8 |

**Unsupported by the encoder:** Timestamp in Second, with no timezone,
or with any other timezone name; and every other type.

A null value encodes as a one-byte marker at the encoder level, but this
non-null-safe join skips null keys before encoding (`join_key_bytes`
returns `None`).


## Decision

*The token.* `GenericBuildKeyStorage { Owned, Inline }`, a `const`
`GENERIC_BUILD_KEY_STORAGE` in `build_rows.rs`, with a commented
`#[allow(dead_code)]` as for earlier tokens.
- `Owned` is today's behaviour.
- `Inline` applies only to a `Generic` in-memory build whose plan is
  **not null-safe** and whose key columns are **all encoder-fixed-width**
  (by the helper below), with a total non-null encoded width of **at most
  32 bytes**. Any other build stays `Owned` under both settings.

*The hash-input contract: preserved.* `InlineJoinKey` holds a length and
a `[u8; 32]`. Its `Hash` feeds exactly what `JoinKey::Generic`'s derived
`Hash` feeds: the `Generic` discriminant, then the byte slice
`&bytes[..len]` as a `Vec<u8>` would hash it.
- **A fixture proves the equality under a shared seed:** `hash_one(&JoinKey::Generic(b))
  == hash_one(&InlineJoinKey::from(b))`, for every eligible width.
- **So the pair holds the hash input and function unchanged.** It
  measures key storage, the table's entry layout, and the lookup
  adapter.

*Equality.* `InlineJoinKey`'s `Eq` compares `len` and `bytes[..len]`
only, as a manual implementation. Every constructor zero-fills the
unused tail, so equal prefixes are equal regardless of padding.

*What `Inline` changes, and only this:*
- **A new representation,** `JoinBuildRowsInner::InlineGeneric(HashMap<InlineJoinKey,
  BuildRowList, RandomState>)`, with the same capacity policy as the
  owned map.
- **The build** encodes each accepted row's key with the production
  per-column encoder into one reused scratch buffer, then copies it
  inline. The encoding is **bit-for-bit production's:** float bits,
  including NaN payloads and signed zero; decimal bits; and timestamp
  tags. No canonicalization is added.
  - Nulls follow the non-null-safe rule: skipped, as today.
  - Duplicates append to the entry's row list in insertion order, as
    today.
  - Errors, null skipping, and the existing build memory-cap estimate
    check are unchanged.
- **The lookup adapter.** `get(&JoinKey::Generic(bytes))` on the inline
  table builds a **stack** `InlineJoinKey` from the probe's
  already-allocated bytes (no new allocation), and looks it up.
  - A probe key longer than 32 bytes misses. A key of a different length
    misses by equality.
  - Every other `JoinKey` variant, and the `get_int32`/`get_int64`
    fallbacks, return `None`, as a `Generic` table does.
- **The probe is unchanged in both arms.** Each probe row is still
  encoded by `join_key_bytes` into a fresh `Vec` and wrapped as
  `JoinKey::Generic`.
- **The other readers** get their inline arms: `values`, `len`,
  `capacity`, `profile_shape` (label `generic_inline`), and
  `accounted_bytes`.
  - **Accounting is engine accounting,** not allocator or RSS
    measurement. It uses the map's actual capacity, `size_of` the key
    and value with alignment, plus each row list's heap capacity: the
    owned formula without the per-entry key buffers.

*The width helper,* `encoded_join_key_width(&DataType) -> Option<usize>`,
beside the encoder, gives the **non-null** encoded width.

*The join-build profile gains two fields, in both arms* (a
behaviour-neutral telemetry addition):
- `representation`, from `profile_shape`;
- `inline_eligible`, the helper's verdict for the build.

That lets the gate match builds across the arms and see which change.

*Fixtures* (under both token settings where they apply):
- **The hash and equality contract:**
  - hash equality with `JoinKey::Generic` under one seed, for every
    eligible width;
  - equal prefixes with differing unused padding are equal;
  - a different length, type tag, or byte is unequal.
- **Result parity, Owned against Inline:**
  - on every path that uses the table: inner, the outer kinds (with
    unmatched-build tracking), semi/anti existence, and scalar rows;
  - with mixed-width composite keys (Int64 + Int32, Int64 + Decimal128,
    three Int64 columns, and float keys with NaN payloads and signed
    zeros);
  - with duplicates (row-list order), overflow row lists, misses, empty
    builds, and all-null builds.
- **The width helper against the encoder:**
  - for every supported type, the helper's width equals the encoder's
    actual output length;
  - the rejected Timestamp forms (Second, no timezone, another zone)
    and Utf8 return `None`.
- **Eligibility:**
  - a total width of exactly 32 bytes is inline, and 33 stays owned;
  - null-safe and variable-width builds stay owned.
- **Accounting:** `accounted_bytes` for the inline form, with duplicates
  and map growth.
- **The benchmark's receipts:** ADR 619 and 620's `generic_hash`
  receipts stay valid under `Owned`, and are named per token.

*The spill policy, one rule everywhere.* `accounted_bytes` feeds
admission (`streaming.rs:4887`, `:4922`), so `Inline` can change
reservation, admission, or spill. **Any difference in spill status
between the arms, in either direction** (a spill appearing or
disappearing), or any refusal present in only one arm, **fails the
gate, blocks retention in a pair, and opens a new record.** Status,
refusal, and spill fields are fail-closed: unreadable is a failure.

*The gate, before any pair.* One run of each of the 22 cells per arm,
at ADR 613's coordinate, with the join-build profile armed. It fails
closed unless:
- every digest and plan fingerprint equals ADR 613's sealed cell;
- the spill policy holds on every row;
- **the census holds:**
  - **the instance identity:** the ordinal in the run's profile is an
    ordering check only. The ordered profile sequence must correspond to
    the verified route and build topology. Where repeated signatures or
    incomplete coverage make the association ambiguous, that instance is
    marked **unknown**, and is not used as a validated breadth instance.
    Nothing is paired silently by position when identity is not
    demonstrated. **q09's replay 2 keeps ADR 620's current-plan
    association** (the verified route mapping and the `partsupp` build);
  - **the identity and eligibility fields must agree** across the arms:
    `replay_id`, `key_plan`, build rows, distinct keys, duplicate keys,
    and `inline_eligible`;
  - `representation` may change from `generic_hash` to `generic_inline`,
    and only for records with `inline_eligible: true`;
  - other fields may differ and are reported, not gated: accounting
    bytes, representation-dependent capacity and layout facts, and the
    measured build duration. The spill policy above applies unchanged;
  - **q09's replay 2 must show that transition,** and keep 6,400,000
    rows, 6,400,000 distinct keys, and 0 duplicates;
  - a cell whose builds are not all covered by profile records is
    **unknown**, not "unchanged".

**Breadth cells** are those with a validated changed instance.

*The pairs.* ADR 597's driver, bound to ADR 613's sealed board, with
ADR 592/593/595/599, one attempt each. Peak RSS, with its label, and
spill are reported beside each verdict.
1. **q09 decides retention** by ADR 588's rule.
2. **Each breadth cell** is paired under ADR 607's interpretation and
   its negative-pair and noise rule, verbatim. **A breadth regression is
   prominently recorded and opens a new record** (as ADR 610 and 611
   did). It does not block retention, and does not silently revert.
3. **If the census names no breadth cell,** q01 is paired as a
   no-change control. It is justified by its **join-free plan**, not by
   an absence of profile records. A regression there is reported, not
   interpreted.

*The mechanism, after the verdicts.* Five armed q09 runs per arm, with
node attribution and the join-build profile only (no cap-check
receipt). Each run is gated as ADR 620's envelopes were:
- identity (status, digest, fingerprint, dataset, coordinate);
- route and stage topology;
- the node tree reconciling;
- top-level and refinement reconciliation, with strict-incomplete named;
- replay 2's profile facts (representation per arm, 6.4 M / 6.4 M / 0,
  `build_micros`) and its required stages.

A missing required field withholds the run. The report gives per-run
data and each arm's distribution: `prep_table_build`, `suffix_probe`,
and `cleanup_build`. They are **diagnostics,** not a paired allocation
decomposition.


## Acceptance

- The code under both token settings, and the fixtures above.
- Ladders green on bigfoot for both trees, with fmt clean and release
  builds warning-free under both settings. The treatment diff is the
  token line alone.
- The gate with its census, the q09 pair and verdict, the breadth or
  control pairs, and the mechanism runs.
- Plan commit, the implementation arms (control and treatment), then
  one implementation closing commit.


## Guardrails

- Only `Generic`, non-null-safe, all-fixed-width, at-most-32-byte
  in-memory builds change. Every other build, the probe's encoding and
  allocation, and the hash input are unchanged in both arms.
- The digest gate on all 22 cells and on every paired row.
- ADR 588's rule, the twelve-row contract, and ADR 599's gate, verbatim.
  One attempt per cell.


## Risks

- **Admission.** A smaller accounted size can change admission or spill.
  The spill policy fails closed on any difference.
- **Lookup layout.** The inline entry (32 key bytes and a length) is
  larger than the owned entry (a `Vec` header), so the table's memory
  access pattern changes. The pair measures the whole effect.
- **Hash state.** The seed is random per build, as today. Result order
  depends only on probe order and row-list order, so digests are
  unaffected.
