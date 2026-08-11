# ADR 0083 aggregate-core implementation receipt — 2026-08-11

This landing repairs the shared quantile sketch, the aggregate-state carrier, and the unsafe
version-1 fact boundary. It deliberately does **not** claim ADR 0083 sealed: CSB version 2, its
dual reader/writer, the mixed-fleet writer fence, the rebuild inventory and driver, and the
ordered multi-block fact/plan-shape receipt remain open.

## Implemented contract

- `KllSketch` retains fixed-width canonical values in weighted levels. Rank selection walks
  cumulative level weights; the discarded equal-weight retained-index mapping is absent.
- The aggregate configuration is fixed at epsilon 1%, `k=800`, and at most 5,601 retained
  values. Tests charge actual per-level capacity and cover sustained insertion and repeated
  merge.
- Compaction uses the execution seed, a 32-byte sketch identity, level, and per-level counter.
  Pair merge reconciles seed, identity and counters commutatively. Equal-height samplers use the
  pre-merge `(identity, seed)` ordering required by the ADR.
- Serialized aggregate state carries levels, seed, identity, counters, and sampler item, weight,
  and height. Decoding checks that weighted levels plus sampler weight equal
  `total_item_count`; malformed state refuses.
- Legacy uncompacted aggregate state remains readable as exact level zero. A compacted legacy
  state whose discarded weights cannot reconcile to its population refuses rather than being
  reinterpreted.
- The KLL aggregate parameter now requires `k` plus the coordinator-issued 256-bit execution
  seed. A legacy `k`-only request is rejected, so no predictable-randomness path silently
  acquires the new guarantee.
- CSB version 1 still preserves min/max, counts, bloom, HLL, small-set and heavy-hitter facts, but
  no longer constructs, writes, recovers, or publishes a range-distribution fact. Absence is the
  safe state until version 2 has its own confidence and fleet contract.

## Discriminating controls

- The same exact population under ascending, descending, shuffled, all-identical and two-valued
  arrival is checked at p1/p25/p50/p75/p99 against the selected one-percent rank bound. The old
  ascending and descending results (899 and 100 for a true median near 500) fail this corpus.
- Every tested merge tree is checked against the exact population independently. Reverse
  receiver order must serialize identically, including the equal-height/different-sampler-item
  case that isolates the tie-break.
- Distinct sketch identities produce distinct compaction streams at identical coordinates.
- A valid state round-trips every randomness and sampler field. A fixture changing only the
  advertised population count is rejected by the weight identity.
- Version-1 writer and recovery fixtures prove range-distribution absence while unaffected
  statistics survive, both with a zero ciphertext budget and an unbounded one.

## Open before ADR 0083 can seal

1. Record the ordered multi-block fact and representative binder plan/timing comparison required
   by Stage 0 item 2. The safe no-facts interval is active; this receipt does not invent the
   missing pre-suppression measurement.
2. Inventory legacy payloads and their rebuild sources.
3. Implement CSB v2, choose and enforce the mixed-fleet writer fence, and add the lazy rebuild
   path.
4. Pin CSB v2's own confidence scope; it cannot inherit the aggregate execution-wide delta.

The aggregate core is implemented and code-gated. ADR 0083 remains proposed and partially
implemented until those storage-migration obligations land.

## Verification

- `scripts/cargo-cgroup test --workspace --no-fail-fast --quiet` — exit 0.
- `scripts/cargo-cgroup check --workspace --all-targets` — exit 0, zero warnings.
- `cargo fmt --all -- --check` and `git diff --check` — clean.
