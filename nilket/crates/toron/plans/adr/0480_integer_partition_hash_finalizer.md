# ADR 480 - direct integer finalizer for fixed-width partition hashing

m06 re-profiled **on the 479 build** (profile-first, since 479 shifted every share).
Partitioning is still the top cost, now hash-bound: `fixed_width_partition_hash`
15.2% self, its wrappers ~10% more, ~25% of m06 total. The fast path still builds a
9-byte buffer and runs `XxHash3_64::oneshot` on it — and XxHash3's oneshot carries
fixed setup/dispatch overhead that dominates for a tiny input, per row, ~60M times
(12M build + 48M probe). Replace it with a direct integer finalizer on the raw value.

## The change and its real work removed

For a single Int32/Int64 partition key, compute the hash as a multiplicative-mix
finalizer of the raw integer (a splitmix64/fibonacci-style `wrapping_mul` +
xor-shift), with **no buffer** and **no XxHash3 invocation**. This removes both the
stack-buffer build and the general-purpose hash machinery — genuine work, in the
479 sense, not a cheap-op wrapper (the 478 anti-lesson).

## The blast radius this cut must contain

This changes the **partition assignment** for integer-keyed joins — the highest-risk
change of the session. Two properties must hold:

1. **Consistency (correctness).** Build and probe rows for the same key must land in
   the same partition. They both call this one function, so any deterministic hash is
   internally consistent — a lost-match bug would surface as a wrong answer in the
   armed sweep. This is the same guarantee 479 relied on; the equivalence-to-generic
   property is *dropped* (it was sufficient, never necessary), so the fast path no
   longer has to match the generic byte encoding.
2. **Distribution (performance safety).** A poor hash over dense sequential keys
   (`l_orderkey` is near-sequential) would pile rows into few partitions, imbalancing
   spill and *slowing* the join. A multiplicative finalizer is chosen precisely
   because it scrambles sequential integers well; a `value % partition_count` would
   not and is rejected. A test asserts near-uniform partition occupancy for
   sequential and strided key ranges.

## Decision

1. `fixed_width_partition_hash` (Int32/Int64, single column) returns the finalizer
   mix of the raw value; the generic serialize-then-hash path is unchanged for every
   other shape.
2. The 479 equivalence test is replaced by (a) a determinism test (same value → same
   hash, always) and (b) a distribution test (sequential and strided i64 ranges spread
   near-uniformly across representative partition counts).
3. Nothing else changes: partition count, spill format, the within-partition index
   hash (`ahash::RandomState`), page layout, join algorithm — all untouched.

## Acceptance

- **Armed 22-query parquet correctness sweep green** — the gate that catches a
  mis-partitioned join losing matches. Full engine suite and architecture green.
- Determinism and distribution tests green.
- **A/B under settled load, ≥12 paired rounds, matched build flags** (479's
  sharpened method): commit first, alternate 479-tip vs 480 on m06 back-to-back,
  verdict is the paired median with unanimous direction required to keep. The first
  noisy batch, if any, is disclosed and discarded, not pooled.
- Full ladder after, to confirm no untouched primitive moved.

## Guardrails

- **Distribution is a correctness-of-performance gate**: if the A/B shows m06 slower,
  the likely cause is partition imbalance from the new hash, and the cut reverts.
- Commit before building the sibling A/B binary (478 lesson).
- No claim that this closes m06; it removes one more slice of the partition term.

## Forward

If kept, the partition wrappers' own overhead (the per-row `len`/`is_null` re-checks
that a batch-resolved reader would hoist, à la 475/477) is the next candidate — but
measured, not assumed, and only if the profile still points there.
