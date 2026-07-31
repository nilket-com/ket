# ADR 479 implementation receipt — MEASURED WIN, KEPT

Profile-directed (not guessed), correctness-proven, and measured **~4.2% faster on
m06** under a clean paired A/B. Kept.

## Profile first (ADR 478's lesson)

m06 profiled before any code change. The owner is partition-key hashing, not the
join proper: `partition_bucket_with_scratch` + `partition_bucket_if_non_null_with_
scratch` ~24% of self-time, `encode_partition_key_value` ~7%; hash lookup and
match-set building barely register. For a single Int32/Int64 key the path serialized
a tag byte + LE bytes into a scratch `Vec<u8>` and hashed the 9-byte buffer per row.

## The change

`fixed_width_partition_hash`: for a single-column Int32/Int64 key, hash the raw value
directly with the **same tag + LE-byte encoding** the generic path uses, so the hash
is byte-identical — a pure shortcut, not a new scheme. Lives inside the shared
partition hash functions, so build and probe rows are consistent by construction.
Every other shape (multi-column, string, decimal, float, null) keeps the exact
generic path.

## Correctness (the gate that matters for a partition change)

`adr479_fixed_width_fast_path_matches_generic_encoding` proves fast == generic hash
across MIN/-1/0/MAX for both widths and that multi-column keys decline the fast path.
Because the hashes are identical, partition assignment is unchanged, so build/probe
still meet. Confirmed by the **armed 22-query parquet correctness sweep (22/22)** —
which is what would catch a join silently losing matches. Full engine suite and
architecture green.

## The A/B (ADR 478 method, sharpened)

Candidate committed **before** building the sibling binary (478's process failure,
avoided). Baseline (478-tip) and 479 binaries built with **identical flags** (a first
479 build accidentally linked polars and was discarded), alternated back-to-back
under shared load.

**A first six-round attempt was invalid and is not data.** It ran through a load
transient (~2,500 ms round-to-round swings that dwarfed the effect), 4/6 favoring
479. It is disclosed here as a discarded attempt, not pooled with the verdict.

**The verdict is the twelve-round matched-build cohort under settled load, alone and
independently sufficient**: base median 26,624 ms, 479 median 25,628 ms, **all 12
paired deltas negative** (-200 to -1,353 ms), median **-1,130 ms (4.2%)**, no sign
flips.

## Outcome

Kept. The 4.2% is a modest, appropriately-scoped profile-to-wall translation: the
fast path removes **one** component of partitioning — the per-row buffer build — for
fixed-width single-key joins, not all partition work, and not the rest of m06.
m06's remaining cost (the `lookup` closure, `SpilledBatchIndex::insert`, page
fetch) is the next target — and the next cut also profiles first, then A/Bs under
settled load with enough rounds to clear variance, since m06's per-run noise can
exceed a single mechanism's effect.
