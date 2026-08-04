# ADR 484 implementation receipt — MEASURED WIN, KEPT

The power-of-two partition mask is **10.9% faster on m06** under a clean matched-build
A/B, contract-safe by construction. Kept. The 483 diagnosis flagged this as possibly a
wash; the A/B corrected that estimate upward — the runtime u64 modulo was ~11% of
m06's wall, not wrapper noise.

## The change

`partition_index_for_hash(hash, count)`: for a power-of-two partition count,
`hash % count == hash & (count - 1)` **exactly**, so bucket assignment and the ADR 480
partition contract are unchanged — only a runtime u64 division becomes an AND.
Non-power-of-two counts keep the modulo. The XxHash3 input bytes are untouched; this
is purely the post-hash reduction. m06's spilled join uses a power-of-two count
(`checked_next_power_of_two`), so it takes the mask path.

## Correctness (bucket identity is the gate)

`adr484_mask_equals_modulo_for_power_of_two_and_preserves_bucket_identity` proves
mask == modulo across a range of hashes for counts 1..256, and that non-power-of-two
counts stay on modulo. Because the reduction is identical for power-of-two counts,
partition assignment — hence which rows meet in the join — is unchanged. Confirmed by
the **armed 22-query parquet correctness sweep (22/22)**, spill tests 69/69, full
engine suite, architecture 2,093/2,093.

## Matched A/B (ADR 479 method, m06, sf=8, settled load ~0.9)

Baseline (483 engine) saved before any edit; candidate committed before its sibling
was built; both release, identical flags, identical size. 12 alternating paired rounds:

| metric | 483 baseline | 484 mask |
|---|---:|---:|
| median execute | 23,480 ms | 20,447 ms |
| paired median reduction | | **2,550 ms (10.9%)** |
| pair signs | | **12/12 faster** |
| worst-case delta | | -1,919 ms |

## Scope held

This is the bounded, contract-safe micro-op ADR 483 authorized — not a resolution of
m06's gap. m06 is now ~20.4 s vs SQLite's 6.5 s (indexed nested-loop): still ~3.1×,
because the frozen partition hash (ADR 480) and the spilled-repartition-both-sides
strategy remain. The larger m06 lever is the budget/strategy question — why a 96 MB
Int64 build spills under a 64 MiB task budget — which stays a separate, deliberate
decision (ADR 454-459 territory), untouched here.
