# ADR 481 implementation receipt — m05 GroupTable diagnosis (no code change)

Diagnosis only, per the discipline: establish m05's owner independently from m04's
string-key path, on the current (479) binary, before any mechanism is selected.
Two owners, both structurally distinct from m04.

## Profile (current binary, m05 = `GROUP BY l_orderkey`, 12M Int64 groups, sf=8)

Symbolized self-cost (harness `stable_file_hash` excluded):

- `group_table::find_bucket_in` — **28.6%** (confirmed self-cost).
- A ~29% cluster of unattributed kernel `[unknown]` frames. These are *consistent
  with* locality pressure from a table too large to stay resident, but do not by
  themselves prove TLB misses or page faults — naming the kernel component would
  need `perf stat` hardware counters (cache-misses, dTLB-load-misses). Recorded as a
  hypothesis, not a measured cause.
- `quicksort` — **11.2%**, with `finalize_bound_grouped_aggregate` alongside.
- `fold_bound_grouped_aggregate` 3.3%; the 479 partition fast path 2.8% (probe-side
  repartition, not grouping).

## Owner 1 — the probe over scattered-heap keys (primary, addressable)

`find_bucket_in` (`group_table.rs:634`) open-addresses over
`keys: &[Vec<GroupValue>]`. Each group key is its **own heap `Vec<GroupValue>`**,
even for m05's single Int64 key — so 12M groups are 12M scattered small allocations.
Every probe both `hash_one`s an enum slice and, on collision, chases
`keys.get(entry)` into a separate heap `Vec` to compare. At 48M probe rows against a
12M-entry table this is cache/TLB-miss-bound — the kernel cluster above is that cost.

This is **not** m04's owner. m04 (3 groups) has a trivially small table; its cost was
per-row `GroupValue::Utf8(String)` allocation and `group_value` dispatch (476/477).
m05 allocates no per-row string and its table is enormous — the owner is key
**storage layout and probe locality**, a different mechanism entirely. Establishing
that independence was the point of profiling m05 rather than assuming m04 transfers.

## Owner 2 — the finalize sort (contract-bound, not free to remove)

`finalize_sorted` (`group_table.rs:238`) unconditionally
`sort_unstable_by(key.cmp)` over all groups — 11.2% at 12M groups. This is **not**
dead work: ADR 002 pins GROUP BY output ordering as *stable by lexicographic
group-key ordering*, and parity/digest tests depend on it. Like ADR 480's
partition-hash stability contract, this cannot be dropped for speed without a
deliberate decision about the output-order contract and every digest that rests on
it. A cheaper *stable* ordering (e.g. radix/partial sort exploiting fixed-width
keys, or ordering during finalize rather than a general comparison sort) could
preserve the contract — but that is a mechanism for a later cut, not this diagnosis.

## Conclusion (scoped, no mechanism selected)

m05's owner is the group-table **probe over a `Vec<Vec<GroupValue>>` key store**
at high cardinality (confirmed: `find_bucket_in` self-cost + scattered per-key heap
allocations; locality pressure is the leading but hardware-counter-unconfirmed
explanation for the kernel frames), with a secondary contract-bound finalize sort. Neither is m04's string-allocation/dispatch path. The addressable primary is
the key storage/probe layout — specifically a fixed-width single-key store/probe that
avoids the per-key heap `Vec`, **preserving ADR 002's sorted-output contract**. The
sort is a separate contract question handled with 480-style care. The mechanism cut
is profile-first and A/B-under-settled-load as established; `perf stat` counters on
m05 would additionally confirm the locality hypothesis before or alongside it.
