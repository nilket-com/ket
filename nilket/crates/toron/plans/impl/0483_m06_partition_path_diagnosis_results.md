# ADR 483 implementation receipt — m06 partition-path diagnosis (no code change)

Diagnosis only, profile-first on the current (482) binary. Unlike m05, m06 has **no
single clean win**: its dominant cost is contract-frozen, and the addressable surface
is a set of small, separately-owned slices. This receipt maps them and classifies
candidates by risk; it selects no mechanism.

## Profile (current binary, m06 = `lineitem ⋈ orders` on Int64 key, sf=8)

Symbolized self-cost (harness `stable_file_hash` excluded), aggregated view:

- `fixed_width_partition_hash` — **17.55%**
- `partition_bucket_if_non_null_with_scratch` — 6.52%
- `partition_bucket_with_scratch` — 6.30%
- `SpilledInnerJoinBuild::lookup` closure — 6.22%
- `SpilledBatchIndex::insert` — 5.24% (build-side)
- `repartition_exchange_batch` — 4.02%
- arrow/parquet decode (`next_buffer`, `rle`) + `lz4` decompress — ~9% combined

Partitioning is ~34% of m06; the join build/probe (`insert`, `lookup`) and
columnar decode make up most of the rest.

## The dominant owner is contract-frozen (ADR 480)

`fixed_width_partition_hash`'s 17.55% is overwhelmingly the `XxHash3_64::oneshot`
call plus the buffer the encoding requires — and ADR 480 established that this hash
**output** is a distributed compatibility contract (exchange producers/consumers must
agree on partition assignment). It cannot be swapped for a faster hash without a
deliberate versioning/rollout decision. So the single biggest slice of m06 is
**off-limits to a perf pass**, which structurally caps how much m06 can move here.

## Addressable, contract-preserving candidates (classified)

1. **Power-of-two modulo → mask** (leading candidate — real work, low risk).
   The wrappers apply `hash % u64::from(partition_count)`, a runtime u64 modulo
   (~20-40 cycles/row). m06's spilled join uses a power-of-two partition count
   (`checked_next_power_of_two`, default 4). For power-of-two `pc`,
   `x % pc == x & (pc - 1)` **exactly** — identical bucket assignment, so both the
   hash output and the partition contract are preserved; only a division becomes an
   AND. Whether the ~12.8% wrapper time is modulo-bound or call/Option overhead is a
   hypothesis the A/B must confirm — it may be a wash.
2. **Batch-resolve the per-row downcast** in `fixed_width_partition_hash` (478-risky,
   small headroom). The per-row `data_type()` match + `downcast_ref` is a tiny
   fraction of the 17.55% (the XxHash3 call and buffer dominate), and ADR 478 showed
   that hoisting a single downcast can *regress*. Low priority.

## The structural ceiling (SQLite lens)

m06 is Toron 22.5 s vs SQLite 6.5 s — the widest gap on the ladder, and SQLite used
an **indexed nested-loop** (probe a B-tree, no build materialization, no shuffle).
Toron's cost is inherent to its strategy: a spilled partitioned hash join that
partition-hashes and shuffles **both** sides (48M probe + 12M build rows). The ~34%
in partitioning **is** that strategy's cost. The largest lever is therefore not a
partition-hash micro-op but the strategy/budget question — *why does a 96 MB
Int64 build side spill at all under the 64 MiB task budget?* — which is ADR 454-459
budget territory, not an engine op. Noted, not pursued here.

## Conclusion (scoped, no mechanism selected)

m06's addressable engine surface is small and fragmented because its dominant cost
(the partition hash) is a frozen contract. The one low-risk, contract-safe,
real-work candidate is the power-of-two modulo→mask; it is modest and A/B-gated, and
may prove a wash. The build/probe substrate (`insert`, `lookup`) and columnar decode
are separate owners each needing their own diagnosis. The strategic ceiling —
spilled repartition of both sides — is a budget/strategy question beyond a micro-op.
A mechanism cut (starting with the mask candidate) is profile-first and
A/B-under-settled-load as established, with expectations sized to a frozen-contract-
bounded surface, not an m05-scale win.
