# ADR 505 implementation receipt — m04 fold kernels falsified; producer boundary pinned

ADR 504 selected a vectorized COUNT-class grouped-fold experiment for m04. ADR 505
implemented and priced two ways of avoiding the per-row hash-table probe. Neither
earned admission, so no executor mechanism ships.

## Iteration zero: batched prefetch

The first implementation applied ADR 497's batched-prefetch pattern to the
single-Utf8 grouped fold: staged hash plus bucket prefetch, an all-hits resolver,
and a vectorized `COUNT` update behind `TORON_BATCHED_GROUPED_FOLD`.

The mechanism was correct: its collision-dense unit test matched the scalar fold,
and end-to-end m04 retained the standing digest, three output rows, and zero spill.
It was nevertheless a performance wash:

| sf=8, batch 8,192 | scalar | batched |
|---|---:|---:|
| grouped fold | ~492 ms | ~488 ms |
| execute | ~798 ms | ~798 ms |

The arm-the-change MLP check explains the wash. m04 incurred only about 300K LLC
load misses over 48M rows (0.006/row), ran at IPC 4.38, and retired the same 15.89B
instructions in both arms. Its three-entry group table is cache-resident. ADR 497
won on m06 because that join probes a 1.44GB table; there is no analogous latency
for prefetch to overlap here.

The implementation was removed before sealing.

## Iteration one: reconstruct a bounded dictionary after hydration

The second implementation tested the remaining fold-local alternative instead of
assuming it away. For the single-Utf8, single-COUNT shape it built a batch-local
dictionary capped at 64 distinct values, counted rows by dictionary slot, then
hashed and updated each distinct group once. Above the cap it declined before
mutating aggregate state and fell back to the generic fold.

The focused test pinned exact counts (including NULL), sorted output, default-on
ablation parsing, and the no-partial-fold high-cardinality decline. A same-binary
alternating 3×3 at sf=8 then rejected the mechanism:

| arm | execute samples | median |
|---|---|---:|
| existing borrowed scalar fold | 722.9 / 710.8 / 719.9 ms | **719.9 ms** |
| reconstruct dictionary after hydration | 847.7 / 846.5 / 849.6 ms | **847.7 ms** |

That is **+17.8%**, with three of three losses. All six rows carry
`build_commit=bd0d9d6e9c8e-dirty`,
`binary_modified_unix=1784806731`, the standing digest, three rows, and zero spill.
Artifacts remain under `target/tpch/adr505_iter1_{control,candidate}_{0,1,2}/`.

The implementation and its ablation were removed. The source tree is byte-for-byte
back at `bd0d9d6e`.

## Decision

m04's remaining grouped-fold cost is compute, but it is not profitably removable
after the producer has hydrated Parquet dictionary/RLE data into a `StringArray`:

- prefetch cannot help a three-entry resident table;
- rebuilding a dictionary in the fold repeats string comparisons after hydration
  and is materially slower;
- a cheaper hash can address only the measured hash share, not recover encoded
  dictionary indices that were discarded upstream.

The 2.5× reference mechanism hashes each encoded distinct value once and folds
integer codes. Toron cannot express that mechanism today: no production scan
producer emits `DictionaryArray`, while the TPC-H Parquet reader deliberately
models the current logical Utf8 boundary. Teaching only that gauge to preserve
dictionary encoding would optimize the proxy rather than the engine.

ADR 505 therefore closes the fold-kernel target with no source change. The real
capability boundary is a production-valid dictionary/RLE-preserving scan contract,
followed by a dictionary-aware grouped fold. That is a storage/scan design with
schema, IPC, fallback, and mixed-encoding obligations; it is not smuggled into this
ADR as a benchmark-only reader switch.

## Rails and forward work

- Both rejected kernels were implemented, correctness-tested, and measured before
  removal.
- No wash or regression remains behind a default-on toggle.
- ADR 499's local exchange bypass remains m04's banked engine win.
- The next performance decision must choose between designing the production
  encoded-column contract and re-grounding the remaining 160–275 ms ladder band as
  engine versus gauge cost. No per-rung kernel is pre-authorized.
