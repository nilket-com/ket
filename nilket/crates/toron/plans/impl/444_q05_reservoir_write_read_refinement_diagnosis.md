# ADR 444 impl - q05 reservoir write/read sub-leaf refinement (schema 2)

Refinement diagnosis on the sealed 443 substrate. Off-by-default; additive; no engine behavior
change. 429 surface untouched.

## Schema-2 extension (`grouped_chain_profile.rs`)

- Bump `PROFILE_SCHEMA_VERSION` to 2.
- **Retain the measured parent timers** for `reservoir_partition_spill_write` and
  `reservoir_read_probe`. Nest fifteen mutually exclusive sub-leaves beneath them (write: writer
  setup, scan/decode, partition routing, IPC encode, encryption, physical write, finish/fsync;
  read/probe: probe partitioning, physical read, decrypt, IPC decode, cache/LRU accounting, index
  rebuild CPU, lookup, materialization).
- **Per-parent gap** = `parent_measured − Σ(its sub-leaves)`, ≤ 10% ceiling each; root residual rule
  unchanged. Sub-leaf overlap rejected; a sub-leaf may only run while its parent is active.
- Counters: unique ciphertext page inventory keyed **`(spill_id, page_index)`** with ciphertext
  length; physical ciphertext read calls/bytes; cache hits/misses; decode counts; **read
  amplification = physical ciphertext read bytes ÷ unique ciphertext bytes**; resident/streamed
  branch counts; `try_pin_decoded_pages` outcomes.
- `GroupedChainSnapshot` is serialize-only: **additive-field compatibility test** (schema-2 JSON is a
  strict superset of schema-1 names/semantics).
- Same construction/parity/publication contract as 443.

## The observer seam (locked)

One **query-neutral, pointer-sized `Option<&…Observer>`** threaded from the admitted grouped-reservoir
path into the shared spill helpers:

- **No process-global lookup; no profiler field on `SpilledInnerJoinBuild`** — ordinary spill users
  pass `None` and construct nothing.
- The observer enforces exclusivity and **pauses CPU timers around nested I/O**: the index-rebuild
  CPU sub-leaf suspends while page-read/decrypt/decode sub-leaves run inside
  `rebuild_index_from_pages`; partition-routing never wraps encode/write; cache/LRU accounting
  (`touch_cache_entry` et al.) is its own sub-leaf, never folded into physical read or decode.
- Threading sites: the write helper (`build_partitioned_streaming_scan_inner_build_reservoir`,
  `streaming.rs:15226`), the probe helper (`:15424`, both resident and streamed branches), and the
  spill primitives they call — every other caller unchanged (`None`).

## Phase 0 — seam map + branch census (before any hook)

1. Confirm the live chain `9507 → 11853 → 12072 → 15226/15424` and the 443-pinned signature on the
   current binary. Drift ⇒ stop and revise.
2. Seam map for all fifteen sub-leaves at the real boundaries (writer create/setup; scan poll +
   decode; `precompute_join_partition_buckets`/routing; `encode_record_batch_ipc_into`; encryption;
   `write_plain_at`; finish/fsync; probe bucketing; file read; decrypt; IPC decode; cache/LRU;
   `rebuild_index_from_pages` CPU; lookup; materialize). Inseparable seams merge with disclosure.
3. **Branch census:** resident (`read_all_batch_pages`) vs streamed (`rebuild_index_from_pages` →
   probing) counts per partition + pin outcomes; both branches covered; zero-taken branches report
   zero, disclosed.
4. No new oracle identities. One qualitative symbol-rich perf child, outside every gate.

## The pure cohort evaluator (harness-side, table-tested)

Pure function over the five children's sub-leaf share/time vectors → G1-G4 verdict:

- **Definitions:** spread = `(max − min)/median`; **zero median is stable only if all five values are
  zero**, else unstable; inputs validated **finite + nonnegative**; G1 "largest" must be **unique**
  within each child; a **zero/zero median comparison cannot satisfy G2**.
- **G1:** same unique-largest sub-leaf 5/5 **and** share spread ≤ 0.25.
- **G2:** top median share ≥ 1.15× runner-up (zero/zero excluded) **and** selected share spread ≤ 0.25.
- **G3:** top two within 1.15×, **exactly one** with absolute-time spread ≤ 0.05, and that one's
  share spread ≤ 0.25.
- **G4:** everything else → red (non-uniqueness only; no producer inference).
- **Table tests:** each arm hit/miss, within-child tie, all-zero cohort, median-zero/nonzero,
  runner-up-zero, boundary ratio 1.15, boundary spreads 0.25/0.05, invalid input rejection.

**Envelope check:** hand ADR 445 a mechanism only if the selected sub-leaf's **median share >
0.2386**; otherwise bank the finding without a hand-off.

## Variance decomposition (required, signed-delta correct)

Between the maximum-write and minimum-write children:

- **Signed exact-sum:** Σ(signed sub-leaf deltas) + parent-gap delta = parent delta (checked
  arithmetic).
- **Ownership vs positive contribution:** `positive_total = Σ max(delta, 0)`. A unique sub-leaf owns
  the variance iff its positive delta ≥ **60% of positive_total**. Negative offsets reported
  separately.
- **Gap-owned:** if the parent gap ≥ 60% of positive_total, the verdict is **under-instrumented** —
  widen the taxonomy; no mechanism naming.
- Else **multi-source variance** reported with each contributor's share. Correlation is context only.
- The receipt notes this localizes variance in the profiled cohort and only *suggests* the ADR 356
  0.1193 source.

## Stability harness

Five fresh strict-sf10 children, 356 envelope, temporally isolated. Report: sub-leaf shares +
absolute times; measured parents + gaps (≤ 10% each); root residual; branch/pin census;
`(spill_id, page_index)` inventory identity (= reservoir spill role, exact) + physical-read/cache/
decode/amplification counters; result tuple + delta-form spill + oracle identities exact 5/5;
evaluator verdict; variance decomposition; cohort wall distribution.

## Off-by-default parity + survival

- Disabled ⇒ observer absent everywhere, profiler `None`, zero state; byte-identical result tuple;
  429 unchanged; additive-field test green.
- One persisted strict sf=10 q05 `pass@compare` under 4 GiB/no-swap, scratch fresh/empty before and
  after.

## Seal gate (council.md, via scripts/cargo-cgroup, after fmt)

```
scripts/cargo-cgroup fmt  -p toron -- --check
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
```

- `exec.rs` 7,499; join-root ratchet holds; box new held state if async bodies grow.
- Architecture guard: extend the 443 guard — measured parents retained; sub-leaf exclusivity +
  parent-active nesting + CPU-pause-around-I/O at the observer; observer threaded only from the
  admitted path (no global, no field on ordinary builds); evaluator existence + table tests;
  schema additivity; 429 untouched; no query literal.
- Focused tests: parent/sub-leaf/gap arithmetic; overlap + outside-parent rejection; CPU-pause
  correctness (nested I/O inside index rebuild); merged-seam disclosure; additive-field
  compatibility; `(spill_id, page_index)` identity vs amplification counters; evaluator tables;
  signed variance-decomposition sum + gap-owned verdict; parity; atomic publication failure.

## Implementation contingencies

- Sub-leaf anchors finalized by the Phase-0 seam map; uncovered stages get sub-leaves before
  measurement; inseparable seams merge with disclosure.
- If the live chain drifted, stop and revise.
- Zero-taken branches and measured-small sub-leaves are kept and disclosed, not dropped.
- If the observer cannot thread without touching ordinary spill users, stop and revise — the seam
  contract is load-bearing.
- **No mechanism** — ADR 445 owns whatever the evaluator + envelope select; G4 red or gap-owned
  variance returns to the council.
