# ADR 602 - q21's replay-1 probe: a hit's refs without an allocation

**Status:** CLOSED, NOT ESTABLISHED. One paired attempt on strix under
the declared gate procedure (results in
`plans/impl/0602_q21_replay1_probe_hit_refs_without_allocation_results.md`):
the gate admitted after the 5 s wait (37 °C, load 0.27), separation and
window verified, digest parity on all twelve rows. Every pair was
positive, with a median gain of 55.3 ms (0.70%), but that is under a
bar of 75.4 ms (2 × the 37.7 ms control range). Under ADR 588's rule
the verdict is `not_established`, and the token reverts to `Collected`;
the implementation stays. The mechanism check on the treatment read
`lookup_form = inline` with all 14,656,608 of replay 1's hits returned
inline: the allocation was removed on every hit, and the observed
median paired difference did not clear the bar. Successor to ADR 601, which retained the
finish-built compact index on the spilled branch (median paired gain
148.2 ms, 1.84% of q21) and, on its mechanism check, read replay 1's
lookups for the first time: 30,343,541 lookups, 14,656,608 hits,
15,686,933 misses, against an index with 5,845,938 entries and no
repeated key. Its forward section names this record's candidate: the
probe side, `refs` allocating per hit. Those counts are readings under
an off range of 8.54%; they are the grounding for this record, not a
savings estimate.


## Context

**What a hit costs now, at `cf754374`.** Every production probe of the
spilled build goes through `SpilledInnerJoinBuild::lookup`
(`exec/join/spill.rs:3666`): `async`, `&mut self`, returning
`Result<Vec<SpilledBatchRowRef>, _>`. On the compact index it calls
`SpilledBatchIndex::lookup` (`spill.rs:1108`), then
`PrimitiveSpilledBatchIndex::lookup` (`spill.rs:1453`): `find_bucket`
(the `ahash` hash and a linear probe), then on a hit
`SpilledBatchRefList::refs` (`spill.rs:964`), which allocates a `Vec`
at capacity `1 + overflow.len()`, pushes the inline first ref unpacked,
and extends with the overflow refs in insertion order. A miss returns
`Vec::new()`, which does not allocate. So on q21 there are 14,656,608
heap allocations and frees per run, one per hit, and every one of them
holds exactly one ref. That follows from the record's facts: no key
repeated, so no entry has an overflow list. After the lookup, `lookup`
reports `matches.len()` to the throughput profile when one is attached,
and, when counting is enabled (the profiled arm only), counts a hit or
a miss on `is_empty()`.

**Who consumes it.** `SpilledInnerJoinBuild::lookup` has six
production call sites, each a
`for build in spilled.lookup(&key).await? { … }` that consumes the
vector by value and does not keep it past the probe row:
`streaming.rs:5997` (`visit_projected_streamed_spilled_inner_join_chunks`),
`streaming.rs:6183` (`visit_streamed_spilled_inner_join_chunks_profiled`),
`streaming.rs:14859` (`visit_streaming_spilled_left_outer_probe_batch`),
`streaming.rs:16794` (`fold_projected_streamed_left_semi_grouped_aggregate`),
and `streamed_spill.rs:188` and `:724`. Two of them,
`streaming.rs:14859` and `streamed_spill.rs:724`, call
`spilled.mark_page_row_matched` inside the loop, a `&self` method on
the same build. Another type, `SpilledJoinBuild`, has its own byte-key
`lookup` (`spill.rs:787`, returning `JoinSpillRows`); it is called
from `spilled_hash.rs` and is not part of this record.

Because the build is borrowed again inside the loop at two sites, and
`lookup` itself takes `&mut self` for its counters, a return type that
borrows the index would not compile at those sites without a wider
restructuring. An owned value that holds a single ref inline compiles
at all six unchanged.

**Nulls, misses, duplicates.** A null key never reaches `lookup`. Under
the primitive plans, `join_key_at` (`exec/join.rs`) returns `None` for
a null `Int32`/`Int64`, and every caller `continue`s. A null-safe or
`Generic` plan takes the `Hash` variant or the legacy `HashMap`, whose
lookups are `.get(key).cloned().unwrap_or_default()`: an owned clone of
the stored `Vec`, or an empty one. A mismatched key variant against a
primitive index returns empty. Duplicates are the general case the
index keeps: the first ref inline and the rest on the overflow list,
in insertion order. The lookup returns all of them in that order.


## Decision

**Return a hit's refs as an owned value that holds one ref inline, so
a unique-key hit allocates nothing, measured as a paired cut under ADR
599's procedure with the retention rule unchanged.**

*The value.* A new type in `exec/join/spill.rs`:

```rust
pub(in crate::exec) enum SpilledLookupMatches {
	Miss,
	One(SpilledBatchRowRef),
	Many(Vec<SpilledBatchRowRef>),
}
```

It provides `len()` and `is_empty()`, and `IntoIterator<Item =
SpilledBatchRowRef>` through a by-value iterator enum (empty, once, or
`vec::IntoIter`) with an exact `size_hint`. `SpilledInnerJoinBuild::lookup`
keeps its shape (`async`, `&mut self`, `Result<_, ExecutionError>`) and
returns `SpilledLookupMatches`. The profile's `record_lookup(len())`
and the hit/miss counters on `is_empty()` are unchanged in meaning.
The six call sites keep their text. The existing
`SpilledInnerJoinBuild::lookup` tests in `spill.rs` and `streaming.rs`
compare the result against a `Vec` or against another lookup (for
example `streaming.rs:36818` and `:37186`). They change to compare
an explicit value: the shape, and the refs collected into a `Vec`. No
`PartialEq<Vec<_>>` is added to make the old comparisons compile.

*Where each shape comes from.* On the compact `Int32`/`Int64` index,
`SpilledBatchRefList` gets a `matches(form)` beside `refs()`:

- first only, under the treatment form, gives `One(first.unpack())`.
- first only, under the control form, gives `Many(vec![first.unpack()])`,
  which is today's allocation, byte for byte.
- first plus overflow, under either form, gives `Many` (first, then
  overflow, in insertion order), which is today's allocation too.
- an absent key, or a mismatched key variant, gives `Miss`.

The `Hash` variant and the legacy `HashMap` path give `Many(cloned)`
for a stored key and `Miss` otherwise, so they allocate exactly as
they do now. The deferred-replay rebuild
(`rebuild_compact_index_and_pin_pages`) builds the same index type and
takes whatever form the build carries.

*The one token.* `SpilledLookupForm { Collected, Inline }`, a field on
`SpilledInnerJoinBuild`. It defaults to `Collected`, today's
behaviour, on every construction path. The spilled branch sets it
beside ADR 601's form (`streaming.rs:24143`) with
`spilled.use_lookup_form(SpilledLookupForm::Inline)`. Control is the
tree with `Collected` at that call site. Treatment is the same tree
with `Inline`: one insertion and one deletion. Both arms carry the same
enum, the same match, and the same branch per hit, so the pair prices
the allocation and nothing else. The receipt names the form
(`lookup_form`: `collected` / `inline`) beside `index_build`.

*Facts.* `lookup_form` on the receipt and in the tpch bands, with the
rule `lookup_form = inline ⇒ index_mode = compact`. Under counting,
one new count, `lookup_inline_hits`: hits returned as `One`. With the
existing counts it satisfies `lookup_inline_hits ≤ lookup_hits`, and
`lookup_inline_hits = 0` under `collected`. On q21's build, with no
repeated key, the arithmetic says `lookup_inline_hits = lookup_hits`
under the treatment. The mechanism check reads it rather than
assuming it.

*The experiment.* Control and treatment as above, two release
executables built on bigfoot from one recipe, staged and hash-verified
on strix, with ADR 588's manifest gate. Then, all by reference to ADRs
597, 599, 600, and 601 and unchanged: the twelve-row driver, ADR 592's
validator with the arm's executable and the sealed digest on every
row, ADR 593's readings and cutoff, ADR 595's separated launch, ADR
599's gate procedure with `--gate-settle-seconds 5`, and one attempt.
**ADR 588's rule: retained only if the median paired gain exceeds
`2 × range_control` and every pair is strictly positive.** Every other
outcome reverts the token to `Collected` in the closing commit. The
mechanism check (ADR 596's driver on the treatment, the same
procedure) follows a verdict, never a refusal. It reports
`lookup_form`, replay 1's hits, misses, and inline hits, the
`inner_replay_1` stage span and calls, and the build terms beside ADR
601's as readings.

*What is established, and what is not.* The allocation count is
established: 14,656,608 single-ref `Vec` allocations and frees per run
on ADR 601's reading, one per hit. Their time cost is not, and this
record gives no estimate. That cost is what the pair measures, and
whether it clears twice the day's control range is for the measurement
to say.

**What this record does not do.** It does not change the hash
function, the bucket layout, `find_bucket`, the `Hash` variant's
cloning, the page reads, or the materialization. It does not make
`Inline` the default on paths other than the spilled branch's call
site. It does not restructure callers to borrow. It does not read the
`inner_replay_1` stage as a bound on the gain.


## Acceptance

- The token at the spilled branch's call site in the treatment tree,
  asserted by diff (one insertion, one deletion). The default
  `Collected` on every other construction path, fixtured.
- Value parity, the whole scope. For `Int64` and `Int32`, under both
  forms, and for the `Hash` variant and the legacy index: the same
  refs in the same order through `lookup` collected into a `Vec`, for
  a unique key, a key repeated within and across batches (`Many`, in
  insertion order), an absent key (`Miss`), a mismatched key variant
  (`Miss`), and nulls under the primitive plans (never reached). Each
  asserts the shape returned and `len()`/`is_empty()` against the
  collected length.
- The iterator: exact `size_hint` on all three shapes, and fused after
  exhaustion.
- The counters: `record_lookup` receives the same lengths, and
  hits/misses match the collected emptiness under both forms;
  `lookup_inline_hits` is zero under `Collected`, and equals the
  `One`-shaped hits under `Inline`; the `lookup_form` fact and its rule
  are on the receipt and in the tpch bands.
- The six call sites compile unchanged; the existing lookup tests
  compare the explicit shape and collected refs; the existing join, spill,
  and tpch ladders are green on both arms.
- Two executables built on bigfoot, both ladders green, staged and
  hash-verified on strix; one run under the declared procedure with
  twelve rows, readings, separation and window verified, the control
  range and the bar, the gain per pair and its median, the verdict,
  and the entry and gate readings reported as observations; or the
  refusal.
- After a verdict, the mechanism check: `lookup_form`, replay 1's
  hits, misses, and inline hits, the `inner_replay_1` span and calls,
  the index facts, and the build terms beside ADR 601's.
- Plan commit, then one implementation/results/closure commit. Every
  outcome other than `retained` reverts the token in that commit.


## Guardrails

- The retention rule, the twelve-row contract, the gate procedure, and
  the threshold are ADR 597/599/601's and ADR 593's, verbatim.
- Build on bigfoot, measure on strix, the separated launch, nothing
  else on strix in the window; no relaunch under this record.
- The hash function, the bucket layout, and the index build are not
  changed under this record. A hashing cut is its own record.


## Risks

- The allocator's fast path for a 16-byte block may be cheap enough
  that the gain sits under twice the control range. ADR 601's range
  was 29.7 ms, and ADR 600's was wider. A `not retained` verdict is
  then the result, and the token reverts.
- Both arms add a match on the form per hit. It is the same in each,
  so it is not netted, but it means the control is not byte-identical
  to `cf754374`'s probe. The pair compares the two forms, not this
  tree against the last.
- `Many` on a duplicate-heavy build still allocates per hit, as today.
  Only unique-key hits change.


## Forward

- Retained: the hash function and `find_bucket` on replay 1 become the
  next candidates on the probe side, grounded on the same counts.
- Beyond q21: the campaign's anchor is ADR 591's sealed strix board
  (Toron 64.836 s against DuckDB 15.683 s; half DuckDB's speed is
  <= 31.366 s). The next record refreshes that board on the closing
  tree under the same coordinate, and the owner after it is chosen by
  absolute recoverable time on the refreshed board, whether that is a
  kernel cut or an architectural change. q01, join-free at 5.0x on ADR
  591's board, is one candidate for attribution, not a precommitment.
