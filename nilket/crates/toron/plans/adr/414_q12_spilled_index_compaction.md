# ADR 414 - q12 spilled-build resident-index compaction

## Context

The sf=10 board after 413: q03/q04/q05/q08/q10/q19 green; **q12 is the sole remaining wall** - the spilled inner
build's resident row index alone exceeds the retained cap
(`site=write, resident_index_bytes=1,074,377,480 / 1,073,741,824`, `writer_peak=0`, `pinned_decoded=0`,
`spill_bytes=0`), reproduced, ~8 s to fail. It is **not** the q05 reservoir seam: the index is built eagerly during
write (`append_spilled_batch_row_ref`, `exec/join/spill.rs:843`), before any grouped runtime exists.

**The compaction target (grounded):** the index is `HashMap<JoinKey, Vec<SpilledBatchRowRef>>` (`spill.rs:480`) with
`SpilledBatchRowRef { page_index: usize, row: u32 }` (`:461`) - and it carries **every structural tax 405 removed from
the in-memory join build, plus two more**:

| tax | detail |
|---|---|
| fat enum key | `JoinKey` (~24-32 B incl. the `Generic(Vec<u8>)` variant) for what is an 8-byte int in q12 |
| per-key heap Vec | one `Vec<SpilledBatchRowRef>` allocation per unique key (405 solved this with inline `BuildRowList`) |
| 16-byte refs | `usize` page + `u32` row + padding; packs into 8 bytes (u32 page, u32 row) |
| std HashMap + SipHash | `HashMap::new()` (`:562`) - neither 399's ahash nor 405's primitive table ever reached this map |

A primitive-keyed, packed-ref, inline-small-list representation - the proven 405/399 recipe applied to this map -
plausibly reduces the index **2-4x**, far beyond what q12 needs.

**The honesty bar (operator-set):** q12 is only ~67 KB (~0.06%) over the cap - *"just green" is too weak*. A cut that
squeaks under would wall again at the next scale. 414 requires **substantial, measured headroom**: the component
payload must show a large `resident_index_bytes` reduction (target: >=2x on the same build), not an edge-shave. And
the accounting must stay **faithful**: the counter formula is updated to mirror the new structure's real bytes -
shrinking the *formula* without shrinking *memory* is a lying green and is rejected.

## Decision

### Phase 0 - measure the index shape (before code)
On q12's walling build (instrumented probe or extended payload): rows, unique keys, key type/plan, refs per key
(duplicate distribution), bytes per ref, and **map overhead vs row-ref payload split** (enum-key + bucket + Vec
overhead vs useful bytes). This quantifies the achievable compaction and sets the measured before/after.

### The cut - compact resident index representation
- Replace the spilled-build index representation for primitive key plans with the 405-family recipe: primitive-keyed
  table (no `JoinKey` enum on the hot path), packed 8-byte refs, inline-small ref lists (`BuildRowList`-style) -
  reusing/generalizing the existing `PrimitiveBuildTable<K>` machinery where it fits. `Generic`/null-safe keys keep a
  correct fallback representation.
- **Faithful accounting:** `resident_index_bytes` recomputed from the new structure's actual footprint; the write-site
  guard stays armed and fires for genuinely-over indexes.
- **Loaded-index sibling decision:** `LoadedSpilledInnerBuild` has the same `HashMap<JoinKey, Vec<SpilledBatchRowRef>>`
  shape on the decoded-pages path. q12's wall is the write-time `SpilledInnerJoinBuild` index; fold the loaded index
  into the same representation only if the abstraction is mechanically identical, otherwise name it as a follow-on.
- **Sibling fold-in (only if clean):** the existence-set index (`HashMap<JoinKey, Vec<SpilledExistenceRowRef>>`) shares
  the representation; fold it in if it is the same mechanical change, else name it a follow-on - no scope stretch.
- Generic; no q12/table branch; **no cap/multiplier/scope change**.

### Fallbacks (named, not smuggled)
If compaction cannot clear the headroom bar: **sharded/on-demand index** or **deferred index** (per-page mini-indexes /
sort-merge probing) is the next mechanism - its own ADR with the order-contract analysis that sharding demands.

## Acceptance
- **Phase 0 recorded:** the index-shape measurements + overhead split + projected compaction, before code.
- **Primary: q12 sf=10 completes** (`pass@compare`) **or advances to a named + reproduced next wall** - with the
  **headroom bar met**: component payload shows `resident_index_bytes` reduced substantially (>=2x target) on the same
  build, not a squeak under the cap.
- **Faithful accounting:** the new counter provably mirrors the new structure (test asserting accounted vs computed
  structure bytes); the guard still fails a genuinely-over index (armed test).
- **Behavior-neutral probing:** lookup results identical (same refs, same order) across representations - locked by a
  test that fails under a reordered/lossy index; probe-side call sites (`streaming.rs:901`/`:5924`/`:6897`) unchanged.
- **Controls:** q03/q04/q05/q08/q10/q19 sf=10 pass@compare - **q19 and q05 especially** (both exercise this exact
  spilled-index machinery; q05's reservoir correctness must hold); q03 sf=1 four-way in band (fp `666e0ac028036f70`).
- **No cap/multiplier/scope bump; full engine gate** (`--no-fail-fast` both crates + architecture after fmt);
  de-branded; no committed `.perf.data`.
- **Scope honesty:** representation cut on the resident index; sharded/deferred index remains the named mechanism if
  ever needed; the full 22-query sweep is the separate closing artifact after 414.

## Guardrails
- **Headroom bar** - >=2x measured reduction target; "barely fits" is rejected as a lying green.
- **Faithful accounting** - the counter mirrors real bytes of the new structure; formula-gaming is the named
  anti-pattern.
- **Guard stays armed** - genuinely-over indexes still fail with the same producer + component payload.
- **Determinism remains lookup-driven** - any random/per-map hash seed is private and cannot affect output order;
  duplicate/ref order is the insertion order within each key, and output must not iterate the index table.
- **Reuse the 405 recipe** - primitive table/packed refs/inline lists; no novel index subsystem.
- **Fold siblings only if mechanically identical** - loaded-index and existence-index siblings are folded only when the
  shared representation really applies; else name them out.
- **Won ground is a control** - all six greens, with q19/q05 as the machinery sentinels.

## Risks
- **Compaction under the bar** -> fallbacks named (sharded/deferred index); honest 404/407-style non-bank if so.
- **Accounting drift** -> the accounted-vs-actual test; guard-armed test.
- **Probe behavior change** (ref order, duplicate order) -> lookup-equality test; q05/q19 correctness sentinels.
- **A next wall behind q12's index** (it was time/no-row three ADRs ago) -> reach-board honesty: name + reproduce.
- **Generic-key regression** -> fallback representation tested; fence mirrors 405/406.

## Forward Arc
q12 landing closes the last named sf=10 wall in the tracked family; the **full 22-query live sweep** then runs as the
closing proof artifact - answering "what remains?" instead of reconfirming a known blocker. After that: the speed
thread (q05's 4x exchange legs, q10's ~105 s, q08/q19), external sort, decode residual. The compaction also likely
*speeds* q05/q19 probing (same index on their hot paths) - recorded as a side observation, not a claim.

## Grounding evidence (file:line)
Index: `exec/join/spill.rs:480` (`HashMap<JoinKey, Vec<SpilledBatchRowRef>>`), `:461` (`SpilledBatchRowRef {
page_index: usize, row: u32 }`), `:562` (`HashMap::new()` - default SipHash), `:843` (`append_spilled_batch_row_ref`
during write; per-key base bytes + per-ref size accounting), existence sibling (`index_membership_key`,
`HashMap<JoinKey, Vec<SpilledExistenceRowRef>>`), loaded-index sibling (`exec/join/streaming.rs:1177`,
`LoadedSpilledInnerBuild`). q12 wall: `site=write, resident_index_bytes=1,074,377,480 / 1,073,741,824`, reproduced
(412/413 results). Probe call sites: `exec/join/streaming.rs:901`/`:5924`/`:6897` (`spilled.lookup(&key)`). The 405
recipe: `PrimitiveBuildTable<K>` + `BuildRowList` inline lists (`exec/join/build_rows.rs`); 399 ahash lesson. 413:
q12 explicitly outside the reservoir seam (`plans/impl/413_one_pass_partition_reservoir_results.md`).
