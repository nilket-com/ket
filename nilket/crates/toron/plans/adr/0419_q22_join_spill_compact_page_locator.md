# ADR 419 - q22 join-spill compact page locator: replace the legacy encoded-key page index

## Context

Post-418 board: 15/22 green, the scan-output accumulator family closed. The spill-writer
resident-state family has three byte-reproduced drivers, and Phase-style grounding this cycle
proved they are **not one mechanism**:

| cell | producer | guarded quantity | over |
| --- | --- | --- | --- |
| q22 | `join spill writer resident state` (`exec/join/spill.rs:338-345`, `SpilledJoinBuild`) | resident index + writer peak + page reserve vs the 64 MiB task budget | 121,631,060 / 67,108,864 |
| q21 | `semi/anti existence-set spill resident state` (`:2071-2089`, `SpilledSemiAntiExistenceBuild`) | resident index + writer peak + page reserve vs the 16x footprint cap | 1,073,742,536 / 1,073,741,824 - and this is the **crossing**, not the size: the retained 417 ledger shows the guard fires ~31.6M rows into the 60M-row lineitem scan |
| q18 | `streamed inner join build spill writer` (`:1240-1248`) | `BudgetDimension::SpillBytes` - disk allowance, not memory | 536,899,968 / 536,870,912 |

**ADR 419 takes q22 only.** q21 shares the symptom but not the contract (duplicate-heavy,
payload-backed `key -> Vec<row ref>`; ADR 414's one-ref-per-key headroom does not transfer) - it
gets a completed-shape classification here and its own representation ADR after. q18's wall is a
different budget dimension entirely - classify-only.

### The q22 wall is fully reconstructible, and direct reuse of ADR 414's index cannot clear it

q22's anti key is a single non-null-safe Int64 (`c_custkey = o_custkey`), encoded as nine bytes
(type tag + payload) into `SpilledJoinBuild`'s legacy `HashMap<Vec<u8>, Vec<usize>>` index
(`:179`). The accounting function charges `64 + key_len + 8 x pages` per entry (`:2545-2557`),
so the measured wall decomposes byte-exactly:

```text
1,500,000 unique keys x (64 + 9 + 8)  = 121,500,000
writer peak                            =      65,524
page reserve                           =      65,536
                                       = 121,631,060   (the measured request)
```

Reusing ADR 414's `SpilledBatchIndex` directly is arithmetically dead: its entry is exactly 24
bytes (i64 key + first `CompactSpilledBatchRowRef` + niche-packed overflow pointer,
`:652-655/:498-501`), and the doubling policies (`:664`, `:680-681`) put 1.5M keys at
4,194,304 x 4 bucket bytes + 2,097,152 x 24 entry bytes = **67,108,864 - the entire 64 MiB task
budget** before writer peak and reserve. That arithmetic stays in the seal as a discriminating
negative lock. (`SpilledBatchIndex` is also not Int32/Int64-only - it has a correct `Hash`
variant for Generic/null-safe plans, `:564`; Int32/Int64 is the compact-specialization fence.)

### The structural invariant the cut stands on: one page per key

`SpilledJoinBuild::write` consumes `build_rows: HashMap<Vec<u8>, Vec<u32>>` - one entry per
unique key by construction (`:188`). The write loop (`:258-295`) moves each entry whole into the
current page, flushes **before** adding an entry that would overflow, and never splits an entry;
an oversized entry becomes its own oversized page. Every key therefore lands in exactly one page,
and the legacy `Vec<usize>` value has length one by construction. The index is semantically a
**single page locator per key** - which is why a scalar compact locator is the right replacement,
and why the invariant must be regression-pinned rather than assumed.

## Decision

### Phase 0 - measure before choosing (before production behavior changes)

Temporary test-only candidate prototypes and instrumentation are permitted for the selection table;
record their results and remove unused probes before the production cut. No candidate becomes a
production path until the table chooses it.

1. Reproduce q22's exact sf=10 wall (persisted `perf-run-one` discipline); record key plan,
   null-safe flag, unique key count, page count, refs per key, writer peak, page reserve, and the
   folded legacy index bytes against the reconstruction above.
2. Pin the one-page-per-key invariant on the artifact and record which board queries ride
   `SpilledJoinBuild` (blast radius for the locator change). If any supported path can put one
   key in two pages: stop and design a compact page-ref list instead - never silently drop pages.
3. **Two-row representation selection** - footprint alone does not pick the winner:

   | candidate | completed resident bytes | representative lookup cost |
   | --- | --- | --- |
   | open-addressed locator (414-style buckets + scalar u32 page value) | computed from bucket + entry capacities (~48 MiB at 1.5M keys) | q22-shaped present/missing probe mix |
   | sorted locator vec (prealloc, append during write, sort at finish, binary-search probe) | `capacity x size_of::<Entry>()` (~24 MB at 1.5M x 16 B) | same keys and probe sequence |

   q22 probes ~15M orders rows; a sorted 1.5M-entry locator implies ~21 comparisons per probe,
   roughly 315M cache-unfriendly comparisons - possibly noise next to page I/O, but that is an
   empirical question, not an assumption. Selection rule: **the smallest representation with
   material sf=10 headroom and no material probe regression against the other candidate**; if
   both are healthy, prefer the sorted vector for its larger margin and later asymptote. A
   focused production-structure fixture plus post-cut q22 timing suffices - this does not become
   a speed ADR.
4. q21: record the crossing position and project the completed shape (input rows, unique keys,
   refs per key, overflow capacity, pages, final accounted bytes). Classification only - changing
   `SpilledSemiAntiExistenceBuild` is out of scope.
5. q18: carry the `SpillBytes` row unchanged as an orthogonal forward item.

### The cut - a locator, not a new index family

- Add a compact primitive page locator to `SpilledJoinBuild`, selected **only** for single
  Int32/Int64, non-null-safe key plans, **before the first insertion**. Generic, composite, and
  null-safe plans keep the encoded `HashMap` path unchanged.
- **Explicit key-plan seam:** the legacy spilled-hash helpers construct `HashMap<Vec<u8>,
  Vec<u32>>` and call `write` without a key plan. Reuse the existing
  `JoinKeyPlan::from_key_arrays(left_key_arrays, right_key_arrays, null_safe_keys)` derivation in
  those helpers and pass the result explicitly. Once a primitive plan is selected, the tagged
  encoded key is decoded only through a plan-specific **checked** decoder (correct tag, exact
  length); a malformed encoded key fails loudly. Eligibility is never inferred from a nine-byte
  payload.
- Page payload bytes, build-row ordering, the 64 MiB budget, writer/page guard terms, the
  producer string, and the armed guard are all unchanged - only the resident locator
  representation changes. Lookup still hands the original encoded key to page decoding, so
  payload matching and build-row order are untouched.
- **Sorted-vector pins (if selected):** Int64 entry `{ key: i64, page_index: u32 }` = 16 bytes
  aligned (Int32 can be 8); preallocate once from `build_rows.len()`, append one locator per key
  during the single write pass, sort after the consumed map iterator is dropped; sorting must not
  allocate a second full-size vector; accounting uses vector **capacity**, not logical length,
  plus the unchanged writer peak and page reserve; keys strictly ordered after finish (a
  duplicate violates the one-key-one-entry contract); binary search returns exactly one page or
  none.
- **Open-addressed pins (if selected):** carry ADR 414's bucket/entry capacity accounting and
  lookup-equality locks with a scalar u32 page value - do NOT generalize
  `PrimitiveSpilledBatchIndex` by retaining its 24-byte ref-list entry; the exact 64 MiB failure
  arithmetic is the negative lock either way.

### Stop-line

Replacing the upstream `HashMap<Vec<u8>, Vec<u32>>` build map with typed `JoinBuildRows` would
broaden the mechanism beyond a locator cut - if the seam forces it, stop the seal and name the
follow-on. Same for a key-in-two-pages discovery in Phase 0.

## Acceptance

- **Primary:** q22 sf=10 reaches persisted strict `pass@compare`, or advances to a
  **byte-reproduced named next wall** with the resident-state producer gone. No family rounding.
- **Eligibility-discriminating locks** (the 417/418 standard):
  - the production helper selects the compact locator **before the first insertion** on a
    primitive non-null-safe plan; legacy insertion count stays zero on the compact path;
  - accounted bytes equal computed bytes, both capacity-based, observed not assumed;
  - a 1.5M-shaped or mathematically equivalent low-cap fixture **clears with real headroom on
    the compact path while the legacy twin fails the resident guard** - the wall-and-win pair in
    one lock;
  - a multi-page fixture pins **one page per key on the actual writer** (every legacy locator
    slot length-1 / every compact lookup exactly one page or none), independent of selection;
  - probe parity: present keys, absent keys, duplicate build rows within one key, page
    boundaries, and preserved build-row order;
  - fallback fences: Generic, composite, and null-safe plans stay on the encoded path;
  - a u32 page-index truncation fence and a malformed-encoded-key loud-failure lock;
  - the guard stays armed (a fixture still trips the unchanged producer past the cap);
  - cleanup leaves scratch empty on write/finish/probe error.
- **Controls:** q21 and q18 retain their exact named producers and bytes (do-not-disturb);
  spill-bearing greens (q03 q07 q09 q10 q12 q13 q14 q16 q19 q20) pass@compare with forensic
  bands.
- **Results doc records:** the legacy decomposition, the two-row selection table with the chosen
  representation's completed capacity/computed bytes and writer/page terms, the lookup-cost
  comparison, post-cut q22 timing, and the O(unique keys) asymptote statement.
- **Full engine gate** after fmt (`--no-fail-fast` both crates + architecture); no cap,
  multiplier, guard-term, producer, or scope change; de-branded; no committed `.perf.data`.

## Guardrails

- **A locator cut, nothing wider** - build map, page encoding, guard, and fallback all unchanged;
  the stop-line fires rather than half-landing a typed-build-rows rework.
- **Eligibility from the explicit key plan only** - never from encoded byte length; checked
  decoding or loud failure.
- **Representation chosen by measurement** - footprint AND lookup cost; the selection table is
  recorded, not asserted.
- **Capacity-based accounting** - `capacity == len` is observed, never assumed.
- **The 64 MiB direct-reuse arithmetic stays as the negative lock** - it is the reason this ADR
  exists and the fence against quietly reintroducing the ref-list entry.
- **q21/q18 are do-not-disturb controls** - their walls are evidence for their own ADRs.
- **Won ground is a control** - the spill-bearing greens re-proven with bands.

## Risks

- **Sorted-lookup latency at 15M probes** -> fenced by the two-row selection; open-addressed
  remains the fallback winner if probes regress.
- **A wall behind the wall** (q22 has never executed past this guard at sf=10) -> reach honesty;
  a named, byte-reproduced next wall is an accepted primary outcome.
- **Key-encoding drift** -> the checked decoder + malformed-key lock keep a format change loud.
- **O(unique keys) asymptote** - any resident per-key locator re-walls eventually (open-addressed
  ~sf=15, sorted ~sf=30 at q22's shape). This cut is a genuine representation win in the ADR 414
  mechanism class (2.5-5x), not a cap nudge - and the forward arc names the structural ceiling
  honestly rather than hiding it.

## Forward Arc

q22 green moves the board to 16/22. q21's completed-shape evidence (recorded here) feeds its own
representation ADR - compact row refs vs page locator with in-page residual filtering are
meaningfully different probe contracts, and the duplicate-heavy existence shape may also be the
first driver for the family's structural answer to unbounded key counts (a spilled/sharded/
deferred index). Behind those: q18's spill-volume/quota decision plus its unreached membership
wall, q02's join-output monolith as the C-spec driver, q05's diagnosis-first OOM, and the
q11/q17 correctness-policy threads. The q10 compact-index speed win stays parallel.

## Grounding evidence (file:line)

q22 producer + guard: `exec/join/spill.rs:335-345`; legacy index type `:179`; bulk write loop
`:258-295` (flush-before-add, entries never split); accounting `:2545-2557` (64 + key_len + 8 x
pages). Wall decomposition: 1,500,000 x 81 + 65,524 + 65,536 = 121,631,060 vs 67,108,864 (ADR
415 results, re-verified this cycle). ADR 414 machinery: `SpilledBatchIndex` `:563-643` (Hash
variant `:564`, compact fence `:570-579`), entry layout `:652-655` + `:498-501` (24 bytes),
doubling `:664`/`:680-681`; direct-reuse arithmetic 4,194,304 x 4 + 2,097,152 x 24 = 67,108,864.
q21 producer: `:2071-2089`; crossing evidence: retained 417 scan ledger (~batch 30,837 x 1,024
rows of 60M). q18 producer: `:1240-1248` (`SpillBytes`). Board rows: ADR 415/417/418 results
docs.
