# ADR 421 - q21 recognized-Neq existence summary: representative + Multi bit instead of row refs

## Context

Post-420 board: 16/22 green. q21 is the last resident-state cell:
`semi/anti existence-set spill resident state` at 1,073,742,536 / 1,073,741,824 (fingerprint
`e2e25953a6a3fefd`) - and that number is a **crossing**, not a size: the guard fires ~31.6M rows
into the 60M-row lineitem scan. ADR 419 measured the completed shape (15M keys, 59,986,052 refs,
1-7 per key): the legacy existence index projects to 2,039,776,832 bytes and even ADR 414's
compact duplicate-list shape to 1,342,403,544 - both over the 1 GiB retained-footprint cap.

Council grounding then reordered the candidate list twice:

- **There is no partition knob.** `prepare_streaming_semi_anti_runtime` builds exactly one
  `SpilledSemiAntiExistenceBuild` per stage (`exec/join/streaming.rs:10333`); sharding would be
  new machinery, not tuning.
- **A wall hides behind every row-ref fix.** The spilled probe groups candidate refs by page per
  preserved batch (`spill.rs:2466-2508`) but reloads pages across batches against an 8 MiB
  cache - fixing only build residency walks q21 toward a q22-style probe amplification. 420's
  lesson applies verbatim: eliminate the page from the probe, don't shrink it.
- **The rows are semantically unnecessary for q21's residuals.** Both stages carry a
  single-column non-null Int64 non-equi residual (`l2.l_suppkey <> l1.l_suppkey` on the semi,
  `l3.l_suppkey <> l1.l_suppkey` on the anti, whose `l_receiptdate > l_commitdate` filter is
  applied upstream of the existence scan). Per key, "does any build row have a value different
  from mine?" is answered exactly by a **representative value plus a Multi bit**: missing key ->
  no match; `Single(v)` -> match iff `v != probe`; `Multi` -> always match. The same matched
  boolean feeds the existing LeftSemi/LeftAnti selection unchanged.

The persisted sf=10 census (Phase 0 re-verifies at HEAD):

| stage | input rows | keys | Single keys | Multi keys | max distinct/key | null key/value rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| full lineitem (semi) | 59,986,052 | 15,000,000 | 2,143,942 | 12,856,058 | 7 | 0 |
| receipt-after-commit (anti) | 37,929,348 | 13,753,474 | 3,372,250 | 10,381,224 | 7 | 0 |

Supplier values span 1..100,000 in the fixture - **evidence, not permission to narrow**: a
sentinel is unsafe on a full-range Int64 domain, and checked u32 narrowing would discover an
unsupported value after commitment, turning a valid fallback query into an error. The
representation is therefore full-width:

```text
entries: { key: i64, representative: i64 }   # 16 bytes
buckets: u32 entry indices (0.5 max load)
multi:   one bit per entry index
```

At the observed key counts both stages land identical 420-style capacities:
33,554,432 x 4 buckets + 16,777,216 x 16 entries + 2,097,152 bitset bytes = **404,750,336 per
stage, 809,500,672 for the pair** - and the pair is the real envelope, because both runtimes are
live before the base probe (`streaming.rs:9490-9520`), leaving ~264 MB of cap headroom before
the inner runtimes. The completed pair therefore fits nominally and collapses ~2.04 GB of row
refs to ~0.81 GB of summaries with **zero pages in the probe**; Phase 0 must still prove that
the live inner term and checked growth transients leave material sf=10 headroom before the ADR
may claim the cell clears.

**The honest ceiling is ~sf=11**, not further: the full-lineitem stage crosses its
16,777,216-entry capacity at ~sf=11.2, and the power-of-two doubling plus the live anti summary
exceeds the cap. This cut is still right at that ceiling: q21 goes green now at bounded cost
with the next wall byte-derived and named, every alternative is strictly worse at sf=10 (CSR
peaks at ~960 MB of unsorted pairs inside ~11% margin *and* keeps the page probe; sharding is
the biggest mechanism for a cell that fits without it), and **the summary is what a future shard
would shard** - moving ~0.81 GB of entries is a 2.5x smaller job than moving row refs, and the
recognizer, fences, parity locks, and probe contract landed here are the durable parts.

## Decision

### Phase 0 - verify the premise on the persisted plan (before code)

1. Dump the actual bound residual programs of both stages: single root, node shape, operand
   ordinals, resolved column types and **nullability** - not inferred from SQL.
2. Re-verify the census above at HEAD (key counts, Single/Multi split, max distinct, zero null
   keys/values) and the retained-column projection (`[l_orderkey, l_suppkey]`).
3. Measure the **live inner-runtime resident contribution** and the preserved probe volume - the
   combined envelope (both summaries + inner runtimes) is the decisive number, and the dynamic
   growth peaks (bucket rehash, entry reallocation, bitset growth) need explicit evidence; "no
   build-time peak" is too strong until measured.
4. **Stop-line:** if sf=10 lacks material combined headroom, or if enforcing the combined peak
   requires a broad accounting rework, stop and compare the shared-key dual summary or the
   sharded build - do not weaken accounting.

### The cut

**1. Structural recognition, then ordinal splitting** (`recognized_neq_existence_summary_columns`,
name illustrative). Using the existing introspection surface (`single_root()` / `node()` /
`ResolvedColumn`; the joined-layout split precedent in
`retained_existence_columns_for_semi_anti_stage`):

- exactly one root, and it is `Binary { op: Neq }`;
- both children are **direct Column nodes** - no casts, wrappers, literals, or nested
  expressions; operands accepted in either order;
- each child ordinal classified against the preserved width; exactly one child per side; the
  existence-local ordinal range-checked after subtraction and cross-checked against both side
  schemas;
- key plan and both value columns non-null Int64 - all proven **before any file or scan I/O**.

Recognition is stronger and simpler than collecting columns and inferring shape. Rejections
(same-side columns, out-of-range ordinals, casts/wrappers, nullable columns, non-Int64,
multi-root, equi) fall back to the unchanged `SpilledSemiAntiExistenceBuild`.

**2. Chain-owned, atomic two-stage preflight.** The generic semi/anti preparer serves other
recognizers; a coincidentally similar residual in an un-vetted topology must not select the
summary. Immediately after chain extraction/projection and **before preparing any inner runtime
or opening any scan/file**, the q21 owner applies the structural recognizer to both the semi and
anti stages. The summary mode is all-or-nothing for this cut: both stages pass and their typed
metadata is handed to a summary-specific preparation path, or neither stage uses the summary and
the existing two-spilled-runtime path remains unchanged. No mixed summary/spilled pair and no
late second-stage eligibility decision are admitted.

**3. Build, checked growth, and probe.** Stream the projected key/value batches directly into the
summary (insert: new key -> `Single(v)`; existing key with different value -> set the Multi bit).
ADR 420's locator is lineage, not a literal reusable constructor: it preallocates from an
already-materialized `build_rows.len()`, while this streaming writer does not know its final
unique-key count. Every bucket/entry/bitset growth is therefore an explicit checked transaction:
reserve the old+new transient against the owner ledger before allocation, grow to a named
power-of-two target, observe and account actual capacities, then insert. `Vec::push` or bitset
growth must never trigger an unguarded implicit allocation. After the first insert the arm is
committed - no fallback/rescan; malformed recognized metadata, allocation failure, or accounting
failure is loud and cleaned.

Probe by key + preserved value column, returning the existing matched bitmap with **zero page
I/O and no per-key candidate/page allocation**; the existing per-batch key grouping, matched
bitmap, and output-selection buffers are not claimed away. The two stages keep separate summary
instances; sharing keys or fusing the two lineitem scans is a different chain-level mechanism.

**4. Combined accounting belongs to the chain owner.** Each summary exports exact capacity
accounting (buckets, entries, bitset, any transient reservation); the chain owner supplies a
shared ledger to both streaming writers and enforces the **combined** check before every growth -
live inner runtimes + completed first summary + growing second summary included. The inner term
must come from current runtime accounting, never a Phase-0 constant or nominal allowance. Two
independent per-summary guards are insufficient even if each is byte-exact. If exact inner
accounting cannot be exposed without the Phase-0 stop-line's broad rework, stop. If second-stage
preparation fails after commitment, the first summary and all created runtimes are cleaned.

**5. Shrink-at-finish is measured and optional - never credited.** Exact arithmetic: shrinking
the semi summary saves 28,657,608 bytes and the anti another 48,757,832 (completed pair
809,500,672 -> 732,085,232); the bucket arrays cannot shrink (0.5 load), and **a reallocating
anti shrink can transiently reach ~1,000,898,648 bytes** with the semi summary resident - only
~72.8 MB under the cap before inner runtimes. Unless the contraction is provably
in-place/no-double-allocation, shrink may reduce steady state while worsening the decisive
peak. No asymptote credit: the ~sf=11 power-of-two ceiling stands regardless.

### Out of scope (named)

Membership-only stages are already page-free
(`membership_only_projected_semi_anti_existence_writes_no_pages`, `streaming.rs:14529`);
compacting their legacy presence map is a separate follow-on alias. CSR row refs and the
sharded/deferred existence build stay named alternatives - sharding remains the structural
unbounded-key answer, now over a 2.5x smaller payload.

## Acceptance

- **Primary:** q21 sf=10 reaches persisted strict `pass@compare`, or advances to a **named,
  byte-reproduced next wall** with the existence resident-state producer gone and zero
  existence page opens on the persisted run. Final task evidence records summary selection,
  rows/keys/Multi counts, completed/peak accounted bytes, and the structurally page-free mode;
  the test counter remains the direct zero-page lock. No cap change, no membership fold-in, no
  page-cache claim, no unbounded-scale claim.
- **Eligibility-discriminating locks:**
  - atomic selection of **both** summary stages before any chain scan/file creation, proven for
    both operand orders; one rejected stage selects the unchanged two-spilled path for both;
  - a structural-recognition lock covering the reject matrix: same-side columns, out-of-range
    ordinals, casts/wrappers, nullable preserved column, nullable existence column, non-Int64
    key or value, multi-root, equi, membership-only - each falls back with zero summary I/O;
  - summary-vs-spilled-twin parity on **both** LeftSemi and LeftAnti for: missing key, one
    repeated value, two distinct values, same-supplier probe, different-supplier probe;
  - zero existence page opens/reads on the summary path (test counter, the 419/420 pattern);
  - exact bucket/entry/bitset capacity accounting (accounted == computed) plus a low-cap
    guard-armed fixture and a growth-boundary lock proving no implicit allocation occurs;
  - a **two-runtime combined-accounting lock**: the owner-level check fails under the combined
    total even when both isolated summaries fit;
  - cleanup locks: first-summary + runtime cleanup on second-stage failure after commitment;
    scratch empty on every error exit.
- **Controls:** q18 reproduces its exact wall (`streamed inner join build spill writer`,
  536,899,968 / 536,870,912); all **eleven** spill-bearing greens (q03 q07 q09 q10 q12 q13 q14
  q16 q19 q20 **q22**) pass@compare with forensic signatures (q22: 31,509,620 spill bytes).
- **Full engine gate** after fmt (`--no-fail-fast` both crates + architecture); de-branded; no
  `.perf.data`.

## Guardrails

- **Structure-first recognition; chain-owned selection** - no residual-coincidence triggering
  through the generic preparer.
- **Full-domain representation** - no sentinel, no data-dependent narrowing, no per-entry tag.
- **Combined accounting at the owner** - never two independent guards, never a nominal
  per-stage allowance, never crediting shrink before it completed.
- **Committed arm** - no fallback/rescan after the first insert; loud errors after cleanup.
- **Unrecognized shapes keep today's build unchanged** - the fallback path's guard and producer
  stay armed.
- **Won ground is a control** - eleven greens + the q18 wall.

## Risks

- **The ~sf=11 ceiling** -> named, byte-derived (power-of-two doubling + live pair), and
  honest; the sharded follow-on inherits a 2.5x smaller payload.
- **Combined-envelope surprises** (inner-runtime residency, growth transients) -> Phase 0
  measures before the cut; the stop-line compares shared-key/sharded rather than weakening
  accounting.
- **Recognizer over-trigger** -> chain-owned selection + the reject-matrix lock.
- **Three-valued logic** -> nullable columns are fenced out at recognition, not handled
  approximately.
- **A wall behind the wall** -> reach honesty; q21's probe volume is measured in Phase 0.

## Forward Arc

q21 green moves the board to 17/22 and closes the resident-state family's third driver. The
sharded/deferred existence design remains the structural unbounded-key answer, now cheaper by
construction. Remaining threads: q18 (SpillBytes quota + unreached membership wall + mixed-combo
handoff), q02 (join-output monolith, the C-spec driver), q05 (diagnosis-first OOM), q11/q17
(correctness-policy), and the q10 speed thread. The membership presence-map compaction is a
small named alias if a driver appears.

## Grounding evidence (file:line)

Existence build + guard: `exec/join/spill.rs:2052-2089` (single build; 16x footprint cap),
producer bytes from the 415/417/419 rows (crossing at ~batch 30,837 x 1,024). Preparation seam:
`exec/join/streaming.rs:10306-10380` (`membership_only` at `:10342`, retained projection at
`:10316-10325`). Runtime coexistence: `:9490-9520`. Probe page grouping: `spill.rs:2466-2508`.
Membership-only page-free: `streaming.rs:14529` (pinning test). Chain extractor residual
requirement: `:2315-2460` (`match_filter: Some` on replay-3/replay-4). Census + projections:
419 results (distribution table) + the Phase-0 re-census in this cycle's wip drafts.
Introspection surface: `BoundExprProgram::single_root()/node()`, `ResolvedColumn`
(ordinal/type/nullability); joined-layout split precedent in
`retained_existence_columns_for_semi_anti_stage`. Capacity arithmetic: 404,750,336 per stage /
809,500,672 pair / 264,241,152 headroom; shrink: 28,657,608 + 48,757,832 -> 732,085,232
completed, ~1,000,898,648 transient during a reallocating anti shrink; ceiling ~sf=11.2
(16,777,216 / 15,000,000).
