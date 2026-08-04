# ADR 458 - reservoir spill page coalescing: rows out, pages out

The reservoir spill writer emits one IPC page per incoming partitioned batch, so page
count — and therefore spill bytes and write/read time — tracks *batch arrival*, not
row volume. Coalesce per-partition writes to a target page size, then re-verdict one
measured cell with the paired instrument. This is the mechanical explanation for the
bridge's immaterial sf=8 verdicts, found in the sealed cohorts' own gate-zero ledgers.

## The grounded diagnosis (from 457's gate-zero profiles, both arms)

- Ledger (`spill_io`, sf=8, 64 MiB cell): OFF writes **749,840 pages / 47,989,007
  rows** (~64 rows/page, ~2.2 KB/page); ON writes **735,285 pages / 11,742,021 rows**
  (~16 rows/page, ~1.4 KB/page). The runtime filter removes 75.5% of rows but only
  **1.9% of pages**.
- Bytes/row inflation is arm-correlated at every measured cell (sf=4: 52.4 → 176.8
  B/row OFF→ON; sf=8: 33.8 → 89.2): filtered batches are sparser, so per-page fixed
  overhead dominates harder. Reservoir stage time tracks bytes/pages, not rows —
  `reservoir_partition_spill_write` fell only 9% under a 4× row cut.
- Code path: `write_partitioned_streaming_scan_inner_build_batches` repartitions each
  scan batch 16 ways and hands each fragment straight to
  `write_projected_batch_chunks_unindexed_profiled` (`spill.rs:1593`), which chunks
  *downward* only (`CROSS_JOIN_OUTPUT_CHUNK_ROWS` cap) and encodes every fragment as
  its own IPC page (`write_page_batch_inner` → `encode_record_batch_ipc_into`).
  Nothing coalesces upward; a 16-way split of a filtered batch yields 16 tiny pages.
- Consequences beyond the bridge: both arms pay it. OFF's ~64-row pages mean the
  whole reservoir regime's spill I/O carries massive per-page overhead — write AND
  read AND decode (decode_count == page count; read_amplification 1.0 hides it
  because it is per-page, not per-byte).

## Decision — coalesce at the writer, measured end to end

**Rail 1 — per-partition write coalescing.** Each `SpilledInnerJoinBuild` gains a
bounded per-partition accumulation buffer: partitioned fragments append until a target
page threshold (rows or bytes, e.g. the existing `CROSS_JOIN_OUTPUT_CHUNK_ROWS` and/or
a byte floor), then one concatenated IPC page is encoded and written; a final flush
seals partials. The buffer's memory is accounted against the task budget (the
accumulation is bounded by target-page-size × partition_count, and the budget
accountant must see it — an unaccounted buffer is the 334-era defect class).

Fork, decided: coalescing at the **writer** (per-partition, after repartition) rather
than pre-repartition batch concatenation — the writer sees exactly the fragments that
become pages, works for every producer of the same writer, and does not disturb the
exchange path. Pre-repartition concat would leave the 16-way split re-fragmenting
whatever it gained.

**Rail 2 — correctness before any number.** The spilled build's read side must be
byte-shape-agnostic already (pages are self-describing IPC) — proven, not assumed:
q05 correctness at a committing cell (answer-check vs the oracle), plus the full
architecture_test and the engine suite (`--no-fail-fast`, engine cut).

**Rail 3 — gate zero + paired re-verdict at the (64 MiB, 2 GiB, sf=8) cell.** After
the fix: gate zero must still pass (identical row identities — coalescing must not
change *what* is spilled, only its page shape), and the full paired cohort re-runs at
the 457 cell in a fresh output directory. Predeclared readings:

- **Ledger moves as designed** (ON page count now tracks its row count; B/row
  parity across arms) **and the bridge becomes material** at the cell → the bridge's
  sf=8 immateriality was an artifact of page-granularity overhead; the 446 line's
  standing changes again, this time with the mechanism understood.
- **Ledger moves but the verdict stays immaterial** → the overhead was real but not
  the binding constraint; the residual wall owner (per the stage profile) is named
  and the reorientation decision inherits it. An honest null with a clean apparatus.
- **Ledger does not move** → the coalescing missed the dominant page producer;
  diagnose before any cohort spend (halt after gate zero).

No cross-cell or cross-ADR wall-clock ranking; the new cohort's verdict is
within-cell, and pre-fix cohorts (451/456/457) remain sealed and are never
re-compared against post-fix numbers — the spill format changed between them.

## Acceptance

- Coalescing bounded and budget-accounted; a test pins that N small same-partition
  fragments produce one page (not N) and that a partial buffer flushes complete.
- q05 correctness green at a committing cell post-fix; architecture_test and full
  engine suite green.
- Gate zero at the 457 cell: same row identities as 457's record (47,989,007 /
  11,742,021), digests identical.
- The new cohort receipt carries the ledger evidence (page counts per arm) alongside
  the HL verdict, in mandatory order, with all three cell coordinates explicit.
- Fresh output directory; no resume against pre-fix journals (the manifest cannot
  express a format epoch — the fresh-dir rule stands in for it, stated in the
  results doc).

## Guardrails and risks

- **Do not change what is spilled.** Coalescing composes pages; any change to row
  content, order-within-partition assumptions, or projection is out of scope and
  gate zero is the tripwire.
- **Budget interaction**: bigger pages shift `SpillBytes` accounting granularity;
  the 2 GiB reserve stays explicit and `BudgetExhausted` halts as
  apparatus-inconclusive, per 456/457 discipline.
- **Blast radius**: the writer serves other spill producers; the engine suite and
  correctness rails are the containment, and behavior-neutral adjacent cleanup rides
  only if it touches this surface.
- **Do not re-judge sealed verdicts.** 451/456/457 stand as measurements of the
  pre-fix format at their cells.

## Forward arc

If the bridge turns material with honest pages, the 446 line is re-opened on new
mechanical footing and the reorientation decision gets a very different first fact.
If not, reorientation proceeds with the wall owner named. Either way, every future
spill-heavy cell inherits the page-shape fix. The engine-side `Default` retarget, the
ADR 446 12x spill-scope gap, `MAX_FILTER_BYTES` cap raise, ADR 132's comparator, q17
remeasurement, q21 residual, q19 re-diagnosis + 438 widening, q13, Cut A (439
mirror), and the generic O(1) LRU rewrite remain queued.
