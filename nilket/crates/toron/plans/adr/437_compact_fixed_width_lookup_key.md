# ADR 437 - Order-preserving compact fixed-width group key

Use one **16-byte inline compact key as the sole retained key** for eligible fixed-width grouped-fold
layouts. The encoding must preserve both `GroupValue` equality and `GroupValue::Ord`, so compact
finalization can sort the compact key directly and decode group columns into Arrow arrays without
retaining or rebuilding `Vec<GroupValue>` keys. Keep the generic full-key path unchanged for every
ineligible layout.

This revises the first ADR 437 implementation after measurement rejected its trade. That version cut
the sampled lookup/equality cost by 40.69%, but its q13 end-to-end gain was only 3.69%, it added
33.6 MiB of final ownership and 50.3 MiB of rehash peak, and its event-floored occupied `>16` tail
spread was 0.27154 against a locked 0.25 gate. It is not sealable as written.

## Context

ADR 436 named successful full-key equality as q13's next producer. The first ADR 437 implementation
proved the representation lever: q13 full-key construction fell from 15.3 million to 1.5 million and
the lookup/equality frame fell from a 22.86% to 14.06% median share. It nevertheless stored each
compact key beside a retained full key because ADR 435 finalization sorted `(full_key, states)` pairs.
The duplicate representation converted a local CPU win into a project-level memory regression and a
sub-band end-to-end result.

There are only two production grouped-finalize callers. Both immediately turn sorted group keys into
Arrow columns and consume aggregate states. They do not require a retained `Vec<GroupValue>` table.

## Decision

### Order-preserving layout

Configure a compact layout once from the evaluated group-array `DataType`s. Every column contributes
a one-byte null marker followed by a fixed-width payload. Admit only supported layouts whose total
width is at most 16 bytes. `Utf8`, `Decimal128` (17 bytes with its marker), over-width, and unsupported
types remain on the exact generic route.

Within one admitted layout, lexicographic compact-byte order must equal the derived lexicographic
`Vec<GroupValue>::cmp` order:

- marker `0` is null and marker `1` is non-null, matching `GroupValue::Null` before the typed variant;
- Boolean is `0` / `1`;
- signed integers, dates, and timestamp values flip the sign bit and use big-endian bytes;
- floats use the existing signed-zero/NaN canonicalization, then store the canonical **unsigned bit
  pattern** in big-endian order.

The float rule intentionally preserves the current derived `GroupValue::Float32(u32)` /
`Float64(u64)` ordering. An IEEE numeric total-order transform would change raw batch order and is
therefore rejected. Types and timestamp units are fixed by the layout and need no per-value
discriminant. Unused inline bytes stay zero.

### Sole compact ownership and finalization

An admitted fold retains compact keys and aggregate states only. Occupied and vacant lookup never
construct a full key. Rehash allocates and replays compact keys, states, and buckets; it allocates no
full-key outer table or payload.

Finalization returns a borrowed route-aware result:

- generic entries retain ADR 435's sorted `(full_key, states)` behavior;
- compact entries sort borrowed `(compact_key, states)` pairs lexicographically by compact key.

The compact result decodes each requested group column directly into its Arrow array. It never
materializes a `Vec<GroupValue>` per group. Aggregate output reads the aligned states through the same
route-aware result. Generic finalization and output stay byte-identical.

### Measurement decision

The revision is sealable only if it fixes the rejected trade, not merely if it compiles:

- q13 final current ownership and projected rehash peak must both fall below the ADR 435 full-key
  baseline (597,440,688 and 511,705,264 bytes respectively); no cap increase;
- the unprofiled q13 median must improve beyond the existing 10% materiality band against the
  22,257.249 ms ADR 435 baseline;
- the predeclared five-child event-floor tail rule must pass, including occupied `>16` spread at most
  0.25;
- the inclusive lookup/equality cycle frame must remain materially below ADR 436;
- the measured compact subset must remain `pass@compare`, and q13 must pass under 4 GiB/no swap.

If the order-preserving sole-key revision cannot meet those bars, remove the compact mechanism and
return to the sealed ADR 436/435 implementation. Do not seal a sub-band, memory-regressing, or
gate-failing variant.

## Acceptance

- Pairwise tests prove compact equality iff full-key equality **and** compact comparison equals
  full-key comparison for admitted null, Boolean, integer, float, date, timestamp, and multi-column
  extrema. Direct raw-batch tests prove output order and values, not only canonical digest equality.
- Compact finalization decodes exact Arrow arrays, including nullable values, float canonical bits,
  and UTC timestamp types. No compact execution path retains or reconstructs a full group-key vector.
- Forced-growth tests prove compact rejection occurs before allocation and preserves buckets, keys,
  states, and ownership. Generic rejection and finalization remain exact.
- The profile receipt is versioned rather than silently reinterpreted. It identifies the lookup/finalize
  route, compact/full retained-key counts, and direct compact-finalize rows while retaining ADR 436's
  four-phase reconciliation.
- Five fresh ledger and five fresh symbol-rich profile children satisfy the operation, overflow,
  tail-stability, output-parity, and inclusive-cycle rails.
- One warmup plus seven unprofiled q13 rows satisfy invariant fingerprint/digest/schema/rows/spill,
  spread at most 0.20, at most one retained retry, and the materiality rule above.
- The empirical compact subset passes persisted strict comparison. The full source/test/golden gates
  pass. Board status remains 22/22, but this is not a full-board redux.

## Guardrails

Residency distinguishes current table ownership, transient rehash peak, and finalize-sort scratch.
Every compact growth and finalize allocation is guarded before allocation. Output Arrow arrays remain
the caller's output ownership and are not cross-summed into group-table ownership.

Keep the generic route exact. Hold `exec.rs` at no more than 7,499 lines. Box new held state if the
async fold body exceeds the deep-recursion stack budget. Profiling remains off by default with locked
output and telemetry parity. No query identity appears in executor code, tests, or mechanism names.

## Forward arc

Only after this revision clears all three rejected product-level bars should q13 be re-profiled for a
new wall. If it does not, remove the compact path; do not layer another optimization over a failed
representation trade.
