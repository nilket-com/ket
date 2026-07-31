# ADR 428 - frugal q21 oracle: a shared-counter stream-merge that validates sf=10 under 4 GiB

## Context

Post-427 board: 18/22 green. ADR 427 made q21's **engine** complete sf=10 under the standard
4 GiB cgroup in 82 s (the partition-at-a-time replay-1 fix), but the canonical in-process
comparison still OOMs at 4 GiB and passes only at 6 GiB, so q21 is recorded
`engine-green / reach-only` with the named `q21_sf10_oracle_residency_gap`; the 6 GiB row is
supplemental evidence carrying no board credit. This ADR retires that gap - a **toron-tpch-only
instrument cut, no engine surface**, following the ADR 425 pattern.

The OOM is the **instrument**. `q21_oracle_from_rows` (`toron-tpch/src/lib.rs:10042`) holds two
co-resident giant maps at sf=10:

| structure | entries | ~bytes |
| --- | ---: | ---: |
| `orders: HashMap<i64, String>` | 15,000,000 | ~840 MB+ |
| `lineitems_by_order: HashMap<i64, Vec<(i64,i32,i32)>>` | 59,986,052 rows / 15M keys | ~2 GB+ |

(suppliers ~SF x 10,000 and 25 nations are negligible.) Same shape as q18's pre-425 dense oracle;
the 425 template applies (unlike q17, which is already O(eligible-parts)).

**The structural gift:** the dense oracle's counting is **already order-at-a-time** - it iterates
`for (orderkey, items) in lineitems_by_order` (`:10065`) and per order computes the semi/anti
existence result (`:10073-10107`). The only non-frugal part is materializing the full maps first.
The frugal oracle streams the same `(order, items)` groups instead of holding them, and reuses
the identical per-order counter so the semi/anti arithmetic cannot diverge.

The exact per-order behavior the frugal path must mirror (counter-fidelity is the main risk):
per order (status `F`), for each lineitem row `l1` - skip unless late (`receiptdate > commitdate`;
`==` is not late); skip unless supplier is SAUDI ARABIA; skip unless **another supplier exists**
on the order (the `EXISTS` clause, `:10093`); skip if **another supplier is also late** (the
`NOT EXISTS`, `:10099`); else `contributing_rows += 1` and `counts[s_name] += 1`. It counts per
**lineitem row** - a sole-late Saudi supplier with several late lineitems on one order counts
each.

## Decision

The public `q21_oracle_from_rows` becomes the **frugal** implementation, computing the identical
`Q21Oracle` (rows + `contributing_rows`) without either giant map. Its signature already takes
`orders`/`lineitems` as iterators, so it consumes them as ordered streams and merges - no caller
change.

### The stream-merge

1. **Small resident lookups** for the SAUDI-ARABIA filter: keep the suppliers `(name, nationkey)`
   and nations maps resident (a few MB at sf=10 - negligible beside the ~3 GB dropped).
2. **Stream-merge orders x lineitem-groups**: assert strictly increasing unique `o_orderkey`
   values on the orders stream; accept nondecreasing raw `l_orderkey` values, grouping contiguous
   equal keys, and assert strictly increasing finalized lineitem-group keys. Advance the orders
   cursor to each group's key for its `o_orderstatus`. Resident peak is one order's lineitems +
   the small supplier/nation maps + the (small) `counts`.
3. **Shared per-order counter** - the design answer to the counter-fidelity risk: the semi/anti
   block (`:10073-10107`) is one function called by **both** the frugal path and the dense
   reference, so the existence arithmetic cannot diverge by construction; parity then validates
   only *delivery* (merge yields the same `(order, items)` groups as the map). All increments in
   this helper - `contributing_rows` and every `counts[s_name]` - are **checked** (loud overflow,
   never wrap).
4. **Accumulate `counts` by `s_name`** (small, bounded by the answer), sort by the existing
   `numwait DESC, s_name`, return.

### The three assertion/merge pins (iter-1, verbatim)

- **Assertion semantics:** raw lineitem keys are **nondecreasing**; contiguous same-orderkey rows
  are one order's group (normal); a finalized group key that is not strictly greater than the
  prior finalized group, or a key that reappears after a different group, is a **loud violation**.
  The orders stream is likewise asserted strictly increasing on `o_orderkey`.
- **Drain both unmatched tails to EOF:** the two-cursor merge consumes the remainder of whichever
  stream outlives the other, so a late ordering violation in a tail is still validated - the
  assertion is load-bearing only if every group is visited. No short-circuit skips tail groups.
- **Checked increments** for `contributing_rows` and every supplier count in the shared helper.

### The honesty spine

- **Dense reference -> `#[cfg(test)]`:** the former dense body moves private under an internal
  test module as `q21_oracle_dense_from_rows` / `strict_q21_oracle_dense`, compiled out of the
  shipped crate, the structural parity reference. Public `strict_q21_oracle` stays and calls the
  frugal seam.
- **Dense-vs-frugal structural equality** (`Q21Oracle` derives `PartialEq/Eq`: full sorted `rows`
  + `contributing_rows`) at sf=0.01 and sf=1. At sf=10 only the frugal oracle runs (the dense one
  OOMs - the point), backed by the 427 answer digest.
- **The non-vacuity guard and `compare_strict_q21`/LIMIT stay byte-unchanged.**

## Acceptance

- **Primary:** the canonical in-process q21 sf=10 comparison **passes under the standard 4 GiB
  cgroup**; `q21_sf10_oracle_residency_gap` is retired; the board moves to **19/22**. The 6 GiB
  run earns no board credit.
- **Parity locks:** dense-vs-frugal structural equality (full sorted rows + `contributing_rows`)
  at sf=0.01 and sf=1.
- **Counter-fidelity edge locks** (through the shared helper on synthetic inputs, and via
  dense-vs-frugal parity): order status != F; an order with a sole supplier (EXISTS fails); a
  second late supplier (NOT-EXISTS fails); a sole-late Saudi supplier with multiple late
  lineitems (each counts); `receiptdate == commitdate` (not late); a non-Saudi late supplier
  (skipped); a lineitem group with no matching order and an order with no lineitems both
  contribute nothing; unknown supplier/nation references skip exactly as in the dense oracle.
- **Order-assertion locks:** a decreasing/reappearing lineitem group key and a
  non-strictly-increasing orders key each fail loudly; contiguous same-key lineitems succeed; a
  tail-only violation is still caught (both tails drained).
- **Scope:** the thirteen sf=10 controls and q17's oracle path untouched; the frugal path is
  q21-only.
- **Full `toron-tpch` gate; no engine surface changed; no `.perf.data`.**

## Guardrails

- **Instrument-only** - no `crates/toron` engine change; comparator, LIMIT, non-vacuity guard
  unchanged.
- **Structural parity or it does not ship** - frugal validated against the dense reference at
  every scale small enough to run both.
- **Counter-fidelity is structural** - the shared per-order helper, not just a parity test.
- **Assert, do not assume** - both stream orders are checked invariants; both tails drained.
- **4 GiB is the bar** - a 6 GiB pass earns no board credit. The cgroup limit and successful
  exit are normative; record cgroup `MemoryPeak` when the transient unit retains it and
  `/usr/bin/time` maximum RSS as the fallback when this systemd version reports it unset.
- **Won ground is a control** - thirteen greens and q17 untouched.

## Risks

- **A frugal-delivery bug** (merge groups != map groups) -> the dense parity locks at sf=0.01/1
  catch it before the unvalidated sf=10 path; the shared counter removes the arithmetic-divergence
  class entirely.
- **Out-of-order generator emission** -> loud assertion on either stream, both tails drained;
  never a silent miscount.
- **Referential-integrity assumptions** -> the merge applies status/supplier/nation filters
  explicitly (an order with no lineitems contributes nothing; an unknown supplier/nation skips),
  exactly as the dense oracle does.
- **Supplier/nation residency** -> a few MB at sf=10; if ever material, the Saudi-key pre-filter
  (iter-0 fork A) is the fallback, but it is not needed at this scale.

## Forward Arc

q21 clean-green at 19/22 closes the throughput cell's instrument follow-on and leaves three
genuinely-hard threads: q17 (oracle boundary-precision fidelity - a precision cut, not
residency), q02 (join-output C-spec rock), q05 (diagnosis-first OOM; the 426 receipt + 427
grace-hash schedule are candidate tools). The stream-merge/shared-counter frugal-oracle pattern
(425 -> 428) is the settled template for any future residency-shaped oracle gap.

## Grounding evidence (file:line)

Dense oracle + co-resident maps: `toron-tpch/src/lib.rs:10042-10061` (`orders`,
`lineitems_by_order`); order-at-a-time counting `:10065`; semi/anti block `:10073-10107` (EXISTS
`:10093`, NOT-EXISTS `:10099`, per-lineitem-row count `:10105-10106`); result build/sort
`:10110-10119`; `Q21Oracle { rows, contributing_rows }`. Public seam `strict_q21_oracle`
`:10127`. Suppliers `SupplierGenerator` (~SF x 10,000). 427 engine result + gap:
`plans/impl/427_q21_partition_at_a_time_replay1_results.md` (engine 82 s under 4 GiB; dense
oracle OOM at 4 GiB, pass at 6 GiB). Pattern precedent: ADR 425 (frugal q18 oracle, public name
preserved, dense `#[cfg(test)]`).
