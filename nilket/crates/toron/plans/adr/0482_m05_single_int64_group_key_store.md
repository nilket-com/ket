# ADR 482 - contiguous single-Int64 GroupTable keys for m05

ADR 481 established that m05's high-cardinality grouped fold is dominated by
`find_bucket_in` probing a `Vec<Vec<GroupValue>>`: every one of its 12 million
single-Int64 group keys is a separate heap allocation. Replace that storage only
for the exact shape `GROUP BY <one Int64 expression>` with a contiguous fixed-slot
key store. The generic GroupTable remains the fallback for every other grouping
shape.

## Evidence and scope

- m05's current profile attributes 28.6% self time to `find_bucket_in`; the table
  holds 12 million single-Int64 keys and probes it for 48 million rows.
- `perf stat` on m05 versus the same-row m02 scalar control records materially more
  cache and DTLB-load misses per row. Hybrid-core accounting prevents assigning those
  counters to one exact frame, but supports locality as a design constraint.
- ADR 002 requires lexicographically stable `GROUP BY` output. `finalize_sorted`
  remains in place and orders the same `GroupValue` keys as before.

This cut does not change the group-key hash policy, probe algorithm, aggregate
states, or final output ordering. It only replaces one layer of key ownership:
`Vec<Vec<GroupValue>>` becomes contiguous one-element key slots when the bound
grouping schema is exactly one `Int64` column. Null remains a normal `GroupValue`
in that slot.

## Rails

1. **Storage and accounting.** Select the compact store from the bound grouping
   schema before folding. Probe it through the existing open-addressing algorithm;
   allocate no inner key vector on insert. Resident/growth accounting must charge
   the actual outer storage and retain the existing pre-allocation guard.
2. **Correctness.** Pin compact-store lookup, null grouping, duplicate counts, and
   ADR 002 sorted output against the generic semantics. A non-Int64 or multi-column
   grouping remains generic.
3. **Board safety.** Run group-table and aggregate tests, architecture checks, the
   full engine suite, and the armed 22-query parquet correctness sweep.
4. **Price.** Commit the candidate before building a sibling baseline. Measure m05
   with alternating matched release binaries under the same feature set and settled
   load. Keep only a signal that clears paired variance; do not pool any noisy
   attempt into the verdict.

## Outcomes

- **Measured win:** retain the compact path with a receipt scoped to m05's
  single-Int64 grouping shape.
- **No measurable win or regression:** revert the code and record that removing
  the heap vectors did not convert to wall time; the next diagnosis remains probe
  topology or the contract-bound sort.
- **Correctness/accounting failure:** stop before pricing; no fallback silently
  changes the requested shape.
