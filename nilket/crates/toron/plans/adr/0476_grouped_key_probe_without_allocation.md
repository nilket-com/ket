# ADR 476 - probe grouped keys without allocating per row

ADR 475 left m04 (`GROUP BY l_returnflag`, three groups) untouched at 21.7× the best
reference — the hoist reached those loops, so grouped cost is not type dispatch. The
remaining per-row work is visible: every row builds a fresh `Vec<GroupValue>` key
and hands it to `entry_or_init` **by value**, so 48M rows allocate 48M vectors to
find three groups. Probe by borrowed slice; allocate only on insert.

## The mechanism

- `entry_or_init(key: Vec<GroupValue>, ...)` (`group_table.rs:141`) takes ownership,
  but everything it calls — `lookup_entry`, `find_bucket`, `find_bucket_in` —
  already operates on `&[GroupValue]`. The owned `Vec` is needed only on the insert
  path, which for m04 happens three times in 48M rows.
- Both grouped call sites build the key per row with
  `group_arrays.iter().map(|a| group_value(a, row)).collect::<Vec<_>>()`.
- `GroupValue::Utf8(String)` means a string key column also allocates a `String`
  per row (m04 groups on `l_returnflag`, a Utf8 column) — a second, separable
  allocation this cut does **not** address; it is measured and named, not fixed.

## Decision

1. `entry_or_init_probe(&mut self, key: &[GroupValue], ...)` performs lookup on the
   borrowed slice and calls `key.to_vec()` **only** when inserting a new group.
   `entry_or_init` remains for owned-key callers, delegating to the same path so
   behaviour cannot diverge.
2. Both grouped loops hoist a reusable key buffer outside the row loop
   (`clear()` + `push` per row) and probe with `&buf`.
3. Resident-byte accounting is preserved exactly: the guard still charges
   `group_key_resident_bytes` for the **inserted** key (the `to_vec` result), never
   for the scratch buffer, so memory bookkeeping and spill thresholds are unchanged.

## Acceptance

- Grouped semantics unchanged: same groups, same order-independence, same null-key
  handling, same `AggState` initialisation, same error paths and messages.
- Memory accounting unchanged — the guard sees the same bytes for the same
  insertions (a test pins that inserting N keys charges what it charged before).
- Full engine suite, architecture, and the armed 22-query parquet correctness sweep
  green.
- Ladder before/after for m04 and m05 recorded, with the per-row grouped cost as the
  headline; if the remaining gap is the `String` key allocation, that is stated as
  the next candidate with its own number.

## Guardrails

- **The spill path is downstream of the guard** — if accounting shifts, spill
  behaviour shifts, and that would be a regression regardless of speed.
- Scope is the probe path only: no hash change, no key encoding change, no
  vectorised grouping. A key-encoding rewrite (fixed-width or interned keys) is a
  separate cut this one is designed to inform.
- No board wall claim; the ladder's arithmetic is the evidence.

## Forward

Whatever remains after the allocation is removed — string interning, hash cost, or
the `GroupValue` enum's width — becomes the next cut, chosen by the same
subtraction method. m06's join gap stays queued behind it.
