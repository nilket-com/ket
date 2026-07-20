# ADR 477 - GroupArrayReader: resolve group-column types once per batch

ADR 476 isolated the remaining grouped cost by elimination: `group_value`
(`aggregate.rs:1800`) is the same per-row dispatch anti-pattern ADR 475 removed from
the value path, one function over — up to twelve `as_any().downcast_ref::<T>()`
tests per row, per group column. Apply the proven fix shape. Nothing else.

## Decision

`GroupArrayReader` resolves a group column's concrete type **once per batch** and
exposes a typed `value(row) -> GroupValue`, mirroring `group_value`'s variant set,
conversions, null contract, and error text exactly. Both grouped loops build one
reader per group column outside the row loop and extract through it. `group_value`
is retained for cold callers and resolves through the same construction so the two
cannot drift.

**Explicitly out of scope** (separate ownership/layout decision, not smuggled in):
`GroupValue::Utf8(String)` keeps allocating an owned `String` per row. Borrowed
keys, interning, and fixed-width key encoding are a design question this cut leaves
untouched — it changes *when types are resolved*, never *what a key owns*.

## Acceptance

- **Ladder movement on m04/m05** recorded before/after with per-row grouping
  overhead as the headline; the residual is attributed (dispatch vs string
  allocation) rather than left as one number.
- **Group-table accounting and spill behaviour unchanged** — the same
  `GroupValue`s, with the same capacities, reach `entry_or_init`; the guard charges
  identical bytes for identical insertions.
- **The armed 22-query parquet correctness sweep green**, plus full engine suite and
  architecture.
- Null keys, mixed-type keys, multi-column keys, and unsupported-type errors behave
  identically (existing tests plus a reader/`group_value` parity test).

## Guardrails

- Semantics first: a faster key extractor that changes grouping, ordering, or a null
  key is a regression regardless of speed.
- No hash change, no key encoding change, no vectorised grouping.
- No board wall claim; the ladder's arithmetic is the evidence.

## Forward

Whatever remains after dispatch is removed is the string-key question, priced by
this cut's residual and decided on its own terms.
