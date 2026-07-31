# Newtype-unit pattern

For any fundamental quantity in Toron — id, ordinal, count, epoch,
attempt number, partition index, byte-count — define a distinct
newtype. Primitives like `u32`, `u64`, `usize`, and `Uuid` must not
cross module boundaries unwrapped.

## Rule

- Add the newtype next to the domain it belongs to —
  `src/distributed/identity.rs` for cluster-coordination ids,
  `src/ir.rs` / `src/ir/execution.rs` for IR-level quantities,
  `src/distributed/task.rs` for task-shaping quantities. Toron has
  no central `units` module; a unit lives near its use.
- Conversions between units go through explicit `From` impls, not
  `as` casts. Never widen / narrow a newtype with `as` — that is
  the exact bug class newtypes prevent.
- A newtype should expose `.get()` (or a named accessor returning
  the inner primitive) and a constructor that enforces any range
  invariant. Validation lives in the constructor; downstream
  callers get to assume the invariant holds.
- A tuple newtype (`pub struct StageId(pub u32)`) is fine when
  there is no invariant to enforce; reach for a validating
  constructor the moment there is one (e.g. a count that must be
  non-zero, an ordinal that must be in-range).

## Why

Confusing two same-shape quantities — a `StageId` vs. a `TaskId`,
a `PartitionId` vs. a `PartitionCount`, a `ColumnOrdinal` vs. a
raw projection index — is a class of bug the type system can
eliminate at compile time. Letting raw primitives flow through
APIs turns "did I pass the right one?" into a runtime question.
The newtype discipline makes unit confusion a compile error.

This matters most on the distributed path, where a single task
spec threads `QueryId`, `StageId`, `TaskId`, `AttemptId`,
`PartitionId`, and `ExchangeId` — six `u32`-shaped quantities
that would be trivially swappable as bare integers.

## Examples already in the codebase

`QueryId`, `StageId`, `TaskId`, `AttemptId`, `PartitionId`,
`ExchangeId`, `TaskIdentity` (in `src/distributed/identity.rs`);
`MorselId` (in `src/ir.rs`); `ColumnOrdinal` (in
`src/ir/execution.rs`); `PartitionCount` (in
`src/distributed/task.rs`) — all distinct newtypes over `u32` /
`Uuid` rather than bare primitives on the APIs that pass them.

## How to apply in review

When a diff introduces `pub fn foo(value: u32)` or a struct field
`count: u32`, ask: is this a generic count, or is it a *specific
kind* of count? If specific, push back and ask for a newtype. If
the newtype already exists, push back and ask the caller to use
it. A bare `u32` on a distributed-path signature is almost always
a unit that should be wrapped.
