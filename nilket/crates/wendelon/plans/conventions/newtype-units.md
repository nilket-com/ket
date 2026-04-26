# Newtype-unit pattern

For any fundamental quantity in Wendelon — hash, offset, timestamp, id,
epoch, ordinal, byte-count, bit-count — define a distinct
`#[repr(transparent)]` newtype. Primitives like `u64`, `u32`, `u8`, and
`[u8; N]` must not cross module boundaries unwrapped.

## Rule

- Add a new unit to `src/schema/units.rs` via the `impl_unit!` macro
  (or a hand-rolled struct for non-primitive widths like `Identity([u8; 32])`).
- Conversions between units go through explicit `From` impls, not
  `as` casts. Never widen / narrow a newtype with `as` — that's the
  exact bug class newtypes prevent.
- Newtypes must implement `Pod + Zeroable` so zero-copy
  (`bytemuck`) round-trips still work end-to-end.
- A newtype should expose `.get()` (returning the inner primitive)
  and a constructor that enforces any range invariants. Validation
  lives in the constructor; downstream callers get to assume the
  invariant holds.

## Why

Confusing two same-shape quantities (e.g., a slab byte-offset vs. a chunk
byte-offset, or a 64-bit hash vs. a 64-bit ring locality) is a class of
bug the type system can eliminate at compile time. Letting raw primitives
flow through APIs turns "did I pass the right one?" into a runtime
question. The newtype discipline makes unit confusion a compile error.

## Examples already in the codebase

`Identity([u8; 32])`, `Locality(u64)`, `SlabId`, `LogIndex`, `Epoch`,
`TenantId`, `TableObjectId`, `BitsPerRow`, `HllPrecision`,
`SmallSetValueCap`, `SmallSetByteCap` — all in `src/schema/units.rs`
or `src/schema/table.rs`.

## How to apply in review

When a diff introduces `pub fn foo(value: u64)` or a struct field
`bytes: u32`, ask: is this a generic count, or is it a *specific kind*
of count? If specific, push back and ask for a newtype. If the
newtype already exists, push back and ask the caller to use it.
