# ADR 047 Row Tombstone Fixtures

These fixtures describe the canonical row-tombstone scenarios used by
the ADR 047 implementation tests. They are text fixtures rather than
serialized slab bytes so the contract stays stable while the storage
encoding remains owned by the Rust helpers.

The fixed table shape is an append-only table with user columns:

- `id: Int64`
- `name: LargeUtf8`

All tombstone records are tombstone-only multi-row records: storage
synthetic columns are present, user columns are absent, and
`__row_tombstone__ = true`.
