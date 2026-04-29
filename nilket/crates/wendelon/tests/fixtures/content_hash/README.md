# ADR 049 Content-Hash Fixtures

Executable coverage lives in:

- `src/storage/content_hash.rs` for canonical row encoding and
  admission identity vectors.
- `src/schema/table.rs` for descriptor validation and schema
  evolution rejects.
- `src/network/data_server.rs` for synchronous duplicate absorption
  on the table-write path.

The locked fixture cases are:

- Nullable additive column with NULL value preserves canonical bytes
  and content-derived identity.
- NFC-equivalent strings, `-0.0`, and NaN variants canonicalize to
  stable bytes.
- Dictionary arrays encode by logical value, not dictionary slot.
- Nested list values are rejected by the V1 encoder.
- `IdentityProjection` derives locality from the content hash key.
- A visible duplicate write is absorbed without replacing the indexed
  row location.
