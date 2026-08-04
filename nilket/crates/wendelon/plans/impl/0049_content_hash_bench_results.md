# ADR 049 Content-Hash Bench Results

Bench target:

```bash
cargo bench --bench content_hash
```

The bench reports isolated ADR 049 hot-path costs:

- `canonical-encode`: sparse type-tagged canonical row encoding.
- `admission-identity`: content-hash write admission with
  `IdentityProjection`.
- `admission-table-spread`: content-hash write admission with
  table-spread locality.
- `duplicate-lookup-half-visible`: visible-key duplicate lookup with
  half of submitted keys present in the visible set.

The 2026-04-29 promotion run covers the required 100k / 1M row
matrix. The admission benchmark chunks rows into legal table-write
batches because a single typed table write is capped at 65,536 rows.

## 2026-04-29 Promotion Gate

Command:

```bash
cargo bench --bench content_hash
```

Output:

```text
content_hash case=canonical-encode rows=100000 iterations=5 total_ns=406273472 ns_per_row=812 rows_per_sec=1230698.12
content_hash case=admission-identity rows=100000 iterations=5 total_ns=589925216 ns_per_row=1179 rows_per_sec=847565.06
content_hash case=admission-table-spread rows=100000 iterations=5 total_ns=658413395 ns_per_row=1316 rows_per_sec=759401.32
content_hash case=duplicate-lookup-half-visible rows=100000 iterations=5 total_ns=10680547 ns_per_row=21 rows_per_sec=46814081.71
content_hash case=canonical-encode rows=1000000 iterations=5 total_ns=4208932603 ns_per_row=841 rows_per_sec=1187949.65
content_hash case=admission-identity rows=1000000 iterations=5 total_ns=6010785250 ns_per_row=1202 rows_per_sec=831838.07
content_hash case=admission-table-spread rows=1000000 iterations=5 total_ns=6642291425 ns_per_row=1328 rows_per_sec=752752.28
content_hash case=duplicate-lookup-half-visible rows=1000000 iterations=5 total_ns=417399398 ns_per_row=83 rows_per_sec=11978934.38
```

Interpretation:

- Canonical encoding stays near 0.8 microseconds/row at both 100k
  and 1M.
- Full content-hash admission with `IdentityProjection` stays near
  1.2 microseconds/row at 1M.
- Table-spread locality adds a small amount over identity projection
  and stays under 1.4 microseconds/row at 1M.
- Duplicate lookup is not the dominant cost; canonical encoding and
  BLAKE3 identity derivation dominate the admission path.
