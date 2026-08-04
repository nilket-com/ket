# ADR 061 Bench Results: Page-Aligned Authenticated Chunk Layout

Date: 2026-05-08

Commands run:

```sh
cargo bench --bench row_group_fetch -- --quick > /tmp/row_group_fetch_061_quick.out
cargo bench --bench row_group_fetch > /tmp/row_group_fetch_061_full.out
cargo bench --bench row_group_fetch > /tmp/row_group_fetch_061_full_final.out
cargo bench --bench row_group_fetch -- --quick > /tmp/row_group_fetch_061_quick_final.out
```

Environment:

- `rustc 1.93.0 (254b59607 2026-01-19)`
- `cargo 1.93.0 (083ac5135 2025-12-15)`
- `x86_64-unknown-linux-gnu`, Linux 6.17.0-22-generic

Scope note: the final full non-quick run was taken after enabling ADR 061 by
default. It produced 515 benchmark rows, including 65 ADR 061 rows. The quick
run was retained as a smoke check; the disposition below uses the final full
100-iteration run.

## Macro-Block Runtime

Fixture:

- 100 MiB plaintext record body.
- 13 adjacent 8 MiB plaintext requests.
- Chunk-size sweep across the closed set `{4, 8, 16, 32, 64} KiB`.
- Expected shape: one coalesced ciphertext GET and one chunk run for every
  chunk size.

| Chunk size | Chunks | GET ranges | Chunk runs | GET bytes | p50 GET bytes | p95 GET bytes | Encrypt ms | Decrypt ms | AEAD calls |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 KiB | 25,600 | 1 | 1 | 105,267,200 | 105,267,200 | 105,267,200 | 42.368 | 66.664 | 25,600 |
| 8 KiB | 12,800 | 1 | 1 | 105,062,400 | 105,062,400 | 105,062,400 | 42.137 | 64.964 | 12,800 |
| 16 KiB | 6,400 | 1 | 1 | 104,960,000 | 104,960,000 | 104,960,000 | 42.179 | 65.211 | 6,400 |
| 32 KiB | 3,200 | 1 | 1 | 104,908,800 | 104,908,800 | 104,908,800 | 40.744 | 66.226 | 3,200 |
| 64 KiB | 1,600 | 1 | 1 | 104,883,200 | 104,883,200 | 104,883,200 | 41.972 | 64.991 | 1,600 |

Disposition:

- Dense GET-count non-regression: pass. Every chunk size emits
  exactly one GET range.
- Dense p50/p95 GET-byte gates at 16 KiB: pass. Both are about
  100 MiB, comfortably above the 1 MiB p50 and 4 MiB p95 gates.
- AEAD call count: pass. Calls equal chunk count with no
  multiplication through planning.
- Decrypt CPU observation: pass on this run. Decrypt time at 16 KiB is within
  noise of the 64 KiB baseline (65.211 ms vs 64.991 ms).

## Warehouse Model

Modeled fixtures:

- 16,777,216 rows, 256 records, 65,536 rows per record, 4,194,304 plaintext
  bytes per record.
- 100,000,000 rows, 1,526 records, 65,536 rows per record, 4,194,304 plaintext
  bytes per record.
- Projection column counts: 1, 4, 16.
- Mask shapes emitted in the ADR 061 full-run matrix: `clustered_first_page`
  and `random_1p`.

Representative clustered-first-page results:

| Rows | Projection columns | Chunk size | Dense p50 GET bytes | Dense p95 GET bytes | Sparse ciphertext bytes | 64 KiB baseline sparse bytes | Sparse reduction |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16,777,216 | 1 | 16 KiB | 4,198,400 | 4,198,400 | 16,400 | 65,552 | 3.997x |
| 16,777,216 | 4 | 16 KiB | 4,198,400 | 4,198,400 | 65,600 | 262,208 | 3.997x |
| 16,777,216 | 16 | 16 KiB | 4,198,400 | 4,198,400 | 262,400 | 1,048,832 | 3.997x |
| 100,000,000 | 1 | 16 KiB | 4,198,400 | 4,198,400 | 16,400 | 65,552 | 3.997x |
| 100,000,000 | 4 | 16 KiB | 4,198,400 | 4,198,400 | 65,600 | 262,208 | 3.997x |
| 100,000,000 | 16 | 16 KiB | 4,198,400 | 4,198,400 | 262,400 | 1,048,832 | 3.997x |

Closed-set modeled reduction on the 1-column clustered-first-page shape:

| Chunk size | Sparse ciphertext bytes | 64 KiB baseline bytes | Reduction |
| --- | ---: | ---: | ---: |
| 4 KiB | 4,112 | 65,552 | 15.942x |
| 8 KiB | 8,208 | 65,552 | 7.986x |
| 16 KiB | 16,400 | 65,552 | 3.997x |
| 32 KiB | 32,784 | 65,552 | 2.000x |
| 64 KiB | 65,552 | 65,552 | 1.000x |

Random 1% masks routed to `dense_decline` in the full-run model for every chunk
size, satisfying ADR 061's random-mask gate by preserving ADR 060's dense
fallback behavior instead of claiming chunk-floor savings on anti-clustered
access.

Disposition:

- Sparse ciphertext-byte reduction at 16 KiB: pass for the modeled warehouse
  clustered-first-page shape. The measured model reports about 4x reduction
  versus the 64 KiB baseline.
- Dense p50/p95 at 16 KiB: pass. The model reports about 4 MiB dense GET
  payloads.
- Random-mask survival: pass by dense decline.

## Current Promotion State

Full-run evidence supports the ADR 061 V1 default of 16 KiB with the closed set
retained for diagnostic rows and future promotion work. The full verification
gate also includes `cargo test`, focused ADR 061 integration tests, bench
compilation, and diff hygiene; those results are recorded in the implementation
handoff rather than duplicated here.
