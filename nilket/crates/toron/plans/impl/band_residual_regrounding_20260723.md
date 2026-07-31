# Band residual re-grounding — 2026-07-23

Protocol re-grounding of the ADR 503 board's 160–275 ms residual band (m00–m03, m07–m10) into
engine-vs-gauge, on the sealed `672398aa` binary (8,192 coordinate, strict sf=8, 2 GiB/2 GiB,
hash-skip on). No decision; it answers the ADR 505 forward fork — is the encoded-column scan contract
the top of the board, or is something cheaper hiding in the band? Method: `engine = execute −
parquet_decode` (the ADR 500/501 decode-floor separation); m00/m01 decode no column, so they pin the
pure scan-stream-framework floor (~190–240 ms).

| rung | execute ms | decode ms (gauge) | engine ms | engine above ~200 ms framework |
|---|---:|---:|---:|---:|
| m00 scan proof | 241 | 0 | 241 | framework only |
| m01 count-shortcut | 190 | 0 | 190 | framework only |
| m02 SUM(qty) | 271 | 77 | 195 | ~0 |
| m07 COUNT(qty) | 227 | 77 | 149 | <0 |
| m08 COUNT(orderkey) | 267 | 106 | 161 | <0 |
| m09 SUM(orderkey) | 319 | 105 | 214 | ~14 |
| m03 predicate+col | 426 | 186 | 240 | ~40 |
| m10 two-col decimal | 484 | 187 | 296 | ~96 |

## Finding

The band is **gauge-dominated**. Each rung = arrow-rs parquet **decode** (0–187 ms; production scans
the native store, not parquet, so this is not a production engine cost) + the **scan-stream framework**
(~190–240 ms of per-batch async over the parquet reader's batches — the ADR 500 gauge artifact;
production emits large in-memory batches, not thousands of parquet batches). The real
**engine-arithmetic above that floor is small**: only m10's per-row decimal arithmetic (~96 ms) and
m03's filter (~40 ms) are notable; m02/m07/m08/m09 are ~zero engine over the floor. The high 9–24×
ratios on m00/m01/m07/m08 are the arrow-rs-vs-native decode/framework gap, not engine deficiency.

## Fork resolution

- **The encoded-column (dict/RLE-preserving) scan contract is the top of the board.** Nothing in the
  band is a larger engine lever than m04's producer-gated ~290 ms fold. The band does not surface a
  cheaper-and-bigger engine target.
- **Runner-up, self-contained:** m10's per-row decimal arithmetic (~96 ms, the "per-row dispatch
  width" the ladder labels) is a real engine cut that needs no cross-ring contract — a candidate if the
  encoded-column design is judged too heavy to open immediately.
- Neither is pre-authorized. The encoded-column contract is a storage/scan design cycle with an
  interior/exterior placement question for council; it is now grounded as the top, not assumed.
