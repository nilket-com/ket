# ADR 382 - Implementation plan: q03 spill-decode read-path

Pairs with `plans/adr/0382_spill_decode_read_path.md`. Diagnostic-first: measure q03's spill-page decode (current
LZ4_FRAME IPC + encryption) vs candidates, decide the budget-safe lever (page-size / raw-internal candidate) or name the
inherent-data floor. q03 only; q07 = ADR 383.

## Phase 0 - confirm once + the measurement table (before code)
- **Once-vs-repeated:** confirm q03 at sf=10 decodes each page once (the 380 preload `build_pages: Vec<RecordBatch>`;
  the parallel + K=1 paths; the 378 G-pass does not re-decode a loaded partition). If any path re-decodes -> cheaper fix
  first.
- **Break down `RecordBatchDecoder::next_buffer`** (symbol-retaining build, 381 method): framing/validation vs decompress
  (LZ4) vs alloc/copy (buffer reconstruction).
- **Fill the table** for current IPC+LZ4 and each candidate (page sizes; raw/internal layout):

  `variant | page_count | rows/page | compressed bytes/page | decode ms/page | next_buffer % | decompress % |
   alloc/copy % | spill_bytes | writer_peak | guard outcome`

- Decide: page-size (if framing-bound + budget-safe) / codec choice (if decompression-bound + budget-safe) /
  raw-internal candidate (if framing/validation-bound + budget-safe) / named floor (if decompress+alloc data-bound and
  candidates do not improve it safely).

## D1 - apply the table-justified lever (or name the floor)
- **Page-size:** adjust rows/page in the spill writer; re-measure `spill_bytes`/`writer_peak`/guard - must be
  budget-safe, with any `writer_peak` increase explicitly bounded and justified.
- **Codec choice:** if decompression dominates, measure a codec/no-compression candidate under the existing spill budget;
  ship only if it cuts decode and remains budget-safe.
- **Raw/internal candidate:** a raw Arrow buffer page layout (skip IPC framing/validation) with the same encryption
  boundary and budget properties; ship only if it cuts decode without blowing `spill_bytes`/`writer_peak`. Broad (all
  spill) - reader+writer+encrypt.
- **Floor:** if decompress+alloc of the projected data dominates, **no engine change** - record the named floor + that
  IPC+LZ4 is not the wrong representation; the spill volume is the term.
- Topology-named; no qNN; no q07.

## Tests / safety net
- **Spill round-trip parity (broad):** every spill shape (inner build, semi/anti, projected) round-trips identically
  under the chosen format/page-size, **encrypted**, multi-page.
- **Budget parity:** `spill_bytes`/`writer_peak` and resident-guard decisions are recorded and budget-safe for the chosen
  variant; any `writer_peak` increase is bounded and does not change caps.
- **q03 correctness:** strict `pass@compare` sf=0.01/sf=1; persisted sf=10 if it advances/greens.
- **No q07 path touched** (partition/take/copy unchanged).

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine/spill cut)
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01   # strict pass@compare
# symbol-retaining re-profile (381 method) q03 sf=10: next_buffer share dropped OR named floor;
#   + spill_bytes/writer_peak/guard for the chosen variant; persisted sf=10 correctness if advanced/green
git diff --check
#   once confirmed; table recorded; next_buffer share dropped OR named floor; spill_bytes/writer_peak/guard no-regress;
#   round-trip parity (all shapes, encrypted); no cap bump; no q07; --no-fail-fast green; no qNN
```

## Results doc
`crates/toron/plans/impl/0382_spill_decode_read_path_results.md`:
- once-vs-repeated confirmation;
- the Phase-0 table (current IPC+LZ4 vs candidates: decode %, decompress %, alloc %, spill_bytes, writer_peak, guard);
- the chosen lever (page-size / raw-internal candidate) **or the named inherent-data floor** with evidence;
- q03 re-profile (next_buffer share before/after) + budget (spill_bytes/writer_peak/guard) no-regress;
- round-trip parity (all shapes + encryption) + q03 correctness;
- confirmation no cap bump, no q07 claim, `--no-fail-fast` green, no qNN.

## Risk / blast radius
- **Raw assumed zero-copy** -> measured candidate only; LZ4+encryption kept; gated on decode + budget.
- **Page-size raises writer_peak** -> budget-gated acceptance; bounded increases must be explicit and guard-safe.
- **Inherent decode** -> named floor.
- **Broad format round-trip break** -> all-shapes encrypted parity test.
- **Regression** -> `--no-fail-fast`.

## Seal
`impl: adr 382 spill decode read path` (or `plan: adr 382 ...` if the outcome is a named floor with no engine change).
Do not commit wip drafts or large `.perf.data`.
