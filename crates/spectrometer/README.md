# Spectrometer

`spectrometer` is an offline behavioral concept-discovery evaluator. It is
not linked into Baryon or libaryon's runtime. The checked-in pilot is
deliberately contaminated and may tune a candidate method; it cannot decide
ADR 0004 or act as the sealed holdout.

Run the syntax, fuzzy-floor, and BM25 matrix:

```text
cargo run -p spectrometer -- --root .
```

Resolved caller/callee features use one persistent offline
`rust-analyzer` LSP session. Pass `--rust-analyzer PATH` to select the
binary. If resolution is disabled or unavailable, those ablation cells are
reported as absent rather than silently falling back to span-only features.
For a fast extractor-only iteration:

```text
cargo run -p spectrometer -- --root . --no-resolve
```

Static vectors are optional and are never downloaded:

```text
cargo run -p spectrometer -- \
  --vectors /models/frequency-ranked.vec \
  --vectors-sha256 EXPECTED_HEX \
  --report /tmp/spectrometer-report.json
```

The text-vector backend supports exact vocabulary only. A fastText `.vec`
export does not carry the binary model's character n-gram tables, so the
report exposes missing-token coverage instead of claiming OOV composition.
Rank-SIF reports both the complete feature bag and the configured
BM25-contribution salience cuts. Salience ordering is deterministic: score
descending, then term ascending.

For resolved features the harness waits for rust-analyzer's experimental
server-status notification to report healthy, quiescent workspace analysis
before issuing batch requests. It deliberately does not reuse libaryon's
interactive one-request-at-a-time navigation seam.

The embedder column is deliberately absent until a later plan specifies a
complete local-model contract.

The checked-in contaminated-pilot evidence and its deliberately negative
result are recorded in [PILOT_RESULTS.md](PILOT_RESULTS.md).
