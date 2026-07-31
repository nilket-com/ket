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

The MiniLM embedder is optional, offline-only, and never downloads artifacts.
Supplying it requires the exact checksum-pinned `model.safetensors`,
`tokenizer.json`, and `config.json` files named in `candidate.toml`:

```text
cargo run --release -p spectrometer -- \
  --root . \
  --embedder-dir /models/all-MiniLM-L6-v2-pinned \
  --embedder-headline-only \
  --report /tmp/spectrometer-minilm.json
```

Omitting `--embedder-dir` reports the tier as absent. Supplying an incomplete
or checksum-mismatched directory fails the run rather than producing corrupt
evidence. `--embedder-headline-only` evaluates the decisive comment-free,
own-name-excluded, literals-off surface; omit it to execute every auxiliary
ablation cell as well.

The checked-in contaminated-pilot evidence, including the deliberate
none-passes result after the compositional tier, is recorded in
[PILOT_RESULTS.md](PILOT_RESULTS.md).
