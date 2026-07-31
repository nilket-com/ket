# Contaminated pilot result

This is tuning evidence only. It does not decide ADR 0004 and must not be
copied into the sealed holdout.

## Decisive reading: MiniLM compositional tier

Run on 2026-07-31 at clean specimen revision
`7f76c8d7716f06edced9b314a08184ffeeb89c63`; the uncommitted Spectrometer
implementation was structurally excluded from its own corpus. The run used:

- `rust-analyzer 1.97.1 (8bab26f 2026-07-14)`;
- checked-in `candidate.toml` (`extractor_version = 1`, config SHA-256
  `3f51513a19f9cc1cb116275ca7235ee3d89ad0abee1b5521a4ea9a32d4d11098`);
- `sentence-transformers/all-MiniLM-L6-v2` at revision
  `1110a243fdf4706b3f48f1d95db1a4f5529b4d41`, through the
  Candle CPU/f32 backend;
- Linux x86-64, release build, four fixed Rayon workers.

The artifact hashes were:

- `model.safetensors`:
  `53aa51172d142c89d9012cce15ae4d6cc0ca6895895114379cacb4fab128d9db`;
- `tokenizer.json`:
  `be50c3628f2bf5bb5e3a7f17b1f74611b2561a3a27eeab05e5aa30f411572037`;
- `config.json`:
  `953f9c0d463486b10a6871cc2fd59f223b2c70184f49815e7efbcab5d8908b41`.

The Rust backend matched the checked-in sentence-transformers cosine fixture
with maximum absolute error `9.537e-7`. The corpus again contained 258 files
and 5,956 source-written functions, and the quiescent batch resolver exactly
reproduced 61,820 observed references: 18,478 in-corpus and 43,342 external or
unresolved.

The recorded run evaluated the decisive discovery surface only: own name
excluded, literals off, all four feature depths, and all three rendering
arms. The other declared cells are explicitly absent in the JSON report and
remain runnable as auxiliary ablations. No strict embedder cell earned a
holdout run:

| Strict discovery features / rendering | hit@1 | hit@3 | MRR | supervisor rank |
| --- | ---: | ---: | ---: | ---: |
| span / channel order | 0.00 | 0.00 | 0.0015 | 1,462 |
| span / salience order | 0.00 | 0.00 | 0.0041 | 64 |
| span / chunked max | 0.00 | 0.00 | 0.0074 | **22** |
| callees / channel order | 0.00 | 0.00 | 0.0016 | 1,468 |
| callees / salience order | 0.00 | 0.00 | 0.0041 | 158 |
| callees / chunked max | 0.00 | 0.00 | 0.0067 | 38 |
| callers / channel order | 0.00 | 0.00 | 0.0020 | 1,528 |
| callers / salience order | 0.00 | 0.00 | **0.0104** | 84 |
| callers / chunked max | 0.00 | 0.00 | 0.0074 | 44 |

The best-MRR cell's six canonical ranks were 84, 2,934, 17, 68, 751, and
1,374. Its apparently near results are still not a usable retrieval boundary:
none reached the top three, so distinction remained `n/a`, exactly as the
corrected metric requires. Chunking recovered more of the long orchestrator
than truncation did (`render_frame` reached rank 22), but not enough to make
the method a candidate.

The round-three decision is therefore **none passes**. No method is frozen as
a discovery candidate, no sealed holdout is authored or run, and no embedder
enters the editor. The deferred concept index may resume as the curated
precision layer. A tier-5 LLM analysis would be a separate future decision,
not an automatic fallback from this result.

Operationally, call-graph/corpus construction took 353.9 s. The 12 declared
strict embedder cells consumed 1,380.1 aggregate index-build seconds with
cross-cell cache reuse; the run retained 42,296 distinct token sequences and
peaked at 1,661,224 KiB RSS excluding the separate rust-analyzer process.
For the representative `render_frame` symbol, uncached caller-document
embedding took 68.8 ms in channel order, 58.4 ms in salience order, and
206.6 ms across all chunked windows. Ranked symbol identities were identical
across the two cached evaluation passes in every cell.

## Superseding reading: 300d + salience

Run on 2026-07-31 at corpus revision
`e6f6404367fc734358ac4581b80a1f8e8cbcc004`, with:

- `rust-analyzer 1.97.1 (8bab26f 2026-07-14)`;
- the checked-in `candidate.toml` (`extractor_version = 1`, config SHA-256
  `d9018d3884631e89671c7f7c0d82c8d4430f3194f5ee1768f0c24cc8bfdcafbb`);
- GloVe 6B 300d text vectors, SHA-256
  `91125602f730fea7ca768736c6f442e668b49db095682bf2aad375db061c21ed`;
- Linux x86-64, release build.

The corpus contained 258 files and 5,956 source-written functions. The
extractor waited for rust-analyzer to report `health=ok` and `quiescent=true`
before issuing batch requests. Two independent builds then produced the same
call graph exactly: 61,820 observed references, partitioned into 18,478
in-corpus and 43,342 external or unresolved references.

No implemented tier earned a holdout run:

| Retrieval / strict discovery features | hit@1 | hit@3 | MRR | distinction | FAR |
| --- | ---: | ---: | ---: | ---: | ---: |
| shipped `:fz` raw-source floor | 0.00 | 0.00 | 0.0035 | n/a | 0.00 |
| BM25, span, own name excluded, literals off | 0.00 | 0.00 | 0.0000 | n/a | 0.00 |
| rank-SIF, span, all terms | 0.00 | 0.00 | 0.0007 | n/a | 0.00 |
| rank-SIF, span, top 16 salient terms | 0.00 | 0.00 | 0.0007 | n/a | 0.00 |
| rank-SIF, span, top 32 salient terms | 0.00 | 0.00 | 0.0009 | n/a | 0.00 |
| rank-SIF, span, top 64 salient terms | 0.00 | 0.00 | 0.0007 | n/a | 0.00 |
| rank-SIF, callers, all terms | 0.00 | 0.00 | 0.0008 | n/a | 0.00 |

The 300d queries retained the first round's 92.86% mean exact-vocabulary
coverage. `render_frame` ranked 1,133 with the full strict span bag and moved
backward to 1,358, 1,616, and 1,402 at salience 16, 32, and 64. The best
strict call-edge result put it at rank 781. Salience truncation therefore did
not reveal a diluted supervisory signal; it usually removed more useful
context than noise. The best MRR anywhere in the full ablation matrix was
0.0011, still with zero hit@3.

The first report's `distinction = 1.00` for `:fz` was a metric flaw: it
rewarded the preferred target for outranking the comparison target even when
both were far below any useful result boundary. Distinction is now supported
only when at least one member reaches the top three; otherwise it is `n/a`.
No second-round cell supported a distinction probe.

The remaining cheap, sanctioned knobs have now been exercised without
post-hoc threshold tuning. A small embedder has earned a buildable
specification as the next experiment pair. This still does not authorize an
editor integration, ADR 0004 acceptance, or a sealed holdout run.

## Operational evidence

- Full corpus + quiescent resolved call graph build: 355.2 s.
- Incremental single-file re-index: 4.26 ms.
- Harness peak RSS: 649,396 KiB (the separate RA process is not included).
- Rank-SIF strict caller-cell query latency: about 1.57 ms/query.
- Rank-SIF strict caller-cell logical feature index: 2,597,015 bytes.
- Ranked symbol identities were identical across the harness's two
  consecutive evaluations in every available cell.

Reproduce with a matching external vector file:

```text
cargo run --release -p spectrometer -- \
  --root . \
  --vectors /path/to/glove.6B.300d.txt \
  --vectors-sha256 91125602f730fea7ca768736c6f442e668b49db095682bf2aad375db061c21ed \
  --report /tmp/spectrometer-pilot.json
```

## Superseded first reading

The 2026-07-30 GloVe 6B 50d run also produced zero hit@3. Its call-edge
features are superseded because the original client began querying before
rust-analyzer finished initial workspace analysis: only 8,121 references
resolved inside the corpus, versus the stable 18,478 above. The lexical and
vector failures motivated the stronger 300d and salience reading, but the old
graph counts are not accepted evidence.
