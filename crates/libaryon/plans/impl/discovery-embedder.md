# Discovery tier 4: the small embedder, fully specified

Scope: add the reserved embedder column to `crates/spectrometer` as a
**complete, buildable contract** — architecture, tokenizer, pooling,
normalization, weights format, runtime, checksums — and run the pilot
round that decides whether ADR 0004 freezes with a candidate tier or with
a deliberate none-passes rule. **Harness-only**: no editor dependency, no
integration authorization, no holdout. Deliverable is one `impl:` commit
reviewed against this plan.

## Why this tier can succeed where rank-SIF could not

Both pilot rounds establish that the failure is compositional, not
lexical: query coverage was 92.86%, the injected space was semantically
live (`with_job_dispatcher` first for *supervisor*), yet `render_frame`
sat at rank 1,133 full-bag and salience cuts made it worse. A weighted
centroid cannot represent *role-from-behavior*; attention composes token
interactions (`dispatch … drain … schedule … enforce` as an orchestration
gestalt, not an average). That is the specific capacity purchased here —
and the honest risk is stated below.

## The contract

**Only model in this pair (pinned):**
`sentence-transformers/all-MiniLM-L6-v2` at upstream revision
`1110a243fdf4706b3f48f1d95db1a4f5529b4d41` —

- architecture: 6-layer BERT encoder, hidden 384, 12 heads;
- tokenizer: WordPiece, the model's own `tokenizer.json`;
- pooling: attention-mask-weighted **mean** over final hidden states;
- normalization: L2 to unit length; similarity = dot product;
- weights: `safetensors`; one `--embedder-dir PATH` resolves exactly
  `model.safetensors`, `tokenizer.json`, and `config.json`; each expected
  **SHA-256 is pinned in `candidate.toml`**, never downloaded;
- max sequence: 256 wordpieces (model limit 512; 256 is the frozen cap —
  recorded, not silent);
- license: Apache-2.0 (recorded in the config for the record's sake).

The upstream `sentence_bert_config.json` confirms 256 as the published
sentence-transformers cap but is not a runtime input: the cap is frozen in
our candidate config and checked against the encoder's 512-position model
limit. No alternate model belongs in this pair; a failed primary model is
evidence, not permission to browse architectures.

**Runtime:** pinned workspace versions of `candle-core` + `candle-nn` +
`candle-transformers` (BERT) + `tokenizers`,
dependencies of **spectrometer only** — the workspace's editor crates gain
nothing. CPU only; no CUDA/Metal features enabled. Thread count fixed and
recorded in candidate config (four Rayon workers for this pilot; a one-worker
5,956-document slice exceeded six minutes, while four workers completed the
same identities in 3m39s) so the determinism contract
— identical ranked identities across consecutive runs, total tie-break —
holds under the existing protocol. Backend string recorded per run.
Batching is length-sorted and constrained by a fixed attention-cell budget:
32 full-length sequences or at most 256 short sequences. Original document
order is restored before ranking; scheduling therefore changes only padding
cost, never model input or identity order.

No `--embedder-dir` means every embedder cell is explicitly absent and the
ordinary run succeeds. Once a directory is supplied, missing files, checksum
mismatch, incompatible tensor names/shapes, or tokenizer/config failure are a
hard run error: corrupt evidence must never masquerade as an absent tier.

## Document rendering (the frozen text form of a behavioral bag)

A sentence encoder needs text, not a multiset. The existing
`FeatureDocument` cannot provide source order (its terms are a `BTreeMap`),
so rendering is a separate pure projection from `Corpus` + `FeatureCell`,
tested against the multiset extractor but not reconstructed from it. Per
symbol, per feature arm, render:

1. **channel-order** (primary): own name (unless excluded), span identifiers
   in source occurrence order, enclosing-context tokens, literal tokens (when
   enabled), then resolved callee/caller names in canonical `SymbolId` order;
   word-split tokens are joined by one space and occurrence duplicates remain;
2. **salience-order** (arm): tokens ordered by BM25 weight descending —
   carried despite round-two's result because truncation changes what
   ordering means (it decides *survival*, not just emphasis);
3. **chunked-max** (arm): WordPiece the complete channel-order stream without
   special tokens, partition it into payload windows, then add `[CLS]`/`[SEP]`
   per window so every encoded chunk is at most 256 tokens; score
   query-vs-symbol as the **max** chunk similarity — the long-orchestrator
   hedge that truncation alone cannot provide. Source/salience arms use the
   tokenizer's right truncation to 256 **including** special tokens.

Feature axes, own-name exclusion, literals channel, and comment stripping
are inherited unchanged from the existing extractor. Queries embed as
their raw text.

The CLI may select `--embedder-headline-only`, which evaluates all four
feature depths and all three renderings at `own-name=excluded/literals=off`.
Other embedder cells remain declared but explicitly absent in that report.
This is the decisive discovery surface and the round-three recorded run;
the complete auxiliary ablation remains runnable without changing config.

## Metrics, pilot, and decision posture

- Same pilot TOML, same metrics (hit@1/3, MRR, distinction with the n/a
  rule, FAR at a candidate threshold), same operational block plus:
  model load time, batch embedding time for the full corpus, per-symbol
  incremental embedding cost, and RSS with weights mmapped.
- Per-query `expected_rank` diagnostics remain the adjudicator — if this
  tier fails, the failure record must say *how far* it failed.
- **Both outcomes freeze ADR 0004**: pass → the embedder is the candidate
  tier, thresholds recorded, sealed-holdout sequence proceeds; fail → a
  deliberate none-passes rule freezes, the concept-index (curation layer)
  resumes as the sole shipped layer, and tier-5 LLM analysis becomes the
  only remaining discovery hypothesis — explicitly a separate future
  decision, not a fallback smuggled in.

## Honest risk

MiniLM is prose-trained; identifier soup is out-of-distribution. The
rendering arms are the hedge, not a guarantee. If the tier passes only
with comment-inclusive features, that is the concept *index* winning, not
discovery — the comment-stripped cells remain the ones that matter for
the claim.

## Non-goals

- Editor integration, `models_dir`, background embedding, revision-stamped
  caches — all post-ADR-0004 integration design.
- The sealed holdout (next link, not this one).
- Fine-tuning, distillation, GPU paths, model conversion tooling beyond
  loading the published safetensors.

## Review checklist

- [ ] All model artifacts path-supplied and SHA-256-verified. No supplied
      model means `absent`; a supplied invalid model fails closed.
- [ ] Pooling/normalization proven against a checked-in reference fixture:
      the model card's four example sentences are encoded once by the pinned
      Python `sentence-transformers` implementation; the fixture records its
      package versions, upstream model revision, artifact hashes, and cosine
      matrix. Rust must match within a stated tolerance. This is backend
      conformance, not benchmark evidence, and no Python enters the harness.
- [ ] Tokenizer cap and truncation policy recorded in every report row.
- [ ] Determinism double-run holds for the embedder cells; backend and
      thread count recorded.
- [ ] Dependencies confined to spectrometer; editor crates byte-identical.
- [ ] Rendering arms independently toggleable; comment-stripped remains
      the default and the headline.
- [ ] PILOT_RESULTS.md appended with round three, same provenance rigor.
- [ ] Model loading happens once per run, document embeddings once per
      rendering/feature cell, and query embeddings once per distinct query;
      the determinism pass re-ranks cached immutable vectors rather than
      doubling transformer inference.
- [ ] A run-wide cache keyed by the final unpadded token-id sequence reuses
      identical embeddings across ablation cells and rendering arms. Cache
      hits change execution cost only; report rows and rankings remain
      independently reconstructible from their declared rendering.
