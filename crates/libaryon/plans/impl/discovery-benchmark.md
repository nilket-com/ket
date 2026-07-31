# Concept-discovery benchmark harness

Scope: an **offline evaluator** — a new workspace member,
`crates/spectrometer` (an instrument that identifies composition from
spectra), *never* an editor feature — that indexes every tracked Rust
function in this repository and measures which retrieval tier can discover
concepts from **behavior alone**. This pair delivers the harness, pilot
tuning, and a freeze-ready candidate method; ADR 0004 (proposed,
method-only), the isolated holdout, and the single decisive run are
subsequent chain links, per the protocol converged 2026-07-30.
Deliverable is one `impl:` commit reviewed against this plan.

## The claim under test

Baryon can find The Supervisor (`render_frame`) — and its colleagues —
from harvested behavioral evidence: **no comments, no own-name, no
designer steering the final examination.** Every arm of the design exists
to make that claim either true and strong, or false and cheap.

## Corpus and units

- Rust-only v1: every source-written `fn` in Git-tracked `*.rs` files
  (harvest via `ra_ap_syntax` parsing). Generated dependency code and
  untracked build output are not corpus. **The evaluator crate excludes
  itself**: its fixtures necessarily contain pilot vocabulary and allowing
  the instrument into the specimen would be direct leakage. Resolution uses
  one persistent, offline `rust-analyzer` process; editor code and visibility
  do not change. In-process identity is canonical path + byte range; reports
  and deterministic tie-breaks use repository-relative path + byte range so
  two checkouts agree.
- The provider-neutral unit policy (`Exclude | ConceptUnit | ContextOnly`)
  is **recorded as the integration design but not implemented here** —
  the benchmark hard-codes Rust functions as units, `impl`s as context.

## Feature extraction (the ablation axes)

Per symbol, build feature bags along these frozen-at-step-2 axes:

1. **name-only** — split identifier words (snake/camel).
2. **+ span identifiers, comment-stripped** — every identifier token
   inside the symbol's byte extent, with comments excluded **by RA token
   kind, never regex**. Explicit `#[doc = "..."]` attributes are excluded
   too: they are documentation in a different spelling and would otherwise
   leak curated concepts.
3. **+ resolved callees** (RA) — call/method-call name references resolve
   through one persistent, offline `rust-analyzer` LSP process; target
   function names are word-split and weighted. The attempted reuse of
   libaryon's interactive per-call `goto_definition` seam was measured and
   rejected during implementation: thousands of requests repeatedly
   republished identical live bytes (2m16s+, one saturated core, 2.8 GiB RSS
   before the diagnostic run was stopped). The harness must not distort an
   editor API into a batch API.
4. **+ callers and edge direction** — invert the resolved callee edges over
   the closed corpus. No second reference-analysis implementation.

Cross-cutting arms, each independently toggleable:

- **own-name exclusion** — the discovery-vs-lucky-naming control. Remove
  both the declaration contribution and every full identifier equal to the
  containing function's name (including recursive calls) before word
  splitting. A name-only/excluded arm is honestly empty.
- **string literals as a separate channel** — status messages may be real
  behavioral evidence; measure whether they are doing the work.
- **comment-stripped is the default everywhere** — `kind.rs` now carries
  curated concept vocabulary in its doc comments; an arm that reads
  comments measures the concept *index*, not concept *discovery*.

## Retrieval tiers (columns of the matrix)

1. **`:fz`-equivalent floor baseline** — the shipped scorer's spec
   reimplemented over raw source lines (exact > word-boundary > subsequence,
   smartcase), with **line hits mapped to their innermost containing
   symbol**. This is one fixed floor, not a feature-ablation column: shipped
   `:fz` reads comments and names, so pretending it participates in the
   stripped/excluded arms would not be equivalent.
2. **BM25** over the feature bags.
3. **Static vectors + rank-SIF** — loader for GloVe/fastText-style **text**
   vector files, supplied by path, SHA-256 verified, never downloaded.
   Retain only the union of corpus and current query tokens while scanning
   the full file. Text `.vec` files provide exact-vocabulary lookup only;
   they do **not** contain fastText's binary n-gram tables and therefore
   cannot promise subword OOV composition. Missing-token coverage is
   reported. Rank-derived Zipf probabilities make this explicitly a
   `rank-SIF` approximation; the configured file must declare that its
   order is frequency-ranked. Int8 quantization is optional and recorded.
4. **Small embedder reservation** — report an explicit `absent` column in
   this pair. “A model path” is not a buildable contract: MiniLM-class
   models require a specified architecture, tokenizer, pooling,
   normalization, weights format, and runtime. Choosing those is a later
   harness-only pair if static vectors fail to earn integration. No empty
   cargo feature pretending an embedder exists.

## Pilot corpus (contaminated, tuning-only)

Checked-in TOML of probes with types:

- `canonical`: supervisor→`render_frame`, auditor→`with_text_buffer_mut`,
  decomposer→reclaim drain, building inspector→`validate_graft`,
  receiving dock→resolver publication, microscope→`uast_inspector`.
- `paraphrase`: watchdog, janitor, bookkeeper, floor manager.
- `distinction` (pairwise): supervisor must **rank `render_frame` above
  `with_text_buffer_mut`**, and auditor the reverse — preference, not
  co-presence.
- `negative`: queries with no honest referent (e.g., connection pool,
  css parser) — scored via calibrated margin threshold; false-accept
  rate is a first-class metric.

Expected positive identities are checked in as repository-relative path +
function name and resolve to the runtime's path + byte-range identity.
The pilot's contamination is documented in the file header: it tunes the
harness and candidate thresholds; it decides nothing.

## Metrics (all reported per cell of the matrix)

- hit@1, hit@3, MRR over positives; distinction-pair win rate; FAR at a
  pilot-calibrated **candidate** threshold recorded for ADR 0004 to accept
  or revise before commitment.
- Operational: index build time, per-query latency, OS-labelled peak RSS
  when available, deterministic logical encoded index size (there is no
  persisted index in this pair), and incremental single-file re-index cost.
- **Determinism**: two consecutive runs must produce identical *ranked
  symbol identities* (total tie-break: repository-relative path, byte range,
  provider kind) — not byte-identical scores. Each run records corpus
  revision, extractor version, config, model checksums, inference
  backend; cross-platform comparisons use a stated score tolerance.

## The sealed sequence (context; steps 3–6 are not this pair)

1. Tune on the pilot and emit one freeze-ready candidate configuration
(this pair). 2. Commit ADR 0004 *proposed*, freezing extractor, tiers,
weights, model hashes, metrics, thresholds, and the full decision rule
including partial passes. 3. An
**isolated author** — a fresh model session with repo access, instructed
away from experiment artifacts; user, Claude, and Codex are all
contaminated — writes the holdout and commits
`SHA-256(nonce ‖ canonical_holdout_bytes)`, nonce held off-repo until
reveal. 4. Reveal, run **once**. 5. Append results plus a quantified
pilot∩holdout overlap audit; the precommitted rule accepts a tier — or
none.

## Non-goals

- Editor integration of any tier (its own pairs, after ADR 0004 accepts).
- Non-Rust providers; the unit-policy enum; persisted indexes.
- Concept-index seeding (deferred — see that plan's status amendment;
  seeding before the holdout run contaminates this corpus).
- Model auto-download, network access of any kind, GPU requirements.
- Bundling or installing `rust-analyzer`. Resolution records the executable
  path/version; an unavailable server makes callee/caller cells explicitly
  absent rather than failing or silently substituting lexical edges.

## Review checklist

- [ ] Harness is a standalone workspace member; editor crates gain zero
      dependencies and zero code.
- [ ] `crates/spectrometer/**` is excluded from its own measured corpus.
- [ ] Comment stripping is RA-token-exact; a test proves ordinary/doc
      comments and explicit doc attributes are absent while raw-string
      literals remain intact in their independent channel.
- [ ] Own-name exclusion and literals channel independently toggleable;
      every matrix cell reproducible from a config.
- [ ] Resolved cells use one persistent RA process, record its version, and
      degrade explicitly to absent; no editor API or code changes.
- [ ] `:fz` baseline maps lines→symbols before scoring.
- [ ] Vector files: path + checksum verification, rank-order declaration,
      and missing-token coverage; an absent file degrades the column, never
      the run. The embedder reservation is explicitly absent.
- [ ] Determinism double-run check wired into the harness itself.
- [ ] Pilot TOML complete with all four probe types and its contamination
      header.
- [ ] No holdout and no ADR 0004 in this pair. Candidate pilot thresholds
      are labelled tuning-only; the next `plan:` link decides whether to
      freeze them.
