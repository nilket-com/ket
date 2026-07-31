# ADR 0004: the concept-discovery verdict — none passes

Status: accepted 2026-07-31. **The first benchmark-decided ADR in this
repository**: its decision section was written by measurements, under
rules fixed before the decisive run. Method: `impl/discovery-benchmark.md`
and `impl/discovery-embedder.md`; instrument: `crates/spectrometer`;
evidence of record: `crates/spectrometer/PILOT_RESULTS.md`.

## The question

Can Baryon discover concept-to-code assignments — *"The Supervisor" →
`render_frame`* — from harvested behavioral evidence alone: no comments,
no own-name, no designer steering the examination? And if so, which
lightweight tier earns editor integration?

## The rules (pre-registered)

A tier earns a sealed-holdout run only by achieving hit@3 on the strict
discovery surface (own name excluded, literals off, comments stripped) of
the pilot's canonical probes, with distinction measured only when a target
reaches the top three, and false accepts calibrated by margin. The pilot
is contaminated by design and tunes; it cannot pass anything — but it can
fail everything, and it did.

## The evidence (three rounds, full provenance in PILOT_RESULTS.md)

| round | tiers | strict hit@3 | best supervisor rank |
|---|---|---:|---:|
| 1 | `:fz` floor, BM25, rank-SIF (GloVe 50d) | 0.00 | not retrieved |
| 2 | rank-SIF (GloVe 300d), salience k∈{16,32,64} | 0.00 | 1,133 (salience: worse) |
| 3 | MiniLM-L6-v2, 12 strict cells, 3 renderings | 0.00 | **22** (chunked span) |

Corpus: 5,956 functions; call graph: 61,820 references, bit-identical
across independent quiescence-waited extractions. Backend conformance to
sentence-transformers: max abs error 9.537e-7. Best MRR anywhere: 0.0104,
whose six canonical ranks (84, 2,934, 17, 68, 751, 1,374) show why an
encouraging average is not a retrieval boundary.

## The findings

1. **The gradient is real and monotone**: token *presence* retrieves
   nothing; token *distribution* (static vectors) reaches the semantic
   neighborhood but ranks ~10³; token *composition* (attention) ranks
   ~10¹. Each capacity step moves toward the answer.
2. **The ceiling is below the bar**: no lightweight tier converts
   behavioral evidence into usable role retrieval on this codebase. The
   dilution hypothesis was tested and rejected (salience ordering
   worsened bag ranks); the capacity excuse was tested and rejected
   (300d); the composition hypothesis moved the needle 50× and still
   missed by an order of magnitude.
3. **Role-from-behavior is a reasoning task at this subtlety.** The
   examples that motivated the experiment were produced by frontier-model
   reasoning over invariants, not by similarity of any kind measured
   here. That is now an evidenced statement, not an anecdote.

## The decision

**None passes.** No tier is a discovery candidate; no sealed holdout is
authored or run (the holdout validates a passing candidate's
generalization — there is nothing to validate); no model, dependency, or
`models_dir` enters the editor under this ADR.

## Consequences

- **The concept index resumes** as the sole shipped layer
  (`impl/concept-index.md`, status amended): curation of discoveries made
  by whatever can actually make them — including the frontier-model
  review loop that made the originals. Its seeding no longer threatens
  any experiment: no sealed run is pending, and every future discovery
  experiment inherits comment-stripped defaults, so authored tags are
  structurally excluded from future corpora.
- **The spectrometer is a standing instrument, not scaffolding.** Any
  future tier — improved lightweight or tier-5 LLM analysis — must beat
  the recorded matrix on the same pilot before earning a holdout, under
  the preserved salted-commitment protocol (isolated author, nonce
  off-repo, run once, overlap audit). The bar is now a number, not a
  feeling.
- **Tier-5 is a separate future ADR**, proposed on its own evidence and
  economics — explicitly not a fallback inherited from this failure.
- Operational truth recorded for any future passing tier: even the failed
  embedder cost 58–207 ms per symbol uncached and ~1.6 GiB RSS at build —
  the async, revision-stamped cache integration design remains the right
  shape whenever something earns it.

## What this ADR does not decide

Editor integration of anything; tier-5's merits; the holdout's authorship
(reserved, protocol intact); the fate of heavier local models at a
configurable path. Each is its own record when its evidence exists.
