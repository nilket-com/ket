# Concept index: search for "The Supervisor", land on `render_frame`

**STATUS AMENDMENT (2026-07-30): deferred behind the discovery benchmark
(`discovery-benchmark.md` → ADR 0004).** This plan is the *curation* layer
of a two-layer design; the benchmark decides the *discovery* layer first.
Sequencing is load-bearing, not preferential: this plan's seeding step
would plant concept words in doc comments and **contaminate the
benchmark's corpus** before its sealed holdout run. After ADR 0004 is
decided, this plan resumes with one amendment: seeds come from accepted
discoveries, not from memory of a conversation.

Scope: a controlled-vocabulary **concept layer** over the symbol layer —
`@concept` annotations authored in doc comments, harvested at grep speed,
queried through the just-shipped picker as its second source. **Zero ML in
v1**; intelligence is spent at authoring time (the plan/review loop already
produces it), query time stays dumb and instant. Acceptance, verbatim:
query "The Supervisor"; top hit `render_frame`. Deliverable is one `impl:`
commit reviewed against this plan.

## Why write-time, not query-time

Concept assignment ("which code realizes this human idea") is expensive to
*infer* and cheap to *record*. The review workflow already mints the
knowledge — supervisor, auditor, the fence, the microscope — as prose
exhaust. This pair captures it as structured, resolvable, in-repo data.
No embeddings, no index server, no GPUs: a curated map beats an inferred
one at any scale a person can read.

## Tag grammar (doc comments — self-resolving by construction)

```rust
/// Frame orchestrator: sequences subsystems, owns budgets and fences,
/// performs no domain work itself.
/// @concept: supervisor, frame-orchestrator — sequences every subsystem
/// @concept: publication-fence — advances the reclaim quarantine
pub(crate) fn render_frame(…)
```

- `@concept: term[, term…] — rationale` on one doc-comment line; multiple
  lines allowed. Terms are lowercase kebab/space words; the rationale is
  the one-line answer shown in the picker row.
- **The key property: tags travel with their item.** A doc comment is
  attached to the function/struct/trait it documents, so renames and moves
  carry the annotation — no path to go stale. The harvester derives the
  symbol from the item header immediately following the comment block
  (fn/struct/enum/trait/mod/impl name — line-level extraction in the
  established audit-test string-scanning idiom; no new parser).
- **Resolvability audit** (the typestate spirit for metadata): a harvest
  pass runs in the audit-test family — an `@concept` block not followed by
  a nameable item **fails the build**, as does a duplicate term pointing at
  the same symbol twice. Orphaned concepts are unrepresentable, not stale.

## Synonym rings

`crates/libaryon/concepts.toml` (or adjacent): small, hand-authored rings —
`supervisor = ["orchestrator", "coordinator", "scheduler"]`. Rings map
term→term only, never term→symbol (symbols live exclusively in doc
comments, preserving the travel-with-item property). The audit checks every
ring member resolves to at least one harvested term or is itself a ring
head.

## Query pipeline

1. Normalize: lowercase, strip leading articles ("the supervisor" →
   "supervisor"), collapse whitespace.
2. Match tiers: exact term > synonym-ring hit > fuzzy (the shipped
   subsequence scorer) over the term list.
3. Tie-break within a tier by **repo occurrence count of the symbol name**
   — a poor-man's fan-in computed with the fuzzy-search corpus machinery
   already in the tree (grep-speed, code-native; `render_frame`'s ~dozens
   of references outrank a once-called helper). Then path depth, then
   lexicographic. Deterministic and test-assertable.
4. Harvest runs per picker-open (corpus is hundreds of tags — grep-speed,
   same no-persisted-index posture as fuzzy v1).

## Picker integration (source two, as designed)

- `@`-prefixed query in the existing picker switches to the concept source
  (fzf convention); `:who QUERY` as the discoverable command alias.
- Result rows: `concept · symbol path · rationale`. Enter jumps via the
  existing gd-powered flow (dirty rules, jump history, Ctrl-o — all free).
- Everything modal (routing, cancellation, generations, overlay) is the
  shipped picker; this pair adds a source, not a surface.

## Seeding

The impl seeds ~15–20 tags from this week's established roles — supervisor
(`render_frame`), auditor (`with_text_buffer_mut`), decomposer
(reclaim drain), building-inspector (`validate_graft`), microscope
(`uast_inspector`), the fence, the treap, the leases — each with its
one-line rationale. Seeding is part of the pair because the acceptance
test depends on it, and because the seed *demonstrates the authoring
convention* future reviews follow.

## Acceptance

`:who the supervisor` (and `@supervisor` in the picker): top hit
`render_frame`, rationale line rendered, Enter lands on the definition,
Ctrl-o returns. Test-asserted end to end, plus: synonym query
("orchestrator") reaches the same hit; a fuzzy near-miss ("supervsor")
still ranks it first; occurrence tie-break proven on a two-symbol fixture.

## Non-goals (parked, promotion paths named)

- **Embeddings**: a CPU-class encoder over annotation strings only, if and
  when vocabulary misses demonstrably hurt. Never over raw code text.
- **Persisted index / watch-driven reharvest**: when harvest latency is
  perceptible, not before.
- **RA reference-graph centrality**: occurrence count suffices until
  ranking disputes prove otherwise.
- **Batch AI tag generation**: authoring stays in the review loop where
  context lives; a one-shot seeding sweep may be proposed separately.
- Cross-repo, Polariton surface, concept editing UI.

## Review checklist

- [ ] Tag grammar parsed per spec; harvester in the audit-test idiom; no
      new dependencies.
- [ ] Orphaned tags and unresolvable ring members fail the build.
- [ ] Symbols derived from adjacent items only — no paths stored in tags.
- [ ] Tier ranking + occurrence tie-break deterministic and tested.
- [ ] Picker source switching clean; `:who` in catalog/help; jumps reuse
      gd machinery with history.
- [ ] Seed vocabulary present with rationales; acceptance passes verbatim.
- [ ] Zero ML, zero persisted state, zero query-time inference.
