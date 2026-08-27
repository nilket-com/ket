# ADR 0142: the settings writeback contract

Status: accepted 2026-08-27. This record discharges the question PRODUCT-0007
§7 deliberately deferred: "whether and how [the pane override] round-trips
into the authored spec is deliberately left to the implementation record,
because it is an editing-model question rather than a specification one."
PRODUCT-0133 §9 answered that question for viewport state — zoom and pan are
session-scoped pane overrides that "must not mutate Rune source." This record
answers it for authored settings, and the answer is the complement: settings
edits made through PRODUCT-0141's inspector are **source edits**, and nothing
else.

It decides the two-authority model; that preview and commit use the identical
candidate edit plan, produced by candidate-source compilation; that relation
reuse during preview is an equality proof over compiler output plus a
separate runtime witness, never an assumption; the editability classification
and the replacement, insertion, reset, and cleanup semantics; a targeted,
acknowledged atomic edit command in libaryon; the attachment liveness
witness; the preview execution contract; the initial capability set; and the
refusal set.

It does not create a durable GUI-settings layer, and rejects one — a durable
local override with visible provenance and promote/reset operations would be
an explicit product feature deserving its own record, not an implementation
stage of this one. It does not decide per-layer editing in layered figures,
notebook-cell span rebasing, or the structural settings withheld from the
initial capability set; those extend this contract in follow-on records. It
depends on PRODUCT-0141's registry, source model, and provenance, and changes
nothing about PRODUCT-0133's viewport authority.

## Context

### The pipeline preview needs already exists

`compile` (`crates/polariton/src/rune_view_compiler.rs:202`) takes source text
through static analysis, partition, presentation-program rendering, execution
of that program in the presentation VM under explicit allocation and
instruction limits, and per-execution relation-program rendering. Its output,
`TransformedRuneView` (`crates/polariton/src/rune_relation_envelope.rs:652`),
carries per relation the origin, the generated relation program text, and the
spanned view intents with their `LocalViewIdentity`. Everything a preview must
compare is already produced on every compile.

What equality means is the trap. `TransformedRuneRelation`'s derived
`PartialEq` includes its source map, and inserting presentation text
legitimately moves original-source offsets without changing the relation
program — so struct equality would refuse reuse for exactly the edits this
record exists to enable. Meanwhile `RunePlotIntent`'s manual `eq` excludes its
span fields as "diagnostic provenance only." The codebase already knows spans
are not semantics; the reuse key must know it too, explicitly.

### The host cannot make a targeted edit

libaryon's command surface has `InsertText`
(`crates/libaryon/src/engine/protocol.rs:305`) and `ReplaceDocument`
(`:480`), and nothing between. `ReplaceDocument` is documented to clear the
file association and reset undo history — disqualifying for an edit that must
feel like the user's own keystroke. There is no range-targeted edit, no
generation guard, and no path that safely edits a buffer other than the
focused one, which a plot pane's producing buffer may well be.

### Identity is historical, not live

`view_panel_ids` (`crates/polariton/src/editor.rs:391`) is deliberately
append-only; stale entries are retained as checkpoint-witness census. A
mapping's presence proves a panel once existed, not that its buffer still
holds its view. Pinned `DurablePlot`s outlive their producing source
entirely. An edit contract that inferred authority from the map would edit
bytes that no longer mean what the map remembers.

## Decision

### 1. Two authorities, and no third

Durable settings authority is authored Rune text. Session viewport authority
is PRODUCT-0133's pane override. There is no durable GUI-settings store: a
slider in the inspector is a way of typing, and what it types is source.

A preview is therefore *ephemeral by construction* — it is a candidate text
that either becomes the buffer's text or is discarded. Nothing in this
contract persists a setting anywhere except the source document.

### 2. Preview is the writeback

An edit gesture produces a **candidate text edit** — the same edit plan a
commit would apply. Preview applies that edit to a shadow copy of the source,
runs the real `compile` on it, proves relation reuse (§3), pairs the candidate
intents with the retained relation artifact, and resolves through the normal
sink-resolution and kernel path. Release commits the identical edit plan to
the buffer through §5's command; cancel discards the shadow.

One semantic path exists: gesture → text edit → compiler → presentation VM →
resolution. There is no gesture-to-spec patch layer, so there is no second
gesture-to-spec mapping whose equivalence to the pipeline must be proven, and
no second mapping to drift as the registry grows. What still requires tests
is the path itself — generation guards, reuse proofs, and pipeline
conformance — not an equivalence between two paths. Structural presentation
edits are not a special case: candidate compilation is the mechanism, not a
fallback.

The ordering also makes preview a validator. The candidate edit is compiled
*before the buffer is ever touched*; an edit plan that produces text the
compiler refuses fails as a preview, visibly, with the source intact. The
patch-preview alternative discovers that failure at commit, after showing
success.

### 3. Relation reuse is an equality proof, never an assumption

Preview reuses the currently retained relation artifact **only** under two
proofs from two evidence sources, kept explicit because they answer different
questions:

- **`RelationPlanKey`** — compiler-derived relation semantics, computable
  from either side's compile output alone: execution count and order; each
  relation's origin and root identity; each generated relation program's
  text; and the mapping of view identities to their executions. Source-map
  offsets are excluded — they move under legitimate presentation edits and
  are not semantics.
- **`ArtifactReuseWitness`** — runtime state: the baseline `RelationPlanKey`,
  the generation of the currently retained artifact, and proof that this
  generation is still the authoritative publication for that baseline.

Reuse requires candidate `RelationPlanKey` equal to the baseline
`RelationPlanKey`, and a valid witness. The first proves the candidate asks
for the same relation work; the second proves the retained answer is still
the answer. A composite proof type may carry both; the evidence sources stay
distinct within it.

The `SettingSpec` claiming an edit is presentation-only is a hint, not
evidence; helpers, aliases, and future grammar can make a textually small
edit relation-visible, and only the compiled programs know. If the plan keys
differ, or the witness fails, preview **refuses** with "this change requires
running the relation again." A settings gesture never silently dispatches
relation work.

Reuse requires new plumbing this record authorizes: the compile-and-dispatch
path retains its transformed view and published artifact generation as a
reusable preview substrate, rather than consuming them transiently.

### 4. Editability is classified per occurrence, from compiler facts

PRODUCT-0141's presentation source model supplies the classification; this
record assigns each class its editing behavior:

- **Direct literal, solely owned** — the edit replaces exactly the relevant
  argument span, or the span-disjoint argument spans of a pair-valued setting
  such as `bounds` or `at`. Nothing outside them is rewritten; the author's
  formatting elsewhere is not the contract's business.
- **Expression or binding reference** — read-only, effective value shown with
  the expression badge. The contract never rewrites computation.
- **Shared occurrence** — an occurrence whose owner set names more than one
  view is read-only under this record, shown with the shared-source badge. A
  preview may honestly show every affected plot, but a slider presented as
  editing one plot must never silently change several; multi-view editing is
  a follow-on record's decision, and until it exists, refusal is the
  behavior.
- **Absent** — the edit inserts the authored call at an insertion point the
  `WritebackSpec` selects from the source model's factual syntax boundaries
  (call-chain extents, container spans, separators). Selection is this
  record's judgement, exercised over PRODUCT-0141's facts. When the
  boundaries admit no unambiguous point — several candidate containers,
  helper-built furniture — insertion refuses rather than guesses.
- **Reset** — the edit removes the authored modifier or constructed child,
  and cleans up a container the removal leaves meaningless: a bare
  `x_axis()`, `padding()`, or `legend()` with no remaining effect does not
  survive its last setting. Cleanup applies only when the planner proves
  sole, inline ownership of the container; shared or helper-defined
  containers fall under the shared-occurrence refusal. The removal recipe
  owns its trivia: adjacent separators and directly attached comments are
  part of the planned edit, not debris.

### 5. libaryon gains a targeted atomic edit

A new engine command — `ApplyDocumentEdits { request_id, buffer_id,
expected_generation, edits }` — with these properties, each load-bearing:

- **buffer-targeted**: valid for a non-focused buffer, since the producing
  source of the active plot pane need not hold focus;
- **generation-guarded**: refused if the buffer's document generation is not
  `expected_generation` — spans computed against one text never edit another;
- **atomic**: all edits in the plan apply or none do; edits within a plan are
  span-disjoint, expressed in the guarded snapshot's coordinates, and applied
  correctly against that snapshot (descending order or an internal coordinate
  transform — libaryon's implementation detail, not the caller's problem);
- **one undo unit**: the user's next undo reverses the whole committed
  gesture, exactly as if they had typed it;
- **identity-preserving**: document and file identity survive, and prior undo
  history survives — the properties `ReplaceDocument` documents itself as
  destroying. Caret and selection undergo ordinary edit transformation rather
  than reset; search, diagnostics, and semantic state follow their normal
  incremental invalidation, exactly as for a typed edit;
- **acknowledged**: the engine answers with a correlated
  `DocumentEditResult { request_id, buffer_id, outcome }`, where the outcome
  is `Applied { new_generation }` or `Refused { reason }`, delivered over a
  non-coalescing path. Queue insertion confirms nothing, and coalesced status
  projections can drop or merge — neither is an acknowledgement. The
  `request_id` is what lets §7's lifecycle hold a preview open against
  exactly its own commit and no other.

libaryon never rebases an edit plan across document generations. A stale
generation is a refusal, and the caller's recovery is a fresh compile
producing a fresh source model.

### 6. Attachment requires a positive liveness witness

Edit authority requires proving, at commit time, all three of: the producing
buffer exists; its current document generation matches the source model's
stamp; and the buffer's current compiled view still contains the producing
`LocalViewIdentity`. The append-only panel map satisfies none of these by
itself.

The witness yields three plot-level states, all surfaced by PRODUCT-0141's
inspector:

- **Attached** — all three hold; capability-declared settings are editable.
- **Out of date** — the source exists but its generation has moved past the
  model's stamp; the inspector remains readable and writeback requires a
  rerun that rebuilds the source model.
- **Detached** — no editable producing source exists (retained and pinned
  plots); permanently read-only for that artifact.

Attachment is plot-level; expression editability is row-level. An attached
plot routinely shows editable literals beside read-only expressions.

### 7. The preview execution contract

- Preview is generation-stamped and newest-wins; a completed preview for a
  superseded gesture is discarded, not displayed.
- At most one candidate compile/VM execution is in flight; intermediate
  slider values coalesce into the newest pending candidate.
- Candidate execution runs under the existing presentation allocation caps
  and instruction budget; a runaway candidate is refused by the same limits
  that govern authored source.
- Preview is transactionally isolated: it mutates no source text, no durable
  session meanings, no categorical identity continuation, and no retained
  resolution state. Its outputs live in the preview generation and die with
  it.
- Release commits the release gesture's own candidate. If that candidate is
  still compiling, the commit waits for it; if it refuses, nothing commits. A
  visible preview that a newer pending candidate has superseded is never
  committed — the commit is always the newest candidate's edit plan, never a
  recomputation from current widget state and never a stale success.
- The preview remains visible until the correlated `DocumentEditResult` for
  its own `request_id` arrives. A `Refused` outcome — generation mismatch
  included — restores the authoritative rendered state.
- `Applied { new_generation }` **promotes the candidate**: the successfully
  compiled candidate output becomes authoritative state, its source model is
  enveloped with `new_generation`, and the proven relation artifact and
  witness are retained. The generation guard makes this sound — the accepted
  document text is exactly the shadow source already compiled — and without
  promotion the contract contradicts itself: the edit advances the document
  generation, the old source model's stamp goes stale, and §6 would mark the
  plot out of date at the moment of its own successful commit. No recompile
  of the buffer is required to stay attached; a later authoritative compile
  simply supersedes the promoted state as usual.

### 8. The initial capability set

A refusal that names no set is untestable, so the set is named. Initially
**editable**, given an attached plot and any of §4's admitted forms — a
solely-owned direct-literal occurrence, a solely-owned reset target, or an
unambiguous insertion point: figure title and subtitle text and their sizes;
axis label, tick, facet label, and legend typography; legend position;
palette, sequential and diverging ramps, and skin selection; hover size; the
four padding edges; axis label text, bounds, `tick_step`, `tick_every`,
`minor_ticks`, tick sizes, and tick rotation; and single-mark point size,
stroke width, bar width, and category gap.

Initially **display-only**: legend hiding (`legend().hide()` is a
zero-argument structural modifier, and bare `legend()` is not valid source —
the evaluator requires `.at(...)`, `.hide()`, or `.font_size(...)`,
`crates/polariton/src/rune_presentation.rs:3826`); stacking presence, percent
normalization,
facet gutter, and other structural modifiers — settings whose edit changes
figure structure rather than a value in place. Nothing architectural
withholds them; candidate compilation previews them like anything else, and
a follow-on record admits them by declaring their `WritebackSpec`.

**Refused regardless of capability**: expression-authored occurrences, shared
occurrences, per-layer targets in layered figures, notebook-cell sources, and
ambiguous insertion — the classes §4 and §6 already close.

### 9. The refusal set

Writeback refuses, with the reason named, on: a stale source generation
(§5); a failed liveness witness (§6); a `RelationPlanKey` mismatch or failed
`ArtifactReuseWitness` (§3); a non-literal target, a shared occurrence, or an
ambiguous insertion (§4); a setting §8 marks display-only; and a candidate
compile failure (§2). Every refusal leaves the buffer untouched and the last
authoritative render standing. There is no partial success anywhere in this
contract.

## Consequences

Scripts stay true. Anything the inspector did to a plot is readable in the
source, shareable with the source, and reversible with one undo — the
inspector is an input method for the Rune surface, not a rival authority. The
cost accepted for that is a candidate compile per (coalesced) preview
gesture; the presentation program is bounded and locally isolated — authored
helpers can spend real instruction budget, which is what the §7 limits are
for — the relation is reused under proof, and anything that moves ink pays
its kernel rebuild under either preview model.

Sequencing, continuing PRODUCT-0141's:

5. `ApplyDocumentEdits` in libaryon, with generation guard and undo-unit
   semantics;
6. retained relation generation, `RelationPlanKey`, and
   `ArtifactReuseWitness`;
7. candidate-source compilation and artifact reuse — read-only preview of a
   hypothetical edit proves the substrate before any commit exists;
8. literal replacement, reset with container cleanup, then syntax-aware
   insertion;
9. the preview/commit/cancel lifecycle of §7 — this record completes;
10. follow-ons: per-layer targeting in layered figures, notebook-cell span
    rebasing into Markdown documents, structural settings withheld from the
    initial capability set, and — if ever wanted — the durable local-override
    product this record declined to create.
