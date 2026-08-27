# ADR 0141: the presentation registry

Status: accepted 2026-08-27. PRODUCT-0007 gave the settings model its cascade
and its typed value domains; PRODUCT-0076 gave the compiler its static
partition; the surface records 0111–0130 grew the presentation vocabulary one
member at a time. This record is about what that growth produced: a surface
that exists in three parallel encodings with nothing but discipline holding
them together, and no machine-readable description from which anything else
can be derived.

It decides four things: that one declarative registry describes every Rune
presentation callable and is the authority from which compiler recognition and
module registration derive; that a semantic settings vocabulary projects from
that registry rather than mirroring it; that compilation emits a presentation
source model — spans, expression shape, and ownership facts for every authored
setting occurrence; and that a read-only settings inspector renders the
projection with effective values and provenance.

It does not decide any mutation of source. Which facts in the source model
authorize an edit, how an edit is applied, previewed, or refused — all of that
is PRODUCT-0142's subject, and this record's artifacts deliberately report
facts without granting authority. It adds no Rune surface member, changes no
spelling, and alters no resolution outcome. It does not decide mark-constructor
or channel-binding UI, facet field selection, per-layer editing in layered
figures, or notebook-cell handling; those are follow-on records.

## Context

### One surface, three hand-synchronized encodings

The presentation surface is spelled in three places that must agree and that
nothing forces to agree:

1. **Recognition.** `PRESENTATION_FREE` and `PRESENTATION_METHODS`
   (`crates/polariton/src/rune_view_compiler.rs:23`, `:55`) are string tables
   the static partition uses to recognize presentation syntax. ~30 free
   constructors, ~30 chained methods.
2. **Registration.** `build_context`
   (`crates/polariton/src/rune_presentation.rs:523`) installs ~60 handwritten
   raw handlers into the presentation module. Because they are raw stack
   functions — variadic arguments, span-carrying custom errors — Rune's module
   metadata records nothing about their signatures. Reflection over the
   `Context` cannot recover the surface; there is nothing to reflect.
3. **Handler-declared signatures and reused typed domains.** Each raw handler
   declares its own arity and argument handling, and — where a value has a
   domain — delegates validation to a spec newtype
   (`crates/polariton/src/plot/spec/mod.rs`: `PointSize`:95,
   `PlotPaddingPixels`:197, `TickRotation`:239, and their families), each
   carrying a validated `new()` with the legal range and a human-readable
   refusal. Toggles and strings introduce no newtype; the domain encoding is
   reused where it exists, not parallel everywhere.

Adding one surface member today touches recognition, registration routing,
and a handler's declared signature, in sync, by hand. The architecture tests
catch some drift after the fact; nothing prevents it.

### Span provenance is ad-hoc and growing ad-hoc

Where authored spans are retained at all, they are retained by bespoke
per-setting structs: `RunePlotIntent`
(`crates/polariton/src/rune_relation_envelope.rs:78`) carries
`domain_spans`, `tick_step_spans`, `tick_every_spans`, `minor_tick_spans`,
`plot_skin_span`, and `facet_span`, each minted by the record that added its
setting, each marked "Diagnostic provenance only; deliberately excluded from
equality." `Origin` (`crates/polariton/src/rune_presentation.rs:144`)
identifies a whole call — constructor through closing parenthesis — and never
an argument within it. Every future spanned setting, on the current pattern,
mints another struct.

These spans are honest about what they are: diagnostics. They can point an
error at source. They cannot say "this argument is a literal you may replace,"
because nothing classifies the argument, and they cannot say where an absent
setting would be inserted, because nothing records the syntax boundaries an
insertion would need.

### Nothing reads the surface but the pipeline

There is no GUI representation of a plot's settings. The inspector arc that
motivates this record needs to enumerate the surface, group it, render a
widget per value shape, show effective values against the PRODUCT-0007 §7
cascade, and distinguish authored from defaulted — none of which any current
encoding can feed. Deriving it from Rune reflection is unavailable (above);
hand-writing it would create a fourth parallel encoding.

## Decision

### 1. One registry describes every callable

A declarative registry — `CallableSpec` — carries one entry per presentation
surface member: the exact Rune spelling, its position (free constructor or
chained method), its argument signature, a reference to its raw handler, and
a one-line doc string — and two receiver fields that are deliberately not
one. Its **registration routes** record where the host installs the handler
today; its **accepted receivers** record where the call is semantically
valid. These differ: `build_context` registers `__font_size` on both
`RuneFigureElement` and `RuneSkin`
(`crates/polariton/src/rune_presentation.rs:574`), and broadly routes figure
methods to receivers whose handlers then refuse them with attributed errors.
Because this record promises no surface behavior change, derived registration
reproduces the current routes exactly, and semantic refusal stays where it
lives — in the handlers. Narrowing registration toward accepted receivers
would change refusal attribution and is a later record's decision.

From the registry derive:

- the compiler's recognition tables, replacing `PRESENTATION_FREE` and
  `PRESENTATION_METHODS` as authored artifacts;
- the module registration loop in `build_context`;
- conformance gates asserting the registry, the recognition set, and the
  installed module agree exactly.

The raw handlers remain handwritten. Their validation logic, span decoding,
and error prose are the hard-won part of the surface and generating them
would trade audited code for a macro. The registry owns *that they exist and
what they are called*, not what they do.

### 2. Settings are a projection of the grammar, not a mirror

The Rune vocabulary is not a settings list. `figure`, `layers`, and the mark
constructors are grammar; `font_size` means three different settings depending
on whether its receiver is a title, subtitle, or legend; `padding()` is one
constructed child carrying up to four settings; `bounds` edits a pair in one
call; `legend()` groups presence, position, and typography.

A second vocabulary — `SettingSpec` — therefore projects from the registry
rather than mirroring it. Each entry carries a stable `SettingKey`, its
inspector section, its logical target (figure, x-axis, y-axis, mark, legend,
padding edge), its value shape (delegating ranges and refusals to the existing
spec newtypes — the registry never restates a domain the newtype already
owns), its applicability by mark family, its dependency and conflict rules
(`tick_step` conflicts with `tick_every`; temporal ticks conflict with
`minor_ticks`; `percent` requires stacking), and its authored/effective
projection.

Callables that are grammar project no setting. One callable may project
several settings; one setting may draw from receiver-dependent callables. The
two levels are related by reference, never by name coincidence.

`SettingSpec` deliberately contains no mutation recipe. What a setting *is*
lives here; how an authored occurrence may *change* is PRODUCT-0142's
`WritebackSpec`, keyed by the same `SettingKey`. This boundary keeps the
registry from accreting GUI metadata, compiler facts, and edit algorithms
into one oversized structure.

### 3. Compilation emits the presentation source model

The compiler, which already lexes every presentation call to build `Origin`
spans, additionally emits a **presentation source model**: for every authored
setting occurrence, its `SettingKey`, its logical target, the call span, the
argument spans, its expression shape (direct literal, or expression/binding
reference), and its **owners** — the non-empty set of view identities the
occurrence contributes to. Ownership is a set because the vocabulary supports
helpers, pass-through functions, and reused definitions — the helper
conformance test (`crates/polariton/src/rune_view_compiler.rs:3332`) wraps
the complete vocabulary in `pass`/`twice` helpers — so one original span can
legitimately feed several `LocalViewIdentity` outputs. Singular ownership
would be a fiction the first shared helper falsifies. No existing test
exercises the plural case directly — helper transport is tested, shared
ownership is only implied by per-slot dependency rendering — so a fixture in
which one helper-defined setting feeds two view identities is part of this
record's conformance obligation, not optional coverage.

For absence, the model records **factual syntax boundaries** — call-chain
extents, container and child-list spans, separator positions — and no more.
Choosing where an absent setting belongs is an editing judgement;
PRODUCT-0142's `WritebackSpec` selects insertion points from these
boundaries, and this model only makes that selection possible.

The compiler computes the model from source text alone, which is all it
knows. The host envelopes the emitted model with the source buffer identity
and the document generation it compiled; those are host facts, and stamping
them is the host's act.

The model **reports facts and grants nothing**. "This argument is the literal
`45.0` at bytes 210..214, owned by one view" is a compiler fact; "you may
replace it" is an editing-model judgement this record does not make. The
artifact is named for what it is — a source model, not an edit plan.

The ad-hoc span structs of `RunePlotIntent` are superseded as a pattern: no
future setting mints a bespoke span carrier. Existing carriers may migrate
into the model as they are touched; this record does not require a bulk
migration.

### 4. Resolution reports per-setting provenance

The inspector shows **effective** values, and an effective value without its
origin is a lie of omission. Resolution therefore reports, per displayed
setting instance — `SettingKey` plus logical target, never `SettingKey`
alone, so later per-layer inspection extends the keying rather than replacing
it — which PRODUCT-0007 §7 tier produced the effective value: library
default, application theme, authored spec, layer, or pane override. Parts of
the authored half already exist as spans; the defaulted half is new plumbing
through `PlotSettings` resolution
(`crates/polariton/src/plot/theme.rs:312`).

### 5. The inspector is read-only, complete in vocabulary, gated in capability

The settings inspector renders the `SettingSpec` projection for the active
host panel's plot: grouped by section, one widget per value shape, effective
value with a provenance badge on every row. It is read-only in this record —
every affordance that implies mutation is absent, not disabled.

The registry is complete; the inspector is capability-filtered, not curated.
A setting appears as displayable the moment its `SettingSpec` exists; it
becomes editable in PRODUCT-0142 the moment its capability is declared there.
There is no second handwritten allowlist.

Three visibility facts are displayed without any editing behavior attached:

- **Expression** (row-level): a setting authored as an expression or binding
  reference shows its effective value with an expression badge.
- **Shared source** (row-level): a setting whose authored occurrence has
  several owners in the source model shows a shared-source badge — this one
  span serves more than the plot being inspected.
- **Attachment** (plot-level): a plot is *attached* when PRODUCT-0142's
  complete positive witness holds — the producing buffer exists, its document
  generation matches the model's stamp, and its current compiled view still
  contains the producing view identity; one whose source exists but whose
  witness fails on generation is *out of date*; one with no editable
  producing source — a retained or pinned `DurablePlot` — is *detached*. The append-only
  `view_panel_ids` map (`crates/polariton/src/editor.rs:391`) is historical
  identity and deliberately not evidence of attachment; the inspector displays
  what a positive witness proves, and PRODUCT-0142 defines that witness.

Targeting needs no part of PRODUCT-0133 stages 3–4. The inspector addresses
the active host panel through the existing `(BufferId, LocalViewIdentity) →
HostPanelId` mapping; `ArtifactToken` identifies a data publication for
selection correctness and is the wrong identity for source inspection.

### 6. First-inspector scope

Sections in the first inspector: figure text and typography; palettes, ramps,
and skin; hover size; padding; x/y axis label, bounds, tick placement, minor
ticks, sizes, and rotation; single-mark styling (point size, stroke width,
bar width, category gap, stacking presence).

Outside it, as vocabulary present but unprojected or undisplayed: mark
constructors and channel bindings (a column-picker surface against the data
schema, a different record); facet field selection; per-layer targeting when
a layered figure has several matching layers; notebook-cell sources, whose
spans need rebasing into the containing Markdown document.

## Consequences

Adding a surface member becomes: one `CallableSpec` entry, one handwritten
handler, and — if it is a setting — one `SettingSpec` entry. Recognition,
registration, conformance, and inspector display follow without further
authorship. Authored recognition and registration drift disappears — those
tables stop being written by hand — and the conformance gates make the
registry's authority over them enforceable rather than aspirational. Handler
behavior remains handwritten and remains the handlers' to get right.

The inspector ships as a genuinely useful read-only surface: every applicable
setting in the initial inspection capability, its effective value, and where
that value came from — a debugging and learning surface for the Rune
vocabulary even before any editing exists.

Sequencing:

1. callable registry; derived recognition and registration; conformance
   gates;
2. `SettingKey` and `SettingSpec`; registry-to-projection conformance;
3. presentation source model and resolved provenance;
4. read-only inspector — this record completes;
5. PRODUCT-0142 takes the editing-model question from here.
