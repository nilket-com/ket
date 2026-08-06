# ADR 0035: authored table skins always render

Status: accepted 2026-08-06. This record decides one thing: that contrast never
decides whether an authored table skin is admitted. It is the table half of the
correction PRODUCT-0034 made for plot chrome, and it changes no projection
semantics, no identity, and no lifecycle.

## Context

PRODUCT-0030 established the rule for authored color: valid but ugly renders,
and only the impossible or ambiguous refuses. PRODUCT-0034 applied it to plot
chrome after a `tab20c_s9` skin was refused for a 3.526:1 title fact — a refusal
that reads to an author as a crash, in exchange for preventing them from looking
at their own choice.

Table skins were specified before that rule existed. PRODUCT-0026 §5 and §6 and
PRODUCT-0027 §5 make insufficient visible contrast an admission condition, so
today:

```text
table().skin("yellows").duotone()   refuses
figure([skin("tab20c_s9"), ...])    renders, and records 3.526:1
```

Same author, same session, same kind of choice, two postures. The difference is
drafting order, not a decision. Density is a real argument about *severity* — a
table is dense text, and illegible cells are worse than a faint gridline — but
severity is the author's to weigh. It does not make the artifact undefined.

## Decision

### 1. Authorship decides admission; contrast does not

> If an author names a theme and its projection is mechanically defined,
> Polariton renders that projection exactly.

No refusal, no fallback, no substitution, no clamping, no repair, and no
unsolicited note. Polariton is a faithful renderer of defined choices, not an
arbiter of good ones.

### 2. Polariton-chosen colors keep their obligation

The rope belongs to the author. It is not a licence for Polariton to ship a bad
default to someone who expressed no preference.

`TableSkin::DEFAULT` — and any future colors Polariton selects when the author
selects none — remain bound by PRODUCT-0023 §3's seven checks at 4.5:1.
PRODUCT-0023 gate 3 survives this record, narrowed to exactly those colors.

The distinction is who chose. `table()` gets our judgment and must be legible;
`table().skin(name)` gets the author's and must be exact.

### 3. Refusal means there is no defined answer

An authored table skin refuses only when:

- the theme identity is unknown to `Theme::try_new`;
- the theme has no categorical palette and one is required (PRODUCT-0028);
- the domain exhausts identity capacity, because PRODUCT-0007 §8 would otherwise
  have to invent identities; or
- the requested projection has no mechanical definition.

Insufficient contrast is on none of those lists, and neither is the degenerate
case worth naming explicitly: a palette whose most-contrasting pair is two
identical colors yields paper equal to ink and invisible text. That projection is
**defined**, so it renders, and the author learns something true about that
palette. Naming it closes the last place a threshold could return under the
banner of "but that one is genuinely broken."

### 4. Contrast remains measured, and inert

The WCAG helpers, the seven checks, the two-mode census, and the 8,480-expression
oracle all stay. They are evidence about what a projection *is*, consulted by
tests, evidence files, and any explicitly requested assay.

None of them may alter rendering, gate admission, emit an unsolicited
diagnostic, or influence a color. A measurement that changes output is not a
measurement.

PRODUCT-0026's census keeps its numbers and loses its verdict. Of 8,480
expressions, Duotone's weakest role meets 4.5:1 for 5,355 and falls below for
3,125; PaletteForward meets it for 5,979 and falls below for 2,501. Those are
facts about the two projections, not a count of what an author may look at.

### 5. No note, either

An earlier draft of this correction proposed converting PRODUCT-0027 §5's
teaching refusal into a non-blocking note carrying the same wording. That is
withdrawn. An unsolicited warning is a guardrail wearing a smaller hat: it still
tells an author their choice was wrong, on an artifact they asked for and can
see for themselves.

Advice remains available where it was always appropriate — an assay someone runs
deliberately.

## Acceptance gates

1. `table().skin("yellows").duotone()` renders its exact defined projection; its
   3.175:1 weakest completed role is recorded as a fact, and no refusal,
   substitution, repair, or unsolicited diagnostic occurs.
2. `table().skin("dark2_s1").duotone()` — PRODUCT-0027's both-projections-fail
   control — renders exactly, and its two mechanism-specific facts remain
   available to an assay.
3. A synthetic palette whose most-contrasting pair is two identical colors
   renders with paper equal to ink, without refusal or repair.
4. `TableSkin::DEFAULT` still passes all seven PRODUCT-0023 §3 checks at 4.5:1;
   PRODUCT-0023 gate 3 is green, narrowed to Polariton-selected colors.
5. Unknown identity, absent categorical capability, exhausted capacity, and an
   undefined projection each still refuse at their exact spans, with their
   existing diagnostics unchanged.
6. PRODUCT-0026's two selectors, PRODUCT-0027's default flip, palette positions,
   panel identity, group layout, and the frame-plus-skin lifecycle are
   behaviorally unchanged.
7. The 8,480-expression oracle and the two-mode census remain green, with their
   acceptance counts reinterpreted as measurements rather than admissions.
8. No plot, chrome, mark, Guide, `UiPalette`, libaryon, or Rune-grammar behavior
   changes.

## Supersession

This record supersedes, **only** where they make contrast an admission
condition:

- PRODUCT-0026 §5 and §6, and acceptance gates 6 and 7;
- PRODUCT-0027 §5, and acceptance gates 6 and 8.

Their projection semantics, mechanism-specific facts, spans, and every other
rule remain in force. PRODUCT-0023 §3 and gate 3 are **not** superseded; §2
narrows them to Polariton-selected colors.

## Rejected

- **Keeping the refusal for tables because text is dense:** density changes how
  badly a poor choice reads, not whether the artifact is defined. Severity is the
  author's to weigh, and refusing denies them the one thing that would let them
  weigh it — seeing it.
- **A non-blocking note:** withdrawn in §5. Unsolicited advice about a visible
  artifact is a smaller guardrail, not the absence of one.
- **Extending the rope to Polariton's own defaults:** nobody chose them, so
  there is no author to hold responsible and no rope to hand over.
- **Repairing, clamping, or substituting a low-contrast projection:** each
  renders something the author did not select, which is worse than either
  refusing or obeying.

## Consequences and sequencing

- One rule now covers both artifacts: authors choose colors, Polariton renders
  defined choices exactly and measures the result truthfully.
- `yellows` completes the arc it began. It exposed the projection defect that
  produced PaletteForward, and it now demonstrates that even the projection it
  defeats is the author's to see.
- The teaching sentence PRODUCT-0027 §5 built is not deleted, only unhooked from
  refusal. If an explicitly requested contrast assay ever ships, it already has
  its wording.
- A later record may add such an assay. It is opt-in by construction or it is
  this record's mistake repeated.
