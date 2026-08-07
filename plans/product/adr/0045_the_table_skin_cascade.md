# ADR 0045: the table skin cascade

Status: proposed 2026-08-07. This record decides one thing: that table skins
join PRODUCT-0007 §7's settings cascade, closing the architectural review's
one agreed capability gap (audit seams S2 and S3). It re-homes the authored
`.skin(...)` value into the cascade's authored tier, defines the library,
application, and pane tiers around it, and supersedes the one sentence of
PRODUCT-0023 a later tier falsifies. It changes **no pixel**, adds **no Rune
surface**, and ships **no new producer** — the application and pane tiers are
defined and tested but unproduced, PRODUCT-0030 §3's staging.

## Context

The consistency audit found that two artifact kinds adopted PRODUCT-0007 §7's
five-tier cascade and the third silently didn't. Plot chrome cascades library
→ application → authored → pane (0032 §2); the categorical palette cascades
identically (0030 §3); mark settings use all five tiers (0017 §2). Table
skins are a two-state model — `TableSkin::DEFAULT` or one authored name
(0025 §6, 0027 §1) — with no application tier, no pane tier, and no
`Override<…>` slot. Audit seam S3.

The consequence is seam S2: nothing can restyle a completed table. A pane
override restyles a completed live plot without re-preparation (0017 §5,
0031 §6); a table's skin is reachable only through a successful rerun. That
asymmetry was never decided — 0023 and 0025 simply predate the cascade's
second and third consumers — and the review resolved it: tables join the
cascade, with the design negotiated there and recorded in
`plans/product/review/consistency_audit.md` §4.

Two boundaries from that negotiation are load-bearing and restated up front.
**Portability stands**: 0025 §1's rule that a Rune buffer's skin never
consults `:set langtheme` is untouched — an application tier is Polariton's
own default, exactly `PlotThemeSpec`'s role for plots, not the editor's
theme. **Atomicity survives by re-scoping, not by accident**: 0023 §1's "a
`TableSkin` is ready to draw without consulting `gui::colors`, `UiPalette`, a
registry, or renderer defaults" is precisely the sentence a pane tier
falsifies, so this record supersedes it explicitly rather than leaving it
verbatim while meaning something different.

## Decision

### 1. The cascaded property is an intent, resolved like the plot skin

```text
TableSkinSpec { identity: theme expression, projection: PaletteForward | Duotone }

table_skin: Override<TableSkinSpec>
```

The tiers carry intent — a theme identity plus its projection — not twelve
resolved colors, for the reason 0032 §2 gives: resolution happens once, at a
defined moment, by the head. The cascade is PRODUCT-0007 §7's, right-biased,
with the graphical-layer tier inapplicable and inherited, exactly as 0030 §3
and 0032 §2 already state for their figure-wide properties:

```text
library floor -> application table skin -> authored .skin(name) -> pane override
```

- **Library floor**: absence all the way down resolves to `TableSkin::DEFAULT`
  — the compiled extraction, not a named theme, for 0032 §2's reason: naming
  an implicit theme would change default pixels.
- **Application tier**: Polariton's own optional default, the analog of
  `PlotThemeSpec`. Defined and tested; **no producer ships here** — the
  config surface that would set it is the expected stop-condition trigger.
- **Authored tier**: `table().skin(name)` and `.duotone()` populate it. The
  Rune grammar, `RuneTableSkinIntent`, the projection semantics, and every
  diagnostic are unchanged; the intent's destination moves from a direct
  constructor path into the cascade's authored slot.
- **Pane tier**: defined and tested via a test-level producer; no pane UI
  ships. When a pane surface exists, it produces this property and nothing
  else changes — the tier is the seam.

### 2. Completion bakes through the authored tier; the pane tier flattens at draw

`TableArtifact { frame, skin }` keeps its shape and its lifecycle. At sink
completion the head resolves the cascade **through the authored tier** —
library, application, authored — and bakes the result into the artifact, so
frame and skin still replace together, fail together, and go stale together
(0023 §5, 0027 §7 unchanged). An application-tier change therefore reaches
existing artifacts at their next completion, exactly as a plot's completed
chrome is retained with its kernel (0031 §8).

The **pane tier alone** is draw-time: the view layer flattens the artifact's
baked skin under any pane value and hands `Table::new` one resolved
`TableSkin`, which it still requires — 0023 §4's no-late-fallback rule is
preserved because flattening happens in the view layer before construction,
never inside `draw`. This is 0017 §4's shape: the artifact carries what was
completed; the widget retains the cascade beside it; the pane tier restyles
live without touching the artifact or its staleness pairing.

That closes seam S2 by construction: a completed table becomes exactly as
pane-reactive as a completed plot, no more, no less.

### 3. Unknown and unsuitable identities keep their semantics, per tier source

An authored identity that fails resolution keeps today's behavior exactly:
the destination fails, siblings complete, the previous frame-and-skin pair
stays stale (0025 §6, 0027 §7). An application-tier identity that fails
resolution is Polariton's own error, not the author's: it falls back to the
library floor and is reported as a diagnostic fact, never as a destination
failure — no author's artifact fails because our default was bad, the
PRODUCT-0035 §2 who-chose rule deciding failure attribution the same way it
decides contrast obligations.

### 4. SQL and Markdown tables join the cascade at the floor

`RESULT_PANEL`, SQL results, and Markdown cell tables resolve through the
same cascade with no authored tier — today that lands on the library floor
and renders byte-identically. When an application producer exists, they
follow it: the observed want behind the closed deferral CI-12 ("theme all
result tables app-wide") is served by this structure, which is why that
closure named this record.

### 5. What this record does not authorize

No Rune grammar or normal-form change. No application-producer config
surface, no pane UI, no `:set` coupling. No change to either projection, any
validation, any diagnostic text, `TableSkin`'s twelve roles, table geometry
or interaction, plot behavior, or `UiPalette`. No pixel changes anywhere.

## Acceptance gates

1. A bare `table()`, the SQL result panel, and a Markdown cell table render
   **byte-identically** to today through the cascade's library floor,
   measured against a recorded draw baseline banked before the re-homing.
2. `table().skin("terrain_rs10")` and `.duotone()` resolve through the
   authored tier to byte-identical skins and identical diagnostics as the
   current direct path, including failure and stale-retention behavior.
3. The cascade is right-biased across all four tiers, proven with test
   producers: application beats library, authored beats application, pane
   beats authored; the layer tier is inapplicable and inherited.
4. Completion bakes library→application→authored into `TableArtifact`;
   frame and skin replace together and a failed rerun retains the pair stale,
   asserted through the existing lifecycle tests unchanged.
5. A test-level pane value restyles a **completed** table at draw without
   modifying the artifact, its staleness pairing, or the frame — and its
   removal restores the baked skin, no rerun involved.
6. An application-tier change reaches an existing artifact only at its next
   completion, never by restyling retained content.
7. `Table::new` still requires exactly one resolved `TableSkin`; no draw
   path consults the cascade, `gui::colors`, or any fallback — flattening is
   complete before construction.
8. A failing application-tier identity falls back to the library floor with
   a diagnostic fact and fails no destination; a failing authored identity
   still fails its destination exactly as today.
9. `:set langtheme` is consulted nowhere in table skin resolution.
10. No Rune grammar, envelope, plot, `UiPalette`, geometry, interaction,
    identity, placement, or lifecycle behavior changes.

## Stop condition

> This record may add `TableSkinSpec`, the `Override<TableSkinSpec>` cascade
> slot with its four-tier resolution, the completion-time bake and draw-time
> pane flattening, re-homing of the authored intent, and the test producers.
> If the implementation requires a config or pane surface, an editor-theme
> coupling, a change to either projection or its validation, a Rune grammar
> change, a visible pixel change, or a change to table geometry,
> interaction, identity, placement, or lifecycle, the work stops and the
> requirement returns as its own record.

The expected trigger is the application producer: the moment someone wants
to *set* the application table skin, that is a configuration-surface record,
and this cascade is its one-line landing site.

## Supersession

This record supersedes, **only** where they predate the cascade:

- PRODUCT-0023 §1's "ready to draw without consulting anything" sentence —
  re-scoped: a `TableSkin` baked at completion is complete for its
  completion; the pane tier is the one draw-time layer, applied in the view
  before construction. §4's required-skin constructor and no-late-fallback
  rule are **not** superseded.
- PRODUCT-0025 §6's "absent means the exact existing `TableSkin::DEFAULT`" —
  absent now means `Inherit`, which resolves today to the same `DEFAULT`
  through the library floor: same pixels, cascade meaning.
- PRODUCT-0027 §1's `table() -> TableSkin::DEFAULT` line — reinterpreted
  identically as the floor of an otherwise-empty cascade.

Every projection rule, diagnostic, register, and lifecycle decision in those
records remains in force.

## Rejected

- **Cascading resolved `TableSkin` values instead of intent:** twelve colors
  per tier with no single resolution moment; 0032 §2 already chose identity-
  in-tiers, resolution-at-the-head for exactly this shape.
- **Making the application tier draw-time like the pane tier:** would restyle
  retained stale content, breaking 0023 §5's pairing; plots bake application
  chrome at completion (0031 §8), and tables should diverge from plots only
  on purpose, this being the record that exists to end an accidental
  divergence.
- **Failing a destination on a bad application identity:** punishes an author
  for Polariton's configuration; the who-chose rule (0035 §2) assigns the
  obligation to whoever picked the value.
- **Shipping an application producer now:** needs a config surface nobody has
  designed, and 0030 §3's staging precedent shows tiers can land proven and
  unproduced.
- **A named theme as the library floor:** changes default pixels by
  implication, rejected for tables exactly as 0032 §2 rejected it for chrome.
- **Waiting for the pane UI to define the pane tier:** the tier is the seam a
  future surface produces into; defining it now is what makes that surface a
  producer instead of a redesign.

## Consequences and sequencing

- The audit's S2 and S3 close together, S3 structurally and S2 as its
  consequence, and the language's three artifact kinds finally answer the
  cascade question identically.
- The unspecified audit cell "whether an application-theme tier could ever
  supply a table skin" is answered: yes, and the tier is waiting for its
  producer.
- SQL results and Markdown tables become one configuration decision away
  from following an application theme, serving closed deferral CI-12's
  observed want without reopening it.
- A future pane surface and a future configuration surface each land as
  producers into existing tiers — one-line integrations against a proven
  seam, the pattern 0030 §3 established.
- The review's ledger is fully paid: every seam the audit ranked is now
  dissolved, documented, or closed by a record.
