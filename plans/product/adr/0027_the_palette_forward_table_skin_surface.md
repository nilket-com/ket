# ADR 0027: the palette-forward table skin surface

Status: accepted 2026-08-05, with contrast admission and advice amended by
PRODUCT-0035. This record decides one thing: that a named Rune
table skin uses PRODUCT-0026's `PaletteForward` projection unless the table sink
explicitly requests `.duotone()`. The default flip and the escape hatch are one
indivisible surface change. It adds no palette policy, table role, plot skin,
data coloring, editor-theme coupling, or configuration surface.

## Context

PRODUCT-0025 made libaryon's compact theme expressions selectable from Rune:

```rune
table().skin("terrain_rs10")
```

That first surface projected every name through the useful high-contrast policy
now called Duotone. PRODUCT-0026 preserved all 8,480 canonical outcomes of that
policy byte-for-byte, added PaletteForward beside it, and deliberately gave the
new projection no production caller. Every substrate caller must still state
its projection explicitly; only this Rune surface may decide what absent
projection syntax means.

PaletteForward better reflects the shared vocabulary's origin as ordered plot
colormaps. It spends eight resolved swatches directly on table surfaces and
accents, then selects measured neutral text. The differential assay is
`yellows`: Duotone's raw anchors clear `4.5:1`, but its completed selected-row
role falls to about `3.175:1`; PaletteForward completes with a weakest ratio of
about `15.790:1`. The validation threshold did not change. The projection did.

Duotone remains worth keeping. Its restrained two-anchor treatment is the real
window style that motivated `terrain_rs10`, and PRODUCT-0026's exact oracle
makes it an accepted capability rather than an implementation to delete. The
surface therefore needs a compact opt-in at the same moment its implicit choice
changes.

The implementation hazard is historical rather than algorithmic. If the head
switches `table().skin(name)` to PaletteForward before `.duotone()` lowers,
accepted Duotone becomes unreachable from Rune. If `.duotone()` lowers while
the old head default remains, the new modifier is observationally redundant.
Those are not useful intermediate states. They are a split implementation of
one decision.

## Decision

### 1. Absence chooses PaletteForward only for a named skin

The surface means exactly:

```text
table()                            -> TableSkin::DEFAULT
table().skin(name)                 -> PaletteForward(name)
table().skin(name).duotone()       -> Duotone(name)
```

“Default projection” in this record means the projection selected after an
author supplied `.skin(name)`. It does **not** mean that bare `table()` acquires
a libaryon palette. Bare Rune tables, SQL results, and Markdown tables retain
the exact PRODUCT-0023 default artifact.

The substrate API remains two-argument and default-free. The head maps the Rune
intent to `TableSkinProjection::{PaletteForward, Duotone}` explicitly; no
`Default`, `Option<TableSkinProjection>`, or one-argument
`TableSkin::from_theme_name` returns below the surface.

### 2. `.duotone()` extends only the table modifier register

The closed table-sink register becomes:

```text
skin(string_literal)   exactly once when a named skin is requested
duotone()              at most once, and only with skin(...)
```

`duotone()` takes no arguments. It is a treatment of an authored palette
identity, so `table().duotone()` refuses rather than styling the compiled
default. Modifier order remains semantically irrelevant under PRODUCT-0025 §4:

```rune
table().skin("terrain_rs10").duotone()
table().duotone().skin("terrain_rs10")
```

lower to the same intent. The first spelling is canonical documentation because
it reads identity before treatment; the second is accepted evidence that the
postfix chain is a flattened register rather than an execution pipeline.

Duplicate `skin` or `duotone`, nonzero `duotone` arity, missing skin identity,
unknown table modifiers, and cross-register modifiers refuse the complete outer
sink span. Outside a proven `.views([...])` table-sink position, these calls
remain ordinary Rune. Relation `r.table()` sugar remains the unstyled default
and gains no modifier spelling.

### 3. The normal form makes incomplete combinations unrepresentable

The current `Option<String>` becomes a table-specific intent equivalent to:

```text
RuneTableSkinIntent =
    Default
  | Theme {
        name: String,
        projection: PaletteForward | Duotone,
    }

RuneTableSink {
    skin: RuneTableSkinIntent,
}
```

The projection enum in this normal form is source intent, not the GUI substrate
type. The envelope remains independent of `gui::table_skin` and carries no
resolved theme, palette, color, `TableSkin`, or suitability fact. The head maps
the two intent variants exhaustively to PRODUCT-0026's two substrate variants.

This shape has no state for “Duotone without a name,” “name without a decided
projection,” or a projection on the compiled default. The complete modifier
chain is flattened before the normal form; source order does not survive.

### 4. Projection failure never changes the authored mechanism

The head resolves exactly the requested projection first. A successful result
becomes the table sink output. A failed projection never renders the other
projection, `TableSkin::DEFAULT`, or partial content.

For ordinary PaletteForward syntax, failure reports PRODUCT-0026's
PaletteForward facts and stops. The head does not probe Duotone or recommend
adding `.duotone()`. Bidirectional recommendation is not needed to establish
the escape hatch and is deferred until actual PaletteForward refusal is a user
problem rather than a census statistic.

For explicit `.duotone()`, the requested Duotone result remains authoritative.
The head may evaluate PaletteForward only to decide whether removing the
modifier is truthful advice. A successful probe does not become content; the
sink still fails and retains any previous artifact stale.

### 5. Duotone refusal teaches only a working removal

The diagnostic policy has three explicit branches:

1. **Unknown theme.** Report the existing unknown-name diagnostic. There is no
   palette to project, so do not probe PaletteForward and do not mention
   `.duotone()`.
2. **Duotone refuses; PaletteForward succeeds.** Preserve the complete Duotone
   factual diagnostic, then state that the default projection succeeds and
   that removing `.duotone()` selects it. `yellows` is the concrete assay. The
   message must describe the failed completed role rather than falsely blaming
   its above-threshold raw anchor range.
3. **Both projections refuse.** Report the requested Duotone diagnostic without
   suggesting a broken alternative. `dark2_s1` is a registered, canonical
   both-failure assay; no synthetic palette or new constructor is needed here.

The probe is pure theme/color resolution over the same name. It emits no panel,
content, placement, or second sink outcome. Advice is head policy composed from
substrate facts, not a new fallback mode or a change to PRODUCT-0026 errors.

### 6. The flip and modifier are one implementation commit

The production change that first recognizes `.duotone()` must also change
unmodified `.skin(name)` completion from explicit Duotone to explicit
PaletteForward. Neither half may appear in an earlier or later commit, behind a
temporary feature flag, as dead grammar, or as cleanup after the other half.

Oracle banking, tests that describe existing behavior, and implementation-plan
documents may precede that commit. No production source may enter either broken
intermediate state. The implementation plan must name the indivisible commit,
and the evidence must inspect its actual diff and parents. This is a historical
acceptance condition; a final-tree test cannot prove it after the fact.

### 7. Identity, lifecycle, and placement remain unchanged

Projection is content, not view identity. Adding or removing `.duotone()` on
the same named or positional table view reuses its `HostPanelId`, source label,
group membership, tile, topology, ratios, focus, and tab state. It does not
retitle panel chrome, reflow a producer group, or open the dashboard.

Successful completion still installs one atomic `TableArtifact { frame, skin }`.
Unknown names and suitability failures remain per destination: sibling sinks
over the same stamped result may complete, while the failed destination keeps
its previous frame/skin pair stale. No state pairs new data with an old skin or
old data with a newly probed alternative.

### 8. Plot skins and the ahead-of-language fixture stay out of scope

This record adds table treatment only. `scatter(...).skin(...)`,
`line(...).skin(...)`, and `bar(...).skin(...)` remain cross-register refusals.
Figure chrome and mark data color remain separate deferred decisions.

The current `../uat/test9.rn` intentionally contains the proposed
`bar(...).skin("greens")` spelling. PRODUCT-0008 atomic recognition therefore
continues refusing that complete view program before dispatch, leaving its last
good panels stale. That is not a PaletteForward failure and this record neither
edits the fixture nor treats its current window as visual acceptance. A focused
table-only corpus assay supplies the executable evidence until plot skin syntax
has its own accepted record.

## Acceptance gates

1. Bare `table()` remains the exact compiled default;
   `table().skin("terrain_rs10")` lowers to PaletteForward; adding `.duotone()`
   lowers the same name to Duotone. The two successful skins equal direct
   PRODUCT-0026 resolution for their requested mechanisms.
2. Direct and named-view table sinks accept the canonical modifier spelling;
   reversed modifier order lowers to the identical normal form, identity, and
   completed skin.
3. Missing skin identity, duplicate modifiers, nonzero `.duotone()` arity,
   malformed chains, unknown table methods, and cross-register methods refuse
   atomically at the outer sink span. Outside proven sink positions they remain
   ordinary Rune, and `r.table()` is unchanged.
4. The normal form represents only compiled default or a name plus one closed
   projection intent. It contains no GUI substrate type, resolved palette,
   color, `TableSkin`, or source-order modifier chain.
5. `table().skin("yellows")` succeeds with the exact PRODUCT-0026
   PaletteForward skin and all seven ratios at `4.5:1` or greater.
6. `table().skin("yellows").duotone()` fails its destination with the truthful
   completed-role Duotone diagnostic plus a working instruction to remove
   `.duotone()`. It renders no PaletteForward fallback or partial artifact.
7. An unknown theme under either spelling retains the unknown-name diagnostic,
   performs no alternative probe, and offers no projection advice.
8. `table().skin("dark2_s1").duotone()` reports the requested Duotone role and
   ratio without suggesting PaletteForward because the default also refuses.
   Bare `table().skin("dark2_s1")` reports PaletteForward facts without probing
   or recommending Duotone.
9. Adding or removing `.duotone()` reuses the same destination and producer
   layout. Success replaces frame plus skin atomically; failure preserves the
   previous pair stale while sibling sinks retain the same shared result
   identity and complete independently.
10. The implementation commit contains both the `.duotone()` grammar/normal
    form and the head's PaletteForward default flip. Neither change exists in
    its parent or a separate follow-up commit, flag, or cleanup.
11. PRODUCT-0026's 8,480-result Duotone oracle and two-mode census remain green;
    PRODUCT-0023 draw/contrast and PRODUCT-0025 identity/register/lifecycle
    behavior change only where this record explicitly supersedes the named-skin
    default and register vocabulary.
12. SQL and Markdown tables, table geometry, panel chrome, `UiPalette`,
    libaryon themes, plot rendering, dashboard, execution, transport, identity,
    and placement behavior are unchanged. No plot mark accepts `.skin(...)`.

## Stop condition

> This record may change the Rune meaning of named table-skin absence to
> PaletteForward, add the zero-argument `.duotone()` member of the closed table
> register, replace `Option<String>` with the bounded intent in §3, map that
> intent explicitly in the head, and compose the three-branch Duotone teaching
> diagnostic from PRODUCT-0026 results. If implementation requires changing
> libaryon, either projection's role policy or validation, PRODUCT-0023 roles or
> contrast math, the substrate API, editor `UiPalette`, SQL/Markdown defaults,
> table geometry, panel identity or placement, plot syntax/rendering, dashboard,
> execution, transport, or the current ahead-of-language dogfood fixture, it
> stops.

## Rejected

- **Flip the default before adding `.duotone()`:** makes accepted Duotone
  unreachable from Rune in an intermediate commit.
- **Add `.duotone()` before flipping the default:** ships a redundant modifier
  whose presence cannot be observed.
- **Default the substrate API:** lets lower-layer callers drift when only the
  Rune surface owns the meaning of absent projection syntax.
- **Apply Duotone to bare `table()`:** invents a palette identity where the
  author supplied none and changes SQL/Markdown compatibility by analogy.
- **Encode projection in the theme string:** leaks a table-artifact choice into
  libaryon's shared palette vocabulary.
- **Silently render the successful alternative:** turns a failed authored
  mechanism into undeclared fallback and breaks stale-content honesty.
- **Suggest an unvalidated alternative:** converts factual diagnostics into a
  guess. Advice appears only after the exact alternative succeeds.
- **Probe Duotone after ordinary PaletteForward failure:** adds bidirectional
  recommendation policy before PaletteForward refusal has become a felt surface
  problem.
- **Generalize `.skin()` to plot marks:** conflates table artifact chrome with
  figure chrome and data encoding, both separately deferred.

## Consequences

- Rune can finally reach PaletteForward without weakening PRODUCT-0023 or
  changing libaryon's theme authority.
- Duotone remains one token away and retains every accepted PRODUCT-0025 result.
- `yellows` becomes a useful table skin under the ordinary spelling, while an
  explicit incompatible treatment fails with a truthful repair.
- The compact theme-expression vocabulary remains portable and git-friendly;
  projection stays an artifact decision made above it.
- The 29.5% PaletteForward refusal rate is now observable from real source. Any
  later per-surface-ink proposal must justify its added acceptance against
  ink-flip frequency rather than treating acceptance alone as improvement.
