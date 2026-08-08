# ADR 0052: authored table column types

Status: accepted 2026-08-08. This record adds the opt-in `.show_types()` table
member without changing the application setting or any existing pixels.

## Context

Polariton already renders semantic data types beneath table column names when
`show_column_types` is true. The application has two producers for that value:
`config.show_column_types(bool)` initializes it, and the Settings checkbox
changes it during a session. Existing Rune, SQL-result, and Markdown-cell
tables redraw when the live value changes. `TableSkin::type_text` already
styles the labels under every accepted skin projection.

Rune authors cannot request the same presentation for one table. The existing
postfix register admits `.skin(...)` and its dependent `.duotone()` member, but
its parser currently treats a named skin as a prerequisite for *any* modified
table. Extending that behavior mechanically would make `table().show_types()`
refuse even though type visibility has no skin dependency.

Stage 0 found and rejected a broader claim before this record was written.
PRODUCT-0045 §2 and gate 6 deliberately bake the unproduced application skin
tier at completion. Column-type visibility is observably live before this cut.
This record does not choose a universal table-property lifecycle and does not
supersede 0045; it preserves the lifecycle its existing producer already has.

## Decision

### 1. `.show_types()` extends the closed table register

The accepted authored spelling is:

```rune
table().show_types()
```

It takes no arguments, may appear at most once, and is valid in every proven
table-sink position that already accepts `.skin(...)`. Modifier order remains
irrelevant: the register is flattened as a set, not executed as a pipeline.

Dependencies belong to members:

- `.show_types()` has no dependency;
- `.skin(name)` has no dependency;
- `.duotone()` requires `.skin(name)`.

Therefore all of these are valid and equivalent where their sets match:

```rune
table().show_types()
table().skin("nord").show_types()
table().show_types().skin("nord").duotone()
table().duotone().show_types().skin("nord")
```

`table().duotone().show_types()` refuses because only `duotone`'s dependency
is absent. Unknown members, duplicates, wrong arities, nonliteral skins, and
members outside a proven sink keep the existing atomic refusal posture.

### 2. The artifact retains authored intent; draw preserves the live producer

Column-type visibility uses PRODUCT-0007 §6's value algebra:

```text
authored_show_column_types: Override<bool>
```

`.show_types()` produces `Set(true)`; omission produces `Inherit`. The completed
`TableArtifact` retains that `Override<bool>` beside its frame and resolved
skin. It does **not** retain a boolean resolved against the application value.
At draw, the existing live application boolean is overlaid by the retained
authored intent:

```text
resolved = authored.unwrap_or(application)
```

This preserves current behavior: an inherited table follows Settings changes
without recompletion, while an authored `.show_types()` table stays visible
when the application value is false. Successful recompletion replaces frame,
skin, and authored type intent together; failure retains the complete prior
artifact stale. Adding or removing `.show_types()` is content, not identity.

The carrier deliberately represents `Set(false)`, but no `.hide_types()`
producer ships. The application default is false, and no exhibit yet asks one
authored table to negate an application-wide true value.

### 3. Other table surfaces remain application-only

SQL-result and Markdown-cell tables have no authored table-sink register. Their
artifacts carry `Inherit`, so both continue following the same live application
value. Bare Rune `table()` does likewise. No configuration, Settings, skin,
geometry, interaction, table identity, or plot behavior changes.

### 4. Uniform application-tier liveness remains undecided

Whether every table property should eventually apply application-tier changes
live is not decided here. The trigger is concrete: the first production
producer for PRODUCT-0045's application skin tier must decide whether existing
artifacts restyle immediately or only after completion, and supersede 0045 if
it chooses the former. This record neither creates that producer nor changes
the skin artifact.

## Acceptance gates

1. `table().show_types()` succeeds alone. It composes in either order with
   `.skin(...)` and `.duotone()`, including inside `view(name, ...)`, and equal
   member sets flatten to equal normal forms.
2. Duplicate or nonzero-arity `.show_types()`, `duotone` without `skin`, and
   unknown table members refuse the complete destination at the full chain
   span. `.show_types()` outside a proven table sink remains ordinary Rune.
3. The envelope and completed artifact carry `Override<bool>`, not a boolean
   pre-resolved against application state. A failed rerun retains the prior
   authored intent with the stale frame and skin.
4. The two-authority matrix is asserted behaviorally:

   | application | authored | rendered types |
   | --- | --- | --- |
   | false | `Inherit` | hidden |
   | true | `Inherit` | shown |
   | false | `Set(true)` | shown |
   | true | `Set(true)` | shown |

   A direct substrate assertion also proves `Set(false)` remains representable
   and wins over application `true`, without adding an authored producer.
5. Changing the live application value after completion redraws bare Rune,
   SQL-result, and Markdown-cell tables without recompletion. A completed
   `.show_types()` Rune table remains shown through the same changes.
6. Visible labels retain the exact existing semantic-type text, placement, and
   `TableSkin::type_text` color under the default and an authored skin. No new
   renderer fallback or styling path is admitted.
7. With the application default false and no `.show_types()` in existing
   buffers, every existing table draw baseline is byte-identical. No expected
   image or SVG is refreshed; any default pixel change fails the record.
8. SQL, Markdown, table scrolling, column resizing, selection, skin projection,
   plot behavior, identity, and destination failure semantics pass unchanged.

## Implementation evidence

- `RuneTableSink` carries `show_column_types: Override<bool>`. The closed
  register accepts the zero-argument member alone or in any order with skin
  members; only `duotone` checks for its named-skin dependency. Full-chain
  spans and exact diagnostics gate wrong arity, duplication, and missing skin.
- `RuneTablePresentation` crosses completion with the resolved `TableSkin` and
  the unresolved authored visibility intent. `TableArtifact` retains those two
  values with its frame. The production ordinary and Markdown draw paths both
  call the same artifact resolver against the application boolean.
- Five PRODUCT-0052 tests cover register normalization and refusal, the
  completion handoff, the two-authority matrix across already-completed Rune,
  SQL, and Markdown artifacts, stale retention, synthetic `Set(false)`, and
  the existing renderer's semantic text, vertical position, and `type_text`
  color. The matrix reuses each artifact at both application values; no
  recompletion hides a baked boolean.
- The default draw and an explicit false draw have byte-identical operation
  signatures. PRODUCT-0023's full pre-extraction operation oracle and every
  other existing draw baseline pass without changing one expected byte.
- `crates/polariton/README.md` documents `.show_types()` and removes the stale
  column-type-label backlog entry while retaining the application-wide
  configuration example.

Three architecture expectations are premise updates, not product changes:
PRODUCT-0023's artifact census now names frame, resolved skin, and authored
type intent instead of exactly two fields; its head handoff now names
`RuneTablePresentation` instead of a bare `TableSkin`; and PRODUCT-0027's old
all-modifiers skin prerequisite now asserts `duotone`'s member-local dependency.
All 40 architecture gates pass. No image, SVG, or operation-oracle constant was
refreshed. The full workspace suite passes; its Polariton portion is 435 unit
tests passed (five live tests ignored), 40 architecture tests, and the trybuild
UI pair.

## Stop condition

> This record may add only the zero-argument `.show_types()` table member, its
> retained `Override<bool>` intent, draw-time overlay with the already-live
> application producer, and the named gates. If implementation requires a
> `.hide_types()` spelling, a skin lifecycle change, a new application or pane
> producer, type-label styling, table geometry changes, or any plot change, the
> work stops and that requirement returns as its own record.

## Rejected

- **Resolve the boolean at completion:** silently makes the existing Settings
  producer stale for Rune tables and loses the `application=true, Inherit`
  matrix cell after a live toggle.
- **Give every modified table a skin dependency:** confuses one member's
  dependency with register membership and makes the requested spelling refuse.
- **Ship `.hide_types()` simultaneously:** adds a surface for a use not yet
  exhibited. `Set(false)` remains available when that request arrives.
- **Use `Presence`:** column-type visibility always resolves to a boolean;
  there is no generated versus authored content distinction for `Hide` to
  preserve.
- **Make application skins live in this cut:** supersedes PRODUCT-0045 without
  a producer or exhibit and turns a bounded opt-in feature into a structural
  lifecycle redesign.

## Consequences

- A Rune table can opt into the type labels Polariton already knows how to
  render and skin.
- Existing buffers and default screenshots do not move by one pixel.
- The artifact records authored intent precisely enough to preserve both stale
  completion semantics and the already-live application control.
- The fourth Stage 0 contradiction caught in this arc is recorded as evidence
  for feasibility review: checking accepted lifecycle gates prevented a small
  feature from silently superseding an unrelated record.
