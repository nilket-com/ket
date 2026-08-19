# ADR 0030: the categorical palette figure surface

Status: accepted 2026-08-05, with gate 16 open and named — the real-window
fidelity observation has not yet been confirmed. Accepted-with-an-open-gate
means the record's decisions are landed and may be depended upon (PRODUCT-0032,
0036, and 0038 already do) while one named observation gate remains
deliberately unclosed; it replaces the prior "proposed" status, which was
untrue the moment other accepted records built on this one. Status corrected
2026-08-06 by the architectural review's record-accuracy pass. This record
decides one thing: how a Rune figure
selects the categorical palette used by plots. It adds `palette(name)` as
figure-wide metadata, gives absence the explicit `viridis` library default,
switches Rune plots from PRODUCT-0029's legacy compatibility resolver to its
palette-aware resolver, and retains live categorical identity across reruns and
authored palette changes. It adds no inferred color channel, perceptual palette
ranking, plot chrome skin, general scale composition, guide policy, or libaryon
behavior.

## Context

PRODUCT-0028 made libaryon's compact theme language authoritative for natural
categorical palettes. A resolved theme may expose an immutable non-empty
sequence in its declared order and natural length. Continuous families expose
twenty colors; `tab20*`, `dark2`, `kelly`, and `prism` expose their accepted
curated sequences; syntax-only curated themes such as `nord` deliberately
expose none. Reverse and shift are already resolved by libaryon over that
natural sequence.

PRODUCT-0029 supplied the product-side consumer. `ColorIdentity` retains stable
validated `PalettePosition` values, a completed `ResolvedPlotSpec` retains exact
colors, a known fresh domain uses even spread, and a live extension uses
deterministic maximin without moving an established category. Two-field bars
may carry exact band fills as mark styling without manufacturing a Color
channel. Geometry and palette capacity remain independent facts.

That path is intentionally unreachable from Rune. `plot::request::plan` still
calls the legacy `resolve_domains` with a fresh `ColorIdentity::default()`
(`plot/request.rs:80`), and `rune_view_outcome` uses that request for Rune single
plots (`gui/state.rs:851`). The compatibility path preserves the old twelve
colors, assigns them sequentially, and gives a two-field bar no band fills.
PRODUCT-0029 §2 requires the first authored selection and the production caller
switch to arrive together.

The surface also has an accepted home. PRODUCT-0007 §5 gives plot-wide stages
named positions instead of extending mark argument lists, and PRODUCT-0016
makes `figure([...])` the level that carries figure-wide metadata. Palette
selection is not a Color channel: it neither names a field nor changes the
channel's `ColorJob`. It selects the categorical sequence used if resolution finds
an already-authored categorical color channel or PRODUCT-0029's eligible bar
band treatment.

The motivating case is deliberately ordinary:

```rune
figure([
    title("SQL = 💩"),
    palette("tab20c"),
    bar("cat", "val").bar_width(0.98),
])
```

The author may prefer `tab20c_s2`, `yellows`, or a combination another person
finds pale, loud, or ugly. That is not a structural error. A compact palette
grammar is useful because it gives the author a large, deterministic choice
space; the product must not turn aesthetic advice into admission control.

## Decision

### 1. `palette(name)` is figure-wide metadata

The canonical spelling is:

```rune
figure([
    palette("tab20c"),
    bar("cat", "val"),
])
```

`palette` takes exactly one string literal and appears at most once in a
figure. Like `title` and `subtitle`, its position relative to other figure
metadata and the body carries no meaning. The figure still contains exactly
one body.

`palette(...)` is recognized only as a direct member of a proven `figure([...])`
sink inside `.views([...])`. These refuse at their own proven spans:

- duplicate palette members;
- zero, two, or more arguments;
- a non-string-literal identity;
- `palette(...)` inside `layers([...])`;
- `palette(...)` used as the figure body;
- a nested `figure` or any other existing malformed figure shape; and
- mark-level `.palette(...)` or `.skin(...)` methods.

Outside those proven positions the call remains ordinary Rune under
PRODUCT-0008 §3. The retained `plot.*` namespace, bare relation sugar, table
modifier register, and mark modifier registers gain no spelling from this
record.

The receiver distinction is intentional. Table artifact treatment remains
`table().skin(name)`. Plot data colors use `palette(name)` at the figure level.
`skin` dresses artifact surfaces; `palette` selects a data-color sequence. Giving
both concepts one receiver-sensitive word would obscure the boundary that
PRODUCT-0007 §10 preserves between chrome and data encoding.

The language now has three attachment points:

| position | attaches to | accepted examples |
|---|---|---|
| member of `figure([...])` | the complete graphical artifact | `title`, `subtitle`, `palette` |
| postfix on a mark | that mark's geometry or Layer-stage styling | `.point_size`, `.stroke_width`, `.bar_width` |
| postfix on `table()` | the table artifact's interior treatment | `.skin`, `.duotone` |

The rule is structural: a property of the whole picture is a figure member; a
property of one mark's drawing is a postfix on that mark. Palette is the color
vocabulary shared by the whole picture, so it is a sibling of `title` and
`subtitle`, not a child or method of `bar`.

### 2. The normal form carries identity, not resolved color

`RunePlotIntent` gains one figure-wide authored property equivalent to:

```text
categorical_palette: Override<Arc<String>>
```

The property occupies the authored-spec tier of PRODUCT-0007 §7's settings
cascade. It may live beside the existing `PlotSettingsLayer` fields or in an
equivalent bounded figure-settings carrier, but there is one semantic property
and one resolution path. The normal form carries no `Theme`,
`ResolvedCategoricalPalette`, `CategoricalPaletteSelection`, `Rgb`, palette
position, `ColorIdentity`, resolved assignment, or kernel object.

The complete figure is flattened before this normal form. Metadata source order
does not survive, and the palette identity is not copied into any mark or plot
specification. `AuthoredPlotSpec` and `LayeredPlotSpec` remain unmigrated.

Omission is `Override::Inherit`, not `None` meaning “let the resolver guess.”
The resolved categorical selection is obtained by cascading the applicable
tiers and then calling PRODUCT-0029's
`CategoricalPaletteSelection::from_theme_name` exactly once. Libaryon remains
the sole parser for bases, aliases, `r`, `c`, `C`, and `sN`.

### 3. The Rune library default is the explicit identity `viridis`

For Rune figure completion, absence resolves to the exact libaryon identity:

```text
library default categorical palette = "viridis"
```

This matches `PlotThemeSpec::DEFAULT_DARK.categorical_ramp`, which is currently
`ColorTheme::Viridis` (`plot/theme.rs:59`), while moving categorical resolution
onto libaryon's authoritative natural palette rather than Polariton's duplicate
ramp enum.

The precedence property is:

```text
library default -> application theme -> authored figure -> pane override
```

The graphical-layer tier is `Inherit` for this figure-wide property. This is the
same five-tier order accepted by PRODUCT-0007 §7 with one inapplicable tier, not
a second cascade. This cut supplies the library and authored producers. Future
application-theme or pane-override producers use the same property and do not
change Rune syntax.

The substrate remains default-free. `CategoricalPaletteSelection`,
`resolve_domains_with_categorical_palette`, and libaryon do not acquire a
one-argument overload, `Default`, fallback identity, or knowledge that Rune
chooses `viridis`. The head composes that policy from lower-layer facts.

This default intentionally supersedes two pieces of Rune compatibility behavior:

- an authored categorical color channel is allocated over the twenty-position
  libaryon `viridis` sequence rather than the legacy twelve-color constant; and
- a two-field bar with an absent Color channel receives stable `viridis` band
  fills rather than one renderer-sampled treatment.

Line and scatter with an absent Color channel remain single-treatment under
PRODUCT-0029 §8. Non-Rune callers of the legacy request entrypoint retain their
current compatibility behavior; retiring that entrypoint and Polariton's
remaining `ColorTheme` catalog is a separate record with its own parity census.

### 4. Authored choice is authoritative, not advisory

Every identity that resolves and declares a categorical palette is admissible.
Polariton applies its exact ordered colors even when the result is low-contrast,
nearly monochromatic, visually loud, culturally awkward, or aesthetically poor.
It performs no perceptual distance scoring, contrast threshold, saturation
threshold, hue-count check, warning-driven substitution, or automatic rotation.

The author controls the cut and orientation through libaryon's existing compact
grammar. `_sN` rotates the ordered sequence and `_r` reverses it; Polariton consumes the
resolved result and does not “improve” it. A valid but ugly choice renders.

Only structural impossibility refuses:

1. the theme identity is unknown;
2. the known theme declares no categorical palette; or
3. the resolved domain exceeds the geometry limit, palette length, or both.

The third case is not aesthetic policing. PRODUCT-0007 forbids eviction,
reassignment, or silent cycling at capacity because each would recolor an
established meaning. Explicit cyclic reuse is a different semantic feature and
would require an authored spelling plus collision and guide rules of its own.

A palette member is legal even when the current body has no categorical
consumer. For example, a line-only figure may carry `palette("tab20c")` and
render unchanged. Figure-wide settings are stable under body edits; the surface
does not make source validity depend on whether the current data resolves a
categorical job. It also does not manufacture a consumer merely to make the
setting visible.

### 5. Consumers remain exactly those accepted by PRODUCT-0029

The selected categorical palette may affect only:

- an authored Color channel whose resolved `ColorJob` is `Categorical`; and
- PRODUCT-0029 band fill for a two-field bar with an absent Color channel and a
  categorical band domain.

The first is an encoding the author already named. The second is mark styling
over an already-resolved band domain and creates no Color channel. In both cases
the completed `ResolvedPlotSpec` carries exact category-to-position-to-color
assignments and the renderer receives no palette or theme expression.

An authored sequential, diverging, or status color job remains authoritative
and does not consume the categorical palette. A line, two-field scatter, or
layered line/scatter composition with absent color does not acquire repeated
colors. PRODUCT-0013's requirement that every layered color channel be Absent
is unchanged.

Palette selection changes neither plot chrome nor `ResolvedPlotSettings`' legacy
renderer ramp. Background, axes, grid, labels, table skins, `UiPalette`, and
non-categorical kernels remain outside this projection. Plot chrome skinning and
the broader retirement of `ColorTheme` remain separate records.

### 6. Position identity survives same-length palette changes

For an established view and semantic lineage, a palette expression selects an
ordered sequence of natural length `m`. Palette identity and modifiers are not added to the
PRODUCT-0007 lineage key. Instead, the head retains continuation state in a bank
keyed by:

```text
(producer BufferId, LocalViewIdentity, natural palette length) -> ColorIdentity
```

Field and layer identity remain internal lineage keys inside `ColorIdentity`.
This produces the following behavior:

- `tab20c -> tab20c_r -> tab20c_s1` reuses the same twenty-position identity;
- every category keeps its `PalettePosition`, while exact colors are reprojected
  through the newly resolved sequence;
- `viridis -> tab20c` does the same because both natural lengths are twenty;
- `tab20c -> dark2` selects a separate eight-position bank, created fresh if it
  has never been used for that view;
- changing back to `tab20c` resumes the retained twenty-position bank; and
- removing an authored palette returns to the `viridis` selection and the
  twenty-position bank.

This is deliberate recoloring caused by an authored treatment change, not the
silent filtering or render-order recoloring PRODUCT-0007 forbids. Positions
continue to follow meanings. A same-length palette change alters only the
projection of those positions; a different-length sequence has a different finite
allocation space and therefore cannot reinterpret the old positions honestly.

Natural length, not theme name, keys the bank because positions are ordinal
members of the selected sequence. Keying by full expression would make `_r` and
`_sN` lose continuation and would turn palette modifiers into a reset button. Keeping
only one bank and clearing it on a length change would make toggling a palette
destroy accepted live identity for no structural reason.

### 7. Continuation belongs to the head and commits with content

`ColorIdentity` is session-scoped plot continuation, not Rune normal form,
relation data, Toron state, renderer state, or libaryon state. The head owns the
bank beside its existing producer/view realization maps.

Resolution receives a clone of the current bank entry and returns a proposed
next `ColorIdentity`. The proposal is committed only when the matching current
revision successfully completes the plot artifact. Channel failure, palette
failure, capacity failure, kernel-construction failure, stale superseded
revision, or destination failure commits no new assignment.

That timing keeps identity and visible content coherent. A category that never
became visible cannot reserve a position merely because an earlier stage of a
failed run observed it. Successful completion replaces the resolved plot and
advances its matching identity atomically from the user's perspective.

Independent origins and views use independent banks. Their execution completion
order cannot affect one another. Several sinks over one stamped result still
share that result by `Arc` identity; palette continuation does not alter
PRODUCT-0010's origin-keyed execution or fan-out guarantee.

Removing a view from source does not immediately discard its bank, matching the
orphan-retention posture of PRODUCT-0010 and PRODUCT-0015. Reintroducing the same
named or positional identity in the same live producer may resume it, with the
field/layer lineage key preventing an unrelated categorical field from
inheriting assignments. Once the producer buffer is actually released,
reopening receives a new monotonically allocated session `BufferId`
(`engine/session.rs:943`) and therefore cannot reach the old banks. Polariton
does not currently receive a text-buffer release event: libaryon observes
release internally (`engine/runtime.rs:1994`) and exposes `PanelDisposed` only
for host-panel buffers, not the released producer. This cut therefore retains
unreachable bank entries until process teardown. Adding a lifecycle event merely
to collect them would be a protocol change with no semantic effect and triggers
§12's deferral. Durable continuation remains blocked on workspace layout and
document identity persistence.

### 8. Failures are factual, attributed, and atomic

Malformed surface syntax refuses during envelope analysis at the offending
member or outer figure span under PRODUCT-0008's atomicity rule. A resolved
unknown identity, missing categorical capability, domain failure, or capacity
failure is attributed to the figure sink span, following PRODUCT-0014 §6's
compatibility-handoff precedent.

Diagnostics preserve the substrate facts:

- unknown identity reports libaryon's existing name failure;
- a known syntax-only theme says that the exact identity has no declared
  categorical palette; and
- capacity reports requested cardinality, geometry limit, exact palette
  identity and natural length, and whether either or both limits were exceeded.

No failure substitutes `viridis`, another palette, the legacy twelve colors, a
single bar fill, a truncated sequence, a perceptually “better” sequence, or partial
assignments. The failed destination retains its previous complete plot stale;
sibling sinks over the same stamped result may still complete, and no partial
kernel or continuation update escapes.

An explicit palette on a figure with no current categorical consumer is not a
failure and produces no warning. Nothing impossible or ambiguous was requested;
the setting is simply unused by that body.

### 9. Surface recognition and the caller switch are indivisible

The production commit that first recognizes `palette(name)` must also:

- resolve absence to explicit `viridis`;
- route Rune single plots through PRODUCT-0029's palette-aware resolver;
- carry proposed continuation state to successful completion; and
- retire the Rune path's fresh per-call legacy identity.

Neither surface nor caller switch may appear in an earlier or later production
commit, behind a temporary flag, as dead grammar, or as cleanup after the other
half. Before the switch, the accepted legacy path remains internally coherent;
after it, the authored setting and default both have one proved consumer. There
must be no commit where the syntax is accepted but ignored, nor one where Rune
plots recolor and bars gain fills before the authoring contract exists.

Oracle banking, tests, representation scaffolding unused by production, the ADR,
and the implementation plan may precede the indivisible commit. Evidence must
inspect that commit and its parent because this historical property cannot be
proved by a final-tree runtime test.

### 10. Real-window evidence proves fidelity, not taste

The high-density six-category bar fixture moves to `../uat/test9.rn` and uses a
figure member rather than the ahead-of-language mark spelling:

```rune
figure([
    title("SQL = 💩"),
    subtitle("Wendelon was here"),
    palette("tab20c"),
    bar("cat", "val").bar_width(0.98),
])
```

The real-window assay confirms that one ordinary host panel renders the exact
six PRODUCT-0029 assignments `c01`, `c05`, `c09`, `c12`, `c16`, and `c20`, in
the established category mapping, with no dashboard and no Color channel.
Changing the member to `palette("tab20c_s1")` must visibly consume the shifted
sequence without changing category positions or panel identity.

This is a fidelity and integration assay, not palette admission. Pale green,
pale lavender, adjacent hues, low contrast, or an author's poor choice do not
fail it. If the exact authored colors reach the bars, the choice is honored.
The screenshot may motivate a later authored change or a different `_sN`; it
may not authorize Polariton to refuse or substitute the selection.

### 11. Identity, labels, layout, and execution do not consult palette

Palette identity is content. Adding, removing, or changing `palette(...)` in the
same named or positional figure slot reuses its `HostPanelId`, chrome label,
producer group, tile, topology, ratios, focus, and tab state. It does not retitle
the panel, reflow its group, open the dashboard, or alter anonymous position.

Canvas title/subtitle remain completion-atomic with the kernel under
PRODUCT-0016. Palette failure leaves the prior title, kernel, and exact colors
stale together, while source-current chrome follows the successfully analyzed
figure as before.

Palette selection never enters Toron source, relation-origin identity,
execution grouping, transport, stamped-result identity, or table resolution.
Two views over one origin still share one execution and one result `Arc` even
when they select different palettes; their product-side resolution and
continuation remain destination-local.

### 12. Deferred work stays deferred

This record does not add:

- mark-level `.skin`, `.palette`, or color modifiers;
- an implicit Color channel for a two-field bar;
- layered color channels or cross-layer scale resolution;
- a categorical legend for band fill;
- plot chrome skinning;
- perceptual palette scoring or accessibility enforcement;
- explicit palette cycling beyond declared capacity;
- a recolor/reset-continuation command;
- general sequential, diverging, or status palette authoring;
- retirement of `ColorTheme` or migration of every plot kernel;
- application-theme or pane-override syntax;
- eager continuation-bank collection on producer-buffer release;
- durable continuation or layout persistence; or
- any libaryon theme, palette, modifier, or natural-length change.

The likely next color record is not inferred here. Evidence may show a need for
plot chrome projection, explicit encoded color in layered plots, broader ramp
migration, or an authored capacity-cycling policy. Each changes a different
stage and keeps its own proof burden.

## Acceptance gates

1. In a proven figure position, exactly one `palette(string_literal)` is
   accepted in any metadata order. Duplicate, wrong-arity, computed, nested,
   body-position, layer-position, and mark-method forms refuse atomically at
   their proven spans; outside those positions calls remain ordinary Rune.
2. The Rune normal form carries one authored categorical palette override and
   no resolved theme, palette, color, position, continuation, kernel, GUI type,
   or source-order metadata chain. Existing plot specifications are unmigrated.
3. Omitted palette resolves explicitly to libaryon `viridis`; an authored name
   right-biases it at the authored tier. The substrate remains default-free and
   receives the resulting explicit `CategoricalPaletteSelection`.
4. A bare six-category two-field Rune bar now carries six stable `viridis` band
   fills and no Color channel. An existing categorical scatter consumes the
   same default selection; line and absent-color scatter remain
   single-treatment.
5. `palette("tab20c")` over six bar categories resolves the complete mapping
   `A..F -> 0,4,8,11,15,19 -> c01,c05,c09,c12,c16,c20`, renders those exact
   colors, and creates no Color channel or band-fill legend.
6. `tab20c_r` and `tab20c_s1` retain the complete category-to-position mapping
   while consuming libaryon's exact reversed and shifted colors. `_s1` proves
   authority delegation rather than a local special-case reversal.
7. A categorical scatter with an authored Color field consumes the selected
   categorical palette through its existing `ColorJob`; it carries ordinary
   color-channel assignments and no band fills. Sequential, diverging, and
   status jobs do not consume this selection.
8. Known palettes with weak, pale, adjacent, or otherwise poor visual choices
   render their exact declared colors. No contrast, distance, saturation, hue,
   warning, substitution, or automatic-shift policy can prevent completion.
9. An explicit palette on a line-only, absent-color scatter, or accepted layered
   figure succeeds without manufacturing a categorical consumer or changing
   its rendering.
10. Unknown identity, known categorical absence (`nord`), palette-only capacity
    (`dark2` above eight), geometry-only capacity, and both-limit failure remain
    distinct, factual, and atomic. None falls back, cycles, truncates, mutates
    continuation, or installs partial content.
11. Three successive successful live resolutions of one view extend one
    retained identity without moving prior positions. Filtering reserves absent
    positions, a returning category recovers its exact selected-palette color,
    and a failed or superseded run commits no proposed assignment.
12. Changing `tab20c -> tab20c_r -> tab20c_s1 -> viridis` retains one
    twenty-position bank and every category's position while changing exact
    colors. Changing to `dark2` uses a distinct eight-position bank; changing
    back resumes the prior twenty-position assignments rather than rebuilding
    them.
13. Two independent views and origins retain independent continuation regardless
    of completion order, while sinks over one origin retain the existing shared
    result `Arc`. Ordinary rerun, failure, filtering, and temporary source
    deletion do not clear banks. An actually released producer's monotonically
    allocated replacement `BufferId` cannot reach its old banks; unreachable
    entries are retained only until process teardown in this cut.
14. The implementation commit contains both `palette(...)` recognition and the
    complete production Rune switch from fresh legacy resolution to explicit
    palette-aware stateful resolution. Neither half exists in its parent, flag,
    dead path, or follow-up cleanup.
15. Adding, removing, or changing a palette preserves panel identity, chrome,
    group topology, ratios, focus, tab state, source order, execution count, and
    dashboard absence. Failure retains the prior canvas title, kernel, and
    colors stale together while siblings may complete.
16. The real `../uat/test9.rn` window displays the exact authored `tab20c`
    six-color bar mapping in an ordinary host panel, and `tab20c_s1` visibly
    rotates the colors without moving categories. The assay judges fidelity,
    not aesthetics.
17. PRODUCT-0007 lineage, PRODUCT-0010 execution identity, PRODUCT-0013 layered
    color refusal, PRODUCT-0016 figure headings, PRODUCT-0028 palette authority,
    and PRODUCT-0029 allocation/rendering gates remain green except where this
    record explicitly supersedes Rune's legacy default caller.
18. Libaryon, Toron, SQL/Markdown tables, table skins, `UiPalette`, plot chrome,
    non-categorical jobs, dashboard, transport, and non-Rune legacy plot callers
    are unchanged.

## Stop condition

> This record may add the literal `palette(name)` figure member, one authored
> categorical-palette override, the explicit `viridis` Rune library default,
> settings resolution for that identity, a session-local per-view/per-length
> `ColorIdentity` bank, the palette-aware Rune request path, proposed-identity
> commit on successful current-revision completion, factual diagnostics, and
> the `test9.rn` dogfood update. If implementation requires changing libaryon's
> theme grammar, resolved palette order, optional capability, or natural length;
> changing PRODUCT-0029's selection or allocation algorithms; migrating either
> plot specification; originating a Color channel; adding a guide; validating
> aesthetics; cycling at capacity; changing plot chrome or a non-categorical
> job; migrating the legacy `ColorTheme` catalog; changing Toron, execution,
> transport, stamped-result sharing, panels, placement, dashboard, tables, or
> workspace configuration; or making continuation durable, work stops.

The anticipated trigger is the completion boundary. If the head cannot carry a
proposed `ColorIdentity` beside a resolved plot and commit it only when the
matching revision becomes content—without putting continuation in a kernel,
message transport to Toron, or panel identity—the work stops. That is a broader
live-artifact lifecycle record, not a shortcut this surface may hide.

## Consequences

- One compact libaryon theme expression finally colors categorical Rune plots
  through the same authority already used by syntax and table skins.
- Bare categorical Rune plots move from a private twelve-color compatibility
  palette to explicit `viridis`; two-field bars gain stable multicolor band
  styling without gaining an encoding or legend.
- Authors retain the right to poor color choices. The system refuses impossible
  allocation, not taste, and `_sN` remains the compact lever for choosing a
  different cut through the ordered palette.
- Palette position is stable across same-length theme changes, including reverse
  and rotation. Different natural lengths retain independent histories instead
  of silently reinterpreting positions.
- Live identity becomes real head-owned Rune state rather than a fresh default
  reconstructed for every completion. Exact colors remain inside completed
  artifacts, so rendering and pinning stay self-contained.
- Buffer release makes old continuation unreachable but is not projected to
  Polariton, so unreachable banks have process-lifetime retention. This bounded
  session cost avoids adding a lifecycle protocol solely for eager collection.
- This is the second independent need for the same lifecycle fact. PRODUCT-0015
  could react to producer release only because its group state lives inside
  libaryon beside `release_buffer`; PRODUCT-0030 cannot project that fact to its
  Polariton-owned state. A third independent consumer triggers a dedicated
  `BufferReleased` host-event record rather than a third local workaround. The
  accumulated evidence, not this cut's kilobytes of unreachable maps, would then
  justify expanding the protocol.
- A palette setting may be temporarily unused while a figure body changes. This
  preserves cut-and-paste authoring and does not invent data semantics to make a
  setting visible.
- Real-window evidence proves that authored colors arrive; it does not appoint
  Polariton as a taste authority.
- Plot chrome, layered color, guides, broader ramp migration, explicit cycling,
  and persistence retain separate records.

## Rejected

- **Retiring the duplicated plot-ramp catalog and the unselected compatibility
  resolver (ledger CI-03, closed 2026-08-19):** a broad renderer-theme migration
  whose cost is the parity census it demands, not the deletion it performs. The
  unselected resolver is a dual entrypoint that is inert for authored work, and
  non-Rune legacy callers keep current behavior. The duplication is a
  maintenance fact, not something users can observe, and no record has produced a
  reason to pay for the census. The compatibility path is preserved. Ratified in
  `plans/product/review/deferred_triage.md` § Ratification — 2026-08-19.

- **`bar(...).skin(name)` or another mark method:** conflates artifact skin with
  data palette, puts a figure-wide setting in a mark register, and makes future
  multi-mark palette sharing receiver-dependent.
- **`figure([skin(name), ...])`:** reuses table artifact vocabulary for data
  colors and collapses PRODUCT-0007's chrome/encoding boundary.
- **A third argument on every mark:** repeats the accumulating-verb ceiling that
  PRODUCT-0007 §5 rejects and cannot express one shared figure palette.
- **Infer a Color channel for two-field bars:** copies a field into a channel the
  author did not name, explicitly forbidden by PRODUCT-0007 §2. Band fill is the
  accepted non-channel treatment.
- **Make palette validity depend on a current consumer:** turns a stable
  figure-wide setting into data- and body-dependent syntax and makes harmless
  cut-and-paste edits refuse.
- **Refuse or warn on weak perceptual separation:** mistakes product taste for a
  structural invariant and defeats the compact palette grammar's purpose. Valid but ugly
  must render.
- **Auto-select a perceptually better subset or rotation:** changes authored
  colors, tends to erase `_sN` observability, and makes the same expression
  depend on product-version color policy.
- **Silently cycle beyond palette capacity:** assigns one color identity to
  several meanings without authored permission and violates PRODUCT-0007's
  no-reassignment rule.
- **Use `Theme::palette()` instead of `categorical_palette()`:** consumes the
  syntax-role permutation and repeats the defect PRODUCT-0028 exists to prevent.
- **Parse modifiers or reconstruct colors in Polariton:** creates a second theme
  language whose behavior can drift from libaryon.
- **Keep fresh `ColorIdentity::default()` on each Rune run:** makes filtering and
  live growth recolor survivors, violating PRODUCT-0007 §8 while claiming to
  expose PRODUCT-0029.
- **Key continuation by full theme expression:** resets identity on `_r` and
  `_sN`, making palette transformations destroy the stable positions they
  are meant to reproject.
- **Force one identity across different natural lengths:** reinterprets validated
  positions in an incompatible allocation space and may index past a shorter
  palette.
- **Discard the old bank on a length change:** makes toggling an authored setting
  destroy live identity even though session memory can retain both finite banks.
- **Commit identity after domain resolution but before content completion:** lets
  a failed or superseded run reserve colors no visible artifact ever used.
- **Put `ColorIdentity` in Rune, Toron, the kernel, or the resolved artifact:**
  crosses established ownership boundaries; Rune carries intent, Toron carries
  relation execution, kernels consume resolved colors, and the resolved artifact
  is already self-contained.
- **Preserve the legacy Rune default after authoring lands:** leaves two palette
  systems in one surface and violates PRODUCT-0029's required production switch.
- **Migrate all `ColorTheme` consumers here:** turns a categorical surface record
  into a broad renderer-theme migration and loses the substrate/surface proof
  split.
