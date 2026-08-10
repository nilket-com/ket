# Live-bridge inventory — plotting language ADRs 0007–0040

Derived inventory of behaviors the records themselves mark as provisional,
bridge, known-defective, documented-but-not-endorsed, or interim — and that no
later record in the set has removed or replaced. Sources: ADR 0007, 0008,
0010–0040; ADR 0030's shipping status confirmed via
`impl/0030_the_categorical_palette_figure_surface_evidence.md` (its status line
still reads "proposed" but the indivisible commit landed 2026-08-05).

No proposals, no prioritization.

---

## Standing

### 1. Layered-plot legend and hover bridge

- **What:** For layered plots, `hover` searches layers back-to-front and returns
  the first geometric hit; the legend delegates to the last authored (topmost
  painted) layer without merging, deduplicating, or rewriting. Authorized "only
  to satisfy the existing `PlotKernel` contract"; "carries no compatibility
  guarantee, is not an acceptance gate, and is not a decision this record makes
  about Guide or interaction." Delegation "is visibly a placeholder, which is
  the property wanted here." Bridge tests "pin current behavior against
  accidental drift; they do not make it accepted."
- **Citation:** ADR 0013 §3 ("The bridge, and why it is non-normative"), §9,
  and consequences ("marked in the source as provisional so that record can
  find it").
- **Why accepted:** `PlotKernel` requires total `hover` and `draw_legend`, so a
  composite implementing the trait cannot decline to answer; deferring until
  Guide/interaction were decided "would block the whole arc on stages the
  motivating case does not need."
- **Replacement:** "A later Guide or interaction record replaces them without
  amending this one." ADR 0013 consequences: "the first thing a Guide or
  interaction record should replace."
- **Still standing:** Reaffirmed unchanged by ADR 0017 §7 ("PRODUCT-0013's
  guide bridge remains unchanged"), ADR 0019 §4 ("remain PRODUCT-0013's
  provisional interaction bridge") and §5 (fixed 3px legend sample: "PRODUCT-0013
  deliberately leaves guide composition provisional"), ADR 0031 §7 and gate 9
  ("remains the same provisional bridge"), ADR 0032 §8 and gate 13
  ("remains provisional and unchanged"). No Guide or interaction record exists
  in the set.

### 2. `Presence::Hide` heading round-trip defect

- **What:** Hiding a plot heading is broken end to end: `theme.rs:283` maps
  `Presence::Hide` to `None`, and `common.rs:709` maps `None` to the generated
  display label — so a `hide_title()` "would render `\"Line + Scatter\"` — the
  precise opposite of the request." ADR 0016 ships headings around it: "At that
  boundary the existing defect remains — documented, cited, and not accepted as
  correct." Gate 11 asserts the flattening "as the known, unrepaired defect,
  not as correct behavior."
- **Citation:** ADR 0016 §6 ("Hiding is unavailable, and this record does not
  pretend otherwise"), gate 11, §11 stop condition, and consequences
  ("`Presence::Hide` acquires a documented, cited defect and a deferred repair").
- **Why accepted:** Repairing the round trip "is a renderer and resolution
  change with its own evidence, and it is not a convenient extension of a
  spelling record"; `Hide` is the named expected trigger of ADR 0016 §11's halt.
- **Replacement:** A later record must preserve three resolved states
  (`Fallback | Show(text) | Hide`); any implementation flattening headings to
  `Option<String>` "is foreclosing the fix rather than deferring it."
- **Still standing:** No record in 0017–0040 repairs heading resolution; the
  chrome records (0031–0034) touch chrome colors only.

### 3. Curated `c`/`C` clipping silent no-op (`nord_c == nord`)

- **What:** For curated theme families, the `c` and `C` clip modifiers have no
  effect: `Theme::try_new` returns from the curated branch before clipping is
  consulted, so `nord_c` and `nord_C` equal `nord`. The behavior is "observed
  and preserved, not accepted as correct"; "whether inapplicable clipping
  should instead refuse is undecided."
- **Citation:** ADR 0024 §3, gate 5 ("guarded as known, unendorsed
  compatibility behavior"), stop condition (changing it triggers the halt), and
  consequences ("visible, tested, and explicitly not blessed as correct").
- **Why accepted:** Changing it "would alter shipped `:set langtheme` behavior
  and requires its own record; this observability cut neither fixes nor
  endorses it."
- **Replacement:** Its own record, if inapplicable clipping should refuse. None
  stated as scheduled.
- **Still standing:** Reaffirmed and extended unchanged by ADR 0028 §5
  ("curated `c` and `C` remain the preserved no-op for both projections") and
  gate 8; ADR 0028's stop condition also forbids changing it there.

### 4. Compatibility plot chrome default and its retired gridline contrast debt

- **What:** `PlotChrome::DEFAULT` is "the pre-extraction compatibility
  baseline, not an authored production skin." Its post-composite gridline
  (white at 0.2 alpha over an effectively black background) measures about
  1.66:1 and "therefore fails the 3:1 [WCAG non-text] criterion." The fact "is
  retained as an executable named result"; the default "is not placed in a
  collection described as 'all validated built-ins'" and no test may describe
  it as a validated authored skin.
- **Citation:** ADR 0031 §4 ("The parity default is measured, not falsely
  certified"), gate 5, and consequences ("an honestly named gridline contrast
  debt").
- **Why accepted:** "An extraction cannot both preserve that output exactly and
  claim the default passes 3:1" (ADR 0031 context); raising grid opacity during
  extraction was rejected as "a visual redesign hidden in a substrate refactor."
- **Replacement:** PRODUCT-0067 makes the deliberate visual change: every
  compatibility construction path uses white at alpha `0.35`, the quietest
  established projection-ladder value that passes the same `3:1` assay.
  PRODUCT-0068 places grids beneath marks, completing the canvas-relative rule
  without changing resolved data colours at intersections.
- **Disposition:** Retired. The old alpha-`0.2` result remains only as the
  negative control proving the assay can still distinguish the accepted and
  rejected states. PRODUCT-0031's extraction and PRODUCT-0032's omitted-skin
  premises are superseded only for grid opacity and grid/mark order; their
  vocabulary and all sibling role values remain standing.

### 5. Legacy twelve-color compatibility resolver and the `ColorTheme` catalog

- **What:** ADR 0029 kept the unselected categorical resolver as "a
  compatibility path, not an implicit PRODUCT-0028 selection," retaining "the
  current twelve colors and current sequential assignment exactly" — "this
  temporary dual entrypoint is the honest cost of keeping PRODUCT-0029 a
  substrate cut." ADR 0030 executed the required production switch for Rune
  plots, but the legacy entrypoint survives for other callers: "Non-Rune
  callers of the legacy request entrypoint retain their current compatibility
  behavior; retiring that entrypoint and Polariton's remaining `ColorTheme`
  catalog is a separate record with its own parity census."
- **Citation:** ADR 0029 §2 ("temporary dual entrypoint"), §8 ("Retiring the
  rest of Polariton's duplicated plot-ramp catalog requires its own record and
  parity evidence"); ADR 0030 §3 and §12 ("retirement of `ColorTheme` or
  migration of every plot kernel" listed as deferred).
- **Why accepted:** Keeping ADR 0029 a substrate cut required a legacy path
  until the authored selection existed; a full `ColorTheme` migration "is
  broader plot-theme migration, not palette-position assignment" (ADR 0029 stop
  condition).
- **Replacement:** A separate retirement record with parity census. Not yet
  written.
- **Still standing (narrowed):** The Rune-caller half of the bridge was removed
  by ADR 0030 §9's indivisible commit (see Since removed); the legacy
  entrypoint, `ColorTheme` catalog, and non-categorical ramp duplication remain.

### 6. Unreachable categorical continuation banks retained until process teardown

- **What:** Polariton cannot observe text-producer buffer release (libaryon
  observes it internally and projects no matching host event), so after a
  producer buffer is released its `ColorIdentity` continuation banks become
  unreachable and "this cut therefore retains unreachable bank entries until
  process teardown." Gate 13: "unreachable entries are retained only until
  process teardown in this cut."
- **Citation:** ADR 0030 §7, §12 ("eager continuation-bank collection on
  producer-buffer release" deferred), gate 13, and consequences ("This bounded
  session cost avoids adding a lifecycle protocol solely for eager
  collection"). Confirmed shipped as described in the 0030 evidence file.
- **Why accepted:** "Adding a lifecycle event merely to collect them would be a
  protocol change with no semantic effect and triggers §12's deferral."
- **Replacement:** ADR 0030 consequences: this is "the second independent need
  for the same lifecycle fact" (after PRODUCT-0015); "a third independent
  consumer triggers a dedicated `BufferReleased` host-event record rather than
  a third local workaround."
- **Still standing:** No later record (0031–0040) adds a `BufferReleased`
  event or collection path.

### 7. Two coexisting plot specifications (`AuthoredPlotSpec` + `LayeredPlotSpec`)

- **What:** `LayeredPlotSpec` was added "beside `AuthoredPlotSpec`, not a
  migration of it," and "two plot specifications coexist until some later
  record earns the right to unify them. That is a real cost — a second shape a
  reader must learn."
- **Citation:** ADR 0013 §1, §10 ("No migration of `AuthoredPlotSpec`... A
  record that later unifies the two is welcome to exist; it is not this one"),
  and consequences.
- **Why accepted:** Unifying would mean "migrating every `AuthoredPlotSpec`
  consumer to serve two marks," a larger cost than coexistence.
- **Replacement:** A future unification record, unscheduled.
- **Still standing:** Repeatedly reaffirmed unmigrated: ADR 0016 gate 11, ADR
  0017 §3 ("Existing `AuthoredPlotSpec` remains unchanged"), ADR 0022 §3, ADR
  0030 §2 and gate 2 ("Existing plot specifications are unmigrated"). No
  unification record exists.

### 8. Cartesian transform collapse/inversion defect (guarded, not repaired)

- **What:** `CoordinateTransformer::cartesian` computes the span via
  `.abs().max(f64::EPSILON)` while the untransformed origin supplies the
  offset, so equal bounds and inverted bounds produce a transform in which
  "forward and inverse disagree — the same pixel maps to two different data
  values depending on direction." Measured in ADR 0036's Stage 0. The surface
  refuses `min >= max` so the defect is unreachable from authored bounds, but
  the transform itself is unchanged: reversed and collapsed axes "are now
  explicitly *unbuilt* rather than accidentally broken."
- **Citation:** ADR 0036 context ("Equal bounds are not defined by the current
  transform"), §5, and consequences ("a later substrate record... inherits a
  transform whose defect is documented with measurements rather than
  discovered again").
- **Why accepted:** Reversed and collapsed axes "each need real substrate
  semantics — transform, ticks, hover, hit-testing, and bar geometry — and each
  is a later record rather than an accidental consequence of `abs()`."
- **Replacement:** A later substrate record implementing reversed and/or
  collapsed axis semantics. Not yet written.
- **Still standing:** ADR 0037–0040 build on the tick and binding surfaces and
  do not touch the transform.

---

## Since removed

### A. Ahead-of-language `test9.rn` plot-skin fixture

- **Was:** ADR 0027 §8 documented that `../uat/test9.rn` "intentionally
  contains the proposed `bar(...).skin(\"greens\")` spelling," which atomic
  recognition "continues refusing... before dispatch, leaving its last good
  panels stale," with a table-only assay standing in "until plot skin syntax
  has its own accepted record."
- **Removed by:** ADR 0030 §10 rewrote `test9.rn` onto the accepted figure
  spelling (`figure([... palette("tab20c"), bar(...).bar_width(0.98)])`), and
  ADR 0032 §1 accepted figure-level `skin(name)` as the plot-skin syntax,
  ending the interim.

### B. Substrate cuts shipped deliberately without a production caller

Each substrate record in the arc explicitly shipped with its capability
unreachable from Rune ("deliberately shipped with no production caller,"
"test-only") as the stated cost of the substrate/surface split, and each was
closed by its paired surface record:

| Interim state (citation) | Closed by |
|---|---|
| `LayeredPlotSpec` unreachable from any buffer — ADR 0013 status/context, noted in ADR 0014 context ("exercised only by its own tests. That was the agreed cost... This record pays it back") | ADR 0014 (`layers([...])`) |
| `PointSize` with no Rune spelling — ADR 0017 ("adds no Rune surface") | ADR 0018 (`.point_size`) |
| `StrokeWidth` with no Rune spelling — ADR 0019 | ADR 0020 (`.stroke_width`) |
| `BarWidth` with no Rune spelling — ADR 0021 | ADR 0022 (`.bar_width`) |
| `TableSkin` catalog with "no authored name" exposed — ADR 0023 §2 | ADR 0025 (`table().skin(name)`) |
| `PaletteForward` with "no production caller, and no source buffer can reach it until PRODUCT-0027" — ADR 0026 §2/§9 | ADR 0027 (default flip + `.duotone()`, one indivisible commit) |
| Categorical palettes/positions with no Rune surface, plus the fresh-`ColorIdentity` legacy Rune caller — ADR 0028 §6 ("no production consumer in this record"), ADR 0029 §2/§10 ("intentionally unreachable from Rune") | ADR 0030 §9 (surface + production caller switch, one indivisible commit) |
| `TickPlacement::Step` "test-only... no production caller until PRODUCT-0038" — ADR 0037 §8 | ADR 0038 (`x_tick_step`/`y_tick_step`) |

---

## Borderline cases excluded, and why

- **ADR 0007 §10's chrome "bridge":** the word "bridge" names an architectural
  mapping of chrome roles between palettes, not a stopgap; ADR 0031 refines the
  sketch into `PlotChrome` rather than replacing a marked-provisional behavior.
- **ADR 0010/0011/0015 durable identity, layout persistence, `PanelRestored`
  producer, session-scoped groups:** framed as deliberate deferrals and
  sequencing ("Session scope is deliberate"), not as provisional or defective
  shipped behavior.
- **ADR 0011 §4's original auto-grouping policy:** shipped, proved wrong, and
  reversed by same-day amendment — but it was never marked provisional when
  shipped, so it fails the inclusion rule for both sections.
- **ADR 0015 §3's wide-table-still-`Compact` rule:** a chosen default with an
  evidence-driven revision path ("in this first cut... requires evidence"), not
  marked as a defect or bridge.
- **ADR 0013's retained `ScatterLinePlotKernel`:** deliberately kept under §7's
  additivity; deletion "is a later cleanup rather than a decision here" — a
  deferred cleanup, not a marked-defective behavior.
- **ADR 0031 §5's mark-decoration "compatibility setting"** (box-plot, violin,
  candlestick, funnel colors): named "compatibility" and value-preserving, but
  the record establishes it as the normative chrome/mark boundary and states no
  replacement, so it is not self-marked as interim.
- **Contrast-as-admission rules** (ADR 0026 §5–6, ADR 0027 §5, ADR 0032 §5,
  ADR 0033 gate 6), superseded by ADR 0034/0035: endorsed decisions later
  corrected, never marked provisional — excluded from "Since removed" by the
  inclusion rule.
- **ADR 0034/0035 contrast facts with no warning UI / opt-in assay:** explicit
  deferral of a future opt-in surface, with current inertness endorsed as
  correct ("a measurement that changes output is not a measurement").
- **ADR 0037/0039 misleading internal names** (`y_ticks` counts intervals;
  `minor_intervals` counts ticks): documented internal naming kept off the
  surface, not shipped behavior.
- **ADR 0040 §6's mark-argument coverage limit:** explicitly "a coverage limit
  rather than a semantic boundary," extendable "at any time, needing no
  record" — deliberate scoping, not a bridge; PRODUCT-0058 later completed the
  coverage without changing this classification.
- **Ahead-of-language fixtures `test12.rn`/`test13.rn`** (ADR 0038, ADR 0040):
  each documented and closed within the very record that cites it, so never a
  standing interim state across records.
