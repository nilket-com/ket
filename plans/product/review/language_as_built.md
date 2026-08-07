# The plotting language as built

Derived 2026-08-06 from PRODUCT-0007, PRODUCT-0008, and PRODUCT-0010 through
PRODUCT-0040. This document states backward what exists. It contains no decision
that is not already in a record; every claim carries its citation, and where two
records disagree the later one governs and the supersession is noted. Companion
documents: `consistency_audit.md`, `deferred_ledger.md`, `live_bridges.md`.

## 1. What a plot is

A plot is three artifacts with three lifetimes (0007 §1): `AuthoredPlotSpec`
(portable intent), `ResolvedPlotSpec` (exact field identity, chosen scales,
domain decisions), `ResolvedLayout` (viewport geometry). Resolution has two
phases — channel resolution against the returned schema, domain resolution
against the returned values — and both run after execution; kernels consume a
resolved specification and never infer (0007 §1–2).

Channel resolution is total and never substitutes (0007 §2). Every channel is
authored `Named | Auto | Absent`; omission is unrepresentable, so inference
authority is always written down. A channel that cannot resolve is a typed,
per-sink refusal at its Rune span.

Semantic type (nominal/ordinal/quantitative/temporal) and scale (point, band,
linear, log, time) are distinct decisions, both named explicitly in the resolved
spec (0007 §3). Durability pins the resolved spec, not the layout — a pin
protects against resolver drift, not data drift (0007 §4). Dual Cartesian y axes
are unrepresentable (0007 §9).

Two specifications currently coexist: `AuthoredPlotSpec` (global encoding) and
`LayeredPlotSpec` (per-layer encoding). 0013 §1 added the second beside the
first and §10 prohibited migration; unification is an unearned later record
(0013 consequences).

## 2. Execution and fan-out

`.views([...])` is the type-state, fan-out, and process boundary: everything at
or below the receiver executes on Toron, everything the call produces stays in
Polariton (0008 §1). Single-view methods are strict sugar — defined as their
expansion, prohibited from carrying private semantics (0008 §2). The
free-function `plot.*` spelling is retained (0008 §6).

The normal form is `ViewSet { relation_origin, ordered sinks }`; every surface
operation appends, nesting never enters the representation, and ordering is
output-root source position, then list order, then chain order (0008 §5).
Bindings emit nothing; a `ViewSet`'s sinks take the position of the expression
that emits them.

Execution is keyed by canonical relation origin: identical canonical origins
within one buffer revision execute once and share one stamped result by `Arc`
identity — a guarantee, not an optimization (0010 §1). Identity is textual after
canonicalization, deliberately not semantic equivalence. Distinct origins are
independent: no cross-origin transaction, snapshot, or ordering. Execution
grouping never changes presentation order (0010 §2).

## 3. View identity and producer state

`ViewIdentity = Named(producer, name) | Anonymous(producer, position)` (0010
§4). Names are unique per buffer; duplicates refuse naming both spans. A named
view keeps its identity across sink-form changes — the name is the address,
mark form is non-identifying. Anonymity is the statement that placement need
not persist (0010 §3). `HostPanelId` is the head's realization and never enters
the normal form (0010 §4).

Producer state maps onto the existing host-panel protocol (0010 §6): a
successful analysis makes the new named-view set authoritative; parse or
envelope failure is not deletion; a failed origin marks only its views stale; a
returning name reattaches to its preserved destination.

The dashboard is demoted to a consumer of views; completing a view neither
opens nor mutates it (0010 §7). Its fate is undecided.

## 4. Placement and groups

Placement requests are advisory; the engine decides (0011 §2). `OpenHostPanel`
carries `Split | TabWith`; Polariton opens every view with `Split` and infers
no grouping — splitting fails loudly, tabbing fails silently, and a default
that guesses wrong should leave evidence (0011 §4, amended after the shipped
grouping policy proved wrong in use). `TabWith` has no automatic caller until a
language grouping signal exists.

A producer's views are established atomically as one engine-owned host-panel
group (0015 §1–2): one group per producer, members in presentation order,
tables `Compact`, plots `Expansive`, row-major tile. Groups have two layout
states — `Automatic` (membership changes may retile) and `Manual` (the author
touched geometry; surviving placements are preserved) (0015 §4). Omitting a
view orphans its tile without reflow; reset (`Retile outputs`) is the sole
reconciliation to the current program's automatic layout (0015 §5–7). All group
state is session-scoped; nothing persists (0015 §9).

## 5. The tree: four levels, one role each

```text
view( name , figure([ metadata , layers([ marks ]) ]) )
 identity      figure         plot body       marks
```

Each level appears at most once on a path and has one role: `view` carries
identity, `figure` carries plot-wide metadata, `layers` carries paint order,
marks carry encodings (0016 §3). The tree is bounded at four levels by
construction. Position determines role: a mark constructor is a view in
`.views([...])` position and a layer in `layers([...])` position (0014 §2);
`figure([mark])`, `layers([mark])`, and bare `mark` in the same slot are the
same view (0014 §6, 0016 §2). Postfix modifier chains flatten before the normal
form and add no semantic level (0018 §2).

Recognition throughout is contextual and atomic (0008 §3): constructors mean
sinks only in proven positions; outside them the identical call is ordinary
Rune; inside a proven list, an unknown or malformed element refuses the whole
program before dispatch, and no presentation syntax reaches Toron.

## 6. The three attachment points

The language has exactly three places a property attaches (0030 §1):

| position | attaches to | members |
|---|---|---|
| member of `figure([...])` | the whole graphical artifact | `title`, `subtitle` (0016); `palette` (0030); `skin` + `.duotone()` (0032/0033); `x_bounds`, `y_bounds` (0036); `x_tick_step`, `y_tick_step` (0038); `x_minor_ticks`, `y_minor_ticks` (0039) |
| postfix on a mark | that mark's Layer-stage styling | `.point_size` (scatter, 0018); `.stroke_width` (line, 0020); `.bar_width` (bar, 0022) |
| postfix on `table()` | the table artifact's interior treatment | `.skin` (0025); `.duotone()` (0027) |

Every register is closed (0018 §1, 0025 §4): unknown members refuse, duplicates
refuse rather than last-wins, modifier and metadata order is semantically
irrelevant, and receiver-incompatible members refuse the complete sink
atomically. A property of the whole picture is a figure member; a property of
one mark's drawing is a postfix on that mark (0030 §1).

Figure metadata order carries no meaning; only `layers` carries an order at all
(0016 §2–3).

## 7. Layers

A layered plot is `NonEmpty<LayerSpec>`, each layer owning mark, encoding, and
typed settings (0013 §1, 0017 §3). Authored order is paint order — first
authored is bottom — with no normalization anywhere between authoring and
painting (0013 §4). Layers resolve independently through 0007 §2's total
resolution; the compatibility rule is: identical x/y field identities, same
scale family, and every layer's color channel `Absent` (0013 §2). Differing
fields are representable and refused — relaxing later is a scale-composition
decision over an existing shape, not a change of shape.

The rendering boundary is a mark renderer (draws marks into a shared transform;
owns no layout, legend, or interaction), not a kernel composition (0013 §3).
`LayerMark` is exactly `Line | Scatter`; bar is styleable but deliberately not
layer-composable (0021 §5, 0022 §3). Resolution is atomic: any failed layer
fails the plot as one artifact (0013 §5). Cross-origin layering is
unrepresentable by construction (0014 §8).

## 8. Settings and the cascade

Two cascade algebras and only two: `Override<T> = Inherit | Set(T)` and
`Presence<T> = Inherit | Show(T) | Hide`, both right-biased with `Inherit` as
identity (0007 §6). Precedence is five tiers in one order: library defaults →
application theme → authored spec → layer → pane override (0007 §7). Tiers
select; they never multiply (0017 §2).

Layer-stage settings are typed and flat — `LayerSettings { point_size,
stroke_width, bar_width }` — never a string-keyed bag (0017 §3, 0019 §2, 0021
§2). The three value types stay separate: `PointSize` and `StrokeWidth` are
relative linear factors in `0.25..=8.0` (1.0 preserves today's pixels);
`BarWidth` is normalized slot occupancy in `(0.0, 1.0]`, default `0.9` (0017
§1, 0019 §1, 0021 §1). Three specimens validated the register architecture, not
a shared abstraction (0021 §2).

Settings are render-time presentation, never prepared data: prepared data is
identical across styled values, and paint and hit geometry consume one resolved
calculation (0017 §5–6, 0019 §3–4, 0021 §3–4).

## 9. Color: one authority, several projections

Libaryon is the sole theme authority: `Theme::try_new` owns names, aliases, and
the `r`/`c`/`C`/`sN` modifier grammar; no consumer reparses it (0024 §3, 0025
§1, 0029 §2). A resolved theme exposes two sibling sequences of one identity:
the twenty-position syntax-oriented palette (0024 §1–2) and an optional
natural-order categorical palette — present for every continuous family and six
declared curated families, absent otherwise; absence is a fact, not an error
(0028 §1–3). Modifiers apply independently to each sequence at its own length
(0028 §5). Generic sRGB luminance/contrast math lives in libaryon; compositing
and thresholds stay with consumers (0024 §4).

The consumer projections, each owned above libaryon:

- **Table skins** (0023, 0025–0027): a closed twelve-role `TableSkin`, required
  by construction at every `Table`, atomic with its frame as `TableArtifact`.
  Two explicit projections: `PaletteForward` (ordered swatches on surfaces,
  measured neutral maximin ink) is the default for `table().skin(name)`;
  `Duotone` (most-contrasting pair as paper/ink) via `.duotone()` (0026 §2–4,
  0027 §1). The substrate is default-free; only the Rune surface defines
  absence (0026 §1).
- **Plot chrome** (0031–0033): a closed eleven-role `PlotChrome`; the default
  is an exact compatibility extraction of today's pixels. One duotone-shaped
  projection with two modes: ordinary `skin(name)` is order-sensitive
  (slot-19 anchor plus its most contrasting mate — 0033, superseding 0032
  §3–4's set-based mandatory projection), and `.duotone()` selects the
  explicit set-based strongest pair. Chrome ends at the artifact boundary and
  never colors marks (0031 §5–6).
- **Categorical data color** (0029–0030): `figure([palette(name)])`, absence =
  explicit `viridis` (0030 §3). `ColorIdentity` retains validated
  `PalettePosition`s, never RGB; resolved artifacts retain exact colors (0029
  §1). Fresh domains get even spread; live growth gets deterministic maximin
  with lower-position ties; assignments never move (0029 §3–4). Geometry and
  palette impose independent capacity limits, both reported when both exceeded
  (0029 §5). Two-field bars get band fill — mark styling over the band domain,
  no manufactured color channel, no legend (0029 §6). Continuation banks are
  head-owned, keyed by (producer, view, natural palette length), committed
  only on successful completion (0030 §6–7).

The disjoint-siblings invariant: `skin` colors chrome only, `palette` colors
categorical marks only; changing one leaves the other byte-identical (0032 §7).
The ownership rope runs through everything: `UiPalette` is editor chrome,
`TableSkin`/`PlotChrome` are artifact chrome, palettes are data encoding, and
none crosses into another (0007 §10, 0023 §6, 0031 §5).

## 10. Axes and ticks

`x_bounds`/`y_bounds` are figure elements taking exactly two literals
(optionally negated); partial bounds are not exposed because the cascade
replaces the group and a merge algebra does not exist (0036 §1–3). The refusal
line is transform consistency, not magnitude: exclusion renders, collapse
(`min == max`) and inversion (`min > max`) refuse because forward and inverse
transforms disagree — measured, not assumed (0036 §5–6). A minimum-span ε is
prohibited.

Tick placement is a closed choice, `Count(NonZeroU32) | Step(f64)`, produced by
one generator that all sixteen former call sites consume; no site may compute a
position (0037 §1, §4). `Count(8)` is the compatibility default. `Step` is a
zero-anchored lattice — ticks do not move when the domain moves, the same
stability argument as palette positions (0037 §3). Minor ticks subdivide the
generator's major intervals; the authored knob is a count (`n` ticks strictly
between majors), because the minor invariant is the subdivision where the major
invariant is the gap (0037 §5, 0039 §2–3). Zero disables minor marks and
gridlines together (0039 §4). Capacity is declared per axis over the combined
major-plus-minor lattice, with checked arithmetic, and exceeding it is an
attributed refusal (0037 §6, 0039 §5). `Step` and minor counts refuse on
categorical, band, and temporal axes — attributed, never silent (0037 §7, 0039
§6).

## 11. The refusal doctrine

The arc converged on one rule, stated at 0035 §1 and applied retroactively:

> If an author names a theme and its projection is mechanically defined,
> Polariton renders that projection exactly.

Refusal means there is no defined answer (0035 §3, 0036 §4): unknown identity,
missing capability, exhausted capacity, no mechanical definition, inconsistent
transform. Insufficient contrast, poor taste, weak separation, and extreme but
finite values are the author's to see. 0034 removed contrast admission from
plot chrome; 0035 removed it from table skins; 0036 §6 prohibited value
thresholds on bounds. Contrast remains measured and inert — a measurement that
changes output is not a measurement (0035 §4). No unsolicited notes either
(0035 §5).

Two bounded exceptions, both stated:

- **Polariton's own defaults keep their contrast obligations** — nobody chose
  them, so there is no author to hand the rope to (0035 §2). `TableSkin::DEFAULT`
  passes its seven checks; the compatibility plot chrome is measured honestly
  (1.66:1 gridline) without being certified (0031 §4).
- **Capacity bounds work, not aesthetics**: a value threshold gatekeeps what an
  author may look at; a capacity limit bounds work that would otherwise not
  terminate (0037 §6, 0039 §5, 0007 §8's categorical exhaustion).

And one absolute: **a setting that cannot apply is never silently dropped**
(0036 §7, 0037 §7, 0038 §3, 0039 §6). Silent ignoring is the one failure mode
the rope does not cover — the author sees an artifact that does not reflect
what they wrote and has no way to learn why. Likewise truncating or thinning
renders something other than what was requested without saying so (0037
rejected).

Refusal happens in two phases, both attributed to the element's span (0038 §3):
lowering-time in the envelope (arity, literal form, duplicates, value validity)
and resolution-time per destination (applicability against the resolved axis,
capacity against the resolved domain). Failure is destination-local: siblings
over the same stamped result complete, and a failed destination retains its
previous complete artifact stale — never mixing old content with new
presentation (0016 §9, 0023 §5, 0027 §7, 0030 §8, 0032 §9).

## 12. Labels

Three label concepts, separately owned (0016 §7–8): the address (view name /
`HostPanelId`), the generated structural description (`display_label`, e.g.
`"Line + Scatter"`), and authored prose (`title`/`subtitle`). Chrome resolves
name, else source-authored title, else generated label — and consults only what
the source authored, so a theme cannot rename a panel. The canvas resolves the
full cascade. Chrome is source-current (refreshes on successful analysis,
before dispatch); the canvas is completion-atomic with its kernel (0016 §7, §9).

## 13. Static literal bindings

A literal argument in a recognized presentation position may be a name that
resolves — through preceding top-level bindings, transitively with a cycle
bound — to a literal of the required kind (0040 §1–3). Resolution is static
lookup, not evaluation; `let t = compute()` and `a + b` remain refusals. The
distinction 0016 §4 lumped together is now split: a named literal exists in the
source at parse time. Refusal attributes to the argument's span and names the
binding (0040 §4). Bindings are retained, never peeled (0040 §5). Coverage is
figure elements; mark arguments are a coverage extension needing no record
(0040 §6). A resolved value is governed by exactly the rule that would govern
it inline (0040 §7).

## 14. The method, as practiced

These are patterns the records themselves state, listed because the audit needs
them named:

- **Substrate/surface pairs.** A capability lands test-only with its semantics
  proved, then a small surface record adds the spelling and lowers onto it:
  0013/0014, 0017/0018, 0019/0020, 0021/0022, 0023+0024/0025, 0026/0027,
  0028+0029/0030, 0031/0032, 0037/0038. The surface is judged on lowering; the
  substrate on behavior (0037 §8).
- **Indivisible commits.** Where a default flip and its escape hatch (0027 §6),
  or a surface and its production caller switch (0030 §9), would each be broken
  alone, the record requires one commit and evidence inspects the diff — a
  historical acceptance condition a final-tree test cannot prove.
- **Halt rules.** Every record carries a stop condition binding its
  implementation; several fired and produced records (0010 §9 → 0011; 0020 §3's
  styled-single domain leak → 0019 amendment) or same-day amendments (0011 §4's
  grouping reversal, 0015 §5's release distinction).
- **Unrepresentable invalid states.** The house rule: `NonEmpty<LayerSpec>`,
  two-variant `LayerMark`, required `TableSkin` at construction, validated
  `PointSize`/`StrokeWidth`/`BarWidth`/`PalettePosition`/`TickPlacement::Step`,
  omission-free authored channels, `TableSkinProjection` without `Default`
  (0013 §1, 0023 §4, 0026 §1, 0037 §1).
- **Ahead-of-language fixtures.** Dogfood buffers are written before their
  syntax exists and refuse atomically until a record closes the gap — six times
  through 0038 (0038 context).
- **Lower layers report facts; upper layers compose policy.** Stated as the
  governing authority principle at 0026 context and applied at every boundary:
  libaryon knows no artifact, the substrate knows no default, only the Rune
  surface defines absence.
