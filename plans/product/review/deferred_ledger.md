# Deferred ledger — plotting-language ADRs 0007–0040 (+ rune-data-products)

A derived inventory of every forward promise or deferral in ADRs 0007, 0008, and
0010–0040 (`plans/product/adr/`), plus `plans/wip/rune-data-products.md`.
One entry per deferred item, merged across records. Entries state what the
records defer in their own terms; nothing here is a proposal or a priority.
Items a later record in the set already resolved are in **Since landed** at the
end. Size signals appear only where a record states one.

---

## Axes & scales

**AX-01 · Scale composition.** Layers naming different x/y fields, mixed scale
families, or a categorical-plus-continuous shared domain require a
scale-composition semantics no record has decided; layered compatibility is
"expressible and rejected, so relaxing the rule later is a new decision over an
existing representation." Bar's exclusion from `layers([...])` is inherited
from the same deferral. — 0013 §2, §5, §9, §10, Consequences; 0014 §4, §11,
Rejected, Consequences; 0021 §5. Precondition: relaxation belongs to
PRODUCT-0007's Scale stage.

**AX-02 · Reversed and collapsed axes.** Inverted bounds (`min > max`) and equal
bounds refuse because the current transform is not self-consistent (forward and
inverse disagree, measured); both "need real substrate semantics — transform,
ticks, hover, hit-testing, and bar geometry — and each is a later record rather
than an accidental consequence of `abs()`." Now "explicitly *unbuilt* rather
than accidentally broken." — 0036 §5, §7, Rejected, Consequences.

**AX-03 · Discrete-axis bounds.** Bounds on a categorical/band axis refuse; "band
slicing, category windows — remain unaddressed and are a different design
question, not a widening of this one." — 0036 §7, §8, Consequences.

**AX-04 · Partial bounds.** `PlotDomainOverrides` holds four independent halves but
the surface takes only pairs; exposing halves would need a merge algebra the
cascade does not have and is "not worth a spelling nobody has asked for."
(Phrased as not-exposed rather than a promise.) — 0036 §3, §8.

**AX-05 · Log axes.** Log placement is not a variant of the closed
`TickPlacement` model and is not authorized; log axes "remain unaddressed and
each extends this closed model rather than replacing it." (0007 §3 names log as
an admissible scale family in the grammar, but no record implements it.) —
0037 §1, Consequences; 0038 §6, Consequences.

**AX-06 · Temporal steps.** `Step` refuses on `AxisType::Date`/`Datetime`/`Time`
"because a step of `1` has no defined unit there, and choosing one is a
temporal-scale decision"; temporal steps remain unaddressed. — 0037 §7,
Rejected, Consequences; 0038 §6, Consequences; 0039 §6 (minor counts refuse on
temporal axes too).

**AX-07 · Nice-number / automatic tick placement.** Rounding "invents a step the
author did not request. It belongs to a later record about automatic placement,
where the author has asked for automatic behavior." — 0037 §1, Rejected,
Consequences; 0038 §6, Consequences.

**AX-08 · Minor grid style.** "Minor grid width, style, and colour remain unauthored"
— the substrate carries independent minor grid width and style, with no
spelling; each later addition "extends this element rather than replacing it."
— 0039 §8, Consequences.

**AX-09 · Minor visibility flags.** `show_*_minor_ticks` and `show_*_minor_grid` stay
internal; count-zero disables marks and gridlines together, and a surface for
the flags "can be added later without changing anything decided here." —
0039 §7, §8, Rejected.

**AX-10 · Mark-factor bound widening.** The `0.25..=8.0` domains for `PointSize` (and
by the same shape `StrokeWidth`) are the accepted first cut; "widening either
bound requires renderer evidence, not a surface-only change." — 0017 §1;
0018 not-authorized ("No change to the accepted bounds").

---

## Color & ink

**CI-01 · Layered color.** Every layer's color channel must be `Absent`; even
identical color fields across layers "would require a shared color identity, a
`color_job` compatibility rule, and a legend that says something coherent about
a scale spanning two marks. Each is a color-scale decision, and all three are
deferred together." Cross-layer palette sharing likewise deferred. — 0013 §2,
§5, gate 4; 0014 §11, Rejected, Consequences; 0029 §10; 0030 §5, §12.

**CI-02 · Sequential / diverging / status palette authoring.** Only categorical
selection is authorable; "general sequential, diverging, or status palette
authoring" is explicitly not added, and non-categorical jobs do not consume the
figure palette. — 0030 §5, §12; 0029 §8.

**CI-03 · `ColorTheme` catalog retirement / legacy resolver retirement.** "Retiring
the rest of Polariton's duplicated plot-ramp catalog requires its own record
and parity evidence"; the unselected compatibility resolver is a temporary dual
entrypoint whose retirement is "a later cleanup"; non-Rune legacy plot callers
keep current behavior, and "retiring that entrypoint … is a separate record
with its own parity census." 0030 calls the alternative "a broad renderer-theme
migration." — 0029 §2, §8, stop condition; 0030 §3, §12, Rejected, stop
condition.

**CI-04 · Explicit palette cycling beyond capacity.** Capacity refuses rather than
cycling; "explicit cyclic reuse is a different semantic feature and would
require an authored spelling plus collision and guide rules of its own." —
0030 §4, §12; (0007 §8 fixes the no-eviction/no-cycling rule it would extend).

**CI-05 · Recolor / reset-continuation affordance.** "An explicit recolor/reset
affordance" for live categorical identity remains deferred. — 0029 §10;
0030 §12.

**CI-06 · Curated `c`/`C` clipping no-op.** Curated themes ignore clipping modifiers;
this is "observed and preserved, not accepted as correct. … Whether
inapplicable clipping should instead refuse is undecided" and changing it
"requires its own record." — 0024 §3, Rejected, stop condition; 0028 §5
(preserved for both projections).

**CI-07 · Opt-in contrast assay / warning surface.** Contrast facts are measured and
inert; "No warning UI is added in this cut. A later surface may display the
retained facts non-blockingly" and "a later record may add such an assay. It is
opt-in by construction or it is this record's mistake repeated." The teaching
wording built by 0027 §5 is retained for it. (0027 §4's bidirectional
recommendation was separately deferred "until actual PaletteForward refusal is
a user problem," then mooted when 0035 removed contrast refusal.) — 0034
Decision; 0035 §4, §5, Consequences; 0027 §4.

**CI-08 · Table non-text contrast criterion.** Gridlines and scrollbar thumbs got no
contrast gate; "if a later record does, it uses the WCAG non-text criterion of
3:1 rather than applying the 4.5:1 text threshold to a hairline." (0031 applied
3:1 to plot chrome; the table roles remain ungated.) — 0023 §3, Rejected.

**CI-09 · Default plot gridline contrast debt.** The compatibility default's gridline
measures ≈1.66:1 and is retained as an executable named result; "raising the
default grid opacity would be a deliberate visual change and needs its own
record." — 0031 §4, Rejected, stop condition; 0032 §2 (omission preserves it);
0034 Context (named as the precedent).

**CI-10 · Broader visualization-theme identity / skin configuration.** "A broader
visualization-theme identity remains deferred until table and plot evidence
both exist" (both now exist, no record yet); user/workspace skin configuration
"does not enter this substrate cut" and is out of scope in every later skin
record. — 0023 §2, Consequences, stop condition; 0025 preamble, stop
condition; 0027 preamble.

**CI-11 · Value-driven table coloring.** "Conditional formatting, per-column styling,
gradients, categorical assignment, and value-driven text or background colors
are data-encoding work and remain unrepresented." — 0023 §7, stop condition.

**CI-12 · SQL / Markdown table skin authoring.** "SQL result and Markdown tables
remain default-skinned; this cut authors skins only from Rune table sinks";
SQL/Markdown skin selection stays in later stop conditions. — 0025 §7, stop
condition; 0027 §1, stop condition.

**CI-13 · Per-surface ink.** "Any later per-surface-ink proposal must justify its
added acceptance against ink-flip frequency rather than treating acceptance
alone as improvement." (A condition on a possible future proposal, not a
promise of one.) — 0027 Consequences.

---

## Layout & identity

**LI-01 · Layout persistence.** No record writes layout to disk or produces
`PanelRestored` ("no producer yet: this arrives with layout persistence for
host panels"). "Workspace layout persistence remains next in its own
editor-wide arc. It must decide restoration of ordinary document panes, tabs,
panel groups, group state, and detached placements together." 0015 §11 states
the size signal: "editor-wide workspace work, not a convenient extension of
this session feature." — 0010 §10, Consequences; 0011 §6, Rejected,
Consequences; 0015 §9, §11, §12, Rejected, Consequences; 0030 §7, §12.

**LI-02 · Durable identity.** The durable named-view key
`(workspace identity, durable document identity, view name)` has an undecided
persistence mechanism; whether libaryon exposes a suitable durable document
identity is "an open precondition, not an assumption." It is "unblocked by
neither this record nor any record that does not also decide layout
persistence, because a durable address has no observable consequence until a
layout survives to reattach it"; "durable named-view identity follows that
observable capability rather than preceding it"; "durable continuation remains
blocked on workspace layout and document identity persistence." — 0010 §5,
§10, Consequences; 0011 §6, Consequences; 0015 §5, §9, §12, Consequences;
0030 §7, §12. Dependency: layout persistence.

**LI-03 · A grouping signal.** Nothing in a Rune buffer distinguishes views that are
alternatives from comparisons, so `TabWith` "ships with two legitimate
consumers and no automatic one," and "a future language record may say that
some views are alternatives and request a tab grouping. That signal would
compose with this group model and finally give `TabWith` an automatic caller."
Repeated Compact/row-major misses "become evidence for a later generic layout
signal." — 0011 §4, Consequences; 0015 §8, Consequences.

**LI-04 · The dashboard's fate.** Demoted to a consumer of views; "whether it survives
as a curated pinboard or is retired in favor of ordinary host panels is a
separate record, made on its own evidence." Its export bounds and
resize-suspension behavior are "properties of plot content rather than of a
layout container, and this record neither migrates nor discards them" — to be
"migrated or retired later under a record with evidence behind it." Every
subsequent record keeps the dashboard untouched. — 0010 §7, Rejected,
Consequences; (kept out of scope by 0011 §6, 0014 §7/§11, 0015 §12 et al.).

**LI-05 · Panel commands and further placement requests.** The placement-request set
"is deliberately small. It grows when a use exists, not in advance of one";
"a future record may add placement requests, decide layout persistence, or
give panels closing and moving commands. None is authorized here." — 0011 §1,
§6, Consequences.

**LI-06 · Content-derived layout shape.** A wide table is still `Compact`; "changing
that rule requires evidence and a timing design that does not move panes as
independent origins complete." — 0015 §3, Rejected ("Sizing tables from column
count"), Consequences.

**LI-07 · Durable member hiding.** "Closing hides a member only until the next
successful run; durable hiding is not authorized here." (Phrased as
not-authorized; no record promises it.) — 0015 §6.

---

## Data & bindings

**DB-01 · Deferred relations and live-table planning.** "Materialized sources are v1;
deferred relations join later without changing this contract." The WIP plan's
steps 7–8 remain open: deferred `db.table(...)` with default `Auto` policy,
adapter planning, dry-run cost gates, cancellation, provenance display, and
density pushdown — named "launch gates for live-table plotting, not
niceties." — 0007 §12; rune-data-products (status note, "Live relations and
the 20 TiB table", Sequence steps 7–8).

**DB-02 · Workspace artifact store / durable saved plots.** "The workspace artifact
store remains undecided per PRODUCT-0002. Saved authored specs and durable
plots cannot ship before that record exists." — 0007 Consequences (and §4's
live/pinned durability depends on it).

**DB-04 · Computed presentation values.** Computed headings — and by extension any
non-literal presentation value — are deferred because "the envelope lowers
statically, before dispatch … admitting it is a runtime evaluation and
value-transport decision, not a spelling." 0040 reaffirms: the deferral
"stands for anything that is not already a literal in the source"; no constant
folding, concatenation, arithmetic, or calls. — 0016 §4, §11, §12; 0036 §2,
§8; 0040 §9, Rejected.

**DB-05 · Non-top-level bindings.** Block-scoped bindings introduce "a resolution
question this record does not need to answer." — 0040 §9, Rejected.

**DB-06 · Shared-encoding sugar.** Withdrawn in corpus v0.7; "shared encoding may
return later as presentation sugar over the sink list." Unauthorized and left
to a future record in both surface records. — 0008 Rejected, §8, Consequences;
0010 §10, Consequences.

**DB-07 · Bindable sink specifications.** "A future record may … make sink
specifications bindable. None of them is authorized here, and each inherits
this normal form." — 0008 §8, Consequences; 0010 Consequences.

**DB-08 · Relation handles.** "A future record may add relation handles …" — 0010 §10,
Consequences; (rune-data-products' explicit-namespace pin is the related
posture).

**DB-09 · Free-function spelling retirement.** Retiring `plot.bar` / `plot.scatter` /
`table(r)` "requires its own record, made when there is use to migrate rather
than in advance of any." — 0008 §6, Rejected, Consequences; 0010 Consequences.

**DB-10 · Line series argument.** An optional series argument for `line` was declined:
"Series identity deserves evidence and a separately specified spelling." —
0012 Rejected.

**DB-11 · Sink-method sugar.** `.points()` and `r.layers([...])` remain unaccepted;
the arc's third-stage list includes "any `.points()` sugar," and PRODUCT-0014
keeps spellings "earned by use rather than added in advance of it." —
0013 Consequences; 0014 §11, Rejected, Consequences.

**DB-12 · New `LayerMark` marks.** "A future record may add marks to `LayerMark`, and
each addition becomes spellable here with no surface change beyond the
admissible-name set." — 0014 Consequences; (0021 §5 keeps Bar out pending
scale composition).

**DB-13 · Plot-specification unification.** `AuthoredPlotSpec` and `LayeredPlotSpec`
coexist; "a record that later unifies the two is welcome to exist; it is not
this one." — 0013 §10, Consequences; 0016 §5, gate 11 (unmigrated); 0030 §2.

**DB-14 · `ScatterLinePlotKernel` retirement.** "Whether it is eventually deleted
depends on evidence that nothing reaches it, which is a later cleanup rather
than a decision here." — 0013 Consequences.

**DB-15 · Cross-origin layering.** One view subscribing to several frames, "with its
own cross-origin failure, revision, and domain-reconciliation semantics — is a
different design and a different record. It is named here only so that its
absence is a decision rather than an oversight." — 0014 §8, Consequences.

**DB-16 · Figure-wide mark-setting defaults.** "A future figure-wide default may use
the authored tier; the existing cascade already makes a per-layer modifier
override it. That surface is deferred." — 0018 Consequences, not-authorized.

**DB-17 · Pane-override and application-theme producers.** Whether and how the
interactive pane override round-trips into the authored spec "is deliberately
left to the implementation record"; pane-override and application-theme
*syntax* remains unadded for palettes and skins ("future application-theme or
pane-override producers use the same property and do not change Rune syntax").
— 0007 §7; 0016 §12; 0017 §10; 0030 §3, §12.

**DB-18 · Heading hiding (`Presence::Hide` repair).** Hiding is unavailable: the
round trip flattens `Hide` to the generated label. "The later repair must
preserve three resolved states: `Fallback | Show(text) | Hide`," and the fix
is 0016's expected stop-condition trigger — "a renderer and resolution change
with its own evidence." No later record in the set repairs it. — 0016 §6,
§11, §12, Rejected, Consequences.

**DB-19 · Axis-label authoring.** "A future record may author axis labels, legends, or
facet structure. Each extends a stage inside `figure([...])`." Axis labels
remain unauthored (0036/0038/0039 added bounds and ticks, not labels). —
0016 §12, Consequences.

**DB-20 · Bare-string source sugar.** `viz.plot("joel")` "waits until one namespace
clearly wins in practice, and may never arrive." — rune-data-products (scope
pins).

**DB-21 · Headless data-products crate.** Extracting `ket-data-products` for source
specs, query policies, provenance, and adapter planning waits until Baryon
needs it — "when earned, not now." — rune-data-products ("Where this lives").

**DB-22 · Properties-panel shared-binding representation.** "A future properties panel
must represent 'this value is shared' rather than silently resolving it" — a
stated obligation on future editing surfaces. — 0040 Consequences.

---

## Interaction & guides

**IG-01 · Guides.** Guide policy is wholly deferred: 0013 §3's legend bridge
(delegate to the last authored layer) "carries no compatibility guarantee …
a later Guide or interaction record replaces them," and is "the first thing a
Guide or interaction record should replace." Point size does not resize the
legend swatch (a guide decision); the line legend's fixed 3px sample "belongs
to Guide … a later Guide record may relate samples to marks"; legend merging
and swatch policy remain deferred; chrome colors legend framing but "does not
decide legend presence, entries, order, labels, grouping, deduplication,
placement, or merging"; no legend or guide authoring exists in the figure
surface. — 0013 §3, §5, Consequences; 0014 §11, Consequences; 0016 §12,
Consequences; 0017 §7, §10; 0019 §5, Rejected; 0021 §4; 0029 §10; 0031 §7,
Rejected; 0032 §8, Rejected.

**IG-02 · Interaction.** Interaction policy is deferred with guides: hover ranking
beyond back-to-front paint order, hit preference between siblings, and "how
any content is worded for a reader" belong to a later record; 0013's hover
bridge is provisional; hover wording remains that bridge under stroke-width
changes. — 0013 §3, §5, Consequences; 0014 §11, Consequences; 0019 §4.

**IG-03 · Facets.** The grammar reserves the Facet stage (0007 §5) and `figure` was
named partly because it "survives facets, where one figure will contain
several plots," but facets remain unaccepted everywhere: listed among the
arc's third-stage records and in every unaccepted list. — 0007 §5; 0013 §5,
Consequences; 0014 §11, Consequences; 0016 §1, §12, Consequences.

**IG-04 · Data-driven size encoding.** "A data-driven size channel, size legend, or
guide-scale decision belongs to PRODUCT-0007's Encoding and Guide stages and
requires another record." — 0017 §7, §9, §10; 0018 not-authorized.

**IG-05 · Categorical legend for band fill.** Band fill "adds no color encoding, it
adds no legend item"; "a categorical legend for band fill" is explicitly not
added. — 0029 §6, §10; 0030 §12.

---

## Lifecycle

**LC-01 · A `BufferReleased` event.** Polariton receives no text-buffer release event,
so palette-continuation banks for released producers are retained until
process teardown. 0030 counts two independent needs for the lifecycle fact
(0015's group state saw release only because it lives inside libaryon) and
rules: "a third independent consumer triggers a dedicated `BufferReleased`
host-event record rather than a third local workaround." Eager
continuation-bank collection is listed deferred. — 0030 §7, §12, Consequences;
0015 §5 (the libaryon-side half).

**LC-02 · Producer-release reattachment.** After a producer buffer is released,
reopening the file creates a new session `BufferId` and a new group;
"reattaching it to the producerless group would require the durable identity
and layout persistence that §9 defers." Same for continuation banks. —
0015 §5; 0030 §7. Dependency: durable identity + layout persistence.

---

## Since landed

Items deferred by one record and resolved by a later record.

**SL-01 · Authored numeric axis bounds.** Landed in 0036 (the `x_bounds`/`y_bounds`
figure elements over the pre-existing `domains` cascade slot, "reachable only
from application defaults" until then). Note: no record in this set explicitly
deferred numeric bounds by name beforehand; 0036 frames the prior state as "a
cascade slot with no spelling" (the same shape as 0016's headings). —
landed-in 0036.

**SL-02 · Requested tab placement.** Deferred-in 0010 §9/§10, Consequences ("a
libaryon protocol extension for requested tab placement is deferred to the
work that needs it") → landed-in 0011 (`TabWith` placement request; 0011
preamble names itself "the libaryon protocol extension PRODUCT-0010 §9 halted
on").

**SL-03 · Multi-origin execution.** 0008 §7's stop condition deliberately excluded
execution changes; the halt fired → landed-in 0010 (origin-keyed execution,
view naming, host-panel realization; 0010 Consequences: "ADR 0008 §7 halted
correctly, and its section 6 additivity holds").

**SL-04 · A line sink.** 0008 §8 authorized "no new sink form" → landed-in 0012
(`line` as the fourth view form; 0012 Context: "PRODUCT-0008 deliberately
authorized no new sink form. Adding one … requires this narrow expansion").

**SL-05 · The layers surface.** Deferred-in 0013 (preamble/§10: "adds no Rune
surface; the `layers([...])` spelling is PRODUCT-0014") → landed-in 0014.

**SL-06 · Bounding the split count.** Deferred-in 0011 Consequences ("Bounding the
split count for buffers with many views waits for a grouping signal rather
than a guess") → landed-in 0015 as producer-scoped tiled groups, which keep
every view visible without a grouping signal (the tab-grouping signal itself
remains open, above).

**SL-07 · Mark-modifier spellings.** Each substrate deferred its spelling to the next
record: 0017 (preamble/§10, `.point_size`) → landed-in 0018; 0019
(preamble/Consequences, `.stroke_width`) → landed-in 0020; 0021
(preamble/Consequences, `.bar_width`) → landed-in 0022.

**SL-08 · Table skin projection and spelling.** Deferred-in 0023 §2/§8 (typed catalog
with no authored name; `.skin(...)` and the register generalization reserved)
→ landed-in 0024 (observable resolved palette, the amended prerequisite) and
0025 (`table().skin(name)`, per-sink closed registers).

**SL-09 · PaletteForward default and `.duotone()`.** Deferred-in 0026 §9 ("PRODUCT-0027
is reserved for exactly two coupled surface decisions"; 0026 Consequences
sized it "a small surface record") → landed-in 0027 (default flip + escape
hatch in one indivisible commit).

**SL-10 · Categorical data palettes for marks.** Deferred-in 0026 Rejected ("mark
coloring as the next projection … is an Encoding/Scale decision") and 0027 §8
("mark data color remain[s a] separate deferred decision") → landed-in
0028 (optional categorical theme palettes), 0029 (stable palette positions,
band fill), 0030 (`palette(name)` figure surface with explicit `viridis`
default and the indivisible caller switch 0029 §2 required).

**SL-11 · Cross-palette live retargeting.** Deferred-in 0029 §2/§9 ("cross-palette
live retargeting is deliberately left to PRODUCT-0030") → landed-in 0030 §6
(natural-length-keyed continuation banks).

**SL-12 · Plot chrome skinning.** Deferred-in 0027 §8 ("figure chrome … separate
deferred decisions"), 0029 §10, 0030 §12 ("plot chrome skinning") → landed-in
0031 (resolved `PlotChrome`), 0032 (figure-level `skin(name)`), corrected by
0033 (order-sensitive default, explicit `.duotone()`) and 0034 (contrast
non-blocking).

**SL-13 · The tick-step surface.** Deferred-in 0037 §8/Consequences ("the
`x_tick_step`/`y_tick_step` spelling is PRODUCT-0038," predicted "small") →
landed-in 0038.

**SL-14 · Authored minor tick subdivision.** Deferred-in 0037 §5/§8 ("authored minor
spacing is not part of this record") and 0038 §6 → landed-in 0039 as a
subdivision count (absolute minor spacing was rejected, not landed).

**SL-15 · Minor-path capacity check.** 0037 §6's capacity limit covered only the
`Step` major path; the gap is named in 0039 Context ("which that record left
unchecked") → landed-in 0039 §5 (total-lattice capacity with checked
arithmetic).

**SL-16 · Static literal bindings.** 0016 §4 lumped named literals with computed
headings and deferred both → the named-literal case landed-in 0040 for all
figure elements; genuine computed values remain deferred (see Data &
bindings).

**SL-17 · Generic modifier abstraction.** Deferred-in 0019 Rejected ("two examples are
not enough evidence") and 0020 ("a third setting must provide evidence before
any shared modifier abstraction is accepted") → settled-in 0022 Rejected:
"three cases justify shared traversal and literal parsing, not open
registration or erased typed dispatch."

**SL-18 · Mark-argument bindings (DB-03).** PRODUCT-0040 §6 pre-authorized mark
arguments as a static-literal coverage extension. The repeated category, value,
and series fields in `test16.rn` supplied the trigger; PRODUCT-0058 routes every
established bar, line, scatter, modified-mark, and layer field position through
the existing static resolver before schema/channel and pair resolution.
Register-argument bindings remain separate. — landed-in 0058.
