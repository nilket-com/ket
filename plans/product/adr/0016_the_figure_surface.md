# ADR 0016: the figure surface

Status: accepted 2026-08-04. This record decides one thing: how a Rune buffer
authors plot-wide heading text, and where that text goes. It adds `figure([...])`
as the figure root, lowers headings into PRODUCT-0007's existing authored settings
layer, and fixes the labels shown on panel chrome and plot canvas. It migrates no
plot specification, teaches no kernel about prose, and defers hiding.

## Context

The substrate is already in place. `PlotSettingsLayer` carries
`title: Presence<Arc<String>>` and `subtitle` (`theme.rs:166-167`), both
participate in PRODUCT-0007 §7's five-layer cascade (`theme.rs:273-274`), and the
renderer already draws them (`common.rs:705`). What is missing is a spelling and
a bridge.

PRODUCT-0007 §5 also decided the shape this spelling must take:

> Each plot-side stage is a named position with its own vocabulary. New
> capability extends a stage; it does not extend a verb's argument list. This is
> the structural alternative to the ceiling reached by systems whose
> visualization surface is a terminal verb with accumulating options.

`.title()` methods on sinks, or a third argument to every mark, are precisely the
accumulating argument list that section rejects.

Two other facts constrain the answer. PRODUCT-0013 §6 and PRODUCT-0014 §7 split
routing from description, so the head already distinguishes a content kind from a
generated label; heading text is a **third** thing and must not collapse into
either. And `plot` is already a name in this language — `plot.bar`,
`plot.scatter`, and `plot.line` are the free-function spelling that PRODUCT-0008
§6 explicitly retains, with `ViewShadows.plot` tracking it in the envelope.

## Decision

### 1. `figure([...])` is a sink and the figure root

```rune
r.views([
    view("revenue-trend",
        figure([
            title("Revenue"),
            subtitle("Trailing twelve months"),
            layers([
                line("month", "revenue"),
                scatter("month", "revenue"),
            ]),
        ]),
    ),
])
```

`figure(...)` is recognized only as the sink of an element of a proven
`.views([...])` list — directly, or wrapped by `view(name, …)` as above — under
PRODUCT-0008 §3's contextual recognition and its atomicity rule.

**Figure names a level, not a synonym.** A figure is one complete graphical
artifact: plot-wide metadata plus one plot body. `chart` would be another word for
`plot` and would name the same level twice. `plot([...])` would collide with the
retained `plot.*` namespace, giving one word two unrelated meanings — and unlike
PRODUCT-0014 §2's position-determines-role rule, which gives one *concept* two
roles, that would be two concepts sharing a word. `figure` also survives facets,
where one figure will contain several plots.

### 2. The normal form

```text
Figure
├── title:    zero or one literal
├── subtitle: zero or one literal
└── body:     exactly one Mark or one Layers
```

Metadata order carries no meaning. Layer order inside the body remains paint
order, exactly as PRODUCT-0013 §4 fixed it.

`figure([mark])` is presentation-equivalent to bare `mark`, and — in the same
named or positional slot — the same view, keeping its panel. This is
PRODUCT-0014 §6's one-element rule applied one level out, and it rests on the same
citation: PRODUCT-0010 §3 makes identity named or positional and §4 makes form
non-identifying.

### 3. Nesting is bounded, and every level has one role

```text
view( name , figure([ headings , layers([ marks ]) ]) )
 identity      figure        plot body      marks
```

Each level appears at most once on a path, and each has a single role: `view`
carries identity, `figure` carries plot-wide metadata, `layers` carries paint
order, marks carry encodings. The tree is therefore bounded at four levels by
construction rather than by a depth rule someone maintains.

Refusals that follow, each at its own span:

- a nested `figure([...])` inside a `figure`;
- **`view(...)` inside a `figure`** — identity wraps the artifact, never the
  reverse, and a `view` inside a figure would be claiming an address for
  something that is not a view;
- `title(...)` or `subtitle(...)` inside `layers([...])`, because headings are
  figure-wide and a layer is not the artifact;
- `table()` as a figure body;
- a missing body, or two bodies.

PRODUCT-0008 §5 keeps nesting out of the *view set*, where sinks accumulate flat.
This is a different axis with a fixed shape, and it does not reopen that rule for
the same reason PRODUCT-0014 §3 did not: order remains a property of the form,
and only `layers` carries an order at all.

### 4. Headings are figure-wide, literal, and at most one each

`title` and `subtitle` take exactly one string literal. Duplicates refuse, and
non-literal heading text refuses at its span.

Computed strings are deferred, and the reason is structural rather than
conservative. The envelope lowers statically, before dispatch, so a computed
heading would require both evaluating Rune and carrying the resulting value back
into presentation — a value that does not exist when the panel is established and
has no path into the settings layer afterward. Admitting it is a runtime
evaluation and value-transport decision, not a spelling.

`subtitle` without `title` is legal. The figure's title then continues through
PRODUCT-0007 §7's cascade and reaches the generated label only when no other
layer supplies one; a theme-supplied title still wins on the canvas. See §7.

### 5. Lowering produces two independent artifacts

```text
figure([ title(t), subtitle(s), body ])
  ->  the existing single or layered plot specification, from body alone
  +   an authored PlotSettingsLayer with Presence::Show for each supplied text
```

Neither `AuthoredPlotSpec` nor `LayeredPlotSpec` is migrated, and no kernel gains
any knowledge of headings. The boundary is already where it needs to be: headings
are figure-wide presentation drawn by `PlotWidget` after the kernel has plotted
(`common.rs:705`), not by the kernel itself. This record keeps them there.

The settings layer is the *authored* layer of PRODUCT-0007 §7's cascade. Library
defaults, application theme, and any future pane override keep their existing
precedence and are untouched by this record.

### 6. Exactly two authored states, and why not three

- heading omitted → `Presence::Inherit`;
- heading supplied → `Presence::Show(text)`.

An empty string literal is an empty string, not a hidden heading.

**Hiding is unavailable, and this record does not pretend otherwise.** The
round-trip is broken in two steps that can be read directly:
`theme.rs:283` maps `Presence::Hide` to `None`, and `common.rs:709` maps `None`
to the generated display label. A `hide_title()` would therefore render
`"Line + Scatter"` — the precise opposite of the request.

The later repair must preserve three resolved states:

```text
Fallback | Show(text) | Hide
```

An `Option<String>` cannot express that distinction, so any implementation of
this record that flattens headings into one is foreclosing the fix rather than
deferring it.

The two vocabularies are different and must not be conflated:

```text
authored layer            Inherit | Show(text) | Hide
resolved heading, today   Option<String>            (the defect)
resolved heading, repaired Fallback | Show(text) | Hide
```

This record constrains only what it adds: the Rune normal form and its handoff
carry `Presence<…>` and introduce **no additional** `Option` flattening ahead of
the existing `apply_presence` boundary. At that boundary the existing defect
remains — documented, cited, and not accepted as correct.

### 7. Chrome and canvas labels

```text
chrome = view name, else source-authored title, else generated label

canvas = the resolved title from PRODUCT-0007 §7's cascade:
           Show(text) -> text
           Fallback   -> generated label
           Hide       -> nothing (deferred; see §6)
```

The two chains are not the same chain with different fallbacks. Chrome consults
**only** what the source authored. The canvas consults the **resolved** value,
so library defaults, the application theme, and any future pane override reach it
by their existing precedence.

A subtitle without a title therefore gets the generated title only when the
cascade resolves to `Fallback`; a theme-supplied title still wins on the canvas.

Subtitle never enters chrome. Anonymous panels keep their positional suffix, so
two anonymous figures titled `Revenue` remain `Revenue 1` and `Revenue 2`.

**Name wins** because a name is what the author chose as the label for a
destination, and it stays stable while the artifact's content and prose change. A
named destination therefore stays `revenue-trend` in chrome even as its canvas
title changes.

Only the **source-authored** title participates in chrome. A title arriving from
the application theme or a future pane override affects the canvas alone.
Otherwise chrome text could change with no edit to the buffer, and a panel would
be renamed by a theme.

Chrome text is a *label for* an address, not the address. `HostPanelId` is the
address, and it is unaffected by any label this section derives.

**Chrome is source-current; the canvas is completion-atomic.** Chrome follows the
latest successfully analyzed source and is refreshed during
`EnsureHostPanelGroup`, before dispatch. The canvas heading changes with its
kernel, on completion. A failed run may therefore show refreshed chrome around a
stale canvas, and that is correct: chrome labels *the current program's view*,
while the canvas labels *the plot that is actually drawn*.

This requires one addition to PRODUCT-0015's behavior. Today a repeated group
request with no missing members returns without touching any descriptor
(`session.rs:4893`), so existing panels are never retitled. A repeated request
must refresh member descriptors **without altering geometry, membership, layout
state, or focus** — none of which PRODUCT-0015 gate 5 enumerates, so its
rerun-preservation guarantee is untouched.

The alternative — chrome waiting for completion, so the whole artifact changes
atomically — was rejected for this cut. Group establishment happens before
dispatch, so it would require a new per-panel descriptor-update command, which is
a protocol addition this record does not need.

### 8. `ViewPresentation` keeps its structural label

`display_label` is not replaced. The authored title is carried beside it:

```text
display_label:  "Line + Scatter"     structural, generated, always present
authored_title: Some("Revenue")      source-authored, optional
chrome_label:   "Revenue 2"          derived per §7
canvas_title:   "Revenue"            derived per §7
```

The generated label remains available for diagnostics and for every fallback in
§7. Overwriting it with prose would destroy the description PRODUCT-0013 §6 and
PRODUCT-0014 §7 were written to establish, one record after establishing it.

### 9. Headings do not disturb identity, layout, or routing

Changing or removing a heading preserves the view's identity, its `HostPanelId`,
its group's layout state, and its geometry. An anonymous panel may be relabeled;
that is a chrome refresh, never a replacement, and it must not enter
PRODUCT-0015's `Manual` state or retile a group.

The **canvas** heading and its kernel become visible together on successful
completion. A failed run leaves the previous complete plot stale, canvas heading
included. Pairing an old kernel with a new canvas heading is prohibited: they
arrive by different paths, so this is a real interleaving, not a hypothetical one.

Chrome is outside that atomicity by §7's decision. It refreshes from the latest
successful analysis, so a failed run may show current chrome around a stale
canvas.

Removing `title(...)` on a later successful run restores the generated label on
the canvas, and on chrome at the next analysis, without replacing the panel.

### 10. Acceptance gates

1. `figure([title, subtitle, layers([line, scatter])])` lowers to a
   `LayeredPlotSpec` from the body alone plus an authored `PlotSettingsLayer`
   carrying `Show` for both headings; no kernel receives heading text.
2. `figure([line(x,y)])` and bare `line(x,y)` are presentation-equivalent, and in
   the same named or positional slot they are the same view with the same panel.
3. Each of these refuses at its own span: duplicate `title`, duplicate
   `subtitle`, missing body, two bodies, `table()` as body, nested `figure`,
   `view` inside `figure`, `title` or `subtitle` inside `layers`, and non-literal
   heading text.
4. `subtitle` without `title` is legal, and with no other cascade layer supplying
   one the canvas title resolves to the generated label. A control with a
   theme-supplied title proves the cascade still wins.
5. The chrome chain holds in all three cases: named and titled yields the name;
   anonymous and titled yields the title with its positional suffix; anonymous and
   untitled yields the generated label with its suffix. Canvas is asserted
   independently in the same test.
6. A subtitle never appears in chrome.
7. A title supplied by the application theme reaches the canvas and does **not**
   reach chrome.
8. Changing and then removing a heading preserves view identity, `HostPanelId`,
   group layout state, and geometry, and does not enter `Manual` or retile.
9. Removing `title(...)` restores the generated label on canvas and chrome
   without replacing the panel.
10. On success, the new **canvas** heading and new kernel become visible
    together; on failure, the previous complete plot and its previous canvas
    heading remain stale together. No state pairs an old kernel with a new canvas
    heading. Chrome is asserted separately: it refreshes from the latest
    successful analysis, so a failed run shows current chrome around a stale
    canvas, and a repeated group request retitles existing panels without
    changing geometry, membership, layout state, or focus.
11. `AuthoredPlotSpec` and `LayeredPlotSpec` are unmigrated, and the Rune normal
    form and its handoff carry `Presence<…>` with no `Option` flattening
    introduced ahead of `apply_presence`. The existing flattening at that
    boundary is asserted as the known, unrepaired defect, not as correct
    behavior.
12. Every existing sink, `layers`, identity, placement, group, and dashboard
    behavior is unchanged.

### 11. The stop condition

> This record may add the `figure` constructor, `title` and `subtitle`
> recognition, lowering into the authored `PlotSettingsLayer`, the chrome and
> canvas chains, the `ViewPresentation` field that carries an authored title, and
> descriptor refresh on a repeated `EnsureHostPanelGroup` request.
> If the implementation requires repairing the `Presence::Hide` round trip,
> evaluating computed heading text, changing any kernel, migrating either plot
> specification, or changing panel identity, placement, group state, or
> PRODUCT-0010's content lifecycle, the work stops and the requirement returns as
> its own record.

The expected trigger is `Hide`. Fixing `theme.rs:283` and `common.rs:709` is a
renderer and resolution change with its own evidence, and it is not a convenient
extension of a spelling record.

### 12. What this record does not authorize

No hiding of any heading. No computed or non-literal heading text. No axis-label
authoring, no legend or guide authoring, no facets, no pane overrides, no theme
change. No new mark, no scale composition, no interaction decision. No change to
Toron, the transport, execution, panel identity, placement, group layout, or the
dashboard. No layout persistence. No retirement of `plot.*` or any other existing
spelling.

## Rejected

- **`plot([...])` as the root:** collides with the `plot.*` namespace PRODUCT-0008
  §6 retains. Position would distinguish them, but PRODUCT-0014 §2's rule gives
  one *concept* two roles; this would give one word two unrelated concepts. If it
  were chosen anyway, the collision would need to be an explicitly accepted
  decision rather than a temporary inconvenience awaiting a retirement no record
  has scheduled.
- **`chart([...])`:** a synonym for `plot`, naming the same level a second time.
- **`.title()` on sinks, or heading arguments on marks:** the accumulating
  argument list PRODUCT-0007 §5 rejects, and it would put figure-wide prose on
  something that is not the figure.
- **Replacing `display_label` with the authored title:** destroys the structural
  description established by PRODUCT-0013 §6 and PRODUCT-0014 §7, and leaves §7's
  fallbacks with nothing to fall back to.
- **Letting theme or pane-override titles into chrome:** a panel would be renamed
  with no edit to the buffer.
- **Representing headings as `Option<String>`:** cannot express
  `Fallback | Show | Hide`, so it forecloses the deferred repair instead of
  waiting for it.
- **`hide_title()` in this cut:** would render the generated label, which is the
  opposite of its name.
- **Subtitle in chrome:** chrome is one short label for finding a pane, and a
  subtitle is by construction the secondary text.

## Consequences and sequencing

- A buffer can finally say what its plot is called, and the arc's last obviously
  missing piece of authored presentation closes.
- Three label concepts become explicit and separately testable: an address, a
  generated description, and authored prose. They have been three things since
  PRODUCT-0013 §6; this record is the first to name all three at once.
- `Presence::Hide` acquires a documented, cited defect and a deferred repair with
  a stated three-state requirement, so the fix cannot be quietly reduced to two.
- PRODUCT-0007's authored settings layer acquires its first Rune producer, having
  been reachable only from application defaults until now.
- A future record may author axis labels, legends, or facet structure. Each
  extends a stage inside `figure([...])` rather than a verb's argument list, which
  is the property §1 was chosen for.
