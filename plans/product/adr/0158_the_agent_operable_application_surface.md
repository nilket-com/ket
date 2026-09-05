# ADR 0158: the agent-operable application surface

Status: proposed 2026-09-04. Application lane, first record; it spans
Polariton's editor and host state, the plot pane, and publication, and it
consumes PRODUCT-0154, PRODUCT-0156, and PRODUCT-0157 without changing them.
PRODUCT-0157's browser gate exposed a product gap: the desktop could be
operated only by a pointer, so an agent verifying a published dashboard
needed the operator to act as its mouse, its fixture builder, and its
verification bridge. PRODUCT-0156 §6 already said the legend is one client
of the action surface and that a harness or an authorized agent dispatches
the same actions by naming a series; PRODUCT-0154 §10 left command-line
publish undecided. This record decides the first application surface an
agent can drive: observe the loaded application, act on a pane through the
same messages the legend sends, publish through the same action the menu
invokes, and receive a receipt. It decides no Rune spelling, no second
plotting language, no live connection into a running window, no agent
authorization model, and no change to any existing acceptance gate.

## Context

### Rune is the representation, whoever wrote it

The operator's posture, stated in review: the plot-settings GUI was removed,
rebuilt on Rune, and put back, so that a chart's authored representation is
code whether a human, an agent, or the GUI produced it. The editing surface
already honours that: PRODUCT-0141's registry makes every setting one
inventory, and the inspector writes back through Rune. What has no code
path is everything that is not authored: the pane's transient state, the
publish action, and the receipt of what was published.

### What already exists headless

The test suite drives the application without a window. `HostPlotState`
applies `PlotPaneMsg` through the same `update` the canvas sends to
(PRODUCT-0150 §4, PRODUCT-0156 §2); the editor's host exports are the same
`HostPlotExport` values the publish action assembles; `assemble` takes those
exports, a witness, and a request, and returns a package or a structured
refusal. PRODUCT-0157's tests publish complete packages through that exact
path. Nothing here is new machinery; it is unexposed machinery.

### What the gate needed and could not ask

Whether a scatter rasterized — decided by `resolve_render_mode` from the
plot area, the point count, and the backend, and reported nowhere; which
series are hidden in a pane, which a saved session deliberately does not
persist (PRODUCT-0133 §9, PRODUCT-0156 §2); and a publish that carries a
chosen hidden set. A fixture had to be authored by hand, sized by eye to
cross a gate no surface reports, hidden by pointer, and published by menu.

## Decision

### 1. One surface, two clients

The application gains a command surface whose verbs are the actions the
GUI already dispatches and whose nouns are the state the GUI already holds.
The GUI and an agent are two clients of it. No verb exists on the surface
that the GUI cannot perform, and no state is observable that the inspector
or the pane does not already resolve. Authored changes stay Rune: a command
never edits a figure's specification, and the surface is not a plotting
language.

### 2. Observe: describe the loaded application, live state distinct from loaded state

`polariton describe <source.rn | --session id>` loads the application
headless exactly as the GUI would, runs the source, and returns one JSON
document: every pane with its figure incarnation (§3), view name, family,
pane size, the series table — ordinals, labels, field, display value — the
hidden set, the resolved render mode for a scatter at the pane's plot area
(`vector`, `downsampled`, or `rasterized`, from the same resolution the
renderer makes), legend geometry, and the inspector's settings with their
provenance tiers. The document separates `loaded` from `live`: a saved
session restores no hidden set, so `loaded.hidden` is always empty and
`live.hidden` is the state after any actions in the same invocation. A
describe that reports a hidden set it did not apply itself is a defect.

### 3. Act: pane messages, addressed by figure incarnation

Actions are `hide`, `show`, `isolate`, and `restore` by series ordinal,
and `resize` by width and height in pixels, each addressed to a pane by
its **figure incarnation**: an opaque token minted when a kernel is
installed in a pane and replaced on every kernel replacement, rerun, or
layer change. A command carrying a stale incarnation is refused with the
current one in the receipt, so an agent cannot act on a figure that has
been replaced under it. Every action is dispatched as the `PlotPaneMsg` the
legend would send, through the same `update`, with the same idempotence,
revision bump, and viewport independence PRODUCT-0156 §2 and gate 5 pin.
Ordinals are the PRODUCT-0157 series-table ordinals of that incarnation.

### 4. Publish: the menu's action, with an authored path

`publish <directory>` invokes the same publish action as Plot → Publish:
the witness capture, the rebuild of selected figures, the assembly, the
guard, and the write, under PRODUCT-0136's three floors. It takes no
option that the menu does not have. The hidden set it snapshots is the
live one, so the sequence "hide, publish" produces the PRODUCT-0157 §3
snapshot without a pointer.

### 5. Verify: a receipt for every command

Every invocation returns a receipt: the commands as parsed, each one's
outcome, the incarnation it acted on, the state after it, and for publish
the package root, the manifest digest, every figure's id with its
visibility and inspection entries, and the refusal reason when refused.
Errors are structured, never prose alone. A receipt is enough for an agent
to cite what it published and for a record to cite what an agent did.

### 6. One invocation, a script of commands

This cut is a batch driver: `polariton drive <source.rn> --commands
<commands.json> --receipt <receipt.json>` runs describe, actions, and
publish in order in one headless application. A live connection into a
running window, where a person and an agent share one session, is the
forward record; nothing here is shaped so that only a batch can use it —
the verbs and the receipt are the same either way.

### 7. Gates stay what they are

PRODUCT-0157 gate 10 is the operator's, in a browser, and this record does
not amend it. The driver makes the fixtures and the packages that gate
needs reproducible; it does not stand in for the person looking. Any later
record that lets an automated pass satisfy part of an operator gate says
so explicitly in that gate.

### 8. What this record does not decide

A Rune spelling for hidden series or for publish; a live IPC into a running
GUI; agent identity or authorization; an MCP or any protocol binding; a
screenshot of the desktop; command-line publish from a saved session
without a source, beyond the `--session` form of describe; and any change
to what a kernel, a pane, or the publisher does.

## Acceptance gates

1. **Describe is the inspector.** For each semantic showcase, `describe`'s
   settings equal the inspector's rows and provenance tiers; the series
   table equals the published `series.json` of the same figure; the render
   mode for the raster fixture is `rasterized` at a 360×360 pane and
   `downsampled` at 800×600, from the same resolution the renderer makes.
2. **Loaded is not live.** A saved session that had a series hidden reports
   `loaded.hidden` empty; after a `hide` in the same invocation `live.hidden`
   names it; a describe with no actions never reports a hidden set.
3. **Actions are the legend's.** For every sequence of PRODUCT-0156 gate 8,
   the driver's state after the commands equals the state the legend
   gestures produce, including unknown and repeated actions; a stale
   incarnation is refused and the receipt names the current one.
4. **Publish is the menu's.** For the same source and the same hidden set,
   the driver's package and the GUI action's package have equal manifest
   identity digests and byte-identical figure files; a refusal (republished
   witness, missing recipe, secret) is the same structured reason.
5. **The receipt is complete.** Every command appears with its outcome; a
   publish receipt names the root, the digest, and each figure's visibility
   and inspection; a parse error in the commands refuses the whole
   invocation before any action.
6. **No new authority.** An architecture guard asserts the driver module
   constructs no kernel, no package, and no settings model of its own: it
   calls the editor, the host state, and `assemble`, and nothing else
   reaches those seams from it.
7. **Both heads** under the PRODUCT-0152 gate, and the full architecture
   suite green after formatting.
8. **The gap closes.** The PRODUCT-0157 gate-10 fixtures are produced by a
   commands file checked in beside the fixture showcase, and the operator's
   remaining step is to open the packages and look.

## Guardrails and stop conditions

1. If a verb needs a code path the GUI does not have, stop: either the GUI
   is missing the capability, which is a plotting or editor record, or the
   verb is a second language.
2. If describe cannot report a state without computing it a second way,
   stop; the surface reads the resolution the renderer makes, never a
   re-derivation.
3. If the driver's package and the GUI's differ for the same state, stop:
   that is a publish defect, not a tolerance.
4. Recheck the record number before the plan commit; two commits, plan
   then one `impl:` cut, corrections amended under review, no push until
   the cut is called stable.

## Risks

- **A batch driver ossifies into the surface.** Mitigated by §6: the verbs
  and receipt are transport-neutral, and the live connection reuses them.
- **Describe drifts from the inspector.** Mitigated by gate 1 comparing the
  two on every showcase, not on a fixture.
- **Agents act on replaced figures.** Mitigated by incarnations in every
  command and refusal on staleness, gate 3.
- **The surface tempts a settings verb.** Authored settings are Rune and
  the inspector's write-back; a `set` verb is out of scope by §1 and a
  guard would name it.

## Forward

The live connection: a person and an agent in one running session, the
agent observing and acting through the same verbs while the person looks.
Then the command-surface posture PRODUCT-0156 named, with §6 there as its
worked example and this record as its second. The web lane resumes with
the navigable rung once PRODUCT-0157 is accepted.
