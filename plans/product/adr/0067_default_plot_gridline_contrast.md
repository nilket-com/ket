# ADR 0067: default plot gridline contrast

Status: proposed 2026-08-09. This record retires CI-09 by changing one
compatibility-default plot-chrome role. It adds no Rune surface and does not
choose whether gridlines render above or below marks.

## Context

PRODUCT-0031 extracted the implicit plot chrome without changing its pixels.
That record measured the visible white-at-0.2 gridline against the effectively
black canvas at approximately `1.66:1`, below its own `3:1` non-text criterion,
and deliberately classified the result rather than falsely certifying it.
PRODUCT-0032 preserved the same omitted-skin pixels so theme projection could
land independently. The deferred ledger names the resulting obligation as
CI-09 and requires a later deliberate visual record to raise the default.

Stacked area supplies the trigger. An opaque area can cover most of a Cartesian
canvas, making gridline layering a separate question, but every layering answer
still exposes some gridline directly against canvas. The default therefore has
to meet the accepted canvas-relative rule before that comparison is meaningful.

Named plot skins already choose the quietest gridline alpha from the closed
ladder `[0.35, 0.4, 0.5, 0.6, 0.75, 1.0]` that reaches the same `3:1` criterion.
The compatibility default alone remains outside the rule.

## Decision

### 1. Every Polariton-chosen default gridline meets 3:1 against its canvas

`PlotChrome::DEFAULT`, `PlotChrome::APPLICATION_DEFAULT`, and
`PlotChrome::legacy_from_iced` change their gridline alpha from `0.2` to
`0.35`. White at `0.35` is the quietest already-established projection-ladder
choice that passes the accepted non-text contrast assay over the default dark
canvases.

The RGB channels, every other chrome role, all text sizes, stroke widths, tick
positions, gridline positions, major/minor distinction, and gridline count are
unchanged. This is an opacity repair, not a redesign of plot furniture.

### 2. The rule ends at the adjacent canvas

The certified statement remains exactly the one PRODUCT-0031 defined:
`gridline` has at least `3:1` contrast against `canvas_background` after alpha
compositing. This record makes no data-versus-chrome claim. A fixed gridline
colour cannot guarantee contrast against arbitrary categorical palette fills.

Whether marks occlude gridlines or gridlines overpaint marks is the following
rendering-strata decision. If gridlines move beneath marks, the canvas-relative
rule becomes complete because a visible gridline is adjacent only to canvas.
If they remain above marks, data-relative contrast remains explicitly
unspecified; this record does not disguise that open branch as a completed
guarantee.

### 3. Named skins and authored intent do not move

Theme-projected plot chrome remains byte-identical. Its projection already
starts at `0.35` and may choose a stronger alpha when its paper/ink pair needs
one. No palette, skin, or register spelling changes, and an explicitly skinned
figure must render exactly as before.

The omitted-skin fallback is intentionally no longer extraction-identical to
the pre-PRODUCT-0031 renderer. This record supersedes only PRODUCT-0031's and
PRODUCT-0032's compatibility premise for the default gridline opacity. Their
role vocabulary, contrast thresholds, projection, refusal posture, and all
other banked values remain accepted.

## Acceptance gates

1. `PlotChrome::DEFAULT` and `PlotChrome::APPLICATION_DEFAULT` use exactly
   white at alpha `0.35`; their visible gridline assays pass `3:1` against their
   respective canvases.
2. `legacy_from_iced` uses the same alpha and passes when supplied the accepted
   dark-paper/light-ink pair. No compatibility reconstruction path can restore
   the retired `0.2` value.
3. A role-by-role comparison with the PRODUCT-0032 bank shows `gridline.alpha`
   as the sole changed field. All ten sibling roles and the gridline RGB
   channels are bit-identical.
4. A marks-free default unskinned Cartesian rendering changes only gridline
   opacity from `0.20` to `0.35`. This isolates the contrast repair even when
   PRODUCT-0068 lands in the same implementation changeset and moves gridlines
   relative to marks. Changing positions, counts, widths, or non-grid draw
   operations does not discharge the gate.
5. At least one named-skin Cartesian rendering is byte-identical before and
   after this record, proving the change did not leak into theme projection.
6. The existing low-contrast test is replaced rather than deleted: it now
   asserts the default passes, the minimum remains `3.0`, and a synthetic
   alpha-`0.2` control still measures approximately `1.66:1` and fails. The
   control keeps the assay falsifiable and preserves the historical premise.
7. The deferred ledger marks CI-09 retired by PRODUCT-0067; accepted records
   remain immutable and the implementation evidence names the bounded
   supersession.
8. The complete Polariton/workspace suite, formatting checks, and
   PRODUCT-0057 Vulkan probes pass with no ignored test newly admitted.

## Consequences

Every unskinned plot has visibly stronger major and minor gridlines wherever
they remain exposed against canvas. This is an intentional global pixel change
after the extraction-parity reason for retaining the weaker value was exhausted.
Named-skin colour values do not move.

PRODUCT-0067 and PRODUCT-0068 may land in one implementation changeset. Their
separate controlled gates isolate opacity from strata, while combined baseline
evidence records one final pixel transition instead of refreshing bar and area
figures through a knowingly temporary stronger-grid-over-mark state.

The stacked-area exhibit can now compare rendering strata without one branch
starting from a known-invalid default. The comparison must still repair or
experimentally substitute the kernel's hardcoded 20%-black area boundary before
judging grid-under: otherwise that branch removes the overlay grid while leaving
its intended replacement cue nearly invisible on dark canvases.

This record does not make the strata choice. A grid-under result applies to all
marks and splits gridlines from axes, ticks, labels, and frames in the overlay
layer; that architectural change requires its own record and faceted-clipping
evidence.

## Rejected

- **Keep `0.2` for extraction parity:** extraction was completed and CI-09
  explicitly docketed this deliberate visual repair.
- **Certify the existing value by weakening the threshold:** changes the rule
  rather than fixing the sole default that violates it.
- **Repair only `PlotChrome::DEFAULT`:** application reconstruction could
  silently reintroduce the same failing value.
- **Raise every named skin to `0.35`:** they already select from that floor and
  some legitimately require a stronger alpha.
- **Guarantee contrast over arbitrary data fills:** impossible for one fixed
  chrome colour without constraining palettes, altering mark opacity, or adding
  adaptive rendering; none belongs to this debt repair.
- **Move gridlines beneath marks here:** couples a global rendering-order
  decision to a one-role contrast correction before the stacked-area exhibit.

## Stop condition

> This record may change the three compatibility-default construction paths
> from gridline alpha `0.2` to exactly `0.35`, update the directly dependent
> tests and enumerated baselines, retire CI-09, and add evidence. If passing the
> accepted assay requires changing another role, RGB channel, stroke width,
> position, gridline count, named-skin projection, Rune surface, mark opacity,
> palette, renderer ordering, or any data-versus-chrome rule, work stops and the
> requirement returns as a separate record.
