# PRODUCT-0150 viewport gesture — implementation evidence

**Status:** implemented, 2026-09-02.

**Amended during implementation.** Three runtime lifetime boundaries were
made explicit in the accepted record as the code made them observable. The
canvas-local drag anchor is stamped so iced cannot carry a predecessor's
press-time transform across kernel replacement; viewport-start and change
messages clear the surface hover because one canvas action cannot publish a
second hover-clear message; and an untouched pane-provenance submit is
revalidated against the current inspector row and displayed value at the
writeback consumer. The close audit also reconciled Stage 0's required kept
`#[ignore]` hover-cost exhibit with gate 17's earlier blanket wording: that
exhibit is the one planned new ignored test, not a tolerated failure.

**Corrected after implementation review, 2026-09-02.** The first landing
minted every canvas message from the widget stamp and incorrectly assumed a
view rebuild between input events. iced dispatches a frame's complete event
batch against one widget tree. The repair chains generations in canvas state,
mints moves from the drag's latest stamp, and carries cumulative wheel geometry
until the rebuilt widget catches up. A same-widget batch test is the
discriminator. The review also prompted an explicit host-focus route for the
captured pan press and a rendered-byte comparison for pane-viewport exports.

## Shipped boundary

The host plot panel and notebook plot surface now own a process-lifetime
viewport incarnation, a monotonically advancing gesture generation, a
`PlotSettings` model containing the committed pane-domain tier, and a
`ViewportMotion` state. The overlay canvas recognizes left-drag pan,
cursor-anchored wheel zoom, and bare double-click reset only inside the final
inner plot rectangle. Ctrl remains the settings chord and Shift-drag remains
reserved for selection.

Cartesian plots gesture on both axes. `CategoricalX` gestures only on y and
`CategoricalY` only on x; two-way categorical, parallel, and radial layouts
refuse. Drag arithmetic is permanently based on the press-time transform.
Wheel zoom anchors against the pre-reflow rectangle and refuses a result below
the larger of the `2^-30` prepared-extent floor and the 1,024-ulp endpoint
floor. The factor in force is 1.1.

Each accepted input event advances the surface stamp and writes only
`pane_override.domains`. Drag release settles immediately. Wheel events start
a 150 ms generation-stamped quiet task; stale generations and predecessor
incarnations are inert. Press invalidates a pending wheel settle, wheel during
drag refuses, and host kernel replacement or notebook republication prevents
old timers and canvas drag state from reaching the replacement.

While moving, rasterized scatter issues no raster request and draws only the
PRODUCT-0139 placed preview or the honest preparing state; hit-testing is
suppressed; and viewport-dependent preparation receives `Moving`. Vector and
downsampled execution remain their settled equivalents. A settled frame makes
the final domain visible to PRODUCT-0149 and produces one exact raster request.

Composite motion is forwarded to every cell's preparation and drawing.
Faceted hexbin therefore defers preparation during motion and re-bins once per
affected cell at settle. Faceted raster scatter deliberately uses
`RasterReusePolicy::Refused`: it accepts exact-key pixels only, contributes no
stale affine image, and shares the surface's single preparing marker. A facet
gesture uses the cell under the cursor as its anchor while committing the
shared figure domain.

The inspector exposes the resulting bounds with pane provenance. Enter on an
untouched pane-provenance bounds row may seed the displayed value into the
existing compile-gated writeback path; every other provenance still requires
a typed draft. The consumer re-resolves the current inspector and rejects a
stale displayed value or provenance. No gesture itself reaches Rune. Kernel
replacement and preview promotion clear the pane viewport, session persistence
stores none of it, and SVG/PNG/AVIF exports consume the current pane model.

## Discriminating evidence

- real canvas press, cursor-move, release, wheel, and double-click events prove
  the vocabulary, Ctrl/Shift arbitration, active-rectangle refusal, per-layout
  axis scope, stamp progression, and reset behavior;
- one unchanged widget receives press, two cursor moves, and release in one
  batch and publishes four consecutive messages ending in the matching
  settle. A two-step wheel burst on that same unchanged widget publishes two
  generations and the second extent is smaller than the first, proving both
  steps contribute;
- presses one pixel inside each plot edge succeed while presses on the title,
  legend/axis bands, and a real facet gutter refuse;
- a drag across a tick-label-width transition repeats exactly from its
  press-time geometry. A wheel transition measures the per-axis reflow drift
  as `(new_origin - old_origin) + u * (new_size - old_size)`;
- cursor anchoring is exact without band reflow, and separate fixtures refuse
  non-finite, relative-floor, and 1,024-ulp-floor zoom results without changing
  the domain;
- eight wheel supersessions consume only the newest settle. Press, host kernel
  replacement, and notebook republication reject delayed settles, including a
  coincident generation from another incarnation. A stamped canvas drag also
  refuses move and release after replacement;
- the state tests prove gestures mutate only the pane tier, re-resolve settings,
  clear hover, and bump render revision. Replacement and preview promotion
  return that tier to `Inherit`;
- hover instrumentation observes no hit call while moving, clears the visible
  bubble on viewport start/change, and resumes on the first settled cursor
  move. The retained ignored exhibit measures one large-scatter hit at one and
  seven million rows when explicitly invoked;
- a real seven-million-point press/move/release sequence places the last exact
  bitmap with zero moving-frame source scans, then mints, builds, and publishes
  exactly one settled raster. The focused run completed this discriminator in
  86.58 seconds in the debug test profile;
- rasterized categorical-x y-pan fails closed at the degenerate x placement,
  shows the preparing marker during motion, and publishes exact pixels after
  settle. Vector and downsampled moving frames match their settled baselines;
- a two-cell faceted hexbin performs zero moving re-bins and exactly one settle
  re-bin per cell. The facet geometry fixture selects the intended cell,
  commits one shared domain, and rejects its gutter;
- the pane submit producer accepts only an untouched pane bounds row, and the
  writeback consumer independently refuses a row whose current provenance or
  displayed value changed before delivery;
- the notebook fixture owns a persistent settings model, accepts the same pane
  messages, and rejects a predecessor settle;
- the existing exact-export fixture now supplies a pane viewport to SVG, PNG,
  and AVIF, verifies those bounds reach resolution, verifies each output's
  container/data signature, and proves the rendered bytes differ from the
  corresponding full-domain export;
- a captured `ViewportStarted` message explicitly focuses its routed host pane,
  preserving the workspace-focus half of the outer click that capture now
  intentionally suppresses; and
- architecture guards pin the two production `Moving` callers, pane-only
  ownership, the 150 ms task seam, composite motion and refused facet reuse,
  current pane-value revalidation, and the unchanged PRODUCT-0140 gate.

PRODUCT-0139's twelve focused tests and PRODUCT-0149's sixteen focused tests
plus two architecture guards pass unchanged. The PRODUCT-0143 hexbin authority
test is amended narrowly: the first `Moving` frame now records stale-preview
state without performing a needless exact preparation.

## Threshold and baseline audit

Against the accepted-plan commit `9237c08ec`, the complete
`resolve_render_mode` function is byte-identical. The library defaults remain
`scatter_max_vector_points: 50_000` and
`scatter_downsample_target: 200_000`, and the raster admission expression
remains `pixel_budget.saturating_mul(8)`. The architecture guard pins all four
facts. PRODUCT-0150 prices none of them.

The census's release-profile test-build failure remains recorded in
`plans/product/impl/0133_gesture_census.md` and was not repaired here: it is
the pre-existing iced unit-renderer bounds failure in table/editor test
widgets, not the plot gesture. The debug all-target/all-feature workspace
check succeeds.

## Verification at close

```text
cargo fmt --all -- --check                                      pass
cargo test -p polariton product_0150 -- --nocapture             pass
                                                               (21 unit pass,
                                                                1 planned ignore,
                                                                2 architecture)
cargo test -p polariton product_0139                             pass (12)
cargo test -p polariton product_0149                             pass
                                                               (16 unit,
                                                                2 architecture)
cargo test -p polariton --test architecture_test \
  -- --test-threads=1                                           pass (114)
cargo test -p polariton --bin polariton -- --test-threads=1     pass
                                                               (1,110 unit pass,
                                                                19 ignored)
cargo check --workspace --all-targets --all-features            pass
git diff --check                                                pass
```

The workspace check emits only the two standing unused-local warnings in
`product_0057_render_evidence.rs`. The close run contains no failure and no
new ignored test beyond the Stage-0 hover-cost exhibit required by this
record.
