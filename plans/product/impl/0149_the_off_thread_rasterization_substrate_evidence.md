# PRODUCT-0149 off-thread rasterization substrate — implementation evidence

**Status:** implemented, 2026-09-02.

**Corrected after implementation review, 2026-09-02.** The first landing
deduplicated before request-id minting but only after publishing
`RasterRequested`. In iced 0.14, that message itself causes another view build
and redraw, so a satisfied raster surface could produce three consecutive
application updates and the framework warning on every frame. The repair adds
a read-only coordinator check at `Program::update`: an identical want now
returns `None` before any message is published. The same review found seven
acceptance claims represented only by architecture inspection or narrower unit
fixtures; the discriminating behavioral coverage below was added before close.

## Shipped boundary

Interactive scatter rasterization no longer scans source points from canvas
draw. Each host-panel or notebook plot surface owns a `RasterCoordinator`;
the coordinator owns one addressed slot per live raster-producing mark,
including authored layers and current facet cells. `RedrawRequested` computes
the final-bounds wants from `Program::update`; the coordinator is consulted
read-only there, and an identical satisfied, pending, or in-flight want
publishes no message. Mutable request handling repeats the comparison before a
request id is minted.

Raster identity is the source publication token plus exact x/y/colour field
binding, the full PRODUCT-0139 raster key, a stable slot path, and a
coordinator-wide never-reused request id. Kernel replacement invalidates all
slots. A completion publishes only when every identity term still matches; a
late completion from an apparently identical earlier kernel lifetime is
dropped.

Execution is serialized per surface and admitted through one process-wide
semaphore sized as `max(1, floor(available_parallelism / 2))`. Scheduling is
newest-within-slot and round-robin across coordinators and ready slots. Each
executing slot owns its own current-request atomic: a sibling request cannot
cancel stable work. Superseded scans observe that atomic before their first
point and at 4,096-point checkpoints, abandon cooperatively, and publish
nothing. Correctness uses no abort handle.

Workers receive the prepared rows by shared `Arc`, an owned snapshot of only
the settings consumed by rasterization, and a transform reconstructed from the
owned raster key and layout. The worker also constructs the native iced image
handle, so publication performs no full RGBA copy on the UI thread. A surface
keeps one last-good artifact per slot.
During compatible motion it applies PRODUCT-0139's affine preview without
chaining; otherwise the pending slot suppresses only its own marks while
siblings paint normally, and the surface emits exactly one `preparing raster…`
marker regardless of pending-slot count.

The synchronous renderer cache is retired. Interactive draw consumes only
coordinator-published pixels, while direct/headless rendering remains exact.
SVG, PNG, and AVIF export rendering now occurs wholly inside the blocking task;
forced-raster export therefore contains data pixels and never exports the
preparing marker. Vector and Downsampled execution, render-mode resolution,
both PRODUCT-0140 constants, and the times-eight pixel-budget factor are
unchanged.

Source artifact identity now survives every scatter construction route,
including styled marks, layered marks, and the default single-layer branch.
The live host panel, notebook editor, composites, and facet grid all thread
their coordinator and stable producer-slot paths through the existing drawing
seams.

## Discriminating evidence

- an actual `Program::update` sequence publishes on first `RedrawRequested`,
  returns `None` for the identical in-flight want, and still returns `None`
  after publication; the lower mutable boundary independently proves N
  identical wants mint exactly one request id;
- delayed completions independently refuse changed publication, field binding,
  raster key, slot, and lifetime request id, and an older completion arriving
  after the newest cannot replace the newest pixels;
- kernel replacement rejects an old completion even when token, binding, key,
  and apparent slot position are preserved;
- only the newest pending want starts, while a running job exits at cooperative
  checkpoints and a request in another slot does not cancel it;
- two otherwise-identical raster layers retain independent slots and authored
  paint order, observed as their distinct native handles; two pending slots
  emit one surface marker. A line-plus-pending-scatter composition proves only
  the raster layer is suppressed. Current facet cells request, publish, and
  draw through stable row/column slots with zero draw-time scans;
- an injected global admission bound of one forces two surfaces through the
  same execution seat; both progress while a stable executing slot completes
  uncancelled beside eight sibling supersessions, and publications remain
  surface-local;
- eight supersessions followed by a final settle publish exactly one raster;
- with a 250,000-point job recorded in flight, the interactive draw scanned
  zero source points, placed no bitmap, emitted one readiness marker, and
  completed in **0.250 ms**, versus the banked 5.2-second synchronous
  large-rung stall;
- PRODUCT-0139 no-chaining is re-derived against coordinator state: every
  placed preview leaves the original native-handle id and full raster key as
  the last-good authority;
- exact SVG export carries an embedded raster image and no preparing marker;
  PNG and AVIF complete through the same exact render path with their expected
  container signatures; and
- architecture guards pin the `RedrawRequested` wake-up seam, renderer-cache
  removal, host/notebook ownership parity, token-preserving construction,
  slot-scoped cancellation, bounded admission, and export rendering inside
  `spawn_blocking`.

## Verification at close

```text
cargo fmt --all -- --check                                      pass
cargo test -p polariton product_0149 --no-fail-fast \
  -- --nocapture                                                pass (16 unit; 2 architecture)
cargo test -p polariton product_0139                             pass (12)
cargo test -p polariton --test architecture_test                pass (112)
cargo test -p polariton --bin polariton -- \
  --test-threads=1                                              pass (1,090; 18 ignored)
cargo check --workspace --all-targets --all-features             pass
cargo test --workspace --all-targets --no-fail-fast -- \
  --test-threads=1                                              pass
git diff --check                                                pass
```

The closing workspace battery passed across every crate and compile-fail
suite. An earlier parallel Polariton run encountered the standing
`product_0063_saved_startup` scheduling flake; its exact isolated rerun and the
complete serial binary both passed. PRODUCT-0149 adds no ignored or tolerated
failure. The all-target check emits only the two pre-existing unused-local
warnings in `product_0057_render_evidence.rs`.
