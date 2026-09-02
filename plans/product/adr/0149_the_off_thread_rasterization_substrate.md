# ADR 0149: the off-thread rasterization substrate

Status: accepted 2026-09-02, proposed 2026-09-01 and revised 2026-09-02 after review found four
blocking seams and a resource-policy gap (request wake-up and
deduplication, raster subject identity, per-producer slots, exact
rendering paths, global admission). PRODUCT-0139 made raster reuse a
policy and measured its worth — 7.9 ms placed-preview frames against
5,153.7 ms re-rasterization, at seven million points — and PRODUCT-0140
made the raster gate honest about what it does not know. Both records
stand on a substrate neither examined: `build_raster_rgba` executes **on
the UI thread, inside canvas draw**. Every true rasterization is a
frozen frame — 5.2 seconds at the 7M-point rung — and the first contact
any gesture arc would deliver is that stall, at settle, on every pan.
The minimap rasterizes on the UI thread too; the "off-UI-thread raster"
follow-on from the 2026-07 minimap series was never written down
anywhere, and this record is its first home.

This record moves the scatter `Rasterized` arm's **interactive** pixel
production off the UI thread: a serialized coordinator per plot surface
with one last-good slot per raster-producing mark, at most one executing
job per coordinator under global bounded admission, identity carried as
raster subject (source publication plus exact field binding) plus raster
key plus generation, requests published from `Program::update` on
`RedrawRequested` with identical-want deduplication, publication only
through the UI message path with newest-wins refusal, cooperative
cancellation checkpoints because a blocking task cannot be aborted once
running, and an explicit preparing state — chrome plus a stated marker,
never a synchronous raster fallback and never another subject's stale
marks. Exact headless rendering — exports and render tests — is
distinguished from interactive canvas rendering and keeps synchronous
exact rasterization inside its own off-thread context; an export never
contains the preparing marker. The mode *decision* is untouched:
`resolve_render_mode`, the threshold constants, and the ×8 pixel-budget
factor stay exactly as PRODUCT-0140 pinned them, and interaction
policy, threshold pricing, and real-gesture evidence remain with their
owning records (0140's successor and PRODUCT-0133 stages 3–4). What
this record owns from 0140 §4's ledger is the **first-frame stall
term's mechanism**: after it, the stall is a visible preparing latency
on a live UI, not a frozen application.

## Context

### The policy is sound and the substrate freezes the app

0139's reuse machinery answers motion frames in 7.9 ms — and then the
settle frame re-rasterizes synchronously inside `Program::draw`
(`plot/common.rs:3186` region → `plot_with_motion` →
`plot_rasterized` → `build_raster_rgba`, `scatter.rs:705`), scanning
every point of every series into a fresh RGBA buffer while the event
loop waits. The gesture arc 0139 names as its own precondition would
therefore activate a 656× motion win bracketed by multi-second UI
freezes at every gesture boundary. The gesture must land on
already-safe machinery; this record is that machinery.

### The request needs a wake-up seam, and draw is not it

`Program::draw` takes `&self` and can neither send messages nor mint
state; the data canvas's `Program::update` currently returns
immediately. The one event that carries final bounds and runs on the
update path is **`RedrawRequested`** — so that event is where a frame's
want is computed and its request published, including on first contact.
And because redraws are routine, the request path must **deduplicate**:
a want identical to the pending or in-flight job mints no generation
and launches nothing — otherwise every redraw would supersede the same
job and the raster would never finish. Deduplication before generation
is as load-bearing as newest-wins after it.

### A blocking task cannot be aborted, so "exactly once" must be re-derived

0139 §5's "exactly one true rasterization per gesture" was stated in a
synchronous world where the statement enforces itself. Off-thread, the
obvious tools mislead: `Task::perform(...).abortable()` and
`JoinHandle::abort()` stop a future *between* poll points, but a
`spawn_blocking` closure runs to completion once started — an abort
handle on it cancels the delivery, not the work, and a "cancelled" scan
of seven million points still burns the blocking pool. The invariant is
therefore re-derived on two cooperative mechanisms (§5): a **serialized
coordinator** that executes at most one job and starts only the newest
distinct want, and **supersession checkpoints** inside the scan loop
that abandon a job whose generation is no longer current. Nothing
relies on `JoinHandle::abort()` for correctness.

### A token is a publication, not a raster subject

The 0143 arc's artifact token identifies *which rows were published* —
but the same artifact can rebuild a panel with different x/y field
bindings while the token, and plausibly the ranges and colours, stay
identical. Pixels prepared from the wrong columns would then match a
token-only identity and be displayed or published as truth. The
identity a raster answers for is therefore the **raster subject**:
source publication *and* the exact field binding the prepared data was
built from. Kernel replacement invalidates previews even when the
token survives it.

### One image per surface cannot represent what surfaces draw

A live `layers([...])` composition holds multiple scatter renderers,
each independently rasterizable, painted in authored order
(`plot/kernels/layered.rs:15`); facets will amplify the same shape.
One last-good raster per surface is structurally wrong there — the
correct shape is **one slot per raster-producing mark** under one
coordinator per surface. An arbitrary historical cache stays rejected;
per-producer slots are correctness, not caching.

### Exports and render tests are exact, not interactive

The notebook editor path (`editor.rs:543`) holds plot state apart from
`HostPlotState` and needs equivalent raster machinery. More critically,
exports currently render **before** entering their blocking task
(`gui/state.rs:5203`): remove synchronous rasterization from rendering
wholesale and an AVIF would faithfully depict the preparing marker.
The record therefore distinguishes two rendering contracts (§8):
interactive canvas rendering, which is asynchronous and honest about
readiness; and **exact headless rendering** — exports, direct render
tests — which runs entirely off the UI thread and builds exact rasters
synchronously inside that context, because its contract is
correctness of output, not liveness of a UI it does not have.

### What the census found that this record must not touch

`resolve_render_mode` is the one production authority for mode
selection, and PRODUCT-0140's stop condition halts any record that
changes it, the threshold, or the ×8 factor. This record wraps
*execution* of an already-resolved `Rasterized` decision; the decision
itself, the Vector and Downsampled arms, and every constant stay
byte-identical. D7 — `current_x_range_bits` answering `(0, 0)` for
non-Cartesian layouts — stays masked by `reuse_placement`'s
degenerate-extent refusal, and this record's key changes must keep
that mask intact. The minimap's UI-thread ink raster is the same
disease in another organ; it is named a follow-on, not annexed (§10).

## Decision

### 1. Only the interactive Rasterized arm goes off-thread

Vector and Downsampled rendering remain synchronous inline drawing,
byte-identical. When `resolve_render_mode` — unchanged, consulted once
per frame under 0139 §6's one-resolution authority — answers
`Rasterized` on an interactive canvas, the draw path **never
rasterizes**: it consults the coordinator (§2) and draws what each
slot honestly has. The synchronous `build_raster_rgba` call disappears
from every interactive draw path, structurally: an architecture gate
forbids rasterization reachable from the production canvas draw. Exact
headless rendering is the stated exception and lives in §8.

### 2. A coordinator per surface, a slot per raster-producing mark

Each plot surface — host panel and notebook editor alike — owns a
raster coordinator holding:

- **one slot per live raster-producing mark** — a lone scatter is one
  slot; a `layers([...])` composition holds one per rasterizing
  layer, painted in authored order; **a faceted surface holds one per
  cell, today, not in some future**: `plot/grid.rs:475` already
  renders live cell kernels that can resolve `Rasterized`, so every
  current facet cell gets an exact async slot now, with request
  enumeration threaded through the composite/grid seam on stable cell
  identities. What 0139 §7 withholds from facets is affine *reuse*
  (the motion preview); asynchronous exactness is this record's and is
  not withheld — each slot carries at most **one last-good raster**
  (the buffer, its full identity, its `image::Handle`);
- **at most one executing job**, coordinator-wide, plus at most one
  pending want per slot — newest-distinct-wins within each slot,
  scheduling across slots and coordinators fair per §5.

The coordinator lives at the widget-owning layer beside
`render_revision`; kernels remain stateless about asynchrony. The
renderer-held `Mutex<Option<ScatterRasterCache>>` is retired; no
second authority for pixels survives.

### 3. Requests are published on RedrawRequested, deduplicated before generation

The request lifecycle runs through `Program::update` on
**`RedrawRequested`**, the one seam that receives final bounds on the
update path — first contact included. Per redraw, for each slot whose
resolved mode is `Rasterized`, the want is computed and compared:

- want equals the slot's last-good identity → nothing;
- want equals the slot's pending or in-flight want → **nothing** — no
  request id minted, no supersession; the running job finishes;
- want differs → mint a request id, record the pending want, and let
  the coordinator schedule it.

Deduplication precedes request-id minting by construction, so routine
redraws cannot starve a job of completion.

### 4. Identity is subject, key, and generation — all three, always

```text
RasterJobIdentity {
    slot_id,      // stable address within the surface: layer ordinal /
                  //   facet cell identity — two otherwise-identical
                  //   scatter layers are distinct here
    subject,      // raster subject: source publication token
                  //   + the exact field binding (x, y, series fields)
                  //   the prepared data was built from
    raster_key,   // geometry, ranges, point size, background, series colours
    request_id,   // COORDINATOR-WIDE monotonic, minted at request
                  //   (post-dedup), never reused — so a slot destroyed
                  //   and recreated by kernel replacement cannot
                  //   collide with its predecessor's completions
}
```

The request id is coordinator-lifetime monotonic rather than per-slot
precisely because per-slot counters reset when replacement destroys
and recreates a slot: an old completion could then match a fresh
`{subject, key, generation}` triple byte for byte. Lifetime identity,
not value identity, rejects it.

The prepared data carries its subject (token threaded through the
existing `create_plot_from_artifact` route, the bare-`create_plot`
mint fallback as 0144 settled; the field binding is the prepared
data's own). A completed buffer **publishes only when every component
still matches** the addressed slot's current want: a wrong or dead
slot address, subject mismatch (publication or binding changed —
kernel replacement invalidates even under a surviving token), key
mismatch, or request-id mismatch each drop the buffer silently and
completely. Publication replaces exactly the addressed slot's
last-good atomically and bumps `render_revision` through the message
path; a dropped buffer bumps nothing.

### 5. Serialization, checkpoints, and global admission

**The scheduling law, stated exactly:** newest wins **within** each
slot; scheduling **across** slots and coordinators is **fair** —
round-robin over coordinators, then over each coordinator's ready
slots — so a slot whose want is stable cannot be starved by a sibling
that changes on every redraw. Per coordinator, at most one job
executes; on completion the round-robin cursor advances. For the job
already running, `build_raster_rgba` gains **cooperative supersession
checkpoints**: the scan consults **its own slot's** current-request-id
cell at a bounded row-chunk cadence and abandons — publishing nothing
— when that slot has a newer request. Request ids are allocated
coordinator-wide (§4's lifetime guarantee) but stored and checked
**per slot**: a new request for slot B never cancels slot A's stable
running job. `JoinHandle::abort()` and `.abortable()` are explicitly
not correctness mechanisms.

**Globally:** coordinators submit through **bounded admission** — a
global limit on concurrently executing raster jobs — because tokio's
blocking pool is not a bounded CPU-work scheduler and several visible
surfaces could otherwise each start a seven-million-point scan at
once. The shipped bound is **`max(1, floor(available_parallelism / 2))`**,
computed once at startup, falling back to **one** when parallelism
discovery fails: half the machine for rasters, the rest left to the
UI thread, the runtime, and the engines — a derivation, not a magic
number, and tunable within this record only on evidence from gate
10's fixture; the *existence* of admission is not tunable.
Admission preserves per-slot newest-pending semantics (a queued want
may be superseded at zero cost) and starves no surface: this is not
the rejected global single worker — surfaces still progress
independently under the fairness law; only simultaneous *execution*
is bounded.

0139 §5 is re-derived, not weakened: **for a slot's final settled
identity, exactly one job publishes**; superseded jobs publish nothing
and abandon at their next checkpoint; reuse still composes only from
the last *published* raster of the same slot and never chains.

### 6. Workers borrow nothing and copy almost nothing

A job receives: the `Arc<ScatterPreparedData>` (immutable shared — the
seven-million-row scan input is never cloned), an owned settings
snapshot of exactly the fields the raster consumes (the raster-key
fields plus resolved series colours — the once-obtained 0139 §6
resolution, snapshotted at request), and its identity. The
`CoordinateTransformer` is **not** sent across the seam: the raster's
transform is affine from the key's ranges and pixel size — 0139 §2's
own finding — and the worker reconstructs it. No lifetime-bound
borrow crosses the thread boundary.

### 7. The preparing state is explicit, and there is no synchronous fallback

When an interactive frame needs a slot's `Rasterized` output and the
slot has **no compatible bitmap**, that slot contributes nothing but
the surface's chrome — axes, gridlines, titles, the resolved layout —
plus an explicit preparing marker in the plot area. Never a
synchronous rasterization to fill the gap; never pixels from another
subject; never a blank pane pretending to be data. The marker is
plain text in the established status idiom, present **whenever an
interactive Rasterized slot lacks a compatible bitmap** — whether its
request is pending publication, queued for admission, or executing —
naming the state, not guessing progress.

**Compatible** means: the slot's last-good exists, **same subject**,
and the 0139 §2 affine-motion conditions hold (only ranges differ).
Then the last-good draws at its `reuse_placement` — the 0139 preview,
now legal during the in-flight window as well as during gesture
motion: same degraded-preview semantics, same no-chaining rule. An
incompatible last-good (different subject, size, or style) is as good
as none, and a dead subject's bitmap is dropped at invalidation, not
displayed. In a layered surface each slot answers independently and a
pending raster slot suppresses **only its own layer's marks**: sibling
vector, line, and downsampled layers paint normally in authored order,
and the surface shows **one** preparing marker regardless of how many
slots are pending — the marker states the surface's condition, not a
per-slot inventory, and markers never stack or overlap.

### 8. Exact headless rendering is its own contract

Exports (SVG/PNG/AVIF) and direct render tests are **exact**: their
output must contain the data, not the substrate's readiness. The
export path moves its *rendering* — not merely its encoding — inside
its blocking task, where it builds exact rasters synchronously in its
own context; the interactive coordinator is not consulted and the
preparing marker cannot appear in an export, gated. Direct render
tests keep synchronous exact rasterization available through the same
test-visible seam the 0139 gates already use; the structural
no-raster-in-draw gate scopes to the production interactive draw path
and names this exception. The notebook editor's plot state
(`editor.rs:543`) owns a coordinator with identical semantics to the
host panel's — one contract, two owners, gated for parity.

### 9. The settled lifecycle, restated end to end

1. `RedrawRequested` computes each rasterizing slot's want.
2. Want == last-good → draw it. Want == pending/in-flight → draw
   compatible preview or preparing state; no new request.
3. Want differs → mint request id, record pending; coordinator
   schedules under global admission.
4. The worker runs with checkpoints; completion returns through the
   message path.
5. Publication: all-three identity match → replace that slot's
   last-good, bump `render_revision`; any mismatch → drop entirely.
6. The next draw serves the true raster.

Motion frames (`ViewportMotion::Moving`, when the gesture arc supplies
them) are unchanged 0139: placed previews from last-good, no requests,
no rasterization — now guaranteed non-blocking by construction.

### 10. What this record does not do

No change to `resolve_render_mode`, `scatter_max_vector_points`,
`scatter_raster_threshold`, the ×8 factor, or the Vector/Downsampled
arms — PRODUCT-0140's stop condition is honoured by construction. No
gesture, no production `ViewportMotion` caller, no interaction policy,
no threshold or downsample-target decision. No facet raster *reuse* —
0139 §7's hold on the affine motion preview stands — but facet cells
**do** receive their exact async slots here (§2), because they are
live raster producers today and gate 1 cannot pass while a
synchronous grid path survives. No minimap change: its UI-thread ink raster and its
revision-fingerprint invalidation are the named follow-on. No D7
resolution — the degenerate-extent mask stays, gated. No progress
percentage or cancellation UI.

## Implementation sequence

**Stage 0 — bank the stall and the seams.** Bank the synchronous
rasterization inside draw at a large rung (the frozen frame), the
immediate-return `Program::update`, the token-only key's
wrong-binding acceptance, and the export path's on-thread render.
Negative controls.

**Stage 1 — identity and coordinator.** The subject (token + binding)
threaded into prepared data, `RasterJobIdentity`, generations, the
coordinator with per-producer slots at both owners (host panel,
notebook editor), retirement of the renderer-held cache.

**Stage 2 — the request seam and the worker.** `RedrawRequested`
want computation with dedup-before-generation, serialized execution
with newest-distinct selection, global bounded admission, checkpointed
`build_raster_rgba`, owned-input payloads, the message path and
`render_revision` bump, all-three publication matching with the
delayed-fake-worker ordering gate.

**Stage 3 — the draw seam and exact paths.** Draw-path consultation of
slots, the preparing state, compatible-preview placement during
flight, layered multi-slot painting in authored order, the structural
no-raster-in-interactive-draw gate, the export path's off-thread exact
rendering, Vector/Downsampled byte-identity.

**Stage 4 — synthetic evidence and re-derivation.** The synthetic
motion driver, responsiveness / newest-wins / dedup / two-surface
evidence, re-derivation of the 0139 gates that assumed synchrony
(superseded precisely, with attribution), full battery.

## Acceptance gates

1. **Interactive draw never rasterizes — grid cells included.** No
   rasterization is reachable from the production canvas draw path,
   the composite/grid cell path (`plot/grid.rs:475` region)
   included, structurally gated with the §8 exception named; the
   banked synchronous-stall control fails against the repaired seam,
   and a faceted fixture with a Rasterized-mode cell rasterizes
   through its cell slot, not in draw.
2. **Coordinator and slots are the only authority.** Per surface: one
   coordinator, one slot per rasterizing mark, at most one executing
   job, at most one last-good per slot; the renderer-held cache is
   gone.
3. **Requests ride RedrawRequested and dedup precedes minting.**
   First contact launches from the redraw event with final bounds;
   N identical consecutive redraws mint exactly one request id and
   the job completes; the banked perpetual-supersession control goes
   red once deduplication precedes minting.
4. **Identity is addressed and lifetime-safe.** Publication with a
   stale token, a stale field binding under a *surviving* token, a
   stale key, or a stale request id each drops completely — and the
   kernel-replacement fixture preserves token, binding, key, and
   apparent slot position, so **only the lifetime identity** (the
   coordinator-wide, never-reused request id) rejects the old
   completion. A matching publication replaces exactly the addressed
   slot's last-good and bumps `render_revision` once.
5. **Newest wins through disorder.** A delayed fake worker completing
   request ids out of order publishes only the newest-matching
   result; superseded pending wants never start; a superseded running
   job abandons at a checkpoint and publishes nothing, proven with an
   instrumented scan counter.
6. **No abort reliance.** Correctness holds with abort handles inert;
   no path depends on `JoinHandle::abort()` stopping work.
7. **The preparing state is honest per slot and singular per
   surface.** With no compatible bitmap a slot contributes no marks —
   no synchronous raster, no other subject's pixels; a subject change
   drops an incompatible last-good rather than displaying it. The
   mixed-layer fixture — a line layer plus a pending Rasterized
   scatter layer — shows the line painted normally in authored order
   with exactly **one** preparing marker on the surface; two pending
   slots still show one marker, never stacked or overlapping.
8. **Compatible flight previews are 0139 previews.** Same-subject
   affine-compatible last-good draws at its `reuse_placement` during
   flight; chaining impossible; incompatibility in any non-range key
   field refuses, field by field (0139 gate shape, re-run against
   slots).
9. **Slot addressing survives identical twins.** A `layers([...])`
   fixture of two **otherwise-identical scatter layers** — same
   subject, same key — rasterizes both as separate slots addressed by
   `slot_id`, paints in authored order, and one layer's publication
   neither replaces nor invalidates the other's.
10. **Scheduling is fair under forced contention, and admission is
    pinned.** The discriminator runs with an **injectable admission
    bound of one** and at least two coordinators — on a four-core
    machine the shipped bound admits both surfaces at once and would
    prove nothing — and shows: round-robin progress for both
    coordinators through the single execution seat; a slot with a
    stable want completing **uncancelled** while a sibling slot's
    want churns on every redraw (pinning both the starvation control
    and §5's slot-scoped cancellation — churn on slot B never
    abandons slot A's running job); and no surface publishing into
    another's slots. A separate assertion pins the production
    derivation: `max(1, floor(available_parallelism / 2))`, falling
    back to one when discovery fails.
11. **Workers own their inputs.** Shared prepared-data `Arc`, owned
    settings snapshot, identity; no borrow crosses the seam; the
    reconstructed transform matches the on-thread affine map to
    tolerance.
12. **Exactly one publication per settled identity.** A synthetic
    settle sequence ending at one final domain yields exactly one
    published raster for that slot identity, however many
    supersessions preceded it.
13. **Exports are exact and off-thread.** An export of a
    Rasterized-mode plot contains data pixels, never the preparing
    marker; its rendering runs inside the export's blocking context;
    SVG/PNG/AVIF each gated. The notebook editor's coordinator
    behaves identically to the host panel's, gated for parity.
14. **Vector and Downsampled are untouched.** Byte-identical
    rendering for both arms; `resolve_render_mode` and every
    0140-pinned constant asserted unchanged.
15. **D7 stays masked.** Degenerate-extent placement still refuses;
    non-Cartesian `(0, 0)` ranges produce no preview and no panic.
16. **Responsiveness is measured.** With a large-rung raster in
    flight, an interactive draw frame completes within a stated bound
    excluding the banked stall by orders of magnitude; the synthetic
    driver's evidence is banked as the substrate oracle the gesture
    arc will consume.
17. **The 0139 re-derivations are precise.** Gates that assumed
    synchronous settle are superseded with attribution; every other
    0139 assertion — placement math, refusal-by-field, no-chaining,
    resolution authority — survives verbatim.
18. **Full battery.** Polariton unit, architecture, trybuild, Rune
    compiler, GUI, and workspace checks pass with no new ignored or
    tolerated failure.

## Guardrails and stop conditions

- If the seam cannot be built without changing `resolve_render_mode`
  or any 0140-pinned constant, halt; that is 0140-successor territory.
- If `RedrawRequested` proves unable to carry the request lifecycle
  (bounds or ordering surprises), halt and find the seam with the
  harness — do not fall back to requesting from draw.
- If the checkpoint cadence measurably degrades the scan, tune the
  chunk size within stated bounds rather than removing checkpoints;
  if no cadence is both cheap and responsive, halt.
- If the subject cannot be threaded without widening `create_plot`
  beyond the 0144-settled shape, halt and negotiate the seam.
- If the preparing state is ever observable with marks in it, a
  synchronous rasterization is ever reachable from interactive draw,
  or an export ever contains the preparing marker, that is a defect
  in this record's premise, not something to accommodate.
- If the shipped `max(1, floor(available_parallelism / 2))` bound
  (falling back to one when discovery fails) shows
  starvation or convoy effects in gate 10's evidence, the derivation
  is tunable within the record on that evidence; the existence of
  admission and the fairness law are not.
- Facet raster *reuse*, the minimap, gesture wiring, threshold
  pricing, and progress UI each reopen their named boundary as a
  separate record.

## Rejected

**`JoinHandle::abort()` as the cancellation mechanism.** A blocking
task runs to completion once started; abort cancels delivery, not
work. Correctness by abort is correctness by luck. Serialization plus
cooperative checkpoints make supersession cheap where it can be free
and bounded where it cannot.

**Requesting from draw.** `Program::draw` takes `&self`, publishes
nothing, and runs on every paint; a request path there is either
impossible or a side-channel. `RedrawRequested` on the update path is
the seam with final bounds and message access.

**Token-only raster identity.** A token names a publication, not a
binding; the same artifact re-bound to different columns would match
and lie. The subject pairs publication with binding.

**A synchronous fallback while the first raster is pending.** It is
the exact stall this record exists to remove, reintroduced on the
worst frame — first contact. The preparing state is honest; the stall
was not.

**Showing another subject's bitmap during flight.** Stale marks from
a different publication or binding are a lie with axes; the 0143
arc's "untagged rows are not renderable truth" applies to pixels
verbatim.

**One last-good per surface.** Structurally wrong for `layers([...])`
and for facets — both live raster producers today; per-producer slots
are the correctness
shape. An arbitrary historical cache remains rejected — slots are
enumerable by the surface's live marks, not a size-tuned cache.

**A global single worker.** Serializing across surfaces would make
one panel's seven-million-point scan block another's just-over-gate
raster for seconds (nothing below the unchanged gate can enter
`Rasterized` at all, so the victim is always a real, large job).
Bounded admission with the fairness law is the opposite trade:
concurrency is bounded globally, progress is owed per slot.

**Exempting exports by leaving them on the UI thread.** Their stall
is as real as the canvas's; the fix is moving their whole rendering
into their blocking context, not tolerating the freeze because a
file was the output.

**Annexing the minimap.** Same disease, different organ, different
cache philosophy (fingerprint versus structural key) — converging
them deserves its own record with the minimap's design given a fair
hearing, not a rider on scatter's arc.

## Consequences

The gesture arc inherits machinery that is safe at first contact: its
settle frames request work instead of performing it, its motion frames
keep 0139's 7.9 ms previews, and the worst first contact becomes a
live UI with honest chrome and a preparing marker instead of a
five-second freeze. Exports keep their exactness contract and lose
their own stall. 0140 §4's first-frame-stall term acquires its
mechanism — a visible latency to price, not a freeze to apologize for
— while its pricing, and both threshold constants, remain exactly
where 0140's stop condition holds them. The identity discipline that
began with prepared relations in 0143 §9 now governs prepared pixels,
extended by this record with the subject/binding distinction and
dedup-before-generation; the next asynchronous artifact in this
codebase inherits that shape as settled rather than invented.
