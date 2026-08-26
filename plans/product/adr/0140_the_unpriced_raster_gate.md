# ADR 0140: the unpriced raster gate

Status: **accepted 2026-08-27.** It follows PRODUCT-0139 and the raster-gate
audit (`plans/product/impl/0140_raster_gate_audit.md`), which is its
prerequisite and which it cites throughout.

This is a **corrective record**. It withdraws the justification behind the
automatic scatter raster gate, retains the gate's current behaviour unchanged as
an explicitly unpriced compatibility heuristic, and states what a replacement
must be. **It deliberately chooses no number.**

It does not change `scatter_raster_threshold`, the `× 8` multiplier, mode
resolution, or any code. It does not decide when a scatter should rasterize —
that is the successor record's subject. It does not disturb PRODUCT-0139, whose
reuse policy is unaffected and whose measurements it depends on. It adds no
authoring surface.

## Context

The automatic gate reads (`plot/kernels/scatter.rs:774-807`):

```
total_points <= scatter_max_vector_points                     -> Vector
supports_embedded_raster
  && total_points > scatter_raster_threshold   (1,000,000)
  && total_points > pixel_budget * 8                          -> Rasterized
otherwise                                                     -> Downsampled
```

`scatter_raster_threshold` came from PRODUCT-0007 with the rest of the settings
model. The `× 8` has no record behind it at all: it is a bare literal
(`scatter.rs:799`) whose only readable meaning is "rasterize above eight points
per pixel", and nothing in the arc ever priced it. This record was opened to
price it.

### What the audit established about the gate's shape

**Both conditions must hold, so the larger binds, and they cross at an inner
plot area of 125,000 px** — roughly 354×354. Above that, any ordinary
full-window plot, the pixel budget binds and the threshold constant is inert.
Below it — small panes, and facet cells in a modest window — the threshold binds
and the budget is inert. This is why the threshold "was never exercised": it has
never *bound*.

**The budget is never the widget rectangle.** It divides the inner plot area,
which is the widget minus every guide band. On one 1200×720 widget the gate
ranges over 3,979,208 … 5,203,000 — a **1.31× spread from chrome alone** — while
the naive `width × height × 8` figure of 6,912,000 is the gate in no
configuration measured. The inner share also falls with size, 0.753 at 1200×720
against 0.424 at 400×300, because guide bands cost roughly fixed pixels. A facet
cell at 1200×720 has a gate of 2,421,608, under half a single plot's.

### What the pricing established, and why it is disqualifying

Cold rasterization costs **≈0.69 µs per point, independent of plot area**. At
the full-window gate that is a **3,666 ms first frame**.

Priced against the mode it is selected over:

| plot | density | downsampled | rasterized | winner |
|---|---|---|---|---|
| 400×300 | 1 | 228.8 ms | 40.0 ms | raster 5.7× |
| 400×300 | 8 | 748.1 | 276.1 | raster 2.7× |
| 400×300 | 16 | 851.9 | 555.4 | raster 1.5× |
| 1200×720 | 1 | 845.9 | 498.3 | raster 1.7× |
| 1200×720 | 2 | 896.3 | 944.5 | downsample 1.05× |
| 1200×720 | **8** | **1,339.6** | **3,644.0** | **downsample 2.7×** |
| 1200×720 | 16 | 2,017.8 | 7,219.3 | downsample 3.6× |

**At the multiplier's own value on a full-window plot, rasterizing is 2.7×
slower than downsampling**, and the gap widens with density. The mechanism is
that downsampled output **saturates** — it draws ~139,700 marks at every
full-window density from 2 upward, so its cost flattens across a 16× point range
while rasterization stays linear. Rasterizing therefore wins where the
downsampled *draw* dominates, at low point counts, and loses where the count is
high: the opposite of the regime the gate selects it for.

**And the crossover is not size-independent.** On a 400×300 pane rasterizing
wins at every density measured; on a full window it wins only at density 1, the
crossover falling between densities 1 and 2. The two disagree in direction, so no single multiplier expresses both. The audit had
reasoned otherwise — both costs are O(n), so the ratio should not depend on area
— and recorded its own correction when the measurement contradicted it.

## Decision

### 1. `× 8` is withdrawn as a speed justification

The reading the bare literal invites — *rasterize once it is faster* — is
**false at every full-window density measured**, and this record says so
explicitly so that nobody re-derives it from the source.

No claim is made that the value is wrong for the product. The claim is narrower
and firmer: **whatever `× 8` is doing, it is not selecting the faster mode**, and
it may not be cited as doing so.

### 2. The current behaviour is retained, unchanged and explicitly unpriced

Nothing in the gate changes. `scatter_raster_threshold` stays 1,000,000, the
multiplier stays 8, and mode resolution is untouched.

It is retained as a **compatibility heuristic**: the behaviour every existing
plot already has, kept because changing it without a policy would trade one
unjustified number for another. Retention here is an explicit decision, not a
default — and it is retention of *behaviour*, not endorsement of the reasoning
that produced it.

### 3. Rasterization's real justification is fidelity and motion, not latency

The timings above price building a frame. Two things they do not capture, both
of which favour rasterizing exactly where the latency comparison does not:

- **Fidelity.** At 10.4M points a downsampled frame draws ~139,700 marks —
  **1.3% of the data** — while a raster stamps all of them. At high density the
  gate's trade is fidelity against latency, not latency against latency.
- **Motion.** PRODUCT-0139 measured a reused raster frame at **7.9 ms** against
  a 5,153.7 ms rescan. Downsampling has no equivalent and re-scans every frame,
  forever. A raster costs 3.7 s once and then moves for 7.9 ms; a downsample
  costs 1.3 s per frame indefinitely.

So a gate that rasterizes at high density is defensible — but on these grounds,
stated, and not on the speed of the first frame.

### 4. A replacement is a session policy, not another constant

The successor record must decide a **product-level policy**, and this record
states its terms so the successor cannot quietly become one more number:

- **First-frame stall.** How long a plot may block when the mode flips. The
  measured cost is ≈0.69 µs/point, so this is a statement about cardinality, and
  3.7 s at the current gate is the figure to argue with.
- **Fidelity floor.** What fraction of the data a static frame may omit.
  Downsampling's ~139,700-mark saturation makes this a *count*, not a ratio: the
  same absolute output whether the source is 1.3M points or 10.4M.
- **Expected motion.** Whether the plot is going to be dragged. Reuse makes
  rasterization cheap per frame *after* the first, so the correct mode depends
  on something no single frame knows. This is the term that makes it a session
  policy rather than a render-time computation.

And two structural requirements:

- **It cannot be one constant.** The crossover disagrees in direction between a
  400×300 pane and a 1200×720 window, so any replacement is a function of plot
  size, or a policy expressed in something other than points-per-pixel.
- **It must say which term binds.** Today's gate is
  `max(threshold, inner_area × 8)`, and which half is operative flips at a
  125,000-px plot area. A replacement that moves one term without saying which
  regime it governs changes nothing where it is inert.

### 5. This record changes no code

Deliberately. The false justification is a comment-level and record-level
problem, not a behavioural one, and the behaviour is retained by §2. An
implementation that changes either constant, or the multiplier, **halts and
requires the successor record** (§7).

## Rejected

**Choosing a new multiplier from this evidence.** The measurements seem to point
straight at one — roughly 1.5 on a full window, and "always" on a small pane.
Rejected because they price only the *build*, and §3 establishes that the real
trade is fidelity and motion, both unpriced. Adopting a number from a partial
cost model is precisely how `× 8` came to exist, and PRODUCT-0138 was withdrawn
for the same error one arc ago.

**Removing rasterization and always downsampling.** Attractive on the numbers:
faster at every full-window density above 1, and simpler. Rejected because it
caps a ten-million-point plot at ~139,700 drawn marks permanently, and forfeits
PRODUCT-0139's reuse — the only mechanism in the product that makes a large
scatter movable.

**Always rasterizing.** The mirror image, and it has the fidelity and motion
arguments on its side. Rejected because it imposes the 3.7 s first frame on
every plot that crosses into it, including plots nobody will ever drag.

**Lowering `scatter_raster_threshold` instead.** The obvious lever, and the one
a reader reaches for. Rejected because above a 125,000-px plot area it is inert:
lowering it changes nothing for any full-window figure, which is where the
behaviour people notice lives.

**Deferring the record until the policy exists.** Cheapest option, and nothing
would break. Rejected because the false justification is live in the source as
an unexplained literal that reads as a speed threshold, and the next reader to
cite it will be citing something this arc has already disproved. The correction
is worth landing on its own.

## Acceptance gates

This record changes no behaviour, so its gates are that its claims are
reproducible rather than that any code does something new.

1. **The measurements reproduce.** `product_0140_gate_pricing_evidence` and
   `product_0140_facet_cold_raster_evidence` re-run on the `evidence` profile
   and reproduce §Context's direction: rasterizing slower than downsampling at
   full-window density 8, faster at every 400×300 density. Absolute figures are
   engine-versioned and are not the gate; the **direction** is.
2. **The path is confirmed, not assumed.** Every arm of both exhibits reports
   its draw calls — `raster_images = 1` and `down_circles > 0` — so a figure
   that silently took a third path cannot be read as evidence. This is the
   check whose absence invalidated PRODUCT-0138.
3. **The gate's inputs reproduce.** `product_0140_gate_input_evidence` yields
   1075×605 at 1200×720, matching the figure PRODUCT-0139's Context already
   carries from an independent exhibit.

## Stop condition

Any change to `scatter_raster_threshold`, to the `× 8` multiplier, or to
`resolve_render_mode`'s arms **halts and requires the successor record of §4**.
This record's retention of current behaviour is what makes that halt safe:
nothing degrades while the policy is negotiated.

## Consequences and sequencing

No migration, no authoring change, no behavioural change, and nothing for a user
to notice. What changes is what may be claimed.

PRODUCT-0139 is unaffected. Its reuse policy does not depend on where the gate
sits, and §3 above strengthens rather than qualifies it: reuse is the reason
rasterizing survives a pricing that would otherwise condemn it.

Sequencing from here:

1. this record;
2. the successor session policy of §4, which needs fidelity and motion priced —
   neither exists yet, and the fidelity term needs a judgement about what a
   139,700-mark frame *shows*, which is a question for contact with a figure
   rather than for a benchmark;
3. the coupled `scatter_max_vector_points` / `scatter_downsample_target` record
   left open by PRODUCT-0138's withdrawal, which shares the downsample-target
   saturation this record measured;
4. facet reuse, held by PRODUCT-0139 §7 for PRODUCT-0133 stages 3–4;
5. D7, confirmed by execution first.
