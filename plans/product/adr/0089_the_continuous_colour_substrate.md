# ADR 0089: the continuous colour substrate

Status: proposed 2026-08-12. Quantitative colour has no authority: three kernels
derive their own domains, the theme cascade discards the colour job on the way
through, and a diverging ramp on `-50…100` puts its neutral at data value 25.
This record introduces one continuous scale owning `{domain, job, ramp, anchor}`,
carries the job through the cascade, and normalizes diverging scales piecewise
about an anchor whose provenance is explicit.

It is a **substrate** record. It adds no Rune word, authors nothing, and leaves
ledger item **CI-02** open — that item is palette *authoring* including status,
and the surface record owns it.

## Context

PRODUCT-0087 made the gradient guide faithful to the marks; PRODUCT-0088 gave
heatmap a two-ended domain. Both deliberately left the same defect and banked it
identically — **PRODUCT-0087 gate 17** for bubble, **PRODUCT-0088 gate 18** for
heatmap — each asserting a diverging ramp over `-50…100` places its neutral at
**25**. Two marks, one defect, one decision outstanding.

### What exists, re-read 2026-08-12

**The job already exists, and the cascade already throws it away.** `ColorJob`
carries `Categorical | Sequential | Diverging | Status` (`plot/spec/mod.rs:679`),
`PlotThemeSpec` holds a ramp per job (`plot/theme.rs:49-51`, defaulting Viridis /
BlueWhiteRed / RedGreen), and `PlotThemeSpec::ramp(job)` already resolves one to
the other (`:68-75`).

Then `apply_layer` does this:

```rust
settings.color_theme = theme.sequential_ramp;   // plot/theme.rs:337
```

One line, and a diverging mark can no longer reach `diverging_ramp`. **The defect
is a lost job, not a missing classification** — an earlier draft of this record
proposed classifying all 37 `ColorTheme` members as sequential or diverging,
which would have declared `BlueWhiteRed` intrinsically diverging while a theme
remains free to place it in `sequential_ramp`. That is a second authority
contradicting the first.

**Three quantitative producers, three domains.** Heatmap derives its domain in
`HeatmapCells`; bubble carries `color_range: (f64, f64)` (`bubble.rs:22`); hexbin
uses `count / max_count` (`hexbin.rs:64`) and publishes `max_count` as its guide
high (`:86`).

**The guide type is ready; its projection is not.** `GradientStops` carries
`value`, `position`, and a `sampling` mode (`gradient_stops.rs:9-28`) where
`Even` reproduces the mark sampler's arithmetic exactly, and it requires only
finite strictly-ascending values — so non-uniform stops are already admissible.
But `GradientStops::projected` maps value **linearly**, and `gradient_section`
(`legend.rs:66`) calls it with `(low, high, theme)`. That projection is where
anchoring enters.

## Decision

### 1. One continuous scale, owning a resolved ramp

```rust
pub struct ContinuousColorScale {
    domain: (f64, f64),        // finite, ascending or equal
    job: ColorJob,             // Sequential | Diverging only
    ramp: ColorTheme,          // active_theme.ramp(job), resolved once
    anchor: ContinuousAnchor,
}
```

The scale **owns a resolved ramp** rather than re-resolving on each use.
`PlotThemeSpec` stays the sole job→ramp authority; the scale records what that
authority answered.

**Construction refuses `Categorical` and `Status`.** Neither is a continuous job:
categorical colour is PRODUCT-0030's palette machinery, and status is a discrete
vocabulary that happens to be stored as a ramp. Admitting them would let a
continuous scale be built over a job that has no continuum.

### 2. The job survives the cascade

`ResolvedPlotSettings` gains exactly this field, beside `color_theme`:

```rust
pub struct ContinuousRamps {
    pub sequential: ColorTheme,
    pub diverging: ColorTheme,
}
```

Two members, not a retained `PlotThemeSpec`: the resolved settings should carry
what a continuous scale can address, and `PlotThemeSpec` also holds chrome and
the two non-continuous slots that §1 refuses. `ContinuousRamps::ramp(job)`
mirrors `PlotThemeSpec::ramp` (`theme.rs:68-75`) over the two continuous jobs.

Pinned precisely:

- **Default** — `ResolvedPlotSettings::default()` (`common.rs:493`) initialises
  it from `PlotThemeSpec::default()`'s two continuous slots, so the default
  resolved settings and the default theme agree by construction.
- **Install** — `apply_layer` gains, in the **same atomic edit** as the retained
  legacy line (`theme.rs:337`):
  ```rust
  settings.continuous_ramps = ContinuousRamps { sequential: theme.sequential_ramp,
                                                diverging:  theme.diverging_ramp };
  settings.color_theme = theme.sequential_ramp;   // retained for unmigrated kernels
  ```
  One edit, so no revision exists in which the two disagree.
- **Tiers** — the carrier is written wherever `color_theme` is written and
  nowhere else, so precedence across `application_theme`, `authored_spec`,
  `layer`, and `pane_override` (`theme.rs:261-264`) is unchanged by
  construction, not by re-derivation.

This is the actual repair. Without it §1 cannot be constructed for a diverging
mark at all, because the only ramp reaching the kernel is `sequential_ramp`.

### 3. The job and anchor reach the kernel

The ramp carrier answers job→ramp; it does not tell a kernel *which* job it has.
That travels separately:

```rust
pub struct ContinuousScaleIntent {
    pub job: ColorJob,
    pub anchor: ContinuousAnchor,
}
```

**Production has exactly one origin today, and it is sequential.** The automatic
channel path hardcodes `color_job: None` (`spec/mod.rs:1544`), and heatmap,
bubble, and hexbin all take that path. No production figure carries a diverging
job at all:

| origin | intent | exists today |
|---|---|---|
| automatic resolution | `{ Sequential, None }` | **yes — the only one** |
| resolved `Diverging` | `{ Diverging, DefaultZero }` | no; test injection only |
| authored anchor | `{ Diverging, Authored(a) }` | no; test injection only |

An earlier draft listed the diverging row as a production origin. It is not one,
and a gate asserting it over the three producers would have asserted a state no
figure can reach.

**This record does not change the resolver.** Making a mark's job diverging is a
decision about *when* divergence is meant — which channel, which data, which
authored word — and that is CI-02's subject, not a substrate detail to fund
sideways. 0089 builds the machinery a diverging job will use, gates it under test
injection, and leaves the origin to the surface record. Likewise `Authored(_)` is
representable and gated but has no carrier from `AuthoredPlotSpec` through
`ResolvedPlotSpec`; §6's asymmetry is a substrate rule awaiting an author.

The consequence for gating is stated plainly at gates 7 and 18: **every diverging
assertion in this record is test-injected**, and the cascade beneath it is real.

### 4. Construction refuses during planning, not during paint

`plot()` and `legend_content()` cannot return an error, so scale construction
cannot refuse there. This follows PRODUCT-0088 §2's validation-then-reconstruction
shape:

- **Planning validates** the `ContinuousScaleIntent` against the derived domain,
  applying §6's and §7's rules. Every refusal in this record — non-continuous
  job, non-finite or descending domain, out-of-domain or boundary authored
  anchor, any anchor on a sequential job, `Authored(_)` on a zero-width domain —
  is raised **here**, where a diagnostic can be returned.
- **The constructor stays fallible.** `ContinuousColorScale::new(...) -> Result<…>`
  is the single place every §6 and §7 rule is enforced, so planning and
  reconstruction cannot drift apart by validating in different places.
- **Each kernel owns one `continuous_scale(&self, settings) ->
  ContinuousColorScale` helper.** The helper calls the fallible constructor and
  `expect`s, naming PRODUCT-0089 and the planning invariant that guarantees the
  call cannot fail. The `expect` lives **here, once per kernel** — not at each
  call site.
- **Paint and guide both call the infallible helper**, and neither sees a
  `Result`.

So the fallibility boundary is: constructor fallible, helper infallible, callers
unaware. An earlier draft called the helper infallible while also telling its
callers to `expect`, which describes two different designs.

There is no prepared-scale carrier between the two calls, exactly as 0088 found
for prepared cells. Equivalence is the claim; the single helper is what makes it
true rather than coincidental.

### 5. Two explicit inverse operations

```rust
fn normalize(&self, value: f64) -> f64;              // marks
fn value_at_normalized(&self, t: f64) -> f64;        // guide stops
```

Marks consume `normalize`; `GradientStops::projected` consumes
`value_at_normalized`. They are inverses of one another and **both are stated**,
because the guide's stop values are produced by the *inverse* map — `-50, -25, 0,
50, 100` on an anchored `-50…100` comes from evaluating `value_at_normalized` at
the canonical positions, not from running the forward map. Describing one
piecewise map and leaving the direction implicit invites implementing it once and
using it backwards.

For a diverging scale with an active anchor:

```text
normalize(v)              = 0.5 * (v - low) / (anchor - low)              , v <= anchor
                          = 0.5 + 0.5 * (v - anchor) / (high - anchor)    , v >  anchor

value_at_normalized(t)    = low + (t / 0.5) * (anchor - low)              , t <= 0.5
                          = anchor + ((t - 0.5) / 0.5) * (high - anchor)  , t >  0.5
```

Each arm receives half the ramp regardless of its width. The arms therefore carry
different value-per-colour scales, which is what a diverging scale means: the
reader is shown distance from a reference, not absolute magnitude. Neither arm is
clipped and neither is rescaled to match the other.

**Both operations are `f64`, and `f32` appears only at the sampling boundary.**
An earlier draft returned `f32` from `normalize`, which made gate 4's round trip
unsatisfiable: an `f64 → f32 → f64` trip carries roughly 1e-7 relative error, on
the order of 1e9 `f64` ULPs, so no implementation could have passed it. The cast
happens where the ramp is sampled — `ColorTheme::get_color(t: f32)`
(`colors.rs:90`) — and nowhere earlier. This is also what today's producers do:
`bubble.rs:69` and `heatmap.rs:41` both divide in `f64` and cast at the end, so
`f64` normalization followed by a sampling cast reproduces current sequential
arithmetic **bit-for-bit**, which is what gate 8 requires.

**Outside the domain, finite values clamp to `0.0…1.0`.** This preserves the
legacy ramp boundary behaviour, where a value past either end takes the end
colour.

**Non-finite input panics, in release as well as debug.** `normalize` asserts
`value.is_finite()` through an unconditional `assert!` naming PRODUCT-0089 — not
`debug_assert!`, and not a refusal:

- It cannot refuse: `normalize` returns `f64`, and giving it a `Result` would put
  a fallible call inside the paint loop that §4 exists to keep infallible.
- It cannot be debug-only: a `debug_assert!` makes the release build return
  `NaN` from the clamp comparison and hand it to `get_color`, which is the silent
  wrong-colour outcome the assertion exists to prevent. The two builds would
  disagree about what is true.

The cost is a panic in a render path, accepted because §4 puts domain and intent
validation in planning: a non-finite value reaching `normalize` means preparation
admitted something it should have refused, which is a defect in this crate rather
than bad user data. B3 is what makes that claim true for bubble.

### 6. The anchor carries its provenance

```rust
pub enum ContinuousAnchor {
    None,
    DefaultZero,
    Authored(f64),
}
```

Three variants rather than `Option<f64>`, for the reason recorded under Rejected.
Provenance travels inside `ContinuousScaleIntent` from wherever intent is formed
to scale construction — which, per §3, is automatic resolution today and an
author only after CI-02.

| job | anchor | behaviour |
|---|---|---|
| Diverging | `Authored(a)` | requires finite and **strictly interior** to a non-degenerate domain; otherwise **refuses** |
| Diverging | `DefaultZero` | uses zero when strictly interior; otherwise **linear projection** |
| Diverging | `None` | linear projection |
| Sequential | `None` | linear projection |
| Sequential | `DefaultZero` or `Authored(_)` | **refuses** |

**The fallback is "linear projection", not "no neutral".** The ramp still has a
midpoint; it is simply mapped to the **domain midpoint** rather than to a chosen
value. Calling it an absence would misdescribe what renders.

The asymmetry between `DefaultZero` and `Authored` is the point. A default that
happens not to fit the data is not an author error — a `10…50` diverging scale
must not refuse merely because zero is unreachable — while an author who names an
out-of-domain anchor has asked for a scale with a zero-width arm, which is a
sequential scale wearing a diverging ramp.

### 7. Degenerate domains stay renderable, but do not swallow authored intent

Current producers require `low == high`: a constant heatmap or bubble, an empty
heatmap, a zero-count hexbin.

| aspect | behaviour on `low == high` |
|---|---|
| `normalize` | `t = 0` — preserving PRODUCT-0088 §6 and today's hexbin |
| guide | omitted, through PRODUCT-0087's existing zero-width rule |
| `None`, `DefaultZero` | **accepted and inactive** |
| `Authored(_)` | **refuses** |

Refusing an authored anchor here is data-dependent validation, and that is
deliberate: the two-arm scale the author explicitly asked for **cannot exist** on
a zero-width domain. Accepting it would make authored intent observationally
inert — the figure would render as though the anchor had never been written.

Defaults are inactive rather than refused for §6's reason: a default that cannot
apply is not a mistake.

### 8. The guide consumes the scale, not its parts

```rust
GradientStops::projected(&ContinuousColorScale) -> GradientStops
gradient_section(label, &ContinuousColorScale) -> …
```

Today's `projected(low, high, canonical, reversed_sampling)` and
`gradient_section(label, low, high, theme)` must **not** merely gain an anchor
parameter. A signature still taking decomposed parts lets the guide assemble a
second authority — a different ramp, a different anchor, a domain that no mark
used — and pass every current fixture, because the fixtures supply consistent
parts. Taking the scale makes divergence unrepresentable rather than merely
tested for.

`GradientStops`' own validation contract is unchanged: it still requires finite
strictly-ascending values, which §5's inverse satisfies on any non-degenerate
domain.

### 9. What this record does not do

**CI-02 stays open.** That item is sequential/diverging/**status** palette
authoring; this record adds no Rune spelling and does nothing for status. It
makes the substrate CI-02 will author over.

No categorical colour, no `ColorIdentity`, no persistence — a continuous scale
allocates no categorical identity, so PRODUCT-0086's machinery is untouched. No
mark geometry. No retirement of the legacy catalog: **CI-03** remains open,
though its shape improves, since retiring the ramp becomes a migration *onto*
something.

**Hexbin is a consumer that can never diverge.** Its domain is `0..max_count` and
counts are non-negative by construction, so it takes a `Sequential` scale with
`None`. It stays producerless and blocked on viewport-aware planning.

**PRODUCT-0090 is a prerequisite of this one, and has landed.** At this record's
drafting boundary bubble's preparation was infallible and fabricated values: an
absent colour column became
`vec![0.0f64; height]` (`bubble.rs:213`), and each field is taken with
`unwrap_or(0.0)` (`bubble.rs:263-266`, the colour field at `:266`) with no finite
validation. Two defects follow
— a non-finite value can reach `normalize` in violation of §5, and **a null
observation renders as a real low-end value**, a data-honesty fault older and
worse than the anchoring one.

Neither is fixable from inside a colour record: the null disposition is a
semantic decision — does a null-coloured point take the low end, render unfilled,
or drop? — and settling it here would make 0089 a bubble-preparation record as
well, against the one-substrate-per-mark shape of 0078, 0081, and 0088.

**Bubble cannot be deferred either.** An earlier draft tried to migrate heatmap
and hexbin while leaving bubble alone, which does not compile: `bubble.rs:101` is
one of exactly three production `gradient_section` callers — with
`heatmap.rs:100` and `hexbin.rs:83` — and §8 refuses the decomposed signature it
passes. There is no ordering in which bubble both keeps its current preparation
and survives this record.

So the dependency runs **bubble substrate → 0089 → CI-02 authoring → the Rune
surfaces**, and 0089 migrates all three producers together.

## Implementation sequence

**Prerequisite — PRODUCT-0090**, per §9. Its null disposition and finite-value
validation landed before this implementation began.

**Stage 0 — bank.** The two neutral-at-25 states, and every current gradient
section and mark colour for heatmap, bubble, and hexbin — re-banked after the
bubble substrate lands, since that record may legitimately move bubble's
null-bearing figures.

**Stage 1 — carry the job.** `ContinuousRamps` on `ResolvedPlotSettings`, its
default, and the single atomic `apply_layer` edit of §2, with `color_theme`
retained and no consumer migrated.

**Stage 2 — the scale.** `ContinuousColorScale`, `ContinuousAnchor`,
`ContinuousScaleIntent`, both inverse operations, and §§6–7's rules.

**Stage 3 — the consumers.** All three: heatmap, bubble, hexbin. Planning
validates intent against domain per §4; each gains its infallible
`continuous_scale(settings)` helper; paint and guide both call it and `expect`.

**Stage 4 — the projection.** `GradientStops::projected` and `gradient_section`
take `&ContinuousColorScale` per §8 and consume `value_at_normalized`.

## Acceptance gates

1. **The banked neutral moves to the anchor.** A diverging scale over `-50…100`
   with `DefaultZero` places its neutral at data value **0**, against
   PRODUCT-0087 gate 17's and PRODUCT-0088 gate 18's banked **25** — asserted for
   **both** bubble and heatmap under §3's test injection, since each banked it
   separately.
2. **Both arms render in full.** On that scale, `-50` takes the ramp's first
   colour and `100` its last. This is what defeats symmetric rescaling: under a
   rescaled `-100…100`, `-50` would land at `t = 0.25`.
3. **The piecewise arithmetic is exact.** `normalize(-25) == 0.25` and
   `normalize(+25) == 0.625` on `-50…100` anchored at zero. Not a uniqueness
   claim — gate 2 already excludes rescaling — but the pin that fixes the formula
   rather than its shape.
4. **The inverse is exact and round-trips.** `value_at_normalized` returns `-50`,
   `-25`, `0`, `50`, `100` at `t = 0, 0.25, 0.5, 0.75, 1`; and for interior
   values on **both** arms,

   ```text
   |value_at_normalized(normalize(v)) - v|  <=  4.0 * f64::EPSILON * (high - low)
   ```

   Four `f64` ULP **of the domain width**, not of `v`: the round trip is four or
   five `f64` operations, but a budget relative to `v` is unsatisfiable near
   `v = 0` on a wide domain, where the absolute error is set by the domain's
   magnitude and the relative error is unbounded. Endpoints, the anchor, and
   asymmetric interior values are asserted **separately**, since a map correct at
   endpoints and anchor can still be wrong between them.
5. **Guide stops come from the inverse.** The projected stops for a 5-stop ramp
   on `-50…100` anchored at zero are `-50, -25, 0, 50, 100` — non-uniform,
   strictly ascending, accepted by `GradientStops` unchanged. An implementation
   projecting linearly yields `-50, -12.5, 25, 62.5, 100` and fails.
6. **Guide and marks agree under anchoring.** Extending PRODUCT-0087 gate 11 to
   the piecewise case: at the renderer's 50 swatch positions, on both legend
   paths, the swatch colour equals the mark colour for the corresponding data
   value within 4 ULP per channel.
7. **The job reaches the mark, through a real cascade.** A figure whose theme
   sets a `diverging_ramp` distinct from its `sequential_ramp`, with a
   test-injected diverging job per §3, renders in the **diverging** ramp.
   Asserted through **at least two tiers** — `application_theme` and
   `pane_override` (`theme.rs:261-264`) — with the pane-level ramp winning, not
   against synthetic `ResolvedPlotSettings` alone: a carrier installed in only one
   tier passes a synthetic construction and still loses the ramp in production.
   The job is injected because none can be resolved (`spec/mod.rs:1544`); the
   cascade underneath is production machinery, which is what this gate is for.
   Today's flattening (`theme.rs:337`) makes it fail, so it also proves §2 landed.
8. **Sequential heatmap and bubble figures are unmoved.** Every banked heatmap
   and bubble mark colour and gradient section is byte-identical to Stage 0 —
   achievable because §5's `f64`-until-sampling rule reproduces `heatmap.rs:41`
   and `bubble.rs:69` exactly. Measured against the **post-prerequisite** bank, so
   this gate holds 0089 responsible for no pixel the bubble substrate moved. It
   does **not** claim universal parity; gate 9 states where parity does not hold.
9. **Hexbin's arithmetic changes, bounded and accepted.** Hexbin computes
   `count as f32 / max_count as f32` (`hexbin.rs:64`); the scale divides in `f64`
   and casts after. These are not equivalent — `269329525 / 1481765933` gives
   `0x3E3A1FF6` today and `0x3E3A1FF5` under the scale, adjacent `f32` values.
   The banked hexbin figures must still be byte-identical, since their counts are
   far too small to diverge, **and** the divergence is recorded as accepted rather
   than discovered later as a regression. Accepted because hexbin is producerless,
   so no user figure exists to shift, and because preserving `f32/f32` inside the
   scale would give it a per-consumer arithmetic mode — two authorities again,
   which is what §1 exists to prevent.
10. **`ContinuousRamps` defaults agree with the theme.** `ResolvedPlotSettings::
    default()`'s carrier equals `PlotThemeSpec::default()`'s two continuous slots
    — Viridis and BlueWhiteRed — so a figure resolving no theme layer and a figure
    resolving the default theme reach the same ramps.
11. **Non-continuous jobs refuse construction.** `Categorical` and `Status` each
    refuse; `Sequential` and `Diverging` construct. Both halves, since a
    constructor refusing everything would pass the first.
12. **Anchor provenance is honoured, all five rows.** Per §6: authored interior
    accepts; authored out-of-domain refuses; authored **at** `low` or `high`
    refuses, since one arm would have zero width; `DefaultZero` on `10…50` falls
    back to linear projection **without refusing**; `DefaultZero` on `-50…100`
    anchors. The `10…50` row is the discriminating one — a policy keyed on the
    value rather than its provenance refuses there and fails.
13. **Sequential refuses any anchor.** `DefaultZero` and `Authored(_)` each
    refuse on a sequential job, naming the job; `None` renders. An implementation
    silently ignoring the anchor passes neither.
14. **Degenerate domains behave per §7.** On `low == high`: `normalize` returns
    `0`, the guide is omitted, `None` and `DefaultZero` construct, and
    `Authored(_)` **refuses**. Constant heatmap, constant bubble, empty heatmap,
    and zero-count hexbin are each byte-identical to Stage 0 under `None`.
15. **Refusal happens in planning and paint cannot reach it.** Every §4 refusal
    returns a planning diagnostic naming the cause; and for each, no `plot()` or
    `legend_content()` call occurs. The paired half matters: a gate asserting only
    that planning refuses would pass an implementation that refuses *and* then
    paints a panicking scale.
16. **Out-of-domain values clamp; non-finite values panic; the finite twin does
    not.** Three parts. `normalize` returns exactly `0.0` below `low` and `1.0`
    above `high`, matching the legacy ramp boundary. `f64::NAN`, `INFINITY`, and
    `NEG_INFINITY` each panic with a message naming PRODUCT-0089 — asserted in a
    **release-profile** test, since a `debug_assert!` would pass a debug-only
    version of this gate. And the **finite twin**: a far-out-of-domain finite
    value such as `1e300` on a `0…1` domain returns exactly `1.0` and does not
    panic, so the gate distinguishes "non-finite" from merely "extreme" rather
    than passing an implementation that rejects everything past the domain.
    Asserted for all three producers — bubble included, which is what its
    prerequisite record buys: a bubble still fabricating `0.0` for nulls would
    **pass** this gate while lying, since `0.0` is finite.
17. **The guide cannot hold a second authority.** `GradientStops::projected` and
    `gradient_section` accept `&ContinuousColorScale`; neither has a parameter
    admitting a ramp, domain, or anchor independent of it. Checked as a signature
    fact, since a value-level test cannot observe a divergence the fixtures never
    construct.
18. **Both paths reconstruct equivalently, through the one helper.** For one
    resolved settings value, the scale obtained inside `plot()` and inside
    `legend_content()` compare equal — both via the kernel's single
    `continuous_scale(&self, settings)` helper, which is the only place the
    fallible constructor is `expect`ed. Equivalence, not identity: there is no
    carrier between the two calls. Asserted against the production helper, not
    two test constructions, which would compare an implementation to itself.
19. **The only production intent is sequential.** All three producers resolve
    `{ Sequential, None }`, and **no** production path constructs `Diverging` or
    `Authored(_)` — the automatic path pins `color_job: None`
    (`spec/mod.rs:1544`). This gate asserts an absence, which is exactly §3's
    claim: 0089 adds no origin, so CI-02 has something left to add. If it ever
    fails, a resolver gained a diverging origin without a record saying when
    divergence is meant.
20. **No categorical identity is allocated.** Resolving any of the three marks
    against a known `ColorIdentity` snapshot returns it unchanged, so
    PRODUCT-0086's machinery is never reached and the session schema is
    untouched.

## Stop conditions

> If the job cannot be carried to the kernel without changing what an unmigrated
> kernel receives, stop. §2 keeps `color_theme` precisely so migration is opt-in;
> a carrier that disturbs the flattened field has become a cascade record.

> If `normalize` and `value_at_normalized` cannot round-trip within gate 4's
> budget, stop and report the residual rather than widening it. Two operations
> that do not invert are two authorities, which is what §1 exists to prevent.

> If any §6 or §7 refusal cannot be raised during planning — because the domain
> is not known until paint, or the intent is not reachable there — stop. The
> alternative is a panic in a render path, and §4 exists precisely to prevent
> refusal from migrating into `plot()`.

> If `GradientStops::projected` or `gradient_section` cannot take the scale
> because some caller has no scale to give, stop. The three production callers
> are `heatmap.rs:100`, `bubble.rs:101`, and `hexbin.rs:83`, all migrated here; a
> fourth is a continuous producer this record did not survey. §8's guarantee is
> structural, and one exception restores the decomposed path for everyone.

> If PRODUCT-0090 has not landed, do not start. Its absence is not
> a scheduling matter: §8 breaks `bubble.rs:101`'s call site, so bubble must
> migrate, and a bubble that still fabricates `0.0` for nulls (`bubble.rs:213`,
> `:262`) cannot uphold §5. Attempting 0089 first forces the null disposition to
> be decided inside a colour record, by whoever is fixing the compile error.

## Supersession

Supersedes no record. **Depends on PRODUCT-0090**, which landed first. Discharges
PRODUCT-0087 gate 17's and PRODUCT-0088 gate 18's banked
positions. **Changes the signatures** of `GradientStops::projected`
and `gradient_section` per §8, without changing `GradientStops`' own validation
contract. Adopts PRODUCT-0088 §2's validation-then-reconstruction shape. Leaves
**CI-02** and **CI-03** open.

## Rejected

**Classify all 37 `ColorTheme` members as sequential or diverging.** Attractive
because anchoring plainly applies to some ramps and not others, and this was an
earlier draft's §2. Rejected because `PlotThemeSpec` already answers the question
by *slot* — the same ramp may legally occupy `sequential_ramp` in one theme and
`diverging_ramp` in another — so an intrinsic classification would be a second
authority contradicting the first.

**Carry the whole resolved `PlotThemeSpec` through the cascade.** Attractive
because it needs no new type and can never fall out of step with the theme.
Rejected because it hands kernels chrome and the two non-continuous ramp slots
that §1 refuses, making a `Categorical` continuous scale merely *disallowed*
rather than unrepresentable at the carrier.

**Return `f32` from `normalize`.** Attractive because `get_color` takes `f32` and
the cast has to happen somewhere. Rejected because it makes gate 4's round trip
unsatisfiable — `f64 → f32 → f64` carries ~1e-7 relative error, on the order of
1e9 `f64` ULPs — and because casting early would perturb banked sequential
colours that `bubble.rs:69` and `heatmap.rs:41` produce by dividing in `f64`
first.

**Add an anchor parameter to the existing `projected` signature.** Attractive as
the minimal diff. Rejected because a guide still receiving decomposed parts can
assemble a ramp, domain, or anchor that no mark used, and pass every fixture that
happens to supply consistent parts. §1's single authority has to be structural.

**Carry authored anchor provenance from `AuthoredPlotSpec` now.** Attractive
because §6 already specifies the authored rules. Rejected because nothing
authors an anchor until CI-02, so the carrier would have no producer and its
gates no distinguishing fixture — a probe that cannot reach one of the states it
names.

**Hardcode the neutral at zero.** Attractive because zero is the common case.
Rejected because a diverging ramp shows distance from a *reference*, and the
references that matter — a baseline, a benchmark, a specification — are often not
zero.

**Use `Option<f64>` for the anchor.** Attractive as the obvious type. Rejected
because it cannot distinguish an unavailable default from a numerically identical
authored value, and §6's entire policy is that one falls back while the other
refuses.

**Rescale the domain symmetrically about the anchor.** Attractive because both
arms would then share one value scale. Rejected because it either invents data —
extending `-50…100` to `-100…100` shows a range nothing occupies — or discards it
by clipping the longer arm.

**Accept an authored anchor on a zero-width domain and ignore it.** Attractive
because it avoids data-dependent refusal. Rejected because the two-arm scale the
author asked for cannot exist there, and rendering as though the anchor were
never written makes authored intent observationally inert.

**Resolve the ramp on each use rather than storing it.** Attractive because the
scale would hold less. Rejected because two resolutions can disagree if the
active theme changes between them, and the mark and the guide resolve at
different moments.

## Consequences

Quantitative colour gains an authority. Kernels keep deriving their own domains —
that is producer work and this record does not move it — but they stop
**normalizing and projecting** those domains independently, which is where the
divergence between mark and guide lived. The job survives the cascade for the
first time, and a diverging ramp becomes usable on asymmetric data.

The `-50…100` case that PRODUCT-0087 and PRODUCT-0088 each banked resolves to a
neutral at zero, and the two records' deliberate non-fixes close together.

The record also costs an unplanned predecessor. Following §8's refusal of a
decomposed compatibility path to its call sites found `bubble.rs:101`, and
through it a preparation that renders null observations as real low-end values —
a defect with nothing to do with continuous colour, found only because a
signature change forced someone to look. PRODUCT-0090 paid that prerequisite
before this implementation began.

CI-02 remains open and is now well-posed: the substrate exists, so authoring can
decide spelling and spans without also deciding what a continuous scale *is*.
CI-03's shape improves for the same reason — retiring the legacy catalog becomes
a migration onto a real authority rather than the removal of the only one.
