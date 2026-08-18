# ADR 0128: the temporal axis authority

Status: proposed 2026-08-17. Polariton can recognize `Date`, `Datetime`, and
`Time` dtypes, but it immediately reduces each continuous axis to an `f64`
range and `AxisType::Datetime(unit)`. That keeps the unit and loses both the
exact timestamp ticks and the timezone that gives calendar values their civil
meaning. The current automatic formatter consequently renders every datetime in
UTC, even when its dtype names another zone.

This is substrate work, deliberately before a temporal tick spelling. It makes
the temporal axis carry the facts its existing labels already need, corrects
zoned default Count labels, and proves the carrier through normal rendering.
No interval placement is introduced here.

## Context

### The current carrier is insufficient

`PlotLayout` holds only `(f64, f64)` axis ranges (`plot/common.rs:223-251`),
and the coordinate transformer consumes `f64` positions (`:396-440`).
`polars_type_to_axis_type` maps `Datetime(unit, _)` to `AxisType::Datetime(unit)`,
discarding the second timezone argument (`:2905-2919`). Representative kernels
cast their source columns to `Float64` before calculating ranges
(`plot/kernels/line.rs:300-326`); 21 production sites call the same dtype mapper.

At a contemporary epoch nanosecond timestamp, adjacent representable `f64`
values are 256 ns apart. A source tick and temporal-domain boundary therefore
cannot both be honestly represented by the current `f64` authority.

`format_label` then converts `AxisType::Datetime` through
`chrono::DateTime<Utc>` (`common.rs:2941-2960`). A zoned source has no way to
reach it. No current UAT fixture contains a zoned datetime, so existing parity
captures prove neither that this behavior is wanted nor that it is safe.

### The upstream type distinction is available

Toron already carries timestamp unit and timezone through Arrow. Its fixed
interval arithmetic preserves a UTC timestamp type (ADR 0152); calendar
month/year arithmetic binds an explicit or session-resolved civil frame into
the timestamp's Arrow timezone metadata before execution (ADR 0153 D6). The
plotter receives a Polars datetime dtype that already contains precisely the
unit-and-zone fact it presently drops.

The missing authority is therefore in Polariton's preparation and layout
carrier, not in Toron's temporal operations.

## Decision

### 1. Every continuous temporal axis retains a typed domain beside its pixel range

Cartesian `PlotLayout` carries an optional exact domain for each continuous
axis, in addition to the existing `f64` range used by the renderer:

```text
TemporalAxisDomain::Date { first_day: i32, last_day: i32 }
TemporalAxisDomain::Datetime { first_tick: i64, last_tick: i64, unit, zone }
TemporalAxisDomain::Time { first_nanos: i64, last_nanos: i64 }
```

Prepared kernels take this value from the original Polars temporal series
before their legacy float projection. The exact domain is not an alternate
coordinate transform and does not change marks, hover, or bounds in this cut.
It is the authority for temporal labels and the future tick selector.

Every Cartesian kernel that can infer a temporal `AxisType` must supply the
matching domain. A mark that cannot retain it must not claim a temporal axis;
the carrier census is part of Stage 0 rather than a best-effort migration.

### 2. `AxisType::Datetime` retains its declared IANA frame

`AxisType::Datetime` changes from a unit-only tag to a unit plus its optional
dtype timezone. The zone is neither guessed from the host nor replaced with
UTC. An absent zone remains the existing UTC interpretation; a named IANA zone
is resolved through a single timezone library at formatting time.

Date is zone-free and Time remains elapsed time-of-day. No calendar operation
is added by retaining the frame; this record only preserves the source fact.

### 3. Existing zoned Count labels become correct, explicitly

Formatting a zoned datetime uses its retained zone for **all** placements,
including the default Count(8) policy. A source dtype may not render the same
instant in UTC under Count and in local civil time under a later interval
policy. One dtype has one display frame.

This is an expected output change for a zoned dtype, not byte-identical parity.
Stage 0 banks a normal Count figure whose source type is explicitly zoned,
captures its existing UTC labels, and records the named-zone labels that replace
them. Unzoned datetime, Date, Time, numeric, categorical, and non-Cartesian
figures remain byte-identical.

The correction has a real consumer before syntax: a current plot author can
already supply a zoned datetime column; its labels are currently shown in the
wrong declared frame.

### 4. Exactness ends only at the existing pixel projection

Temporal **selection** operates on the exact domain value: `TemporalAxisDomain`
retains its `i64`/`i32` endpoints, and two instants one nanosecond apart remain
distinguishable where `f64` cannot tell them apart. The existing transformer
receives the corresponding `f64` projection only after a typed tick has been
selected. This record does not change that projection or claim
nanosecond-perfect pixels.

**Formatting is exact to the current displayed resolution, not to the domain.**
`format_label` still takes an `f64`, so a label is rendered from a projected
value carrying roughly 256 ns of error at contemporary epoch nanoseconds. That
is sound only because no temporal formatter displays finer than milliseconds --
`Datetime` renders `%Y-%m-%d %H:%M:%S` and `Time` renders three fractional
digits -- so the projection error is many orders of magnitude below the finest
digit any label prints.

This is a stated dependency, not an incidental one. An exact formatting path is
deliberately not built here: it would widen the carrier past the pixel boundary
this record is drawing, for no visible difference at today's resolution.

The future interval selector must prove adjacent selected ticks have distinct
projected coordinates. That refusal belongs to the selector record because no
interval selection exists here; this record merely preserves the information
needed to make the test meaningful.

### 5. The carrier is a parity-preserving migration before the label correction

Implementation lands in two stages:

1. Thread `TemporalAxisDomain` and timezone metadata through every eligible
   prepared kernel, layout, grid layout, and `AxisBandRequirement`, with the
   legacy formatter still used. Existing captures are byte-identical.
2. Make `format_label` consume the retained zone, update only the banked zoned
   Count exhibit as its stated expected delta, and prove all other defaults
   unchanged.

The split makes the label change attributable. It also prevents a later
temporal-placement feature from using a carrier whose production propagation was
never independently established.

## Gates

1. **Carrier census.** Every production `polars_type_to_axis_type` call site
   is enumerated. Each temporal-capable Cartesian path either carries the exact
   domain and zone or fails Stage 1; no path silently falls back to `f64` only.
2. **Exact before float.** A contemporary epoch nanosecond datetime retains
   its original `i64` endpoints and unit through preparation and layout even
   though its legacy pixel range is `f64`.
3. **Zone propagation.** A non-UTC Polars datetime dtype reaches `AxisType`,
   the temporal domain, grid-cell layouts, and formatted labels with the same
   named zone. A mutation dropping the dtype timezone fails structurally.
4. **Zoned Count correction.** A banked default-Count figure changes from its
   pre-stage UTC labels to its declared zone's labels at a fixed instant. It
   renders rather than falling back to host-local time or UTC.
5. **One dtype, one display frame.** The same zoned dtype formatted through
   ordinary axis text, hover text, and a selected-label requirement has one
   zone-correct string. No placement policy chooses a second frame.
6. **Unzoned parity.** Existing Date, unzoned Datetime at all three units, Time,
   and numeric formatter banks are byte-identical.
7. **Grid congruence.** Faceted temporal cells retain the figure-wide resolved
   timezone and their own exact domains without changing the existing shared
   grid geometry.
8. **No interval surface.** `TickPlacement` remains Count/Step only;
   `tick_every` remains an unknown presentation modifier. This proves the
   record has not committed a public placement before its substrate is proven.
9. **No host authority.** Formatting a named-zone source under two different
   host timezones produces identical operations and labels.

## Stop conditions

1. If a Polars datetime timezone is not an IANA name the chosen formatter can
   resolve, stop. Do not silently use the host zone or stringify a different
   instant; the unsupported metadata needs a boundary decision.
2. If any Cartesian temporal path cannot retain an exact domain through its
   current prepared-data model, stop and widen the carrier record. A partial
   mark rollout would make a dtype's meaning depend on mark selection.
3. If layered or faceted axes combine temporal inputs with different units or
   zones, stop. Selecting a dominant frame is a figure-authority decision, not
   a preparation default.
4. If the zoned Count exhibit reveals a different formatter defect (such as
   DST-gap construction rather than display), stop and name it rather than
   calling the expected label delta complete.
5. If a temporal label is ever to display finer than milliseconds -- sub-second
   `Datetime` digits, or `Time` beyond its three fractional digits -- stop.
   §4's soundness rests on displayed resolution staying coarser than the `f64`
   projection error, and that change inverts the relationship: labels would then
   be rendered from a value that cannot represent the digit they print. It needs
   an exact formatting path carried from `TemporalAxisDomain`, which is a
   carrier decision, not a formatter tweak. PRODUCT-0129's projection refusal
   does not cover it -- that gate guards adjacent ticks collapsing, not the
   precision of a single label.

## Rejected

- **Wait for `tick_every`.** The lost zone already makes automatic labels
  incorrect; surface syntax is not required to make the carrier's defect real.
- **Keep UTC for Count and use local time only for interval ticks.** One
  source dtype would then have two display frames governed by its tick policy.
- **Replace the coordinate transformer now.** This record retains exactness to
  the projection boundary; changing mark geometry is a separate scale record.
- **Infer a zone from the host.** A saved workbook must not change its temporal
  labels when opened on another machine.

## Consequences

PRODUCT-0128 is an independently useful substrate repair: a zoned datetime
plot finally labels the instants in its declared frame. It also gives the next
record a production-proven exact temporal domain and removes the temptation to
teach an interval selector to recover facts that preparation discarded.
