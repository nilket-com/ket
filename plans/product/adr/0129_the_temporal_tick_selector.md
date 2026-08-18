# ADR 0129: the temporal tick selector

Status: proposed 2026-08-17. PRODUCT-0128 retains exact temporal domains and
their civil frame. Toron already distinguishes fixed nanosecond intervals from
calendar month intervals. This record makes that two-kind interval meaningful
to plot selection and band reservation **before** Rune can author it.

The separation is intentional. A selector that appears plausible in unit tests
but cannot carry a timezone, preserve exact points, or agree with the shared
band authority is substrate debt, not a safe public setting.

## Context

`TickPlacement::Step(PositiveFinite<f64>)` is a zero-anchored numeric lattice
(`plot/common.rs:64-140`). It cannot describe a month. Toron has the required
value shape already: `LoweredIntervalLiteral::{FixedNanos(i64),
CalendarMonths(i32)}` in `sql_planner.rs`, created by one interval grammar.

That type currently belongs to Toron's planner. Polariton's presentation VM is
separate, and a copied parser or an `f64` conversion would recreate the drift
and category error that upstream interval ADRs deliberately avoided.

## Decision

### 1. Extract one engine-free interval literal carrier

`LoweredIntervalLiteral` and its literal parser move to a shared
Arrow- and engine-free Toron client module, renamed `TemporalInterval`:

```text
FixedNanos(i64)
CalendarMonths(i32)
```

Toron SQL/Rune lowerers consume that shared value without changing their
accepted grammar or arithmetic. Polariton consumes it only as a future tick
policy. Full names, plurals, and Toron's accepted abbreviations (`ns`, `us`,
`ms`, `s`, `min`, `mins`, `h`, `d`) are one grammar; week, quarter, compounds,
and fractions remain refused. **Zero and negative quantities remain accepted
interval deltas**, exactly as they are upstream: `-1 day` is meaningful in
timestamp arithmetic and `0 days` is a valid literal even though neither can
be a tick stride.

### 2. Add a typed temporal placement, but no Rune spelling

`TickPlacement` gains `Temporal(PositiveTemporalInterval)`, where the positive
newtype has private fields and its only constructor converts a
`TemporalInterval` after checking its selected `i64`/`i32` payload is strictly
positive. The interval carrier itself remains lossless; **the placement, not
the grammar, owns the stride rule**. A zero cannot enter the positive newtype,
and a negative interval is rejected by its checked constructor.

That constructor is crate-private to the selector substrate. No presentation
method, compiler vocabulary, setting slot, or application validation accepts it
yet; external code cannot construct the private positive carrier by naming an
enum variant. Default Count and numeric Step remain unchanged.

This lets the selector prove its own selection, capacity, grid, and band
contracts before a user program can depend on it.

### 3. Applicability mirrors Toron's point/delta algebra

| temporal domain | `FixedNanos` | `CalendarMonths` |
|---|---:|---:|
| Date | refuses | accepts |
| Datetime { unit, zone } | accepts | accepts |
| Time | accepts | refuses |

Numeric and categorical axes cannot construct a temporal placement. This
matches Toron: Date plus fixed duration is deferred, Date plus calendar months
is zone-free, a timestamp accepts both with its retained civil frame, and Time
has no calendar position.

### 4. Fixed and calendar intervals have different lattices

Fixed selection is an absolute lattice: Unix epoch for `Datetime`, midnight for
`Time`, with checked integer arithmetic. It is elapsed time; `days(1)` remains
exactly 86,400 seconds across DST.

Calendar selection is a civil lattice in the retained frame: month starts for
`months(n)`, January starts for `years(n)`, with checked month stride. It never
starts at the first data point, so a domain beginning 31 January cannot drift
onto a different day through end-of-month clamping. Date uses the same lattice
without a zone.

### 5. Selection is pre-layout and owns all major-tick consumers

The selector is pure over `TemporalAxisDomain` and `PositiveTemporalInterval`.
It consults neither plot bounds, transform, renderer geometry, nor measured
glyphs. It returns one bounded typed vector. Its exact formatted labels feed
`AxisBandRequirement`; its projected positions feed axis text and major grid
lines. There is no temporal candidate sampler or second grid selector.

`MAX_GENERATED_TICK_POSITIONS` is checked before allocation. Capacity returns a
typed selector error; a future surface must attribute it to its modifier rather
than silently draw no labels.

### 6. Projection is checked, never repaired

Each selected exact point is projected once to the existing `f64` transformer
boundary. If adjacent selected points collapse to one projected coordinate, the
selector refuses. It does not round, deduplicate, change the interval, or move
the text. That makes the current renderer's precision limit an explicit
surface boundary instead of a nearly invisible misplot.

### 7. Calendar minor ticks are deliberately absent

`minor_ticks` remains numeric-only. The existing minor path interpolates
adjacent `f64` major ticks and has no stated meaning for unequal calendar
months. A temporal-minor policy needs its own fixed/calendar design.

## Gates

1. **One grammar.** SQL, relation Rune, and the shared parser normalize every
   accepted full and abbreviated literal identically, including signed and zero
   literals, and refuse the same invalid unit/compound/fraction forms. The old
   Toron lowering has no second parser.
2. **Stride validation is downstream.** `0 days` and `-1 month` parse to their
   upstream `TemporalInterval` values, then each refuses only when conversion
   to `PositiveTemporalInterval` is requested. A mutation moving either refusal
   into the shared parser fails grammar parity.
3. **Kinds cannot collapse.** `1 day` yields `FixedNanos`; `1 month` and `1
   year` yield `CalendarMonths`; no conversion represents calendar months as a
   fixed duration.
4. **Private substrate boundary.** Only the selector module can construct
   `Temporal(PositiveTemporalInterval)` before the surface record; there is no
   presentation registration, compiler vocabulary, or public raw constructor.
   An architecture test attempts the disallowed construction from a sibling
   module and fails to compile.
5. **Applicability matrix.** The §3 table is exercised directly against exact
   Date, Datetime-with-zone, and Time domains, including every refusal.
6. **Fixed DST.** Hourly fixed ticks cross a spring-forward transition as
   elapsed instants and format through the retained local frame without
   inventing a civil hour.
7. **Calendar boundaries.** Monthly and yearly selection begin on civil month
   and January boundaries, even when the domain begins 31 January; leap and
   short months do not shift later selections.
8. **Single selected set.** Text labels, grid positions, and band labels derive
   from one selected temporal vector. A mutation restoring endpoint candidates
   to the band or a second grid selector fails.
9. **Cycle exclusion.** Two plot bounds over the same exact domain and interval
   select the identical vector. The selector reads no geometry.
10. **Capacity.** The just-under-limit case returns exactly its requested ticks;
   the over-limit case errors before allocating or drawing.
11. **Projection refusal.** Contemporary nanosecond ticks that collapse in `f64`
   refuse; a representable neighboring interval produces distinct positions.
12. **Default parity.** Count and numeric Step output, including PRODUCT-0127's
   selected-label requirement, are unchanged. No Rune source can create the
   new placement in this record.

## Stop conditions

1. If extracting the interval carrier makes `toron-client` depend on Arrow,
   Toron's executor, or Rune, stop and create a dedicated shared literal crate.
2. If a required selector case needs a host timezone or an unretained frame,
   stop: PRODUCT-0128 has not delivered its authority yet.
3. If a temporal interval needs space-adaptive selection, stop. That reopens
   PRODUCT-0114's cycle and requires a policy record with a conservative
   pre-layout requirement.
4. If a temporal Cartesian kernel lacks an exact domain after 0128, stop rather
   than treating its existing `f64` range as an interval domain.

## Consequences

This record is an inert selector substrate by design. Its result is not a new
authoring form; it is a selector whose fixed/calendar split, zone behavior,
capacity, projection, and band delivery are proved before a Rune method turns
those semantics into a public commitment.
