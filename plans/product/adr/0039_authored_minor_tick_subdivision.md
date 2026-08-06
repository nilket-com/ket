# ADR 0039: authored minor tick subdivision

Status: proposed 2026-08-06. This record decides one thing: the Rune spelling
that sets how many minor ticks subdivide each major interval, including none. It
adds one figure element per axis and completes PRODUCT-0037 §6's capacity rule on
the minor path, which that record left unchecked.

## Context

The substrate already carries three separate minor settings per axis:
`x_minor_ticks: u32` defaulting to **4**, `show_x_minor_ticks: bool`, and
`show_x_minor_grid: bool`, with independent minor grid width and style. Every
draw site gates on both the flag and the count — `if show_y_minor_grid &&
y_minor_ticks > 0` (`common.rs:1133`, `:1256`, `:1359`, `:1755`) — so a count of
zero already silences marks and gridlines together without a separate toggle.

PRODUCT-0037 §5 made minor ticks subdivide the generator's major intervals, and
`TickPlacement::minor_positions` (`common.rs:109`) is the single site that does
it. What is missing is a spelling.

Two substrate facts shape the rules below.

**Minor capacity is unchecked.** PRODUCT-0037 §6's limit is evaluated only on the
`Step` major path (`common.rs:93-99`) and counts only major positions.
`minor_positions` performs no capacity check at all. So a step of `0.001` over
`0..7` yields 7,001 majors — legal — and a minor count of 9 then produces 63,009
unchecked positions. Each input is legal alone; the product is not. Exposing a
minor count without extending the limit would reopen precisely the unbounded-work
hole PRODUCT-0037 closed for majors.

**The internal parameter name is misleading.** `minor_positions(major,
minor_intervals)` iterates `(1..=minor_intervals)` over `(minor_intervals + 1)`,
so the value is a count of minor **ticks**, not of intervals. The record defines
the semantics rather than inheriting the parameter's name.

## Decision

### 1. `x_minor_ticks` and `y_minor_ticks` are figure elements

```rune
figure([
    y_bounds(0, 7),
    y_tick_step(1),
    y_minor_ticks(0),
    bar("cat", "val"),
])
```

They join the metadata group of PRODUCT-0016 §2 beside `x_bounds`, `y_bounds`,
`x_tick_step`, and `y_tick_step`, recognized only as elements of a `figure([...])`
list, at figure level for PRODUCT-0013 §5's reason: one shared domain means one
lattice per axis.

### 2. A count is the right parameter here, and a step was right there

PRODUCT-0037 §1 rejected an authored count for *major* ticks and PRODUCT-0038 §6
declined to expose one. This record exposes a count. That inversion is
structural, not a reversal.

Major ticks sit on an absolute lattice, so the invariant an author holds is the
**gap**; a count silently changes meaning whenever the domain changes. Minor
ticks are defined *relative to a major interval*, so the invariant is the
**subdivision**; a count stays meaningful when the domain, the bounds, or the
major step changes.

Absolute minor spacing would also break alignment. Nothing forces an authored
spacing to divide the major interval, so a minor spacing of `0.3` against a major
step of `1` puts a minor tick at `0.9` — a hairline from the major at `1.0` —
repeated at every interval. Subdivision counts make alignment structural rather
than a coincidence the author must arrange.

The name is safe to expose for the same reason the major one was not.
`y_ticks` counts *intervals* while claiming to count ticks, which is why
PRODUCT-0037 kept it internal. `y_minor_ticks` counts ticks, so the surface
inherits no ambiguity.

### 3. `n` places exactly `n` ticks strictly between consecutive majors

```text
n = 0   no minor ticks
n = 1   one tick at the midpoint of each major interval
n = 4   four ticks, dividing each major interval into five
```

`n` minor ticks create `n + 1` sub-intervals. Minor positions never coincide with
a major position.

The argument is a non-negative integer, written as an unsuffixed numeric literal.
Following PRODUCT-0038 §2, the grammar accepts what an author writes and the
value refuses what is invalid: `y_minor_ticks(-1)` and `y_minor_ticks(2.5)` are
parsed and then refused by value, naming the value rather than reporting a
spelling error.

Each element may appear at most once. Duplicates refuse.

### 4. Zero disables minor marks and minor gridlines together

Because every draw site already gates on `count > 0`, `y_minor_ticks(0)` removes
both minor tick marks and minor gridlines on that axis. No separate toggle is
introduced, and none is needed for the request that motivated this record.

### 5. Capacity covers the combined lattice

PRODUCT-0037 §6's declared limit applies to the **total** generated positions —
majors plus minors — and `minor_positions` gains the check it never had.

The total is computed **after the major positions are derived**, because the two
placements subdivide different quantities:

```text
Count(n)   total = major_count + (n × minor_count)
                   where major_count = n + 1

Step       total = major_count + ((major_count − 1) × minor_count)
```

The estimate uses **checked arithmetic and is evaluated before any allocation**.
A minor count near `u32::MAX` must saturate into the refusal rather than
overflowing the estimate into a small number that slips past the limit — a
bypassed capacity check is worse than no check, because it looks guarded.

Exceeding the limit is an attributed refusal naming the axis, the major
placement, the minor count, the resolved domain, and the limit.

This is not a new rule. It is PRODUCT-0037 §6 applied to a path that record left
unguarded, and its justification is unchanged:

> A value threshold gatekeeps what an author may look at. A capacity limit bounds
> work that would otherwise not terminate.

A legal major lattice combined with a legal minor count can exceed the limit; the
refusal names the combination rather than blaming either input.

### 6. Applicability is linear numeric, and never silent

A minor count refuses on categorical, band, and temporal axes, which have no
major lattice to subdivide, per PRODUCT-0037 §7. As in PRODUCT-0036 §7 and
PRODUCT-0038 §3, the refusal attributes to the element's span and is never
silently dropped.

Applicability and capacity are resolution-time facts; arity, literal form, and
value are lowering-time facts. The two-phase split is PRODUCT-0038 §3's,
unchanged.

### 7. The visibility flags stay internal

`show_x_minor_ticks` and `show_x_minor_grid` separately control minor tick marks
and minor gridlines, so the substrate can draw one without the other. This record
exposes neither.

Count-zero already satisfies "make them go away," which is the request. A surface
for the flags would add two more figure elements per axis to express a
distinction no author has asked for, and it can be added later without changing
anything decided here.

### 8. What this record does not authorize

No absolute minor spacing. No exposure of the major count or of the visibility
flags. No minor grid width, style, or color. No nice-number or automatic
placement. No log or temporal subdivision. No change to major placement, the
anchor, bounds, marks, palettes, skins, chrome, identity, placement, or
lifecycle.

## Acceptance gates

1. A figure with no minor element resolves to the existing default of 4 and
   renders byte-identically to today.
2. `y_minor_ticks(0)` removes both minor tick marks and minor gridlines on the y
   axis, and leaves x untouched.
3. `y_minor_ticks(1)` places exactly one position at the midpoint of each major
   interval; `n` yields `n` positions and `n + 1` sub-intervals for `n` in
   1, 2, 4.
4. No minor position coincides with a major position.
5. The element behaves identically under `TickPlacement::Count` and
   `TickPlacement::Step`, asserted for both.
6. `y_tick_step(0.001)` over `0..7` is legal alone, and `y_minor_ticks(9)` is
   legal alone; together they refuse, attributed, naming the combination. No
   truncated lattice is produced. A separate assay drives a minor count near
   `u32::MAX` and observes the same refusal rather than an overflowed estimate
   or an allocation.
7. `x_minor_ticks` refuses on `bar("cat", "val")` and on a temporal axis,
   attributed to its span and never silently dropped.
8. Lowering-time refusals fire at their own spans with distinct diagnostics:
   wrong arity, non-literal argument, duplicate element, `y_minor_ticks(-1)`, and
   `y_minor_ticks(2.5)`. The last two name the value, not the grammar.
9. `x_minor_ticks` and `y_minor_ticks` are independent.
10. No major placement, bounds, kernel, grid style, or other behavior changes.

## Rejected

- **Absolute minor spacing:** nothing forces it to divide the major interval, so
  it produces hairline gaps beside majors and makes alignment a coincidence. §2.
- **A separate on/off toggle:** every draw site already gates on `count > 0`, so
  zero is the off switch and a second element would be a synonym.
- **Exposing `show_*_minor_ticks` and `show_*_minor_grid`:** four more elements
  per figure for a distinction nobody has requested. §7 leaves them available to a
  later record.
- **Exposing the major count alongside:** `y_ticks` counts intervals, and
  PRODUCT-0037 §1 and PRODUCT-0038's Rejected list both decline it.
- **Leaving minor capacity unchecked:** the current gap. A minor count multiplies
  an already-legal major lattice, and unbounded work is not something an author
  can be given rope for — it does not render badly, it does not render.
- **Refusing a negative or fractional count at the grammar:** worse diagnostic,
  per PRODUCT-0038 §2.

## Consequences and sequencing

- The smallest kind of cut in this arc: one figure element per axis, one lowering,
  and one capacity check the previous record should have had.
- PRODUCT-0037 §6's capacity becomes total rather than major-only, closing a hole
  that was unreachable before this surface existed and would have been reachable
  the moment it did.
- Minor grid width, style, and colour remain unauthored, as do the visibility
  flags. Each extends this element rather than replacing it.
- Together with PRODUCT-0036 and PRODUCT-0038, an author can now state a domain,
  a major gap, and a subdivision — the three numbers that determine an axis —
  without touching a default.
