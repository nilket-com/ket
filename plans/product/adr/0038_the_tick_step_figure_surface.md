# ADR 0038: the tick step figure surface

Status: proposed 2026-08-06. This record decides one thing: the Rune spelling
that selects PRODUCT-0037's `TickPlacement::Step`. It invents no placement
semantics — the anchor, capacity, applicability, and minor-tick rules were all
decided there — and adds no substrate, kernel, or renderer work.

## Context

PRODUCT-0037 landed the placement model and shipped it test-only:
`TickPlacement::Step` has no production caller, one generator serves all sixteen
tick sites, and `Count(8)` remains the default so nothing rendered differently.

Everything the spelling needs already exists. Figure children are recognized at
`rune_relation_envelope.rs:1200-1252`, and `parse_signed_numeric_literal`
(`:1322`) already accepts the literal grammar PRODUCT-0036 fixed for bounds.
What remains is two names and a lowering.

The fixture is written and ahead of the language, for the sixth time in this arc.
`../uat/test12.rn` contains `y_tick_step(1)` and currently reports an unknown
figure child — which is correct, and is exactly the gap this record closes.

## Decision

### 1. `x_tick_step` and `y_tick_step` are figure elements

```rune
figure([
    title("🌟SQL"),
    y_bounds(0, 7),
    y_tick_step(1),
    bar("cat", "val"),
])
```

They join `title`, `subtitle`, `palette`, `skin`, `x_bounds`, and `y_bounds` in
PRODUCT-0016 §2's metadata group, recognized only as elements of a `figure([...])`
list.

Figure level is forced rather than chosen, for PRODUCT-0036 §1's reason:
PRODUCT-0013 §5 gives every layer one shared Cartesian domain, so a per-mark tick
lattice would be a per-mark domain, which that record makes unrepresentable.

### 2. One argument, in PRODUCT-0036's literal grammar

Each takes exactly one argument: an unsuffixed numeric literal, optionally
wrapped in one unary negation. Integer and float spellings are the same value, so
`y_tick_step(1)` and `y_tick_step(1.0)` lower identically — which is what an
author writes by hand, and what `test12.rn` already contains.

Each element may appear at most once. Duplicates refuse.

**Unary negation is accepted by the grammar and refused by the value.** A step
must be strictly positive per PRODUCT-0037 §1, so `y_tick_step(-1)` is invalid —
but rejecting it at the grammar would report *"requires unsuffixed numeric
literals"*, which describes a spelling problem for a value problem. Accepting the
literal and refusing the value reports *"step must be strictly positive"*, which
names the actual mistake. The diagnostic is the reason, not the parser's
convenience.

### 3. Refusal happens in two phases, and both attribute to the span

**At lowering**, in the envelope, before dispatch:

- wrong arity;
- a non-literal argument;
- a duplicate `x_tick_step` or `y_tick_step`;
- a zero, negative, or non-finite value.

**At resolution**, destination-local, after the domain and axis are known:

- an axis that does not accept `Step` — categorical, band, or temporal, per
  PRODUCT-0037 §7;
- a lattice exceeding PRODUCT-0037 §6's declared capacity.

The second class cannot be decided in the envelope: axis type comes from resolved
data and the position count needs the resolved domain, neither of which exists
before dispatch. This is the split PRODUCT-0036 already established for
`x_bounds` on a categorical axis, and it inherits that record's §7 rule — the
refusal attributes back to the figure element's span and is **never silently
dropped**.

### 4. Omission changes nothing

A figure with no tick-step element resolves to `Count(8)` and renders
byte-identically to today. This record adds a way to ask; it changes no default.

### 5. Bounds and step are independent

`x_bounds`/`y_bounds` set the domain. `x_tick_step`/`y_tick_step` set the lattice
over whatever domain resolves. Neither requires the other, per PRODUCT-0037 §3 —
a step over a data-derived domain is defined, and requiring bounds would refuse a
defined request.

Used together they are the motivating case: `y_bounds(0, 7)` with
`y_tick_step(1)` yields 0, 1, 2, 3, 4, 5, 6, 7 in place of today's 0, 0.9, 1.8,
2.6, 3.5, 4.4, 5.2, 6.1, 7.

The two axes are likewise independent: setting `y_tick_step` leaves x at
`Count(8)`.

### 6. What this record does not authorize

No authored minor spacing — minor ticks continue to subdivide the major lattice
per PRODUCT-0037 §5. No exposure of the tick *count*, whose `y_ticks` name means
intervals. No nice-number or automatic placement. No log or temporal step. No
change to placement semantics, capacity, the anchor, bounds, marks, palettes,
skins, chrome, identity, placement, or lifecycle.

## Acceptance gates

1. `figure([y_bounds(0, 7), y_tick_step(1), bar("cat", "val")])` lowers to
   `TickPlacement::Step(1.0)` on the y axis and renders exactly
   0, 1, 2, 3, 4, 5, 6, 7 — the motivating case, end to end.
2. `y_tick_step(1)` and `y_tick_step(1.0)` produce identical lowerings.
3. `y_tick_step(1.0)` with no bounds produces PRODUCT-0037 §3's zero-anchored
   lattice over the data-derived domain, proving no bounds requirement exists.
4. Each lowering-time refusal fires at its own span with a distinct diagnostic:
   wrong arity, non-literal argument, duplicate element, `y_tick_step(0)`,
   `y_tick_step(-1)`, and a non-finite value. The negative case names the value,
   not the grammar.
5. Each resolution-time refusal attributes to the element's span and is never
   silently dropped: `x_tick_step` on `bar("cat", "val")`, a temporal axis, and a
   step exceeding PRODUCT-0037's declared capacity.
6. A figure with no tick-step element resolves to `Count(8)` and renders
   byte-identically to today.
7. `x_tick_step` and `y_tick_step` are independent; setting one leaves the other
   at the default.
8. A `layers([...])` body receives one figure-wide placement per axis through its
   existing shared Cartesian domain; no per-layer placement is constructed.
9. No substrate, generator, capacity, kernel, minor-tick, bounds, or other
   behavior changes.

## Rejected

- **Exposing the tick count:** `y_ticks` means intervals, so eight yields nine
  positions, and a count silently changes meaning whenever the domain changes.
  PRODUCT-0037 kept `Count` as the compatibility default precisely so the surface
  would not have to inherit that ambiguity.
- **Refusing unary negation at the grammar:** cheaper to parse and worse to read.
  §2 prefers the diagnostic that names the real problem.
- **Requiring `y_bounds` alongside `y_tick_step`:** PRODUCT-0037 §3 makes the
  lattice defined over any resolved domain, so requiring bounds would refuse a
  defined request — the posture PRODUCT-0034 through PRODUCT-0036 removed.
- **One element setting both axes:** the axes differ in applicability — a bar's y
  accepts a step while its x does not — so a combined element would half-apply or
  half-refuse. It would also be asymmetric with `x_bounds`/`y_bounds`.
- **A mark-level tick step:** PRODUCT-0013 §5 gives layers one shared domain, so
  a per-mark lattice is unrepresentable rather than merely unwise.
- **Authored minor spacing in this cut:** a second closed question with its own
  anchor and capacity consequences, and nothing in the motivating case needs it.

## Consequences and sequencing

- `../uat/test12.rn` runs as written, and the arc's sixth ahead-of-language
  fixture stops being ahead.
- `TickPlacement::Step` acquires its production caller, closing the two-record
  pattern for the fifth time.
- The `guides` cascade group gains its first Rune producer, after headings in
  PRODUCT-0016, the authored skin layer in PRODUCT-0025, and `domains` in
  PRODUCT-0036.
- Authored minor spacing, automatic nice-number placement, log axes, and temporal
  steps each remain unaddressed and each extends PRODUCT-0037's closed model
  rather than replacing it.
