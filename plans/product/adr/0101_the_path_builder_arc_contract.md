# ADR 0101: the `PathBuilder` arc contract

Status: proposed 2026-08-13, **redrafted after Stage 0**. `PathBuilder::arc_to`
has never said whether it **connects** from the current point or **starts a new
sub-path**, and both implementations chose the latter independently. This
silently breaks **two live shapes**: radial dial's annular rings render as
disconnected arcs closed by chords, and pie's slices render as chord-bounded
circular *segments* rather than centre-bounded sectors. This record pins the
contract, repairs both, accepts a bounded change in curve realization, and adds
the two gate kinds that would have caught it.

**Stage 0 fired both original stop conditions**, which is why this is a redraft
rather than an amendment. The evidence is in Context.

It precedes step 16 of `plans/wip/plot-catalog-restoration.md`.

## Context

### The defect

`radial_dial.rs:91-92` and `:100-101` build each ring as:

```rust
builder.arc_to(center, outer_r, start, start + sweep);
builder.arc_to(center, inner_r, start + sweep, start);
builder.close();
```

Outer arc forward, inner arc backward, close — the standard annular sector, and
the intent is correct.

**Both implementations begin a new sub-path.** The canvas adapter calls
`self.builder.arc(...)` (`common.rs:637-642`), which moves implicitly. The SVG
adapter emits `"M {x} {y} A ..."` (`export.rs:50-52`) — an explicit `M` on every
call. So the second arc never connects to the first, `close()` closes only the
second sub-path, and filling spans each open sub-path with a chord.

### This is not a PRODUCT-0057 backend divergence

An earlier reading classified it as one, reasoning that an SVG oracle would pass
where the production canvas failed. **That is wrong**: `export.rs` emits its own
`M`, so a rendered SVG reproduces the same chords. The two adapters agree.

It is a **shared, unspecified contract defect** — a question `PathBuilder` never
answered, answered the same way twice by accident. Classifying it correctly
matters, because "the backends disagree" would send the repair to the wrong layer
and would credit the oracle boundary with a divergence it does not have.

### Why nothing caught it

The draw-operation assertions record arc **operands** — centre, radius, angles —
and every one of them is correct. What is wrong is the **topology**: how many
sub-paths exist and how they join. No assertion in the codebase records path
verbs, so no test could see it.

The defect was found by a screenshot, after the mark's first exhibit shipped.

### `pie` is wrong too, and an earlier draft said it was fine

`pie.rs:43-44` calls `move_to(center)` and then `arc_to`. The `move_to` is dead —
the arc moves again regardless — so `close()` joins the arc's **endpoint to its
own start**, producing a chord-bounded circular **segment**. A pie slice is a
centre-bounded **sector**. These are different shapes.

An earlier draft of this record asserted pie "is nonetheless drawn correctly,
because a single arc plus `close()` yields the chord-bounded shape a slice
happens to want." That was wrong, asserted without checking the geometry, and
Stage 0 measured it:

| filled area, no AA | pixels |
|---|---:|
| current chord-bounded segment | **12,336** |
| correct centre-bounded sector | **11,191** |

So the dead `move_to` is not merely ignored — its being ignored is a second live
defect, in a mark that ships today.

### Stage 0: both stop conditions fired, independently

Measured on the designated iced/wgpu evidence host.

**Connection changes pie's pixels**, which the first stop condition anticipated:

| tolerance | joined pie diff, MSAA | joined pie diff, no AA |
|---|---:|---:|
| 1.0 – 0.01 | 1,301 px | 1,147 px |
| 0.001 | 1,306 px | 1,156 px |

**Decomposition changes boundary realization independently.** Rendering kurbo
cubics against the *old* chord topology — isolating decomposition from connection
— still differs from lyon quadratics by **76 MSAA pixels** and **25 no-AA
pixels** across tolerances 1.0–0.01.

Two effects, separately real. Neither is a tuning artifact.

## Decision

### 1. `arc_to` connects when a sub-path is open

| state | behaviour |
|---|---|
| no current sub-path | **move** to the arc start |
| a current sub-path exists | **connect** to the arc start with a line, then append the arc |
| after either | the current point is the **arc end** |

Both adapters implement it. This makes radial dial's outer-arc → inner-arc →
`close()` a genuine annular sector with two radial caps, and makes pie's
`move_to(center)` meaningful instead of dead.

**iced has no connected arc primitive, so the adapter appends the arc itself.**
`Builder::arc` delegates to `ellipse`, which does an **unconditional**
`move_to(arc.sample(0.0))` before emitting quadratic béziers
(`iced_graphics-0.14.0/src/geometry/path/builder.rs:105-122`). A preceding
`line_to` is therefore discarded, and §1 is undeliverable through `arc()`.

The canvas adapter instead does what `ellipse` does, minus the move:

- compute the arc's start point, and `move_to` or `line_to` it per the table;
- decompose the arc with `kurbo::Arc::append_iter(tolerance)`, which yields
  `PathEl::CurveTo` (`kurbo-0.13.1/src/arc.rs:55,91,132`) — **cubic** segments —
  and append each with `Builder::bezier_curve_to`. Not `quadratic_curve_to`: an
  earlier draft named the wrong verb because it assumed kurbo decomposed the way
  lyon does.

**No new dependency, but not a free substitution.** `kurbo` is already direct
(`polariton/Cargo.toml:42`), so the geometry authority exists. But iced's
`ellipse` path emits **lyon quadratics** while this emits **kurbo cubics**: the
curves are geometrically equivalent and their realized edge pixels are **not
guaranteed identical**.

**The tolerance is `0.1`.** kurbo derives its subdivision count from it
(`arc.rs:57-58`), so it determines realized geometry directly. Stage 0 measured
tolerances 1.0 through 0.01 as producing **identical** pixels, with 0.001
differing by 5–9 px; `0.1` sits in the interior of that stable band, so the pin
is adjacent to no observed transition on either side. A value at the band's edge
would make an implementation detail able to flip realization.

**The realization change is accepted, not denied.** Stage 0 isolated it: kurbo
cubics against the old topology still differ from lyon quadratics by 76 MSAA and
25 no-AA pixels. That is a real, bounded change to boundary antialiasing, and
this record gates it rather than claiming a parity it does not have.

The SVG adapter needs only to drop its leading `M` in favour of an `L` when a
sub-path is open (`export.rs:50-52`).

An earlier draft decided §1's semantics without checking that iced could express
them — the contract was settled and unimplementable at the same time.

### 2. The contract is fixed at the seam, not compensated in kernels

No kernel gains an explicit `line_to` between its arcs, and none is rewritten to
avoid the pattern.

`PathBuilder` has two implementations that both guessed, and a kernel-local fix
would leave the next annulus to guess again while every operand assertion stays
green. This is PRODUCT-0097's reasoning applied to a second seam: repair the
contract, not its consumers.

### 3. Path topology is asserted, not just operands

A gate records a **normalized semantic trace** — `Move → Arc → Line → Arc →
Close` — capturing the adapter's move-or-line decision and each arc's operands,
and asserts that an annulus is **one connected sub-path with two radial caps**.

**Semantic, not physical, and the distinction is forced.** SVG emits one `A` per
arc; iced emits several cubic `CurveTo`s for the same arc. Their *physical* verb
sequences can never match, so a shared trace of emitted curves is impossible by
construction — an earlier draft asked for "every verb a path emits," which
contradicted gate 1's own `move, arc, line, arc, close` expectation.

The boundary is therefore explicit: the **semantic trace** proves the join
decision and the arc parameters, and **PRODUCT-0057's oracle (§4)** proves iced's
physical cubic realization. Neither gate can do the other's work.

**The trace is taken at the adapter, not at the kernel recorder.** The existing
kernel-level recorders observe two `arc_to` *calls*; the move-or-line decision
happens **inside** each adapter, so a kernel recorder cannot see it. Teaching the
recorder to expand `arc_to` would make it a **third implementation** of §1's
contract, and the gate would then be comparing two implementations of the thing
under test.

So: one **normalized topology trace** emitted by the adapters themselves, shared
by the iced and SVG conformance gates. Both must produce the same normalized
verb sequence for the same path.

This is the gate class that was missing. Every existing assertion checks what the
arcs *are*; none checks how they *join*, which is the entire content of §1.

### 4. A production-canvas visual oracle, under PRODUCT-0057

Radial dial gains a visual check against the **production renderer**, added to
the existing feature-gated `product_0057_render_evidence` target
(`Cargo.toml:72-73`), on its designated evidence host.

Extending that target rather than the portable suite is deliberate: it preserves
PRODUCT-0057's license and avoids implying the default suite can prove
iced/wgpu pixels, which it cannot.

**This is required even though §1's defect is shared by both adapters.** The
accepted screenshot is the evidence: operation-level assertions passed, both
adapters agreed, the exhibit was reviewed and signed off, and the figure was
still wrong. Agreement between recorders is not agreement with what renders.

Under 0057's classifier §1 is an assertion gap, so the SVG adapter is licensed
for the topology gate. §4 is not about this defect's class — it is about the
class of defect a topology gate still cannot see.

### 5. What this record does not do

No new path verbs. No change to `circle`, `rectangle`, `move_to`, `line_to`, or
`close`. **No kernel source changes at all** — pie and radial dial keep the calls
they already make; what changes is what those calls mean. No new authored
options, and no revisiting of pie's aggregation authority, which is step 17's.

## Implementation sequence

**Stage 0 — bank two red states.** The radial dial exhibit as it renders today
with its chord-closed rings, **and pie as it renders today with chord-bounded
segments** — plus the SVG export of both. Two marks, because §1 repairs two live
shapes and an earlier draft banked only one.

Stage 0 is **complete**: its measurements are in Context, and both original stop
conditions fired.

**Stage 1 — the contract.** §1 in both adapters, with the doc comment on
`PathBuilder::arc_to` stating it.

**Stage 2 — the topology gate.** §3.

**Stage 3 — the production oracle.** §4.

## Acceptance gates

1. **A two-arc annulus is one connected sub-path.** The **semantic** trace for a
   radial dial ring is `Move → Arc → Line → Arc → Close` — **one** move, and a
   line joining the arcs — and **both adapters produce it identically**. Against
   the banked sequence, which has two moves and no line. Physical realization is
   gate 3's, per §3.
2. **The caps are radial.** Both straight segments in the annulus point at the
   centre — the geometric statement of §1, and the property the screenshot
   showed violated. A gate asserting only sub-path count would pass a shape
   joined by an arbitrary line.
3. **Radial dial renders as annular sectors in the production canvas.** §4's
   oracle, against the banked chord rendering.
4. **The SVG export changes identically.** The exported path for the same figure
   gains the connecting line and loses the second `M`. Both adapters, since §1's
   premise is that they were wrong together — an SVG that still emits two `M`s
   means the contract landed in one place.
5. **`pie`'s `move_to(center)` becomes load-bearing.** Removing it changes pie's
   rendered output. Today it changes nothing, so this gate fails before §1 and
   passes after — the cheapest available proof that the connecting behaviour is
   real.
6. **A lone `arc_to` still moves.** With no open sub-path, `arc_to` starts one.
   Paired with gate 1, since a contract that always connects would join unrelated
   shapes into one path.
7. **The current point is the arc end.** A `line_to` immediately after an
   `arc_to` starts at the arc's endpoint, not at its centre or its start.
8. **Pie's pixels change to a centre-bounded sector.** Against the banked chord
   rendering, the repaired slice is measurably *smaller* — Stage 0's **12,336 →
   11,191** luminous pixels without AA. Inverted from an earlier draft, which
   asserted parity; the gate now fails if pie renders identically, because
   identical means the dead `move_to` is still dead.
9. **Pie's semantic trace gains the connection, before the arc.**
   `Move(center) → Line(arc start) → Arc → Close`.

   The line runs **from the centre to the arc's start**, and `close()` supplies
   the return from the arc's end back to the centre — together the sector's two
   radial edges. An earlier draft wrote `Move → Arc → Line → Close`, which is
   radial dial's shape — where the line is the *cap between two arcs* — misapplied
   to a single-arc mark. Under that trace a conforming adapter would fail while
   one appending the arc before its radial connection would pass.

   Pie's SVG path string necessarily differs from the bank — the formerly dead
   `M` becomes an `L` — so byte-identity is not claimed anywhere for pie.
10. **The curve-realization change is bounded and recorded.** Against the old
    topology, kurbo cubics at tolerance `0.1` differ from lyon quadratics by **76
    MSAA** and **25 no-AA** pixels. Asserted as a recorded value rather than a
    parity claim, so a future decomposition change is visible rather than
    absorbed.
11. **The tolerance is `0.1` and sits inside the stable band.** Tolerances 1.0
    through 0.01 render identically; 0.001 differs. Both halves, since a gate
    asserting only that `0.1` works would not notice the band moving under it.
12. **The production caller census stays exactly pie and radial dial.** No other
    kernel calls `arc_to`. Asserted structurally, because an earlier draft
    listed five more from a file-level string match that swept in test-double
    `PathBuilder` implementations — the census was wrong, and only a structural
    gate keeps it honest.
13. **Every kernel's draw-operation sequence is unchanged.** The calls each kernel
   makes are identical to Stage 0 — this record changes what `arc_to` means, not
   what any mark asks for.

## Stop conditions

> **Both original stop conditions have fired and are now decisions**, not
> conditions: connection changes pie's geometry (§ Context), and kurbo cubics
> change boundary realization independently. They are recorded here so a later
> reader sees that the record proceeded *because* they were measured, not despite
> being unchecked.

> If any mark other than pie or radial dial changes a pixel, stop. §2 fixes a
> seam with exactly two production callers; a third means the census is wrong
> again, and it has been wrong once already in this record.

> If the two Stage 0 effects cannot be measured **independently** in the
> implementation as they were in the probe, stop. Gate 8 and gate 10 attribute
> pixel movement to different causes, and a combined measurement would let either
> explanation absorb the other's error.

> If decomposing the arc requires a geometry authority beyond `kurbo`, stop and
> record the dependency as its own decision. §1 is implementable today only
> because `kurbo` is already a direct dependency; needing a new one makes this a
> dependency record wearing a contract repair.

> If the topology gate cannot record verbs without changing what kernels emit,
> stop. §3 observes the path; a gate that alters the thing it measures is not
> evidence.

> If the production oracle cannot be built without a new backend capability,
> stop and record the cost. PRODUCT-0093 §6 declined glyph measurement for this
> reason, and a visual oracle that needs the same door opened is a larger
> decision than this repair.

## Supersession

Supersedes no record. Corrects a misclassification made while surveying this
defect — it is an assertion gap, not the PRODUCT-0057 backend gap first
reported. Precedes step 16. Leaves every kernel's draw-operation sequence
untouched.

## Rejected

**Add an explicit `line_to` between the arcs in radial dial.** Attractive because
it fixes the visible defect in one kernel with no contract change. Rejected
because `PathBuilder` would still not say what `arc_to` does, the next annulus
would guess again, and pie's existing `move_to` would stay dead.

**Make `arc_to` always start a new sub-path, and add a separate connecting
verb.** Attractive because it preserves today's behaviour exactly and adds
capability beside it. Rejected because the current behaviour is the defect: every
existing two-arc call site wants connection, and the one kernel that wrote the
connecting form already assumed it.

**Classify this as a PRODUCT-0057 backend gap and require production-renderer
evidence for the topology fix.** Attractive because a screenshot found it and the
production renderer is where it was visible. Rejected because `export.rs:50-52`
emits its own `M`: the adapters agree, so the SVG is licensed for §3's topology
gate. §4 is added for a different reason, stated separately, rather than by
misfiling this defect.

**Rely on the topology gate alone and skip the production oracle.** Attractive
because §3 catches this defect precisely and cheaply. Rejected because §3 is
another operation-level assertion, and the accepted screenshot is direct evidence
that operation-level assertions passed while the figure was wrong. A gate class
that has just failed to be sufficient should not be the only reinforcement.

## Consequences

`PathBuilder` gains an answer to a question it never asked. Two implementations
had guessed the same way, which made the guess invisible: nothing disagreed, so
nothing surfaced.

The defect is the arc's clearest example of an **assertion gap** as PRODUCT-0057
defines it. Every operand was right. The draw list was right. What was wrong had
no representation in any test, because path topology had never been recorded —
and a gate can only catch what it can express.

It was found by looking at a picture, one exhibit after the mark became
authorable. That is the fourth defect in this arc surfaced by a new figure rather
than by a survey — after PRODUCT-0090's fabricated nulls, PRODUCT-0093's clipped
gradient, and PRODUCT-0097's faceted hover — and the second found *after* a
visual acceptance had already been given.

**Pie was found by Stage 0, and only because two stop conditions demanded
measurement instead of assertion.** The record had already claimed pie was
correct. Nothing in the codebase disagreed: pie has shipped its chord-bounded
segments for as long as it has existed, no assertion described its topology, and
no reviewer had reason to doubt a shape that looks like a pie slice at a glance.
It took rendering the alternative and counting pixels — 12,336 against 11,191 —
to show that the figure everyone had accepted was the wrong shape.

That is the strongest argument in this arc for stop conditions that require a
measurement rather than a judgement. A stop condition phrased as "if X, stop"
only helps when someone is obliged to find out whether X.
