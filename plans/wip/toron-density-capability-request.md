# Request: a Toron density capability

Status: request, 2026-08-14. **Not a ket record** — this asks for a Toron-owned
capability whose Rune surface, planning, execution, and relation output belong in
`nilket/crates/toron`. Likely Toron ADR 552, though 0551
(`fusion_invariant_base_relation_attribution`) is active without its `impl:`
commit, so the number cannot be claimed yet.

Ket references this **by title** until it lands, then consumes it from the violin
substrate and surface records.

## Why ket is asking

Violin is the last mark in `plans/wip/plot-catalog-restoration.md`, and it is
where the standing rule **"marks render; relations compute"** finally costs
something.

Every prior invocation in this arc was cheap, because the capability already
existed:

| mark | needed | already registered? |
|---|---|---|
| pie (PRODUCT-0102) | `.aggregate([...], [sum(...)])` | yes |
| box plot (PRODUCT-0081–0083) | `approx_quantile` | yes |
| **violin** | **density** | **no** |

So the rule has been enforced nine times at the price of a diagnostic and an
exhibit. Here it is the price of an engine feature, and ket is asking rather than
quietly keeping the computation.

**Both cheap escapes were considered and refused.** A **stepped histogram** —
expressible today with `ntile` or bucket arithmetic — is not a restored violin; it
is a different mark. And **keeping the KDE in Polariton** preserves exactly the
statistics-in-renderer authority this arc removed from pie, funnel, radial dial,
heatmap, and bubble.

## What Polariton does today, and why it is not defensible

`violin.rs:350-368` builds a fixed-bin density grid per violin via
`compute_kde(&y_slice, tex_height_bins, y_min, y_max, bandwidth)`.

**The bandwidth is `(y_max - y_min).max(0.001) * 0.03`** (`violin.rs:363`) — three
percent of the **global** value range, identical for every series in the figure
regardless of its spread or its row count.

That is not a bandwidth derived from data. A tight distribution and a diffuse one
receive the same smoothing, and adding an outlier to any series rescales the
smoothing of **every** series. Moving the computation relation-side would be the
first time violin's smoothing parameter came from a statistic at all.

## What Toron already has

`rune_engine.rs:1515-1535` registers `approx_quantile`, `linear_regress`,
`count_all`, `count`, `sum`, `avg`, `min`, `max`, and a full window set (`rank`,
`row_number`, `dense_rank`, `ntile`, `lag`, `lead`, `first_value`, `last_value`,
`nth_value`).

**Quartiles therefore need nothing new.** Toron ADR 0546 established stable
grouped approximate quantiles and 0547 established Tukey summary **relation
composition** — which is the precedent this request is shaped against.

**Absent**: `stddev`, `variance`, and any density or bucketing function.

## What ket is asking for

**Relation rows**, not a scalar aggregate:

```text
group | evaluation value | density
```

One row per (group, evaluation point). Following ADR 0547's shape — a summary
*relation* the mark consumes — rather than a function returning an opaque blob.

Ket consumes it as ordinary rows: the violin substrate validates them the way
PRODUCT-0104 validates multivariate rows, and the kernel renders what it is given.

## Decisions this capability record owns

Ket has opinions but no standing to decide these; they are engine semantics.

**Bandwidth, per series, from observed data.** Whatever rule is chosen —
Silverman, Scott, a stated alternative — it must derive from *that series'*
statistics, not the figure's global range. This is the request's substantive ask:
the current constant is the defect, and relocating it unchanged would move a
fabrication rather than remove one.

**Degenerate series.** A single row, all-identical values, or zero variance each
have no meaningful bandwidth. Whether they refuse, emit a spike, or emit nothing
is a policy this record should state — ket's substrate will enforce whatever it
says, but cannot invent it.

**The evaluation grid.** How many points, over what extent, and whether the extent
is per-series or shared across groups. Shared extent makes violins comparable;
per-series extent makes each one resolved. Ket has a preference for **shared**,
since a violin plot's whole purpose is comparison across categories, but the
capability may have reasons to differ.

**Kernel choice**, and whether it is authored or fixed.

**Exactness and non-finite handling**, consistent with whatever `approx_quantile`
already does — ket's marks refuse non-finite values, so rows carrying `NaN`
density would be refused at the substrate rather than rendered.

## What ket is not asking for

No change to `approx_quantile` or to ADR 0546/0547's summaries — violin's box
component is already served. No histogram or bucketing capability; the stepped
alternative was refused above. No rendering concern: extents, scaling, and
violin geometry stay in Polariton.

## What ket will do once it lands

0. **Re-survey the delivered relation schema first.** Do **not** draft the violin
   substrate from the shape requested above. The response columns, their types,
   the evaluation-grid semantics, and the degenerate-series policy are **Toron's
   decisions**, and this document states an ask, not an outcome.

   This is a live hazard rather than a formality. Ket has three times in this arc
   treated a proposed or grepped shape as a delivered one — a test-only
   `"aggregate"` assay cited as a production registration, a `head`-truncated
   census reported as complete, and `MarkFields::all()`'s seven-slot array read as
   a property rather than a ceiling. A request document is the ideal artifact for
   that mistake, because its requested shape reads exactly like a specification.

1. **Violin substrate** — consume prepared summaries and density rows **as
   delivered**, validate them, remove `compute_kde` and the `0.03` constant from
   the kernel.
2. **Violin surface and live exhibit** — the authored spelling, and an exhibit
   demonstrating the relation-side pipeline end to end, as PRODUCT-0103's pie
   exhibit demonstrated `.aggregate(...)`.

Until then violin renders as it does today, and the plan carries this as an
unnumbered external dependency.

## One note on sequencing

Toron ADR 0551 is active without its adjacent `impl:` commit. Ket is not blocked
in the meantime — twenty-one of twenty-three catalog steps are landed and violin
is the only remaining mark — so this request is not urgent, only next.
