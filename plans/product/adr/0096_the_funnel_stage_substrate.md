# ADR 0096: the funnel stage substrate

Status: proposed 2026-08-13. Funnel's preparation is infallible and produces
figures from data it has silently altered: null values are dropped while their
stage rows are kept, missing stages become invented `Stage {i}` labels, a null
stage becomes the literal text `null`, negative or rising values produce
geometry a funnel cannot mean, and hover can print `NaN%`. This record makes
funnel preparation fallible, refuses what it cannot honestly draw, and removes
the second width authority.

It is step 11 of `plans/wip/plot-catalog-restoration.md` and a prerequisite of
the funnel surface. It also gives PRODUCT-0097 a validated funnel kernel as a
parity subject.

## Context

### The fabricated-value defect, at its third site

PRODUCT-0088 §3 named it for heatmap and PRODUCT-0090 paid it for bubble. Funnel
carries a fourth variant of the same class and a fifth that is worse: it does not
merely fabricate a value, it **drops a row's value while keeping its label**, so
every later stage is drawn under the wrong name.

### What exists, read 2026-08-13

**Nulls drop from values but not from stages.** `values` collects through
`into_no_null_iter()` (`funnel.rs:195`), which skips nulls; `stages` collects
every row (`:184-194`). One null value therefore leaves `stages.len() >
values.len()` and shifts every subsequent label by one. Where a cast failure has
already emptied the stage column, the same misalignment inverts and
`self.prepared_data.stages[stage_idx]` (`:127`) indexes out of bounds.

**Missing and null stages are invented.** A missing stage column produces
`format!("Stage {}", i)` (`:165`, `:174`); a null stage reaches
`v.to_string().replace("\"", "")` (`:189`) and renders as the literal text
`null` — the same stringification defect PRODUCT-0090 §9 found in bubble's label.

**The width authority is injectable and floored.** `total_max: f32` is a stored
field (`:17`) consumed by paint (`:48-49`) and hover (`:123`), derived as
`values.iter().fold(0.0, f32::max).max(1.0)` (`:196`). The `.max(1.0)` floor
silently rescales any funnel whose values are all below 1. The field is also
constructed independently of any values — in the **production** empty-result path
(`:156`) and in a test oracle (`mark_style_oracles.rs:189`) — so it is a second
truth beside the values it summarizes.

**Conversion can divide by zero.** `let conversion = (val / prev_val) * 100.0`
(`:131`). With a zero stage followed by a zero stage, that is `0.0 / 0.0` — `NaN`,
printed as `NaN%`.

**Everything is `f32` and every label is rounded to an integer.** `values:
Vec<f32>` (`:16`), cast to `Float32` (`:180`), and both the in-mark label
(`:78`) and hover (`:128`) format with `{:.0}`.

**Row order is already preserved.** No sort appears in the kernel. §1 is
therefore a contract that fixes existing behaviour in place, not a repair.

## Decision

### 1. Relation row order is the stage order, and the mark never sorts

Stages render in the order the relation supplies. Funnel does not sort, and does
not reorder by value.

A funnel's vertical order **is** its meaning — it is a sequence of stages, not a
ranking — so ordering is the relation's to state. This matches PRODUCT-0082 §6,
which refused unordered input while naming `.sort(...)` as the author's remedy
rather than sorting inside the mark.

Today's behaviour already conforms; stating it prevents a later "helpful" sort.

### 2. Stage names are non-null and unique

A null stage **refuses**. A duplicate stage name **refuses**.

Uniqueness is key identity, exactly as PRODUCT-0088 §1 decided for heatmap cells:
two rows claiming the same stage make the figure depend on row order in a way the
author cannot see, and a funnel with two "Checkout" stages has no defined
geometry.

**A literal `"null"` string is a valid stage name.** It is data, not absence, and
the two must be distinguishable — a fixture exists for exactly this, because an
implementation that stringifies before checking nullity passes every other gate.

### 3. Values are non-null, finite, and non-negative

Each refuses on its own terms, attributed to the offending row. A funnel stage is
a count or magnitude; a negative one has no width to draw.

### 4. The first value is positive, and later values are non-increasing

A funnel narrows. Rising values refuse, because the geometry would widen and the
figure would assert a conversion above 100%.

**Equal plateaus are valid** — non-increasing, not strictly decreasing. A stage
that loses nobody is a real and common result.

**A final zero is valid.** A funnel that ends in total loss is meaningful.

**The first value must be strictly positive**, which is what keeps §5's total rate
defined and §7's width derivable.

### 5. A stage following zero renders `Conversion: —`

Once a stage is zero, the stage after it has no defined conversion: `0 / 0` is
the `NaN%` at `funnel.rs:131`. It renders as an em dash, not as `0.0%`, because
zero percent is a measurement and this is its absence.

**Total rate stays defined** for every stage, because §4 guarantees the first
value is positive. The two rates fail independently, and only one of them can.

### 6. An empty schema-valid relation is a valid empty result

Zero rows with both columns present and castable yields **no marks and no
legend**, and fabricates no stages.

An empty relation missing a required column still **refuses** under §8 —
emptiness is not a licence to skip schema validation, the distinction
PRODUCT-0092 gate 13 made discriminating.

### 7. Width derives from the validated first stage; `total_max` is removed

The stored `total_max` field (`funnel.rs:17`) is deleted. Width comes from the
first stage's value, which §4 guarantees is positive.

Two reasons. It is a **second truth**: a stored maximum can disagree with the
values it summarizes, and it is already constructed independently of any values, in production at
`:156` and in a test oracle at `mark_style_oracles.rs:189` —
the same shape PRODUCT-0084 and PRODUCT-0089 §1 each removed. And its `.max(1.0)`
floor (`:196`) silently rescales any funnel whose values are all below 1, so a
funnel of conversion *fractions* renders narrower than the same shape scaled up:
`[0.8, 0.4, 0.1]` fills **64%** of the bounds where `[80, 40, 10]` fills **80%**
— four-fifths as wide.

Using the first stage rather than the maximum is equivalent under §4 — the first
value **is** the maximum when the sequence is non-increasing — and it says so by
construction instead of by scan.

### 8. Column dtypes, exact conversion, and label formatting

**Dtype admission is explicit.** `stage` admits the supported **nominal** dtypes —
`String`, `Categorical`, `Enum`, `Boolean` (`spec/mod.rs:2484-2487`) — and a
**numeric** stage column refuses. `value` admits the **quantitative** dtypes.

This matters because `FunnelStages::from_rows` receives strings: preparation is
the last place a numeric stage could be silently stringified into a plausible
label. The refusal is paired with a **valid `Boolean` or `Categorical` stage**
fixture, so it cannot be satisfied by rejecting every non-`String` column.

A missing or uncastable column refuses, attributed to channel and field with no
row, as PRODUCT-0090 §4 and PRODUCT-0092 §3 settled. The `Series::new_empty`
substitutions (`:180-183`) are removed.

**Conversion to `f64` must be exact, and `f64` alone does not guarantee it.**
Casting `2^53 + 1` to `f64` silently yields `2^53`, so a widened type is not a
correctness contract by itself.

PRODUCT-0082 already built the authority: `exact_f64` and `exact_signed`
(`kernels/boxplot.rs:395-404`). **It is extracted and shared, not reimplemented** —
two exactness rules for the same question would be the second-authority defect
this arc has removed four times.

An author who wants ordinary floating approximation can cast to `Float64`
upstream, which states the intent in the relation rather than hiding it in a
mark.

**Labels use `format_label`** (`common.rs:2133`), not `{:.0}` (`:78`, `:128`).

The claim is **standard quantitative formatting instead of forced integer
formatting** — not exact source spelling. `format_label` deliberately uses one
decimal place, so `0.25` renders as `0.2`. Preserving the author's exact spelling
would need a different formatter and a larger decision about numeric presentation
across every mark; this record adopts the shared one, and its third stop
condition guards the boundary.

### 9. One validated stage collection

Following PRODUCT-0088 §2, PRODUCT-0090 §5, and PRODUCT-0089 §4:

```rust
prepare_funnel_data(...) -> Result<FunnelPreparedData, FunnelPreparationError>
FunnelStages::from_rows(fields, rows) -> Result<FunnelStages, FunnelStagesError>
```

Two layers for PRODUCT-0090 §5's reason: column and cast faults are discovered
while producing rows, so `from_rows` cannot own them. The preparation error wraps
the stage error unchanged. `from_rows` takes the field names so its error can
name channel, field, and 1-based row.

`FunnelStages` is **opaque** — its stages and its derived width are one authority,
per PRODUCT-0090 §6 — with a trybuild compile-failure test and a valid
construction twin.

**Refusal precedence**, one diagnostic per malformed relation, in three phases:

| phase | order | error | attribution |
|---|---:|---|---|
| preparation | 1 | `stage` column missing | channel + field |
| preparation | 2 | `stage` column not a supported nominal dtype | channel + field |
| preparation | 3 | `value` column missing | channel + field |
| preparation | 4 | `value` column not quantitative | channel + field |
| row-local | 5 | null stage | field + row |
| row-local | 6 | null value | field + row |
| row-local | 7 | non-finite value | field + row |
| row-local | 8 | inexact `f64` conversion | field + row |
| row-local | 9 | negative value | field + row |
| sequence | 10 | duplicate stage | the **stage name**, and both rows |
| sequence | 11 | first value not positive | field + row 1 |
| sequence | 12 | rising value | field + **both** rows |

**Preparation is field-first, not fault-kind-first.** All of `stage`'s faults are
checked before any of `value`'s, so a relation whose stage column is missing
*and* whose value column is the wrong dtype reports the **stage**. Ordering by
fault kind instead would make the reported column depend on which kind of fault
happens to exist elsewhere — the same reasoning PRODUCT-0092 §5 used for its
per-field precedence.

Then rows scan in producer order, stage faults before value faults within a row.

**The sequence phase is entirely cross-row**, including duplicate detection — an
earlier draft called only the ordering rules cross-row, which was wrong: a
duplicate is a fact about a pair, exactly as PRODUCT-0088 §2 established for
heatmap keys. "First value not positive" concerns one row but is a property of
the sequence, so it belongs to the phase that reads the sequence.

### 10. What this record does not do

No Rune word — that is PRODUCT-0098. No faceted hover repair — PRODUCT-0097 owns
the coordinate seam, and this record must not compensate for it kernel-locally.
No colour: funnel's stage colours are the surface's, along with the authored
family. No legend. No aggregation.

Funnel remains producerless.

## Implementation sequence

**Stage 0 — bank.** Every current funnel figure. As with heatmap and bubble this
is a **refactor oracle, not user pixels** — funnel has no production producer, so
nothing banked is user-reachable. The malformed fixtures are banked as **negative
controls**: their current renders are the red states this record converts into
refusals.

**Stage 1 — the collection.** `FunnelStages`, `FunnelStagesError`, §9's
precedence, with no consumer migrated.

**Stage 2 — the boundary.** Planning calls `prepare_funnel_data` and returns
diagnostics; the factory re-prepares and `expect`s.

**Stage 3 — the kernel.** Funnel consumes the validated collection; `total_max`
is deleted, `into_no_null_iter` and the `Series::new_empty` substitutions are
removed — not guarded.

**Stage 4 — arithmetic and labels.** `f64`, `format_label`, and §5's em dash.

**Stage 5 — re-bank.** The post-0096 parity bank that PRODUCT-0097 and
PRODUCT-0098 consume.

## Acceptance gates

1. **A null value refuses; it does not shift labels.** The banked red state is a
   figure whose stages after the null carry the wrong names. Both halves: the
   refusal, and the absence of any render.
2. **A null stage refuses, and the literal `"null"` does not.** Two fixtures. An
   implementation that stringifies before checking nullity accepts both and fails
   here — the discriminating pair for §2.
3. **A duplicate stage refuses**, naming the stage and both rows.
4. **Non-finite and negative values refuse**, each naming its row. `NaN`,
   `INFINITY`, `NEG_INFINITY`, and `-1.0` — four cases.
5. **A finite twin does not refuse.** A very large finite value constructs, so
   gate 4 cannot pass by rejecting everything unusual.
6. **Rising values refuse, naming both rows.** `[100, 50, 60]` refuses and names
   stages 2 and 3, since a reader needs the pair to see the rise.
7. **Equal plateaus construct.** `[100, 100, 50]` renders. Paired with gate 6,
   since "non-increasing" and "strictly decreasing" differ by exactly this case.
8. **A final zero constructs.** `[100, 40, 0]` renders.
9. **A non-positive first value refuses.** `[0, 0]` refuses at row 1, since §7's
   width and §5's total rate both depend on it.
10. **A stage after zero renders `Conversion: —`.** On `[100, 0, 0]`, the third
    stage's hover shows an em dash — not `0.0%`, and not `NaN%`, which is the
    banked red state from `funnel.rs:131`.
11. **Total rate stays defined everywhere.** On the same fixture every stage
    prints a numeric total rate, because §4 guarantees a positive first value.
    Paired with gate 10: the two rates must fail independently.
12. **Row order is preserved exactly.** A relation whose stages are not in
    descending alphabetical or value order renders in **relation order**. No sort
    appears in the kernel — asserted structurally as well, since a value-order
    fixture can coincide with relation order by accident.
13. **`total_max` is gone, and sub-unit funnels scale correctly.** The field does
    not exist; a funnel of `[0.8, 0.4, 0.1]` fills the same proportion of its
    bounds as `[80, 40, 10]`. The banked red state is the `.max(1.0)` floor
    (`:197`) rendering the first at one-eightieth width.
14. **Labels use standard quantitative formatting, not forced integers.**
    `[0.5, 0.2]` prints `0.5` and `0.2`, against the banked `{:.0}` output of `0`
    and `0`. The fixture avoids `0.25`, which `format_label` renders as `0.2` by
    design (`common.rs:2133`) — the claim is the shared formatter, not exact
    source spelling.
15. **Conversion to `f64` is exact.** The discriminating trio: `2^53` accepted,
    `2^53 + 1` **refused**, `2^53 + 2` accepted. The middle case is the only
    integer in that neighbourhood `f64` cannot represent, so a gate testing only
    a large value passes a plain cast.
16. **The exactness authority is shared, not duplicated.** Funnel's conversion
    routes through PRODUCT-0082's `exact_f64` (`boxplot.rs:395`); no second
    exactness rule exists. Structural, per §8.
17. **Stage dtype admission is a pair.** A numeric stage column **refuses**; a
    `Boolean` or `Categorical` stage column **constructs**. Both halves, since
    refusing every non-`String` column satisfies the first alone.
18. **The `f32` → `f64` geometry change is accepted explicitly.** Normalizing in
    `f64` and realizing to `f32` changes rendered widths: `[3, 1]` in a 100 px
    bound yields `0x41d55555` where the legacy `f32` path yields `0x41d55557`.
    This gate records the new value as intended rather than leaving it to surface
    as a parity failure.
19. **A missing column refuses without a row**, and no `prepare_funnel_data`
    render follows.
20. **An empty schema-valid relation succeeds with no marks and no legend**, and
    fabricates no stages. Paired with **an empty relation missing a column, which
    refuses** — the discriminating pair, since an implementation short-circuiting
    on `height == 0` passes the first alone.
21. **The collection cannot be built by literal.** A trybuild compile-failure case
    paired with a valid `from_rows` twin.
22. **Precedence is deterministic across all three phases.** Four pairs: a missing
    column with a null row reports the **column**; a row with a null stage and a
    null value reports the **stage**; a relation with both a duplicate and a rise
    reports the **duplicate**; and — the discriminator §9 added — a relation whose
    **stage column is missing and whose value column is the wrong dtype** reports
    the **stage**, proving preparation is field-first rather than
    fault-kind-first.
23. **Valid figures are unmoved except where two gates say otherwise.** Every
    banked well-formed funnel is byte-identical to Stage 0, excepting exactly two
    deliberate changes: the label formatting pinned by gate 14 and the `f64`
    geometry pinned by gate 18. Naming both is the point — an unqualified parity
    claim would be false, and a vague exception would let an unrelated drift
    through.
24. **The fallbacks are gone, not guarded.** `into_no_null_iter`,
    `Series::new_empty`, and `format!("Stage {}", ...)` do not appear in funnel's
    preparation. Structural, because a guarded fallback passes every value-level
    gate while leaving the defect one refactor away.

## Stop conditions

> If refusing a rising value breaks a shipped fixture that is not demonstrably
> malformed, stop. That fixture is evidence the mark is being used as something
> other than a funnel, which is a surface question and not one to settle in
> preparation.

> If the faceted hover defect must be touched to make any gate here pass, stop.
> PRODUCT-0097 owns the coordinate seam and needs this kernel as a **parity
> subject**; a kernel-local compensation would corrupt the baseline it is
> supposed to provide.

> If a fixture requires precision `format_label` does not offer — exact source
> spelling, or more than one decimal — stop. §8 deliberately adopts the shared
> formatter rather than introducing a second one, and changing numeric
> presentation is a product-wide decision, not a funnel one.

> If deriving width from the first stage disagrees with the maximum on any valid
> fixture, stop. Under §4 they are the same number; a disagreement means the
> non-increasing rule is not being enforced where §7 assumes it is.

## Supersession

Supersedes no record. Extends the fabricated-value class PRODUCT-0088 §3 named
and PRODUCT-0090 paid, at its third site. Adopts PRODUCT-0090 §5's two-layer
shape and §6's opacity, and PRODUCT-0088 §1's key-uniqueness reasoning. Blocks
PRODUCT-0097, which needs a validated funnel kernel, and PRODUCT-0098. Touches no
ledger item.

## Rejected

**Drop null-valued rows and their stages together.** Attractive because it
"fixes" the misalignment without refusing anything. Rejected because a funnel with
a silently removed stage misstates every conversion rate that spans it — the
figure would be arithmetically consistent and factually wrong, which is worse
than a refusal.

**Treat a rising value as a new funnel segment or clamp it to its predecessor.**
Attractive because real relations contain rises and refusing is inconvenient.
Rejected because both inventions assert something the data does not say: a clamp
prints a conversion of exactly 100% that was never measured, and segmentation
invents a boundary the author never authored.

**Render `Conversion: 0.0%` after a zero stage.** Attractive because it avoids a
special case. Rejected because `0 / 0` is undefined, not zero, and printing a
measurement where none exists is the defect PRODUCT-0087 repaired in the gradient
guide.

**Keep `total_max` but validate it against the values.** Attractive as a smaller
change. Rejected because a gate that checks two authorities agree is standing in
for a decision that should remove one of them — the same reasoning PRODUCT-0094
applied to fields and origins.

**Keep `f32` to avoid touching arithmetic.** Attractive because funnel values are
usually counts. Rejected because every other quantitative kernel is `f64` after
PRODUCT-0089 §5, and a lone `f32` mark is a second numeric contract nobody will
remember.

**Sort stages by descending value so the funnel always narrows.** Attractive
because it makes §4 unfalsifiable. Rejected because a funnel's order is its
meaning: sorting would silently reorder a sequence the author supplied and turn a
data error into a plausible-looking figure.

## Consequences

Funnel preparation becomes fallible, and five kinds of silence become
diagnostics. Some relations that render today will refuse, which is the point —
each currently produces a figure containing stages, labels, or rates that were
never measured.

The label-shift defect is the arc's clearest example of why fabrication is worse
than failure: a dropped null does not merely lose one observation, it renames
every stage after it, and the figure gives no sign.

Two authorities disappear. `total_max` stops being storable beside the values it
summarizes, and its `.max(1.0)` floor stops rescaling sub-unit funnels.

PRODUCT-0097 gains a validated parity subject, which is why this record precedes
it. The second stop condition exists to keep that subject clean: a kernel that
compensated locally for the coordinate seam would be a baseline that already
contains the bug being fixed.
