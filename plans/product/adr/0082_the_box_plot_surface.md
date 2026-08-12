# ADR 0082: the box plot surface

Status: proposed 2026-08-12. This record makes the box plot authorable in Rune
over the Tukey substrate PRODUCT-0081 established, as seven explicitly named
columns. It repairs the box body's legacy ramp fill, adds no legend, allocates no
colour identity, and changes no persistence schema.

## Context

PRODUCT-0081 reshaped the kernel to `TukeyBox` with private fields and a
validated constructor over ADR 547 rows, removed the raw aggregation authority,
and installed a producerless typed refusal. It deferred exactly two things to
this record: the spelling with its column mapping, and the box body fill at
`plot/kernels/boxplot.rs:96`, still `settings.color_theme.get_color(t)`.

Outlier point styling is **not** deferred — PRODUCT-0081 §5 pinned radius `3.0`,
`mark_style.fill`, and a `mark_style.decoration` outline at width `1.0`, together
with the band-axis offset geometry. This record does not reopen it.

## Decision

### 1. Seven explicit columns, positionally bound

```rune
box(
    "category",
    "whisker_low",
    "q1",
    "median",
    "q3",
    "whisker_high",
    "outlier",
)
```

Every column is named at the call site. There is no convention-based lookup, no
prefix matching, and no partial form.

**The reason is that ADR 547 permits authored aliases.** Its composition is
ordinary SQL, so an author may name the projected columns anything —
`lo`/`hi`, `p25`/`p75`, or names carried from an upstream view. A mark that went
looking for a column called `whisker_low` would work on the record's example
query and fail, or worse silently mis-bind, on any relation that spelled it
differently. That is hidden inference: behaviour depending on a coincidence
between the author's SQL and the mark's expectations, with nothing at the call
site to show it happened.

Seven arguments is verbose, and the verbosity is the honest cost of every binding
being visible. This is the same posture PRODUCT-0078 took in refusing
`histogram(value)`: the shorter spelling was available and bought its brevity
with an inference the record would not make.

### 2. The outlier sentinel is a row property, not a schema property

ADR 547 represents a category with no outliers as a single row whose `outlier` is
null, and PRODUCT-0081's ingestion reads that null as the no-outlier sentinel.

**A nullability requirement is not implementable here, and an earlier draft of
this section asserted one.** Toron's Arrow batches are decoded into a Polars
`DataFrame` (`adapters/toron/polars_compat.rs:12`), and resolution sees only
`Schema<DataType>` — field nullability does not survive the decode. A
non-nullable `Float64` column and a nullable one containing no nulls are
indistinguishable at the point the check would run, so the gate would either
pass vacuously or refuse valid input.

The contract is therefore stated over rows:

- `outlier` must be **quantitative**;
- a **null** value means that category has no outliers;
- a result in which **no** row is null is valid — it simply means every category
  has at least one outlier.

Nothing is lost by the change. The case the schema rule was reaching for — a
relation that later grows a no-outlier category — is caught by the same row
semantics the moment such a row appears, and PRODUCT-0081 §8 already refuses the
genuinely malformed shapes (two null rows for one category, null and non-null
coexisting).

### 3. The ingestion and axis contract

`BoxPlotPreparedData` requires `y_axis_type`, `x_label`, and `y_label`
(`plot/kernels/boxplot.rs:244-249`), none of which the mark can leave unstated.
The full contract:

| element | decision |
|---|---|
| `category` | any dtype that resolves as categorical; a **null** category refuses at that argument's span — a box with no category has no band |
| five summary columns | **quantitative and non-null in every row**; a null refuses, because PRODUCT-0081's `TukeyBox` has no representation for an absent quartile |
| `outlier` | quantitative; null is the §2 sentinel |
| conversion | **lossless to `f64`**; an integer or decimal that cannot be represented exactly refuses at its argument's span, naming the row |
| `x_label` | the **category argument's column name**, matching how every other mark labels a categorical axis |
| `y_axis_type` | **`AxisType::Linear`**. Each value column is admitted independently as quantitative; no cross-column dtype agreement is required |
| `y_label` | **empty** — see below |

**Conversion is lossless, not merely non-saturating.** `TukeyBox` stores `f64`
while §3 admits every quantitative dtype, so the two disagree above `2^53`:
`2^53 + 1` is a perfectly good `i64` and has no exact `f64`. "Checked, never
saturating" does not decide that case — it would silently round, and a box plot
that reports a quartile one unit away from the stored value is misreporting a
statistic. Exact values render; a value that cannot round-trip refuses. The
alternative — accepting a documented rounding — was rejected because the whole
arc's position is that a mark reports what the database holds.

**Exact dtype agreement across the summary columns is not required**, and an
earlier draft demanded it. It would reject harmless shapes — integer whiskers
beside floating quartiles, or columns that took different casts through the same
SQL — while buying no behaviour: `AxisType` admits `Linear`, `Percent`, `Date`,
`Datetime`, and `Time`, and every quantitative dtype this mark accepts maps to
`Linear` after checked `f64` conversion. A rule that refuses valid input and
changes no output is a rule that only costs.

**The value-axis label is a real decision, and the honest answer is to leave it
blank.** The relation carries five summary columns and an outlier column; it does
**not** carry the name of the measure they summarize. Every alternative asserts
something untrue: labelling the axis `median` names one of six things it shows,
and picking any other column name is worse. An exported figure with a blank value
axis is incomplete; one labelled `median` is wrong, and the arc has consistently
preferred the former.

This is the **derived-scale axis title** question arriving with its first
concrete consumer — the item docketed since the percent charts labelled their
value axis `revenue` while ticks read `0%…100%`. It also fires the ledger's
axis-label-authoring trigger, whose stated condition was the first real request
for prose axis titles. Both belong to that record, not this one; stop condition 5
names the hand-off.

### 4. The body fill moves to `mark_style.fill`

`settings.color_theme.get_color(t)` (`:96`) is replaced by
`settings.mark_style.fill`.

**`mark_style.fill` is not a resolved colour assignment**, and this section does
not discharge PRODUCT-0064 by itself. That obligation is met the way
PRODUCT-0079 met it for the histogram: **§§4 and 8 jointly** — this section
removes the legacy bypass, and §8 establishes that the mark allocates no colour
identity, so there are structurally **no assignments to consume**. Claiming the
fill *is* a resolved assignment would misdescribe the tier, which resolves a
Polariton-chosen default rather than an authored encoding.

Decoration and outlier styling are unchanged: PRODUCT-0080 migrated the box's
structure to `mark_style.decoration`, and PRODUCT-0081 §5 settled the points.
After this record the mark draws entirely from the mark-style tier and samples
the legacy catalog nowhere.

### 5. One fill for every category, and no legend

Every box in a figure takes the same `mark_style.fill`. The fill does not vary by
category, and no palette is consumed.

Category is already encoded on the categorical axis. Varying fill by category
would encode it twice — the argument PRODUCT-0079 §1 used to remove
count-as-colour from the histogram — and would additionally make the mark a
palette consumer, amending PRODUCT-0030 §5 to give one mark a second encoding of
a variable already carried by position.

No legend follows from that rather than being decided separately: with no colour
encoding there is nothing to disambiguate, which is PRODUCT-0044 §1's omission
rule, and PRODUCT-0044 §5 already reaches the same outcome for band-fill bars on
the same reasoning — the axis is the guide.

### 6. Refusals are attributed by scope

With seven arguments, "a column is wrong" is not a diagnostic. But not every
failure belongs to one argument, and an earlier draft demanded per-argument
attribution for all of them while its own gate placed cross-field invariants at
the mark. The split, by **scope of the failing rule**:

| scope | attributed to | examples |
|---|---|---|
| a single field | that **argument's span** | a missing column; a non-quantitative `q1`; a null in a summary column; a null `category`; an unrepresentable conversion |
| across fields or across a category's rows | the **complete `box(...)` span** | `q1 > median`; an outlier inside the whisker span; disagreeing summaries; coexisting null and non-null outlier rows |

The rule is that a diagnostic points where the fix is. `q1 > median` does not
indict either column — the author must look at both, and probably at the SQL that
produced them — so pointing at one would send them to the wrong place.

**Per-argument attribution needs a mechanism this record must fund.** Today the
rewritten call injects a single origin and `RuneMark` retains only that
(`rune_presentation.rs:838`), so seven distinct argument spans do not exist at
execution time. Stage 2 therefore **retains the seven argument use-site origins
through execution into the sink**; without it, gate 12 is unimplementable and the
single-field column of the table above degrades to the whole-mark span.

### 7. Empty modifier register, vertical orientation only

The mark's closed modifier register (PRODUCT-0046 §1) is **empty**. `bar_width`
and `category_gap` are the candidates by resemblance to `bar`, and both are
declined here rather than left ambiguous: box geometry is not authored in this
cut.

Orientation is vertical only. PRODUCT-0055 resolves bar orientation from the
channel pair, but seven positional arguments have no ambiguous pair to resolve,
so a horizontal box plot needs its own decision and has no requester.

### 8. No colour identity, and no persistence schema change

Verified rather than inherited by analogy. `resolve_explicit_band_fills`
(`plot/spec/mod.rs:2658`) returns empty unless the kernel fields are `Bar` or
`StackedBar`; a box plot's are neither. The mark takes no Color channel per §5.
So no assignment and no allocation space is created, `DurableLayer::from_runtime`
is never reached, and `polariton_session` stays at v2 with no new
`AuthoredFamily` variant — `BoxPlot` appears nowhere in `session_persistence.rs`
today and does not need to.

This is the same chain PRODUCT-0079 §7 established for the histogram, and it
holds for the same reason: a mark that encodes no colour allocates no colour.

### 9. Faceting composes through the existing categorical grid

A box plot's x channel is categorical, so PRODUCT-0041's grid and PRODUCT-0065's
shared categorical domains apply unchanged. No facet-specific work is added, and
none is needed: the band-axis offset geometry of PRODUCT-0081 §4 reads
`band_width` from the transform, which a faceted cell already supplies.

### 10. What this record does not decide

**No relation-side Tukey helper.** Writing ADR 547's four-CTE composition by hand
is real friction, and a helper that emits these seven columns would remove it.
That is a database or relation-surface decision with its own semantics, and
inventing it inside a mark record would repeat the mistake PRODUCT-0078 refused
when it declined to let `histogram` bin. A future helper shortens the SQL and
emits the same seven columns **without changing `box(...)` semantics**, which is
precisely why the mark can be specified now.

**The accepted positions are enumerated.** "A figure body and nowhere else"
was too loose — read literally it excludes the canonical
`relation.views([box(...)])`, which is how every mark in this language is
actually written. The exact set:

| position | |
|---|---|
| `relation.views([box(...)])` | **accepted** — the canonical form |
| `view("name", box(...))` | **accepted** — a direct named mark |
| `view("name", figure([… box(…) …]))` | **accepted** |
| `figure([… box(…) …])` | **accepted** |
| `layers([… box(…) …])` | **refused** — PRODUCT-0013 §2's unresolved scale composition, as PRODUCT-0079 scoped `histogram` |
| legacy `plot.box(relation, …)` | **refused** — no legacy position is added |
| `relation.box(…)` sugar | **refused** — PRODUCT-0008 §6, spellings are earned by use |

The direct named mark is accepted rather than refused because the wrapper
already takes one: `view(...)` is defined over "a name string and one sink"
(`rune_view_compiler.rs:563`), so requiring `view("name", figure([box(…)]))`
would be a **box-only exception** to a general rule, and this record has no
reason to make one.

Gate 3 asserts every row, accepted and refused, because a scope stated in prose
and gated only on the canonical form leaves the three refusals unproven.

No horizontal orientation, no authored box geometry, no authored colour for the
mark, and no change to outlier styling.

**No Arrow-native result seam — but this record is its concrete trigger.** §2
had to weaken a contract for a reason that is not about box plots: Polariton's
adapter converts Toron's Arrow IPC batches into a Polars `DataFrame`
(`adapters/toron/polars_compat.rs:12`) before resolution runs, and the resulting
artifact carries no field nullability. **The distinction is unavailable at the
point the mark needs it.**

Stated that narrowly on purpose. Arrow IPC *can* carry nullability, but nothing
here has yet shown that Toron emits a meaningful nullable/non-nullable
distinction — proving that is the Arrow-seam record's Stage 0, and claiming the
database "had" information we have not looked for would be the same
unverified-upstream assumption that produced the `approx_quantile` error.

The repair is an Arrow-native result artifact behind a Polariton data interface,
with Polars demoted from the application's truth boundary to an optional
computation adapter — not a second bespoke dataframe engine, since Arrow should
own the representation and Polariton only the product-facing wrapper. That is
predominantly viz-team work in this repository: the database team's part is
preserving and documenting the public Arrow result contract, and Stage 0 of that
record should first verify the schema and batches are already faithfully on the
wire, in which case the repair is entirely ours. The joint acceptance property is
that **a nullable and a non-nullable field remain distinguishable end to end**.

It is cited here rather than attempted here, and stop condition 4 names what
would escalate it.

## Implementation sequence

**Stage 1 — the fill repair.** `:96` moves to `mark_style.fill`, before any word
exists, so the mark is theme-correct before it is authorable.

**Stage 2 — resolution and spelling.** Resolved kernel fields for the seven
columns and the `box` word, replacing PRODUCT-0081's typed refusal.

Seven fields is more than any existing mark carries, and the projector today
knows `x`/`y`/`color` plus the histogram's `end`. Every stage below must be
widened, or five box fields resolve correctly and are then **projected away** —
a failure that looks like a kernel bug and is a plumbing one:

1. `AuthoredPlotSpec` — carries seven authored field positions;
2. channel resolution — binds all seven against the returned schema;
3. `project_named_channels` — projects all seven, not the x/y/color triple;
4. prepared-contract validation — admits the box shape;
5. the kernel factory — constructs `TukeyBoxes` from the projected columns.

Stage 2 also **retains the seven argument use-site origins** through execution
into the sink, per §6; gate 12 is unimplementable without it.

**Stage 3 — the exhibit.** A `uat` buffer over `box_probe`.

## Acceptance gates

1. **The word resolves.** `box(...)` with the seven columns compiles through the
   production path and produces the `TukeyBox` sequence PRODUCT-0081 validated,
   asserted against a real `uat` buffer as PRODUCT-0076's gate did.
2. **No convention lookup exists.** A relation whose columns are named `lo`,
   `p25`, `mid`, `p75`, `hi` binds correctly when those names are passed
   positionally, and nothing in the resolution path matches on the literal string
   `whisker_low`. The renamed-column fixture is the discriminating half — a
   convention-based implementation passes gate 1 and fails this.
3. **Every surface position resolves as §10 enumerates.** All seven rows:
   `relation.views([box(...)])`, `view("name", box(...))` as a direct named mark,
   `view("name", figure([...]))`, and a bare `figure([...])` accept;
   `layers([...])`, legacy `plot.box(relation, ...)`, and `relation.box(...)`
   refuse. The three refusals are the discriminating half — an implementation
   that registered the word globally passes every accepting case — and the direct
   named mark is the second, since one that only unwrapped figures would pass the
   other three.
4. **Both valid outlier shapes render; a non-quantitative one refuses.** A
   relation containing null outlier rows renders, **and** a relation in which
   every category has an outlier and no row is null renders — the second is the
   discriminating half, since a nullability-shaped implementation would refuse
   it. A non-quantitative `outlier` column refuses at that argument's span.
5. **Conversion is lossless, proven at the `f64` boundary.** A triplet, all
   three cases required:
   - `2^53` renders with that exact value;
   - `2^53 + 1` refuses at **that field's** argument span, naming its row and the
     offending value;
   - `2^53 + 2` **renders**.

   The third case is chosen to prove that **representability, not magnitude,
   governs admission**. Above `2^53` the `f64` grid steps by two, so `2^53 + 2` is
   exact while `2^53 + 1` is not — a value *larger* than the refused one being
   admitted is what distinguishes an exact round-trip check from a
   `value <= 2^53` ceiling, which passes the first two cases and fails this one.
   A nearby value *below* the threshold would not discriminate at all, and a
   blanket integer refusal is already excluded by the first case.
6. **The axis contract holds, and mixed dtypes are accepted.** `x_label` is the
   category argument's column name; `y_label` is **empty**; `y_axis_type` is
   `AxisType::Linear`. A relation whose summary columns carry **different**
   quantitative dtypes — integer whiskers beside floating quartiles — **renders**,
   which is the discriminating half: an agreement rule refuses it while passing
   every other assertion here. A non-quantitative field, a null `category`, and a
   null in any summary column each refuse at their own argument's span.
7. **The body fill is resolved.** The emitted box fill equals
   `mark_style.fill`; under a second skin it differs; and `color_theme.get_color`
   is absent from the box plot kernel.
8. **Fill does not vary by category.** A three-category figure emits the **same**
   fill for all three boxes. This is the discriminating gate for §5 — a
   per-category palette would satisfy gate 7 and fail here.
9. **No legend.** `legend_content` returns `None` for every box plot, asserted as
   absence.
10. **No colour identity is allocated, asserted at resolution.** Resolving a box
   plot against a known `ColorIdentity` snapshot returns it unchanged — no
   assignment, no allocation space — so `DurableLayer::from_runtime` is never
   reached.
11. **A session round-trips.** The `polariton_session` owner payload is schema v2,
   the panel mapping is present, the colour relation is empty, and no plot-type
   or `AuthoredFamily` spelling appears **in that owner payload** — the document
   owner is expected to retain the authored `box(...)` source text.
12. **Single-field refusals name their own argument; cross-field refusals name
   the mark.** A missing column, a non-quantitative `q1`, and a null `category`
   each refuse at the span of *that* argument — three distinct spans, asserted as
   spans rather than as distinct messages, since with seven arguments "which one"
   is the whole diagnostic. `q1 > median` and disagreeing summaries refuse at the
   complete `box(...)` span. Both halves are asserted, because an implementation
   that attributed everything to the mark would pass a per-argument gate that only
   checked messages.
13. **Each value refusal surfaces at the mark.** PRODUCT-0081's typed constructor
    errors reach the author attributed to the mark's span, naming the category
    and the failing rule, for at least an outlier inside the whisker span and
    disagreeing summaries. Each is paired with a valid twin that renders.
14. **The register is empty.** `.bar_width(...)` and `.category_gap(...)` on a
    box plot refuse as inapplicable, per PRODUCT-0016's rule that a setting which
    cannot apply is never silently dropped.
15. **Faceting composes.** A faceted box plot renders per-cell boxes with shared
    categorical domains, and the band-axis outlier offsets are computed from each
    cell's own `band_width` — a cell narrower than the figure must tighten
    spacing, or the offsets were read from the wrong extent.
16. **The exhibit covers every traced case.** The live `box_probe` buffer renders
    `twin` (duplicate outliers), `lowhigh` (two-sided), `none` (no outliers),
    `flat`, and `single` in one figure.

## Stop condition

1. **An author asks for the columns to be inferred.** §1 refuses convention
   lookup on the grounds that ADR 547 permits aliases. A request to infer them is
   a request to reintroduce hidden inference, and belongs to the relation-helper
   decision of §10 — where the helper can *guarantee* the names it emits — rather
   than to the mark.
2. **A horizontal box plot is requested.** §7 declines it and PRODUCT-0055's pair
   resolution does not reach a seven-argument form, so it needs its own record.
3. **A box plot acquires a colour assignment.** §8's chain fails and the
   `AuthoredFamily` question returns as a real decision rather than an avoided
   one.
4. **A second contract must be weakened for the same reason.** §2 gave up a
   schema property because Polars discards it. If another decision in this arc
   has to be softened because the truth boundary discards information the wire
   may carry, the
   Arrow-native seam stops being a cited trigger and becomes a prerequisite.
5. **The blank value axis is judged unacceptable on the exhibit.** §3 leaves
   `y_label` empty because the relation does not carry the measure's name. If the
   rendered figure shows that a blank value axis is not shippable, the remedy is
   the derived-scale axis title record — authored titles, or a derivation rule
   that does not name one of six columns — and not an inference invented here.

## Supersession

Supersedes PRODUCT-0081's producerless typed refusal, which existed to be
replaced here. Discharges PRODUCT-0064's obligation for this mark through §§4
and 8 jointly — bypass removed, and no assignments existing to consume — not
through the fill being an assignment. Amends no colour record: PRODUCT-0030 §5's
palette-consumer enumeration is left intact by §5's refusal to join it.

## Rejected

**Infer the columns by convention.** Attractive because `box("category", "value")`
over an ADR 547 result would be far shorter, and the record's own example query
uses the canonical names. Rejected because those names are authored, not
guaranteed — the composition is ordinary SQL — so inference would bind correctly
only by coincidence and would mis-bind silently otherwise.

**Accept a subset and derive the rest.** Attractive because `whisker_low` and
`whisker_high` could in principle be recomputed from the quartiles and the fences.
Rejected because recomputation is aggregation: the whiskers are *observed values
inside the fences*, not arithmetic on the summary, and a mark that derived them
would be the second authority PRODUCT-0078 and PRODUCT-0081 both removed.

**Vary fill by category.** Attractive because it matches band-fill bars and looks
richer. Rejected because category is already on the axis, so it encodes one
variable twice, and it would make the mark a palette consumer — pulling colour
identity, a legend question, and the persistence schema into a record that
otherwise touches none of them.

**Ship the relation-side Tukey helper in this record.** Attractive because the
seven-column SQL is the real friction an author meets, and the helper would land
with a motivating case in hand. Rejected as scope: it is a relation-surface
decision with its own semantics, and a mark record inventing relation verbs is
the boundary PRODUCT-0078 drew when it refused to let `histogram` bin.

## Consequences

The box plot becomes authorable, and the mark draws entirely from the mark-style
tier — the first mark specified end to end after PRODUCT-0080 existed, so it
never passes through the legacy catalog at any stage.

The seven-argument call is the most verbose spelling in the language, and the
record accepts that as the price of no hidden inference. It also creates the
clearest case yet for a relation-side helper: the friction is now concrete,
attributable, and removable without touching `box(...)`.

Both remaining marks from the original gate are now specified — histogram and box
plot — leaving hexbin, still blocked on viewport-aware planning, with a
`bins: HashMap<(i32,i32),u32>` and a hardcoded data-space radius of `0.02`
(`plot/factory.rs:50`) that make it wrong today rather than merely unauthored.
