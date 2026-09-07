# Polariton UAT exhibits

`testNN.rn` files are historical regression probes, named by birth order and
cited by product records as provenance. Do not rename them merely to tidy the
directory: a record may honestly cite a probe that was later deleted, but a
rename makes a live exhibit appear missing. New purposeful, presentation-ready
exhibits use semantic names that describe what they show.

Seed the four-quarter sales fixture, then open the twelve-panel bar showcase:

```sh
nilket toron sql --file uat/demo_sales.sql
cargo run -p polariton -- uat/test21.rn
```

`demo_sales.sql` is idempotent. Its quarterly values preserve the annual totals
used by the unfaceted panels while giving each region a distinct seasonal shape,
so the faceted panels can expose a quarter-assignment defect.

`test21.rn` is both a runnable product exhibit and a PRODUCT-0076 regression
input. Keep this repository copy authoritative; files in a neighboring `uat`
directory are local conveniences and are not build inputs.

Open the semantic Trends & relationships showcase with its purpose-built seed:

```sh
nilket toron sql --file uat/plot_trends_showcase.sql
cargo run -p polariton -- uat/plot_trends_showcase.rn
```

One relation drives its line, area, scatter, and bubble views. The four fields
are intentionally direct mark inputs: `period`, `series`, `value`, and
`magnitude`.

Open the semantic Distribution & variation showcase with its raw-observation
seed:

```sh
nilket toron sql --file uat/plot_distribution_showcase.sql
cargo run -p polariton -- uat/plot_distribution_showcase.rn
```

One relation drives four different statistical preparations: grouped averages
for bars, fixed numeric bins for the histogram, Tukey summaries for the box
plot, and density plus median for violins. The aggregation stays visible in
Rune; marks receive only their prepared inputs.

Open the semantic Composition & progress showcase with its two purpose-built
relations:

```sh
nilket toron sql --file uat/plot_composition_showcase.sql
cargo run -p polariton -- uat/plot_composition_showcase.rn
```

The ordered conversion table drives both pie and funnel views: one renders
parts of a total, the other renders the same values as a declining progression.
The separate readiness table supplies named radial-dial readings.

Open the semantic Multivariate & matrix showcase with its long- and wide-form
relations:

```sh
nilket toron sql --file uat/plot_multivariate_showcase.sql
cargo run -p polariton -- uat/plot_multivariate_showcase.rn
```

The heatmap reads an explicit long-form capability matrix. Radar and parallel
read the same wide operating profiles, making their difference one of visual
question rather than data preparation.

Seed the PRODUCT-0157 gate-10 fixtures, then produce the gate-10 package
without a pointer. PRODUCT-0158's driver opens the application headless,
runs the source, hides one series per pane, sizes the raster pane under the
gate, and publishes; the receipt names every figure's incarnation, its hidden
set, and the render mode the renderer resolved:

```sh
nilket toron sql --file uat/plot_fixtures_showcase.sql
cargo run -p polariton -- drive uat/plot_fixtures_showcase.rn \
  --commands uat/plot_fixtures_showcase.commands.json \
  --receipt /tmp/plot_fixtures_showcase.receipt.json
cargo run -p polariton -- serve plot_fixtures_showcase_driven
```

The commands file addresses each figure by its view name; `polariton
describe uat/plot_fixtures_showcase.rn` prints the same receipt shape with
the loaded figures and their incarnation tokens, which a script can address
instead. `polariton describe --session <id>` does the same for a saved
session. To open the same showcase in the window:

```sh
cargo run --release -p polariton -- uat/plot_fixtures_showcase.rn
```

Release, deliberately: a debug build multiplies mark preparation by tens.
Since PRODUCT-0159 the scatter arms batch one fill per mark instead of one
compound path, so the million points arrive without resizing the pane; the
desktop's worst unserviced tick during that build measured about 260 ms in
release. Once under the raster gate the raster arm draws in tens of
milliseconds.

A percent-normalized stacked area, a line whose second series label is the
PRODUCT-0155 hostile string, and a 1,157,625-point categorical scatter that
crosses the automatic raster gate only in a pane under about 380 × 380 pixels.
Hide one series in each pane before Plot → Publish, so every figure carries a
snapshot.

Open the PRODUCT-0123 axis-member reference exhibit after seeding the same
sales relation:

```sh
cargo run -p polariton -- uat/test38.rn
```

`test38.rn` demonstrates both legal composition forms: a chained `x_axis()`
and separate `y_axis()` members whose distinct properties compose. It is a
surface reference, while `test37.rn` remains the long-label geometry probe.

Open the PRODUCT-0124 facet-label typography exhibit after seeding the same
sales relation:

```sh
cargo run -p polariton -- uat/test39.rn
```

`test39.rn` is the compact member-ownership reference: `facet("region")`
owns both its inter-cell gutter and its category-header `label_size`.

Open the PRODUCT-0125 legend-member exhibit after seeding the same sales
relation:

```sh
cargo run -p polariton -- uat/test40.rn
```

`test40.rn` shows both legal legend forms: one chained `legend()` member and
separate members whose placement and typography compose.

Seed the PRODUCT-0144 viewport-prepared observations, then open the authored
hexbin and temporal-refusal exhibit:

```sh
nilket toron sql --file uat/hexbin_probe.sql
cargo run -p polariton -- uat/test41.rn
```

`hexbin_probe` contains several dense clusters plus separated boundary points,
so repeated cell occupancy and the distinction between source count and maximum
bin count are visible. Its temporal companion must refuse on the `"stamp"`
argument without disturbing the healthy view.

Seed the PRODUCT-0146 forecast relations, then open the authored fill-between
exhibit:

```sh
nilket toron sql --file uat/fill_between_probe.sql
cargo run -p polariton -- uat/test42.rn
```

`fill_between_probe` renders temporal x in both the band-only and optional-middle
forms. The crossed companion refuses at row 2 on the `"low"` argument without
disturbing either healthy view. Both relations author an ascending sort before
plotting: Toron table scans have no row-order contract, while fill-between
deliberately treats strictly increasing x as an authored data requirement.

Seed the PRODUCT-0148 OHLC relations, then open the authored candlestick
exhibit:

```sh
nilket toron sql --file uat/candlestick_probe.sql
cargo run -p polariton -- uat/test43.rn
```

`candlestick_probe` authors an ascending timestamp sort and shows rising,
falling, and doji geometry across a visible weekend gap. The torn companion
refuses at row 2 on the `"high"` argument without disturbing the healthy view.
The render gate is discharged only by driving this file through the live
application and capturing the resulting candles; compilation alone is not the
visual proof.

Open the semantic temporal-tick exhibit with its DATE seed:

```sh
nilket toron sql --file uat/temporal_tick_intervals.sql
cargo run -p polariton -- uat/temporal_tick_intervals.rn
```

`tick_every("1 month")` is calendar placement, not numeric stepping. The
exhibit keeps the source as a DATE column so month-length semantics are visible
in the workbook rather than implied by an epoch number.

Open the PRODUCT-0079 histogram exhibit after seeding the same sales relation:

```sh
cargo run -p polariton -- uat/test25.rn
```

`test25.rn` uses Toron's canonical alias-first `FLOOR` bucketing directly. Its
second panel deliberately removes one returned interval, making the sparse-bin
contract visible rather than asking Polariton to synthesize a zero-count bin.

Seed both relations before opening the PRODUCT-0082/0083 box-plot and
PRODUCT-0085 axis-title exhibit:

```sh
nilket toron sql --file uat/demo_sales.sql
nilket toron sql --file uat/box_probe.sql
cargo run -p polariton -- uat/test26.rn
```

`box_probe.sql` is idempotent and retains duplicate observations deliberately.
Its five groups cover two-sided outliers, no outliers, coincident duplicate
outliers, a singleton, and a flat distribution.

Seed the PRODUCT-0092 heatmap relations, then open the sparse-cell exhibit:

```sh
nilket toron sql --file uat/heat_probe.sql
cargo run -p polariton -- uat/test27.rn
```

`heat_probe` is pre-aggregated to one row per observed cell, with one deliberate
cross-product gap and a signed domain. `heat_probe_raw` retains a duplicate
North/Q1 key; its companion view must refuse and direct the author to aggregate
in the relation.

Seed the PRODUCT-0095 bubble relation, then open its labelled and unlabelled
figures:

```sh
nilket toron sql --file uat/bubble_probe.sql
cargo run -p polariton -- uat/test28.rn
```

`bubble_probe` has a signed colour domain, a wide size range, and one null label.
Both gradient and size-sample legend sections must remain unclipped. The labelled
figure omits the null label's hover heading; the four-role figure generates
`Point 0`, `Point 1`, … headings.

With both continuous fixtures seeded, open the PRODUCT-0091 authoring exhibit:

```sh
nilket toron sql --file uat/heat_probe.sql
nilket toron sql --file uat/bubble_probe.sql
cargo run -p polariton -- uat/test29.rn
```

`test29.rn` separates ramp-slot selection from mark job selection: Magma remains
sequential, GreenPurples is anchored at zero, and BlueWhiteRed is anchored at an
authored value of 15. The heatmap's sparse cell and bubble's paired gradient and
size guide remain visible regression subjects.

Seed the PRODUCT-0098 funnel relations, then open its authored and refusing
figures:

```sh
nilket toron sql --file uat/funnel_probe.sql
cargo run -p polariton -- uat/test30.rn
```

`funnel_probe` is already ordered and contains an equal-valued Paid/Renewed
plateau, so reorder-stable categorical assignments can be distinguished without
violating the non-increasing substrate. `funnel_probe_rising` deliberately rises
from 60 to 75 and must refuse at the value argument. The faceted view keeps the
authored funnel on PRODUCT-0097's non-origin hover seam.

Seed the PRODUCT-0100 radial-dial relations, then open the grouped and ungrouped
surface exhibit:

```sh
nilket toron sql --file uat/radial_dial_probe.sql
cargo run -p polariton -- uat/test31.rn
```

`radial_dial_probe` deliberately arrives as West, East, South, North and must
render in PRODUCT-0099's lexical category order. `radial_dial_ungrouped` has no
label field; its one honest `category: None` is displayed as `All Data` only at
the legend and hover presentation sites.

Seed the PRODUCT-0103 pie relation, then open its aggregated and refusing
figures:

```sh
nilket toron sql --file uat/pie_probe.sql
cargo run -p polariton -- uat/test32.rn
```

`pie_probe` deliberately contains duplicate categories. The first relation
explicitly aggregates, restores the `category` / `value` names, and sorts the
slices clockwise by descending value. The raw companion must refuse at the
category argument and name `.aggregate(...)`; together they demonstrate that
Toron, not the pie mark, owns summation.

Seed both PRODUCT-0105 multivariate relations, then open the radar and parallel
surface exhibit:

```sh
nilket toron sql --file uat/radar_probe.sql
nilket toron sql --file uat/parallel_probe.sql
cargo run -p polariton -- uat/test33.rn
```

The radar relation is interleaved by category while the authored axes use a
deliberately different order from the table schema. The parallel relation uses
eight dimensions, proving projection exceeds PRODUCT-0094's former seven-field
ceiling; its faceted companion keeps parallel hover live in a non-origin cell.

Seed the violin relation before opening the PRODUCT-0107 composition exhibit:

```sh
nilket toron sql --file uat/violin_probe.sql
cargo run -p polariton -- uat/test34.rn
```

`test34.rn` is the canonical PRODUCT-0107 §2 composition: Toron ADR 552's
`kernel_density` joined to a grouped `approx_quantile` median inside **one**
invocation. Polariton compiles relations and never synthesizes them, so this
authored spelling is the contract — a source-level control pins it, because
nothing at runtime can prove two statistics shared an execution.

It renders both the prepared relation table and its violin panel. The table
keeps the relation-side grid and median inspectable; the violin consumes those
same four delivered fields without recomputing a density or median.

The `lonely` group is deliberate. It holds one observation, ADR 552 emits no
rows for it, and the prepared relation must simply not carry it — absence is not
a zero-density category.

For a presentation-focused seven-cohort violin showcase, seed its separate
fixture and open `test35.rn`:

```sh
nilket toron sql --file uat/violin_showcase.sql
cargo run -p polariton -- uat/test35.rn
```

`test35.rn` keeps the same one-plan relation-side density/median composition as
the canonical probe, but its seven healthy cohorts are shaped for legibility:
compact bell, bimodal, two opposite tails, plateau, narrow peak, and an upper
shoulder. It deliberately shares the density-width scale, so width communicates
relative concentration rather than making every violin equally wide.

Seed the PRODUCT-0114 long-label probe before opening the bottom/left-band
exhibit:

```sh
nilket toron sql --file uat/axis_labels_probe.sql
cargo run -p polariton -- uat/test37.rn
```

`test37.rn` is a deliberately asymmetric faceted bar figure. Its compact and
verbose facets carry different categorical label lengths, so the default capture
can show current crowding and later prove that one figure-wide reservation keeps
facet cells congruent.
