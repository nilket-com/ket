# Polariton UAT exhibits

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
