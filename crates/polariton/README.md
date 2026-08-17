## Polariton

A strong coupling of data and visualization.

#### Installing:

If `cargo deb --install` is configured in your environment, Polariton can be installed directly:

```bash
cargo deb -p polariton --install
```

#### Features:
 - Query 10M rows into local memory in milliseconds.
 - Memory model is polars::frame::DataFrame for blazing fast analytics.
 - Instant scolling, no kill -9 required to scroll though 10M rows.
 - Made with 100% Rust, no JVM garbage factories included. 🦀
 - Batteries included, all drivers are bundled.
 - Generic SQL syntax highlighting.
 - Supports both DDL and DML.
 - Resizable & Rearrangeable ui panels.
 - Rune-only startup configuration.

#### Configuration:

Polariton reads user-authored configuration from `init.rn`. On Linux, the
default path is `~/.config/polariton/init.rn` (or
`$XDG_CONFIG_HOME/polariton/init.rn`). macOS and Windows use their standard
application configuration directories.

```rune
pub fn main(config) {
    config.window_size(
        1920,
        1080,
    )?;
    config.show_column_types(true)?;
    // Reserve four editor columns at vertical splits and one row at horizontal splits.
    // Omit this to retain Polariton's compact, zero-gutter default.
    config.pane_divider_gutter(4, 1)?;
}
```

Libaryon's editor settings live in a separate `editor` function in the same
file. Persistent `:set` commands update only this function; Polariton's
application-owned `main` function and all other Rune code are preserved.
`:set init` or `:init` writes a full snapshot of the current editor settings,
while `:init0` restores the editor defaults.

Missing configuration is a clean no-op. A broken script is reported in the
status bar while Polariton still opens with compiled defaults.

#### Saved sessions:

Create a durable session explicitly, checkpoint it from the Session menu, and
reopen it with its complete identity:

```bash
polariton --session new
polariton --session list
polariton --session 24631c77d510daf33ae6331e1f4776ce
```

The list shows each closed session's committed generation, UTC filesystem time,
document count, and document paths so the full reopening identity can be chosen
without maintaining a second name registry. A row marked `active` is already
held by another window; its document metadata is intentionally unknown because
listing never reads a live writer's snapshot without the store lock. The
`UPDATED_UTC` column describes the current committed generation, not whether an
open window has uncheckpointed changes.

#### Static presentation bindings:

Repeated field names can be factored into top-level string bindings. The
envelope resolves them before the returned relation schema chooses channels or
bar orientation:

```rune
let category_field = "region";
let value_field = "revenue";
let series_field = "product";

workspace.table("sales").views([
    bar(category_field, value_field, series_field),
    figure([
        facet(series_field),
        bar(category_field, value_field, series_field)
            .bar_width(0.9),
    ]),
])
```

Direct and aliased top-level bindings work in every established bar, line,
scatter, and area field position, including accepted x/y fields inside
`layers([...])`.
They are static source factoring, not runtime variables: computed strings and
non-top-level bindings refuse. Numeric mark modifiers such as
`.bar_width(width)` still require inline literals.

#### Rune table presentation:

The application setting applies to every table. A Rune table can opt in
independently in a proven view position:

```rune
workspace.table("events").views([
    table().show_types(),
])
```

`.show_types()` composes in either order with the table's `.skin(...)` and
`.duotone()` members.

#### Rune bar presentation:

Axis titles are text members, so their size attaches to the title. The
no-argument form preserves a kernel-derived label while changing only its
typography. Generated tick labels have no member, so they use dedicated words:

```rune
figure([
    y_label().font_size(20),       // keeps the derived y-axis title
    x_label("Quarter").font_size(18),
    x_tick_size(12),
    y_tick_size(14),
    bar("product", "value"),
])
```

Generated x-axis tick labels can rotate in degrees when long labels need more
horizontal separation. The angle is bounded from `-90` through `90`; it does
not rotate the authored `x_label(...)` axis title:

```rune
figure([
    x_tick_rotation(45),
    bar("product", "value"),
])
```

Grouped bars can reserve normalized space between category clusters without
changing the spacing model inside each series slot:

```rune
workspace.table("sales").views([
    bar("region", "revenue", "product")
        .bar_width(0.9)
        .category_gap(0.2),
])
```

`category_gap` accepts values from `0.0` (the unchanged default) up to, but not
including, `1.0`. The reserve stays centered on each categorical label and
scales with the plot.

Bar orientation follows the resolved x/y channel pair, without a second mark
word. Put the quantitative value on x and the category on y for a horizontal
bar; the same grouped colors and bar register apply:

```rune
workspace.table("sales").views([
    figure([
        x_bounds(0, 150),
        bar("revenue", "region", "product")
            .bar_width(0.9)
            .category_gap(0.2),
    ]),
])
```

Exactly one axis must be quantitative. The other keeps Bar's existing
non-quantitative category role; pairs with two quantitative or two
non-quantitative axes refuse rather than guessing an orientation.

Categorical order is reading order. Horizontal categories render from top to
bottom in their resolved display-domain order, and grouped series use the same
top-to-bottom order as the named legend. Relation sorting remains the authority:
sort ascending for an ascending list, or descending to put the first-ranked bar
at the top. Heatmap rows follow the same top-down y-domain rule; x remains left
to right.

Add `.stacked()` to select stacked geometry without changing the authored Bar
family or its color identity. It composes with both orientations and with the
existing bar geometry modifiers:

```rune
workspace.table("sales").views([
    bar("region", "revenue", "product")
        .stacked()
        .bar_width(0.8)
        .category_gap(0.2),
    bar("revenue", "region", "product")
        .category_gap(0.2)
        .stacked(),
])
```

Segments stack in legend order outward from data zero: upward for vertical
bars and rightward for horizontal bars. Authored bounds clip that true-zero
geometry rather than relocating its origin. Grouped and stacked forms of the
same series retain the same resolved colors. The two-field form is a valid
one-segment stack and carries no legend.

Stack preparation remains first-row-wins for each category/series pair, not an
implicit aggregation. Only positive finite values participate in the summed
axis, rectangles, hit testing, and hover; aggregate or filter in the relation
when different data semantics are intended.

Add `.percent()` to project either stacked family onto a fixed `0..100` scale:

```rune
workspace.table("sales").views([
    figure([
        y_tick_step(25),
        bar("region", "revenue", "product")
            .stacked()
            .percent(),
    ]),
    figure([
        x_tick_step(25),
        bar("revenue", "region", "product")
            .percent()
            .stacked(),
    ]),
])
```

Modifier order is irrelevant. Each category uses the canonical values the mark
actually draws as its denominator; later duplicate rows are not aggregated.
The value-axis domain is semantic and fixed, so value-axis bounds refuse even
when written as `0..100`. Tick steps are percentage points. Hover reports the
absolute value and total alongside share and cumulative percentage. A
zero-total category draws nothing rather than inventing a `0%` share.

#### Rune line presentation:

Lines accept an optional categorical series field. Rows connect only within
their series and retain relation order, so sort explicitly when x order matters:

```rune
workspace.table("sales")
    .sort([col("product").asc(), col("units").asc()])
    .views([
        line("units", "revenue", "product")
            .stroke_width(2.0),
    ])
```

The named series share the figure palette and legend with grouped scatter and
bar marks. The three-field form is a standalone mark and is not accepted inside
`layers([...])`.

#### Rune area presentation:

Areas use a standalone mark word. The two-field form draws one filled region;
the optional categorical series field produces a baseline-outward stack with
named swatches in the same display-domain order:

```rune
workspace.table("sales")
    .sort([col("month").asc(), col("product").asc()])
    .views([
        figure([
            palette("vapor_s11"),
            area("month", "revenue", "product"),
        ]),
    ])
```

Area x accepts quantitative or temporal data; y is quantitative. Duplicate
`(x, series)` rows are first-row-wins, so aggregate explicitly in the relation
when totals are intended. Non-finite and non-positive samples contribute zero.
The two-field form has no legend, and areas remain outside `layers([...])`.
Gridlines render beneath opaque fills while the resolved boundary and exterior
axis furniture remain visible.

`.percent()` applies the same local-denominator projection at each x sample.
The area keeps its absolute values for hover, uses an exact `0..100` y domain,
and accepts percentage-point y tick steps. A zero-total x sample pinches every
band to zero. A fully occupied percent area can occlude every interior gridline;
the percentage ticks and resolved series boundaries remain above the fills.

#### Adapters:
 - SQLite (file or memory)
 - Parquet (single file)

#### Screenshots:
![Polariton SQL IDE interface showing query editor and results pane](doc/img/screenshot_1.png)

#### Backlog:
 - Jump to row
 - Shift+Enter to run code
 - Tab/Shift+Tab gui navigation
 - Improved status bar
 - Sort / Filter by column
 - Run selected code, not the entire code editor.
 - Password protection.
 - More data sources (Postgres, MySQL, BigQuery, Redshift, ad inf...)
 - Scatter / Line / Bar plots
 - Hexbin / Kernel Density plots
 - Qwen Coder SQL generator
