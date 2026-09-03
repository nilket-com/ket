# PRODUCT-0151 selection context — implementation evidence

**Status:** implemented 2026-09-02; amended after implementation review
2026-09-03.

## Shipped boundary

Selection now has a producer-neutral, exact vocabulary in `selection.rs`.
`ExactScalar` keeps booleans, signed and unsigned integers, canonical finite
floats, strings, dates, datetimes with unit and zone, times, durations with
unit, and decimals with scale distinct. `FieldRef` carries source name,
semantic type, and the complete physical scalar type. Nulls, NaNs, containers,
and values whose source provenance cannot be retained do not become selection
constraints.

Every logical binding carries an opaque `InputIdentity`; every materialized
publication also carries its fresh `ArtifactToken`. Annotated SQL binds by
registry name, an unannotated result by its result panel, a notebook result by
cell identity, and Rune by `(BufferId, RelationLineage)`. The Rune compiler
derives the lineage structurally from the first relation call and its parsed
literal arguments, follows let bindings, normalizes `ket` and `workspace`, and
does not inspect relation-verb names. Derived artifacts inherit the complete
publication. The grid factory now inherits the artifact token rather than
minting one per cell.

Nine admitted kernel modules retain exact candidates and implement predicate
projection: scatter, bar, box plot, violin, heatmap, histogram, candlestick,
fill-between, and the line/scatter layered dispatcher. Line itself and every
other family remain refused. Candidates contain only the source fields in ADR
0151's closed table. Bar preparation detects the pre-existing duplicate-pair
collapse and makes that plot non-click-selectable. `PlotHit` carries the exact
candidate beside its unchanged presentation fields; hover prose remains
derived from the presentation half only.

Predicate projection scans retained candidates once when a selection changes.
It produces kernel-owned data-space anchors; interactive frames only reproject
those anchors through the current transform. Facet projections retain one
anchor collection per stable cell, and draw does not rescan cell kernels.
Brush intervals use exact typed inverse projection, including temporal axes,
and round integral and temporal endpoints inward. A between-ticks brush is an
honest empty interval whose rectangle remains visible.

`SelectionContexts` owns the OR-within/AND-across algebra, per-publication
staleness, total sibling preflight, self-exclusion, and revision. Rebuilds are
newest-wins and always start from the retained unfiltered artifact plus a
`PlotRecipe`; they never filter a previously filtered kernel and never leave
the process. The prior kernel remains visible while pending, sharing the one
surface preparation marker with raster work. Retirement clears only the
retired publication's predicate and schedules every survivor to rebuild from
the remaining conjunction.

Host plots, notebook plots, host tables, and notebook tables all register and
retire through the same context boundary. Plot rebuilds preserve the pane
viewport and settle the surface. Table emphasis is derived from exact values,
so sorting or filtering cannot move it onto unrelated rows. Exports snapshot
publication, predicate, and revision at dispatch; a pending export rebuilds
that snapshot in its blocking job and is unaffected by a later selection.

The interactive vocabulary is live: Shift click/drag replaces, Shift+Alt
toggles add/subtract, Shift-click on empty plot space clears, and Escape
clears only the active predicate of the plot under the cursor. Bare Alt still
reaches PRODUCT-0150 pan. Empty clears are true context no-ops, and ready host
plots, notebook plots, and tables deduplicate an unchanged displayed filter
before scheduling work. Tables publish through an
explicit `on_selection` callback on release and Escape only; multi-column,
null, and unrepresentable selections disclose through the `Selection · `
status lane. Selection paths contain no Rune writeback gesture and no engine
adapter branch.

Unsigned brush intervals clamp a lower edge below zero inward to zero when the
upper edge is representable. An export whose dispatch-snapshot selection
cannot be rebuilt now refuses through the export status lane; it never renders
the stale displayed kernel as if it answered the snapshot.

## Discriminating evidence

- adjacent nanosecond datetimes that collapse to one plotted `f64` produce
  distinct exact click candidates, while the hover prose stays byte-identical;
- signed/unsigned identity, large integers, canonical zero, temporal unit and
  zone, duration unit, decimal scale, NaN refusal, and table losslessness are
  exercised directly;
- every admitted family has a field-set/anchor fixture; heatmap and histogram
  require both authored fields, candlestick and fill-between use x alone,
  box/outlier candidates differ as recorded, grouped bars carry category and
  group, and a line layer refuses;
- the admission architecture assay enumerates exactly the nine implementation
  modules and names every refused family;
- Rune lineage survives sorting, root aliasing, whitespace/reformatting, and
  republication, distinguishes two literal heads in one buffer, and refuses a
  non-literal head;
- all four binding paths keep input identity while republishing a new token;
  derived artifacts and facet cells inherit the full publication;
- recipe tests apply successive predicates from the original artifact and
  preserve the existing allocation and pane-domain tier;
- reversed completions and completion after surface replacement are inert;
- overlapping raster and selection readiness emits one marker, and pending
  export bytes equal the corresponding settled-revision export while differing
  from a later selection;
- algebra tests cover OR within, AND across, categorical subtraction,
  multi-field add, disjunctive range, incompatible type parameters, absent
  sibling fields, stale publications, self-exclusion, and atomic refusal;
- retirement tests assert survivor *displayed rows* after rebuilding from only
  the predicates that remain;
- integral and temporal brush tests pin inward rounding, the empty interval,
  facet partition constraints, and anchor reprojection after pan, zoom,
  label-band reflow, and rebuild;
- real canvas events prove Shift and Shift+Alt reach selection while bare Alt
  remains pan; an Escape matrix proves unavailable cursor, empty predicate,
  and unrelated canvases publish nothing, while the active plot under the
  cursor clears;
- context and consumer tests prove an empty clear advances no revision and
  schedules no rebuild, and unchanged ready filters schedule no host,
  notebook, or table job;
- a crossing-zero unsigned brush clamps inward to zero, and a failed pending
  export rebuild refuses instead of emitting stale bytes; and
- real table press/release/Escape events prove the callback boundary, while the
  standing Ctrl-C oracle remains `A\t1\nB\t2\nC\t3\n` and value-derived
  emphasis follows reordered rows.

## Stage-0 cost exhibit

The retained ignored exhibit was run explicitly in the debug test profile on
the one-million-row fixture:

```text
scatter filtered rebuild       3.021629179 s
predicate projection           843.741814 ms
ten overlay reprojections       11.229 us
hexbin filtered rebuild        335.018755 ms
```

The ten-frame measurement is the important separation proof: projection is
paid once per selection change, while ordinary redraws only reproject retained
anchors.

The required release invocation was attempted:

```text
cargo test --release -p polariton --bin polariton \
  product_0151_exhibit_one_million_scatter_filter_rebuild_and_projection \
  -- --ignored --nocapture --test-threads=1
```

It does not reach this exhibit because the standing release-profile test build
fails with the twelve iced unit-renderer `Renderer`, `Paragraph`, and `Editor`
bound errors in `gui/table.rs` and `editor.rs`. This is the same unrelated
failure banked in `0133_gesture_census.md` and preserved by PRODUCT-0150; ADR
0151 does not repair or obscure it. The debug exhibit and all normal-profile
tests compile and pass.

## Compatibility audit

PRODUCT-0151 widens `TableArtifact` atomically, adds three facet geometry
traversals, and adds `selection` to the binary module graph. The older PRODUCT
0023, 0030, 0057, 0080/0083, 0097, 0114, and 0126 architecture guards were
updated narrowly to recognize those additions without weakening their original
boundaries. The full architecture suite passes. PRODUCT-0150's complete focused
selection also passes unchanged after the gesture arbitration and hover lane
were extended.

## Verification at close

```text
cargo fmt --all -- --check                                  pass
cargo test -p polariton --bin polariton product_0151 \
  -- --test-threads=1                                       pass
                                                           (44 pass,
                                                            1 planned ignore)
cargo test -p polariton --bin polariton product_0150 \
  -- --test-threads=1                                       pass
                                                           (23 pass,
                                                            1 planned ignore)
cargo test -p polariton --test architecture_test \
  -- --test-threads=1                                       pass (116)
cargo test -p polariton --bin polariton -- \
  --test-threads=1                                          pass
                                                           (1,153 pass,
                                                            20 ignored)
cargo check --workspace --all-targets --all-features        pass
git diff --check                                            pass
```

The all-target check emits only standing unused test-helper warnings. There is
one new planned ignored test: the Stage-0 million-row cost exhibit required by
this record.
