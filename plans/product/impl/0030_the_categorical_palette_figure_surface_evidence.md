# PRODUCT-0030 implementation evidence

Recorded 2026-08-05 after the pre-switch oracle and the indivisible production
commit. Rune figures now select libaryon's categorical palette vocabulary,
omission resolves explicitly to `viridis`, and live palette positions advance
only with current visible plot content.

The installed real-window fixture is open for the final fidelity observation.
Gate 16 remains deliberately open until that observation is confirmed; this
document does not turn exact automated color evidence into an aesthetic claim.

## Feasibility findings

`parse_figure` already had one unordered metadata seam, so `palette` flattened
beside title and subtitle without changing the accepted four-level tree or either
plot specification. `PlotSettingsLayer` could carry one authored identity while
the dedicated categorical resolver skipped the inapplicable graphical-layer
tier. Renderer settings and the legacy `ColorTheme` path stayed unchanged.

The request boundary admitted a second explicit, default-free entrypoint:

```text
PlotRequest + CategoricalPaletteSelection + ColorIdentity
    -> ResolvedPlot + proposed ColorIdentity
```

The head can snapshot one bank entry before independent origin dispatch. The
existing `RuneRelationPlotReady` handler rejects stale revisions before installing
panel content, so committing continuation immediately after that install needs no
new transport, panel type, or kernel state.

Polariton cannot observe text-producer release. Libaryon owns `release_buffer`
and projects no matching host event. Monotonic `BufferId` allocation makes an old
bank unreachable after actual release; its small map remains allocated until
process teardown. PRODUCT-0015 and PRODUCT-0030 are now two independent consumers
of this missing lifecycle fact. A third requires a dedicated protocol record.

## Historical proof

```text
P  90a8c7f243877cb020831b50632e91728797a8c9
   plan: product 0030 categorical palette figure surface
O  51e763cf3961ef7ecedb004361aa6324f0900cbf
   test: bank the Rune categorical caller boundary
I  0ad4525b6cc0122652ff74c8647a1ac8d7109cd9
   impl: author categorical palettes on Rune figures
```

`git rev-parse I^` returned exactly `O`. The parent has no `palette` figure
member, no Rune categorical default, no stateful Rune request, and no completion
proposal. The single `O..I` diff contains all four:

```text
figure parser       "palette" direct-member recognition and authored override
Rune default        DEFAULT_CATEGORICAL_PALETTE = "viridis"
Rune request        plan_with_categorical_palette(selection, identity)
completion          set_rune_view_plot, then rune_categorical_identities.insert
```

There is no feature flag, dormant caller, or follow-up production commit between
those states. The pre-switch snapshot remains byte-identical through the legacy
`plot::request::plan` entrypoint.

## Surface and normal form

Envelope assays accept one literal member in every metadata order, including a
named figure, and flatten it to:

```text
PlotSettingsLayer.categorical_palette = Override::Set(Arc<String>)
```

Duplicate, wrong-arity, computed, metadata-only, layer-child, mark `.palette`,
and mark `.skin` forms refuse at their proven spans. Calls outside a proven
figure sink remain ordinary Rune. The envelope contains no selection, resolved
palette, color, palette position, continuation, kernel, or GUI type.

Cascade assays prove:

```text
viridis -> application -> authored -> pane override
```

The graphical-layer value is ignored for this figure-wide property. The explicit
request contains neither `viridis` nor libaryon name parsing; the head supplies
both policy and the already-resolved selection.

## Exact mappings and author sovereignty

A bare six-category Rune bar now resolves with no Color channel and these exact
`viridis` band fills:

```text
A ->  0 -> #440153    B ->  4 -> #3C4582
C ->  8 -> #297D8C    D -> 11 -> #34A27F
E -> 15 -> #77CD58    F -> 19 -> #FDE724
```

Authored `tab20c` resolves:

```text
A ->  0 -> c01 #3182BD    B ->  4 -> c05 #E6550D
C ->  8 -> c09 #31A354    D -> 11 -> c12 #C7E9C0
E -> 15 -> c16 #DADAEB    F -> 19 -> c20 #969696
```

`tab20c_r` and `tab20c_s1` retain positions and consume exact resolved colors;
the shift is the delegation control. A `yellows` bar completes with its exact
declared pale sequence. No contrast, distance, saturation, hue, warning,
substitution, or automatic shift participates.

An authored categorical scatter uses ordinary Color-channel assignments and no
band fills. Line, absent-color scatter, and accepted layers validate an explicit
palette without manufacturing a consumer. Layered color remains absent.

Unknown identity and known `nord` categorical absence remain distinct. The real
Rune path also exercises `dark2/9` palette-only, `tab20c/13` geometry-only, and
`dark2/13` both-limit failures; every message preserves requested cardinality,
geometry limit, exact palette identity, and palette limit without fallback.

## Live continuation and content commit

The head integration assay drives actual resolved plots through
`Message::RuneRelationPlotReady`:

```text
visible 1   A -> 0
visible 2   A -> 0, B -> 19
visible 3   A -> 0, B -> 19, C -> 9
filtered    A -> 0, C -> 9
returned    B -> 19
```

Same-length `tab20c -> tab20c_r -> tab20c_s1 -> viridis` retains positions
`0,19,9` while exact colors change. `dark2` creates a separate eight-position
bank with fresh `0,4,7`; returning to `tab20c` resumes `0,19,9`.

A nine-category `dark2` failure leaves the retained eight-position bank at three
assignments. A four-category proposal delivered with a superseded revision does
not commit; the next current category receives position 14, proving the stale
category never reserved it. Two named views over two origins grow independently
when the origins finish in each order.

The architecture assay observes stale-check, panel install, and bank insert in
that order. Continuation is absent from Rune normal form, plot specs, resolved
artifacts, kernels, editor panels, dashboard, tables, adapters, and Toron.

## Boundary and regression evidence

The implementation changes only the authorized Rune envelope, settings, request,
internal GUI completion path, PRODUCT-0029 caller-status comment, architecture
guards, and implementation plan. Libaryon, Toron, adapters, renderer kernels,
factory, plot pane, editor layout, dashboard, tables, and configuration are
unchanged.

The high-density external fixture is now `../uat/test9.rn` and contains:

```rune
figure([
    title("SQL = 💩"),
    subtitle("Wendelon was here"),
    palette("tab20c"),
    bar("cat", "val").bar_width(0.98),
])
```

`cargo run -p polariton -- ../uat/test9.rn` built and launched successfully. The
real-window exact-color/shift observation remains the sole open acceptance item.

## Gate ledger

1. **Surface context:** passed by literal, permutation, refusal, and ordinary-Rune assays.
2. **Bounded carrier:** passed structurally and by architecture census.
3. **Explicit default:** passed with exact `viridis`, cascade, and default-free request controls.
4. **Rune default consumers:** passed by exact bar and categorical-scatter resolution.
5. **Authored tab20c:** passed by complete mapping and absent Color channel on band fill.
6. **Modifiers:** passed by invariant positions and exact reverse/shift colors.
7. **Authored Color:** passed through the existing categorical scatter channel.
8. **Right to poor choices:** passed by exact `yellows` completion.
9. **Harmless unused setting:** passed for line, scatter, and layers.
10. **Factual failures:** passed for unknown, absent, and all three capacity combinations.
11. **Live continuation:** passed through current content, filtering, failure, and stale assays.
12. **Length banks:** passed through 20/8/20 retargeting and exact resumption.
13. **Independence/lifecycle:** passed for two origins/views; release retention follows the proved branch.
14. **Indivisible history:** passed by immutable `O..I` inspection.
15. **Panel/layout/execution:** passed by stable named panel, dashboard absence, and full regressions.
16. **Real window:** open; awaiting author confirmation of exact visible mapping and `_s1` shift.
17. **Prior product records:** passed by the full Polariton and architecture suites.
18. **External boundaries:** passed by file census, architecture guards, and compatibility builds.

## Verification

```text
cargo fmt --all -- --check                      passed
cargo test -p polariton --features toron        338 passed; 5 live-only ignored
polariton architecture tests                    30 passed
cargo test -p libaryon                          passed (1,452 unit tests plus auxiliary suites)
cargo check -p polariton --no-default-features  passed
cargo build -p polariton                        passed
git diff --check                                passed
```

The closure commit will record this evidence commit's immutable hash, the
`I..E` documentation-only census, the real-window observation, ADR acceptance,
and corpus v0.30. No code may change after `I`.
