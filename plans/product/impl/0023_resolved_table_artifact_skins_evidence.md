# PRODUCT-0023 implementation evidence

Recorded 2026-08-04 after the pre-extraction oracle, three implementation
commits, and complete cross-feature verification. Tables now receive one closed,
resolved artifact skin, and completed frames retain that skin atomically across
SQL-result, Rune-view, and Markdown-cell lifecycles. PRODUCT-0024 remains the
first Rune surface caller.

## Stage 0 findings

The actual Iced `Table::draw` path accepted a test-only recording renderer, so
the compatibility oracle observes production drawing rather than a parallel
paint model. Its fixed two-column/four-row fixture exercises types, alternating
rows, selection, and both scrollbar thumbs. The banked sequence contains 39
ordered fill/text operations and remained byte-for-byte unchanged after the
renderer began consuming `TableSkin::DEFAULT`.

The default and the contrast gate were compatible before refactoring. Ratios
computed from the landed `TableSkin::DEFAULT`, including the skin's declared
selection alpha, are:

```text
cell / even row             15.402:1
cell / odd row              13.158:1
header / header background  14.951:1
type / header background     8.139:1
gutter / gutter background  14.951:1
cell / selected even row    12.817:1
cell / selected odd row     10.921:1
```

`PanelContent::Table` and `NotebookCellOutput::Table` were both private Polariton
carriers, and host failure already changed only status for completed content.
One `TableArtifact { frame, skin }` therefore covers both lifecycle families
without a protocol or failure-path change.

## Consumer census

The two production widget construction sites remain the complete census:

- the ordinary host-panel path covers `RESULT_PANEL`, Rune table views, and any
  other table-backed panel;
- the Markdown-cell path covers inline notebook tables.

Both destructure the stored artifact and call the required-skin constructor.
`Table::new` has no defaulting overload, so a future omitted consumer fails to
compile.

## Gate ledger

1. **The skin is closed and complete.** `TableSkin` has exactly twelve required
   `TableRgba` fields. The architecture assay rejects optional and string-keyed
   representations.
2. **The default preserves actual drawing.**
   `product_0023_pre_extraction_draw_operation_oracle` still matches the 39-step
   oracle captured in `f2dc10ae`; value equality is not used as a substitute.
3. **Every built-in passes visible contrast.**
   `product_0023_every_builtin_meets_text_contrast_after_declared_alpha_compositing`
   checks all seven pairs at 4.5:1. Its opaque-selection control proves the
   declared alpha is read rather than assumed.
4. **The renderer requires and consumes the skin.** `Table::new` requires a
   `TableSkin`; `product_0023_table_draw_reads_only_its_required_resolved_skin`
   excludes table constants, `UiPalette`, plot ramps, and implicit defaults.
5. **All table surfaces use the same widget and default.** The required
   constructor compiles at exactly the host and Markdown sites, and the default
   operation oracle remains unchanged through both shared paths.
6. **A non-default skin reaches every role and only color changes.**
   `product_0023_skin_changes_every_color_role_and_nothing_else` supplies twelve
   unique sentinels and compares operation order, geometry, text, clipping,
   selection, and scroll setup against the default draw.
7. **Frame and skin move atomically.**
   `product_0023_completed_table_artifacts_replace_and_stale_as_atomic_pairs`
   replaces distinct frames and skins through result, Rune, and Markdown paths,
   then proves host failure retains the complete prior pair as stale.
8. **The skin cannot change surrounding chrome or layout.** The real recording
   renderer proves all table work is under the supplied artifact clip.
   `product_0023_skin_only_replacement_does_not_mutate_host_panel_layout`
   compares descriptor, pane identity, rectangle, focus, tabs, and tab
   activation. The Markdown output remains inside the unchanged `preview_row`
   wrapper; the architecture assay keeps both artifact types out of libaryon.
9. **Plot and editor color authorities are unchanged.** The implementation
   touches no plot production file, `UiPalette`, or libaryon file, and the
   architecture assays keep their types out of `TableSkin` and `Table::draw`.
10. **No new surface or behavior escaped the substrate.** Rune, envelope,
    execution, transport, configuration, identity, placement, dashboard, table
    geometry, and interaction production paths are unchanged.

## Commit and scope trace

- `5f216f7a` — PRODUCT-0023 record and implementation plan;
- `f2dc10ae` — pre-extraction production draw oracle;
- `e8b3f956` — closed skin model and computed contrast validation;
- `94749e4a` — required skin consumption through the real renderer;
- `022f6ab6` — atomic frame/skin carriers and lifecycle/boundary assays.

Production changes are confined to `gui/{table,table_skin,colors,mod}.rs` and
`editor.rs`; the permitted architecture test is the only other code file.

## Verification

```text
cargo fmt --all -- --check
passed

cargo test -p polariton --features toron
299 passed; 5 live-service tests ignored; 24 architecture tests passed

cargo check -p polariton --no-default-features
passed (feature-disabled factory paths report existing dead-code/unused-import warnings)

cargo test -p libaryon
1411 passed; 22 ignored; 13 architecture tests and quit-latency test passed

cargo build -p polariton
passed

git diff --check
passed
```

All ten PRODUCT-0023 gates are discharged.
