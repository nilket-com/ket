# PRODUCT-0136 evidence — the CSV export surface

Implementation of PRODUCT-0136, following
`plans/product/impl/0136_the_csv_export_surface.md`. This evidence lands in
the implementation half of the plan/implementation pair.

Verification at close: `cargo test -p polariton --bin polariton` — **960
passed, 0 failed, 8 ignored**; `cargo test -p polariton --test
architecture_test` — **97 passed**. The known pre-existing `product_0063`
parallel-run flake did not fire in this close run.

`cargo clippy -p polariton --all-targets -- -D warnings` is not currently a
green repository gate: Rust 1.97 reports 29 pre-existing diagnostics across
the plotting, presentation, relation-envelope, table, and architecture-test
surfaces (for example `clippy::manual_saturating_arithmetic` in
`plot/common.rs`, `clippy::bool_assert_comparison` in
`architecture_test.rs`, and existing large-enum diagnostics). This arc fixed
its one direct `needless_update` finding in the notebook capabilities
construction; no PRODUCT-0136 lint exemption was added. Repairing the
repository-wide lint baseline is independent work, not scope for CSV export.

## Gate ledger

### Stage 0 — seams confirmed

- `ResultArtifact` retains Arrow `RecordBatch`es, so the canonical writer is
  direct and needs no new public artifact surface.
- A notebook execution receives named cell snapshots and, now, the registry
  snapshot required by `workspace.result(...)`. Plain Rune buffers remain in
  the presentation pipeline and outside this record.
- `RuneCell` previously projected on resolution; it now carries the artifact
  until a frame consumer (`plot_show`) requires its compatibility projection.

### Stage 1 — canonical artifact export

- `arrow-csv` is pinned at Arrow 58.3.0. `ResultArtifact::write_csv` writes
  Arrow backing directly with its default header/comma/RFC-4180/UTF-8 policy
  and returns the row count.
- The Polars compatibility path uses the already admitted IPC seam and the
  same Arrow writer; it adds neither the Polars `csv` feature nor a new
  Polars-facing export API.
- `product_0136_arrow_csv_is_canonical_and_keeps_the_projection_lazy` pins
  the canonical bytes for large integers, round-trip floats, sub-second
  timestamps, dates, embedded comma/quote/newline text, and null runs.
- `product_0136_polars_compatibility_projection_matches_arrow_csv_bytes`
  asserts byte identity across both backing paths. The Arrow direct test also
  proves export leaves the lazy Polars projection unset.

### Stage 2 — Rune word and write discipline

- `workspace.cell(name).export_csv(path)` and
  `workspace.result(name).export_csv(path)` both write through the artifact
  seam and append an explicit `ExportReceipt` only after completion.
- The path is the runtime Rune string as amended: relative paths remain
  process-CWD relative; Polariton supplies no expansion or templating and
  creates no directories.
- `OpenOptions::create_new(true)` enforces create-never-clobbers. Existing
  files refuse with their path; missing-parent and later writer failures are
  Rune diagnostics with their path, never silent.
- The host tests cover both receivers, quoted CSV content, collision refusal,
  a surfaced write failure, and the unchanged unknown-result diagnostic.

### Stage 3 — notebook receipt surface

- The default-build notebook path passes result snapshots beside cell
  snapshots. Export-only cells are valid; the prior "did not publish a plot"
  refusal remains for a cell with neither plot nor receipt.
- Receipts carry through `RunRuneCellResult` and `NotebookPlotReady`. An
  export-only cell reports the routine path-and-row-count status; a mixed
  cell reports one combined plot-and-export status after the plot settles.
- If plot planning fails after a completed export, the Rune error remains in
  `status_error` while the receipt remains visible in `status_msg`.
  `product_0136_notebook_receipts_survive_without_a_plot_and_beside_plot_errors`
  and `product_0136_notebook_refuses_only_when_it_produces_nothing` pin those
  cases; the existing notebook plot surface test asserts the combined success
  status is not overwritten.

## Cross-stage guards

- `product_0136_csv_export_keeps_arrow_canonical_and_polars_csv_unadmitted`
  textually guards the exact Arrow dependency and the absence of a Polars CSV
  feature.
- No menu, dialog, file browser, import, format options, registry enumeration,
  or presentation-compiler surface was added.
- `crates/libaryon`, `rune_view_compiler.rs`, `rune_presentation.rs`, and the
  durable-session schema remain outside the diff.

## Left open, correctly

CSV import remains held for a Toron Arrow-IPC ingest contract. Other formats,
options authoring, directory creation, overwrite confirmation, a grid command
surface, and plain presentation-buffer export each require their own record.
