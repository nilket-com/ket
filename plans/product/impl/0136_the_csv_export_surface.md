# Implement the CSV export surface

Scope: implement the amended PRODUCT-0136. Add `export_csv` to the local Rune
host's named receivers, shipping through **Rune notebook cells** in the
default build; an Arrow-canonical CSV writer on the result artifact with an
equivalence-gated compatibility projection for Polars-backed artifacts; the
artifact-first receiver; the export receipt channel to the interface; and the
three write floors. The implementation adds no CSV import, no Polars `csv`
feature, no menu surface, no options authoring, no other formats, and no
presentation-envelope surface.

The work lands as **one implementation commit** — the pair convention:

```text
impl: adr 0136 csv export surface
```

The stages below are work order and gates, not commit boundaries. Evidence
lands in `0136_the_csv_export_surface_evidence.md` within the same commit.

## Halt rule

PRODUCT-0136 permits the `export_csv` word on `workspace.result(...)` and
`workspace.cell(...)`, `arrow-csv` as the canonical serializer, an internal
Polars projection held byte-identical by the equivalence gate, defaults-only
format, create-never-clobbers refusal, and surfaced write failures.

Stop and return the finding to PRODUCT-0136 or a new owning record if
implementation requires any of:

- the Polars `csv` feature, or any new Polars-facing API surface;
- a menu item, dialog, file browser, or overwrite-confirmation flow;
- exporting unannotated ad hoc results, notebook plots, or registry
  enumeration;
- CSV import in any form — adapter, loader, or ingest;
- options authoring (delimiter, encoding, quoting, header, type formats);
- directory creation, product-provided path expansion or templating, or any
  write outside the authored path — a path *computed by user code* is an
  ordinary runtime string and expressly permitted by the amended §1;
- a second serialization that the equivalence gate does not leash;
- any change to `crates/libaryon`, the presentation compiler
  (`rune_view_compiler.rs` / `rune_presentation.rs`), or the durable-session
  schema.

The presentation-compiler fence is absolute: plain Rune buffers in the
default build route through the presentation pipeline, and this record does
not touch it. If shipping the word seems to require an envelope change, halt
— the plain-buffer surface is a later record, not scope creep here.

### Where work may land

- `crates/polariton/Cargo.toml` — `arrow-csv` at the pinned Arrow version;
- `crates/polariton/src/result_artifact.rs` — the export seam: canonical
  Arrow path over the existing `ResultBacking::Arrow` batches, the
  compatibility projection for `ResultBacking::Polars`, and the shared
  row-count return;
- `crates/polariton/src/rune_host.rs` — the `export_csv` associated function
  on the cell type both receivers already return, the artifact-first cell
  (projection deferred to `plot_show` and any other frame consumer), and the
  export receipts collected on the execution;
- `crates/polariton/src/gui/state.rs` — the notebook Rune path: result
  snapshots passed beside cell snapshots, the export-only execution admitted
  (the zero-output refusal keys on *no product*, not *no plot*), and the
  receipt rendered as the path-and-row-count status; and
- `crates/polariton/src/gui/messages.rs` — the receipt riding the existing
  notebook completion message, if its payload needs a field.

## Stage 0 — census (facts established during plan review)

- Both receivers return `RuneCell`, so one associated function covers both.
- The default build routes plain Rune buffers through the presentation
  scheduler (`run_rune_code`, toron arm); only notebook cells and the
  reduced head reach `rune_host`. The shipping surface is therefore the
  notebook cell, per the amended §1.
- The notebook host today receives `cells` only
  (`RuneCapabilities { cells, ..default }`); result snapshots must be added.
- `RuneCell` eagerly projects (`artifact.projection()` at receiver
  resolution); the artifact-first amendment moves that to consumers.
- The notebook path refuses zero plot requests ("did not publish a plot");
  the refusal must key on producing nothing rather than producing no plot.
- Remaining census work: confirm the Arrow backing's batches are reachable
  (or gain a module-private accessor), and pin `arrow-csv`'s default value
  formatting on the fixture — floats, large integers, timestamps, dates,
  nulls, strings requiring RFC-4180 quoting — as the canonical bytes the
  projection must reproduce.

## Stage 1 — the artifact export seam

- `ResultArtifact` gains one export entry point writing CSV to an
  `io::Write`, returning the row count. Arrow backing → `arrow_csv::Writer`.
  Polars backing → the internal projection, producing the same bytes.
- **The equivalence gate:** one logical table constructed both ways —
  `from_arrow_ipc` and `from_polars` — exported through both paths, asserted
  **byte-identical**. The fixture covers every value class the census pinned,
  including floats that expose display rounding (`0.1 + 0.2`), sub-second
  timestamps, embedded commas, quotes, and newlines, and null runs.
- Full precision is the gate's consequence, not a separate mechanism: the
  canonical path round-trips values, so the projection must.

## Stage 2 — the artifact-first receiver and the Rune word

- `RuneCell` becomes artifact-first: it carries the `Arc<ResultArtifact>` and
  defers `projection()` to the consumers that need a frame (`plot_show`);
  an Arrow-backed export never materializes a Polars frame.
- `export_csv(path)` on the cell type: runtime string path; create-new
  semantics — an existing file refuses with a diagnostic naming the path; a
  write failure surfaces through the existing Rune diagnostic path; nothing
  is silent; no directory creation.
- Unknown receivers are untouched: name resolution fails before the word is
  reached, with the existing wording.
- Each successful export appends an **export receipt** (path, row count) to
  the execution's outputs.

## Stage 3 — the notebook surface and the receipt

- The notebook Rune path passes result snapshots beside cell snapshots, so
  `workspace.result(...)` resolves in the shipping surface.
- The zero-output refusal keys on *no product*: zero plots **and** zero
  receipts keeps today's wording; zero plots with receipts is a valid
  export-only cell; the one-plot ceiling is unchanged.
- **Export-only cell:** receipts render directly as the routine
  path-and-row-count status message.
- **Mixed export-plus-plot cell:** a receipt rendered at execution time would
  be overwritten by the plot pipeline's own status ("Building notebook
  plot…", then the plotted report). Receipts therefore ride through
  `NotebookPlotReady` and one **combined** success status is emitted after
  the plot settles, naming both products. If plot construction fails, the
  plot error is preserved as the error — and the completed export is still
  made visible beside it: the file exists on disk, and a status that hides a
  finished write is a false report.

## Acceptance gates

- The equivalence gate passes byte-identically across the full fixture.
- An Arrow-backed export runs without constructing a Polars frame, asserted
  (the artifact's lazy projection cell stays unset after export).
- An export-only notebook cell succeeds with the receipt rendered; a cell
  producing nothing keeps today's refusal wording.
- A mixed export-plus-plot cell emits one combined status after the plot
  settles, with the receipt not overwritten by the plot pipeline; a mixed
  cell whose plot fails preserves the plot error while the completed export
  remains visible.
- Existing-file refusal, write-failure surfacing, and the success report
  each have a test.
- `workspace.result("absent").export_csv(...)` produces the existing
  unknown-name diagnostic, unchanged.
- Polars features in `crates/polariton/Cargo.toml` are byte-identical to the
  pre-arc list — no `csv`; asserted textually alongside the existing
  dependency guards.
- The menu roots are unchanged (the existing menu census tests keep
  passing untouched).
- `crates/libaryon`, `rune_view_compiler.rs`, and `rune_presentation.rs`
  have empty diffs.
- Full suite and architecture tests green; counts reported passed and
  ignored separately; the known 0063 flake noted if seen, not
  re-investigated.
