# Rune line view sink

Scope: implement PRODUCT-0012 as one coherent cut. The implementation extends
the accepted Rune view vocabulary and routes it through Polariton's existing line
specification, kernel, renderer, and ordinary host-panel surface.

## Halt rule

No files under libaryon, Toron, the transport, plot kernels, or plot renderers may
change. No layout or dashboard policy may change. If an acceptance gate requires
one of those changes, stop and return the finding to PRODUCT-0012.

## Stage 1: envelope and normal form

- Add `RuneViewForm::Line` and its diagnostic label.
- Recognize `plot.line(r, x, y)`, `r.line(x, y)`, and contextual
  `line(x, y)` with the inherited provenance, shadowing, and atomicity rules.
- Require exactly two string literals.
- Prove the three spellings yield the same origin, intent, form, and retained
  source.

Discharges gates 1 and 2.

## Stage 2: authored specification resolution

- Admit explicit x/y channels for `PlotType::Line`.
- Project them to `ResolvedKernelFields::Line { series: None, x, y }`.
- Prove exact field identity and normal domain resolution against a frame.

Discharges gate 3.

## Stage 3: execution and presentation evidence

- Extend shared-result evidence with a line sink.
- Change the two-origin dogfood fixture from scatter to line and complete every
  origin through the real head.
- Assert the line owns an ordinary plot panel, the two tables remain distinct,
  presentation order is unchanged, and dashboard state stays absent.
- Add an ignored live Toron gate and update the executable corpus.

Discharges gates 4, 5, and 6.

## Permitted files

- `crates/polariton/src/rune_relation_envelope.rs`
- `crates/polariton/src/plot/spec/mod.rs`
- `crates/polariton/src/editor.rs`
- `crates/polariton/src/gui/state.rs`
- `crates/polariton/src/adapters/toron/mod.rs`
- `plans/product/corpus/rune_workspace_corpus.md`
- PRODUCT-0012's ADR and this plan

The external dogfood buffer may be updated as user acceptance evidence, but it is
not part of the repository commit.
