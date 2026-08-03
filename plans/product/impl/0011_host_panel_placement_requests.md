# Host-panel placement requests

Scope: implement PRODUCT-0011 as one narrow protocol extension and one Polariton
policy. The engine remains the sole layout authority. No work in this plan may
add persistence, durable document identity, a new projection field, panel move
or close commands, dashboard behavior, or plotting-language syntax.

## Representation

Replace `OpenHostPanel.split_direction` with the public request type:

```text
HostPanelPlacement::Split { direction }
HostPanelPlacement::TabWith { panel_id }
```

The type lives beside `EditorCommand` in libaryon's protocol and is re-exported
from `libaryon::engine`. It is a request, not a result and not a command of its
own. No acknowledgement or decline event is added.

## Stage 1: libaryon protocol and behavior

### First appearance

For an unknown `panel_id`, resolve placement before constructing its view:

- `Split` uses the current split path unchanged.
- `TabWith` resolves the named live panel's buffer, view, and pane. If all are
  live, append the new host-panel view to that pane. With `focus: false`, retain
  both the previously active tab and the previously focused pane. With
  `focus: true`, activate the appended tab and focus its pane.
- If any `TabWith` link is unavailable, decline the request and use the engine's
  ordinary horizontal-split fallback. The fallback is implementation policy,
  not a protocol guarantee; tests assert successful fallback, not its geometry.
- If a requested or fallback split cannot fit, preserve the existing
  `No room to open a host panel` behavior and release the unplaced buffer.

Viewport dimensions for a tabbed view come from the target pane's active tab,
not from the globally focused pane. This keeps construction local to the pane the
engine selected.

### Reopen

For a known `panel_id`, do not inspect placement. `focus: false` remains inert.
For `focus: true`, find the live view, activate that exact tab, then focus its
pane. This is the pre-existing focus promise made observable by tab stacks.

### Projection and disposal

`DocTabSummary.title` uses `HostPanelDescriptor.title` when its buffer is a host
panel, ahead of the file-path fallback. No projection shape changes.

Use the existing document-tab close path unchanged. Its buffer reference count
must emit one `PanelDisposed` for the closed host-panel tab without disturbing
siblings.

### Behavioral gates

Add runtime tests which exercise commands through a live engine rather than
calling layout helpers directly:

1. Open an anchor with `Split`; assert a second pane and a divider.
2. Open a sibling with `TabWith(anchor)` and `focus: false`; assert pane count is
   unchanged, focus is unchanged, the anchor stays active, and the two tab
   summaries carry descriptor titles in order.
3. Activate and reopen tabs to prove existing-panel placement is ignored and
   `focus: true` activates the exact tab.
4. Request `TabWith` a missing id and assert the panel appears without an error.
5. Close one host-panel tab and observe exactly its disposal while the sibling
   remains projected.

## Stage 2: Polariton grouping policy

All existing non-view panels translate mechanically:

- query results keep `Split { Horizontal }`;
- dashboard keeps `Split { Vertical }`;
- test-only direct host panels keep the direction they currently exercise.

`prepare_rune_view_panels` already receives views in PRODUCT-0010 presentation
order and allocates identities before results arrive. Resolve the first identity
as the collection anchor. For an unknown panel:

- index zero requests `Split { Horizontal }`;
- every later index requests `TabWith { panel_id: anchor }`.

Known panels receive only `RefreshHostPanel`; reruns never relocate author-owned
layout. A later new view may request the current first view as its anchor, while
existing views remain wherever the author put them.

### Behavioral gate

Drive `CodeEditor::prepare_rune_view_panels` with four named views. Through the
real engine projection, assert one additional pane contains four tab summaries
in source order and that the original document pane retains focus. Existing
query-result and dashboard tests remain green and provide the regression gates
for their unchanged split requests.

## Permitted files

- `crates/libaryon/src/engine/protocol.rs`
- `crates/libaryon/src/engine/mod.rs`
- `crates/libaryon/src/engine/runtime.rs`
- `crates/libaryon/src/engine/runtime/system.rs` for live protocol tests
- `crates/libaryon/src/engine/render.rs`
- `crates/polariton/src/editor.rs`
- `plans/product/adr/0011_host_panel_placement_requests.md`
- this plan and its evidence file
- `plans/product/corpus/rune_workspace_corpus.md` in a separate evidence commit

Any need to edit Toron, the transport, dashboard state, plotting-language
analysis, projection structs, layout persistence, or commands other than
`OpenHostPanel` stops the implementation and returns to a new record.

## Verification

```text
cargo fmt --all -- --check
cargo test -p libaryon
cargo test -p polariton --features toron
cargo check -p polariton --no-default-features
```

The implementation evidence must map every PRODUCT-0011 acceptance gate to a
behavioral test and record that the halt rule did not fire.
