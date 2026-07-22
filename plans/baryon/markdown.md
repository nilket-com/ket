# Specification: Markdown Support

## Objective
Add first-class Markdown support in two tracks:

1. Basic `.md` and `.markdown` syntax highlighting through the existing SVP language-provider pipeline.
2. A rendered Markdown preview in a side pane, implemented as an image viewport rather than a terminal text document.

## Phase 1: Basic Syntax Highlighting

Status: complete.

### Scope
- Add a `MarkdownProvider` and register it with the language registry.
- Support `.md` and `.markdown` extensions.
- Add a lightweight, error-resilient tokenizer/highlighter for common Markdown constructs:
  - ATX headings.
  - Fenced code blocks.
  - Inline code spans.
  - Links and image links.
  - List markers.
  - Block quotes.
  - Horizontal rules.
  - HTML comments.
  - Emphasis markers.
- Reuse existing `TokenCategory` values instead of adding theme categories.
- Re-run highlighting over the full document in `semantic_reactor` so constructs that need context, especially fenced code blocks, settle after the async pass.

### Non-Goals
- No Markdown rendered preview in phase 1.
- No full CommonMark parser for highlighting.
- No nested inline parser perfection. The goal is stable, helpful color without disrupting editing.
- No language-aware highlighting inside fenced code blocks yet.

### Suggested Category Mapping
- Heading marker and heading text: `Type`.
- Link label: `Function`.
- Link destination: `String`.
- Inline code and fenced code body: `String`.
- Fences, list markers, punctuation, and emphasis delimiters: `Punctuation` or `Operator`.
- Block quote marker and HTML comments: `Comment`.

## Phase 2: Image-Rendered Preview Pane

Status: in progress. The previous terminal virtual-document preview has been removed. The preview track should only proceed as an image-rendered pane.

### Architecture
- Add a new command, likely `:mdpreview` / `:mp`, backed by `EditorCommand::MarkdownPreview`.
- Split the focused pane vertically and create a read-only preview pane to the right.
- Add a new `DocumentKind::MarkdownImagePreview(MarkdownImagePreviewState)`.
- Do not create a UAST root, virtual token stream, or editable/readable text document for the preview.
- Store the source pane id, source path, source state id, source snapshot text, scroll offset, and a source-line to preview-pixel map.
- Rebuild the image preview snapshot when the source document state changes.
- Render the preview pane through the UI image protocol path, similar to the minimap and diff bridge.
- Block editing commands in preview panes, like the file browser does.

### Parser Choice
- Use an explicit direct dependency for preview parsing.
- Preferred v1 parser: `pulldown-cmark` with default features disabled.
- Avoid relying on the current transitive `pulldown-cmark` from `ra_ap_ide`.

### Rendering Strategy
- Current cut: render Markdown blocks into an RGBA image sized to the pane's content area.
- Use actual rasterized layout semantics: larger headings, paragraph spacing, list indentation, quote rules, code block backgrounds, and table blocks when possible.
- Render preview text with anti-aliased system font rasterization; keep the 8x8 bitmap font only as a fallback when no usable font can be loaded.
- Render only the current visible preview region whenever possible rather than a whole-document bitmap.
- Provide a terminal-cell fallback only as a loading/error placeholder, not as the primary preview.
- Keep the implementation independent from the source editor's Markdown syntax highlighter.

### Incremental Cuts
- Cut 1: remove the old virtual document preview, add `MarkdownImagePreview`, and render a basic image pane.
- Cut 2: replace line-oriented Markdown heuristics with `pulldown-cmark` event/block extraction and source byte ranges. Done.
- Cut 2a: replace scaled bitmap preview glyphs with anti-aliased system font rasterization. Done.
- Cut 3: use the parser block ranges to build a source-line to preview-pixel map and sync scroll by rendered block position instead of source line, including rendered-bottom alignment when the source viewport reaches EOF. Done.
- Cut 4: improve tables, task-list checkboxes, links, images, and fenced-code language highlighting.

### Terminal Constraints
- Terminals are fixed cell grids; they cannot display real larger fonts through ordinary TUI text drawing.
- `ratatui-image` can display raster images in capable terminals and is the correct path for real heading sizes and richer layout.
- Preview behavior must degrade gracefully when the image protocol is unavailable.

### Scroll Sync
- Diff sync is a useful pattern for paired panes, but Markdown preview needs its own mapping.
- Use source-line to preview-pixel spans generated during Markdown layout.
- On source navigation or scroll, update the preview snapshot's scroll offset before rendering.

## Phase 3: Polish
- Highlight fenced code blocks using registered Baryon language providers when the fence info string matches a language id or extension.
- Add robust table layout.
- Add richer checkbox/task-list presentation.
- Add image support for Markdown image tags.
- Add link hover/status affordances when the mouse protocol can support it.

## Implementation Order
1. Implement phase 1 highlighting.
2. Remove the terminal virtual-document Markdown preview. Done.
3. Add command and read-only `MarkdownImagePreview` pane skeleton. Done.
4. Add UI image controller and fallback placeholder for preview panes. Done.
5. Add initial RGBA rendering for headings, paragraphs, lists, quotes, and code blocks. Done.
6. Switch preview layout input to `pulldown-cmark` parser events with source ranges. Done.
7. Add source-to-preview-pixel scroll sync. Done.
8. Add richer styling, tables, links, images, and fenced-code language highlighting.
