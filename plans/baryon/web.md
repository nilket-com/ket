# Web Language Support

Open the door for practical web-language support without embedding compiler-grade
tooling for every language path. The near-term goal is excellent lexical
highlighting for HTML and useful JavaScript/TypeScript support, while keeping
diagnostics and formatting in an async tool-integration layer.

## 1. Manual HTML Highlighter

- Add a lightweight `.html`/`.htm` provider modeled after TOML, CSV, and
  Markdown.
- Tokenize comments, doctype, tag delimiters, tag names, closing tags,
  attribute names, equals signs, quoted/unquoted attribute values, and entities.
- Highlight text content conservatively; leave normal prose mostly unstyled.
- Detect `<script>` and `<style>` raw-text regions, but initially render their
  bodies as plain/string-like spans instead of recursively parsing JS or CSS.
- Do not build structural `GreenNode`s in this cut. HTML should work as a
  highlight-only language first.

## 2. Biome-Backed JS/TS Provider

- Add standalone `.js`, `.jsx`, `.ts`, and `.tsx` support using Biome parser
  crates if they build cleanly in Baryon.
- Keep this in `LanguageProvider::lexical_baseline`, producing normal
  `HighlightSpan`s over the viewport.
- Map Biome syntax tokens into Baryon's existing categories: keywords, strings,
  comments, numbers, punctuation, operators, identifiers, functions, classes,
  and constants where available.
- Defer diagnostics, formatting, code actions, and project intelligence to a
  later async integration; the first provider should be color only.

## 3. Embedded Script Highlighting

- Reuse the JS/TS lexical provider inside HTML `<script>` ranges.
- Add script-body offsets back to document coordinates before emitting spans.
- Start with ordinary inline JavaScript. Treat module scripts the same for
  highlighting unless a later pass needs better semantic distinctions.
- Keep `<style>` bodies plain until there is a CSS highlighter or external
  grammar path.

## 4. Biome Tool Integration

- Add Biome CLI or LSP integration outside the synchronous lexical baseline.
- Use it for diagnostics, formatting, range formatting, code actions, and safe
  fixes.
- Prefer a separate async manager instead of coupling process lifecycle,
  document sync, or server crashes to `LanguageProvider`.
- Treat Biome LSP as an editor-services layer, not a syntax-coloring API.
