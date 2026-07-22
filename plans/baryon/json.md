# JSON Syntax Coloring

JSON is a strong candidate for another hand-rolled highlighter. The grammar is
small, the useful visual distinctions are clear, and the first cut can stay in
the same lightweight `LanguageProvider` shape as TOML, CSV, Markdown, and HTML.

## Assessment

- JSON lexical highlighting is lower-risk than TOML and much simpler than HTML.
- The main useful distinction is object keys vs string values; that only needs a
  small object/array state stack in the highlighter.
- The first cut should be strict JSON, not JSONC. Comments and trailing commas
  are common enough to support later, but they should not complicate the initial
  provider.
- Do not build structural `GreenNode`s initially. JSON can support a cheap
  object/array topology later, but syntax coloring does not require it.

## 1. Strict JSON Provider

- Add `JsonProvider` for `.json`.
- Implement `json_parse.rs` with a permissive byte tokenizer:
  - strings with escape skipping and unterminated-string recovery
  - numbers with sign, fraction, and exponent scanning
  - `true`, `false`, and `null`
  - `{`, `}`, `[`, `]`, `:`, and `,`
  - whitespace/newline tokens for state transitions but no visual spans
  - unknown single-byte tokens so live typing cannot poison the pass
- Implement `json_highlight.rs` as a small state machine over tokens:
  - object keys as `Function`
  - string values as `String`
  - numbers as `Number`
  - booleans and null as `Constant`
  - braces, brackets, and commas as `Punctuation`
  - colons as `Operator`
- Implement `json_provider.rs` with `lexical_baseline` and a full-buffer
  `semantic_reactor` rerun for absolute spans.
- Leave `parse_structural_topology` unset so JSON remains highlight-only.

## 2. Focused Tests

- Tokenization tests for strings, escapes, numbers, literals, punctuation, and
  malformed/incomplete input.
- Highlighting tests for:
  - object keys vs string values
  - nested objects and arrays
  - booleans/null/numbers
  - offset-map translation
- Provider tests for extension routing, semantic offset application, and the
  default absence of structural topology.

## 3. Optional JSONC Cut

- Add `.jsonc` as a separate provider or mode only after strict JSON is stable.
- Support `//` and `/* ... */` comments as `Comment`.
- Decide explicitly whether trailing commas should be highlighted permissively
  without being treated as diagnostics.

## 4. Optional Structural Cut

- Consider a lightweight `GreenNode` topology only if folding, structural diff,
  or JSON navigation becomes valuable.
- A future topology could map the document to `RelationalTable`, object/array
  entries to `RelationalRow`, and primitive spans to `Token`.
- Keep this separate from the first syntax-coloring cut so JSON lands with a
  small blast radius.
