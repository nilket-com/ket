# Specification: TOML Support (Completed)

## Status: Complete
**Date**: April 11, 2026

## 1. Architectural Strategy (Implemented)
-   **Language Provider**: `TomlProvider` in `src/svp/toml_provider.rs`.
-   **Hand-rolled Tokenizer**: Implemented in `src/svp/toml_parse.rs`. This avoids external dependencies (like `toml_edit`) and provides high performance for large files.
-   **Theme-Aware Highlighting**: Implemented in `src/svp/toml_highlight.rs`.
    -   **Keys**: Mapped to `TokenCategory::Function` for visibility.
    -   **Headers**: Mapped to `TokenCategory::Type` for cohesive coloring.
    -   **Punctuation**: Remains as `TokenCategory::Punctuation` for visual clarity.

## 2. Structural Topology (Implemented)
-   **GreenNode Contiguity**: The `toml_semantic.rs` builder ensures every byte is covered by a `SemanticKind` node, preventing structural desyncs in the `structural_graft`.
-   **Navigation**:
    -   `[section]` / `[[section]]` → `RelationalTable`.
    -   Key-value pairs → `RelationalRow`.
    -   Motions (`]m`, `[m`) jump between sections correctly.

## 3. Cargo Version Checker (Deferred)
**Decision**: Postponed or potentially cancelled.
-   **Reasoning**: Avoids adding networking and complex caching logic to the core editor. External tools like `cargo-outdated` already handle this effectively.
-   **Visual Hint Potential**: If revisited, would use the `DiagnosticSpan` system for icons and hints.

## 4. Implementation Details
-   `src/svp/toml_parse.rs`: Fast, error-resilient tokenizer.
-   `src/svp/toml_highlight.rs`: Stack-based state machine for context-aware highlighting.
-   `src/svp/toml_semantic.rs`: Contiguous `GreenNode` builder with recursive invariant validation.
-   `src/svp/toml_provider.rs`: Orchestration and registration.

## Next Steps
-   **File Browser**: Implement the "Virtual UAST" based file browser as defined in `plans/file_browser.md`.
-   **Diff Support**: (See `plans/diff.md`).
