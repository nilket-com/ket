# Specification: CSV / TSV Support

## Objective
Implement high-performance, error-resilient support for Comma-Separated Values (CSV) and Tab-Separated Values (TSV) using Baryon's `RelationalTable` and `RelationalRow` UAST infrastructure.

## 1. Architectural Strategy

-   **Language Provider**: Implement `CsvProvider` (handles `.csv`) and `TsvProvider` (handles `.tsv`) in `src/svp/csv_provider.rs`.
-   **Hand-rolled Tokenizer**: A fast, single-pass tokenizer in `src/svp/csv_parse.rs` that handles:
    -   Field delimiters (`,` for CSV, `\t` for TSV).
    -   Quoted fields (including escaped quotes `""`).
    -   Newlines within quotes.
-   **Structural Mapping**:
    -   The entire file is a `RelationalTable`.
    -   Each line (record) is a `RelationalRow`.
    -   This enables standard motions like `]m` / `[m` to jump between records.

## 2. Highlighting Strategy

To provide visual "columnar" feel without full grid alignment:
-   **Alternating Columns**: Use two different `TokenCategory` types for adjacent columns to provide visual separation.
    -   Column 1, 3, 5... → `TokenCategory::Function`
    -   Column 2, 4, 6... → `TokenCategory::Variable`
-   **Delimiters**: Mapped to `TokenCategory::Punctuation` (subtle).
-   **Quotes**: Mapped to `TokenCategory::String`.
-   **Numbers**: Heuristically detect numbers to map them to `TokenCategory::Number`.

## 3. Structural Contiguity (GreenNode)

Consistent with the TOML implementation, the `GreenNode` builder must ensure zero gaps:
-   **Rows**: Map each logical record to a `RelationalRow`.
-   **Gap Filling**: Explicitly emit `Token` leaves for newlines and any trailing trivia.
-   **Invariant**: `node.byte_len == node.children.iter().map(|c| c.byte_len).sum()`.

## 4. Implementation Roadmap

1.  **Tokenizer**: Implement the RFC 4180-compliant tokenizer in `src/svp/csv_parse.rs`.
2.  **Highlighter**: Implement alternating column highlighting in `src/svp/csv_highlight.rs`.
3.  **Topology**: Implement the `GreenNode` builder in `src/svp/csv_semantic.rs`.
4.  **Provider**: Register `CsvProvider` and `TsvProvider` in the language registry.

## 5. Future Stretch Goals (Out of Scope)
-   **Alignment**: Virtual text padding to align columns visually.
-   **Sorting**: Engine-level command to sort the `RelationalTable` based on a specific column.
-   **Filtering**: UAST queries to hide rows that don't match a criterion.
