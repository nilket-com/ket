# Specification for SQL Language Support (with dbt/Jinja templating)

## Overview
Implement deep, compiler-grade SQL support in Baryon using the `sqlparser-rs` crate. This implementation completely avoids LSP/JSON-RPC in favor of a native, embedded approach. To gracefully handle embedded templating languages like Jinja and dbt (e.g., `{{ ref('table') }}`, `{% if ... %}`), we will use **Byte-Preserving Shadow Masking** before passing the text to the parser.

## 1. Dependencies
Add the `sqlparser` crate to `Cargo.toml`.
- Add `sqlparser = "0.46"` (or latest stable) to dependencies.

## 2. File Structure
Create the following files in `src/svp/`:
- `sql_provider.rs`: Implements the `LanguageProvider` trait for SQL.
- `sql_parse.rs`: Contains the logic for the Byte-Preserving Shadow Masking and the AST parsing.
- `sql_highlight.rs`: Maps `sqlparser` tokens and AST nodes to Baryon's `TokenCategory` and `SemanticKind`.
- `sql_semantic.rs`: AST walking logic to generate `GreenNode`s.

Update `src/svp/mod.rs` to export the new modules and include the `SqlProvider` in whatever registry holds language providers (likely in `src/engine/language.rs` or `src/svp/ingest.rs`).

## 3. Byte-Preserving Shadow Masking (`src/svp/sql_parse.rs`)
Implement a fast pre-processor that scans a raw byte slice for Jinja/dbt tags and produces two things:
1. **Macro Spans:** A list of `(start_byte, end_byte)` tuples representing the exact locations of `{{ ... }}`, `{% ... %}`, and `{# ... #}` blocks.
2. **Shadow String:** An owned `String` of the exact same byte length as the original input, where:
   - `{{ ... }}` is replaced with `"___...___"` (double quotes filled with underscores matching the exact length). This tricks the SQL parser into treating it as a valid quoted identifier.
   - `{% ... %}` and `{# ... #}` are replaced with spaces (` `) of the exact same length. This erases control flow and comments from the parser's view without altering subsequent byte offsets.

## 4. Phase 1: Lexical Baseline (`sql_provider.rs` -> `lexical_baseline`)
- **Input:** `viewport_bytes` and `offset_map`.
- **Logic:**
  1. Run the Byte-Preserving Shadow Masking pre-processor on `viewport_bytes`.
  2. Emit `HighlightSpan`s for all discovered Macro Spans using a distinct category (e.g., `TokenCategory::Macro`).
  3. Tokenize the *Shadow String* using `sqlparser::tokenizer::Tokenizer::new(&dialect, &shadow_string).tokenize()`. Use a forgiving dialect like `GenericDialect` or `PostgreSqlDialect`.
  4. Iterate through the resulting tokens. Map `sqlparser::dialect::Token` variants to Baryon's `TokenCategory` (e.g., `Keyword`, `String`, `Number`, `Operator`).
  5. Ignore any tokens whose byte ranges fall entirely within a known Macro Span (since we already colored those blocks).
  6. Return the combined, sorted list of `HighlightSpan`s.
- **Error Handling:** If `Tokenizer::tokenize()` fails, fallback to rendering just the Macro Spans and formatting the rest as plain text, or safely ignore the error and render whatever tokens successfully parsed.

## 5. Phase 2: Semantic Reactor (`sql_provider.rs` -> `semantic_ast`)
- **Input:** `file_bytes`.
- **Logic:**
  1. Run the Byte-Preserving Shadow Masking pre-processor on the entire file.
  2. Parse the *Shadow String* using `sqlparser::parser::Parser::parse_sql(&dialect, &shadow_string)`.
  3. **Success Path:** If parsing succeeds, walk the resulting `Vec<Statement>` AST.
     - Map high-level constructs (e.g., `Statement::Query`, CTEs) to `SemanticKind::FunctionDef` or similar structural boundaries to enable code folding.
     - Map Table names and Column identifiers to `SemanticKind::Class`, `SemanticKind::Variable`, etc.
     - Return the built `GreenNode` tree.
  4. **Error Path (Graceful Degradation):** If parsing fails (which is expected for complex, disjointed dbt macros), catch the error. Return a flat `GreenNode` containing only the lexical tokens, or an empty structural tree, relying on Phase 1 to keep the viewport highlighted.

## 6. Integration
- Instantiate `SqlProvider` and register it in the application's language initialization sequence.
- Bind the `.sql` extension (and potentially `.j2` or `.dbt` if appropriate) to this provider.
- Set the UI label to `"SQL (dbt)"` or `"SQL"`.