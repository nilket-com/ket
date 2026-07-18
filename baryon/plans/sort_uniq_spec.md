# Specification for `:sort` and `:uniq` Commands

## Overview
Implement the Vim-like ex-commands `:sort` and `:uniq`. Both commands will allow operating on an optional line range, defaulting to the current selection or file. They modify the text by sorting or removing duplicate lines within the specified range.

## 1. Protocol Layer (`src/engine/protocol.rs`)
- **Rename `SubstituteRange`:** Rename `SubstituteRange` to a more generic name like `CommandRange` to allow it to be shared across commands (`:s`, `:sort`, `:uniq`). Update existing references in `SubstituteAll` and `SubstituteConfirm` commands.
- **Add `EditorCommand` Variants:**
  - `Sort { range: CommandRange, reverse: bool, unique: bool, numeric: bool, ignore_case: bool }`
  - `Uniq { range: CommandRange, ignore_case: bool }`

## 2. Command Parsing (`src/ui/command.rs` and `src/ui/visual.rs`)
- **Fix Visual Mode Command Entry (`src/ui/visual.rs`):**
  - In `handle_visual_key`, handle `KeyCode::Char(':')` by transitioning to command mode.
  - Set `self.current_mode = EditorMode::Command;`.
  - Pre-populate the command buffer with the visual range: `self.command_buffer = "'<,'>".to_string();`.
  - Clear prefixes using `self.clear_prefixes();`.
- **Extract Range Parsing:** Extract the range parsing logic from `parse_substitute` into a reusable function, e.g., `parse_command_range(cmd: &str, cursor_line: DocLine) -> Option<(&str, CommandRange)>`. This function should return the remaining unparsed command string and the parsed range.
  - Make sure the range parser supports `'<,'>` (which should map to `CommandRange::Visual` or `SubstituteRange::Visual` representing the current visual selection). If there isn't a `Visual` variant, add one to `CommandRange`.
- **Parse `:[range]sort`:** Parse the `:sort` command.
  - Support the `!` modifier for reverse sorting (`:sort!`).
  - Support trailing flags like `i` (ignore case), `u` (unique), and `n` (numeric).
  - Dispatch the `EditorCommand::Sort`.
- **Parse `:[range]uniq`:** Parse the `:uniq` command.
  - Support the `i` (ignore case) flag.
  - Dispatch the `EditorCommand::Uniq`.

## 3. Engine Implementation (`src/engine/runtime.rs` and `src/engine/runtime/edit.rs` or `search.rs`)
- **Dispatch Commands:** Update `runtime.rs` to route `EditorCommand::Sort` and `EditorCommand::Uniq` to new handler functions, e.g., `handle_sort_command` and `handle_uniq_command` (they can be placed in `edit.rs` or alongside `substitute_text_deltas` in `core.rs`).
- **Processing Logic:**
  1. Use `resolve_byte_range` to determine the start and end bytes of the targeted `CommandRange` in the document.
  2. Read the text segment using `read_loaded_document`.
  3. Split the text into lines (maintaining line endings if possible, or appending them later).
  4. **For `:sort`:** Apply the standard sort, reverse sort, numeric sort, and/or unique filtering based on the provided flags.
  5. **For `:uniq`:** Iterate over the lines, retaining only the first occurrence of adjacent duplicate lines (as per standard `:uniq` behavior, which typically only deduplicates *adjacent* matching lines).
  6. Re-join the processed lines into a single `String`.
- **Apply Delta:**
  - Generate a single `TextDelta` that deletes the text within the originally resolved byte range and inserts the newly processed `String`.
  - Pass the delta to `apply_document_deltas` to commit the change to the undo history and trigger a rerender.
  - Update the `status_message` to reflect the number of lines modified or sorted.
