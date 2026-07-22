# Specification for Escape Deselection Feature

## Overview
Currently, executing an Ex-command (like `:sort`) after making a visual selection leaves the visual selection actively highlighted in the engine, even though the frontend returns to `Normal` mode. Furthermore, pressing `Escape` in `Normal` mode does not clear this residual selection. 

To provide a more intuitive, Vim-like experience, we need to ensure that the `Escape` key acts as a universal clearing mechanism for active visual selections when in `Normal` mode, and we should also clear the selection when aborting out of `Command` mode.

## 1. Normal Mode Escape Handler (`src/ui/normal.rs`)
- **Objective:** Pressing `Escape` in `Normal` mode should send a command to the engine to clear the visual selection.
- **Action:** In the `handle_normal_key` function, locate the `KeyCode::Esc` match arm.
- **Implementation:** Add `self.send_command(EditorCommand::ClearVisualSelection);` within the block. It should look like this:
  ```rust
  KeyCode::Esc => {
      let has_active_search = self
          .current_viewport
          .as_ref()
          .and_then(|view| view.search_pattern.as_ref())
          .is_some();
      self.clear_prefixes();
      self.send_command(EditorCommand::ClearVisualSelection); // Add this line
      if has_active_search {
          self.send_command(EditorCommand::ClearSearch);
      }
  }
  ```

## 2. Command Mode Escape Handler (`src/ui/command.rs`)
- **Objective:** Aborting out of `Command` mode (which happens when you press `Escape` after typing `:`) should also clear any visual selection that might have been active.
- **Action:** In the `handle_command_key` function, locate the `KeyCode::Esc` match arm.
- **Implementation:** Add `self.send_command(EditorCommand::ClearVisualSelection);` before transitioning to `EditorMode::Normal`.

## 3. Command Execution (`src/ui/command.rs`)
- **Objective:** Executing a command from `Command` mode (by pressing `Enter`) should generally clear the visual selection.
- **Action:** In the `handle_command_key` function, locate the `KeyCode::Enter` match arm.
- **Implementation:** Add `self.send_command(EditorCommand::ClearVisualSelection);` prior to setting `self.current_mode = EditorMode::Normal;`. This ensures that after running `:sort` (or any other command), the highlighting is correctly dismissed.