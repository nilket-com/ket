# Specification: File Reload

## Objective
Refresh loaded text buffers from their backing file without requiring users to close
and reopen tabs. Manual reload must protect unsaved edits, while automatic reload
should keep clean buffers current when another process changes the file.

## Commands
- `:e` reloads the current file from disk. If the current buffer is dirty, it
  enters confirmation mode with `Reload and discard changes? [y/n]`.
- `:e!` force reloads the current file from disk and discards unsaved edits.
- `:set reload` enables automatic reload and persists the setting.
- `:set noreload` disables automatic reload and persists the setting.

Existing `:e {path}` behavior continues to open the provided path. `:edit {path}`
is accepted as the long-form alias.

## Automatic Reload
`reload = true` is stored under `[ui]` in `settings.toml` and defaults to true.
The frontend sends a lightweight reload-check command periodically. The engine
tracks a file stamp for each text document using file length and modification
time.

When automatic reload is enabled:
- Clean text buffers reload immediately when their visible file path changes on
  disk.
- Dirty text buffers are not reloaded. The editor records the new disk stamp and
  shows a status message directing the user to `:e!`.
- Virtual documents such as the file browser and Markdown preview are ignored.

Reloading clears undo history and marks the buffer clean because the document is
rebased to the current file contents.
