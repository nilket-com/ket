# Specification: Rune-only configuration

Baryon is pre-release: Rune becomes the sole user configuration mechanism and the TOML
settings file is removed outright — no compatibility layer, no dual-source precedence
rules. This plan covers the replacement API, the startup semantics, the fate of engine
persistence, the deletion, and the worked migration of the existing dotfile.

## 1. What settings.toml is today (two roles, one file)

1. **User-authored startup preferences**: theme, six booleans (`minimap`, `wrap`,
   `line_numbers`, `relative_line_numbers`, `visible_whitespace`, `reload`), `savedurability`,
   `[ui.colors.*]` overrides, and `[theme.languages]` per-language themes. Parsed by the
   hand-rolled TOML reader in `engine/settings.rs` into `EngineSettings`, threaded into
   `EditorSession::new`.
2. **Machine-written state**: `persist_ui_settings` re-renders and rewrites the whole file
   whenever a persist-flavored command runs (`PersistLangTheme`, persistent `:set` variants
   via `persist_view_settings` in `runtime/system.rs`). `BOOLEAN_SETTING_KEYS` couples the
   option registry's names to the TOML keys.

Role 1 moves to Rune. Role 2 becomes a syntax-aware source edit rather than a generated
snapshot: ordinary `:set` operations may replace only the literal configuration call for
the one canonical key they own. The explicit `:set init` export remains the bulk
reconciliation tool, while `:set init!` is the deliberate replacement escape hatch.

## 2. Decision: `:set` performs a targeted Rune edit

Persist-flavored operations update the live session first, then edit the configured
`init.rn` when the source has a structurally safe update point:

- Parse with Rune's AST and recognize a top-level `let config = editor.config();` binding,
  a terminal `Ok(())`, and literal `config.set`, `config.ui_color`, or
  `config.language_theme` calls.
- Replace, insert, or remove only the statement for the command's canonical key. Preserve
  every unrelated byte of user code. Canonical aliases and duplicate calls collapse to
  one statement.
- Reparse the candidate before writing, compare the exact source bytes again immediately
  before the atomic replacement, and refuse a stale source instead of merging blindly.
- If `init.rn` is open and clean, compare its UAST snapshot with disk and reload the
  resulting file through the normal document pipeline. If it is dirty, keep the session
  change but report `session only`; never save or discard the user's edits.
- Create an absent file as a minimal one-setting program. A reset against an absent file is
  already satisfied and creates nothing.
- Embedders which do not configure an init script remain session-only. This prevents a
  library engine or test from discovering and mutating the Baryon user's default path.

`:set init` remains a bulk snapshot/reconciliation command for recovery and explicit
export. It uses the same AST key recognition, removes cleared built-in color/language
overrides, and preserves unknown configuration calls and other user statements.

Rejected alternative: a machine-owned state file in the XDG state dir applied after
`init.rn`. It would preserve the UX but reintroduce two config sources and an ordering
contract; targeted Rune edits keep one source of truth.

## 3. Rune API: one authority, the option registry

No parallel settings surface. Built-in options are set through the same option-registry
names `:set` already uses (`option_registry.rs` is the single authority; its test already
asserts registry keys == `BOOLEAN_SETTING_KEYS`):

```rune
pub fn main(editor) {
    baryon::set(editor, "theme", "purples")?;
    baryon::set(editor, "minimap", false)?;
    baryon::set(editor, "wrap", true)?;
    baryon::set(editor, "savedurability", "atomic")?;

    baryon::ui_color(editor, "accent", "#FFFFFF")?;
    baryon::ui_color(editor, "gutter.surface", "#080808")?;

    baryon::language_theme(editor, "rust", "viridis_cs8")?;
}
```

- `baryon::set(editor, name, value) -> Result<(), OptionError>`: routes through the option
  registry exactly as `:set name=value` does — same names, same validation, same typed
  errors (unknown option, invalid value). Value is a Rune string/bool/int; conversion
  errors are expected `Err`, never a VM panic.
- `baryon::ui_color(editor, key, hex)` and `baryon::language_theme(editor, lang, theme)`:
  colors and language themes are structured maps, not flat options, so they get dedicated
  typed calls. Keys use the dotted paths from the old `[ui.colors]` tables; hex parsing and
  theme-name validation reuse the existing `UiColorOverrides` / `Theme::try_new` checks and
  return the same diagnostics `settings.rs` produced as startup warnings.
- All three are ordinary stateful host calls: they charge the HostBudget, are valid during
  source evaluation (`main`) *and* inside any callback — unlike `spawn`, which stays
  callback-only. Setting an option from a callback behaves exactly like `:set` at that
  moment (render effects included, via `HostEffects.needs_render`).
- **Error ergonomics**: `?` on every line is the intended style, so `main`'s return value
  becomes meaningful: publication inspects it, and a returned `Err` is reported through the
  startup status (naming the option and value) exactly like a script diagnostic. Today the
  return value is ignored, which would make `?` silently swallow config typos — closing
  that hole is part of this plan, and it applies to every callback ABI uniformly (a
  callback returning `Err` reports like a thrown error).

## 4. Startup semantics

- The engine starts from compiled-in defaults (`EngineSettings::default()` minus the file
  load). `init.rn` runs at its existing point in startup under `STARTUP_INSTRUCTION_BUDGET`;
  because that happens before the first viewport is rendered, a theme set in `main` is the
  first frame's theme — matching today's TOML behavior with no new sequencing.
- Script errors keep the existing contract: compile/eval failure reports through the
  startup status; the editor comes up on defaults. Partial application stands (settings set
  before the error stick) — same as any failed callback's committed edits, and simpler than
  transactional config.
- `EngineSettings` survives as the internal defaults+current-state struct; only its TOML
  ingress/egress dies.

## 5. Deletion

- `engine/settings.rs`: TOML parser (`parse_settings`, `strip_toml_comment`,
  `parse_toml_*`), renderer (`render_settings`, `render_ui_colors`, `escape_toml_*`),
  `load_settings*`, `persist_ui_settings`, and the settings-path platform resolution
  (`default_settings_path*`, `SettingsPathEnv`, XDG/AppData probing) — all removed with
  their tests. `SaveDurability` and `EngineSettings` (sans `settings_path`) remain.
  Platform-aware config-directory probing is retained in a smaller `init.rn` path
  resolver; only the TOML settings-file path and persistence plumbing disappear.
- `runtime/system.rs`: TOML persistence disappears. Persist-flavored commands retain their
  session state transition and route the corresponding canonical single-key edit to
  `init.rn` per §2.
- `RuntimeContext.settings_path` and every plumbing thread of it disappears.
- `BOOLEAN_SETTING_KEYS` moves to (or is re-derived in) `option_registry.rs`, which becomes
  the sole owner of option names.

## 6. Worked migration (the author's dotfile)

`~/.config/baryon/settings.toml` translates to `~/.config/baryon/init.rn` (the path the
host already loads):

```rune
pub fn main(editor) {
    baryon::set(editor, "theme", "purples")?;
    baryon::set(editor, "minimap", false)?;
    baryon::set(editor, "wrap", true)?;
    baryon::set(editor, "line_numbers", true)?;
    baryon::set(editor, "relative_line_numbers", false)?;
    baryon::set(editor, "visible_whitespace", true)?;
    baryon::set(editor, "reload", false)?;
    baryon::set(editor, "savedurability", "atomic")?;

    baryon::ui_color(editor, "accent", "#FFFFFF")?;
    baryon::ui_color(editor, "surface", "#220033")?;
    baryon::ui_color(editor, "editor.background", "#000008")?;
    baryon::ui_color(editor, "search.highlight", "#9900FF")?;
    baryon::ui_color(editor, "minimap.surface", "#000002")?;
    baryon::ui_color(editor, "gutter.accent", "#FFFFFF")?;
    baryon::ui_color(editor, "gutter.surface", "#080808")?;
    baryon::ui_color(editor, "gutter.text", "#8844AA")?;
    baryon::ui_color(editor, "preview.surface", "#000000")?;

    baryon::language_theme(editor, "markdown", "purples_rcC")?;
    baryon::language_theme(editor, "md", "onedark_s3")?;
    baryon::language_theme(editor, "rust", "viridis_cs8")?;
}
```

This file doubles as the first repo example. An `examples/` directory follows as its own
small phase: `config.rn` (the above, genericized), `jobs.rn`, `autocmds.rn`, `commands.rn`
— each a runnable, commented script exercising one API family.

## 7. Acceptance and regression tests

1. A startup script setting theme + colors + toggles produces a first frame with all of
   them applied (full-engine test, like the existing host.rs integration tests).
2. `baryon::set` matches `:set` byte-for-byte on names, validation, and error text; unknown
   options and malformed values are typed `Err`s, and a quota-exhausted script still panics
   the VM per the `baryon::*` contract.
3. Setting options from a command/autocmd/job-completion callback takes effect and renders,
   identically to interactive `:set`.
4. `ui_color` and `language_theme` reject the same invalid inputs the TOML loader warned on
   (bad hex, unknown theme), as expected errors.
5. Persist-flavored commands edit only their canonical literal call in `init.rn`; unrelated
   Rune code is byte-preserved, stale or dirty sources are refused, clean open buffers
   reload, and `:set init` / `:set init!` retain their bulk export behavior.
6. Startup script failure lands on defaults with the existing startup-status diagnostic.
7. Full lib suite serial in default and blocking-SVP modes, clippy `--all-targets`,
   formatting.

## 8. Non-goals

- No TOML reading, migration shim, or auto-translation of existing files.
- No Option B state file in this cut (§2 records the seam for later).
- No new option kinds beyond what `:set` supports today plus colors/language themes.
- No filesystem watcher or arbitrary hot-reload of `init.rn`. Reloading a clean open buffer
  immediately after Baryon's own targeted write uses the existing reload machinery.
