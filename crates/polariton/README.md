## Polariton

A strong coupling of data and visualization.

#### Installing:

If `cargo deb --install` is configured in your environment, Polariton can be installed directly:

```bash
cargo deb -p polariton --install
```

#### Features:
 - Query 10M rows into local memory in milliseconds.
 - Memory model is polars::frame::DataFrame for blazing fast analytics.
 - Instant scolling, no kill -9 required to scroll though 10M rows.
 - Made with 100% Rust, no JVM garbage factories included. 🦀
 - Batteries included, all drivers are bundled.
 - Generic SQL syntax highlighting.
 - Supports both DDL and DML.
 - Resizable & Rearrangeable ui panels.
 - Rune-only startup configuration.

#### Configuration:

Polariton reads user-authored configuration from `init.rn` and never rewrites
it. On Linux, the default path is `~/.config/polariton/init.rn` (or
`$XDG_CONFIG_HOME/polariton/init.rn`). macOS and Windows use their standard
application configuration directories.

```rune
pub fn main(config) {
    config.window_size(
        1920,
        1080,
    )?;
    config.show_column_types(true)?;
}
```

Missing configuration is a clean no-op. A broken script is reported in the
status bar while Polariton still opens with compiled defaults. Settings changed
through the UI last only for the current session; durable preferences belong in
`init.rn`.

#### Adapters:
 - SQLite (file or memory)
 - Parquet (single file)

#### Screenshots:
![Polariton SQL IDE interface showing query editor and results pane](doc/img/screenshot_1.png)

#### Backlog:
 - Jump to row
 - Shift+Enter to run code
 - Tab/Shift+Tab gui navigation
 - Improved status bar
 - Column type labels
 - Sort / Filter by column
 - Run selected code, not the entire code editor.
 - Password protection.
 - More data sources (Postgres, MySQL, BigQuery, Redshift, ad inf...)
 - Scatter / Line / Bar plots
 - Hexbin / Kernel Density plots
 - Qwen Coder SQL generator
