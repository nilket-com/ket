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
 - Encrypted persistent settings (window size, adapter configurations)
 - Column type labels
 - Sort / Filter by column
 - Run selected code, not the entire code editor.
 - Password protection.
 - More data sources (Postgres, MySQL, BigQuery, Redshift, ad inf...)
 - Scatter / Line / Bar plots
 - Hexbin / Kernel Density plots
 - Qwen Coder SQL generator
