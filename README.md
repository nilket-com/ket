# Ket

Ket is the shared home for the Baryon editor engine and Polariton data-visualization application.

## Repository layout

- `baryon/` — Baryon's terminal editor and the reusable `libaryon` headless editor engine.
- `polariton/` — Polariton, a Rust application for data exploration and visualization.

The repository is a single Cargo workspace. Run checks and tests from the root:

```bash
cargo check --workspace --all-targets
cargo test --workspace
```

## Direction

`libaryon` is intentionally frontend-agnostic. The planned code-editor experience in Polariton
will use it as a local workspace dependency:

```toml
libaryon = { path = "../baryon/crates/libaryon" }
```

That boundary keeps editor state, document editing, selections, undo/redo, and language services
in Baryon, while Polariton owns its GUI, data model, and visualization workflows.
