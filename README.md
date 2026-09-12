# Ket

Ket is a Rust workspace containing Baryon, the reusable headless `libaryon`
editor engine, and Polariton, a data-exploration and visualization application.

## Repository layout

- `crates/libaryon/` — the headless editor engine, document model, language services, and encrypted-file support.
- `crates/baryon/` — Baryon's terminal frontend.
- `crates/polariton/` — Polariton's iced GUI frontend.

The repository is a single Cargo workspace. Run checks and tests from the root:

```bash
cargo check --workspace --all-targets
cargo test --workspace
scripts/cargo-ctrl
```

For the resource-isolated local test gate and the SSH-backed Bigfoot worker, see
[`ci/README.md`](ci/README.md).

For native Windows build commands and current validation status, see
[`docs/windows.md`](docs/windows.md).

## Direction

`libaryon` is intentionally frontend-agnostic. Polariton consumes it as a local workspace dependency:

```toml
libaryon = { path = "../libaryon" }
```

That boundary keeps editor state, document editing, selections, undo/redo, and language services
in Baryon, while Polariton owns its GUI, data model, and visualization workflows.

## Applications

- [ket](docs/ket.md) is the transparent product router and canonical command-line front door.
- [Baryon](docs/baryon.md) is the modal terminal editor and the primary reference for the headless engine.
- [Polariton](crates/polariton/README.md) is the GUI data notebook and visualization application.

Shared development infrastructure lives in `ci/`, and technical plans live in `plans/`.
The optional Ubuntu user service for a persistent local Toron workspace lives in
[`packaging/ubuntu/`](packaging/ubuntu/README.md).
