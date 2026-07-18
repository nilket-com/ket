# Architecture

Ket contains two Rust products in one Cargo workspace.

```text
baryon (terminal editor frontend) ──┐
                                     ├── libaryon (headless editor engine)
polariton (GUI, data, visualization) ─┘  [planned editor consumer]
```

`libaryon` is the shared editor core. It must not acquire terminal-frontend dependencies; the
`baryon` binary remains one consumer, and Polariton will become another through a workspace path
dependency. Integration should happen through a Polariton-specific adapter layer rather than by
placing GUI code in `libaryon`.
