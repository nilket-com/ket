# PRODUCT-0155 gate 1: the JavaScript engine probe

Observational evidence, 2026-09-03, for the dev-dependency the record names.

| fact | observed |
|---|---|
| crate | `boa_engine` 0.22.0, `default-features = false` |
| licence | Unlicense OR MIT (registry metadata) |
| build script | none in `boa_engine`; one in its closure, `syn`, a duplicate major of a crate already present, whose script is feature detection in Rust |
| native code | no `*-sys`, `cc`, or `cmake` crate in the closure |
| lockfile growth | 25 packages: `boa_ast boa_engine boa_gc boa_interner boa_macros boa_parser boa_string cow-utils dynify dynify-macros futures-concurrency num-bigint oneshot phf phf_generator phf_macros phf_shared regress ryu-js small_btree syn synstructure tag_ptr utf16_iter write16` |
| default features left off | `float16`, `xsum`, `temporal` — none needed for containment and a distance compare |
| heads | the probe test compiles and passes in the default head (first compile about nine minutes) and in the reduced head (about seven) |
| where | Polariton dev-dependency only; never in either head's binary, never in a package |

The harness (`crates/polariton/src/publication.rs`, `product_0155_tests`)
loads the complete `assets/web/inspect.js` into a `Context` with no
`document`, so the installer never runs, then calls the shipped
`polaritonResolve` with each conformance vector. There is no second
resolver. The architecture guard admits exactly this crate as a
dev-dependency and refuses any other engine.
