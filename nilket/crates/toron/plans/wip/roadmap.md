# Toron Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) —
git holds the archeology, this file looks forward.

## Status

ADR 001 (Toron Foundation and Engine Contract) is promoted.
Implementation is starting at M0.

## Shipped Foundations

- **ADR 001 Toron Foundation and Engine Contract** — Greenfield Arrow-native execution, no DataFusion, single crate, `toron-ir` as the engine contract, `StorageScan` boundary.

## Forward Sequence

1. **M0: Monorepo Scaffold** — Flatten workspace, Toron compile target.
2. **M1: Single-Process IR Execution** — In-memory Arrow batches.
3. **M2: PBC Lowering Contract** — Pushdown translation.
4. **M3: Wendelon Client Adapter** — Real Wendelon storage wrapping `wendelon-client`.
5. **M4: Single-Process Morsels** — Parallel unit before distribution.
6. **M5: Coordinator / Worker Boundary** — QUIC incremental Arrow result streaming.
7. **M6: First Shuffle** — Hash partition exchange over QUIC.
8. **M7+: Frontends and Follow-Ups** — SQL, Rune, Encrypted spill, RDMA, advanced capabilities.

## What Not To Do Next

- Do not adapt DataFusion / Ballista.
- Do not implement SQL before the IR contract is handwritten and tested.
- Do not call Wendelon internals from operators.
- Do not build a distributed runtime before single-process semantics work.
- Do not branch protocols inside operators.
- Do not extract Wendelon crates before M2 / M3 pressure.
