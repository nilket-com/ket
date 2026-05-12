# Toron Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) —
git holds the archeology, this file looks forward.

## Status

ADR 001 (Toron Foundation and Engine Contract) is promoted.
Implementation is starting at M0.

## Shipped Foundations

- **ADR 001 Toron Foundation and Engine Contract** — Greenfield Arrow-native execution, no DataFusion, single crate, `toron-ir` as the engine contract, `StorageScan` boundary.
- **M0: Monorepo Scaffold** — Flatten workspace, Toron compile target.
- **M1: Single-Process IR Execution** — In-memory Arrow batches.
- **M2: PBC Lowering Contract** — Pushdown translation.
- **M3: Wendelon Client Adapter** — Real Wendelon storage wrapping `wendelon-client`.
- **M4: Single-Process Morsels** — Parallel unit before distribution.
- **M5: Coordinator / Worker Boundary** — QUIC incremental Arrow result streaming.
- **M6: First Shuffle** — Hash partition exchange over QUIC.
- **M7: Rune Authoring Surface** — Typed DataFrame API, sandboxed execution.
- **M8: SQL Compatibility** — Parser lowered to handwritten IR.
- **M9: RDMA / RoCEv2 Transport Seams** — Memory pooling and transport abstraction.
- **M10: Encrypted Spill** — Local NVMe spill with Wendelon crypto primitives.
- **M11: Advanced Kernels** — Linear regression, quantile sketches, time buckets.

## Forward Sequence

1. **V1 Stabilization** — Production hardening, metrics integration, and full SQL surface coverage.





## What Not To Do Next

- Do not adapt DataFusion / Ballista.
- Do not implement SQL before the IR contract is handwritten and tested.
- Do not call Wendelon internals from operators.
- Do not build a distributed runtime before single-process semantics work.
- Do not branch protocols inside operators.
- Do not extract Wendelon crates before M2 / M3 pressure.
