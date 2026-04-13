# API & SDKs

Wendelon exposes a raw, hyper-fast byte interface. SDKs bridge the gap between this interface and human-friendly tooling.

## 1. Rust Native Client
- The primary SDK. Manages the QUIC connection pool, handles `EPOCH_STALE` retries, generates `ClientWriteId` for idempotency, and maintains a cached snapshot of `ring(E)` to route requests directly to the correct Data Node.

## 2. DataFusion TableProvider
- An implementation of `TableProvider` mapping a Wendelon Namespace to a DataFusion `DataFrame`.
- **Predicate Pushdown:** Converts DataFusion `Expr` trees into Wendelon Pushdown Bytecode (WPBC).

## 3. Postgres Wire Protocol (PGWire) Adapter
- To support legacy BI tools (Tableau, Grafana) that cannot link the Rust SDK, we will build a lightweight, stateless proxy daemon.
- This proxy speaks PGWire to the client, parses standard SQL into DataFusion logical plans, executes them against the Wendelon `TableProvider`, and translates the Arrow RecordBatches back into Postgres row formats.