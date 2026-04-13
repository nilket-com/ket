# API & SDKs

Wendelon exposes a raw, hyper-fast byte interface. SDKs bridge the gap between this interface and human-friendly tooling.

## 1. Rust Native Client
- The primary SDK. Manages the QUIC connection pool, handles `EPOCH_STALE` retries, generates `ClientWriteId` for idempotency, and maintains a cached snapshot of `ring(E)` to route requests directly to the correct Data Node.

### 1.1 API Sketch
```rust
// Initialize the client (connects to the Control Ring to fetch ring topology)
let client = WendelonClient::connect(ClientConfig {
    control_endpoints: vec!["node1.wendelon:4002", "node2.wendelon:4002"],
    tls_cert: cert,
    ..Default::default()
}).await?;

// Unary Write
let resp = client.write(
    NamespaceId(1),
    "user_123_profile",
    arrow_record_batch,
    WriteOptions::default().durability(DurabilityLevel::QUORUM)
).await?;

// Linearizable Read (with WPBC predicate pushdown)
let read_resp = client.read(
    NamespaceId(1),
    "user_123_profile",
    ReadOptions::linearizable()
        .with_pushdown(wpbc_bytecode_buffer)
).await?;

// Bulk Write (Client-streaming)
let mut stream = client.bulk_write(NamespaceId(1)).await?;
stream.send(arrow_batch_1).await?;
stream.send(arrow_batch_2).await?;
let results = stream.finish().await?;
```

## 2. DataFusion TableProvider
- An implementation of `TableProvider` mapping a Wendelon Namespace to a DataFusion `DataFrame`.
- **Predicate Pushdown:** Converts DataFusion `Expr` trees into Wendelon Pushdown Bytecode (WPBC).

## 3. Postgres Wire Protocol (PGWire) Adapter
- To support legacy BI tools (Tableau, Grafana) that cannot link the Rust SDK, we will build a lightweight, stateless proxy daemon.
- This proxy speaks PGWire to the client, parses standard SQL into DataFusion logical plans, executes them against the Wendelon `TableProvider`, and translates the Arrow RecordBatches back into Postgres row formats.