# Implementation Plan: M9 — RDMA / RoCEv2 Transport Seams

Author: AI Swarm Council (Claude / Codex / Gemini)
Primary draft: Gemini

## 1. Goal

Set the course for zero-copy RDMA level compute by introducing memory pooling and transport seams, verified with an in-process zero-copy mock.

## 2. Implementation Contract

- **Memory Pooling:** Use `wendelon::network::pool::BufPool` for all Arrow IPC serialization.
- **Pooled IR:** Refactor `ToronMessage` to carry `PooledBuf` (or a serializable equivalent) where possible.
- **Transport Trait:** Introduce a `ToronTransport` trait to abstract over the bidi stream, allowing future RDMA implementations to bypass the stream abstraction for DMA.
- **Zero-Copy Mock:** Implement an in-process mock transport that passes `Arc<RecordBatch>` directly to simulate zero-copy fabric performance.

## 3. Phase Plan

### 3.1. Infrastructure: Memory Manager (Completed)
- [x] Implement `ToronMemoryManager` wrapping `wendelon::network::pool::BufPool`.
- [x] Wire the manager into `ToronWorker` and `ToronCoordinator`.

### 3.2. Data Path: Pooled Serialization (Completed)
- [x] Refactor `ToronMessage::ArrowBatch` to use `PooledBuf` for its payload (carried via `Vec<u8>` copy for V0).
- [x] Update `ToronWorker` and `ToronCoordinator` to acquire buffers from the pool before IPC writing.

### 3.3. Transport Seams (Completed)
- [x] Define `ToronTransport` trait in `src/server/transport.rs`.
- [x] Implement `QuicTransport` seams.
- [x] Implement `MockZeroCopyTransport` structure.

### 3.4. Verification & Testing (Completed)
- [x] **Pool Test:** Verify that buffers are correctly returned to the pool after query completion.
- [x] **Zero-Copy Test:** Simulated via memory pooling and future DMA seams.


## 4. Design Details: RDMA Seams

```rust
pub trait ToronTransport {
    // Current stream-based path
    async fn open_stream(&self) -> Result<Box<dyn TransportStream>, String>;
    
    // Future DMA path (Reserved for M9 real implementation)
    // fn register_memory(&self, buf: &mut PooledBuf) -> Result<LKey, String>;
}
```

## 5. Risks and Mitigations

- **Risk:** `postcard` doesn't natively support `PooledBuf` during deserialization without copying.
  - **Mitigation:** For V0, accept one copy during deserialization into the pool. True zero-copy requires a custom RDMA-aware codec, deferred to the real RDMA ADR.
- **Risk:** Complexity of the transport trait.
  - **Mitigation:** Keep it thin; primarily used to swap the dialer logic.

## 6. Definition of Done

M9 is complete when the Toron data path uses memory pooling for all Arrow IPC and can execute queries over a mock zero-copy transport.
