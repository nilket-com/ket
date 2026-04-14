# Plan: Implement Server-Side WPBC Pushdown Execution

## Summary
This plan details the implementation of Target #4: the server-side execution of Wendelon Pushdown Bytecode (WPBC). Following the creation of the Smart Client SDK (Target #2), the Wendelon Data Ring server must now process incoming `QUERY_REQ` frames, scan the requested key ranges from the local `SlabManager`, execute the attached WPBC bytecode to filter and project the Arrow batches, and return the zero-copy results via `QUERY_RESP`.

## Motivation
Per ADR 005, Wendelon achieves high performance by pushing down compute (filtering and projection) to the storage layer, avoiding the network cost of moving terabytes of raw data to external execution engines like DataFusion. The client SDK is already framing these requests; however, the server currently returns `Unsupported` when it receives a `QUERY_REQ`. The server must now fulfill its half of the contract by natively executing WPBC directly against the local storage (and L2 tiered slabs).

## Goals
- **Range Scanning:** Extend the `SlabManager` (and `StorageEngine` trait) to support scanning. It must be able to return all valid `RecordLocation`s where the `key_hash` falls within a `[key_range_start, key_range_end]` bounds.
- **`QUERY_REQ` Handler:** Extend `WendelonDataServer::handle_data_frame` in `src/network/data_server.rs` to route `MsgType::QUERY_REQ` to a new query execution path.
- **Consistency Checks:** Ensure the query handler respects the `min_hlc` and `expected_epoch` consistency gates (defined in `QueryRequestMeta`), returning `E_SAFE_BEHIND` or `EPOCH_STALE` hints if the local replica is not sufficiently up-to-date.
- **WPBC Execution:** Extract the WPBC bytecode from the request frame's trailing section. For each Arrow batch retrieved from the slab, run the `wpbc::execute` interpreter to evaluate the predicates and perform column projections.
- **Result Assembly:** Construct a `QUERY_RESP` frame containing the filtered Arrow IPC bytes (zero-copy) and the final row count in the metadata.

## Non-Goals
- Complex query planning or SQL parsing on the server (ADR 005 strictly forbids this).
- Merging multiple Arrow IPC batches into a single contiguous batch. The server streams the filtered batches back as separate HRFT frames or a concatenated stream, bounded by memory limits.
- Background execution of distributed joins or cross-node aggregations.

## Implementation
1. **Storage Range Scan:** Add `fn scan(&self, start: u64, end: u64) -> Vec<RecordLocation>` to `SlabManager`. Since the local index (`DashMap`) is partitioned, this may involve iterating over the map.
2. **Query Dispatch:** In `handle_data_frame`, match on `MsgType::QUERY_REQ`.
3. **Consistency Gate:** Perform the same `Consistency::LINEARIZABLE` checks as the `read` method, comparing `min_hlc` against the local state machine's E_safe watermarks.
4. **Execution Loop:**
   - Scan the `SlabManager` for locations.
   - For each location, read the Arrow IPC bytes (from L1 or L2).
   - If `bytecode_len > 0`, decode the WPBC program, deserialize the Arrow bytes into a `RecordBatch`, pass it to `wpbc::execute`, and then serialize the resulting `RecordBatch` back to IPC bytes.
   - If `bytecode_len == 0`, skip the WPBC execution (it's a raw dump).
5. **Response Construction:** Send a `QUERY_RESP` containing the computed metrics (`rows` count) and the filtered Arrow payload.

## Risks and Mitigations
- **Risk:** High memory consumption if a query matches a massive range of keys and the server attempts to load them all into a single frame.
  - **Mitigation:** V1 can implement a maximum payload limit per frame. If the filtered data exceeds the frame size, the server may need to chunk responses or simply return a memory-capped subset.
- **Risk:** Decoded Arrow batch re-serialization negates the zero-copy advantage.
  - **Mitigation:** The WPBC execution requires deserialization to evaluate predicates, but projection creates zero-copy slices. The final re-serialization is an accepted cost of pushdown, which is still dramatically cheaper than transferring all the raw bytes over the network.

## Timeline
This is Target #4. To be implemented following the completion of the Lazy Tiering Background Sweeper.

## References
- `plans/adr/005_compute_pushdown_wpbc.md`
- `src/schema/wpbc.rs`
- `src/network/data_server.rs`
- `src/storage/manager.rs`