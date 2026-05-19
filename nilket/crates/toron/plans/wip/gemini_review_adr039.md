# ADR 039 Code Review

**Reviewer:** Gemini
**Phase:** Code Review

I have reviewed the staged git commits for ADR 039. The implementation cleanly matches our finalized Iteration 5 consensus and perfectly executes the strict storage/compute wedge.

## Alignment with Council Plan
1. **Toron Simplification:** The `WendelonStorageAdmin::drop_table_storage` implementation in `crates/toron/src/storage.rs` has been vastly simplified. It now makes a single `client.drop_table_with_reclaim(TableSchemaDropRequest::current(...))` call. The complex orchestration has been entirely removed from Toron.
2. **Wendelon Orchestration:** The Wendelon `control_server.rs` now takes full ownership of the multi-stage orchestration via `run_table_schema_drop`. This correctly keeps the consensus and storage logic inside Wendelon's process boundary.
3. **Payload Naming:** The user's directive is faithfully executed. `TableSchemaDropRequest` is exposed via the `wendelon-client` (inside `compute.rs`) as an unsuffixed sum type, and constructed via `::current()`. There are zero instances of `TableSchemaDropRequestV1` in `crates/toron`.
4. **Idempotency and Generation:** `DropOperationId` has been introduced and uses `tenant_id`, `table_object_id`, and `schema_generation`. `query_id` correctly does not factor into idempotency. 
5. **No Local Toron Cache:** As requested in Iteration 5, the transient `fenced` state was NOT added to Toron's `WendelonDescriptorRegistry`. Toron relies correctly on the network and Wendelon's structured error mapping to identify fenced tables.
6. **Data-Path Rejection:** The `data_server.rs` (Wendelon's data path) has been updated to check `table_drop_fences` before accepting operations.

## Conclusion
This PR reflects our most mature distributed systems thinking yet. By driving a wedge between Toron (compute) and Wendelon (storage) and pushing the orchestration down into Wendelon, we have successfully modeled the "one algebraic ring over disjoint hardware" principle. I fully approve. 

Codex, Claude, the floor is yours for the final sign-off.