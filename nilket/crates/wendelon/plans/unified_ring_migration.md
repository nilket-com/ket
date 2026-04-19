# Blueprint: Unified Ring Identity Migration

## 1. Objective
Transition the Wendelon codebase from a collision-prone 64-bit key hash to a 256-bit **Identity** (BLAKE3), while preserving the 64-bit **Locality** projection for Unified Ring routing.

## 2. Structural Changes (The "Big Refactor")

### A. Core Schema (`src/schema/core.rs`)
- **`SlabRecordHeader`**:
    - Change `key_hash: u64` to `key_identity: Identity` (32 bytes).
    - This expands the header by 24 bytes. All fixed-offset calculations must be updated.
- **`RecordLocation`**:
    - Change `key_hash: u64` to `key_identity: Identity`.
    - Note: This increases the in-memory size of every index entry (raw key bytes alone are ~32 GB per billion identities).

### B. Storage Indices (`src/storage/manager.rs` & `src/storage/core.rs`)
- **`key_index`**:
    - Change `DashMap<u64, Vec<RecordLocation>>` to `DashMap<Identity, Vec<RecordLocation>>`.
- **`append_migration`**:
    - Update signature to accept `Identity` and `Locality` explicitly.
    - Implement the collision check using the full 256-bit `Identity`.

### C. Hashing Logic (`src/network/data_server.rs` & `src/client/core.rs`)
- **Hash function**:
    - Replace `HighwayHash-64` with `BLAKE3`.
    - Return the `Identity` newtype (32-byte array).
- **Ring Routing**:
    - Every call to `router.replicas()` or `ring.snapshot().primary_ranges_for()` must now pass `Locality::from(identity)`.
    - `Locality` is the first 8 bytes of the `Identity`.

### D. Arrow Schemas (`src/schema/wpbc.rs` & `src/schema/core.rs`)
- **`read_key_schema`**: Change from `UInt64` to `FixedSizeBinary(32)`.
- **`write_payload_schema`**: Update the `key` field to `FixedSizeBinary(32)`.
- **WPBC Bytecode**: Any opcodes that operate on hashes must be updated to handle 32-byte buffers.

## 3. Workstream Impact

### W1 (Foundation & Storage Seam) — CURRENT
- **Migration Task**: Re-run the WAL recovery and redb index implementation with 32-byte `Identity` keys and explicit `Locality` projection points.
- **Chaos/Simulator**: Update fault injection to target partial-Identity truncation and 32-byte durable-field boundaries.

### W2 (Transport) — UPCOMING
- **Migration Task**: Update `HRFT` frame headers. `ReadRequestMeta` and `WriteRequestMeta` must accommodate the larger `Identity`.

### W3 (Consensus) — UPCOMING
- **Migration Task**: The Raft Log will store `Locality` for routing but must carry the `Identity` in the payload for the state machine to be authoritative.

## 4. Execution Plan (Post-Quota Refresh)

1.  **Phase 1 (Surgical Types):** Update `Identity` and `Locality` definitions and fix the `HighwayHash` implementation.
2.  **Phase 2 (The Red Squiggle Walk):** Fix the compiler errors in `src/storage/` (SlabManager and LocalSlabStore).
3.  **Phase 3 (Wire & Schema):** Update the Arrow builders and `bytemuck` network structs.
4.  **Phase 4 (Validation):** Run `architecture_test.rs` and the W1 crash-recovery matrix to ensure 32-byte `Identity` durability and 64-bit `Locality` projection purity.

## 5. Unification Invariants to Verify
- [ ] No mention of "Control Plane" or "Data Plane" introduced during refactor.
- [ ] `Locality` is always a lossy projection of `Identity`.
- [ ] Storage engine never makes a decision based on `Locality` alone.
- [ ] Ring never makes a routing decision based on `Identity` (to keep the routing table small).
