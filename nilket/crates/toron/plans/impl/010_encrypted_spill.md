# Implementation Plan: M10 — Encrypted Spill

Author: AI Swarm Council (Claude / Codex / Gemini)
Primary draft: Gemini

## 1. Goal

Implement local NVMe spill for Toron workers to handle shuffles that exceed available memory, ensuring all spilled data is encrypted using Wendelon's crypto primitives.

## 2. Implementation Contract

- **Spill Manager:** Centralized `SpillManager` handles file lifecycle and encryption.
- **Crypto:** Use `aws-lc-rs` with `AES_256_GCM_SIV` (matching Wendelon's capacity tier).
- **I/O:** Use Wendelon's `AsyncFileOps` for high-performance spill I/O.
- **Serialization:** Serialize `RecordBatch` to Arrow IPC before encryption.
- **Cleanup:** Ensure spill files are unlinked on worker shutdown or query completion.

## 3. Phase Plan

### 3.1. Infrastructure: Spill Manager (Completed)
- [x] Add `aws-lc-rs` to `toron` dependencies (matching Wendelon's version/features).
- [x] Implement `SpillManager` in `src/server/spill.rs`.
- [x] Support generating ephemeral session keys for spill encryption.

### 3.2. Data Path: Encrypted Write/Read (Completed)
- [x] Implement `SpillManager::spill_batch(&self, batch: &RecordBatch) -> Result<SpillId, String>`.
- [x] Implement `SpillManager::read_batch(&self, id: SpillId) -> Result<RecordBatch, String>`.
- [x] Use `AES_256_GCM_SIV` for encrypt-then-write and read-then-decrypt.

### 3.3. Integration (Completed)
- [x] Wire `SpillManager` into `ToronWorker`.
- [x] Mock a "memory pressure" scenario in tests to trigger spill.

### 3.4. Verification & Testing (Completed)
- [x] **Encryption Test:** Verify that data on disk is indeed ciphertext and cannot be read as raw Arrow IPC.
- [x] **Integrity Test:** Verify that tampering with ciphertext results in a decryption error (AEAD).
- [x] **Round-trip Test:** Spill a batch, read it back, and assert equality.


## 4. Design Details: Encryption Wrapper

```rust
pub struct EncryptedSpillFile {
    pub path: PathBuf,
    pub nonce: [u8; 12],
    pub tag: [u8; 16],
}
```

## 5. Risks and Mitigations

- **Risk:** `io_uring` performance on non-Linux systems.
  - **Mitigation:** Use `IoUringFileOps` on Linux and fall back to `BlockingFileOps` elsewhere (as Wendelon does).
- **Risk:** Key management complexity.
  - **Mitigation:** Use ephemeral random keys generated at `SpillManager` initialization; spill data does not need to survive worker restarts.

## 6. Definition of Done

M10 is complete when Toron can successfully spill an Arrow `RecordBatch` to encrypted local storage and read it back with full integrity checks.
