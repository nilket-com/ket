# ADR 375 - q07 spill page-write buffer reuse (named-site perf cut)

## Context

The second perf-arc cut, from ADR 374's re-profile (q07 next stack: allocation/memcopy/free pressure). The **named
site** is `SpilledInnerJoinBuild::write_page_batch` (spill.rs:538): per call it allocates `refs = Vec::new()` (no
capacity), `encode_record_batch_ipc(batch)` (a fresh plaintext `Vec` per batch), then
`write_join_spill_plaintext_page` (seals in place + appends the auth tag). Called per build batch -> the per-batch
malloc/free churn the profile flagged (`unlink_chunk` 13.8B leaf, `free` 8.86B, `malloc` 6.34B, memmove). `perf report
--stdio -g` on the existing `adr374_q07.perf.data` recovered the site despite the inferno folded collapse flattening
~95B samples to `toron-tpch`. Fix = **reuse a builder-owned mutable scratch buffer across batches**. Profile-based,
named-site-scoped; green incidental.

## Decision

### The reuse (mutable scratch, not an immutable slice)
`write_join_spill_plaintext_page` **seals in place and appends the auth tag**, so the reusable object is a
**mutable `Vec<u8>` scratch buffer** owned by `SpilledInnerJoinBuild`, not a `&[u8]`. Do **not** require the writer to
take `&[u8]`.

- Add a builder-owned scratch `Vec<u8>` (and reuse `refs` via `with_capacity`/clear).
- Per batch: clear the scratch (keep its allocation), encode the IPC plaintext **into** it (an `encode_into(&mut buf,
  batch)` variant of `encode_record_batch_ipc`), then seal-in-place + write.
- The per-row `join_key_at` allocation is correctness-bound (keys feed refs); touch only if cleanly reusable.

### Write ownership boundary (load-bearing - the reuse is real only if the Vec comes back)
Today the write path **consumes** the scratch and **clones** again at the bottom: `write_join_spill_plaintext_page`
moves the `Vec` into `write_plain_all` (spill.rs:1407), which `bytes[..].to_vec()`-clones the sealed bytes per
`write_plain_at` call (spill.rs:1418), and `AsyncFileOps::write_plain_at` (wendelon file_ops.rs:67) consumes a `Vec`. So
encoding into scratch reuses **nothing** unless the buffer is returned. The cut must do **one** of:
- **(primary) return-the-Vec-after-write:** the seal/write helper takes the scratch by value (or `&mut`), seals in
  place, writes, and **returns the buffer to the builder** (drained/cleared) so its allocation is reused next batch -
  eliminating the per-batch **encode** allocation. The `write_plain_all` `.to_vec()` clone is then named **residual**
  (a per-page copy that remains). OR
- **(only if no broad file-ops churn) borrow-capable write:** a no-extra-copy write path so `write_plain_all` doesn't
  `.to_vec()` - acceptable only if it does not churn the `AsyncFileOps` trait + all impls; otherwise leave it as the
  named residual for a follow-up.

This cut **removes the fresh IPC/plaintext encode allocation; it does not necessarily remove every per-page write
allocation** (the `write_plain_all` clone) unless the lower write path is changed. State the residual explicitly.

### Scope
Only `write_page_batch` (the named site). Do **not** touch the partition path (`partition_bucket`/
`encode_partition_key_value`), q17, or q21 - they are queued next stacks. Topology-named; no qNN in core.

## Acceptance
- **Plaintext equality:** the IPC plaintext encoded into the reused scratch **equals** a fresh `encode_record_batch_ipc`
  of the same batch (byte-for-byte). (Not "page bytes identical" - the sealed page ciphertext depends on key+nonce; only
  control that in a test that fixes key+nonce, otherwise assert plaintext + round-trip, not raw page bytes.)
- **Round-trip equality:** a real spill write/read round-trip (through the actual write + read APIs) returns the same
  rows/keys as before the change.
- **Budget/accounting unchanged:** `spill_bytes`, `writer_peak`, the budget guards, and producer names are identical.
- **Write ownership outcome stated:** either the seal/write helper returns the scratch buffer for reuse (encode
  allocation eliminated) with the `write_plain_all` `.to_vec()` clone named **residual**, or a borrow path removed the
  clone without broad file-ops churn. The results doc must say which, and name any residual copy.
- **Profile (success metric, narrowed):** re-profile q07 (373 method) - the **encode/`write_page_batch`-attributed**
  allocation/copy share drops materially, and the **next hot stack is named**. Do **not** expect the whole ~84%
  alloc/memcopy bucket to vanish (the `write_plain_all` clone if residual, partitioning, Arrow `take`, shmem writes,
  allocator/system cost remain). Green is incidental (q07 may green or advance).
- **`--no-fail-fast` required** (engine cut, spill path); de-branded; prior greens unchanged; no cap bump.

## Guardrails
- **Reusable object is a mutable `Vec<u8>` scratch** (seal-in-place + auth tag) - not a `&[u8]`; the writer keeps its
  in-place sealing.
- **Implement only `write_page_batch`** - partition path / q17 / q21 queued, not this cut.
- **Equality is plaintext + round-trip + accounting**, not raw sealed-page bytes (nonce-dependent).
- **Profile success is the named-site share dropping**, not the whole bucket; record the next stack.
- **No cap bump; spill semantics/accounting unchanged; --no-fail-fast; de-branded; no qNN in core.**

## Risks
- **Scratch reuse corrupts a page** (stale bytes if not cleared, or aliasing) - the plaintext-equality + round-trip
  parity tests gate it.
- **Over-claiming the bucket** - the narrowed success metric (write_page_batch-attributed share) prevents claiming the
  whole 84%.
- **API churn** - `encode_into` must match `encode_record_batch_ipc` exactly (plaintext-equality test).
- **Regression** - `--no-fail-fast` required (spill path).

## Forward arc
375 (write_page_batch scratch reuse) -> re-profile q07 -> next stack (partition_bucket/encode_partition_key_value, or
shmem-write I/O) -> q17 grouped-aggregate fold/finalize + spill-read I/O -> q19 spill-read I/O -> q21 712-byte
resident-state quick-win -> eventually parallelism (the deeper single-thread systemic). The profile drives each cut.
