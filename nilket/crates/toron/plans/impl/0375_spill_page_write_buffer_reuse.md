# ADR 375 - Implementation plan: q07 spill page-write buffer reuse

Pairs with `plans/adr/0375_spill_page_write_buffer_reuse.md`. Reuse a builder-owned mutable `Vec<u8>` scratch (+ refs
capacity) across batches in `SpilledInnerJoinBuild::write_page_batch` (spill.rs:538) to remove the per-batch malloc/free
churn (profile-named). Named-site only; profile-based success; spill semantics unchanged.

## Phase 0 - confirm the site + the API shape
- Confirm `write_page_batch` is the dominant q07 allocation site via `perf report --stdio -g -i
  target/tpch/profiles/adr374_q07.perf.data` (write_page_batch self + shmem + memmove), not the folded collapse.
- Read `encode_record_batch_ipc` and `write_join_spill_plaintext_page`:
  - confirm the writer **seals in place + appends the auth tag** (so it needs an owned/mutable `Vec<u8>`, not `&[u8]`);
  - design `encode_record_batch_ipc_into(&mut buf, batch)` (clear + encode into `buf`) producing **byte-identical**
    plaintext to `encode_record_batch_ipc(batch)`.
- **Trace the write ownership boundary:** `write_join_spill_plaintext_page` moves the `Vec` into `write_plain_all`
  (spill.rs:1407), which `bytes[..].to_vec()`-clones per `write_plain_at` (spill.rs:1418); `write_plain_at`
  (wendelon file_ops.rs:67) consumes a `Vec`. Decide the ownership outcome before code:
  - **(primary) return-the-Vec:** make the seal/write helper take the scratch by value (or `&mut`), seal, write, and
    **return the buffer** (drained) to the builder so the **encode** allocation is reused; record the `write_plain_all`
    `.to_vec()` clone as **named residual**.
  - **(only if no broad `AsyncFileOps` churn) borrow path:** a no-extra-copy write that drops the `.to_vec()`; else
    leave it residual for a follow-up.
- Decide the reuse set: scratch `Vec<u8>` for plaintext + `refs` `with_capacity(num_rows)`/clear. Leave per-row
  `join_key_at` unless cleanly reusable.

## D1 - the cut (write_page_batch only)
- Add `scratch_plaintext: Vec<u8>` (and optionally a reused `refs` buffer) to `SpilledInnerJoinBuild`.
- In `write_page_batch`: `self.scratch_plaintext.clear()`; `encode_record_batch_ipc_into(&mut self.scratch_plaintext,
  batch)`; seal+write via the **return-the-Vec** helper so the scratch allocation comes back to the builder for the next
  batch; reuse `refs` (clear + `with_capacity(num_rows)` on first use).
- **The encode allocation is eliminated; the `write_plain_all` `.to_vec()` clone remains residual** unless the borrow
  path was chosen - name it in the results doc.
- Keep the page format, `spill_bytes`/`writer_peak` accounting, budget guards, and producer names identical.
- Do NOT touch the partition path, q17, or q21. Topology-named; no qNN in core.

## Tests / safety net
- **Plaintext equality:** `encode_record_batch_ipc_into(&mut buf, batch)` bytes `==` `encode_record_batch_ipc(batch)`
  for several batch shapes (empty handled upstream; multi-column; nulls).
- **Scratch reuse correctness:** encode batch A into the scratch, then batch B (reusing the same buffer) -> B's plaintext
  `==` a fresh encode of B (no stale-byte bleed from A).
- **Real round-trip:** write several batches through the spill write API and read them back -> same rows/keys as a
  fresh-buffer build (through the actual write/read paths, not constructed pages).
- **Accounting unchanged:** `spill_bytes` and `writer_peak` after N writes match the pre-change values.
- **(If a key+nonce fixture is controllable)** sealed-page bytes identical; otherwise assert plaintext + round-trip only.

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine cut, spill path)
scripts/cargo-cgroup test -p toron-tpch
# re-profile q07 (373 method) inside systemd-run --scope, debug symbols:
#   systemd-run --user --scope ... perf record -F 99 --call-graph dwarf -D 60000 -o ...adr375_q07.perf.data -- \
#     timeout --signal=INT 120s target/release/toron-tpch perf --sf 10 --query q07 --data-source parquet ...
#   perf report --stdio -g -i ...adr375_q07.perf.data  -> write_page_batch-attributed share dropped; name next stack
# q07 persisted sf=10 correctness only if it greens:
#   systemd-run --user --property=MemoryMax=4G ... correctness --query q07 --sf 10 --data-source parquet --data-dir ...
git diff --check
#   plaintext-equality + round-trip + accounting tests green; write_page_batch alloc share dropped + next stack named;
#   no cap bump; --no-fail-fast green; no qNN
```

## Results doc
`crates/toron/plans/impl/0375_spill_page_write_buffer_reuse_results.md`:
- the perf-report site confirmation (write_page_batch attribution from `adr374_q07.perf.data`);
- the reuse implemented (scratch `Vec<u8>` + refs) **and the write-ownership outcome**: return-the-Vec (encode
  allocation eliminated) with the `write_plain_all` `.to_vec()` clone named **residual**, OR a borrow path that removed
  it without broad file-ops churn - state which, and name any residual copy;
- plaintext-equality + scratch-reuse + round-trip + accounting test results;
- **before/after re-profile** for q07: the encode/write_page_batch-attributed allocation/copy share before and after,
  and the **next hot stack** (which may be the residual write clone, partitioning, or shmem I/O); whether q07 greened
  (incidental);
- confirmation no cap bump, spill accounting unchanged, `--no-fail-fast` green, no qNN.

## Risk / blast radius
- **Stale-byte bleed** - scratch not cleared / partial encode; the A-then-B reuse test + plaintext equality gate it.
- **encode_into drift from encode_record_batch_ipc** - plaintext-equality test.
- **Page/round-trip break** - real write/read round-trip test.
- **Over-claim** - success is the named-site share, not the whole bucket.
- **Regression** - `--no-fail-fast` required.

## Seal
`impl: adr 375 spill page write buffer reuse`. Do not commit wip drafts or large `.perf.data`.
