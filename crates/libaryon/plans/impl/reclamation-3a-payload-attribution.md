# Reclamation stage 3a: owner-attributed payload bytes

Parent: `../adr/0001-ecs-memory-reclamation.md` stage 3, first half.
Stage 3 splits into two pairs: **3a attributes payload bytes exactly; 3b
retires them** (quarantine, dropping, stale-read contract audit — its own
plan). Scope here: counters, the API seam that makes them exact, and display.
**No payload is freed, no residency/eviction behavior changes, no id/device
semantics change.** Deliverable is one `impl:` commit reviewed against this
plan.

## Vocabulary (the distinction stage 1 deferred)

- **Authoritative payload**: `virtual_data` that is the *only* copy of its
  content — nothing on a backing device reconstructs it. Grafted trees,
  edited/spliced leaves, output panels, unsaved documents. This is the leak:
  orphaned generations retain it forever.
- **Hydrated residency**: `virtual_data` (full or partial ranges) filled from
  a registered backing device via a `SvpPointer`. Reconstructible, already
  tracked globally (`resident_bytes`, `resident_accounted_bytes`), already
  evictable. Stays **global and unowned** in 3a.

Owner attribution covers authoritative payload only.

## Write-site and boundary audit (verified 2026-07-29)

Every production `virtual_data` write belongs to one of these classes:

| site | class | thread / accounting boundary |
|---|---|---|
| `structural_graft.rs` leaf distribution | authoritative replacement | engine / `mutate_text_buffer` |
| `uast/mutation.rs` inserts, deletes, splits, and in-place splices | authoritative replacement or mutation; physical-tail copies stay hydrated | engine / `mutate_text_buffer` |
| `folding/synthetic.rs` synthetic token creation; synchronous preview hydration | authoritative creation or hydrated publication | engine / `mutate_text_buffer` |
| `document/io.rs` save rebind | authoritative → hydrated transition | engine / `mutate_text_buffer` |
| `runtime/system.rs` in-memory document creation | authoritative creation | engine / `mutate_text_buffer` |
| `session.rs` output-panel replacement | authoritative replacement | engine, **outside** `mutate_text_buffer` |
| `file_browser.rs` synthetic document replacement | authoritative replacement | engine, **outside** the text-buffer boundary |
| `svp/resolver.rs` full and partial completion publication | hydration | I/O threads; never owner accounting |
| `python_ast.rs` / `rune_ast.rs` `apply_*_semantic_update` | obsolete authoritative writer, test-only callers | remove or `#[cfg(test)]` |

The two important conclusions are narrower than “sample every engine write”:

1. Hydration is the only concurrent payload writer and targets only a
   span-backed node. Authoritative writes and class transitions are
   engine-serialized, so monotonic registry-counter sampling is sound where an
   owner boundary exists.
2. Not every authoritative write is inside `mutate_text_buffer`. Output panels
   and file browsers are real payload owners and need an explicit accounting
   boundary; otherwise their bytes enter the global counters without entering
   any owner.

The legacy Python/Rune apply paths must be deleted (preferred) or test-gated.
A source-audit test must also reject production uses of the old ambiguous
writer and direct `virtual_data` assignment/mutation outside the registry's
intent methods. Test fixtures may use a clearly test-only helper.

## The seam: make every write and transition declare intent

`hot_swap_virtual_data` currently hides five different operations. Make its
locked residency implementation private and expose intent-specific methods
(exact names may vary):

```rust
/// Span-less node; replace its sole authoritative copy and count the length delta.
fn replace_authoritative_data(node, data)
/// Span-less node; mutate its sole copy under the residency lock and count old/new.
fn mutate_authoritative_data(node, mutate)
/// Span-backed node; publish reconstructible full data without owner counters.
fn publish_hydrated_data(node, data)
/// Span-backed node; remove and return reconstructible full data for a split.
fn take_hydrated_data(node) -> Option<Vec<u8>>
/// Keep resident bytes, clear the span, and count the whole payload as authoritative.
fn promote_hydrated_data(node)
/// Install the saved-file span, count any authoritative bytes out, then drop residency.
fn rebind_data_to_saved_span(node, span)
```

- Full and partial hydration require `span.is_some()` after the generation
  check while holding `resident_data_lock`; this is an always-on assertion.
- Authoritative replace/mutate require `span.is_none()` and update new global
  monotonic counters `authoritative_bytes_added` and
  `authoritative_bytes_removed` from the old/new length delta. They measure
  retained byte quantity, not allocation churn: replacing 100 bytes with 100
  bytes changes neither counter.
- Promotion and rebind perform the class change and its counter update under
  the same residency lock. No caller may clear/install a leaf span and adjust
  its payload in separate uncounted steps.
- Engine code that copies already-resolved bytes onto a still-span-backed
  physical prefix/tail uses the hydrated method. It must not become owner
  payload merely because the engine, rather than the resolver, published it.
- Physical split paths use the hydrated take method instead of raw
  `virtual_data.take()`. It updates residency/LRU but never authoritative
  counters; the returned bytes remain reconstructible from the unchanged
  span until the split publishes its replacement pieces.
- Empty span-less leaves are authoritative with length zero. Interior nodes
  have no payload. No extra per-node class bit is needed: production mutation
  clears the span exactly when content diverges.
- Partial hydration remains residency-only. `drop_resident_data` and budget
  eviction always assert that a span-less node has no non-empty full payload;
  stage 3a must not gain a way to evict authoritative bytes.

This is a compile-time naming seam plus a source-audited raw-field seam. Merely
renaming the old setter is insufficient: every in-place `Vec::splice`,
`drain`, `take`, and direct assignment in production must pass through one of
the methods above so its delta is visible.

## Global counters: physical class vs logical lifetime

Keep these concepts separate:

- `authoritative_bytes_added/removed` track physical class changes and length
  changes. Their difference is every authoritative byte still retained in the
  registry, reachable, parked, or orphaned.
- `orphaned_payload_bytes_added/removed` track logical lifetime. Added grows
  when retained authoritative bytes leave their final owner; removed remains
  exactly zero in 3a and becomes 3b's drop counter.

Therefore the intended registry-wide reconciliation is:

```text
authoritative_added - authoritative_removed
    == attributed_live + attributed_parked + orphaned_payload
```

Production owners must cover text documents, output panels, and the file
browser's synthetic tree. The implementation may place the existing
`UastOwnerAccounting` on `FileBrowserBuffer` or use an equivalently exhaustive
synthetic-owner record, but “unowned authoritative bytes” is not a permitted
remainder.

## Owner accounting (extends stages 1–2)

`UastOwnerAccounting` gains:

```text
payload_added - payload_removed == payload_live + payload_parked
```

Here `payload_removed` is an **owner-lifetime** counter. It includes both a
physical class removal (save/rebind or shrink) and a logical transfer to the
orphan gauge. Do not equate it to the registry's physical
`authoritative_bytes_removed` counter.

- `mutate_text_buffer` samples the physical authoritative pair and the
  orphan-added counter around the closure. Ordinary orphan traffic is removed
  from live payload; typed parked retirement is split out exactly as stage 2
  splits node retirement.
- Output-panel replacement and file-browser rebuild use an explicit
  sample/record boundary (or the exact delta returned by the intent API) on
  their own owner record. They do not rely on a text view.
- `ParkedGenerationRoot` gains `payload_bytes`; `RootTransition` gains the
  corresponding revived payload. Park moves live payload to parked. A cache
  hit asserts zero physical authoritative delta and moves the stored amount
  back. Retire moves live payload to orphaned.
- `pending_parked_payload_retirements` mirrors
  `pending_parked_retirements`. It is required because the sampled orphan
  counter alone cannot tell whether an eviction retired the acting root or a
  cached root.
- Always-on checked arithmetic is retained.

### Every logical retirement emits once

`note_orphaned_payload(bytes)` is the global lifetime event and bumps
`orphaned_payload_bytes_added`. It is emitted alongside the stage-1/2 node
retirement at:

- successful graft descendant replacement (the old live payload; assert the
  stable root has zero payload);
- mutation pruning/disconnect (read the node's authoritative length before
  unlinking; currently only a leaf can contribute nonzero bytes);
- current-root replacement and final buffer release;
- parked eviction, redo-tail truncation, attached-shadow disposal, and parked
  release;
- abandoned allocated candidates that already received authoritative bytes.

Rejected grafts and candidates discarded before any authoritative write emit
zero. A root replacement must not also count the same old payload as ordinary
closure retirement. Buffer release accounts both current and parked payload
before dropping the owner record.

This explicit abandoned-candidate rule is required for reconciliation:
candidate bytes were added to the registry and remain retained even if the
candidate never became the owner's live root.

## Surfacing and guards

- The owner header gains visually separate byte fields, for example
  `owner-nodes: … · owner-payload: live=B parked=P retired=R added=A`.
- The global diagnostics gain `orphaned-payload=B`.
- The exhaustive accounting destructure and full owner-header assertion gain
  the byte fields. Their successor message is: owner attribution must never
  include hydrated residency; that stays in `reachable-resident`.
- Inspector refresh identity includes the new owner and global counters, so
  payload-only movement redraws the header.

## Tests and proof obligations

1. Byte algebra: growth/shrink, underflow/overflow, physical removal,
   park/revive, parked eviction, and root retirement.
2. Exact reachable reconciliation: a `#[cfg(test)]` metadata-only walk summing
   span-less leaf `virtual_data` lengths equals `payload_live` after unnamed
   typing, graft/regraft, splice-heavy insertion/deletion, synthetic folding,
   output-panel replacement, file-browser rebuild, save/rebind (exactly zero
   for nonempty saved leaves), reload, generation park/revive, eviction, and
   release.
3. Registry-wide reconciliation includes synthetic owners and abandoned
   candidates; there is no unexplained authoritative remainder.
4. Hydration neutrality: a cold file hydration (full and partial) changes
   residency only—no authoritative, owner, or orphan counter.
5. Promotion/rebind round trip: hydrated bytes become authoritative once,
   edits adjust only their length delta, and save removes them once.
6. Orphan baseline: begin from a cold file-backed flat tree. After `N` accepted
   grafts of exact size `S`, orphaned payload is exactly `(N - 1) * S`, current
   live payload is exactly `S`, and retained authoritative bytes reconcile to
   their sum. Record this as 3b's before-number.
7. Failure paths: rejected graft, failed root replacement, failed shadow sync,
   and abandoned authoritative candidate leave owner algebra exact; only the
   abandoned candidate enters the orphan gauge.
8. API/source architecture test: ambiguous writer gone, hydration requires a
   span, obsolete Python/Rune writers cannot ship, and no production raw
   `virtual_data` write bypasses the registry seam.
9. Guard updates remain exhaustive and retain the no-hydrated-attribution
   message.

## Non-goals

- No dropping, quarantine, or eviction-policy change (3b).
- No per-node byte column: current authoritative length is derived under the
  residency lock from `span` plus `virtual_data`.
- No hydrated-residency attribution.
- No claim that owner counters alone describe physical registry retention;
  the orphan gauge is the explicit third bucket.

## Review checklist

- [ ] Every production full write, in-place mutation, and class transition uses
      the intent-correct API; raw-field and legacy paths are source-audited.
- [ ] Hydration cannot bump authoritative counters and cannot publish to a
      span-less node after its generation check.
- [ ] Save/rebind and promotion are atomic class transitions with exact tests.
- [ ] Text, output-panel, and file-browser authoritative bytes all have owners.
- [ ] Ordinary, root, parked, release, and abandoned-candidate retirement each
      emit exactly once into the orphan gauge.
- [ ] Owner algebra and registry-wide live + parked + orphan reconciliation are
      asserted with checked arithmetic.
- [ ] Park/revive byte symmetry and zero physical delta on a cache hit are
      asserted.
- [ ] Orphan removal stays zero in 3a; authoritative data is never evicted.
- [ ] Guards and inspector refresh identity include the new fields.
- [ ] Residency, eviction, and rendering behavior are unchanged; full suite and
      clippy are green.
