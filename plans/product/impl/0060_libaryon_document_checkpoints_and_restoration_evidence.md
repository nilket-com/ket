# PRODUCT-0060 implementation evidence: libaryon document checkpoints and restoration

Status: accepted 2026-08-08.

Plan: `555c958b`
(`plans/product/adr/0060_libaryon_document_checkpoints_and_restoration.md`). The
review stack was compressed to one implementation commit for publication.

## Result

Libaryon now has an explicit saved-engine lifecycle:

```text
SessionStore::open_writable(SessionId)
    -> EngineHandle::spawn_saved(config, SavedSession<Writable>)
    -> ordered EngineHandle::checkpoint()
    -> atomic generation publication
    -> explicit reopen and pre-thread document restoration
```

The ordinary `EngineHandle::spawn` path remains ephemeral. Neither Baryon nor
Polariton discovers a session, checkpoints implicitly, or selects a “last
session.” A saved engine owns the exclusive store handle for its lifetime.
Restoration validates the complete owner image and builds every ordinary
document before starting the engine thread or publishing a viewport.

Owner `libaryon_documents`, schema 1, contains a strict JSON manifest followed
by raw byte extents in ascending `DocumentId` order. The base snapshot remains
the sole owner of exact paths and identity. Checkpoints include only ordinary
author-owned text documents; output panels, Git snapshot partners, browser and
preview buffers, UAST inspectors, and host panels remain recomputable or
host-owned. Encrypted plaintext makes the whole checkpoint refuse.

Restored roots are newly allocated through the production UAST registry path.
Scratch and dirty documents use checkpoint bytes. Clean readable files follow
current disk, while changed, missing, or unreadable cases are reported in one
startup warning. Dirty restoration has an explicit dirty baseline and empty
undo/redo history. Automatic, explicit, explicit-plaintext, and unavailable
provider choices retain distinct provenance.

Until the next owner supplies accepted topology, documents appear exactly once
in one pane, ordered by ascending `DocumentId`, with the lowest ID active. This
is an inspectable fallback, not layout persistence.

## Acceptance-gate ledger

| Gate | Evidence | Result |
| --- | --- | --- |
| 1. Compiler-observed read-only closure | Six `tests/ui/session_store_*_{read_only,writable}.rs` pairs bank method-specific failures and passing twins through identical public imports; the old source-shape proxy is gone and `trybuild` is dev-only | pass |
| 2. Explicit exclusive saved-engine entrypoint | `session_{writable,read_only}_spawn_saved` provide a passing twin and bank the exact writable/read-only mismatch; `product_0060_saved_engine_rejects_initial_file_before_start_and_releases_lock` proves the competing `initial_file` authority refuses before startup and releases the lock | pass |
| 3. Strict arbitrary-byte codec | `product_0060_codec_round_trips_arbitrary_bytes_and_orders_ids` and `product_0060_codec_refuses_malformed_extents_and_id_sets` cover invalid UTF-8/NUL, strict fields, magic, extents, ordering, overflow, trailing bytes, schema, and ID-set equality | pass |
| 4. Empty predecessor and future schema | `product_0060_saved_engine_refuses_incomplete_and_future_document_images_pre_start` distinguishes valid empty predecessor, nonempty missing owner, and unknown owner schema; generic read-only open preserves the future payload byte-for-byte | pass |
| 5. Positive durable-candidate set | `product_0060_checkpoint_persists_only_positive_document_candidates` constructs ordinary scratch/file plus second view, output, snapshot, browser, Markdown preview, UAST inspector, and host panel; exactly the two ordinary document extents commit | pass |
| 6. Durable/runtime bijection | `product_0060_saved_document_bindings_are_bijective_and_never_reuse_ids` covers scratch, second view, close, path update, replacement, and monotonic non-reuse; owner bytes contain no runtime IDs | pass |
| 7. Projection and fresh fallback identities | Pane and tab projections carry `Some(DocumentId)` only for saved ordinary documents; reopen tests preserve `DocumentId` meaning while the fallback allocates fresh process-local views and buffers | pass |
| 8. Ordered capture and publication faults | `product_0060_saved_engine_checkpoints_an_ordered_prefix_and_restores_it` proves A/checkpoint/B restores A while live state holds AB. `product_0060_checkpoint_faults_preserve_atomic_publication_boundary` proves old truth before `CURRENT`, complete new truth after it, poison refusal for later edits, live copy-out bytes, and opaque sibling preservation | pass |
| 9. Bounded all-or-nothing capture and restore | Boundary tests use small injected budgets and pin production to 256 MiB. `product_0060_restore_refuses_a_clean_file_that_grew_past_the_budget` proves disk growth is structural refusal rather than recovery fallback | pass |
| 10. Encrypted plaintext exclusion | `product_0060_capture_publishes_exact_bytes_and_refuses_atomically` installs an encrypted backing, observes the security diagnostic, and proves the last committed owner remains unchanged | pass |
| 11. Exact dirty bytes with empty history | `product_0060_restores_exact_dirty_bytes_and_id_order_without_undo_history` restores arbitrary non-UTF-8 bytes, dirty state, no undo replay, nonconsecutive IDs, ascending tabs, and lowest-ID focus | pass |
| 12. File authority matrix | `product_0060_file_restore_matrix_reports_every_divergence` covers unchanged clean, changed clean, changed dirty, and missing clean files; disk/checkpoint authority, dirty state, retained path, and aggregate warnings are asserted together | pass |
| 13. Exact Unix path authority | `product_0060_saved_engine_restores_and_saves_exact_non_utf8_paths` opens two distinct paths with the same lossy label, restores both without aliasing, edits/saves the exact first path, and proves the replacement-character sibling is never created. Existing lexical/symlink path tests pin noncanonical durable spelling | pass |
| 14. Language provenance | `product_0060_language_selection_retains_authored_provenance` and `product_0060_language_selection_provenance_survives_unavailable_providers` distinguish Auto, Explicit, ExplicitPlaintext, and unavailable explicit IDs; a second checkpoint retains the unavailable authored ID | pass |
| 15. Deterministic one-pane fallback | The exact dirty-byte/order test uses intentionally nonconsecutive IDs and asserts one tab per document in ascending order with index 0 active; no exact split, cursor, host panel, or `PanelRestored` claim is made | pass |
| 16. Default behavior remains unchanged | Saved operation requires an explicit store handle and entrypoint. Shutdown does not checkpoint; the ordered-prefix test proves B is absent after graceful shutdown. No Polariton source, Rune surface, SQL/Toron state, or rendering baseline changes | pass |

## Implementation findings

Four findings strengthened the implementation without widening the record.

First, strict restoration cannot be deferred into the command loop. The engine
now accepts an optional already-built `EditorSession`; `spawn_saved` decodes,
reconciles files, detects duplicate canonical paths, creates roots, and builds
the fallback before it spawns either the engine or resolver bridge thread.
Structural refusal therefore cannot leak an empty handle or viewport.

Second, restored resident bytes exposed an accounting premise hidden by empty
document construction. The first exact non-UTF-8 save retired payload that the
new root had not credited and tripped the UAST owner algebra. `new_from_bytes`
now records its initial authoritative payload alongside its two nodes; the
save-after-restore oracle makes that accounting load-bearing.

Third, a clean disk file that grows beyond the restore budget must refuse. An
early version routed the synthetic budget error through the missing/unreadable
recovery branch and marked checkpoint bytes dirty. Capacity is not file
unavailability; it now remains a structural pre-start error, covered with a
nine-byte file and an eight-byte injected limit.

Fourth, publication uncertainty needs a user-actionable later error. The first
post-pointer failure reports the injected store error, retains the lock, and
keeps live bytes. Every later checkpoint says those edits are not
session-checkpointable through that engine and instructs copy-out followed by
a fresh reopen. No in-place pointer observation weakens PRODUCT-0059.

## Implementation sequence

The review stack isolated nine logical steps before publication compression:

1. compiler-observed saved-session capability closure;
2. exact `PathBuf` and `LanguageSelection` authority refactor;
3. strict bounded document-owner codec;
4. runtime/durable document identity bindings and projections;
5. synchronous ordered checkpoint publication;
6. complete pre-start restoration and deterministic fallback;
7. fault, candidate, file, language, capacity, and exact-path boundary oracles;
8. downstream projection fixtures, explicit-consumer architecture premise, and
   heavy-quota test stabilization; and
9. compiler-observed writable-only `spawn_saved` entrypoint.

## Verification

- `cargo test -p libaryon product_0060 --lib --no-fail-fast` — 17 behavior
  tests passed, zero ignored.
- `cargo test -p libaryon --no-fail-fast` — full libaryon unit, integration,
  architecture, trybuild, and rustdoc suite passed; only pre-existing manual
  benchmark/acceptance tests remain ignored.
- `cargo test --workspace --all-targets --no-fail-fast --quiet` — passed across
  Baryon, Ket, Libaryon, Polariton, Toron, and Wendelon, including both
  trybuild suites; no persistence test is ignored.
- `cargo clippy -p libaryon --tests -- -A clippy::deref_addrof -A
  clippy::map_entry -A clippy::len_without_is_empty -D warnings` — passed; the
  allowances are the pre-existing libaryon findings used by PRODUCT-0059.
- `cargo fmt --all -- --check` and `git diff --check` — passed.
