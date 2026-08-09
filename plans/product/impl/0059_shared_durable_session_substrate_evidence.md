# PRODUCT-0059 implementation evidence: shared durable session substrate

Status: accepted 2026-08-08.

Plan: `150c6b0a`
(`plans/product/adr/0059_shared_durable_session_substrate.md`). Implementation
ships as `impl: adr 0059 shared durable session substrate` together with this
evidence.

## Result

libaryon now owns four production substrate outputs:

```text
SessionId
DocumentId scoped by SessionId
SavedSession<ReadOnly | Writable> under a lifetime flock
one versioned owner-partitioned snapshot published through CURRENT
```

The default Baryon and Polariton paths consume none of them. Constructing the
platform locator does not create a directory, and no engine or head source
mentions `SessionStore`, `SavedSession`, or `DocumentId`. Nothing visible
survives restart yet; document content and restoration are the next consumer.

`SessionId` is a random nonzero 128-bit value with one strict lowercase-hex
spelling. `DocumentId` is a nonzero monotonic session-local `u64`; removal never
rewinds its persisted allocator. Scratch documents mint without a path, and
attach, rename, move, and save-as alter only an exact Unix-path byte attribute.
No runtime `BufferId`, `PaneId`, or `ViewId` enters the snapshot.

The store is rooted under the head-neutral `libaryon/sessions` platform state
path. `session.lock` is a permanent private inode. Existing sessions acquire
`LOCK_SH | LOCK_NB` or `LOCK_EX | LOCK_NB` before reading one byte of `CURRENT`.
Every raw descriptor uses atomic `O_CLOEXEC | O_NOFOLLOW`; the write API exists
only on `SavedSession<Writable>`, while a compile-fail rustdoc proves a
read-only handle cannot call `commit`.

Each commit writes a private immutable generation containing strict
`store.json` plus opaque per-owner bytes, synchronizes it, renames it into
place, and only then replaces and synchronizes `CURRENT`. Unknown owners are
copied byte-for-byte. Owner descriptors carry an independently governed schema
version and an exact byte length, but libaryon never parses the payload.

## Acceptance-gate ledger

| Gate | Evidence | Result |
| --- | --- | --- |
| 1. Canonical distinct session identity | `product_0059_identity_and_owner_grammars_refuse_unsafe_spellings` pins zero/uppercase/length/separator/traversal refusals; `product_0059_mint_collision_retries_without_touching_the_existing_session` forces a collision and observes the old bytes unchanged | pass |
| 2. Session-scoped non-reused document identity | `product_0059_ids_paths_rebinding_and_owner_payloads_round_trip` mints scratch/file IDs `1,2,3`, removes `2`, and proves no reuse; a second session may mint local `1` only under a distinct `SessionId` | pass |
| 3. Path independence and exact bytes | The same test attaches a non-UTF-8 Unix path to scratch, commits, reopens, and recovers exact `OsStr` bytes under the same `DocumentId`; a new session does not inherit identity from the path | pass |
| 4. Runtime rebind without serialized counters | Durable documents are rebound in reverse order to fresh `BufferId`s; the manifest is asserted free of `BufferId`, `PaneId`, and `ViewId` | pass |
| 5. Exclusive/shared lock matrix and ordering | `product_0059_lock_contention_precedes_current_and_readers_share` uses separate processes: EX excludes EX/SH, two SH holders coexist, and a deliberately malformed `CURRENT` still yields contention before corruption | pass |
| 6. Crash release and close-on-exec | The lock test proves `SIGKILL` release. `product_0059_exec_child_cannot_become_a_phantom_lock_holder` verifies `FD_CLOEXEC`, kills a holder after it execs a long-lived child, reacquires EX while that child remains alive, then kills the control child | pass |
| 7. Platform and filesystem boundary | `product_0059_state_roots_are_head_neutral_and_platform_exact` pins XDG fallback and macOS paths; source docs bound `flock` to local filesystems and never claim reliable NFS detection | pass |
| 8. Atomic publication and post-pointer truth | `product_0059_faults_never_publish_a_partial_generation` covers three pre-pointer boundaries. `product_0059_failure_after_current_rename_exposes_the_new_complete_generation` proves pointer replacement publishes the new snapshot and poisons the old handle until reopen | pass |
| 9. Unreferenced-generation recovery | Every fault leaves only old truth readable; exclusive reopen reclaims incomplete and complete-unreferenced generations. `product_0059_retry_reclaims_an_unpublished_same_generation` proves an in-process retry cannot collide with its abandoned target | pass |
| 10. Opaque owner preservation | `product_0059_unknown_owner_survives_base_only_commit_byte_for_byte` carries arbitrary non-UTF-8 bytes and schema `77` through a libaryon-only update; the corruption assay rejects a descriptor/payload length mismatch | pass |
| 11. Strict corruption refusal | `product_0059_corruption_refuses_without_guessing_or_rewriting` injects an unknown base field and session-ID mismatch, proves refusal, and compares the complete tree before/after failed open | pass |
| 12. Permissions and containment | The round-trip recursively asserts no group/world bits and an invariant lock inode across commit/cleanup. `product_0059_no_follow_opens_keep_store_symlinks_inert` attacks root, lock, `CURRENT`, generation, and owner entries and proves outside controls unchanged | pass |
| 13. Ordinary startup remains ephemeral | `product_0059_session_store_is_capability_closed_and_has_no_startup_consumer` finds no engine consumer; default paths never call the explicit store | pass |
| 14. Polariton remains downstream | The same architecture test finds no `SessionStore`, `SavedSession`, or `DocumentId` in Polariton production source | pass |
| 15. Existing view identity unchanged | No Polariton source changes. `LocalViewIdentity::Named` and `Anonymous(output_order)` retain their exact definitions and tests; anonymous edit stability remains explicitly unpromised | pass |
| 16. Zero surface and pixels | Only libaryon core vocabulary/tests plus records change; no engine startup, protocol, Rune, layout, database, plot, table, renderer, SVG, or baseline changes | pass |
| 17. Exclusive mutation capability | Production mutators exist only in `impl SavedSession<Writable>`; architecture coverage enumerates them, the generic implementation is read-only, and the compile-fail rustdoc rejects `SavedSession<ReadOnly>::commit` | pass |

## Implementation findings

Three implementation findings strengthened the accepted design without
widening its scope.

First, subprocess tests initially ran beside ordinary store tests. During the
necessary interval between `fork` and `exec`, the child temporarily shares its
parent's open file descriptions; `O_CLOEXEC` closes them at exec, not fork.
Another test thread could therefore observe a correct transient contention.
The session fixture family is serialized while its actual contenders remain
separate processes. The gate remains post-exec: a live exec child cannot retain
the lock.

Second, a fault after generation rename but before `CURRENT` leaves a complete
same-number generation. Commit and exclusive open now reclaim unreferenced
generations under the lock before staging, making both crash recovery and an
in-process retry deterministic.

Third, pointer rename is the commit point. If the subsequent directory sync
fails, reporting the old in-memory generation would let a retry delete the
newly visible generation. The handle now advances immediately and becomes
publication-uncertain, refusing cleanup or another commit until a fresh locked
open re-establishes `CURRENT`. This is the one new refusal produced by the
implementation audit.

The containment audit also caught a tempting symlink mistake before it landed:
chmodding a path before a no-follow open would mutate a symlink target. Root
creation now opens with `O_NOFOLLOW`, verifies ownership, and applies `fchmod`
through the trusted descriptor.

## Verification

- `cargo test -p libaryon product_0059 -- --nocapture` — 13 behavior tests and
  1 architecture test passed; subprocess helpers also complete with zero
  ignored PRODUCT-0059 tests.
- `cargo test -p libaryon` — 1,453 unit tests passed with the existing 22 manual
  tests ignored; 15 architecture tests, 1 quit-latency test, and the compile-fail
  rustdoc passed.
- `cargo test --workspace --all-targets --no-fail-fast --quiet` — passed with
  no failures across Baryon, Ket, Libaryon, Polariton, Toron, and Wendelon,
  including Polariton's trybuild pair.
- `cargo clippy -p libaryon --tests -- -A clippy::deref_addrof -A
  clippy::map_entry -A clippy::len_without_is_empty -D warnings` — passed. The
  three allowances are pre-existing libaryon findings outside PRODUCT-0059.
- `cargo fmt --all -- --check` and `git diff --check` — passed.
