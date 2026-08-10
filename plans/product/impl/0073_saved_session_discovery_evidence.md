# PRODUCT-0073 implementation evidence: saved-session discovery

Implemented 2026-08-10 against
`plans/product/adr/0073_saved_session_discovery.md`.

Plan: `97a120b4`.

## Outcome

`polariton --session list` is now a terminal action. It resolves the
platform-default libaryon store, prints a stable human report, and exits before
loading startup Rune, configuring rustls, connecting to Toron, constructing an
engine, or opening iced. Bare startup and the existing `new` and exact-ID GUI
selectors retain their prior paths.

Libaryon's new head-neutral discovery operation enumerates the existing root
without creating it and classifies each canonical identity as `Committed`,
`Active`, `Damaged`, or `Unreadable`. Foreign root entries retain exact bytes
and are reported without traversal. Discovery composes the existing
`open_read_only` boundary, so a live exclusive owner wins before `CURRENT` and
produces an ID-only active row rather than a racy snapshot.

Committed rows carry only validated base-store facts: generation, immutable
generation-directory modification time, and `DocumentId`-ordered document
paths. Owner payload bytes are length-validated by the store but never decoded
as Polariton semantics. Scratch documents use `<scratch>`; non-UTF-8, control-
character, and literal `<scratch>` paths use exact lowercase hexadecimal, so
two different paths cannot acquire the same display identity.

The installed state was exercised directly:

```text
STATE      UPDATED_UTC           GEN  DOCS  SESSION
committed  2026-08-09T22:26:59Z    4     1  24631c77d510daf33ae6331e1f4776ce
  document 2  /home/me/work/ket/../uat/test16.rn
```

## Implementation finding

The complete workspace sweep found one bounded premise update. PRODUCT-0059's
architecture gate forbade every `SessionStore` mention in Polariton because no
surface consumer then existed. PRODUCT-0073 deliberately adds the first one.
The gate now permits `SessionStore::platform_default()?.discover()?` only in
`main.rs`'s terminal action, while continuing to reject store access elsewhere
and any `create` or `open_writable` authority at that seam. The implementation
did not weaken or remove the capability boundary; it made the accepted
read-only consumer explicit.

## Acceptance-gate ledger

| Gate | Evidence | Result |
| --- | --- | --- |
| 1. Terminal action | `ApplicationAction::ListSessions`, parser controls, and `product_0073_terminal_list_is_non_mutating_discriminating_and_head_neutral` prove listing exits as a process without GUI startup; positional controls refuse | pass |
| 2. Empty and unsafe roots | The libaryon census gate proves an absent root remains absent; the process prints exactly `No saved sessions.`. A mode-unsafe root refuses | pass |
| 3. Committed projection and order | The process gate pins full IDs, document counts, scratch/file/non-UTF-8 lines, and UTC shape. `committed_session_order_is_newest_then_generation_then_identity` pins mtime, generation, ID, and unknown-time ordering independently | pass |
| 4–5. Active and lock ordering | A real exclusive child remains alive while discovery reports `Active`; corrupting `CURRENT` does not change that result. After SIGKILL/reap, the exact pointer corruption becomes `Damaged` | pass |
| 6. Damage remains visible | Existing store corruption fixtures plus the discovery damage control cover malformed pointer, manifest, owner length, and missing-boundary refusals without recovery or omission | pass |
| 7. Raw and wrong-type entries | `product_0073_canonical_wrong_types_are_unreadable_and_an_unsafe_root_is_not_empty` pins canonical regular-file and symlink entries as unreadable; the census gate retains UTF-8 and non-UTF-8 foreign names byte-exactly | pass |
| 8. Zero mutation | The census records the complete file-byte tree before and after discovery; active, damaged, foreign, and absent-root controls prove no cleanup, pointer write, lock creation, or root creation | pass |
| 9. Head-neutral truth | The process fixture commits deliberately invalid `polariton_session` JSON and still lists the base-valid session as committed; source guards prohibit owner semantic reads | pass |
| 10. Exact paths | The process pins normal UTF-8, non-UTF-8 hex, scratch, and a real path spelled `<scratch>` as distinct output | pass |
| 11. Existing startup and features | Existing argument tests remain unchanged through a local adapter; new/list/ID/ephemeral controls pass. The no-default-feature gate refuses listing explicitly before store access | pass |
| 12. Architecture | Polariton and libaryon guards keep listing out of `StartupTarget`, restrict the store consumer to the terminal entrypoint, and forbid writable operations, payload decoding, aliases, deletion, indexing, and autosave there | pass |
| 13. Documentation | `crates/polariton/README.md` documents list/copy/open, active opacity, and the difference between committed time and dirty-state freshness | pass |
| 14. Verification | Focused, package, feature-matrix, all-feature check, serial workspace, formatting, and diff gates pass; no ignored discovery test was added | pass |

## Verification

```text
cargo test -p libaryon product_0073 --lib                 pass (4)
cargo test -p polariton product_0073                     pass (1 architecture + 1 process)
cargo test -p polariton --no-default-features \
  listing_refuses_explicitly_without_saved_session_support pass
cargo test -p libaryon --lib --no-fail-fast              pass (1487; 22 existing ignored)
cargo test -p polariton --all-targets --no-fail-fast     pass
cargo check --workspace --all-targets --all-features     pass
cargo test --workspace --all-targets --no-fail-fast \
  -- --test-threads=1                                    pass after the named premise update
cargo fmt --all -- --check                               pass
git diff --check                                         pass
```

No GUI, engine, Toron protocol, session schema, owner payload, persisted byte,
plot, table, SVG, GPU, or baseline changed. The local Toron PRODUCT-0544 handoff
receipt remains unrelated and untracked.
