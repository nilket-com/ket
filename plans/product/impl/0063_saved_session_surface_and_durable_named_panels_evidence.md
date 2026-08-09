# PRODUCT-0063 implementation evidence: saved-session surface and durable named panels

Status: accepted 2026-08-09.

Plan: `02351647`
(`plans/product/adr/0063_saved_session_surface_and_durable_named_panels.md`).

## Result

Polariton can now create or continue one exact saved session with
`--session new` or `--session <SessionId>`. The ordinary no-selector and
positional-file launches remain ephemeral. Saved startup resolves the platform
store before Iced starts and transfers the non-clone writable handle through a
one-shot initializer into PRODUCT-0062's registered application owner.

Saved windows display the full canonical identity and expose a saved-only
Session menu. Manual and close-time publication share one typed checkpoint
controller. A successful saved close freezes every ordinary message, abandons
undispatched runs, admits only the existing Toron drain completion, and then
closes. A refused close stays live and alone arms the explicit `Close without
checkpoint` escape. Ephemeral close keeps both pre-existing exits unchanged.

The production restart fixture creates a session through the same resolver as
`main`, checkpoints dirty scratch bytes plus two named outputs and a categorical
allocation, drains a real local-workspace owner, reopens by the copied identity,
and observes exact panel IDs, group, logical placement, canonical colour
lineage, and unresolved placeholders before rerun. A separate process fixture
edits after a successful checkpoint, dies by `SIGKILL`, and proves that the
kernel released the lock while the predecessor—not the uncommitted edit—remains
authoritative.

## Acceptance-gate ledger

| Gate | Evidence | Result |
| --- | --- | --- |
| 1. Startup matrix | `saved_session_selector_is_exact_and_exclusive`, `first_positional_argument_is_the_initial_file`, and `double_dash_allows_a_dash_prefixed_path` pin accepted forms and specific missing/duplicate/short/uppercase/malformed/zero/conflicting refusals | pass |
| 2. Default ephemeral | The production-authority fixture holds an existing saved session while resolving `Ephemeral { initial_file: None }`; the title/menu gates retain `Polariton` and no Session action, and the ephemeral constructors never touch `SessionStore` | pass |
| 3. Production create/open | `product_0063_saved_startup_checkpoint_close_and_reopen_use_production_authority` uses `resolve_startup(CreateSaved)` and `resolve_startup(OpenSaved(id))`, then `new_with_authority`/`open_saved_with_init`; only `XDG_STATE_HOME` is injected | pass |
| 4. Visible round-trip identity | `product_0063_window_title_names_only_saved_sessions` pins the exact title, 32-byte copied text, and parse-back identity; `CopySessionId` consumes that one tested formatter | pass |
| 5. Contention | The production-authority fixture opens the same selected ID concurrently, requires an identity-bearing contention refusal, then successfully reopens the same ID after the first editor drops | pass |
| 6. Coupled manual checkpoint | The production fixture publishes panel meanings and colour state through `CodeEditor::checkpoint_session`; PRODUCT-0061's `product_0061_coupled_publication_faults_never_mix_owners` remains the complete seam-fault oracle | pass |
| 7. Close before teardown | The production fixture checkpoints a dirty Rune scratch, two-panel layout/group, and colour allocation before taking a real workspace owner; reopen observes the dirty bytes and every durable meaning | pass |
| 8. One stale attempt | The fixture mints two panels against the preceding projected census, proves one close refusal with the typed stale cause and no close task, waits for the next census, then proves the next deliberate close succeeds with no retry loop | pass |
| 9. Frozen successful close | The fixture leaves a run waiting on its snapshot, freezes after checkpoint, and injects run initiation, run completion, and scratch completion. The top-level closed-state guard rejects every message except `LocalWorkspaceStopped`; the admitted completion alone emits close. State and status remain unchanged, and reopen matches the committed generation | pass |
| 10. Visible failure and deliberate discard | Manual not-ready checkpoint leaves `SavedCloseState::Open` and cannot invoke discard. Close-time refusal enters `Failed`, makes the menu action visible, and only that action enters `Frozen` and closes without advancing a generation | pass |
| 11. Publication uncertainty | PRODUCT-0060's post-`CURRENT` fault gate proves the saved handle is poisoned and a fresh open observes one complete generation. PRODUCT-0063 preserves that typed engine error through `PolaritonCheckpointError`; its failed-close state supplies the only discard route and contains no recovery/reopen mutation | pass |
| 12. Crash is not close | `product_0063_sigkill_is_not_a_close_checkpoint` kills the real holder after a visible uncheckpointed edit, proves reopen succeeds while omitting those bytes, and therefore pins both lock release and the absence of Drop publication | pass |
| 13. Durable named panels | The production restart fixture carries two named outputs across the real store/open boundary and pins exact `HostPanelId`s, group, placement, and colour. Both are unresolved on startup and reuse their exact panels on explicit preparation/rerun | pass |
| 14. Scratch/document namespace | Gate 13's producer is an untitled saved scratch. PRODUCT-0060's path-retarget and distinct-document identity gates plus PRODUCT-0062's reversed-open-order and alien-session controls remain green; keys are still `(DocumentId, LocalViewIdentity)` within one `SessionId` owner | pass |
| 15. Restore never executes | Reopen restores dirty authored bytes and both placeholders with neither plot nor table content. The only startup tasks remain scratch-database creation and local-workspace discovery; no SQL/Rune execution task is constructed. Explicit preparation is required to resolve the placeholders | pass |
| 16. Feature and oracle honesty | Default and `--no-default-features` all-target suites pass; the latter runs `product_0063_no_toron_build_refuses_saved_authority_explicitly`. The PRODUCT-0057 opt-in Vulkan evidence target builds and all four raster probes pass | pass |

## Implementation findings

The first restart extension deliberately compared PRODUCT-0061's logical
placement facts rather than runtime `PaneId` and `ViewId`. Those counters are
reminted after restart; requiring their raw values to match would reject a
correct restoration. The durable oracle therefore compares panel identity,
group, rectangle, tab cardinality/activation, and focus correspondence.

The opt-in PRODUCT-0057 target found a genuine synthetic-crate seam. The first
implementation put `StartupTarget` in `main.rs`; the renderer harness mirrors
production modules but does not include binary-root items, so its all-feature
build could not name the type. Moving the closed vocabulary into the GUI state
module gives the binary and harness one definition without widening public API.
This is a different maintenance class from the earlier missing-module defect,
and the real opt-in build—not the source-text module guard—caught it.

The no-default all-target sweep exposed an old PRODUCT-0042 test that consumed
toron-only Rune relation types without carrying the feature gate already held
by its production path. Adding that test gate changes no default behavior and
makes the accepted no-toron build claim executable rather than binary-only.

The stale application refusal became a distinct `SessionCheckpointError`
variant. Its display text is unchanged; the type lets the close surface offer
the bounded “close again to retry” guidance only for the failure that is
actually retryable, without parsing prose at the UI boundary.

PRODUCT-0062's temporary module-wide dead-code allowance was removed. The
owner codec, capture, join, constants, and runtime colour carrier are now all
consumed by production startup/checkpoint paths, so ordinary compiler warnings
again guard the module.

## Verification

- `cargo test -p polariton product_0063_ -- --nocapture` — four PRODUCT-0063
  surface, restart, crash, identity, and menu tests passed; zero ignored.
- `cargo check -p polariton --all-targets` — passed without warnings.
- `cargo test -p polariton --all-targets --no-fail-fast` — 493 unit tests,
  44 architecture tests, and the coordinate-space trybuild gate passed; only
  the five pre-existing live-Toron tests remain ignored.
- `cargo test -p polariton --no-default-features --all-targets --no-fail-fast`
  — 324 unit tests, 44 architecture tests, and trybuild passed; the no-toron
  saved-session refusal executed. Existing feature-matrix warnings remain.
- `cargo test -p libaryon --all-targets --no-fail-fast` — 1,483 unit tests,
  18 architecture tests, quit latency, and all seven trybuild pairs passed;
  only pre-existing manual/benchmark tests remain ignored.
- `cargo test --workspace --all-targets --no-fail-fast --quiet` — passed across
  the complete workspace with no baseline refresh.
- `cargo test -p polariton --test product_0057_render_evidence --features
  product-0057-render-evidence` — passed on the Intel UHD 770 Vulkan adapter;
  overlap, clip, MSAA, and text probes all remained automated and stable.
- Targeted Polariton clippy passed with the repository's existing explicit
  lint allowances; a bare `-D warnings` still reports only the previously
  recorded libaryon and plotting lint debt, not PRODUCT-0063 code.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

## Implementation history

The reviewable working stack banked ephemeral close, added the production
surface, strengthened restart/crash gates, and repaired the opt-in harness
seam in separate commits. It is compressed to one
`impl: adr 0063 saved session surface and durable named panels` commit after
this evidence and the requirement audit pass.
