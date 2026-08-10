# PRODUCT-0071 implementation evidence: durable-session attachment in status chrome

Implemented 2026-08-10 against
`plans/product/adr/0071_durable_session_attachment_in_status_chrome.md`.

Plan: `a38b5bfd`.

## Outcome

Polariton now uses the constant title `Polariton` for both iced's native title
and its custom title bar. PRODUCT-0063's dynamic `application_title` helper is
deleted, so no dormant producer continues to construct an opaque session ID for
chrome that does not consume it.

A saved authority adds exactly one persistent left-lane `durable session`
segment to the production `StatusBar`; an ephemeral authority adds none. The
label names attachment only. It remains unchanged beside successful and failed
checkpoint prose and carries no saved/clean/dirty claim, identity, generation,
or checkpoint age.

The saved-only Session menu remains action-only: `Checkpoint` and `Copy session
ID` in the ordinary state, plus PRODUCT-0063's existing failed-close escape when
armed. The menu displays no identity bytes. Copy still returns the complete
32-character canonical value accepted by `--session`.

Toron's ordinary attachment prose now reads `Attaching Toron workspace…` and
`Joined Toron workspace.`. It no longer exposes the unrelated workspace nonce
or calls that authority a generic session.

## Oracle correction

PRODUCT-0063 gate 4 asserted `application_title(&state)`, but Polariton's custom
decorations never consumed that helper. PRODUCT-0071 classifies the defect as a
PRODUCT-0057 assertion gap while preserving PRODUCT-0057's instrument boundary:
there is no licensed iced-widget chrome recorder.

The replacement evidence observes the production models that the widgets
actually consume:

- `StatusBar::product_0071_text_segments` is `#[cfg(test)]`, returns references
  to already-stored text and its lane, performs no formatting, and is absent
  from production;
- `build_menu_roots` is the exact model passed to `MenuBar::new`; and
- the title has no dynamic model at all—both construction sites contain the
  same literal.

`product_0071_chrome_consumes_the_guarded_title_status_and_menu_models` supplies
the narrow wiring half. It proves `main_screen` calls `.view()` on the sole
`app_status_bar` result, `MenuBar` consumes `build_menu_roots`, the custom title
has no session input, and the retired dynamic helper and full-title spelling are
absent. These source-shape assertions are appropriate because the guarded
property is wiring; they complement the behavioral saved/ephemeral controls and
claim no raster truth.

## Acceptance-gate ledger

| Gate | Evidence | Result |
| --- | --- | --- |
| 1. Constant title | `product_0071_chrome_consumes_the_guarded_title_status_and_menu_models` pins `.title("Polariton")`, the one custom `text("Polariton")` consumer, absence of title session state, absence of `application_title`, and absence of `Polariton — session` | pass |
| 2. Consumed attachment | `product_0071_durable_session_segment_names_attachment_not_freshness` compares otherwise-identical saved and ephemeral `StatusBar` models and requires exactly one left-lane segment only in the saved case; the architecture gate proves `main_screen` consumes that model directly | pass |
| 3. No freshness claim | The same behavioral gate repeats the saved case beside `Session checkpointed` and a checkpoint failure, pins the attachment segment unchanged, and rejects saved/clean/dirty text from persistent left-lane segments | pass |
| 4. No opaque ID in chrome | The title/status wiring gate proves no identity enters the title or reconstructed status path; the two Toron state gates use the recognizable fixture nonce and assert it absent; `product_0071_session_menu_keeps_identity_behind_the_copy_action` pins the exact two ordinary menu labels and no identity row | pass |
| 5. Exact CLI token | `product_0071_copied_identity_remains_the_exact_cli_authority` pins all 32 lowercase hexadecimal bytes and parse-back equality. The architecture gate proves `Message::CopySessionId` still calls `iced::clipboard::write(session_identity_text(session_id))` | pass |
| 6. Distinct authorities | The existing Ready-discovery gate now requires exactly `Attaching Toron workspace…`; `product_0071_joined_toron_workspace_chrome_omits_the_nonce` drives the success handler and requires exactly `Joined Toron workspace.` with the nonce absent. The durable segment is independently model-gated | pass |
| 7. PRODUCT-0063 preserved | All PRODUCT-0063 focused tests pass, including Session-menu visibility, production create/open/checkpoint/close/reopen, and the SIGKILL boundary. Only the superseded title assertion was removed | pass |
| 8. Honest oracle boundary | The plan and this evidence name the assertion gap, absence of a licensed iced recorder, consumed `StatusBar`/`MenuRoot` models, test-only observation, structural wiring guard, and separate clipboard/parser oracle | pass |
| 9. Bounded suites | Default Polariton, no-default focused, all-feature/all-target check, inherited PRODUCT-0063, complete single-threaded workspace, coordinate trybuild, and PRODUCT-0057 Vulkan evidence all pass. No baseline is refreshed | pass |

## Verification

```text
cargo test -p polariton product_0071 -- --nocapture
    pass (4 unit + 1 architecture)
cargo test -p polariton product_0063 -- --nocapture
    pass (3, including isolated restart and SIGKILL children)
cargo test -p polariton --no-default-features product_0071 -- --nocapture
    pass (3 unit + 1 architecture; existing feature-matrix warnings only)
cargo check --workspace --all-targets --all-features
    pass
cargo test -p polariton --all-targets --no-fail-fast
    pass (530 unit + 45 architecture + 1 trybuild; 5 pre-existing live tests ignored)
cargo test -p polariton --all-features \
  --test product_0057_render_evidence -- --nocapture
    pass (Intel UHD 770 Vulkan; all PRODUCT-0057/0068 probes)
cargo test --workspace --all-targets --no-fail-fast -- --test-threads=1
    pass
cargo fmt --all -- --check
    pass
git diff --check
    pass
```

No plot, table, SVG, GPU, dashboard, persistence schema, store, document,
layout, colour, or Toron protocol baseline changed.
