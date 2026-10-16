# ADR 0174: session-store discovery is not a method name

Status: proposed 2026-10-16. Application lane. Corrects the PRODUCT-0059
architecture guard after ADR 0173 added the MCP bridge. It does not change
saved-session authority or the MCP surface.

PRODUCT-0059 keeps `SessionStore` out of the engine and gives the desktop
explicit consumers of its capabilities. PRODUCT-0073 then made the terminal
entrypoint's `list_saved_sessions` command the read-only consumer of
`SessionStore::discover`. The architecture test expressed that rule by
rejecting `.discover()` in every Polariton production file except `main.rs`.
That names a Rust method spelling, not the saved-session capability. ADR
0173's MCP bridge also has `Bridge::discover`, which finds live window
advertisements through `live::discovery`; it owns no `SessionStore` and has
no saved-session authority. The guard therefore rejects an allowed,
unrelated discovery operation.

## Decision

The PRODUCT-0059 guard follows `SessionStore`, rather than the unqualified
`.discover()` spelling:

1. Production uses of `SessionStore` remain confined to the existing saved
   desktop surfaces (`main.rs` and `gui/state.rs`); the engine remains free
   of it.
2. The terminal listing is checked directly in `main.rs`: its
   `list_saved_sessions` implementation calls
   `SessionStore::platform_default()?.discover()?`, and that bounded listing
   contains neither creation nor writable opening.
3. Other discovery methods remain outside this guard. They are reviewed by
   the authority they actually carry; in particular, MCP live advertisement
   discovery is not saved-session discovery.

This keeps the intended failure modes: moving `SessionStore` into an engine
module or an unapproved Polariton surface fails, and replacing terminal
listing with a writable store operation fails. Adding a distinct discovery
method does not fail solely because its name is `discover`.

## Acceptance

`product_0059_store_stays_capability_closed_and_later_consumers_are_explicit`
passes with ADR 0173's MCP bridge present, fails if a production Polariton
module outside the two approved saved-session surfaces refers to
`SessionStore`, and fails if the terminal listing no longer uses the
read-only `SessionStore::platform_default()?.discover()?` path.
