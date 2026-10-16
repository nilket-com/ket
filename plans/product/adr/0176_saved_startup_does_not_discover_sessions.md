# ADR 0176: saved startup does not discover sessions

Status: proposed 2026-10-16. Application lane. Closes a PRODUCT-0059
architecture-guard gap found after ADR 0174 landed. It does not alter
saved-session behavior.

ADR 0174 replaced a false-positive scan of every Polariton `.discover()`
method with a direct assertion on the terminal's
`SessionStore::platform_default()?.discover()?` call. The existing
`SessionStore` allowlist still permits `gui/state.rs`, because it owns saved
startup creation and writable reopening. It did not explicitly prevent that
second surface from acquiring read-only discovery authority as well.

## Decision

`main.rs::list_saved_sessions` remains the only Polariton saved-session
discovery surface. The PRODUCT-0059 guard continues to require its explicit
read-only call and separately rejects `.discover()` in `gui/state.rs`.

The guard is deliberately scoped to the saved-startup surface. It does not
inspect generic `.discover()` methods elsewhere in Polariton, so ADR 0173's
MCP live-window discovery remains independent from saved-session authority.

## Acceptance

`product_0059_store_stays_capability_closed_and_later_consumers_are_explicit`
passes with the current MCP bridge and writable GUI startup, and fails if
`gui/state.rs` adds a discovery call or if the terminal listing loses its
read-only `SessionStore::discover` path.
