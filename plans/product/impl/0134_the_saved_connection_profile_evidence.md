# PRODUCT-0134 evidence — the saved connection profile

Implementation of the amended PRODUCT-0134, following
`plans/product/impl/0134_the_saved_connection_profile.md`. Five commits, in the
plan's order:

```text
246185fb  test: adr 0134 capture adapter configuration baselines
9f10d3be  impl: adr 0134 decompose postgres and mysql connection fields
836eff72  impl: adr 0134 add the encrypted profile store
53026f12  impl: adr 0134 add lazy unlock and refusal behavior
d33e6097  impl: adr 0134 connect the saved profile surface
```

Verification at close: `cargo test -p polariton --bin polariton` — **943
passed, 0 failed, 8 ignored**; `--test architecture_test` — **95 passed**;
clippy clean for every file this arc touched. The known pre-existing
`product_0063_saved_startup` parallel-run flake fired intermittently during
verification runs and passes in isolation; it is unrelated to this arc and
already recorded.

## Gate ledger

### Stage 0 — census and baselines

- Census tests pin the pre-arc surface: one plaintext field per adapter, the
  `connection_identity` trim §4 later promotes to profile identity, and the
  single `connect:new` menu action.
  (`adapters/product_0134_baseline.rs`, `gui/components.rs` tests.)
- Per-adapter 0b baselines, no network: SQLite end-to-end (`memory` + temp
  file), Parquet end-to-end against a locally written fixture, Postgres and
  MySQL pinned at the typed-string → driver-argument seam
  (`tokio_postgres::Config` parse, `mysql_async::Opts::from_url`), BigQuery and
  Toron configuration-only.
- No production behavior changed in the commit.

### Stage 1 — decomposed fields

- Postgres and MySQL declare host, port, database, user, password; `password`
  is the only secure field — the masking branch at
  `gui/components.rs` `adapter_configuration_view` gained its first production
  consumer. The four uncredentialed adapters are byte-identical
  (`product_0134_fields_decompose_only_the_credentialed_adapters` pins all
  six).
- Assembly happens once per adapter, directly to driver arguments —
  `postgres::config_from_fields`, `mysql::opts_from_fields` — with the
  stage 0 typed-string parses as equality oracles
  (`product_0134_*_fields_match_the_typed_string_oracle`). No connection
  string is built and re-parsed, so a password needs no percent-encoding
  (asserted: field `p@ssword` equals URL `p%40ssword`).
- Port refuses non-numeric input at connect time; empty optionals fall to
  driver defaults (5432/3306, no dbname, no password).
- The ad hoc string is a separate input kind: its own dialog section and
  message, `AdapterConfiguration::{Postgres,MySQL}` retained for it unchanged,
  and a submission mixing kinds refuses
  (`AdapterState::input_kind_error`; a field still holding its seeded default
  is untouched).
- `AdapterConfiguration` lost its derived `Debug`; the manual impl redacts the
  password in the field variants (`…debug_redacts` asserts the rendering).

### Stage 2 — the encrypted store

- `profile_store.rs`: name-keyed map of `{adapter_type, fields}` sealed as one
  `BARYENC\0` file beside `init.rn` (`default_path` reuses
  `configuration::default_init_path`; `configuration.rs` itself is untouched).
  `secrets.rs` consumed unchanged — the arc's libaryon diff is empty.
- Round trip across instances reconstructs an identical
  `AdapterConfiguration` through `from_saved`
  (`product_0134_store_round_trips_a_profile_across_instances`).
- §4 identity rules each have a refusal test: create-on-existing,
  rename-onto-existing, empty/whitespace name, trim-then-exact matching, and
  the deliberate `prod`/`Prod` coexistence pin.
- A failed open reports, locks again, and leaves the sealed file
  byte-identical; retry succeeds. `initialize` refuses an existing file.
- The store file is written atomically with owner-only permissions (asserted
  0o600 on unix). Sealed bytes disclose neither profile names nor field
  values (`…sealed_bytes_disclose_no_names_or_credentials`).
- `from_saved` consumes the field map the editor produces; the single-string
  form is gone.

### Stage 3 — lazy unlock and refusal

- The prompt opens only through `request_profile_access`; a session that never
  touches the store is never prompted (asserted).
- First write creates the passphrase with a double entry; a mismatch refuses
  **before any file exists** (asserted: no file on disk after the refusal).
- Cancel is routine `status_msg` feedback, never `status_error`; the
  application stays usable, no adapter is connected, and the scratch target is
  not silently substituted (the PRODUCT-0005 failure shape, asserted).
- Wrong passphrase keeps the prompt open for retry with the store intact.
- Profile and document secret sessions are independent instances
  (`…profile_session_is_independent_of_other_secret_sessions`), and no locked-
  store error can surface Baryon command vocabulary
  (`…locked_store_errors_use_polariton_wording` sweeps every locked-path
  error for `:unlock`/`:eenc`/`:wenc`).

### Stage 4 — the surface

- Connect menu: `connect:new` first and unchanged; `Save profile`; locked →
  one `Profiles…` access-requesting action (names live inside the
  ciphertext); unlocked → per-profile Connect / Edit / Delete, names taken
  verbatim after fixed id prefixes.
- A profile saved in one process connects in the next, driving the unlock
  prompt, the pending operation, `from_saved`, and a real SQLite connect
  (`product_0134_profile_saved_in_one_process_connects_in_the_next`).
- `from_saved`'s `#[allow(dead_code)]` is removed; `load_profile` is its
  production caller. Status identity is the profile name.
- §4 at the surface: the dialog Name is the identity; save is create
  (refuses existing) or edit-replace per `editing_profile`; rename is edit
  plus a changed Name (refuses onto existing); delete removes from store and
  menu; `connect:new` clears the editing identity.
- Saving while the ad hoc kind is active refuses: a profile is named fields,
  never a connection string.

## Deviations and judgment calls

1. **Field requiredness.** The record does not decide which fields the dialog
   requires. Implemented: non-secure fields required, secure fields optional —
   an absent credential is an authentication policy (trust auth, passwordless
   local), not a form error. The four single-field adapters keep their exact
   prior requiredness.
2. **Port defaults seed.** `port` declares `default_value` 5432/3306, seeded
   by `select()` exactly as SQLite's `memory` already was. The stage 0
   `placeholder_examples_do_not_become_connection_values` test was updated to
   assert precisely its claim: placeholder examples never seed; declared safe
   defaults do.
3. **`unlock` refuses a missing file.** Creation is only `initialize`, so a
   mistyped store path can never silently mint an empty store. Slightly
   stronger than the record's text; aligned with §8's distinct first-write
   flow.
4. **The zeroizing boundary.** Prompt state holds `Zeroizing<String>` and the
   store takes `Zeroizing<Vec<u8>>`, but iced's `on_input` necessarily hands
   over a transient plain `String` per keystroke, moved into the zeroizing
   container and dropped. The plan's "never copied into a plain `String`" is
   met everywhere the code controls; the widget-boundary transient is the
   floor iced imposes.
5. **Architecture guards amended, each citing its record.** PRODUCT-0062's
   serde guard becomes a two-owner allowlist (session owner + this store,
   citing PRODUCT-0134), and its site collection is now sorted — `read_dir`
   order was a latent flake. The PRODUCT-0057 mirror gains `profile_store`.
   The PRODUCT-0071 menu guard string tracks `build_menu_roots`' third
   argument.
6. **`SavedProfile` has no `Debug`.** A profile carries secure values;
   redaction lives once, on `AdapterConfiguration`. A store payload can never
   transit a debug format.
7. **Store schema.** `schema_version: 1` inside the ciphertext; a newer
   version refuses rather than guessing. `deny_unknown_fields` on the payload.
8. **`ProfileStore::lock` has no production caller.** No lock affordance was
   specified; the §7 independence test exercises it, and a menu affordance is
   a later decision. Annotated, not deleted.
9. **Stage 0 pins deliberately updated where the surface moved.** The
   fields-census and connect-menu tests were rewritten in stages 1 and 4
   respectively — they pinned the surfaces those stages exist to move. The
   four uncredentialed adapters and both typed-string oracles stayed
   byte-identical throughout, re-asserted at every stage.

## Cross-stage guards, verified at close

- **No credential outside the secure boundary:** masked input, redacting
  `Debug`, no-`Debug` profile, sealed-bytes disclosure test, and no adapter
  field ever enters the durable session — `session_persistence.rs` has an
  empty diff across the arc.
- **`init.rn` untouched:** `configuration.rs` has an empty diff; the store
  path reuses `default_init_path`'s directory only.
- **`crates/libaryon` untouched:** empty diff across the arc; the halt rule
  ("libaryon must not learn what a connection profile is") never fired.
- **The ad hoc path stays whole:** stage 0b baselines pass unchanged at every
  stage.

## Left open, correctly

Connection testing before save; per-profile passphrases; passphrase rotation;
keychain/agent discovery; DSN import; a richer list surface; a store lock
affordance; custom CA / TLS policy fields (the WebPKI-roots limitation note
stays on the Postgres adapter). Each requires its own record per the ADR's
Consequences.
