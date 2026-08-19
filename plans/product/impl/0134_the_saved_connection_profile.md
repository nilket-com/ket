# Implement the saved connection profile

Scope: implement PRODUCT-0134. Decompose the Postgres and MySQL field surfaces
into named connection and authentication fields, add a name-keyed encrypted
profile store, unlock it lazily behind its own secret session, and connect the
saved-profile surface. The implementation adds no TLS or `sslmode` policy, no
custom CA material, no connection testing, no per-profile passphrase, no
keychain or credential-agent integration, and no DSN import.

The work is six commits, in this order:

```text
test: adr 0134 capture adapter configuration baselines
impl: adr 0134 decompose postgres and mysql connection fields
impl: adr 0134 add the encrypted profile store
impl: adr 0134 add lazy unlock and refusal behavior
impl: adr 0134 connect the saved profile surface
evidence: close saved connection profile
```

The first commit changes no production behavior. It pins the current dialog,
adapter-field, and connect-path boundaries before the field surface changes
under them. The evidence commit records the gate ledger and verification; it
contains no implementation change.

**Sizing, stated up front because the arc has been mis-sized twice.** The
cryptography is finished and needs no work. `from_saved` is a sketch with the
wrong signature, not a finished half. The real cost is stages 1 and 4 — two
connection dialogs and the profile surface — not stage 2. Any plan revision that
treats this as "wire up `from_saved`" is wrong.

## Halt rule

PRODUCT-0134 permits decomposed connection and authentication fields for
Postgres and MySQL, a profile map keyed by a user-chosen name, masking through
the existing `is_secure` branch, a `BARYENC\0` store in the configuration
directory, a separate `SecretSession`, lazy unlock at the first store operation
requiring access to the profile store, first-write passphrase creation, and
refusal that leaves
the application usable.

Stop and return the finding to PRODUCT-0134 or a new owning record if
implementation requires any of:

- an `sslmode`, TLS-policy, or custom-CA field — explicitly out of scope, and
  transport policy owns its own record;
- connection testing before save;
- a per-profile passphrase, passphrase rotation, or re-keying;
- system keychain or cloud credential-agent discovery;
- importing an existing DSN, or parsing a connection string into fields;
- decomposing SQLite, Parquet, BigQuery, or Toron;
- any path that injects a stored credential into an ad hoc connection string,
  which would demand the precedence rules §10 refuses;
- a second encrypted-file format, or any change to `BARYENC\0`;
- profile-name case folding, Unicode normalization, or similarity matching;
- writing any profile data into `init.rn`.

The work also stops if `crates/libaryon` must learn what a connection profile
is. PRODUCT-0001 fences libaryon against connectors; this arc consumes
`secrets.rs` as a primitive and adds no profile vocabulary to it. A required
change *inside* `secrets.rs` is a halt, not a convenience.

### Where work may land

Expected production areas are:

- `crates/polariton/src/adapters/common.rs` for `AdapterField` declarations and
  any field-map type shared by the store and the dialog;
- `crates/polariton/src/adapters/postgres.rs` and `adapters/mysql.rs` for the
  decomposed `FIELDS` and their connect-time assembly;
- `crates/polariton/src/adapters/driver.rs` for `AdapterConfiguration`,
  `from_saved`'s new signature, and the connect path;
- a new product-side profile-store module under `crates/polariton/src/` for the
  encrypted map, its name identity, and its create/edit/rename/delete rules;
- `crates/polariton/src/configuration.rs` for the store path, reusing
  `config_dir` rather than adding a second path rule;
- `crates/polariton/src/gui/components.rs` for the configuration dialog, the
  masking branch at `:967`, and the Connect menu; and
- `crates/polariton/src/gui/state.rs` and `gui/messages.rs` for unlock messages,
  refusal wording, and status reporting.

`crates/libaryon/src/secrets.rs` is consumed unchanged. Any diff to it, or to
the durable-session schema, is a halt unless a cited Stage 0 finding proves it
is an existing seam rather than a new contract.

## Stage 0 — census and baselines

### 0a. Census the current configuration surface

Record, with citations, the present state of every boundary this arc moves:

- each adapter's `FIELDS` (six adapters, all single-field, all
  `is_secure: false`);
- `AdapterState { name, fields, configuration, connection }` and
  `connection_identity` (`driver.rs:127`), including the existing required Name
  field that §4 promotes to profile identity;
- `AdapterConfiguration::from_saved` (`driver.rs:148`), its `#[allow(dead_code)]`
  marker, and its existing unit tests;
- the masking branch in `adapter_configuration_view` (`components.rs:967`) and
  the fact that no adapter reaches it;
- the Connect menu's single `connect:new` item;
- `SecretSession`'s API surface and the `BARYENC\0` format, including
  `file_salt`, `write_private_state_atomically`, and the `Zeroizing` discipline;
- `config_dir` (`configuration.rs:290`) and `default_init_path`.

### 0b. Baseline configuration and assembly, per adapter

Adapters do not share one input kind, so there is no single "connection string"
baseline to take. Only Postgres and MySQL accept a connection string; BigQuery
takes a project id and authenticates through ambient application default
credentials; SQLite and Parquet take paths; Toron takes an endpoint.

Baseline what each adapter actually does with its configuration, and require no
external network:

| adapter | baseline |
|---|---|
| SQLite | real connect, `memory` and a temp-file path |
| Parquet | real connect against a repository fixture file |
| Postgres | configuration and connect-argument assembly from a typed string |
| MySQL | configuration and connect-argument assembly from a typed string |
| BigQuery | configuration only; ADC is not exercised |
| Toron | configuration only; a live deployment is not required |

SQLite and Parquet give deterministic end-to-end connects. The other four are
pinned at the point where configuration becomes driver arguments — which is
precisely the seam stage 1 moves for Postgres and MySQL, and precisely what must
stay byte-identical for the other four.

**The regression surface that matters:** the ad hoc input path for Postgres and
MySQL must keep working identically while the field surface changes beneath it,
and the four uncredentialed adapters must not move at all.

### Acceptance gates

- No production behavior changes.
- Every claim in the census carries a file and line citation.
- Each adapter has a baseline of the kind named above; none requires a network.

Commit: `test: adr 0134 capture adapter configuration baselines`.

## Stage 1 — decompose the Postgres and MySQL field surfaces

### The fields

Per PRODUCT-0134 §2, exactly:

| adapter | fields | secure |
|---|---|---|
| Postgres | host, port, database, user, password | password |
| MySQL | host, port, database, user, password | password |

SQLite, Parquet, BigQuery, and Toron are **untouched**. Adding fields to an
adapter that carries no credential is a halt.

### Assembly happens once

A field map is assembled into whatever the driver requires at exactly one point
per adapter, on the connect path. No second assembler exists, and the dialog
never builds a connection string for display.

`port` stays its own field and refuses non-numeric input at connect time.
`AdapterFieldType` remains `Text`.

### Both input kinds coexist

An ad hoc connection string and a decomposed field set are separate inputs to the
same adapter. Neither is derived from the other, and stage 0b's baseline proves
the ad hoc path is unchanged.

### Acceptance gates

- Postgres and MySQL declare five fields each, with `password` secure.
- The masking branch at `components.rs:967` has its first production consumer.
- The per-adapter stage 0b baselines pass unchanged: SQLite and Parquet still
  connect end to end, Postgres and MySQL assemble identical driver arguments
  from a typed string, and BigQuery and Toron configurations are byte-identical.
- No credential value appears in any status message, diagnostic, log, or copied
  text.
- The other four adapters' `FIELDS` are byte-identical to stage 0.

Commit: `impl: adr 0134 decompose postgres and mysql connection fields`.

## Stage 2 — the encrypted profile store

### Shape

A map keyed by profile name; each value an adapter type plus a field map. One
`BARYENC\0` file beside `init.rn`, resolved through `config_dir`. Sealed with
`SecretSession::write`, read with `open`, salt via `file_salt`.

### Identity and collisions

Per §4, exactly: names compared after trimming leading and trailing whitespace,
empty or all-whitespace refused, **no** folding or normalization. Create refuses
on an existing name; edit replaces in place; rename refuses onto an existing
name; delete removes the entry.

### `from_saved`

Its signature changes to consume the field map. The single-string form is
removed, not deprecated alongside. `#[allow(dead_code)]` comes off when stage 4
supplies the caller; its existing unit tests are rewritten against the map.

### Acceptance gates

- Round-trip: seal, reopen, and reconstruct an `AdapterConfiguration` identical
  to the one the editor produced.
- Create-on-existing, rename-onto-existing, and empty-name all refuse, each with
  its own test.
- `prod` and `Prod` coexist as distinct profiles, asserted deliberately so the
  chosen exact-match rule is pinned rather than incidental.
- A failed open leaves the store file unmodified — no clear, no rewrite, no
  migration.
- The store file is written with private permissions, atomically.
- `secrets.rs` is unchanged.

Commit: `impl: adr 0134 add the encrypted profile store`.

## Stage 3 — lazy unlock and refusal behavior

### Timing

Per the amended §8, the passphrase is requested at the **first operation
requiring access to the profile store** — list, select, save, edit, rename, or
delete — not at connect. Listing profiles needs the key, because §6's single sealed file keeps
names inside the ciphertext.

Startup, editing, Rune authoring, Parquet scanning, ad hoc connections, and the
scratch SQLite target never prompt. Once unlocked, the session serves every
subsequent operation for the process lifetime.

### First write creates the passphrase

When no store file exists, the first save establishes the passphrase with a
double-entry confirmation and seals a new file. This is a separate flow from
unlocking an existing store and is implemented as one: a mistyped passphrase at
creation would seal a store that can never be opened.

### A separate session

Polariton's profile `SecretSession` is a distinct instance from libaryon's
document session. Locking or unlocking one must not affect the other, and this
is asserted, not assumed — it is the §7 property most likely to be silently lost
to a convenient shared reference.

### Refusal

Cancel or wrong passphrase leaves the application running with no adapter
connected. No modal wedge, no exit, no fallback to a different connection, and
specifically no silent fall-through to the scratch SQLite target — PRODUCT-0005
already recorded that failure mode once, where a failed Toron connect silently
answered SQL from an empty scratch engine.

`require_passphrase` returns `"encrypted files are locked; use :unlock"`. That
is Baryon command vocabulary describing a command Polariton does not have.
Polariton maps the locked condition to its own wording; the raw string never
reaches a Polariton dialog.

### Acceptance gates

- A session that never accesses the saved-profile store is never prompted.
- Unlocking the profile session leaves the document session locked, and the
  reverse, each asserted directly.
- Cancel and wrong-passphrase both leave the application usable, with the store
  intact and the scratch target not silently substituted.
- A refused unlock disables the saved-profile surface only; ad hoc configuration
  and connection remain fully available.
- Opening the Connect menu with a locked store prompts once, and cancelling
  leaves the menu usable with no profile list rather than an error state.
- Creating a store double-enters the passphrase; a mismatch refuses without
  writing a file.
- No Baryon command vocabulary appears in any Polariton-facing message.
- The passphrase is `Zeroizing` end to end and is never copied into a plain
  `String`.

Commit: `impl: adr 0134 add lazy unlock and refusal behavior`.

## Stage 4 — connect the saved profile surface

The Connect menu gains save, list, select, edit, rename, and delete over stored
profiles, alongside the existing `connect:new`. Selecting a profile drives the
stage 3 unlock, then `from_saved`, then the ordinary connect path.

The dialog's existing required Name field becomes the profile identity rather
than a second parallel label.

### Acceptance gates

- A profile saved in one process is selectable and connects in the next.
- `from_saved`'s `#[allow(dead_code)]` is removed and it has a production caller.
- Deleting a profile removes it from the store and from the menu.
- `connect:new` still produces an unsaved, session-scoped connection.
- Reported identity in the status bar is the profile name.

Commit: `impl: adr 0134 connect the saved profile surface`.

## Cross-stage guards

**No credential leaves the secure boundary.** A value from a field declared
`is_secure` must never appear in a status message, a `StatusDetail` diagnostic,
a log line, a panic message, a copied selection, an exported artifact, or a
durable session snapshot. The last one is the easy miss: `session_persistence`
checkpoints on close, and a credential captured there would be written outside
the encrypted store entirely.

**Nothing lands in `init.rn`.** It is plaintext, user-authored, and rewritten by
`:set`.

**No engine or adapter branch in store logic.** The store holds adapter types
and field maps; it does not know what any adapter does with them.

**The ad hoc path stays whole.** Every stage re-runs the stage 0b baselines. A
decomposed field surface that quietly breaks typed connection strings has traded
one adoption blocker for another, and the four uncredentialed adapters must stay
byte-identical throughout.

## Evidence and review checklist

- [ ] Stage 0 census cites file and line for every boundary claim.
- [ ] Per-adapter stage 0b baselines pass unchanged at every stage.
- [ ] Store creation double-enters the passphrase; mismatch writes no file.
- [ ] Locked store leaves ad hoc configuration and connection fully usable.
- [ ] Postgres and MySQL dialogs show five fields, password masked.
- [ ] Four uncredentialed adapters byte-identical to stage 0.
- [ ] Store round-trip, all three refusal cases, and `prod`/`Prod` coexistence.
- [ ] Failed open leaves the store file untouched.
- [ ] Profile and document secret sessions proven independent.
- [ ] No prompt in a session that opens no saved profile.
- [ ] No Baryon vocabulary in Polariton messages.
- [ ] No credential in status, diagnostics, logs, exports, or session snapshots.
- [ ] `secrets.rs` and the durable-session schema unchanged.
- [ ] Full suite and architecture tests green; counts reported as passed and
      ignored separately, not summed.
