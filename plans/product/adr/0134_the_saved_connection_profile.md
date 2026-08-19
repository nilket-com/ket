# ADR 0134: the saved connection profile

Status: accepted 2026-08-19. This record decides how Polariton stores a
connection a user wants back tomorrow, and how the credential inside it is kept.

Amended 2026-08-19, before implementation began: §8's unlock trigger moved from
first connect to the first operation requiring access to the profile store,
since listing
and editing profiles also need the key; §8 adds the distinct first-write
passphrase-creation flow; §9 states that a locked store disables only the
saved-profile surface. No other section changed.

It decides three separated concepts — the ad hoc connection string, the saved
profile, and the secure field — the decomposed adapter field surface that makes
the third expressible, the profile's field-map shape and its encrypted store, the
unlock session and its timing, and the behavior when unlock is refused. It does
not decide connection testing, credential discovery from external agents,
per-profile passphrases, passphrase rotation, or the full vendor DSN surface of
any adapter. It takes no position on the catalog browser, which
`plans/product/review/deferred_triage.md` § Ratification — 2026-08-19 drops.

## Context

Polariton cannot remember a connection. Every launch, the user retypes a
connection string — credentials included — into the Configure Adapter dialog.
`AdapterState { name, fields, configuration, connection }` is process-local and
dropped on exit. The Connect menu has exactly one item, `connect:new`. There is
no connection list, no edit, no delete, and no reconnect.

Three pieces of the eventual answer already exist, and each is misshapen for the
job in a way worth naming before building on it.

**`AdapterConfiguration::from_saved` is a sketch, not a finished half.** It sits
at `crates/polariton/src/adapters/driver.rs:148` marked `#[allow(dead_code)]`
with the comment "The encrypted-profile reader is the next consumer." Its
signature is `(adapter_type: &str, config_value: &str)` — one opaque string per
adapter. That is expressible only because every adapter today happens to carry
exactly one field. It cannot represent a profile in which one field is secret and
another is not, which is the whole purpose of the store.

**`is_secure` has never been set true, and under the current field shape it
cannot usefully be.** `AdapterField` carries `is_secure: bool` and
`adapter_configuration_view` has a masking branch at
`crates/polariton/src/gui/components.rs:967` that no adapter reaches. The reason
is structural: the credential is *inside* the connection string.
`postgresql://user:password@host:5432/db` is one field whose secret and
non-secret parts cannot be separated. Marking it secure hides the host and
database the user is trying to read; marking it plain leaves the password on
screen and in any file that stores it. The flag is not unused because nobody got
to it. It is unusable because the field surface makes it meaningless.

**The cryptography is finished and wired to the wrong consumer.**
`crates/libaryon/src/secrets.rs` ships `SecretSession` with Argon2id key
derivation and XChaCha20-Poly1305 sealing over a `BARYENC\0` file format, with
`Zeroizing` passphrase handling, per-file salts read back by `file_salt`, and
atomic private-permission writes. Its only consumer is
`libaryon/src/engine/session.rs` for `:eenc` / `:wenc` / `:unlock` on encrypted
*text documents*. No connection profile touches it.

The configuration surface is also already spoken for.
`~/.config/polariton/init.rn` (`configuration.rs`, `default_init_path`) is
user-authored Rune, is plaintext, and is rewritten in place by persistent `:set`
commands. It is not a candidate for credential storage.

The product consequence is the gap between a demo and a tool. Everything else in
the adapter layer works — six backends, compound statement planning, an Arrow
seam, and as of this week TLS to managed Postgres. The reason nobody keeps
Polariton open is that connecting to their database is a retyping exercise every
morning.

## Decision

### 1. Three concepts, separated and separately named

- An **ad hoc connection string** is a one-shot, session-scoped value typed into
  the dialog. It remains fully supported and is not deprecated by this record.
- A **saved profile** is a named, durable connection described by
  adapter-specific **named fields**. It is never a connection string.
- A **secure field** is a named field whose value is masked in the interface and
  encrypted inside the profile store.

These are distinct kinds, not modes of one kind. A profile is not a parsed
connection string, and a connection string is not an unnamed profile. Nothing in
this record converts between them.

### 2. The adapter field surface decomposes, bounded to connection and authentication

Each adapter declares named fields for what it needs to *reach* a source and to
*authenticate* to it. Nothing else. Vendor DSN options, tuning parameters, and
driver flags are out of scope and remain reachable through the ad hoc connection
string.

| adapter | fields | secure |
|---|---|---|
| Postgres | host, port, database, user, password | password |
| MySQL | host, port, database, user, password | password |
| SQLite | path | — |
| Parquet | path | — |
| BigQuery | project_id | — |
| Toron | endpoint | — |

**Decomposition is not uniform, and that is deliberate.** SQLite and Parquet
address files. BigQuery authenticates ambiently through application default
credentials. Toron authenticates through the local deployment's mTLS. Those four
adapters have no credential to separate, so they keep a single field and gain
nothing from being split. Only Postgres and MySQL carry a secret, and only they
decompose.

`AdapterFieldType` remains `Text`. A port is a text field that refuses
non-numeric input at connect time; introducing a typed numeric field is a
dialog-surface decision this record does not need.

### 3. A profile is a field map, and `from_saved` consumes the shape the editor produces

A stored profile is an adapter type plus a map from field key to field value —
the same shape `AdapterState.fields` already uses and the same shape the profile
editor writes.

`AdapterConfiguration::from_saved` changes signature to consume that map. Its
current single-string form is replaced, not supplemented. It has no production
callers, so this costs nothing.

**There is exactly one path from stored profile to live configuration.** The
editor produces a field map; the store holds a field map; `from_saved` consumes a
field map. No adapter-specific string assembly happens at more than one point,
and no second reader exists to drift from the first.

### 4. A profile is identified by a user-chosen name, unique in the store

The encrypted payload is a map **keyed by a profile name the user chooses**. Its
value is an adapter type and the field map of §3.

The name is the profile's identity. It is what a connection list displays, what
a reconnect selects, what a delete removes, and what
`AdapterState::connection_identity` (`driver.rs:127`) already reports in the
status bar — the dialog's existing required Name field becomes this name rather
than a second, parallel label.

Names are compared **exactly**, after trimming leading and trailing whitespace,
which is the trim `connection_identity` already applies at `driver.rs:131`. An
empty or all-whitespace name refuses. No case folding, Unicode normalization, or
similarity rule is applied: `prod` and `Prod` are two profiles. That is a chosen
consequence rather than an oversight — near-collision detection requires a
normalization policy, and settling one here would decide more than this record
should.

Collision behavior follows from the operation, not from a prompt:

- **Creating** a profile under an existing name **refuses**. A create never
  silently replaces a stored credential.
- **Editing** a stored profile and saving **replaces it in place**, because the
  identity being edited is the identity being written.
- **Renaming** onto an existing name **refuses**, for the same reason a create
  does.

### 5. Secure fields are masked in the interface and encrypted in the store

A field declared `is_secure` is rendered through the existing masking branch and
is encrypted inside the profile store. A field not so declared is rendered
plainly and stored plainly *within the same encrypted file*.

The distinction is about **display and blast radius**, not about two encryption
tiers. The whole profile store is one sealed `BARYENC\0` file; `is_secure`
additionally guarantees that a value is never shown on screen and never appears
in a status message, diagnostic, log line, or copied text.

### 6. The store is one encrypted file in the configuration directory

Profiles live in a single `BARYENC\0` file beside `init.rn`, resolved by the
existing `config_dir` rule (`configuration.rs:290`) that already handles XDG,
macOS, and Windows.

It reuses `secrets.rs` unchanged: `seal`, `open`, `write`, and `file_salt`, with
`write_private_state_atomically` for the write. **The product gains no second
encrypted-file format.** A format that already survives review for encrypted
documents is the format for encrypted profiles.

Profiles are never written to `init.rn`. That file is plaintext, user-authored,
and rewritten by `:set`; a credential placed there would be disclosed by a
feature working exactly as designed.

### 7. Profiles hold a separate `SecretSession` from encrypted documents

Polariton's profile store owns its own `SecretSession` instance. It is never the
instance libaryon uses for `:eenc` / `:wenc` / `:unlock` text documents.

Sharing the session would mean that unlocking a text document to read it also
unlocks every stored database credential — a privilege escalation the user never
requested and would have no reason to expect. The *primitive* is shared; the
*session* is not. Locking one does not lock the other.

### 8. Unlock is lazy, at the first operation requiring access to the profile store

The passphrase is requested at the **first operation requiring access to the
profile store** — listing, selecting, saving, editing, renaming, or deleting a
profile, whether that access decrypts an existing file or creates a new one. It
is never requested at startup.

Connecting is not the trigger, because it is not the first operation that needs
the key. A user opening the Connect menu to *see* their profiles has already
required store access, and gating on connect alone would leave the list unable
to render.

Polariton opens for editing, Rune authoring, Parquet scanning, ad hoc
connections, and the scratch SQLite target, none of which touch the store. A
startup prompt would tax every session for a feature some sessions never use. A
user who never opens the profile store is never asked for a passphrase.

**Listing requires the passphrase.** This follows from §6's single sealed file:
profile names live inside the ciphertext, so no list can be shown before unlock.
That is accepted rather than worked around. A plaintext index beside the sealed
values would disclose names — which routinely carry hostnames and environment
labels — and would introduce the second on-disk format §6 refuses.

**First write creates the passphrase rather than entering one.** When no store
file exists, the first save establishes the passphrase, confirmed twice, and
seals a new file. This is a distinct flow from unlocking an existing store and
must not be collapsed into it: a mistyped passphrase on creation would seal a
store the user can never open.

Once unlocked, the session stays unlocked for the process lifetime and serves
every subsequent operation without re-prompting.

### 9. Refused unlock leaves the application fully usable

Cancelling the prompt, or entering a wrong passphrase, leaves Polariton running
with no adapter connected. It does not wedge a modal, does not exit, and does not
fall back to a different connection.

**The configuration dialog remains usable while the store stays locked.** A
refused unlock disables the saved-profile surface only. Ad hoc configuration and
connection remain fully available, so a user who cannot recall the passphrase can
still work.

The failure is reported through the ordinary status surface. A wrong passphrase
is reported as a wrong passphrase and the user may retry; the store is not
cleared, rewritten, or migrated on a failed open.

`SecretSession::require_passphrase` currently returns the message
`"encrypted files are locked; use :unlock"`. That is Baryon command vocabulary
and it is wrong in a Polariton dialog. Polariton maps the locked condition to its
own wording rather than surfacing an editor command that does not exist here.

### 10. No credential precedence rules, because no second credential path exists

There is no password-injection path that overlays a secret onto an ad hoc
connection string. A connection either comes from a profile's field map or from a
typed connection string, and never from both.

This is the point of §1's separation. A "connection string plus optional
password" design would immediately require rules for which credential wins when
both are present, what happens when they disagree, and whether an empty override
means "no password" or "use the embedded one." Those rules would be invented to
serve a convenience nobody asked for.

## Rejected

- **Prompting to overwrite on a name collision.** Makes destroying a stored
  credential a one-keystroke confirmation on the most routine path in the
  editor; the operation already carries the intent, so create refuses and edit
  replaces.
- **Case-insensitive or normalized name matching.** Requires a folding and
  normalization policy — case, Unicode form, internal whitespace — that no user
  has asked for and that would silently merge two profiles a user meant to keep
  apart.
- **A single opaque `config_value` per adapter.** Cannot express a profile in
  which one field is secret and another is not, which is the store's purpose. It
  is the existing dead-code signature and it is replaced rather than kept.
- **Marking the whole connection string secure.** Masks the host and database the
  user is trying to verify, in order to hide a password embedded in the middle of
  them.
- **A connection string plus an injected password field.** Creates two credential
  paths and demands precedence, disagreement, and empty-override rules to
  arbitrate between them.
- **Sharing libaryon's document `SecretSession`.** Makes reading an encrypted
  note unlock every database credential.
- **Unlocking at startup.** Charges every session for a feature some sessions
  never use.
- **Storing profiles in `init.rn`.** Plaintext, user-authored, and rewritten by
  `:set`; disclosure would follow from correct behavior.
- **A second encrypted-file format.** `BARYENC\0` already carries per-file salts,
  authenticated encryption, and atomic private writes.
- **Modeling the full vendor DSN surface now.** Unbounded per adapter, and the
  ad hoc connection string already covers the long tail.
- **Deprecating ad hoc connection strings.** They are the fastest path for a
  one-off query and a correctness oracle against the decomposed fields.
- **Decomposing adapters that carry no credential.** SQLite, Parquet, BigQuery,
  and Toron would gain fields without gaining meaning.

## Consequences and sequencing

**The dead code is replaced, not adopted.** `from_saved`'s signature changes and
its `#[allow(dead_code)]` is removed when the reader lands. Its existing unit
tests are rewritten against the field map. Because it has no production callers,
this is free — but the record should not be read as "wiring up existing work."

**Postgres and MySQL acquire real connection dialogs.** Five fields each, with
the last masked, replacing one long URI. This is the visible product change and
the bulk of the interface work. The other four adapters are untouched.

**`is_secure` becomes reachable.** The masking branch at `components.rs:967` gets
its first consumer since it was written.

**A custom-CA field now has somewhere to live.** PRODUCT-0134 does not add one,
but the WebPKI-roots limitation recorded on the Postgres adapter — Mozilla roots
only, no private CA — becomes expressible as a named field once the surface is
decomposed, rather than requiring a connection-string parameter.

**Sequencing.** This record is the second of four selected arcs in
`plans/product/review/deferred_triage.md` § Ratification — 2026-08-19, after the
ratification itself and before query cancellation and CSV. No interaction work
(PRODUCT-0133 stages 3–4) and no typography work displaces it.

**Deferred by design, each requiring its own record:** connection testing before
save; per-profile passphrases; passphrase rotation and re-keying; credential
discovery from system keychains or cloud credential agents; import of existing
DSNs; a connection list surface richer than select, edit, and delete; and the
full vendor option surface of any adapter.
