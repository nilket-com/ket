# ADR 0005: discover and attach the local Toron workspace

Status: accepted 2026-08-02. This record implements stage 1 of PRODUCT-0002
section 9: discovery and attachment through the versioned runtime and readiness
contracts. It decides identity and observation only. It does not decide who may
create a workspace, and it takes no position on process ownership.

## Context

TORON-532 gave the local profile a durable identity: an advisory ownership lock,
an atomically published `runtime.json` carrying profile UUID, session nonce, PID,
process start token, and endpoints, and a `LOCAL_READY` publication line. Nothing
outside `nilket/crates/nilket/src/local.rs` reads any of it. `LOCAL_READY` has no
consumer in the repository.

Polariton instead carries a private copy of the product's endpoint assumption.
`crates/polariton/src/adapters/toron/mod.rs` hardcodes `127.0.0.1:7742` as both
placeholder and default value, duplicating `local.rs`'s `SQL_ENDPOINT` constant
across two workspaces. The user is then asked to confirm, through the Connect
dialog, a fact the running deployment already published.

The failure behavior is worse than the ceremony. A failed Toron connect prints
the real cause to stderr only; the interface says `"Adapter connection failed."`
Because `AdapterState::sql_connection` returns `self.connection.or(self.scratch)`,
SQL then silently executes against an in-memory SQLite database. A user whose
deployment is merely still booting can receive a working prompt backed by an
empty scratch engine, labeled only by the provenance string `"scratch"`.

Measurement shows the remaining gap is not computational. Attaching to a running
profile costs no measurable time, and cold readiness is roughly 400 ms including
real mTLS, raft recovery, and two process spawns. The local workspace already has
SQLite's activation energy. What it lacks is a contract that lets a second
product observe it.

## Decision

### 1. Nilket owns the runtime contract; a public crate publishes it

The versioned runtime schema, the profile root resolution rule, and every
validation predicate belong to Nilket. They move out of the `nilket` binary into
a small public crate in the Nilket workspace, which both the supervisor and
consumers depend on. The supervisor keeps sole authority to write.

The crate exposes discovery and the manifest type. It does not expose supervision,
configuration generation, PKI, or role spawning. Consumers receive a typed
observation naming the profile root; they never receive the runtime-manifest
path, a file handle, or a raw parsed document.

### 2. Discovery returns one closed, typed state

```text
Absent | Starting | Ready | Unhealthy | Stale | Corrupt | Incompatible | Unverifiable
```

- **Absent** — no profile, or a profile with no ownership claim.
- **Starting** — an ownership claim exists without a published runtime manifest.
- **Ready** — every condition in section 3 holds.
- **Unhealthy** — identity verifies, but the Toron health exchange fails.
- **Stale** — a manifest does not describe the live owner: no held lock, a lock
  held by a different process, a dead PID, or a process start token that does not
  match the recorded one.
- **Corrupt** — a profile artifact is damaged: malformed, internally inconsistent,
  or present as a symlink or non-regular file where the supervisor writes a
  regular one.
- **Incompatible** — an artifact is well formed and declares a schema version this
  consumer does not implement. The state names which artifact.
- **Unverifiable** — the observation itself could not be performed: a permission
  error, an unreadable artifact, or a platform that does not expose the lock
  representation this consumer requires.

Three distinctions here are deliberate and none may be collapsed.

`Corrupt` and `Incompatible` separate damage from version skew, which must remain
diagnosable after the schema advances. `Corrupt` and `Unverifiable` separate a
broken deployment from a broken observer: a consumer that cannot read a profile
has learned nothing about that profile and must not report it as damaged. A file
that cannot be read and a file that parses wrongly are different findings.

Both versioned artifacts participate. The profile marker carries `format_version`
and the runtime manifest carries `version`; each is subject to skew independently.
Every artifact is therefore decoded in two steps — parse the version header, then
decode the body that version defines. A future schema that changes required fields
must report `Incompatible`, never `Corrupt`, because its version is inspected
before its body is interpreted.

`Starting` is required, not incidental. The supervisor claims ownership before it
writes any identity artifact and publishes its manifest only after both roles
reach readiness, so this state covers the entire cold boot — including the first
moments of a first-ever run, when neither marker nor manifest exists yet. A held
ownership claim is therefore evidence that outranks every missing artifact:
absence of identity beneath a live claim is `Starting`, never `Absent` and never
`Corrupt`. Ownership must accordingly be observed before contents are read. The
composed-session record will wait on this same state rather than inventing a
second definition of "not yet".

### 3. Ready requires converging evidence

`Ready` requires all of:

- both artifacts declare schema versions this consumer implements;
- the runtime manifest's profile UUID matches the profile marker's;
- the ownership lock is observably held, **and its holder is the PID the manifest
  records**;
- that PID is live and its process start token matches the manifest;
- the recorded SQL endpoint is a loopback address;
- the Toron health exchange against that endpoint succeeds; and
- the identity is unchanged when re-observed after that exchange.

No single condition is sufficient. A recycled PID cannot establish ownership
without the lock; a held lock cannot establish liveness without health; a healthy
listener on the recorded port cannot establish identity without the profile UUID.
This preserves TORON-532's rule that PID is diagnostic, not authority.

Binding the lock holder to the recorded PID closes a real window. A restarting
profile briefly presents a lock held by its new supervisor beside a manifest
written by the old one. Attaching there would fuse two deployments' identities.
That observation is `Stale`; like `Starting`, its correct consumer response is to
observe again rather than to attach.

Re-observation closes the same window from the other side. Health is a
comparatively slow exchange, and a deployment may restart during it. A result that
fuses one session's manifest with another session's health proves nothing about
either, so identity is confirmed once after health resolves — after **either**
outcome. Confirming only after success would leave the failure path fused: a
restart mid-exchange would surface as `Unhealthy`, blaming a new session for the
silence of the one it replaced. A changed identity is re-classified rather than
reported.

Health is proved through the public `toron-client` health exchange. Discovery
introduces no second health definition and no port-open substitute.

### 4. Discovery is read-only

Discovery does not spawn a process, remove or rewrite a manifest, clean a stale
profile, generate configuration or PKI, send a signal, or acquire any lock. It
holds no lifecycle ownership and takes none by observing.

The prohibition on lock acquisition is a correctness requirement, not caution.
The supervisor's own startup treats a failed lock acquisition as proof that
another instance owns the profile and proceeds to report that instance's identity.
A consumer that acquired the lock even momentarily could therefore convert an
ordinary poll into a spurious startup failure for a concurrently starting
deployment. The lock must be observed, never taken.

A `Stale` observation stays stale. Cleaning it is an ownership act reserved for
whoever may create a workspace.

### 5. Polariton renders first and discovers asynchronously

Startup never blocks on discovery. The interface draws immediately and the
observation arrives as an ordinary asynchronous message. A workspace that is
`Starting`, unreachable, or slow to answer delays no frame.

### 6. A verified Ready workspace attaches without ceremony

On `Ready`, Polariton attaches to the discovered endpoint and reports the
workspace it joined. The Connect dialog does not open. Selecting a source
explicitly remains available and unchanged; discovery removes a required step,
not a capability.

Attachment is not ownership. Polariton joins a deployment it did not start and
does not stop it, consistent with PRODUCT-0002 section 9.

### 7. Failure never impersonates success

No discovery outcome other than `Ready` produces an attached workspace.

`Corrupt`, `Incompatible`, and `Unverifiable` do not fall through to the
previously hardcoded endpoint, to a scan of the canonical port, or to any other
guess. Each is an actionable refusal naming the observed state. `Unverifiable`
additionally names what could not be read, because its remedy is usually the
observer's environment rather than the deployment.

SQLite remains available as an explicitly labeled SQL scratch capability. It may
not stand in for a local workspace: a failed discovery must never present itself
as a successful workspace connection. Rune relations, which have no scratch
equivalent, receive a typed `local workspace unavailable` result carrying the
discovery state, not a generic adapter error.

The interface surfaces the observed state and its cause. `"Adapter connection
failed."` is not an acceptable rendering of eight distinguishable conditions.

### 8. One authority, guarded

The product's canonical Toron endpoint disappears from Polariton. The verified
runtime contract supplies it; a compiled-in copy is not a fallback for an absent
one.

An architecture guard with synthetic positive controls proves that Polariton
contains neither the runtime manifest filename or schema nor the canonical Toron
port, and that it reaches discovery only through the public crate. The guard
exists because private "temporary" discovery logic is exactly how a second
authority is created. Its failure cites this record.

## Rejected

- **Polariton parsing `runtime.json` directly:** creates a second schema
  authority in another workspace and guarantees drift at the next version bump.
- **A single boolean or `Option` result:** collapses damage, version skew, cold
  boot, and honest absence into one indistinguishable failure, which is precisely
  the current defect.
- **Reporting a failed observation as a damaged profile:** a consumer that could
  not read a profile has learned nothing about it, and blaming the deployment for
  the observer's permissions or platform sends every diagnosis in the wrong
  direction.
- **Validating only the runtime manifest's version:** leaves the profile marker's
  `format_version` unchecked, so marker skew arrives disguised as inconsistency.
- **Decoding an artifact before inspecting its version:** makes any future schema
  that changes required fields indistinguishable from damage.
- **Accepting a held lock as ownership without matching its holder:** fuses a
  restarting profile's new supervisor with its predecessor's manifest.
- **Reading identity artifacts before observing ownership:** reports the opening
  moments of a first-ever boot as an absent profile, because the supervisor holds
  the lock before it writes anything to identify itself.
- **Re-confirming identity only when health succeeds:** leaves the failure path
  able to blame a newly started session for its predecessor's silence.
- **Probing the lock by attempting to acquire it:** races the supervisor's own
  acquisition and can turn a background poll into another process's startup
  failure.
- **Treating a held lock without a manifest as corrupt:** misreports every cold
  boot as damage.
- **Health alone, or a port-open check, as proof of readiness:** attaches to
  whatever occupies the port, discarding profile identity.
- **Blocking startup on discovery:** makes an unreachable or slow workspace an
  unresponsive editor.
- **Falling back to the hardcoded endpoint when the manifest is unusable:**
  reintroduces the duplicated assumption at exactly the moment it is known wrong.
- **Silent SQLite substitution on discovery failure:** presents an empty scratch
  engine as the user's workspace, which is a correctness failure, not a
  convenience.
- **Auto-starting a deployment when discovery returns `Absent`:** process
  ownership, teardown, and orphan recovery are a separate decision.
- **Cleaning a stale manifest during discovery:** an ownership act performed by
  an observer.

## Consequences and sequencing

- Reaching a working local workspace loses a required step: a running deployment
  is joined without opening the Connect dialog.
- The duplicated `127.0.0.1:7742` product assumption leaves Polariton, and the
  runtime schema gains its first external consumer and therefore its first real
  version-compatibility obligation.
- Failure becomes legible. Eight states replace one string: a booting deployment
  stops looking identical to a broken one, and a profile the observer cannot read
  stops looking like a profile that is damaged.
- `LOCAL_READY` remains the publication event for launchers. Discovery reads the
  runtime record; the two contracts stay distinct and both stay owned by Nilket.
- Toron earns no PRODUCT-0002 promotion credit here. Attaching to an explicitly
  started deployment is not lazy default availability.
- PRODUCT-0006 inherits exactly one clean question: who may create and own a local
  workspace when discovery returns `Absent`. It may then compose `ket local` on
  top of this contract without redefining readiness.
- Implementation receives its own plan and must land state-classification,
  read-only, race, refusal-polarity, and architecture-guard positive controls
  before the behavior is claimed complete.
