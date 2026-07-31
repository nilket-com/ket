# Toron first-contact findings

Status: implementation contact completed 2026-07-31; **Joel's experiential
verdict remains pending.** Parent: `toron-first-contact.md`. This is evidence
from building and exercising the deliberately temporary Polariton adapter, not
a claim that Toron has passed ADR 0005's promotion gates.

## Startup friction

The Toron and Wendelon example configurations each started quickly and exposed
their advertised listeners. They did not, together, form a queryable local
deployment: the non-TLS Toron storage-client branch constructs an empty
transport-provider map and a static topology whose leader is `None`. A
storage-backed query therefore cannot discover Wendelon's Control Ring leader.
This is a real first-contact deployment gap, not an adapter defect.

Toron also deliberately rejects `SELECT` without `FROM`, so a constant-query
smoke test cannot bypass storage. The automated acceptance consequently used
Toron's public sanctioned Wendelon harness constructor: a real Wendelon engine
with installed schema and rows, a real Toron daemon, and Toron's production
loopback QUIC SQL listener at `127.0.0.1:7742`. The mockable part was the
internal Toron-to-Wendelon transport, not the SQL endpoint exercised by
Polariton.

The product-facing instruction remains honest: Polariton connects to an
already-running Toron deployment; it does not manage either daemon in this
pair. Before Toron can feel local-first, Nilket needs a documented one-command
development deployment whose default configuration can execute a stored
query.

## Protocol and client gaps

Toron's public crate exposes the wire types, but its QUIC connection, framing,
result collection, and cancellation client live privately in `toron::cli`.
Polariton therefore carries a feature-gated thin client that mirrors the
postcard wire and proves compatibility against a live Toron endpoint. This is
acceptable for contact, but duplicated protocol enums are a migration hazard.
A reusable Toron SQL client crate/API is the obvious Nilket follow-up.

The SQL protocol also lacks a side-effect-free health/ping exchange. Polariton
can verify the QUIC handshake when a profile is connected, but a connection
closed without opening a SQL stream is warning-shaped in Toron's logs. A public
client should own a first-class health probe rather than make every consumer
choose between a noisy handshake and a fake query.

The first-contact client is intentionally loopback-only. It refuses remote
addresses with an explicit message rather than pretending to support Toron's
mTLS deployment surface.

## Cancellation behavior

Toron's wire cancellation is real: the live test sends a separate `Cancel`
frame and Toron answers with its typed `Cancelled` acknowledgement. The
current Polariton `DatabaseAdapter::dispatch(&mut self) -> ExecutionResult`
surface cannot expose a query id, cancellation handle, streamed progress, or
an interrupt method. A genuine UI cancellation therefore remains **absent**;
only the client protocol was exercised. Widening that closed pre-seam trait in
this pair would have hidden rather than learned the capability requirement.

Query execution already runs off the GUI interaction path, so the missing
interrupt does not require blocking the UI, but a mistaken query continues in
the backend until completion or Toron's statement timeout.

## Result-shape pain

Toron returns independently decodable Arrow IPC streams. Polars 0.53's
`IpcStreamReader` consumes them without an Apache Arrow dependency in
Polariton, and compatible batches stack cleanly. All conversion is quarantined
in `adapters/toron/polars_compat.rs`.

The pain is materialization, not schema conversion. The legacy adapter result
requires one complete `DataFrame`; the thin client therefore retains and
decodes the whole result behind an explicit 128 MiB ceiling. This is adequate
for first contact and wrong for the eventual analytics seam.

## Capability needs for ADR 0006 and the Arrow seam

The shared analytics seam must express at least:

- an Arrow-native record-batch stream rather than a completed frame;
- a stable execution handle/query id with genuine cancellation;
- boundedness/backpressure and an honest partial-result state;
- statement timeout and remote error codes as structured capabilities;
- a side-effect-free connection-health exchange;
- whether mutation, DDL, transactions, and pushdown are supported;
- renderer/plot consumption that can begin before full materialization.

These are capability queries, never adapter-brand checks. Plot configuration
(ADR 0006) should consume a relation/stream through this seam and keep
execution policy separate from plot style.

## Unsupported-SQL experience

Refusal was typed and legible. The adapter preserves Toron's error code and
message (for example `SyntaxError` and `PlanningError`) rather than flattening
them into a generic connection failure. Two immediate product lessons were
visible: malformed SQL reports syntax honestly, and `SELECT` without `FROM`
reports the precise unsupported frontend construct. Polariton still has only a
string error surface, so future UI treatment cannot yet branch on those codes.

## Codex implementation gate

Passed against the live endpoint described above. The query

```sql
SELECT x, y FROM polariton_first_contact ORDER BY x
```

returned four Wendelon-backed rows `(1,1), (2,4), (3,9), (4,16)`. Those bytes
crossed Toron QUIC framing, postcard, Arrow IPC, the quarantined Polars
conversion, and Polariton's existing scatter kernel. The kernel exported a
26,205-byte SVG containing four plotted marks; no Toron-specific plotting path
exists. The ignored live fixture recreates the proof and writes the artifact to
`TORON_FIRST_CONTACT_PLOT` (default: the system temporary directory).

## The headline — Joel alone

**Pending.** After selecting and executing a real Toron query in Polariton,
inspecting its table, plotting it, making a mistake, and encountering the
current cancellation limitation: did you want to do it again?
