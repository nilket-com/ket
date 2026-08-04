# Wendelon ADR 075: Idempotent Atomic Counter Fetch-Add Producer Contract

**Status:** Promoted (cross-ring council, 3-of-3 — Claude, Codex, Gemini). Migration:
`plans/impl/0075_idempotent_counter_fetch_add.md`.
**Seed:** the **D0 prerequisite of Toron ADR 207** (durable optimizer evidence). 207's re-grounding
established that cluster-shared optimizer memory is a *fixed counter matrix*, not a per-node snapshot or
registry, and that the correct primitive for concurrent durable counter mutation is **fetch-and-add (FAA)**,
owned by the storage ring. This ADR is the Wendelon-side producer primitive; **Toron ADR 207 is the
consumer** (the 072/073 precedent — a Wendelon-home producer a later Toron consumer calls).
**Council:** drafted via the cross-ring council; Codex iter-0 set the decision/fixture/guard frame + four
grounding questions; Gemini iter-1 answered them (`u64`-LE storage, exact-response idempotency, `Absent`
semantics, `delta==0` reject); Claude iter-1 grounded the atomicity mechanism (primary ownership + the
per-key version chain) and reconciled the storage-representation fork; Codex iter-2 sharpened the two
load-bearing crux points (the per-key read-modify-write critical section; structural — not caller-discipline
— type safety). Gemini affirmed the reconciliation. Strong convergence: 3-of-3 on the spine and the full
D-set.
**Category:** Wendelon storage-ring substrate ADR — data-ring write primitive. Exterior Substrate → Wendelon
home. Reuses the data-ring transport, the per-key primary-owner routing, the version chain, the `write_id`
idempotency machinery, and the group-commit/finalization durability path; no Control-Ring RPC, no Toron
production change.
**Placement:** number assigned in the Wendelon sequence at promotion (073 precedent).

## Summary

Wendelon exposes opaque KV `write(key, value)` / `read(key)` — last-writer-wins by HLC over a per-key
version chain. A *shared counter* (many writers incrementing one key) cannot be expressed safely on that
surface: a client read-modify-write `put` from two writers both read the same version and one increment is
lost. ADR 075 adds a first-class **idempotent atomic fetch-add**: a `COUNTER_ADD(key, delta, write_id)`
data-ring operation that routes to the key's **primary owner**, applies `value += delta` inside a **per-key
read-modify-write critical section** on that primary, is **idempotent by `write_id`** (a retry applies the
delta exactly once and replays the original `previous`/`current`), is **durable** through the normal
quorum/finalization response path, and is protected from blob/counter cross-writes by a
**structural** type/key-domain boundary. The atomicity and exactly-once semantics live where they belong —
the ring that owns the key — instead of being faked by every caller. **Producer-only**; the end-to-end
cluster-memory claim is Toron ADR 207's.

## Motivation

Toron nodes are assumed unstable on a days horizon, so the optimizer's accrued evidence must outlive the
writer process **and** the writer node, and be readable by any coordinator. The durable shape is a fixed
counter matrix (`optimizer_evidence/verdict/{kind}/{verdict}`), incremented by any coordinator. On
`put`/`get` HLC-LWW this is unbuildable: a single shared key loses concurrent increments (no CAS), and
per-writer keys conflate the ephemeral writer with the durable datum (orphaned keys under churn, and an
enumeration registry that is itself a multi-writer hazard). The missing primitive is **atomic add on a
durable key** — which only the storage ring can provide correctly, since it owns per-key serialization and
durability. FAA, not CAS, is the precise fit for a pure-counter workload: it lets Wendelon own the atomicity
+ retry/idempotency instead of exporting a read-modify-write loop to every caller.

## Decisions (promotion-locked)

**D1 — First-class `COUNTER_ADD` operation, not a `write` variant.** A new data-ring request kind
`COUNTER_ADD_REQ`/`RESP` with fixed-Pod `CounterAddRequestMeta` (`write_id`, `client_hlc`, `expected_epoch`,
`tenant_id`, `proxy_hop`, `durability`, `await_finalization`, `key_identity`, `key_len`, `delta: u64`) →
`CounterAddResponseMeta` (ordinary write coordinates — status, log pos/epoch/HLC, echoed `write_id`,
finalization flag — plus `previous_value` and `current_value`). Routed by the same
`identity_for_key(tenant_id, key)` discipline as point ops. Public facade: `Client::counter_fetch_add(...)`
and `Client::counter_read(...)`. Grow the wire deliberately (`no-premature-backward-compat`); no `V1`, no
project name in the protocol surface.

**D2 — Atomicity is primary-routed AND per-key read-modify-write serialized.** `owner_of_identity` routes a
counter key to one **primary** (ring.rs:199) — the correct single-writer placement. But that, plus the
version-chain insertion-order guarantee (storage/core.rs:502, "different HLCs for the same key cannot
interleave"), is **necessary but not sufficient**: insertion order does not by itself serialize the *unit*
`read latest → checked_add(delta) → append replacement version`. Two concurrent adders could each read the
same `previous` and both append from it, losing an increment. ADR 075 therefore adds a **per-key mutation
critical section** on the primary covering the whole read-compute-append sequence — exposed as a
`StorageEngine::counter_fetch_add` / `SlabManager::counter_fetch_add` atomic update, or a server-side keyed
lock around the existing read+append path. The headline gate must fail if two concurrent adders can both
observe the same `previous_value`.

**D3 — Existing durability/finalization, no side channel.** Once the primary accepts the mutation under D2,
it flows through the normal data-ring durability, durable-quorum, `expected_epoch`, lease, pending-
finalization, and duplicate-replay machinery — same as an ordinary `WriteRequest`. Not a metrics counter, an
in-memory accumulator, or a Control-Ring shortcut.

**D4 — Exactly-once idempotency replays the counter result.** A retry with the same canonical `ClientWriteId`
must not add again; it returns a duplicate/replayed response whose `previous_value`/`current_value` equal the
first accepted mutation. The idempotency cached outcome becomes a **typed** mutation outcome —
`CachedMutationOutcome::{ Write(...), CounterAdd { response, trailing } }` — so duplicate-replay never guesses
which response shape a `write_id` produced. (Concurrent *unique* write_ids add once each; a *duplicate*
write_id adds once total.)

**D5 — `u64` counters; deltas fail closed.** Counter state is an unsigned 64-bit value. `delta == 0` is
rejected (a mutation, not a read — explicit point reads observe the value). Overflow (`previous + delta >
u64::MAX`) is rejected **without mutating** — no wrap, no saturate.

**D6 — Absence is absence.** `counter_read` on an unknown key returns `None` — the storage API tells the
truth about existence; it does not fabricate `0`. `counter_fetch_add` on an absent counter bases at `0` and
**creates** it (first `+δ` → `current = δ`). Consumers that want a default (Toron's optimizer evidence) map
`None → 0` outside Wendelon.

**D7 — Type safety is structural, never caller discipline.** A blob `write` must not clobber a counter, and
`COUNTER_ADD` must not reinterpret an existing blob as a counter. Two acceptable shapes:
  - **Preferred:** storage records carry a durable `Blob | Counter` kind. The counter payload is still the
    little-endian `u64` (so `counter_read` stays a cheap point read), but **cross-kind writes fail closed**.
  - **Fallback (no record kind):** Wendelon derives a **domain-separated** physical key for counters
    (`counter-domain ‖ caller-key`), **server-verifying** the derived `key_identity` (not trusting the caller
    to pre-prefix), and rejects ordinary blob writes into the reserved counter domain. The caller key stays
    generic; the physical collision with blob keys is made unrepresentable.
  An untyped 8-byte value guarded only by "callers must not blob-write this key" is **not acceptable** — it
  recreates the cross-writer discipline problem this ADR exists to remove.

**D8 — Point-addressed reads, no registry.** Counter reads are point reads of a known counter key. No prefix
scan, registry, per-node key set, or table-shaped enumeration. The motivating consumer's `(kind, verdict)`
matrix is a fixed enumerable key set it reads directly.

**D9 — Producer-only.** Ships the Wendelon primitive + client facade + tests. **No Toron production change.**
Toron ADR 207 stays stashed until this contract is fixed, then resumes as fixed-key `fetch_add(+1)` + point
reads.

## Architecture Guards (promotion-locked)

- `adr075_counter_add_is_first_class_operation_not_blob_write`
- `adr075_counter_add_has_per_key_read_modify_write_serialization`
- `adr075_counter_add_reuses_client_write_id_idempotency`
- `adr075_duplicate_counter_add_replays_without_mutating`
- `adr075_counter_add_goes_through_finalization_not_metric_side_channel`
- `adr075_counter_blob_type_safety_is_structural`
- `adr075_no_prefix_scan_registry_or_node_keys`
- `adr075_existing_blob_point_write_stays_last_writer_wins`
- `adr075_counter_surface_is_brand_clean_no_v1_suffix`
- `adr075_no_toron_production_change`

## Integration Fixtures (required)

- **Headline — `counter_fetch_add_concurrent_unique_writers_sum_exactly`:** N concurrent tasks
  `counter_fetch_add(key, 1)` with unique write_ids → a later `counter_read(key)` = N. Constructed so a
  read-compute-append without per-key serialization (D2) loses increments and fails.
- **`counter_fetch_add_two_racing_adders_do_not_share_previous_value`:** two concurrent adders to an absent
  key observe `(0→1)` and `(1→2)` in some order, **never both `(0→1)`** (the D2 critical-section proof).
- **`counter_fetch_add_retry_is_idempotent_and_replays_values`:** the same write_id retried returns
  duplicate/replayed status, identical `previous`/`current`, and the counter increases once (D4).
- **`counter_fetch_add_overflow_rejects_without_mutating`; `counter_fetch_add_zero_delta_rejects`** (D5).
- **`counter_read_unknown_key_returns_absent`** (D6).
- **`blob_write_to_counter_key_is_rejected`; `counter_add_to_blob_key_is_rejected`** (D7, structural).
- **`counter_fetch_add_survives_leader_change_or_restart`:** after finalization, a new primary / restarted
  node reads the durable sum and a duplicate replays the original values (D3/D4).
- **`counter_fetch_add_failed_quorum_does_not_poison_later_previous_value`** — *conditional*: required only
  if grounding shows the visibility model can expose a locally-appended-but-unfinalized version to a later
  primary read; if grounding proves that state is already excluded, replace with the source/behavioral proof.

## Out of Scope

General `CAS` / conditional put (the next storage-ring decision, for non-counter read-modify-write); signed
counters / decrement / compare-and-add; reset / delete; multi-key atomic transactions; prefix scan /
registry; SQL/Rune exposure; Toron ADR 207 implementation (resumes after this contract is fixed).

## Council History

Cross-ring council, seeded by Toron ADR 207's D0 prerequisite. The convergence arc: 207 framed durability
first as a Toron-local snapshot, then cluster-visible via per-writer keys; the operator's "nodes unstable on
a days horizon" + "per-node keys defeat the purpose" reframed the durable object as a **cluster counter
set**, and the council landed on **idempotent FAA owned by Wendelon** (CAS deferred). Codex iter-0 framed the
D-set/fixtures/guards + four grounding questions; Gemini iter-1 answered them; Claude iter-1 grounded the
atomicity mechanism and reconciled the storage fork; Codex iter-2 sharpened the per-key RMW critical section
and structural type safety; Gemini affirmed. 3-of-3 on the spine and the full D-set.

## Conclusion

ADR 075 ships FAA as a real storage-ring primitive — primary-routed, **per-key read-modify-write
serialized**, idempotent under `ClientWriteId`, durable through the normal write/finalization path, and
protected from blob/counter cross-writes by a **structural** type or key-domain boundary. It gives Toron ADR
207 the primitive it actually needs — durable cluster-shared counters that survive node loss — without local
files, per-node keys, registries, snapshots, or last-writer-wins lost updates.
