# ADR 136: Wendelon-Backed Hard Table-Size Facts (Toron Consumer + Default Flip)

**Status:** Promoted. **Operator-steered ("Codex's idea — data data data").**
Quorum: operator + Claude + Codex on the Wendelon-facts cut; Gemini held Selectivity
Propagation (now the lead ADR 137 candidate — [[feedback_claude_is_promotion_quorum_of_one]],
Gemini divergence is the 2/3 bar). This is the cut that makes the 129–135 CBO spine
**fire on real production data** instead of test-only `seed_optimizer_facts`.

**This council promotes 136-T (the Toron consumer + default flip).** The producer
half is named here as a precise contract and handed to Wendelon's sequence as
**136-W** ([[feedback_adr_placement_interior_exterior]] — exterior/storage-ring work
returns home; [[feedback_cross_ring_admin_orchestration_boundary]] — Wendelon owns
its producer choreography; Toron consumes, holds no Wendelon-owned stats cache).

**Category:** Toron interior — catalog/optimizer fact ingestion.
**Predecessors:** 129 (fact spine + `TableOptimizerFacts`), 130–132 (the rules that
spend facts), 134 (EXPLAIN — the validation surface), 135 (the multi-way star the
inferred plans now execute).
**Migration:** `plans/impl/136_wendelon_backed_table_size_facts.md`.

## Doctrine — make the spine fire on data, the safe way
> ADRs 129–135 built a complete cost-based broadcast optimizer, but in production it
> is blind: `TableBinding::from_descriptor_with_logical_types` ends at
> `optimizer_facts: TableOptimizerFacts::default()` (`catalog.rs:204`), so every
> catalog built from a Wendelon descriptor sees `None`/`None` and hashes. Facts only
> ever arrive via the test-only `seed_optimizer_facts*`. The unlock is to let real
> table-size facts ride alongside descriptor sync and populate the binding — but
> **only as hard upper bounds**, because the 129 contract is asymmetric: an overcount
> merely suppresses broadcast (safe), an **undercount silently broadcasts a table
> that is too big — a correctness bug**. Two things follow: the byte fact must bound
> the *materialized* build footprint (not compressed storage bytes), and anything
> unknown/absent/mismatched must map to `default()` → hash. Producer-known stale
> facts must be omitted or surfaced as unknown; Toron only validates descriptor
> identity and carries `manifest_epoch` as provenance in this cut.

## Decisions

**D1 — Facts are descriptor-adjacent, never descriptor fields.** Do not touch
`TableSchemaDescriptor` — it is schema identity (fingerprinted, equality-checked,
upsert-keyed; `schema/table.rs:1000`). Dynamic table facts ride in a **sidecar**
bundled with descriptor sync, identity-tagged with at least `(tenant_id,
table_object_id, schema_generation, physical_descriptor_fingerprint, manifest_epoch)`.

**D2 — Hard bounds only.** The consumed fact is `{materialized_byte_upper_bound:
Option<ByteCount>, row_upper_bound: Option<RowCount>}` plus the identity tag. Unknown,
absent, soft, or schema-mismatched → `TableOptimizerFacts::default()` → hash.
Producer-known stale facts must be omitted or surfaced as unknown; Toron carries
`manifest_epoch` but does not invent an independent freshness rule in 136-T. Overcount
safe; undercount is a contract violation. **A *known-empty* table is a valid hard
fact distinct from unknown** — represent or normalize it as
`row_upper_bound = Some(0)` **and** `materialized_byte_upper_bound = Some(0)` so the
existing byte-first broadcast chooser infers broadcast below cap. Do not collapse
known-empty into `None`.

**D3 — The byte unit is the *materialized* build footprint (Claude's correction,
load-bearing).** Wendelon's natural number is compressed/encoded storage bytes — a
**lower** bound on decompressed Arrow memory/IPC size; consuming it as the byte bound
would undercount and wrongly broadcast. The consumed field bounds the materialized
build side; 136-W may produce it via an uncompressed/logical-bytes ledger, a
schema-derived `row_upper_bound × max_row_width`, or another conservative proof. Toron
names the field `materialized_byte_upper_bound` so that wiring a storage `size_bytes`
into it is awkward by construction. Toron must **not** map Wendelon
`LsmRunSummary.size_bytes` (`lsm.rs:62`, run-scoped) or `estimated_size_bytes`
(`lsm.rs:252`, a *soft estimate*) into the byte bound.

**D4 — Consumer lands first, safe-by-construction.** Add facts-aware ingestion
(constructors + registry/bootstrap/sync paths) that builds `TableBinding` with
`TableOptimizerFacts`; descriptor-only constructors keep defaulting to unknown. With
no 136-W producer yet, facts are absent → the flipped default is **inert** (do-no-harm
hash). The instant Wendelon ships facts, inference fires with zero further Toron
change. 136-T is fully testable now against synthetic sidecar fixtures.

**D5 — Dynamic facts refresh the binding even when descriptor identity is unchanged
(Codex's correction).** A tiny dimension grows without any schema change. Descriptor
sync today returns the existing binding when `same_descriptor_identity(...)` holds;
the facts-aware sync path must **update `TableBinding.optimizer_facts` on a sidecar
change even if the descriptor fingerprint is unchanged** — facts track `manifest_epoch`,
not schema generation. No silent fact staleness from same-descriptor refreshes.

**D6 — Flip the default deliberately.** Replace derived `PlannerConfig::default()`
with an explicit impl setting `auto_broadcast_max_build_bytes = Some(1 MiB)`, still
clamped by `default_auto_broadcast_cap_ceiling()` (`binder.rs:4010`, 16 MiB of the
64 MiB build budget). Missing facts still hash through the existing
`MissingBuildByteUpperBound` path, so the flip is safe before 136-W. Land the flip
together with the missing-facts→hash gate so the do-no-harm surface is pinned in one
cut.

**D7 — No separate stats RPC, no Toron-side stats lifecycle.** Facts travel with
descriptor bootstrap/sync; Toron stores them in `TableBinding` and consumes once. No
Wendelon-owned stats cache or independent refresh loop on the Toron side
([[feedback_cross_ring_admin_orchestration_boundary]]).

## 136-W contract (handed to Wendelon's sequence — not built here)
A table-identity-keyed hard-facts producer: per-table upper-bound ledger keyed by the
D1 tuple; values are hard upper bounds or unknown, monotone (count admitted
rows/bytes; do not decrement on delete/compaction this cut — churned tables exceed the
cap and fall back to hash); the byte bound is a **materialized** upper bound (D3);
surfaced through catalog-sync alongside the descriptor without mutating it. Grounded
constraint: `LsmRunSummary` is run-scoped (no `tenant_id`/`table_object_id`), so 136-W
must define a table-scoped sidecar, not sum manifest runs.

## Behavior gates
- `star_query_infers_broadcast_without_seed_optimizer_facts` — SQL **and** Rune
  star-schema queries over Wendelon-style descriptor + synthetic sidecar fixtures emit
  `broadcast_inferred` EXPLAIN rows and fragment via 135's one-scan-share star shape,
  with **no `seed_optimizer_facts`**. Headline; fails on `main` (`from_descriptor`
  defaults unknown).
- `descriptor_sidecar_facts_populate_table_optimizer_facts` — descriptor + sidecar →
  binding carries materialized byte + row bounds.
- `legacy_descriptor_paths_still_default_unknown` — descriptor-only constructors/sync
  produce unknown facts (129 dark-launch discipline intact for seeded paths).
- `planner_default_on_missing_facts_still_hashes` — `PlannerConfig::default()` has
  inference enabled, but absent facts → `hash_by_default`.
- `facts_refresh_updates_binding_without_descriptor_identity_change` (D5).
- `mismatched_facts_default_to_unknown` — schema-gen / fingerprint mismatch →
  unknown; `manifest_epoch` carried as provenance. Producer-stale facts arrive as
  absent/unknown in this Toron cut.
- `known_empty_table_infers_broadcast` — known-empty facts normalize to both
  `row_upper_bound = 0` and `materialized_byte_upper_bound = 0`, broadcasting below
  cap rather than hashing; distinct from unknown.
- `materialized_byte_bound_not_storage_size` — the contract accepts a materialized
  bound, not compressed `size_bytes`; a storage-bytes fixture is rejected.
- Regressions: all 129–135 optimizer / EXPLAIN / broadcast-star gates green.

## Architecture guards
- `adr136_table_schema_descriptor_not_modified` (D1).
- `adr136_facts_are_descriptor_adjacent_sidecar` (D1).
- `adr136_table_binding_constructible_with_optimizer_facts` (D4).
- `adr136_descriptor_refresh_updates_optimizer_facts` (D5).
- `adr136_planner_default_enables_auto_broadcast` (D6).
- `adr136_missing_facts_still_hash` (D2/D6 do-no-harm).
- `adr136_byte_upper_bound_is_materialized` (D3 — name + no storage-byte mapping).
- `adr136_no_wendelon_producer_in_toron` (D7 — producer is 136-W's).

## Non-goals
The 136-W Wendelon producer (own ADR); selectivity propagation through filters
(Gemini's cut → ADR 137); remote-live multi-producer fan-in; row-local fact
preservation beyond known-empty; richer cost units; join reordering. No
`TableSchemaDescriptor` / Wendelon wire / substrate / scheduler change in 136-T.

## Council history
iter-0 three-way: Codex (Wendelon facts), Gemini (selectivity), Claude (remote-live
fan-in). Claude's iter-1 grounded that the facts-flip is cross-ring/producer-blocked
(descriptor carries no stats) and that pinned-Rune broadcast already reaches the
sealed path; surfaced the fire-vs-fast priority fork. Operator steered to Codex's cut
("data data data"). iter-2 convergence: Claude set the cross-ring pair + the
materialized byte-unit safety correction (D3) + safe-by-construction ordering (D4);
Codex folded both and added dynamic refresh (D5), the explicit flip (D6), and the
no-RPC boundary (D7); Gemini held selectivity, sequenced to 137. Grounding caught a
degraded-channel phantom (a non-existent `_with_facts` seam) before it entered the
premise.

## Follow-ons
136-W (Wendelon producer); ADR 137 selectivity propagation through filters (Gemini —
row-local fact survival; PK-equality → `row_upper_bound` = 1, plus a separately
proved hard byte cap before broadcast can use it); remote-live
multi-producer fan-in (complete 135's live path); richer cost units; join reordering.

## Promotion criteria
A SQL and a Rune star query over Wendelon-style descriptor + synthetic sidecar facts
infer broadcast and fragment via 135's star shape with no `seed_optimizer_facts`
(fails on `main`); missing/mismatched facts hash; known-empty broadcasts;
`PlannerConfig::default()` enables inference yet hashes without facts; descriptor
refresh updates facts without identity change; `TableSchemaDescriptor` unchanged; the
byte bound is materialized (storage-bytes fixture rejected); the eight `adr136_*`
guards + 129–135 guards green; `cargo test -p toron`, `--test architecture_test --
adr136`, `cargo check -p toron --all-targets`, `git diff --check` all green.
