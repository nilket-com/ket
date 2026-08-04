# ADR 439 - Primitive Int64 resident spill index on the partitioned streamed scan-build path

## Context

**438 selected exactly this.** The q19 diagnosis named `(D1 join probe/output, probe_lookup_select)` as
the dominant (domain, leaf), 5/5 stable, with the generic `hashbrown HashMap::get` frame as the perf
before-receipt. Phase 0 grounded the cause: the live owner
`try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks` (`streaming.rs:4774`) writes build
pages **unindexed** (`write_batch_chunks_unindexed`) then calls `rebuild_index_from_pages`
(`spill.rs:1739`) **without first calling `use_compact_resident_index`** (`spill.rs:1374`), so its single
non-null-safe `Int64` `p_partkey` key falls to the generic `HashMap<JoinKey, Vec<SpilledBatchRowRef>>`
instead of the primitive index that already exists.

**The machinery is already built; this path just never opts in.**
- `rebuild_index_from_pages` builds the compact index iff enabled (`spill.rs:1747`) and accounts/guards
  its residency in the same loop (`spill.rs:1783-1788`).
- `SpilledBatchIndex::for_key_plan` (`spill.rs:918`) is the shape fence: `null_safe → Hash`,
  `Int32/Int64 → primitive`, `Generic → Hash`.
- `JoinKeyPlan::from_build_schema_and_probe_schema` (`join.rs:1225`) resolves **null-safe joins to
  `Generic`** (`join.rs:1232`) and requires a single build/probe key column; only single-column
  non-null-safe `Int32`/`Int64` reach a primitive plan.
- `SpilledInnerJoinBuild::lookup` (`spill.rs:2071`) is compact-aware.
- The reorient inner path (`streaming.rs:13650`) runs the enable→rebuild→lookup config gate-green, but
  opts in pre-write because it indexes during write; this path writes unindexed, so its opt-in belongs
  **immediately before rebuild**.

## Decision — opt the partitioned build into the primitive resident index (cut B, surgical)

Immediately before each partitioned `SpilledInnerJoinBuild`'s `rebuild_index_from_pages`
(`streaming.rs:~4931`), when the key plan is primitive-eligible, call `use_compact_resident_index()`.
`key_plan` and `null_safe_keys` are in scope (`streaming.rs:4850`).

**Explicit fence:** `!null_safe_keys && matches!(key_plan, JoinKeyPlan::Int32 | JoinKeyPlan::Int64)`.

**Behavior table under B (complete):**
- `Int32`/`Int64` non-null-safe → `compact_index` primitive; `self.index` empty.
- `Generic` (includes every null-safe join, per `join.rs:1232`) → `compact_index == None`; legacy
  `self.index` **untouched**.

B never calls the opt-in outside the eligible shape, so generic and null-safe paths are byte-for-byte
unchanged. There is no "null-safe → Hash" case under B (that is cut A only).

**Fence/enum drift** is mitigated by an **architecture guard that slices the live owner and asserts the
guarded opt-in occurs before its adjacent `rebuild_index_from_pages`** — ordering, not mere co-presence.

Cut **A** (unconditional mirror) remains a deferred cleanup, after independent generic-accounting parity.

## Acceptance

- **Path selects the primitive index.** A test drives the production sequence — unindexed writes →
  `finish` → guarded opt-in → `rebuild_index_from_pages` — over ≥2 partitions for the q19 shape (`part`,
  `p_partkey`, non-null-safe `Int64`) and asserts each `compact_index` is `SpilledBatchIndex::Int64(_)`,
  `self.index` empty.
- **Generic + null-safe untouched (B's promise), tested honestly.** The null-safe test **derives** the
  plan from nullable `Int64` build/probe schemas via `from_build_schema_and_probe_schema(null_safe=true)`,
  asserts it is `Generic`, inserts **actual null keys**, and verifies null-safe lookup parity on the
  legacy `self.index` — it does **not** pair `JoinKeyPlan::Int64` with `null_safe=true` (an incoherent
  combination `join_key_at` never encodes). A `Generic` non-null build likewise asserts `compact_index ==
  None`.
- **Residency preserved.** `resident_index_bytes == compact_index.accounted_bytes()` post-rebuild;
  `guard_resident_state(RebuildIndex)` fires under a tight budget.
- **Architecture guard** binds the guarded opt-in to occur **before** the adjacent rebuild inside
  `try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks` (owner-sliced, order-checked). No
  query number.
- **The wall shrinks — measured, ADR 356 dynamic band (the 437 lesson).** Predeclare before measuring.
  The band is **`max(0.10, 2 × max(0.0556, current_spread))`** computed from the measured sample —
  **0.1112 is only its minimum** (when `current_spread ≤ 0.0556`), not a fixed threshold. Unprofiled ADR
  356-shaped strict sf=10 q19 (1 warmup + 7 measured) must clear the band on median **execute wall** vs.
  the 83.45 s baseline. Carry the **≤0.20 stability gate, one retained retry, never average attempts.**
  Sub-band → honest-red → revise/remove; never a moved band, never a leaf-only claim.
- **Survival rail (operational).** One strict sf=10 q19 **persisted-Parquet pass@compare under 4 GiB /
  no-swap**, `TasksMax=512`, `RuntimeMaxSec=600`, with `target/tpch/join_spill` fresh/empty before **and**
  after, and fingerprint/digest/schema/row/spill invariance. Required because resident representation
  changed; the memory-unbounded rail alone cannot carry survival. No full-board redux.
- **Diagnostic corroboration (not the speed claim), absolute + relative.** Five fresh 438-ledger children
  must show **both** the normalized share **and** the absolute median micros of `probe_lookup_select` /
  D1 fall vs. 438's five-child receipt — a share can fall merely because another leaf grew, so absolute
  time is required. The unprofiled seven-run wall remains the actual gate.
- **Output invariance.** fingerprint `65c3c2c0b0a99063`, sealed Arrow IPC digest, schema, 1 row,
  51,471,552 spill bytes.
- **No engine-behavior change beyond the index representation; no query branding.** `exec.rs` ratchet
  7,499; join root ratchet 3,713.

## Guardrails and risks

- **Reorient precedent de-risks the machinery**; 439 adjusts only opt-in placement (pre-rebuild, because
  writes are unindexed here).
- **Fence/enum drift** — the owner-sliced, order-checked architecture guard is the mitigation.
- **Band honesty** — the dynamic 356 band, not a fixed floor; the 437 rejection is the precedent.

## Forward arc

First q19 mechanism. If the primitive index moves D1 below D2, the next candidate is **residual-program
factoring of the physically triplicated common ship predicates** (438 census: `l_shipmode` 26/61/97,
`l_shipinstruct` 31/66/102). Decimal arithmetic is not justified (D3 was 0.003%).

## Deferred (named)

- **Cut A (unconditional mirror)** after independent generic-accounting parity — standalone `refactor:`.
- **q17 remeasurement** before q17 is diagnosed.
- **Residual-program factoring** — the next q19 candidate if D1 drops below D2.
