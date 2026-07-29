# Reclamation stage 4: refuse allocation at registry capacity

Parent: `../adr/0001-ecs-memory-reclamation.md` stage 4. Scope: capacity
exhaustion becomes refusal with a clear message, never a panic, and never a
half-mutated tree. **No reclamation change, no id/page work, no capacity
increase.** Small stage by design — it must land before stage 5 so
reclamation bugs there degrade instead of crash. Deliverable is one `impl:`
commit reviewed against this plan.

## Honesty note

Stage 4 makes exhaustion *survivable*, not avoidable: stage 3 reclaims bytes,
not ids; `next_id` still only grows. The refusal message should say what the
user can actually do (close buffers frees nothing yet; restarting does;
capacity is a startup constant) without pretending otherwise. Stage 5 is what
makes exhaustion avoidable.

## Current state (verified 2026-07-29)

- `try_alloc_node_internal` (`ecs/registry.rs:303`) already returns `Option`;
  the panic lives only in the `alloc_node_internal` wrapper (`:298`,
  `expect("UastRegistry capacity exceeded during split")`).
- `reserve_ids`/`reserve_chunk` already return `Option`; ingest already maps
  `None` to an error string.
- Production panic reach is narrower than the initial sketch: graft allocation
  (`structural_graft.rs` `allocate_graft_node`), mutation splits
  (`uast/mutation.rs`), and synthetic fold materialization
  (`folding/synthetic.rs`) call the panicking wrapper. Document, output-panel,
  file-browser, load, and generation-swap constructors already use fallible
  `reserve_chunk` paths; stage 4 verifies and normalizes them rather than
  rebuilding them. Test modules may keep explicit fixture `expect`s.

## Policy: reserve first, refuse whole operations

The atomicity rule: **no operation may mutate its first existing node until
every new node it will require is reserved, or it has selected an
allocation-free fallback.** Never use a check-then-reserve sequence: it is a
TOCTOU race against other registry users. Never conservatively reserve ids
that will go unused: `next_id` is the scarce resource this stage protects.

| operation | exact admission | on refusal |
|---|---|---|
| structural graft | provider nodes **plus every synthetic fan-out `Group` node**; promote the existing `graft_allocation_count` fixture helper to checked production code | validate first, then reserve before metric-scan cancellation or detach; record `StructuralIssue::CapacityRefused`; keep the last good topology and applied state |
| physical-leaf insertion/split | one virtual insertion/tombstone plus `ceil(tail_len / DMA_CHUNK_SIZE)` physical-tail nodes, known from the span and split offset | ordinary preflighted edits promote the already-resident leaf and splice in place when the exact chunk is unavailable; a low-level split with no safe resident fallback returns a typed capacity error before generation/metrics/edges move |
| virtual-leaf edits and resident physical deletes | zero | remain editable at full capacity |
| synthetic fold materialization | exact 1–5 nodes from the already-built plan (fold boundary, optional prefix/suffix, optional partial endpoint copies) | keep the existing structural boundary or leave the fold unmaterialized; the global capacity warning explains why |
| load / reload / ingest | exact `node_count` | existing error path; unify the message wording |
| output panel / file browser / unnamed document | exact 2, already fallible | refuse creation with the existing operation-specific message |
| generation-swap fresh ingest | exact count | existing abandoned-candidate path already handles the error cleanly — verify, don't rebuild |

Mechanism:

- Delete the panicking `alloc_node_internal` entirely. Graft recursion and
  synthetic-fold construction consume an exact `RegistryChunk`; assert in
  tests that their count and consumption agree. This also removes per-node CAS
  traffic from grafts.
- Mutation code reserves its exact split chunk before changing content
  generation, metrics, residency class, or edges. The normal document-edit
  preflight already makes physical bytes resident; if exact reservation fails,
  insertion promotes that resident leaf and completes without allocating.
  Fallibility stays at low-level split boundaries used by fixtures and any
  future non-preflighted caller—do not thread a conservative group reservation
  through `document/access.rs`, because unused reserved ids would themselves
  accelerate exhaustion.
- Counters: a refused operation moves **no** stage-1/3 counters and emits no
  retirement—assert this in tests. A successful allocation-free edit moves
  the ordinary authoritative payload/class counters but allocates and retires
  zero nodes. Exact chunks are reserved only after every other fallible
  validation; no reserved-but-abandoned range is permitted.
- The semantic pipeline will re-request after a `CapacityRefused` graft on
  the next state change — that is acceptable (no hot loop: requests are
  state/length-gated), but the refusal must not clear
  `applied_structural_state_id`; the document keeps its last good topology
  and freshness display shows the refusal.

## Surfacing

- `RegistryStatsSnapshot` gains `capacity`. The inspector's global header
  line shows `global-allocated=A/C`, and when monotonic allocation first
  crosses a warning threshold (90%, a named const) the header and one
  session-scoped status message say so—before exhaustion, not only at it.
- Refusal messages are operation-specific ("Edit refused: UAST registry is
  full", "Semantic topology refused: …") and name the registry, not a
  generic OOM.
- The full-line owner-header guard will trip if the global line changes
  shape — update consciously, as established.

## Tests and proof obligations

1. **Panic-free at exhaustion**: with tiny registries, exercise physical and
   virtual typing/paste/delete, low-level split, graft application, fold
   materialization, load, `:uast`/panel creation, and generation ingest at
   exactly-full capacity. Safe fallbacks succeed; unavoidable allocations
   refuse with messages; nothing panics (no `should_panic` for capacity).
2. **Atomicity**: a refused low-level split leaves the tree, counters,
   topology revision, and ledger byte-identical (walk-compare); a refused
   graft leaves the previous topology grafted and
   `applied_structural_state_id` intact. A full-capacity preflighted insertion
   succeeds allocation-free and preserves bytes/undo.
3. **Graft pre-reserve**: capacity check happens before detach — a refused
   graft never orphans the old children (assert no retirement notice).
4. **Warning threshold**: crossing 90% surfaces the warning exactly once per
   session (allocation is monotonic before stage 5); header shows `A/C`.
5. **Editability after refusal**: cursor motion, undo within existing state,
   saving, and closing buffers all still work at full capacity.
6. **Source audit**: extend the established audit family — no production
   `expect`/`unwrap` on `reserve_chunk`/`reserve_ids`/`try_alloc_*` return
   values; the panicking wrapper no longer exists to call.

## Non-goals

- No capacity configuration or growth (the constant stays a constant).
- No emergency reclaim-on-pressure (ids are not reclaimable until stage 5;
  wiring bytes-reclaim to allocation pressure buys nothing and couples
  layers).
- No change to reclaim, parking, or accounting semantics.

## Review checklist

- [ ] Panicking allocator deleted; audit test prevents reintroduction.
- [ ] Every inventory row's fallback/refusal path implemented and tested at
      tiny capacity; zero panics.
- [ ] Refused operations are counter-neutral and retirement-neutral.
- [ ] Graft refusal precedes detach and preserves freshness state.
- [ ] Warning threshold fires once per session; header shows `A/C`.
- [ ] Messages name the registry and the honest remedy.
