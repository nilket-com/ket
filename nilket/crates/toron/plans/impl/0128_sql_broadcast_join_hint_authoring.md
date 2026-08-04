# Impl Plan — ADR 128: SQL Broadcast Join Hint Authoring

**Migration for** `plans/adr/0128_sql_broadcast_join_hint_authoring.md`.
**Scope:** `crates/toron/src/sql_planner.rs` production + its unit tests;
distributed SQL gates in `crates/toron/src/lib.rs`; guard retarget in
`tests/architecture/rune_frontend.rs`.
**Non-scope:** sqlparser version/dialect, Rune, IR, binder, fragmenter,
scheduler, executor, PBC, Wendelon, live-shuffle.

## Pre-flight anchors (verify before editing)
| Concern | Anchor |
|---|---|
| SQL parser dialect (unchanged) | `sql_planner.rs:1408`, `:1430` (`GenericDialect {}`) |
| GenericDialect already captures hints | `sqlparser-0.62.0/src/dialect/generic.rs:288` (`→ true`); default `false` `dialect/mod.rs:1534`; `parse_select` call `parser/mod.rs:14715`, gate `:14913` |
| `OptimizerHint{prefix,text,style}` (raw `text`) | `sqlparser-0.62.0/src/ast/mod.rs:11983` |
| SELECT hint destructure + blanket reject (replace) | `sql_planner.rs:2534`, `:2560-2561` |
| explicit join loop (thread strategy) | `:2635-2646` |
| EXISTS join stays hash | `:3598-3607` |
| `build_join_plan` + Broadcast arm (INNER reject) | `:4043` / `:4067` (`:4068`) |
| `plan_qualifiers` | `:6114` |
| `TableRef` (match target) | `catalog.rs:29` |
| guard to retarget | `tests/architecture/rune_frontend.rs:1882` |

## Phase 0 — Failing gates first
Author all gates + guards. Verify on `main`:
`generic_dialect_preserves_select_optimizer_hints` **passes** (hints arrive
today), `sql_broadcast_hint_emits_broadcast_join_shape` **fails by rejection**
(the `:2560` blanket reject). This pairing is the honest proof the seam is
"replace the reject," not "enable capture"
([[feedback_cut_must_deliver_the_name]]).

## Phase 1 — Import hint types
Extend the `sqlparser::ast` import with `OptimizerHint, OptimizerHintStyle`. **Do
not** touch the dialect (the `adr128_generic_dialect_remains_the_sql_dialect`
guard backstops this).

## Phase 2 — Strict recognizer
`fn parse_broadcast_join_hint(hints: &[OptimizerHint]) -> Result<Option<TableRef>, FrontendError>`:
empty → `Ok(None)`; `len > 1` → reject; require `hint.prefix.is_empty()` +
`matches!(hint.style, OptimizerHintStyle::MultiLine)`; delegate text to
`parse_broadcast_hint_text(&hint.text) -> Result<TableRef, FrontendError>`
(trim; case-insensitive `BROADCAST`; one `(ident)`; unquoted ASCII identifier; no
trailing). Hand-match tokens; never call back into sqlparser's expression parser.
Do not add a new `FrontendError` variant — reuse `unsupported(...)` with wording
clear enough for the negative tests. Unit-test the recognizer against the
malformed matrix in isolation.

## Phase 3 — Replace the reject; thread strategy
Replace `:2560-2561` with `let mut broadcast_hint = parse_broadcast_join_hint(optimizer_hints)?;`.
In the join loop:
`let strategy = join_strategy_for_hint(&mut broadcast_hint, &plan, &right, kind)?;`
then `build_join_plan(plan, right, kind, on, strategy)?`; after the loop,
`reject_unused_broadcast_hint(&broadcast_hint)?`. `join_strategy_for_hint`:
no hint → `HashPartitioned`; target in `plan_qualifiers(&right)` → reject (probe
side); target in `plan_qualifiers(&plan)` with the left having exactly one
qualifier and `kind == Inner` → mark consumed, `Broadcast`; target reachable only
inside a composite left (>1 qualifier) → reject; target appearing >once → reject
ambiguous; else leave pending → `HashPartitioned`. Match on `TableRef`
unqualified name.

## Phase 4 — Preserve the other reject sites
Leave `lower_insert/update/delete` optimizer-hint rejects,
`validate_exists_select_shape`, `validate_derived_select_shape`, and
`lower_exists_join`'s `HashPartitioned` untouched.

## Phase 5 — Distributed SQL gates (`src/lib.rs`)
`sql_hash_join_plan()` / `sql_broadcast_join_plan()` via `SqlPlanner::plan(...)`
(the latter with `/*+ BROADCAST(c) */`), mirroring the 125 Rune tests: digest
compare hash vs broadcast over the existing TCP join harness; set
`broadcast_build_bytes_per_task` / `broadcast_replicated_bytes_per_query` to
`ByteCount::new(1)` and assert the `BroadcastReplication` terminal error.

## Phase 6 — Guard retarget
Retarget `adr125_no_sql_hint_authoring_yet` → `adr128_sql_broadcast_hint_authoring_is_narrow`
(presence of the recognizer + `OptimizerHint`/`OptimizerHintStyle` consumption +
`JoinStrategy::Broadcast` in the join loop; `lower_exists_join` hash-only;
`GenericDialect` unchanged; unknown hints still reject). Add
`adr128_generic_dialect_remains_the_sql_dialect`,
`adr128_broadcast_hint_recognizer_consumes_optimizer_hint_text`, and
`adr128_broadcast_hints_do_not_touch_execution_substrate` (widen the existing 125
no-touch marker scan to `adr128`).

## Verification
`cargo test -p toron sql_broadcast_hint` · `cargo test -p toron
generic_dialect_preserves_select_optimizer_hints` · `cargo test -p toron --test
architecture_test adr125 adr128` · the 125 Rune + distributed broadcast tests ·
`cargo check -p toron --all-targets` · `git diff --check`.

## Risk register
- **Raw-text over-permissiveness:** `OptimizerHint.text` is unparsed; a loose
  matcher could accept combined hints/expressions. Keep it exact; the
  malformed-matrix gate pins it.
- **Wrong-side / composite-left:** reject — never swap operands or broadcast the
  accumulated left.
- **Silent fallback:** any unconsumed hint errors after the loop.
- **Scope creep:** no dialect/parser/substrate change — the
  `adr128_generic_dialect_remains_the_sql_dialect` guard is the backstop (and the
  memorial to the iter-1 over-engineering).

## Acceptance
SQL planner shape tests + distributed digest/cap gates + retargeted 125/128
guards + existing 125 Rune broadcast guards + `cargo check -p toron --all-targets`
+ `git diff --check` green; unhinted joins still hash; broadcast reachable only
via the recognized `/*+ BROADCAST(alias) */` hint.
