# ADR 128: SQL Broadcast Join Hint Authoring

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** on the
behavioral design; the two parser-mechanics corrections (hints are **raw text**,
not parsed `Expr`; `GenericDialect` **already captures** comment hints, so **no
dialect change**) were grounded against sqlparser 0.62 and are surfaced in
§Council history, not papered. Claude carried the unnecessary `ToronDialect`
through iter-1 and conceded it in iter-2; Gemini's iter-1 impl sketch treated
hints as `Expr` and is superseded by the raw-text recognizer. Codex drove both
groundings; the surviving behavioral shape is common to the three latest peer
positions after replacing Gemini's `Expr` implementation sketch with the
grounded raw-text helper.
**Category:** Toron interior — SQL frontend authoring over the existing
distributed broadcast-join execution. No new IR / wire / dialect / binder /
fragmenter / scheduler / executor substrate.
**Predecessors:** 125 (Rune broadcast authoring + strategy-aware
`build_join_plan` + the `adr125_no_sql_hint_authoring_yet` guard, pre-positioned
to retarget); 121 (`plan_qualifiers` qualifier resolution); 122 (SQL/Rune join
authoring); 124–127 (live-shuffle substrate stabilized before this frontend cut).
**Migration:** `plans/impl/128_sql_broadcast_join_hint_authoring.md`.

## Doctrine — expose the strategy, recognize one keyword, change nothing else

> Broadcast execution is complete + verified (125): `build_join_plan`'s
> `JoinStrategy::Broadcast` arm (`sql_planner.rs:4067`) builds left=`Exchange(1)`/
> right-bare; binder `validate_strategy_shape` (`binder.rs:3664`) + fragmenter
> `fragment_broadcast_join_plan` (`fragment.rs:473`) enforce INNER-only +
> build-side placement; 125's headline proved digest-equivalence with hash. The
> SQL gap is a **single seam**, grounded: `GenericDialect` already lands
> `/*+ … */` in `Select.optimizer_hints` (`generic.rs:288` →
> `supports_comment_optimizer_hint = true`; the default is `false` at
> `dialect/mod.rs:1534`; `parse_select` calls `maybe_parse_optimizer_hints`
> unconditionally at `parser/mod.rs:14715`, which gates on that method at
> `:14913`), but `lower_select` rejects **all** hints (`:2560`), and the hint
> payload is **raw text** (`OptimizerHint{prefix,text,style}`,
> `ast/mod.rs:11983`), not parsed args. ADR 128 replaces the blanket reject with:
> **recognize** exactly `BROADCAST(<ident>)` from the raw text and thread
> `JoinStrategy::Broadcast` when it names the build side; reject everything else.
> Downstream validators are the receipt ([[logical_identity_receipt_doctrine]]);
> the frontend selects + rejects early. **No parser/dialect change** — sqlparser
> is consumed as-is.

## Decisions

**D1 — No dialect or parser change.** Keep `GenericDialect {}` at both production
parse sites (`sql_planner.rs:1408`, `:1430`); it already captures comment hints.
Import `OptimizerHint` / `OptimizerHintStyle`. The cut touches **only**
`lower_select`'s hint handling + the explicit join loop — no `ToronDialect`, no
`AnsiDialect`, no SQL preprocessor.

**D2 — Raw-text recognizer, not a parser.** `parse_broadcast_hint_text(text:
&str) -> Result<TableRef, FrontendError>` over `OptimizerHint.text`: trim;
case-insensitive `BROADCAST`; exactly one parenthesized **unquoted ASCII
identifier** (`[A-Za-z_][A-Za-z0-9_]*`); no trailing tokens. Require empty
`prefix` + `OptimizerHintStyle::MultiLine` (the `/*+ … */` form). Any deviation —
single-line hint, prefixed `/*toron+ … */`, quoted/multi/zero args, trailing
text — → bounded `unsupported(...)`. Hand-match tokens; never call back into
sqlparser's expression parser. One shape recognized, everything else refused.

**D3 — Replace the blanket reject; thread strategy at the join loop (`:2646`).**
The `:2560-2561` `"optimizer hints are not supported"` reject becomes: parse at
most one broadcast hint (`>1` → reject), carry it into the join loop. Per join:
`Broadcast` iff the hint names the current left/build relation **and** the left
has exactly one qualifier **and** `kind == Inner`; else `HashPartitioned`. After
the loop, an unconsumed hint → reject (covers hint-with-no-join and
unknown-relation).

**D4 — Build-side resolution via `plan_qualifiers` (121).** Match the hinted
identifier against `plan_qualifiers(&plan)` (accumulated left/build) vs
`plan_qualifiers(&right)` (probe) at each explicit join. The target matches a
visible unqualified relation name (normally the alias: `small s` →
`BROADCAST(s)`; an unaliased one-part table may match its name;
schema-qualified-unaliased should alias, as `BROADCAST(app.small)` is outside the
one-identifier grammar). Right-side match → reject (no operand swap — that
changes output column order, an optimizer behavior not an authoring hint).
Target appearing >once in visible qualifiers → reject as ambiguous. Target only
reachable inside an already-composite left (>1 qualifier) → reject: a
single-relation hint cannot name a multi-relation build side.

**D5 — No silent fallback.** Malformed / duplicate / unknown / right-side /
non-INNER / ambiguous / composite-left / unused broadcast hints all →
`FrontendError`. A user-supplied broadcast intent either shapes the plan or fails
loudly. Unhinted joins stay hash-partitioned, byte-for-byte unchanged.

**D6 — INNER-only mirrored; durable enforcement stays downstream.**
`build_join_plan`'s Broadcast arm already rejects non-INNER (`:4068`), re-checked
by binder + fragmenter. SQL may reject early for a clearer authoring-time error
but adds **no** SQL-only broadcast validator.

**D7 — Engine-generated / nested paths unchanged.** `lower_exists_join` stays
`HashPartitioned` (`:3598`, engine-generated, not user-authored).
`validate_exists_select_shape` / `validate_derived_select_shape` keep rejecting
non-empty optimizer hints. ADR 128 covers only top-level explicit joins lowered
by `lower_select`.

**D8 — Retarget the 125 guard, don't delete (discipline #4).**
`adr125_no_sql_hint_authoring_yet` (`rune_frontend.rs:1882`) currently asserts
absence of `BROADCAST`/`JoinStrategy::Broadcast` in the planner. Retarget →
`adr128_sql_broadcast_hint_authoring_is_narrow`, pinning the positive invariant:
SQL still uses `GenericDialect`; the planner consumes
`OptimizerHint`/`OptimizerHintStyle` (not raw comment scanning); the recognizer
accepts only `BROADCAST(<ident>)` from `OptimizerHint.text`; the join loop may
select `Broadcast` only via that hint; `lower_exists_join` stays hash-only;
unknown hints still reject; no parser-dependency/dialect change.

**D9 — Scope boundary.** Production touch: `sql_planner.rs` only. Tests/guards:
`src/lib.rs` + `tests/architecture/rune_frontend.rs`. **No** sqlparser
version/dialect change, **no** preprocessor, **no** IR, **no**
binder/fragmenter/scheduler/executor/Rune change, **no** right-side broadcast,
**no** non-INNER widening, **no** cost-based selection.

## Behavior gates
- `generic_dialect_preserves_select_optimizer_hints` — parse-only proof that
  `GenericDialect` captures `SELECT /*+ BROADCAST(s) */ …` into
  `Select.optimizer_hints`. **Passes on `main`** — a *premise* gate documenting
  the seam (the others fail on main).
- `sql_broadcast_hint_emits_broadcast_join_shape` — hinted INNER join lowers to
  `Join{strategy: Broadcast, left: Exchange(partition_count:1), right: bare}`.
  **Fails on `main` by rejection** (`:2560` rejects the hint today).
- `sql_broadcast_hint_digest_matches_hash_join_over_tcp` (headline) — hinted SQL
  broadcast = unhinted SQL hash digest over the existing distributed harness
  (cross-checked against 125's Rune-broadcast digest).
- `sql_broadcast_hint_respects_cap_terminally` — hinted SQL broadcast over a tiny
  broadcast budget hits the existing `BroadcastReplication` terminal error.
- `sql_inner_join_without_hint_stays_hash_partitioned` (regression).
- `sql_broadcast_hint_targeting_probe_rejects` · `…_non_inner_rejects` ·
  `…_malformed_unknown_duplicate_rejects` · `…_unused_rejects` ·
  `…_composite_left_rejects` — the D5 reject matrix.
- Regression: `lower_exists_join` stays hash-partitioned.

## Architecture guards
- `adr128_sql_broadcast_hint_authoring_is_narrow` — retargeted from the 125 guard
  (D8 invariants).
- `adr128_generic_dialect_remains_the_sql_dialect` — `sql_planner.rs` uses
  `GenericDialect` at the production parse sites; no `ToronDialect`/`AnsiDialect`,
  and no production hint path that scans or reparses the original SQL text.
  Fixture SQL in tests may still contain `/*+ ... */`. *(Pins the iter-1→iter-2
  correction permanently — a guard born from the grounding miss, so the
  over-engineering cannot creep back.)*
- `adr128_broadcast_hint_recognizer_consumes_optimizer_hint_text` — the recognizer
  takes `&[OptimizerHint]`/`OptimizerHint` and references `OptimizerHintStyle`, not
  the original SQL string (Codex iter-2 — sharper than a generic recognizer pin).
- `adr128_broadcast_hints_do_not_touch_execution_substrate` — no `adr128` markers
  in `exec.rs`, `distributed/fragment.rs`, `distributed/scheduler.rs`,
  `binder.rs`, `pbc.rs`, Wendelon. Existing 125 Rune-broadcast +
  `build_join_plan`-shape guards stay.

## Council history
- **iter-0:** 3-of-3 on the topic. Claude challenged the roadmap's "just read
  `optimizer_hints`" shorthand but introduced two wrong premises — hints arrive as
  parsed `Expr`, and `GenericDialect` doesn't capture them (so a `ToronDialect` is
  needed) — and briefly probed the wrong sqlparser version (0.59; toron is 0.62),
  self-flagging.
- **iter-1:** Codex grounded both premises against 0.62 — `OptimizerHint` is raw
  text, and `GenericDialect` already returns `supports_comment_optimizer_hint =
  true` (`generic.rs:288`). Gemini framed stage 2 as a *recognizer* (good) but its
  impl sketch still typed hints as `&[Expr]` (wrong, superseded). Claude's iter-1
  conceded the raw-text point but **still carried the unnecessary `ToronDialect`**.
- **iter-2:** Claude concedes the dialect change entirely, converging on the
  no-dialect-change shape; Codex iter-2 is point-for-point identical. The
  recognizer + threading + qualifier check + reject matrix + guard retarget were
  3-of-3 from iter-1. No load-bearing divergence remains.

The honest arc: three sqlparser-layer corrections (version, payload type, dialect
capability), each caught by grounding the actual crate source before promotion —
[[feedback_concession_cycle_ground_not_headcount]] applied to a dependency's
internals.

## What to do next
Non-INNER broadcast widening (per-kind binder/fragmenter proofs); right-side
broadcast / operand swap (needs output-order + qualifier design); cost-based
selection (needs cardinality estimates absent from IR); chunk-granular sealed
manifest (remaining live-shuffle memory link).

## Promotion criteria
All behavior gates green (headline digest-equivalence; the shape gate must fail on
`main` beforehand; the premise gate passes on `main`); the four `adr128_*` guards
hold; the retargeted 125 guard holds; 122/125 + join-kind suites green; `cargo
test -p toron` + `cargo test -p toron --test architecture_test adr125 adr128` +
`cargo check -p toron --all-targets` + `git diff --check` green. The hint must
change plan shape only through the parsed `OptimizerHint` path.
