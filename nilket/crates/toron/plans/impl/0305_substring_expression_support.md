# ADR 305 — Implementation plan: SUBSTRING expression support

Pairs with `plans/adr/0305_substring_expression_support.md`. Engine cut (`crates/toron`: lowering + IR + binder +
exec + walkers + guards). Anchors are current-tree. Mirror `Like`/`Extract`.

## Phase 0 — confirm the anchors
- `lower_expr` catch-all (SUBSTRING falls here). `SqlExpr::Substring { expr, substring_from, substring_for,
  special, shorthand }` (sqlparser 0.62). `BoundExprNode::Like` / `Extract` are the closest mirrors.
  `arrow::compute::kernels::substring::substring_by_char` is available through Arrow's `arrow_string` re-export.
- Enumerate every `SqlExpr`/`Expr` walker that matches `Cast`/`Extract`
  (`reject_nested_aggregate`, aggregate/subquery detection, hidden-sort validators, structural hash/eq visitors,
  storage-returning validators, logical-cast collection, and bound-program column walkers) — the new variant
  needs each.

## D1 — lowering (`sql_planner.rs`)
- `SqlExpr::Substring { expr, substring_from, substring_for, special, shorthand }` arm in `lower_expr` (before the
  catch-all):
  - lower `expr` → `Expr::Substring { input: Box<Expr>, from: i64, length: Option<i64> }`.
  - **`from`/`for` must be integer literals** — match `substring_from`/`substring_for` as constant `SqlExpr::Value`
    integers; a non-literal (per-row) `from`/`for` → `unsupported(...)` precisely. Missing `substring_from` →
    reject (require explicit start).
  - **`from <= 0` → `unsupported(...)` cleanly** (no clamp — operator's conservative semantics). `from >= 1` lowers
    to `from`.
  - accept both sqlparser encodings: `special == false` for `FROM`/`FOR` and `special == true` for comma form
    (same positional meaning); `shorthand == true` (`SUBSTR`) rejects precisely for this cut; the `SIMILAR`/regex
    form (if a distinct shape) → reject precisely.
- Add `Expr::Substring` to the walkers (recurse into `input`; `from`/`length` are scalars — no recursion needed but
  the arm must exist so the variant isn't dropped).

## D2 — IR + binder
- `ir.rs`: `Expr::Substring { input: Box<Expr>, from: i64, length: Option<i64> }`, with PBC eligibility false.
- `ir/execution.rs`: `BoundExprNode::Substring { input: BoundExprId, from: i64, length: Option<u64> }` (mirror
  `Like`). Add it to builder append, nullable, display/debug, `node_children`, and any helper lists so it is not
  missed.
- binder: bind `Expr::Substring` → check `input` binds to `Utf8` (else `TypeMismatch` precise); carry the validated
  `from` (≥1) / `length` (≥0); output type `Utf8`. `append_substring` builder mirroring `append_like`.
- Add the new `BoundExprNode` arm to `pbc.rs` as not PBC-eligible / unsupported by bytecode, matching `Like` and
  the temporal scalar nodes.
- **Reconcile IR-variant architecture guards** (variant lists/counts in `crates/toron/tests/architecture/`) — a new
  `BoundExprNode` variant trips them. `cargo check -p toron --all-targets` after.

## D3 — exec (`exec.rs`)
- `evaluate_substring(input_array, from, length)`:
  - `arrow_start = from - 1` (1-based → 0-based; `from >= 1` guaranteed by the binder, so `arrow_start >= 0`).
  - `arrow_len = length` (`None` → rest of string). `FOR 0` returns the empty string; negative `FOR` length rejects
    at bind before it can become `u64`.
  - Use the character-counted Utf8 path: downcast `DataType::Utf8` to `StringArray` and call
    `arrow::compute::kernels::substring::substring_by_char(input, arrow_start, arrow_len)`. Do not use the
    byte-counted `substring` kernel as the primary SQL implementation; it can split non-ASCII code points.
  - Return `StringArray`; nulls propagate.

## Tests
- **Engine** (binder/exec test home + an `arrow`/string probe):
  - `SUBSTRING('abcde' FROM 2 FOR 3)` → `'bcd'` (the 1-based mapping); `SUBSTRING('abcde' FROM 1 FOR 2)` → `'ab'`
    (q22's shape); no-`FOR` → rest (`SUBSTRING('abcde' FROM 3)` → `'cde'`); a NULL input row → NULL.
  - Character semantics, not byte semantics: `SUBSTRING('Γabc' FROM 2 FOR 2)` → `'ab'`.
  - **`SUBSTRING(x FROM 0 …)` and a negative start → reject cleanly** (the conservative semantics; not clamped);
    negative `FOR` rejects; `FOR 0` returns empty.
  - a **non-literal** `from`/`for` (e.g. `FROM c_acctbal`) → reject precisely.
  - `SUBSTR(x, 1, 2)` rejects precisely in this cut; `SUBSTRING(x, 1, 2)` works.
  - revert the lowering arm → rejects at the catch-all (discriminating).
- **Architecture:** the new `BoundExprNode`/`Expr` variant reconciled in the variant guards; full `architecture_test`.
- **Gauge** (`toron-tpch`): verify-first re-measure q22.

## Phase 3 — verify-first re-measure q22
```
target/debug/toron-tpch run --query q22 --mode dev   --sf 0.01 --allow-blocked
target/debug/toron-tpch run --query q22 --mode strict --sf 0.01 --allow-blocked
```
- **Success = q22 moves OFF `unsupported_substring@plan`** to its next wall (correlated `NOT EXISTS`, the
  `AVG`-threshold subquery, or another), measured + classified honestly (a precise reason or a documented
  `fail(unrecognized@phase)`). `pass@execute` is **not** required. Update the q22 harness row to the new measured
  shape. If the next wall has no precise classifier yet, record the raw error (names ADR 306).

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`) — FULL toron (new IR variant + walkers)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- run --query q22 --mode dev --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- run --query q22 --mode strict --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
git diff --check
```

## Results doc
`crates/toron/plans/impl/0305_substring_expression_support_results.md`: the SUBSTRING semantics (1-based→0-based;
character-counted; `from ≤ 0` rejects; negative `for` rejects; literal-only); the slice-test results; q22's **new
measured wall** (off `unsupported_substring` → what's next, naming ADR 306); same-suite no-regression (14 green
reach cells / 14 correctness cells unchanged).

## Risk / blast radius
- **A new `Expr` + `BoundExprNode` variant is wide** — every walker + variant guard needs the arm; `--all-targets`
  + `--no-fail-fast` + `architecture_test` are the net (a missed walker = silent bug; a missed guard = red seal).
- **1-based → 0-based mapping** — pin with exact-slice tests; `from ≤ 0` rejects, never guesses.
- **Scope** — literal `from`/`for`, `from ≥ 1`, `for ≥ 0`; defer per-row, `<1` clamp, `SUBSTR`, SIMILAR. Don't
  widen.
- **q22 acceptance is "moves off", not green** — the next honest wall is the win.

## Seal
`impl: adr 305 substring expression support`. Do not commit wip drafts.
