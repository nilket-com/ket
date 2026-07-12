# ADR 422 - Implementation plan: q11 scale-correct FRACTION

Pairs with `plans/adr/422_q11_scale_correct_fraction.md`. One bounded fixture-contract cut in
`toron-tpch`: a four-row bit-exact q11 scale table (`(f64 bits, hundredths_n, SQL literal)`)
feeding one shared SQL renderer and a scale-explicit integer oracle; unsupported q11 scales
reject pre-I/O with exit-2/no-telemetry harness semantics. No Toron engine surface changes.

## Phase 0 - record before the cut

1. **Spec pin:** cite TPC-H 3.0.1 Clause 2.4.11.3 item 2 (FRACTION = 0.0001/SF) and 2.4.11.4
   (qualification value 0.0001 = the sf=1 case) in the results doc.
2. **Rail-scale inventory:** enumerate every scale q11 actually runs at in the rails (reach
   default, correctness sf=0.01, sf=0.02 controls, sf=1, sf=10 board target) - this is the
   four-row domain's justification; unsupported values become loud errors, so the inventory
   must be complete.
3. **Rendered-literal record:** for each table row, the exact SQL literal (`0.01`, `0.005`,
   `0.0001`, `0.00001`) and a bind-level check that Toron and DuckDB parse the same threshold
   semantics from it (decimal literal, no float drift at the boundary).
4. **Reproduce the vacuous baseline** at sf=10 (engines zero rows; oracle non-vacuity guard
   fail) for the before/after record.
5. **Substitution audit:** walk the TPC-H substitution clauses / QGEN parameter contract for
   the other 21 fixtures and record whether any other SF-dependent parameter is frozen into
   fixture SQL. Fixture comments are not evidence.
6. **Selectivity census before implementation:** independently apply the four exact fractions
   to generated q11 groups at sf=0.01/0.02/1/10 and record pass/fail group counts. All four
   must be non-empty proper subsets with at least one threshold failure. If any admitted row
   is degenerate, stop and re-scope the table/rail contract; do not weaken the comparator guard.

## Phase 1 - the cut

1. **`Q11Scale` table:** bit-exact constructor over the four rows
   (`0.01 -> (1, "0.01")`, `0.02 -> (2, "0.005")`, `1.0 -> (100, "0.0001")`,
   `10.0 -> (1000, "0.00001")`); lookup by `f64::to_bits` equality; any other value ->
   `Err("q11 requires an exactly supported scale ...")`. No rounding, no epsilon, no
   float-formatted literals. Make it a public opaque `Copy` value with private fields and one
   public fallible constructor: the existing public oracle helpers are integration-test API,
   and no caller may construct a mismatched `(n, literal)` directly.
2. **Shared renderer:** `q11_sql(mode, Q11Scale)` renders the one query text with the row's
   literal. Both spec structs (`:1780/:1839`) change `sql: &'static str` ->
   `Cow<'static, str>` (21 queries borrowed, q11 owned). All four seams (persisted spec,
   memory spec, `check_strict_q11`, reach helpers) consume the renderer; the static constants
   are removed from execution paths (retained only as the sf=1 golden reference if any test
   wants the literal comparison).
3. **Pre-I/O rejection ordering** (the harness-error pin):
   - the two direct reach helpers change to `Result<Attempt, String>`, validate before table
     construction, and `run_command` maps `Err` to a harness error before emitting a row;
   - `check_strict_query_with_data_source` validates before the memory/persisted fork; direct
     `check_strict_q11` changes to `Result<CorrectnessAttempt, String>` and validates before
     constructing tables, so library callers receive an error rather than failure telemetry;
   - `build_perf_query_spec`: validate before spec construction;
   - `build_persisted_perf_query_spec`: validate **before** `prepare_persisted_tpch_dataset` -
     no dataset generation/load/fingerprint on the error path.
   All map to the established ADR 261 usage-error semantics: exit 2, no telemetry row.
4. **Scale-explicit oracle:** `q11_threshold_passes(value, global, scale)` runs
   `value x 100 x hundredths_n > global` with checked i128 multiplication (overflow = loud
   oracle error); `q11_oracle_from_rows(..., scale)` and `strict_q11_oracle(sf)` thread the
   exact scale; no implicit-threshold entry point survives for synthetic tests to keep.
5. Comparator/guards unchanged: non-empty proper subset + threshold-failure requirements stay
   armed exactly as today.

## Tests (discriminating)

- **Table locks:** four exact selections; alternate spellings (`"0.010"` -> same row);
  `f64::from_bits(0.01.to_bits() - 1)` and `+ 1` both reject; `0.0001`, NaN, infinity, and an
  unsupported positive scale reject loudly through the library API.
- **Harness-error locks:** CLI reach, correctness, memory perf, and persisted perf each exit 2
  with no telemetry row on an unsupported q11 scale; the persisted case asserts no dataset
  directory/fingerprint was created.
- **Ownership lock:** memory export, persisted export, reach, and correctness render
  byte-identical q11 SQL per mode/scale; a source grep proves no execution path references the
  static constants.
- **Arithmetic locks:** sf=1 reduces to the exact current predicate; the reduced inequality at
  n=1/2/1000 against hand-computed fixtures; checked-overflow failure is loud.
- **Selectivity locks:** generated oracles at sf=0.01, sf=1, sf=10 are non-empty proper
  subsets with threshold failures; strict-boundary synthetic pins `>` (value exactly at
  threshold excluded). Since sf=0.0001 is unsupported by `Q11Scale`, its scale-scoped
  non-vacuity lock has two independent assertions: table lookup rejects it, and the unchanged
  comparator rejects a synthetic empty oracle paired with a correctly typed zero-row q11
  batch. The latter must reach the non-vacuity guard (not fail first on missing schema), and the
  test must not bypass the scale constructor to run the renderer/oracle at an unsupported
  value.
- **sf=1 identity lock:** dev + strict q11 compare output-identical before/after the cut.

## Phase 2 - re-measure

```bash
scripts/cargo-cgroup build -p toron-tpch --release
mkdir -p target/tpch/adr422
# live scaled correctness at the rail scales (small scales are cheap)
for sf in 0.01 0.02 1; do
  target/release/toron-tpch correctness --query q11 --sf "$sf" --mode strict \
    --data-source memory >> target/tpch/adr422/q11_small.out 2>&1
done
# the board target: persisted strict sf=10
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  /usr/bin/time -v target/release/toron-tpch correctness --query q11 --sf 10 --mode strict \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr422/q11_sf10.json 2> target/tpch/adr422/q11_sf10.err
rm -rf /tmp/toron_join_spill
# usage-error proof (exit 2, no correctness telemetry, no dataset prep)
reject_dir=target/tpch/adr422/reject_data
rm -rf "$reject_dir"
correctness_before=$(stat -c %s target/tpch/correctness.jsonl 2>/dev/null || echo 0)
set +e
target/release/toron-tpch correctness --query q11 --sf 0.0001 --mode strict \
  --data-source parquet --data-dir "$reject_dir" \
  > target/tpch/adr422/q11_reject.out 2>&1
reject_status=$?
set -e
echo "exit=$reject_status" >> target/tpch/adr422/q11_reject.out
test "$reject_status" -eq 2
test ! -e "$reject_dir"
correctness_after=$(stat -c %s target/tpch/correctness.jsonl 2>/dev/null || echo 0)
test "$correctness_before" = "$correctness_after"
```
- q11 sf=10: pass@compare (board 17/22) **or** the recorded non-vacuous mismatch with row/value
  evidence - the next correctness finding, not a failure of this cut.
- Row counts recorded per scale (expect ~1,048 at sf=1; non-empty proper subsets at 0.01/10).
- Engine controls are untouched by a fixture-crate cut; run the eleven-green control loop only
  if any shared engine surface was touched (expected: none).

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
scripts/cargo-cgroup test -p toron --test architecture_test        # source-scan guards
scripts/cargo-cgroup test -p toron --no-fail-fast                   # engine unaffected; cheap safety
git diff --check
git ls-files '*.perf.data'
```

## Deliverable (results doc)

`crates/toron/plans/impl/422_q11_scale_correct_fraction_results.md`:
- Phase 0: spec citations, rail-scale inventory, rendered literals + bind parity, the vacuous
  baseline, the substitution audit verdict;
- the cut: table, renderer/ownership change, pre-I/O rejection ordering per rail,
  scale-explicit oracle;
- q11 outcomes per scale with row counts; the sf=10 compare verdict (green or the recorded
  real mismatch);
- locks: why each is discriminating (adjacent-bit and no-dataset-prep especially);
- confirmation: guard untouched, sf=1 identity, no engine surface, exit-2 semantics preserved,
  no static-constant execution path; scope honesty (q21/q18/q02/q05/q17 remain named).

## Risk / blast radius

- **A real post-fix mismatch** -> recorded as the next correctness finding; not massaged.
- **Literal binding drift between engines** -> Phase-0 bind parity + boundary synthetic.
- **Missed SQL seam** -> ownership lock + constant grep.
- **Cow churn wider than measured** -> the alternative owned-string shape is sanctioned if
  smaller; the lock is byte-identical rendered SQL, not the container type.
- **Future scale additions done sloppily** -> extending the domain requires a new table row
  plus its boundary tests by construction.

## Seal

`plan: adr 422 q11 scale correct fraction` (the dual). Implementation:
`impl: adr 422 q11 scale correct fraction`. Do not commit `.perf.data`.
