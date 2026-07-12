# ADR 422 - q11 scale-correct FRACTION: fix the degenerate fixture the non-vacuity guard caught

## Context

Post-421 board: 16/22 green. q11 has been red since the 415 sweep with a distinctive signature:
Toron reaches output in ~4.7 s (166 MB spill), **both engines return zero rows, and the
independent oracle's non-vacuity guard refuses the result**. 415 classified it as "a
correctness-instrument issue to resolve before performance work on that cell." Council grounding
this cycle closed the mystery in closed form:

1. **The SQL hardcodes the unscaled fraction.** `Q11_DEV_SQL` / `STRICT_Q11_SQL`
   (`toron-tpch/src/lib.rs:315-333`) embed `SUM(...) * 0.0001` at every scale; no scale reaches
   the SQL text.
2. **The oracle uses the same unscaled threshold.** `q11_threshold_passes` (`:7110-7115`) checks
   `value_raw x 10,000 > global_value_raw`; `strict_q11_oracle(scale_factor)` passes the scale
   only to the row generators (`:7187-7205`). Engines and oracle **agree** - all three compute
   zero rows at sf=10.
3. **Zero rows is mathematically forced at sf=10.** Each part has four `PARTSUPP` rows, so even
   the conservative all-German upper bound is `< 4 x 9,999 x 1,000.00 = 39,996,000` per part.
   The measured German national total is ~8.10e11 at sf=10, making the unscaled `0.0001`
   threshold ~8.10e7. No part can pass. The fixture question is degenerate;
   the execution is healthy.
4. **The instrument worked.** The compare guard requires a non-empty proper subset with at least
   one threshold failure (`:7246-7258`); it correctly refused to bank empty-vs-empty as green.
5. **The official query scales the parameter.** TPC-H 3.0.1, Clause **2.4.11.3 item 2**:
   `FRACTION` is `0.0001 / SF` (Clause 2.4.11.4's qualification value `0.0001` is the sf=1
   case; 2.4.11.2 keeps FRACTION in the scalar-subquery multiplier, exactly where the fixture's
   literal sits). Fixing the parameter is spec-conformance, not a canonical dodge - today's
   text simply is not official q11 at sf != 1.

Two facts the design must absorb (both council-corrected during iteration):

- **There is no n/100 scale contract to lean on.** `parse_scale_factor` accepts any finite
  positive `f64` (`:9753-9762`) and the harness pins `0.0001` as valid. The exact contract must
  be *created*, explicitly and loudly - never reconstructed by `round(sf x 100)`, which would
  silently coerce nearby accepted values.
- **Non-vacuity is scale-scoped, not universal.** At sf=0.0001 the spec fraction is exactly 1
  and no positive group can exceed the total it contributes to - a mathematically empty result
  is *correct* there and must stay honestly rejected by the instrument, not mislabeled green.

## Decision

### The exact scale contract: a three-row bit-exact table

One closed value from a bit-exact constructor - conceptually
`Q11Scale { hundredths_n, fraction_sql }`:

```text
0.01_f64.to_bits() -> { n=1,    "0.01"    }
1.0_f64.to_bits()  -> { n=100,  "0.0001"  }
10.0_f64.to_bits() -> { n=1000, "0.00001" }
```

Phase 0 also measured the proposed sf=0.02 direction control before implementation: the scaled
oracle selected 0 of 821 positive German groups. Because the unchanged guard correctly rejects
that degenerate result, sf=0.02 is not an admitted q11 rail scale; it rejects like every other
unsupported value. The operational contract contains the non-degenerate correctness scale,
sf=1 identity scale, and sf=10 board target only.

- **The SQL literal lives in the table** alongside `n` - never computed or formatted through
  `f64`. The renderer consumes `fraction_sql`; the oracle consumes `hundredths_n` in the
  reduced, fully checked predicate `value_raw x 100 x hundredths_n > global_value_raw`
  (equivalent to `value > global x 0.0001 / (n/100)`; reduces exactly to today's
  `value x 10,000 > global` at n=100, so sf=1 is bit-identical).
- **Bit equality is the eligibility test.** `"0.01"` and `"0.010"` parse to the same binary64
  value and select the same row; the adjacent bit patterns do not. No epsilon, no `sf x 100`
  arithmetic, no coercion. Extending the domain means adding an explicit `(bits, n, literal)`
  row plus its boundary tests - the ADR does not claim support for every official or accepted
  harness scale.
- Any other q11 scale is a loud `q11 requires an exactly supported scale` **usage/harness
  error** with the established exit-2 / no-telemetry semantics.
- `Q11Scale` is a public opaque `Copy` value because the existing public oracle helpers are
  exercised from integration tests. Its fields stay private and its only public constructor is
  the fallible bit-exact lookup; callers cannot manufacture an inconsistent `(n, literal)`.

### Pre-I/O rejection at every rail (the propagation pin)

The unsupported-scale error is a harness error, never an `Attempt { status: Fail }`, a compare
failure, or a late executor error - and it must fire **before fixture generation or persisted
dataset preparation**:

- the direct reach helpers (`run_dev_q11` / `run_strict_q11`) change from `Attempt` to
  `Result<Attempt, String>` and validate before constructing q11 tables; `run_command` maps the
  error to `harness_error` before emitting a row. This is required so the direct public helpers
  also reject unsupported values without panic or query-failure telemetry;
- `check_strict_query_with_data_source` validates q11 before the memory/persisted fork (so
  persisted preparation cannot start); direct `check_strict_q11` likewise returns
  `Result<CorrectnessAttempt, String>` and validates before constructing q11 tables, so an
  oracle-scale error cannot become a compare `Fail`;
- `build_perf_query_spec` validates before `perf_query_spec` constructs q11 tables;
- `build_persisted_perf_query_spec` validates **before** `prepare_persisted_tpch_dataset` - an
  unsupported scale must not generate, load, or fingerprint a dataset before returning the
  usage error.

One bit-exact `Q11Scale::try_from_f64`-shaped constructor owns eligibility. Generic high-level
rails may wrap it in a query-aware preflight, while direct q11 helpers call it themselves. The
observable contract is fixed: every public q11 entry rejects before fixture or persisted I/O,
and every CLI rail maps that rejection to exit 2 with no telemetry.

### One renderer owns every execution path

q11 SQL is currently selected independently at four seams (persisted specs `:4602/:4686-4700`,
in-memory specs `:4671/:4786-4790`, `check_strict_q11` `:7361-7392`, reach helpers
`:10507-10528`). Both SQL-bearing spec structs hold `sql: &'static str` (`:1780/:1839`); they
become `Cow<'static, str>` (or a measured smaller owned equivalent): the other 21 queries stay
borrowed, q11 owns its rendered text, and every path calls the same renderer. **No static
`Q11_DEV_SQL`/`STRICT_Q11_SQL` selection may remain on any execution path.**

### Scale-explicit oracle

`q11_threshold_passes` and `q11_oracle_from_rows` receive the selected exact scale explicitly -
synthetic tests cannot silently retain the old implicit `0.0001` contract. Every multiplication
stays checked `i128`; overflow is a loud oracle error; no `f64` FRACTION participates anywhere
in the compare.

## Acceptance

- **Live target:** persisted strict q11 sf=10 reaches compare and either **passes (board
  17/22)** or records the first genuinely non-vacuous q11 mismatch as the next correctness
  finding - both outcomes are wins over the vacuous status quo.
- **Selectivity locks (scale-scoped non-vacuity):** generated sf=0.01, sf=1, and sf=10 oracles
  are non-empty proper subsets with threshold failures; the Phase-0 sf=0.02 census records
  `0 pass / 821 fail` and the exact table rejects it. A strict-boundary synthetic pins `>`
  independent of generator accidents. Because sf=0.0001
  is not an admitted `Q11Scale`, its mathematical-emptiness lock is deliberately split: the
  scale table rejects it, and a synthetic empty `Q11Oracle` plus a correctly typed zero-row
  output batch proves the unchanged comparator guard still refuses empty-vs-empty. No
  unsupported scale bypasses the constructor, and the lock must reach the guard rather than
  fail earlier on absent output schema.
- **Scale-fence locks:** all three table rows select their exact `(n, literal)` pair; alternate
  spellings (`0.010`) select the same row; `f64::from_bits(bits +/- 1)` both reject; `0.0001`,
  `0.02`, NaN/infinity via direct library call, and an otherwise-valid unsupported positive
  scale all reject loudly.
- **Harness-error locks:** CLI reach, correctness, memory perf, and persisted perf each return
  **exit 2 with no telemetry row** for an unsupported q11 scale; the persisted case proves no
  dataset preparation occurred.
- **Ownership lock:** memory export, persisted export, reach, and correctness render
  byte-identical q11 SQL for the same mode/scale.
- **sf=1 identity:** renders the existing `0.0001` literal, reduces to the existing predicate,
  and the sf=1 compare is output-identical.
- **Audit:** the TPC-H substitution clauses / QGEN contract (not fixture comments) establish
  whether q11's FRACTION is the only SF-dependent parameter frozen into fixture SQL.
- **Controls:** full `toron-tpch` suite; the eleven engine greens untouched (no engine surface
  in this cut); full gates after fmt; no `.perf.data`.

## Guardrails

- **Exactness is structural** - the `(bits, n, literal)` row is the single source of truth for
  both renderer and oracle; no float formatting, no rounding reconstruction, no epsilon.
- **The instrument stays armed** - the non-vacuity guard is untouched; the fix makes the
  question non-degenerate, not the guard quieter.
- **Usage errors stay usage errors** - exit-2/no-telemetry semantics preserved at every rail,
  pre-I/O.
- **q11-only** - no repository-wide scale-type rework (a different cut if ever needed); q17's
  policy gap is a different defect class, carried.
- **Honest in both directions** - a post-fix mismatch is recorded as a real finding, not
  massaged.

## Risks

- **The scaled compare exposes a real engine/oracle divergence** -> that is the instrument
  working; record it as the next correctness cut's opening evidence.
- **Decimal literal binding differences between Toron and DuckDB at the threshold boundary** ->
  Phase 0 records the rendered literal per scale and proves both engines bind the same
  semantics; the boundary synthetic pins strictness.
- **A missed q11 SQL selection seam** -> the ownership lock (byte-identical rendered SQL on all
  four paths) plus a no-static-constant grep in the seal audit.
- **Silent scale coercion regression later** -> adjacent-bit rejection tests pin the contract.

## Forward Arc

q11 green (or its first real mismatch) leaves the board at 17/22-or-evidence with five cells:
q21 (post-summary probe/fold throughput - profiling-first, the speed thread's territory), q18
(SpillBytes quota + membership wall + mixed-combo handoff), q02 (join-output C-spec driver),
q05 (diagnosis-first OOM), plus q17's policy thread. The q11 exact-scale table is also the
template if q17's oracle policy gap turns out to be parameter-shaped.

## Grounding evidence (file:line)

SQL: `toron-tpch/src/lib.rs:315-333` (hardcoded `0.0001`; strict aliases dev). Oracle:
`:7110-7115` (`x 10,000` threshold), `:7117-7185` (`q11_oracle_from_rows`), `:7187-7205`
(scale only reaches generators). Guard: `:7246-7258` (non-empty proper subset + threshold
failure). Scale parsing: `:9753-9762` (any finite positive f64); harness test pins 0.0001
valid. Four SQL seams: `:4602/:4686-4700`, `:4671/:4786-4790`, `:7361-7392`, `:10507-10528`;
spec structs `:1780/:1839` (`&'static str`). Bounds arithmetic: four rows/part x
availqty <= 9,999 x supplycost <= 1,000.00 -> <39,996,000/part vs ~8.10e11 measured German
total at sf=10. Spec: TPC-H 3.0.1 Clause
2.4.11.3 item 2 (FRACTION = 0.0001/SF), 2.4.11.4 (qualification 0.0001 at sf=1). 415 rows:
`plans/impl/415_sf10_full_board_live_sweep_results.md` (q11 zero rows, oracle guard fail,
"correctness-instrument issue").
