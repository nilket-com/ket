# ADR 422 results - q11 scale-correct FRACTION

## Outcome

ADR 422 is implemented. q11 now renders the TPC-H scale-correct FRACTION from one exact scale
contract, and the independent oracle consumes the same contract with checked integer
arithmetic. Persisted strict q11 at sf=10 moved from a vacuous zero-row guard failure to
`pass@compare` with 8,685 rows. The sf=10 board moves from **16/22 to 17/22 green**.

No Toron engine code changed. The cut is confined to `toron-tpch` SQL ownership, oracle
arithmetic, rail error propagation, and tests.

## Phase 0 evidence

### Spec and substitution audit

[TPC-H 3.0.1](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf),
Clause 2.4.11.3 item 2, defines q11 FRACTION as `0.0001 / SF`; Clause 2.4.11.4's
qualification value is `0.0001` at sf=1. The audit of Clauses 2.4.1.3 through 2.4.22.3 found
no other query substitution parameter derived from SF. Q11 is the only frozen SF-dependent
fixture substitution in the 22-query set.

The closed-form zero-row proof is conservative: TPC-H generates four `PARTSUPP` rows per part,
with `PS_AVAILQTY <= 9,999` and `PS_SUPPLYCOST <= 1,000.00`, so a part's all-German upper
bound is `< 39,996,000`. The measured sf=10 German total is 810,291,376,524.68; the old
unscaled threshold was 81,029,137.65. No part could pass it.

The retained-golden baseline lock executes the old `0.0001` SQL and old sf=1 oracle threshold
over sf=10 data. It reproduced zero engine rows, zero oracle rows, and the comparator's
`oracle non-vacuous guard failed` result. This agrees with the committed ADR 415/366 live
evidence (Toron and DuckDB zero rows, 166,552,400 spill bytes).

### Exact-scale census and stop-line

The proposed sf=0.02 direction control was measured before admission and hit the plan's
stop-line: its scaled oracle is still empty. It was removed from the contract rather than
weakening the guard.

| candidate SF | `n` | SQL FRACTION | selected | positive groups | threshold failures | global raw cents | disposition |
| --- | ---: | --- | ---: | ---: | ---: | ---: | --- |
| 0.01 | 1 | `0.01` | 1 | 374 | 373 | 95,025,425,444 | admitted |
| 0.02 | 2 | `0.005` | 0 | 821 | 821 | 221,906,658,336 | rejected (degenerate) |
| 1 | 100 | `0.0001` | 1,048 | 29,818 | 28,770 | 7,874,103,109,405 | admitted |
| 10 | 1,000 | `0.00001` | 8,685 | 304,774 | 296,089 | 81,029,137,652,468 | admitted |

The final domain is therefore exactly sf=0.01 / 1 / 10. `0.02`, `0.0001`, adjacent binary64
values, NaN/infinity, and every other positive scale reject loudly.

## Implementation

### One exact value, two consumers

`Q11Scale` is a public opaque `Copy` value with private fields and one bit-exact fallible
constructor. Each admitted row stores both `hundredths_n` and its SQL literal:

```text
0.01 bits -> { n=1,    literal="0.01" }
1.0  bits -> { n=100,  literal="0.0001" }
10.0 bits -> { n=1000, literal="0.00001" }
```

There is no epsilon, `round(sf * 100)`, float-formatted literal, or unchecked constructor.
Alternate text such as `0.010` works only because it parses to the identical sf=0.01 bits.

`q11_sql(Q11Scale)` substitutes the stored literal into a single `[FRACTION]` template. The
public `Q11_DEV_SQL` / `STRICT_Q11_SQL` constants remain only as sf=1 golden references; no
execution path selects them. Both perf spec structs now store `Cow<'static, str>`: the other 21
queries remain borrowed and q11 owns its rendered text. Memory and persisted q11 specs render
byte-identical SQL.

The oracle now threads `Q11Scale` explicitly through `q11_oracle_from_rows` and
`q11_threshold_passes`. Its only threshold predicate is checked `i128` arithmetic:

```text
value_raw * 100 * hundredths_n > global_value_raw
```

At sf=1 this is exactly the prior `value_raw * 10,000 > global_value_raw` comparison.

### Pre-I/O harness errors

The direct q11 reach helpers now return `Result<Attempt, String>`; direct q11 correctness also
returns `Result<CorrectnessAttempt, String>`. Validation happens before q11 fixture generation.
The generic correctness rail validates before its memory/persisted fork, memory perf validates
before spec/table construction, and persisted perf validates before
`prepare_persisted_tpch_dataset`.

The live unsupported-scale proof returned exit 2 with this message:

```text
q11 requires an exactly supported scale (0.01, 1, or 10); got 0.0001
```

`target/tpch/correctness.jsonl` stayed byte-size identical (63,982 bytes before and after), and
the fresh persisted data directory was not created. Subprocess locks cover reach, correctness,
memory perf, and persisted perf; all emit empty stdout and exit 2.

## Live results

| SF | Toron result | elapsed / execute | rows | top `(partkey, value)` | DuckDB parity |
| --- | --- | ---: | ---: | --- | --- |
| 0.01 | `pass@compare` | 696.2 ms | 1 | `(1376, 13,271,249.89)` | same row/value |
| 1 | `pass@compare` | 1,086.0 ms | 1,048 | `(129760, 17,538,456.86)` | same count/top/value |
| 10 persisted | `pass@compare` | 10,050.4 ms | 8,685 | `(393251, 20,382,773.62)` | same count/top/value |

The sf=10 release perf row records:

- execute: 4,781.721 ms;
- spill bytes: 166,552,400 (unchanged mechanism volume);
- output rows: 8,685;
- plan fingerprint: `02e70dc5eac79715`;
- output schema fingerprint: `fnv1a64:9963b1b3f1acd665`;
- digest:
  `ArrowIpcDigest([61, 134, 12, 196, 165, 15, 119, 194, 145, 121, 126, 177, 40, 201, 32, 183, 166, 95, 161, 228, 1, 127, 252, 4, 58, 127, 107, 227, 8, 152, 241, 157])`.

The cgrouped correctness run used 1,143,184 KiB maximum RSS and exited 0. DuckDB's perf rail
retains its separate decimal-precision schema fingerprint (`schema_mismatch` flag), but its
q11 row counts and top values match the integer oracle and Toron exactly; this is not a
FRACTION-binding or value mismatch.

## Discriminating locks

- Three exact table rows and stored literals; alternate spelling and adjacent-bit fences.
- Unsupported positive, sf=0.02, sf=0.0001, NaN, and infinity rejection.
- Runtime q11 SQL is owned; non-q11 SQL remains borrowed; persisted/memory SQL is identical;
  sf=1 rendering equals the retained golden text.
- Direct library and all four CLI rail rejection paths; persisted rejection creates no data.
- Reduced checked arithmetic at n=1/100/1,000, strict-boundary exclusion, and overflow error.
- Scale-explicit synthetic oracle; no implicit-threshold helper remains.
- Correctly typed zero-row output reaches the unchanged non-vacuity guard.
- Heavy ignored locks were run explicitly: old sf=10 vacuous baseline and the admitted-scale
  selectivity census.
- Source audit finds no runtime reference to the static q11 SQL constants.

## Verification

- `scripts/cargo-cgroup fmt -- --check` - pass.
- `scripts/cargo-cgroup check -p toron-tpch --all-targets` - pass.
- `scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast` -
  292 passed; the two heavy ADR 422 tests are ignored in the ordinary suite and passed when run
  explicitly.
- `scripts/cargo-cgroup test -p toron --test architecture_test` - 2,079 / 2,079 passed.
- `scripts/cargo-cgroup test -p toron --no-fail-fast` - pass.
- Release build and live sf=0.01 / sf=1 / persisted sf=10 commands - pass.
- `git diff --check` - pass; no tracked `.perf.data`.

The eleven sf=10 engine controls were not rerun: the implementation changes no Toron engine
surface, matching the implementation plan's conditional control rule. q21, q18, q02, q05, and
q17 remain the five named board threads.
