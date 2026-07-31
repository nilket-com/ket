# ADR 450 implementation receipt

The scale-ladder probe is implemented with fresh per-rung datasets, profile
directories, and spill cleanup. It classifies the two known non-commit
diagnostics, hard errors, and an inconclusive harness-blocked outcome. Partial
ladder records are persisted even on hard failure.

## Observed ladder

The measured ascending probe reached:

- sf=1: non-committing, Guard A (`admitted chain did not commit the generic
  grouped reservoir path`), 6.5 s.
- sf=2: non-committing, Guard A, 34.0 s.
- sf=3: non-committing, Guard A, 58.1 s.
- sf=4: hard error, `unsupported_decimal_arrow` (`Decimal128(15, 2)` where
  `Int64` was expected), 68.0 s.

This was not a no-commit result. The persisted harness kept a full
source-schema reader when the runtime filter was present (`lib.rs:1724-1725`)
but still passed that full batch through reduced-column inflation
(`lib.rs:1799-1804` and the normal streaming branch). The inflater placed raw
column 2 into source slot 8, producing the exact Decimal/Int64 mismatch. The
ladder was therefore **inconclusive, blocked before reservoir resolution**, not
`no_commit`.

The harness repair makes both branches read the same projected union and pass
it through the same source-schema inflation before filtering. The architecture
guard pins this relationship.

## Corrected committing-region rerun

After the repair, one clean sf=4 run completed with:

- outcome: `committed`
- `reservoir_build_rows`: 3,885,538
- execute time: 403,886.322 ms
- manifest: `fnv1a64:4ef73b4e796c7cf6`
- selected fixture scale: `sf=4`

The final rerun used the parity-complete harness: both filter arms read the
same projected source-column union and use the same inflation path. The
runtime-filter arm is therefore not a full-column I/O treatment.

This is a path-reachability result only. It does not claim filter effect,
throughput improvement, or ablation correctness parity; ADR 449 gate zero must
rerun at sf=4 before any paired cohort.

This is a reachability/affordability finding, not a throughput result. sf=10
remains untrusted because it never produced an accepted profile in this probe.
