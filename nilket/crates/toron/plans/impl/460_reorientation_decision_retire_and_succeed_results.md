# ADR 460 implementation receipt

A decision cut: no code changed. Chain reorientation is retired on the post-458/459
ledger; the `MAX_FILTER_BYTES` cap raise is promoted as the successor with its
predicted effect quantified below. All figures cite sealed artifacts.

## The ledger the decision rests on

ON-arm stage attribution (gate-zero profiles, post-458 format):

| stage | sf=8 (64 MiB, 2 GiB) | sf=10 (64 MiB, 4 GiB) |
|---|---:|---:|
| root span | 21.12 s | 27.54 s |
| `runtime_prep_build` | 7.76 s (36.7%) | 9.77 s (35.5%) |
| `reservoir_partition_spill_write` | 6.51 s (30.8%) | 8.65 s (31.4%) |
| `base_accumulation` | 3.80 s (18.0%) | 4.84 s (17.6%) |
| `reservoir_read_probe` | 1.92 s (9.1%) | 2.81 s (10.2%) |
| `suffix_join` | 1.00 s (4.7%) | 1.32 s (4.8%) |

459 sealed this ledger complete: whole-query spill reconciles to these stages to the
byte at both scales, so no unattributed producer can revise the shares.

Reorientation's addressable set is the reservoir write/read minus the irreducible
lineitem scan (the write leaf spans scanning all 47,989,007 sf=8 source rows in both
arms; the scan survives any chain shape). `runtime_prep_build` — the largest owner —
is prefix-stage builds that reorientation retains, and `base_accumulation` becomes
its built side. Bounded upside, board-wide grouped-geometry reshape cost (442's
risk): retired.

## The successor's predicted effect (estimate, assumptions named)

From 446's FPR analysis (cap 1 MiB ≈ 17.3% FPR; ~2.7 MiB ≈ ~1%), applied to the
sf=8 gate-zero identities:

- ON reservoir today: 11,742,021 rows = true matches + FPR × 47,989,007.
- Inferred true matches ≈ 11,742,021 − 0.173 × 47,989,007 ≈ **3.44M rows**.
- At ~1% FPR: ≈ 3.44M + 0.48M ≈ **3.9M rows** — a further ~3× reservoir reduction
  from a cap constant.

Assumption: 446's FPR figures transfer to this cell's key distribution. The successor
cut's gate zero measures the actual count; a miss is a result. The paired instrument,
fingerprint/budget-locked manifests, and the 10%-of-OFF-median floor stand ready
unchanged.

## Queue effect

- "Chain reorientation (demoted)" → **retired**, re-opening condition stated in the
  decision ADR (a cell whose addressable share clears the reshape risk, argued in
  this ledger's vocabulary).
- `MAX_FILTER_BYTES` cap raise → queue head, predicted effect attached.
- DuckDB's 206 MB shape remains retained context.
