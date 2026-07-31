# ADR 470 implementation receipt

`hash_smaller_build_placement` defaults **off**, decided on ADR 469's six-cell price
table (wins +17.1%/+22.5% at q02/q11; material regressions −77.5%/−195% at q19/q14;
q17 −2.4% detectable; q20 null). One-line flip plus test recalibration:

- The capability tests opt in explicitly (`choose_join_strategy` carried-cause test,
  the adr464/466 suite already flagged) — they test the mechanism, not the default.
- The adr469 knob-contract test pins the new `(false, "default")` provenance.
- PlannerConfig doc names the evidence and the re-activation owners (regression
  diagnosis with q14 as the fixture; placement-aware costing).

Gates: binder 633/0, architecture 2,093/0, toron-tpch adr469 suite green, q17
parquet sentinel green, full engine suite green. Production behavior returns to the
pre-464 placement everywhere; the 469 census/prices are default-independent
(explicit arms) and stand unchanged.
