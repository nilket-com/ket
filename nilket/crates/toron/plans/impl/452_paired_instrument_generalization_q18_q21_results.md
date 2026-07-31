# ADR 452 implementation receipt

Rails 0 and 1 are implemented and tested; Rails 2 and 3 never fired because no
committing rung was observed beyond q05. 452 seals as a reachability result: q21 is a
complete no-commit through sf=4, while q18's reachability remains open behind a
pre-existing sf=2 engine error.

## Rail 0 — fingerprint-locked resume

The paired-ablation manifest now records the persisted dataset's
`manifest_fingerprint`, computed by `load_persisted_tpch_dataset` (which hash-verifies
every table file) at launch and stamped into the live gate-zero record; resume requires
the stored manifest to match the fingerprint the in-run gate zero just revalidated. A
regenerated dataset at the same path refuses; an empty fingerprint never launches; a
pre-fingerprint manifest can never match and is refused, not grandfathered. Pinned in
`adr449_paired_journal_is_durable_and_resume_config_locked`.

## Rail 1 — query-parametric ladder and the taxonomy gap it exposed

The 450 ladder takes `--query`; q05 keeps the legacy artifact names (449's fixture-scale
contract unmoved), other queries get `_q{NN}`-suffixed ladder/selected-scale artifacts
with the query stamped in every record.

First execution exposed a real taxonomy gap: q18 and q21 children **passed with zero
grouped-chain profiles** — the admitted-chain surface was never entered, so neither
commit guard could fire, a state the q05-shaped classifier (pass ⇒ exactly one profile)
mis-read as a hard error. Pass-with-zero-profiles is now a third named non-commit
diagnostic (`no grouped-chain surface entered`), still a climb signal; ambiguous
multi-profile passes remain hard errors. Pinned by
`adr452_classifier_pass_with_zero_profiles_is_non_committing`, which fails under the
pre-452 classifier.

## Observed ladders (release, sf 1–4, per-rung isolation)

- **q21: complete no-commit.** Non-committing at every rung (sf 1/2/3/4, all
  `no grouped-chain surface entered`; 1.9 s / 6.7 s / 10.0 s / 17.3 s). The reservoir —
  and therefore the 446 bridge — is unreachable for q21 across the affordability range.
  Sealed result; no oracle, no cohort.
- **q18: blocked at sf=2 by a pre-existing engine red.** sf=1 is non-committing (same
  diagnostic, 2.9 s); sf=2 fails at EXECUTE with `committed membership-first chain
  could not stream its terminal stage`. A profiling-free probe reproduces the failure
  identically (persisted at
  `target/tpch/adr452_ladders/q18_sf2_no_profile_probe/latest_raw.jsonl`, no profile
  env set), so this is an engine defect on q18's membership-first path at capacity,
  not a harness or profiling artifact — scale-triggered (sf=1 passes) and independent
  of this cut's driver-only changes. q18's conclusion is **no observed commit**, not
  no-commit: reachability remains open behind the queued q18 strict one-pass repair;
  the honest red is recorded, not reclassified.

## Verdict

No committing rung was **observed** beyond q05: q21 establishes no-commit through
sf=4; q18 establishes it only at sf=1, with sf≥2 unprobed behind the engine error. The
query-shaped oracle and cohort rails therefore never ran — by construction, not
omission.

Scope of the generalization, stated precisely: the **ladder** is query-parametric and
the **resume manifest** is fingerprint-locked, but `--paired-ablation` remains q05-only
(`main.rs` still refuses other queries) and gate zero still calls the q05-specific
oracle. A future committing non-q05 query gets a ladder rung for free; its query-shaped
gate-zero oracle and cohort unlock are Rail 2/3 work that was correctly deferred and
remains to be built.
