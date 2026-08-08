# Plotting-language architectural review

Produced 2026-08-06, after PRODUCT-0040, as a full-stop synthesis and audit of
the plotting arc (PRODUCT-0007, 0008, 0010–0040). Everything here is derived
from the records; nothing is proposed. Four documents:

- **`language_as_built.md`** — the language stated backward: the three-artifact
  plot model, origin-keyed execution, the four-level tree, the three attachment
  points and their closed registers, the settings cascade, the color system,
  axes and ticks, the refusal doctrine, and the method patterns the records
  themselves practice. Correctness test: contains no decision that is not
  already in a record.
- **`consistency_audit.md`** — a behavior matrix across the three artifact
  kinds (table skins, plot chrome, plot data), eight ranked candidate seams
  where records disagree without acknowledging it, two record-accuracy defects,
  four unspecified cells, and fifteen acknowledged asymmetries checked and set
  aside.
- **`deferred_ledger.md`** — every forward promise collected: 58 open items in
  six areas, with all deferring citations and stated dependencies, plus 18
  since-landed items after PRODUCT-0058 closed DB-03.
- **`live_bridges.md`** — the 8 behaviors shipped as explicitly provisional or
  documented-not-endorsed and still standing, the 2 closed classes, and the
  borderline exclusions with reasons.
- **`deferred_triage.md`** — the ledger's original 59 open items seeded into
  will do / if asked / closed with reasons and closure targets, the Guide and
  persistence clusters triaged as one decision each, facets held out.

Review outcomes to date are in `consistency_audit.md` §4: S5 dissolved, S1
resolved as documentation with an identity/capability split, S2+S3 agreed as
the one capability gap (table-skin cascade record), S4's data-column criterion
measured and failed (default viridis 1.378:1 at position 0 on black), S6
repaired by restatement, and the four record-accuracy fixes applied to
0030/0035/0036/0040.
