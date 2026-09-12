# ADR 0169 exhibits

Produced 2026-09-12 on the Windows desktop:
`polariton drive --execute by-command uat/plot_trends_showcase.rn --commands commands.json --receipt receipt.json`
against the Bigfoot lab tenant 1 seeded under ADR 0167. The script is connect
(mTLS, the Windows tenant-1 identity), run_source, describe, publish. The receipt
states `execution: by-command`, `loaded: []` (nothing ran on open), the
execution identity (buffer 2, generation 1, connection generation 1, id 1), and
four applied commands; the manifest names producer Toron and 24 rows; the four
SVGs are the showcase views as published.
