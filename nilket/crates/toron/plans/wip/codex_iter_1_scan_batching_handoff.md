# Scan-throughput rerun handoff

**Status:** Toron/Wendelon handoff to viz, 2026-08-21. Untracked.

`0577`, `0578`, and Wendelon `0088` are landed. Please rerun
`live_scatter_threshold_evidence`; its daemon-path rate is now the evidence
gate.

`0088` proves 5,000 Wendelon rows now arrive as two batches, but it did not
improve the in-process read rate (4,638 -> 4,505 rows/s). If the full path
rises toward approximately 4,500 rows/s, per-frame daemon overhead was
material. If it stays near approximately 460 rows/s, profile per-location
storage materialization and PBC before choosing a `SingleRow`/`MultiRow`
direction.
