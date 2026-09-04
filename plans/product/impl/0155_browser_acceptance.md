# PRODUCT-0155 gates 5 and 8: browser acceptance

Recorded 2026-09-03 from the UAT environment, Chromium 152 on Linux/X11,
against the local implementation commit of the record.

| gate | observed |
|---|---|
| 8 · prose | tooltip prose matched the packaged desktop prose exactly for scatter, bubble, box plot, and heatmap |
| 8 · deferred | every deferred family published without a projection, exactly as its manifest entry states |
| 6 · hostile | a label carrying `</script><script>…</script> & <` rendered as text; one executable script element; no injected script node |
| 5 · workload | maximal structurally admissible browser fixture using a real exported hit and the shipped `inspect.js`: 18,070 regions at 1,048,538 bytes; one additional region encoded to 1,048,596 bytes, crossing the 1 MiB package bound (publisher omission is proven by gate 4's test, not by this fixture) |
| 5 · timing | over 200 pointer events after warm-up on that projection: median 0.20 ms, p95 0.30 ms, maximum 2.10 ms, 36,139 counted operations, correct tooltip output |
| visual | tooltip renders and positions correctly; subtitle and label clipping remain plotting-arc work |

The timing row is evidence for later records, not a CI gate; the operation
count is the CI-gated quantity and sits under the 65,536 bound.
