# ADR 0167 exhibits

Produced 2026-09-12 on the Windows desktop through `polariton act` against
the Bigfoot lab instance (Wendelon node 1, Toron worker for tenant 1), after
`nilket wendelon tenant create --tenant 1` and `nilket toron sql --file
uat/plot_trends_showcase.sql`.

- `receipt_before2.ndjson`: connect, probe read and write, before the tenant
  existed (`tenant_unavailable(8)`, `storage unavailable`).
- `receipt_after-probe.ndjson`, `receipt_after-rows.ndjson`: the same window
  and connection after the create and the seed; the cells apply.
- `receipt_trends.ndjson`, `receipt_publish.ndjson`: the plotting notebook's
  connect, two cells, describe, resize, and publish.
- `manifest.json`, `portfolio.arrow`: the published package's manifest and
  its dataset (producer Toron, 8 rows); `portfolio_decoded.txt` is the
  Arrow stream read back with `arrow` 58 (`StreamReader`), and equals the
  weekly sums of the seed's three series.
- `portfolio_growth.svg`: the published figure.
