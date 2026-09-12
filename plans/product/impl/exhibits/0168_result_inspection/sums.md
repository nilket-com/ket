# ADR 0168 acceptance

```sql {#sums}
SELECT period, SUM(value) AS value FROM trend_showcase GROUP BY period ORDER BY period
```
