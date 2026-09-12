# Native plotting: SQL to pixels

Run these cells in order on the scratch SQLite connection. No external database
is needed. Each Rune cell reads the named SQL result and updates the plot pane.
The render smoke harness runs the same cells and captures each plot at two sizes.

```sql {#trend, output=none}
SELECT 1 AS month, 12 AS revenue
UNION ALL SELECT 2, 18
UNION ALL SELECT 3, 15
UNION ALL SELECT 4, 27
UNION ALL SELECT 5, 24
UNION ALL SELECT 6, 36;
```

```rune {#monthly_revenue, output=pane}
viz.plot(workspace.cell("trend")?).scatter("month", "revenue").line().show()?;
```

```sql {#observations, output=none}
SELECT 10 AS temperature, 18 AS demand
UNION ALL SELECT 12, 22
UNION ALL SELECT 14, 19
UNION ALL SELECT 16, 31
UNION ALL SELECT 18, 27
UNION ALL SELECT 20, 38
UNION ALL SELECT 22, 34
UNION ALL SELECT 24, 47;
```

```rune {#temperature_and_demand, output=pane}
viz.plot(workspace.cell("observations")?).scatter("temperature", "demand").show()?;
```
