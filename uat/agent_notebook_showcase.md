# An analysis an agent can drive

The SQL cell runs on the active connection (the local scratch database when
none is attached). The Rune cell plots its named result. Inspect this
notebook through `describe_notebook`, then use the same cell actions to
open your data, change the query, run the cells, and publish the figure.

```sql {#data, output=none}
SELECT 1 AS x, 3 AS y UNION ALL SELECT 2, 6 UNION ALL SELECT 3, 4;
```

```rune {#plot, output=pane}
viz.plot(workspace.cell("data")?).scatter().line().show()?;
```
