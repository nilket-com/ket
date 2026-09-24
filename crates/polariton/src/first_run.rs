//! PRODUCT-0161: the offline welcome and a path to the person's own data.
use std::path::{Path, PathBuf};

pub(crate) const EXAMPLE: &str = r#"# Welcome to Polariton

This is a working notebook. The SQL starts on your local scratch database.
Opening your data connects the file and changes the SQL below to read it.
The plot is built here on your computer. No account, network, or workspace is needed.

```sql {#first-data, output=none}
SELECT 1 AS x, 12 AS y UNION ALL SELECT 2, 18 UNION ALL SELECT 3, 15 UNION ALL SELECT 4, 27;
```

```rune {#first-plot, output=pane}
viz.plot(workspace.cell("first-data")?).scatter().line().show()?;
```

## Try your own data

Use **Open my data** below with a Parquet or CSV file. We start with its first two
numeric columns. Edit the SQL to choose other columns, place the cursor in the
cell and press Run; run the plot cell again after changing the query.
Make a deliberate change, then use **Plot → Publish** to keep your figure.

## Explore Rune views

Rune relations run in a Toron workspace. **Start workspace** is optional; the
notebook above keeps working if no workspace is available.

## Reporting

Reporting is off by default. This example and your analysis work without it.
The notice below shows this run's setting. Configuration is read at startup.
If enabled, reports contain app and platform versions, step outcomes and times,
and random identifiers to group runs. They never contain your paths, data or queries.
"#;

#[derive(Default)]
pub(crate) struct Welcome {
	pub active: bool,
	#[cfg(toron_local)]
	pub workspace_requested: bool,
	#[cfg(toron_local)]
	pub workspace_seeded: bool,
	pub path: String,
	pub stage: u8,
	pub marker: Option<PathBuf>,
	pub own_kind: Option<crate::reporting::Reason>,
	pub rendered: bool,
	pub own_data_ready: bool,
	pub changed: bool,
	pub kept: std::sync::Arc<std::sync::atomic::AtomicBool>,
	pub reporting_on: bool,
	pub original_source: String,
}
impl Welcome {
	pub fn eligible(root: &Path, empty_start: bool) -> bool {
		empty_start && !root.join("first-run-shown").exists()
	}
	pub fn mark_shown(&mut self) -> Result<(), String> {
		if let Some(path) = self.marker.take() {
			std::fs::create_dir_all(path.parent().unwrap()).map_err(|e| e.to_string())?;
			match std::fs::OpenOptions::new()
				.write(true)
				.create_new(true)
				.open(path)
			{
				Ok(file) => file.sync_all().map_err(|e| e.to_string())?,
				Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
				Err(e) => return Err(e.to_string()),
			}
		}
		Ok(())
	}
}
pub(crate) fn own_sql(table: &str, x: &str, y: &str) -> String {
	let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
	format!(
		"SELECT {} AS x, {} AS y FROM {};\n",
		quote(x),
		quote(y),
		quote(table)
	)
}

#[cfg(test)]
mod tests {
	use super::*;
	#[test]
	fn product_0161_marker_suppresses_only_empty_starts_after_showing() {
		let root =
			std::env::temp_dir().join(format!("polariton-first-marker-{}", uuid::Uuid::new_v4()));
		assert!(!Welcome::eligible(&root, false));
		assert!(Welcome::eligible(&root, true));
		let mut welcome = Welcome {
			marker: Some(root.join("first-run-shown")),
			..Welcome::default()
		};
		welcome.mark_shown().unwrap();
		welcome.mark_shown().unwrap();
		assert!(!Welcome::eligible(&root, true));
		std::fs::remove_dir_all(root).unwrap();
	}
	#[test]
	fn product_0161_sql_column_names_are_quoted() {
		let notebook = own_sql("data", "a\"b", "value");
		assert!(notebook.contains("\"a\"\"b\" AS x"));
	}
}
