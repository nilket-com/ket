use super::session::{
	CursorState, FileBrowserBuffer, FileBrowserEditorMut, FileBrowserRow, FileBrowserRowKind,
	FileBrowserState,
};
use crate::core::{DisplayRow, DocDisplayCol, DocLine, StateId, TopologyRev};
use crate::ecs::{NodeId, UastRegistry};
use crate::uast::kind::SemanticKind;
use crate::uast::metrics::SpanMetrics;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

pub(crate) enum BrowserEntryAction {
	None,
	OpenFile(PathBuf),
}

pub(crate) fn create_file_browser_buffer(
	registry: &UastRegistry,
	root_path: PathBuf,
) -> Result<(FileBrowserBuffer, NodeId), String> {
	let expanded_dirs = HashSet::new();
	let visible_rows = build_visible_rows(&root_path, &expanded_dirs)?;
	let mut chunk = registry
		.reserve_chunk(2)
		.ok_or_else(|| "Not enough document-node capacity for file browser".to_string())?;
	let root = chunk.spawn_node(
		SemanticKind::RelationalTable,
		None,
		SpanMetrics {
			byte_length: 0,
			newlines: 0,
		},
	);
	let leaf = chunk.spawn_node(
		SemanticKind::Token,
		None,
		SpanMetrics {
			byte_length: 0,
			newlines: 0,
		},
	);
	chunk.append_local_child(root, leaf);
	unsafe {
		*registry.virtual_data[leaf.index()].get() = Some(Vec::new());
	}
	Ok((
		FileBrowserBuffer {
			root_id: root,
			topology_rev: TopologyRev::ZERO,
			file_path: root_path.to_string_lossy().into_owned(),
			state: FileBrowserState {
				root_path,
				expanded_dirs,
				visible_rows,
			},
			cached_sum_tree: None,
		},
		leaf,
	))
}

pub(crate) fn initialize_file_browser(
	registry: &UastRegistry,
	tab: &mut FileBrowserEditorMut<'_>,
	root_path: PathBuf,
) -> Result<(), String> {
	let expanded_dirs = HashSet::new();
	let rows = build_visible_rows(&root_path, &expanded_dirs)?;

	tab.document.state = FileBrowserState {
		root_path: root_path.clone(),
		expanded_dirs,
		visible_rows: rows,
	};
	tab.document.file_path = root_path.to_string_lossy().into_owned();
	tab.cursor.collapse();
	tab.cursor.set_active_visual(None);
	tab.view.scroll_y = 0;
	tab.view.scroll_row_offset = DisplayRow::ZERO;
	tab.document.cached_sum_tree = None;
	tab.view.common.reset_minimap_cache();

	rebuild_file_browser_document(registry, tab, None)
}

pub(crate) fn refresh_file_browser(
	registry: &UastRegistry,
	tab: &mut FileBrowserEditorMut<'_>,
) -> Result<(), String> {
	let preserve_path = selected_browser_row(tab.document, tab.cursor).map(|row| row.path.clone());
	rebuild_file_browser_document(registry, tab, preserve_path)
}

pub(crate) fn activate_selected_browser_entry(
	registry: &UastRegistry,
	tab: &mut FileBrowserEditorMut<'_>,
) -> Result<BrowserEntryAction, String> {
	let Some(row) = selected_browser_row(tab.document, tab.cursor).cloned() else {
		return Ok(BrowserEntryAction::None);
	};

	match row.kind {
		FileBrowserRowKind::Directory { expanded } => {
			let state = &mut tab.document.state;
			if expanded {
				state.expanded_dirs.remove(&row.path);
			} else {
				state.expanded_dirs.insert(row.path.clone());
			}
			rebuild_file_browser_document(registry, tab, Some(row.path))?;
			Ok(BrowserEntryAction::None)
		}
		FileBrowserRowKind::File => Ok(BrowserEntryAction::OpenFile(row.path)),
	}
}

pub(crate) fn click_browser_entry(
	registry: &UastRegistry,
	tab: &mut FileBrowserEditorMut<'_>,
	line: DocLine,
	double_click: bool,
) -> Result<BrowserEntryAction, String> {
	let Ok(index) = usize::try_from(line.get()) else {
		return Ok(BrowserEntryAction::None);
	};
	let Some(row) = tab.document.state.visible_rows.get(index).cloned() else {
		return Ok(BrowserEntryAction::None);
	};

	let root = tab.document.root_id;
	let topology_rev = tab.document.topology_rev;
	let state_id = StateId::ZERO;
	tab.cursor.primary_mut().place_at_line_col(
		registry,
		root,
		&mut tab.view.folds,
		topology_rev,
		state_id,
		line,
		DocDisplayCol::ZERO,
		false,
		super::session::Affinity::After,
	);
	tab.cursor.sync_preferred_col();

	match (row.kind, double_click) {
		(FileBrowserRowKind::Directory { .. }, false) => {
			activate_selected_browser_entry(registry, tab)
		}
		(FileBrowserRowKind::File, true) => Ok(BrowserEntryAction::OpenFile(row.path)),
		(FileBrowserRowKind::Directory { .. }, true) | (FileBrowserRowKind::File, false) => {
			Ok(BrowserEntryAction::None)
		}
	}
}

pub(crate) fn collapse_selected_browser_entry_or_parent(
	registry: &UastRegistry,
	tab: &mut FileBrowserEditorMut<'_>,
) -> Result<bool, String> {
	let Some(index) = selected_browser_row_index(tab.document, tab.cursor) else {
		return Ok(false);
	};
	let rows = &tab.document.state.visible_rows;
	let Some(current) = rows.get(index) else {
		return Ok(false);
	};

	let target = match current.kind {
		FileBrowserRowKind::Directory { expanded: true } => Some(current.path.clone()),
		_ => rows[..=index].iter().rev().find_map(|row| match row.kind {
			FileBrowserRowKind::Directory { expanded: true }
				if row.path != current.path && current.path.starts_with(&row.path) =>
			{
				Some(row.path.clone())
			}
			_ => None,
		}),
	};

	let Some(target) = target else {
		return Ok(false);
	};

	tab.document.state.expanded_dirs.remove(&target);
	rebuild_file_browser_document(registry, tab, Some(target))?;
	Ok(true)
}

fn selected_browser_row<'a>(
	document: &'a FileBrowserBuffer,
	cursor: &CursorState,
) -> Option<&'a FileBrowserRow> {
	let index = selected_browser_row_index(document, cursor)?;
	document.state.visible_rows.get(index)
}

fn selected_browser_row_index(document: &FileBrowserBuffer, cursor: &CursorState) -> Option<usize> {
	let rows = &document.state.visible_rows;
	if rows.is_empty() {
		return None;
	}
	Some((cursor.abs_line().get() as usize).min(rows.len().saturating_sub(1)))
}

fn rebuild_file_browser_document(
	registry: &UastRegistry,
	tab: &mut FileBrowserEditorMut<'_>,
	preserve_path: Option<PathBuf>,
) -> Result<(), String> {
	let (root_path, expanded_dirs) = {
		let state = &tab.document.state;
		(state.root_path.clone(), state.expanded_dirs.clone())
	};

	let rows = build_visible_rows(&root_path, &expanded_dirs)?;
	let text = render_rows(&rows);
	let target_line = preserve_path
		.and_then(|path| rows.iter().position(|row| row.path == path))
		.unwrap_or_else(|| {
			selected_browser_row_index(tab.document, tab.cursor)
				.unwrap_or(0)
				.min(rows.len().saturating_sub(1))
		});

	tab.document.bump_topology_rev();
	// Rebuilding the browser replaces its synthetic content without a stable
	// byte-range mapping, so no closed fold can be carried across it.
	tab.view.folds.clear();
	let root = tab.document.root_id;
	let leaf = registry
		.get_first_child(root)
		.expect("file browser buffer always has its synthetic leaf");
	update_browser_document_text(registry, root, leaf, &text);

	tab.document.state.visible_rows = rows;

	tab.document.cached_sum_tree = None;
	tab.view.common.reset_minimap_cache();
	tab.cursor.collapse();
	tab.cursor.set_active_visual(None);

	if text.is_empty() {
		tab.cursor
			.primary_mut()
			.reset_to_start(leaf, tab.document.topology_rev, StateId::ZERO);
		tab.view.scroll_y = 0;
		tab.view.scroll_row_offset = DisplayRow::ZERO;
		return Ok(());
	}

	let topology_rev = tab.document.topology_rev;
	let state_id = StateId::ZERO;
	tab.cursor.primary_mut().place_at_line_col(
		registry,
		root,
		&mut tab.view.folds,
		topology_rev,
		state_id,
		DocLine::new(target_line as u64),
		DocDisplayCol::ZERO,
		false,
		super::session::Affinity::After,
	);
	tab.cursor.sync_preferred_col();
	Ok(())
}

fn update_browser_document_text(registry: &UastRegistry, root: NodeId, leaf: NodeId, text: &str) {
	let bytes = text.as_bytes();
	let newlines = count_newlines(bytes);

	unsafe {
		*registry.metrics[root.index()].get() = SpanMetrics {
			byte_length: bytes.len() as u64,
			newlines,
		};
		*registry.metrics[leaf.index()].get() = SpanMetrics {
			byte_length: bytes.len() as u64,
			newlines,
		};
		*registry.spans[leaf.index()].get() = None;
		*registry.virtual_data[leaf.index()].get() = Some(bytes.to_vec());
	}
}

fn count_newlines(bytes: &[u8]) -> u64 {
	bytes.iter().filter(|&&byte| byte == b'\n').count() as u64
}

fn build_visible_rows(
	root_path: &Path,
	expanded_dirs: &HashSet<PathBuf>,
) -> Result<Vec<FileBrowserRow>, String> {
	let mut rows = Vec::new();
	scan_directory(root_path, 0, expanded_dirs, &mut rows)?;
	Ok(rows)
}

fn scan_directory(
	dir: &Path,
	depth: u16,
	expanded_dirs: &HashSet<PathBuf>,
	rows: &mut Vec<FileBrowserRow>,
) -> Result<(), String> {
	let mut entries = std::fs::read_dir(dir)
		.map_err(|err| format!("File browser error: {}: {}", dir.display(), err))?
		.collect::<Result<Vec<_>, _>>()
		.map_err(|err| format!("File browser error: {}: {}", dir.display(), err))?;

	entries.sort_by(|a, b| {
		let a_is_dir = a.file_type().map(|t| t.is_dir()).unwrap_or(false);
		let b_is_dir = b.file_type().map(|t| t.is_dir()).unwrap_or(false);
		b_is_dir.cmp(&a_is_dir).then_with(|| {
			a.file_name()
				.to_string_lossy()
				.cmp(&b.file_name().to_string_lossy())
		})
	});

	for entry in entries {
		let name = entry.file_name();
		if matches!(name.to_str(), Some("." | "..")) {
			continue;
		}

		let path = entry.path();
		let is_dir = entry
			.file_type()
			.map_err(|err| format!("File browser error: {}: {}", path.display(), err))?
			.is_dir();
		if is_dir {
			let expanded = expanded_dirs.contains(&path);
			rows.push(FileBrowserRow {
				path: path.clone(),
				depth,
				kind: FileBrowserRowKind::Directory { expanded },
			});
			if expanded {
				scan_directory(&path, depth.saturating_add(1), expanded_dirs, rows)?;
			}
		} else {
			rows.push(FileBrowserRow {
				path,
				depth,
				kind: FileBrowserRowKind::File,
			});
		}
	}

	Ok(())
}

fn render_rows(rows: &[FileBrowserRow]) -> String {
	let mut out = String::new();
	for (index, row) in rows.iter().enumerate() {
		if index > 0 {
			out.push('\n');
		}
		for _ in 0..row.depth {
			out.push_str("  ");
		}
		match row.kind {
			FileBrowserRowKind::Directory { expanded } => {
				out.push_str(if expanded { "▼ " } else { "▶ " });
				out.push_str(&display_name(&row.path));
				out.push('/');
			}
			FileBrowserRowKind::File => {
				out.push_str("  ");
				out.push_str(&display_name(&row.path));
			}
		}
	}
	out
}

fn display_name(path: &Path) -> String {
	path.file_name()
		.unwrap_or(path.as_os_str())
		.to_string_lossy()
		.into_owned()
}

#[cfg(test)]
mod tests {
	use super::*;

	fn temp_dir(name: &str) -> PathBuf {
		let nanos = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.expect("time should move forward")
			.as_nanos();
		std::env::temp_dir().join(format!(
			"baryon-file-browser-{}-{}-{}",
			name,
			std::process::id(),
			nanos
		))
	}

	#[test]
	fn rows_sort_directories_before_files() {
		let root = temp_dir("sort");
		std::fs::create_dir_all(root.join("src")).expect("create src dir");
		std::fs::write(root.join("README.md"), "").expect("write readme");
		std::fs::write(root.join("Cargo.toml"), "").expect("write cargo");

		let rows = build_visible_rows(&root, &HashSet::new()).expect("build rows");
		let names: Vec<String> = rows.iter().map(|row| display_name(&row.path)).collect();
		assert_eq!(names, vec!["src", "Cargo.toml", "README.md"]);

		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn rows_expand_nested_directories() {
		let root = temp_dir("expand");
		let src = root.join("src");
		let engine = src.join("engine");
		std::fs::create_dir_all(&engine).expect("create nested dirs");
		std::fs::write(src.join("main.rs"), "").expect("write main");
		std::fs::write(engine.join("render.rs"), "").expect("write render");

		let mut expanded = HashSet::new();
		expanded.insert(src.clone());
		let rows = build_visible_rows(&root, &expanded).expect("build expanded rows");
		assert_eq!(rows.len(), 3);
		assert_eq!(rows[0].path, src);
		assert_eq!(rows[1].path, engine);
		assert_eq!(rows[2].path, root.join("src/main.rs"));

		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn rendered_text_has_one_line_per_row() {
		let rows = vec![
			FileBrowserRow {
				path: PathBuf::from("/tmp/src"),
				depth: 0,
				kind: FileBrowserRowKind::Directory { expanded: true },
			},
			FileBrowserRow {
				path: PathBuf::from("/tmp/src/main.rs"),
				depth: 1,
				kind: FileBrowserRowKind::File,
			},
		];

		let text = render_rows(&rows);
		assert_eq!(text, "▼ src/\n    main.rs");
		assert_eq!(text.lines().count(), rows.len());
	}
}
