//! PRODUCT-0162: attributed notebook snapshots; the engine owns fence parsing.
use super::*;
use crate::driver::{CellRef, NotebookCell, NotebookDescription};
use libaryon::core::{BufferId, StateId};

pub(crate) struct CellSnapshot {
	pub buffer: BufferId,
	pub generation: StateId,
	pub cell: MarkdownCellExecution,
	pub edit: libaryon::engine::DocumentTextEdit,
}
impl CodeEditor {
	pub(crate) fn notebook_description(
		&self,
		wanted: Option<&str>,
	) -> Result<NotebookDescription, String> {
		let viewport = self.viewport.as_ref().ok_or("editor has no projection")?;
		let mut buffers = viewport
			.panes
			.iter()
			.filter(|p| !p.markdown_cells.is_empty())
			.map(|p| p.buffer_id)
			.collect::<Vec<_>>();
		buffers.sort();
		buffers.dedup();
		if let Some(wanted) = wanted {
			buffers.retain(|b| b.get().to_string() == wanted);
		}
		if buffers.len() != 1 {
			return Err(if buffers.is_empty() {
				"notebook buffer unavailable"
			} else {
				"multiple notebooks: specify a buffer"
			}
			.into());
		}
		let buffer = buffers[0];
		let (text, generation) = self
			.document_snapshot(buffer)
			.ok_or("notebook text not yet projected")?;
		let projected = libaryon::engine::project_notebook_cells(text);
		let mut names = std::collections::HashSet::new();
		let mut total = 0;
		let mut cells = Vec::new();
		for (index, cell) in projected.iter().enumerate() {
			let name = match &cell.identity {
				MarkdownCellIdentity::Explicit { id } => {
					if !names.insert(id.clone()) {
						return Err("duplicate notebook cell identity".into());
					}
					Some(id.clone())
				}
				_ => None,
			};
			let source = text
				.get(cell.body_range.start.get() as usize..cell.body_range.end.get() as usize)
				.ok_or("invalid notebook cell range")?;
			total += source.len();
			if total > 32 * 1024 {
				return Err("notebook cell bodies exceed 32 KiB".into());
			}
			cells.push(NotebookCell {
				cell: index as u32,
				name,
				language: cell.language_id.clone(),
				source: source.into(),
			});
		}
		Ok(NotebookDescription {
			buffer: buffer.get().to_string(),
			generation: generation.get().to_string(),
			cells,
		})
	}
	pub(crate) fn acknowledge_notebook_edit(
		&mut self,
		target: &CellRef,
		source: &str,
		new_generation: StateId,
	) {
		let Ok(snapshot) = self.notebook_cell_snapshot(target) else {
			return;
		};
		// Cache only a transaction the engine acknowledged, and never overwrite
		// a newer projection which may already have crossed the other channel.
		if self.document_state_ids.get(&snapshot.buffer) != Some(&snapshot.generation) {
			return;
		}
		if let Some(text) = self.document_texts.get_mut(&snapshot.buffer) {
			text.replace_range(
				snapshot.edit.start.get() as usize..snapshot.edit.end.get() as usize,
				source,
			);
			self.document_state_ids
				.insert(snapshot.buffer, new_generation);
		}
	}
	pub(crate) fn notebook_cell_snapshot(&self, target: &CellRef) -> Result<CellSnapshot, String> {
		let description = self.notebook_description(Some(&target.buffer))?;
		if description.generation != target.generation {
			return Err("notebook source changed".into());
		}
		let buffer = BufferId::new(target.buffer.parse().map_err(|_| "invalid buffer")?);
		let (text, generation) = self
			.document_snapshot(buffer)
			.ok_or("notebook source unavailable")?;
		let mut cells = libaryon::engine::project_notebook_cells(text);
		if target.cell as usize >= cells.len() {
			return Err("notebook cell unavailable".into());
		}
		let cell = cells.remove(target.cell as usize);
		let start = cell.body_range.start.get() as usize;
		let end = cell.body_range.end.get() as usize;
		Ok(CellSnapshot {
			buffer,
			generation,
			edit: libaryon::engine::DocumentTextEdit {
				start: cell.body_range.start,
				end: cell.body_range.end,
				replacement: String::new(),
			},
			cell: MarkdownCellExecution {
				identity: cell.identity,
				presentation: cell.presentation,
				language_id: cell.language_id,
				code: text[start..end].into(),
				source_start_line: text[..start].bytes().filter(|b| *b == b'\n').count() + 1,
			},
		})
	}
}
