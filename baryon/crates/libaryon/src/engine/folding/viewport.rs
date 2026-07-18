use crate::core::{DocLine, DocLineCount, DocVisualRow, VisualRowCount};
use crate::ecs::{NodeId, UastRegistry};
use crate::uast::FoldPolicy;

pub(crate) fn visual_line_index_for_doc_line<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	target_line: DocLine,
) -> DocVisualRow {
	let mut visual_index = DocVisualRow::ZERO;
	let mut line_accumulator = DocLine::ZERO;
	let mut visit = Some(root);

	while let Some(node) = visit {
		let idx = node.index();
		let has_children = unsafe { (*registry.edges[idx].get()).first_child.is_some() };
		let is_folded = has_children && folds.is_folded(node);
		if has_children && !is_folded {
			visit = registry.get_first_child(node);
			continue;
		}

		let metrics = unsafe { &*registry.metrics[idx].get() };
		let node_end_line = line_accumulator + DocLineCount::new(metrics.newlines);
		let contains_target_line = target_line < node_end_line
			|| (target_line == node_end_line
				&& (!is_folded || registry.next_node_skipping_children(node).is_none()));
		if contains_target_line {
			if is_folded {
				return visual_index;
			}
			return visual_index + VisualRowCount::new((target_line - line_accumulator).get());
		}

		if is_folded {
			visual_index += VisualRowCount::new(1);
		} else {
			visual_index += VisualRowCount::new(metrics.newlines);
		}
		line_accumulator = node_end_line;
		visit = registry.next_node_skipping_children(node);
	}

	visual_index
}

pub(crate) fn max_visual_line_index<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	max_doc_line: DocLine,
) -> DocVisualRow {
	visual_line_index_for_doc_line(registry, root, folds, max_doc_line)
}

pub(crate) fn doc_line_for_visual_index<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	target_visual_index: DocVisualRow,
	max_doc_line: DocLine,
) -> DocLine {
	let mut visual_index = DocVisualRow::ZERO;
	let mut line_accumulator = DocLine::ZERO;
	let mut visit = Some(root);

	while let Some(node) = visit {
		let idx = node.index();
		let has_children = unsafe { (*registry.edges[idx].get()).first_child.is_some() };
		let is_folded = has_children && folds.is_folded(node);
		if has_children && !is_folded {
			visit = registry.get_first_child(node);
			continue;
		}

		let metrics = unsafe { &*registry.metrics[idx].get() };
		if is_folded {
			if visual_index >= target_visual_index {
				return line_accumulator;
			}
			visual_index += VisualRowCount::new(1);
		} else {
			let next_visual = visual_index + VisualRowCount::new(metrics.newlines);
			if target_visual_index <= next_visual {
				return line_accumulator
					+ DocLineCount::new((target_visual_index - visual_index).get());
			}
			visual_index = next_visual;
		}

		line_accumulator += DocLineCount::new(metrics.newlines);
		visit = registry.next_node_skipping_children(node);
	}

	max_doc_line
}

pub(crate) fn snap_line_to_visible_boundary<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	line: DocLine,
) -> DocLine {
	let visual_index = visual_line_index_for_doc_line(registry, root, folds, line);
	let max_doc_line = DocLine::new(registry.get_total_newlines(root));
	doc_line_for_visual_index(registry, root, folds, visual_index, max_doc_line)
}

pub(crate) fn pan_scroll_y_to_keep_cursor_visible<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	scroll_y: u64,
	cursor_line: DocLine,
	max_doc_line: DocLine,
	viewport_lines: u32,
) -> u64 {
	let viewport_lines = viewport_lines.max(1);
	let viewport_rows = VisualRowCount::new(u64::from(viewport_lines.saturating_sub(1)));
	let max_visual = max_visual_line_index(registry, root, folds, max_doc_line);
	let mut top_visual = visual_line_index_for_doc_line(
		registry,
		root,
		folds,
		DocLine::new(scroll_y).min(max_doc_line),
	);
	let cursor_visual = visual_line_index_for_doc_line(registry, root, folds, cursor_line);

	if cursor_visual < top_visual {
		top_visual = cursor_visual;
	} else {
		let bottom_visual = top_visual + viewport_rows;
		if cursor_visual > bottom_visual {
			top_visual = cursor_visual - viewport_rows;
		}
	}

	doc_line_for_visual_index(
		registry,
		root,
		folds,
		top_visual.min(max_visual),
		max_doc_line,
	)
	.get()
}

pub(crate) fn scroll_viewport<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	scroll_y: u64,
	delta: i32,
	max_doc_line: DocLine,
	_viewport_lines: u32,
) -> u64 {
	let top_visual = visual_line_index_for_doc_line(
		registry,
		root,
		folds,
		DocLine::new(scroll_y).min(max_doc_line),
	);
	let max_visual = max_visual_line_index(registry, root, folds, max_doc_line);
	let new_visual = DocVisualRow::new(
		(i128::from(top_visual.get()) + i128::from(delta)).clamp(0, i128::from(max_visual.get()))
			as u64,
	);
	doc_line_for_visual_index(registry, root, folds, new_visual, max_doc_line).get()
}

pub(crate) fn clamp_cursor_line_to_viewport<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	cursor_line: DocLine,
	scroll_y: u64,
	max_doc_line: DocLine,
	viewport_lines: u32,
) -> DocLine {
	let viewport_lines = viewport_lines.max(1);
	let viewport_rows = VisualRowCount::new(u64::from(viewport_lines.saturating_sub(1)));
	let top_visual = visual_line_index_for_doc_line(
		registry,
		root,
		folds,
		DocLine::new(scroll_y).min(max_doc_line),
	);
	let max_visual = max_visual_line_index(registry, root, folds, max_doc_line);
	let bottom_visual = (top_visual + viewport_rows).min(max_visual);
	let cursor_visual = visual_line_index_for_doc_line(registry, root, folds, cursor_line);
	if cursor_visual < top_visual {
		doc_line_for_visual_index(registry, root, folds, top_visual, max_doc_line)
	} else if cursor_visual > bottom_visual {
		doc_line_for_visual_index(registry, root, folds, bottom_visual, max_doc_line)
	} else {
		cursor_line
	}
}

#[cfg(test)]
mod tests {
	use super::{
		clamp_cursor_line_to_viewport, doc_line_for_visual_index,
		pan_scroll_y_to_keep_cursor_visible, scroll_viewport, visual_line_index_for_doc_line,
	};
	use crate::core::{DocLine, DocVisualRow};
	use crate::engine::folding::FoldSet;
	use crate::engine::test_support::{build_foldable_document, build_line_document};

	#[test]
	fn pan_scroll_y_keeps_visible_cursor_stationary() {
		let (registry, root) = build_line_document(200);
		assert_eq!(
			pan_scroll_y_to_keep_cursor_visible(
				&registry,
				root,
				&crate::uast::NoFolds,
				10,
				DocLine::new(20),
				DocLine::new(200),
				50,
			),
			10
		);
	}

	#[test]
	fn pan_scroll_y_moves_up_when_cursor_above_view() {
		let (registry, root) = build_line_document(200);
		assert_eq!(
			pan_scroll_y_to_keep_cursor_visible(
				&registry,
				root,
				&crate::uast::NoFolds,
				30,
				DocLine::new(12),
				DocLine::new(200),
				50,
			),
			12
		);
	}

	#[test]
	fn pan_scroll_y_moves_down_when_cursor_below_view() {
		let (registry, root) = build_line_document(200);
		assert_eq!(
			pan_scroll_y_to_keep_cursor_visible(
				&registry,
				root,
				&crate::uast::NoFolds,
				10,
				DocLine::new(65),
				DocLine::new(200),
				20,
			),
			46
		);
	}

	#[test]
	fn pan_scroll_y_keeps_cursor_on_last_visible_line_stationary() {
		let (registry, root) = build_line_document(200);
		assert_eq!(
			pan_scroll_y_to_keep_cursor_visible(
				&registry,
				root,
				&crate::uast::NoFolds,
				10,
				DocLine::new(29),
				DocLine::new(200),
				20,
			),
			10
		);
	}

	#[test]
	fn scroll_viewport_clamps_to_document_bounds() {
		let (registry, root) = build_line_document(100);
		assert_eq!(
			scroll_viewport(
				&registry,
				root,
				&crate::uast::NoFolds,
				0,
				-3,
				DocLine::new(99),
				50,
			),
			0
		);
		assert_eq!(
			scroll_viewport(
				&registry,
				root,
				&crate::uast::NoFolds,
				45,
				10,
				DocLine::new(99),
				50,
			),
			55
		);
	}

	#[test]
	fn clamp_cursor_line_to_viewport_uses_visible_bounds() {
		let (registry, root) = build_line_document(100);
		assert_eq!(
			clamp_cursor_line_to_viewport(
				&registry,
				root,
				&crate::uast::NoFolds,
				DocLine::new(7),
				10,
				DocLine::new(99),
				20,
			),
			DocLine::new(10),
		);
		assert_eq!(
			clamp_cursor_line_to_viewport(
				&registry,
				root,
				&crate::uast::NoFolds,
				DocLine::new(40),
				10,
				DocLine::new(99),
				20,
			),
			DocLine::new(29),
		);
	}

	#[test]
	fn visual_line_helpers_collapse_folded_subtrees() {
		let (registry, root, folded) = build_foldable_document();
		let mut folds = FoldSet::default();
		folds.insert(folded);

		assert_eq!(
			visual_line_index_for_doc_line(&registry, root, &folds, DocLine::ZERO),
			DocVisualRow::ZERO
		);
		assert_eq!(
			visual_line_index_for_doc_line(&registry, root, &folds, DocLine::new(1)),
			DocVisualRow::new(1)
		);
		assert_eq!(
			visual_line_index_for_doc_line(&registry, root, &folds, DocLine::new(2)),
			DocVisualRow::new(1)
		);
		assert_eq!(
			visual_line_index_for_doc_line(&registry, root, &folds, DocLine::new(3)),
			DocVisualRow::new(2)
		);

		assert_eq!(
			doc_line_for_visual_index(
				&registry,
				root,
				&folds,
				DocVisualRow::new(2),
				DocLine::new(registry.get_total_newlines(root)),
			),
			DocLine::new(3)
		);
	}

	#[test]
	fn pan_scroll_y_uses_folded_visual_rows() {
		let (registry, root, folded) = build_foldable_document();
		let mut folds = FoldSet::default();
		folds.insert(folded);

		assert_eq!(
			pan_scroll_y_to_keep_cursor_visible(
				&registry,
				root,
				&folds,
				0,
				DocLine::new(3),
				DocLine::new(3),
				2,
			),
			1
		);
	}
}
