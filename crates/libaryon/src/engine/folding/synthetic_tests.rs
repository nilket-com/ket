use super::super::state::nearest_foldable_boundary;
use super::super::test_support::{build_source_backed_document, doc_line_of, doc_line_of_after};
use super::{
	materialize_fold_boundary_at_cursor, resolve_fold_boundary_at_cursor,
	resolve_fold_boundary_at_cursor_tracked,
};
use crate::core::{DocByte, DocDisplayCol, DocLine, StateId, TopologyRev};
use crate::ecs::UastRegistry;
use crate::engine::core::refresh_sum_tree;
use crate::engine::document::edit::{apply_deltas_to_document, apply_deltas_to_document_internal};
use crate::engine::folding::FoldSet;
use crate::engine::language::GreenNode;
use crate::engine::structural_graft::apply_graft;
use crate::engine::test_support::{build_document, build_grafted_rust_document};
use crate::engine::undo::{TextDelta, UndoLedger, byte_offset_from_line_col};
use crate::uast::UastProjection;
use crate::uast::kind::SemanticKind;
use crate::uast::metrics::SpanMetrics;
use std::fmt::Write as _;

#[test]
fn synthetic_fold_does_not_materialize_inside_topology_group() {
	let parts: &[&[u8]] = &[
		b"const P: u8 = 0;\n",
		b"fn main() {\n    work();\n}\n",
		b"const S: u8 = 1;\n",
	];
	let source = parts.concat();
	let green = GreenNode {
		kind: SemanticKind::RelationalTable,
		provider_kind: crate::uast::ProviderKindId::NONE,
		byte_len: source.len() as u32,
		children: vec![GreenNode {
			kind: SemanticKind::Group,
			provider_kind: crate::uast::ProviderKindId::NONE,
			byte_len: source.len() as u32,
			children: parts
				.iter()
				.map(|part| GreenNode {
					kind: SemanticKind::Token,
					provider_kind: crate::uast::ProviderKindId::NONE,
					byte_len: part.len() as u32,
					children: Vec::new(),
				})
				.collect(),
		}],
	};
	let registry = UastRegistry::new(16);
	let root = registry.alloc_test_node();
	unsafe {
		*registry.kinds[root.index()].get() = SemanticKind::RelationalTable;
		*registry.metrics[root.index()].get() = SpanMetrics {
			byte_length: source.len() as u64,
			newlines: source.iter().filter(|&&byte| byte == b'\n').count() as u64,
		};
	}
	apply_graft(&registry, root, &green, &source).expect("test graft is byte-identical");
	let cursor = registry.find_node_at_line_col(
		root,
		&crate::uast::NoFolds,
		DocLine::new(1),
		DocDisplayCol::ZERO,
	);
	let parent = unsafe { (*registry.edges[cursor.node_id.index()].get()).parent }
		.expect("cursor token parent");
	assert_eq!(
		unsafe { *registry.kinds[parent.index()].get() },
		SemanticKind::Group
	);
	let allocated_before = registry.stats().allocated_nodes;

	assert_eq!(
		materialize_fold_boundary_at_cursor(&registry, root, DocLine::new(1), DocDisplayCol::ZERO,),
		None
	);
	assert_eq!(registry.stats().allocated_nodes, allocated_before);
}

#[test]
fn synthetic_fold_refuses_without_mutating_at_full_capacity() {
	let source = "fn main() {\n\twork();\n}\n";
	let (registry, root) = build_document(source);
	let remaining = registry.remaining_node_capacity();
	let filler = u32::try_from(remaining).expect("fixture capacity fits u32");
	let _filler = registry
		.reserve_chunk(filler)
		.expect("fill registry exactly");
	let before = registry.stats();
	let child_before = registry.get_first_child(root);

	assert_eq!(
		materialize_fold_boundary_at_cursor(
			&registry,
			root,
			DocLine::new(1),
			DocDisplayCol::new(1),
		),
		None
	);
	assert_eq!(registry.stats(), before);
	assert_eq!(registry.get_first_child(root), child_before);
	assert_eq!(
		registry
			.read_loaded_slice(root, DocByte::ZERO, DocByte::new(source.len() as u64))
			.expect("unchanged source"),
		source.as_bytes()
	);
}

#[test]
fn materialize_fold_boundary_works_for_multichunk_source_backed_rust_file() {
	let filler_line = "// filler filler filler filler filler filler filler filler filler\n";
	let filler_count = 3_000usize;
	let mut text = String::with_capacity(filler_line.len() * filler_count + 256);
	for _ in 0..filler_count {
		text.push_str(filler_line);
	}
	text.push('\n');
	let target_line = DocLine::new(filler_count as u64 + 1);
	writeln!(
		&mut text,
		"fn target() {{\n    if cond {{\n        alpha();\n        beta();\n    }}\n}}"
	)
	.expect("append target function");

	let (registry, root, path) = build_source_backed_document(&text);
	let fold =
		materialize_fold_boundary_at_cursor(&registry, root, target_line, DocDisplayCol::ZERO)
			.expect("source-backed multi-chunk function fold should materialize");
	let folded_leaf = registry
		.get_first_child(fold)
		.expect("fold should have one child");
	let folded_text = registry
		.read_node_bytes_sync(folded_leaf)
		.expect("folded text should be readable");
	let folded_text = std::str::from_utf8(&folded_text).expect("folded text should be utf8");
	let trimmed = folded_text.trim_start_matches(['\n', '\r']);
	assert!(
		trimmed.starts_with("fn target() {\n"),
		"expected target function fold, got:\n{folded_text}"
	);
	assert!(folded_text.contains("alpha();"));
	assert!(folded_text.contains("beta();"));

	let _ = std::fs::remove_file(path);
}

#[test]
fn materialize_fold_boundary_builds_nested_block_inside_single_leaf_document() {
	let (registry, root) = build_document(
		"fn main() {\n    if true {\n        alpha();\n        beta();\n    }\n    gamma();\n}\n",
	);

	let fold = materialize_fold_boundary_at_cursor(
		&registry,
		root,
		DocLine::new(2),
		DocDisplayCol::new(8),
	)
	.expect("nested block fold should materialize");

	assert_ne!(fold, root);
	let mut folds = FoldSet::default();
	folds.insert(fold);

	let tokens = registry.query_viewport(root, &folds, DocLine::ZERO, 10);
	assert!(
		tokens
			.iter()
			.any(|token| token.is_folded && token.node_id == fold)
	);
	assert!(tokens.len() > 1);
}

#[test]
fn tracked_fold_resolution_bumps_only_when_materializing_topology() {
	let (registry, root) = build_document(
		"fn main() {\n    if true {\n        alpha();\n        beta();\n    }\n    gamma();\n}\n",
	);
	let line = DocLine::new(2);
	let col = DocDisplayCol::new(8);
	let cursor = registry.find_node_at_line_col(root, &crate::uast::NoFolds, line, col);
	let folds = FoldSet::default();
	let mut topology_rev = TopologyRev::ZERO;

	let fold = resolve_fold_boundary_at_cursor_tracked(
		&registry,
		root,
		&folds,
		cursor.node_id,
		line,
		col,
		true,
		&mut topology_rev,
	)
	.expect("nested block fold should materialize");
	assert_eq!(topology_rev, TopologyRev::new(1));

	let cursor = registry.find_node_at_line_col(root, &crate::uast::NoFolds, line, col);
	assert_eq!(
		resolve_fold_boundary_at_cursor_tracked(
			&registry,
			root,
			&folds,
			cursor.node_id,
			line,
			col,
			true,
			&mut topology_rev,
		),
		Some(fold)
	);
	assert_eq!(topology_rev, TopologyRev::new(1));
}

#[test]
fn collapsed_synthetic_fold_can_be_targeted_for_reopen() {
	let (registry, root) = build_document(
		"fn main() {\n    if true {\n        alpha();\n        beta();\n    }\n    gamma();\n}\n",
	);
	let fold = materialize_fold_boundary_at_cursor(
		&registry,
		root,
		DocLine::new(2),
		DocDisplayCol::new(8),
	)
	.expect("nested block fold should materialize");
	let mut folds = FoldSet::default();
	folds.insert(fold);

	let target = registry.find_node_at_line_col(root, &folds, DocLine::new(1), DocDisplayCol::ZERO);

	assert_eq!(target.node_id, fold);
	assert_eq!(
		nearest_foldable_boundary(&registry, root, target.node_id),
		Some(fold)
	);
}

#[test]
fn resolve_fold_boundary_reuses_existing_folded_rust_boundary() {
	let source = include_str!("../structural_graft.rs");
	let (registry, root) = build_grafted_rust_document(source);

	let fold_line = DocLine::new(
		source
			.lines()
			.position(|line| line.contains("fn graft_real_rust_file_newline_consistency()"))
			.expect("target function line should exist") as u64,
	);
	let cursor_col = DocDisplayCol::new(4);
	let cursor = registry.find_node_at_line_col(root, &crate::uast::NoFolds, fold_line, cursor_col);
	let mut folds = FoldSet::default();
	let fold = resolve_fold_boundary_at_cursor(
		&registry,
		root,
		&folds,
		cursor.node_id,
		fold_line,
		cursor_col,
		true,
	)
	.expect("should resolve a fold boundary for the Rust test function");
	folds.insert(fold);

	let collapsed_cursor = registry.find_node_at_line_col(root, &folds, fold_line, cursor_col);
	assert_eq!(
		nearest_foldable_boundary(&registry, root, collapsed_cursor.node_id),
		Some(fold)
	);

	let reopen_target = resolve_fold_boundary_at_cursor(
		&registry,
		root,
		&folds,
		collapsed_cursor.node_id,
		fold_line,
		cursor_col,
		true,
	)
	.expect("collapsed Rust fold should resolve back to the same boundary");

	assert_eq!(reopen_target, fold);
}

#[test]
fn resolve_fold_boundary_reopens_body_created_rust_fold_from_visible_line() {
	let source = include_str!("../structural_graft.rs");
	let (registry, root) = build_grafted_rust_document(source);

	let body_line = DocLine::new(
		source
			.lines()
			.position(|line| line.contains("let mut result = Vec::new();"))
			.expect("target function body line should exist") as u64,
	);
	let body_col = DocDisplayCol::new(8);
	let body_cursor =
		registry.find_node_at_line_col(root, &crate::uast::NoFolds, body_line, body_col);
	let mut folds = FoldSet::default();
	let fold = resolve_fold_boundary_at_cursor(
		&registry,
		root,
		&folds,
		body_cursor.node_id,
		body_line,
		body_col,
		true,
	)
	.expect("should resolve a fold boundary from inside the function body");
	folds.insert(fold);

	let visible_line = registry
		.query_viewport(root, &folds, body_line.saturating_sub(1), 3)
		.into_iter()
		.find(|token| token.is_folded && token.node_id == fold)
		.map(|token| token.absolute_start_line)
		.expect("folded Rust function should remain visible on its marker line");
	let visible_cursor =
		registry.find_node_at_line_col(root, &folds, visible_line, DocDisplayCol::ZERO);

	let reopen_target = resolve_fold_boundary_at_cursor(
		&registry,
		root,
		&folds,
		visible_cursor.node_id,
		visible_line,
		DocDisplayCol::ZERO,
		true,
	)
	.expect("visible marker line should resolve back to the folded boundary");

	assert_eq!(reopen_target, fold);
}

#[test]
fn materialize_fold_boundary_prefers_function_inside_impl_over_parent_impl() {
	let (registry, root) = build_document(
		"impl UastRegistry {\n    fn alpha(&self) {\n        first();\n    }\n\n    fn beta(&self) {\n        second();\n        third();\n    }\n\n    fn gamma(&self) {\n        fourth();\n    }\n}\n",
	);

	let fold = materialize_fold_boundary_at_cursor(
		&registry,
		root,
		DocLine::new(6),
		DocDisplayCol::new(8),
	)
	.expect("method fold should materialize");
	let folded_leaf = registry
		.get_first_child(fold)
		.expect("synthetic fold should have one child");
	let folded_text = registry
		.read_node_bytes_sync(folded_leaf)
		.expect("folded text should be readable");

	assert_eq!(
		std::str::from_utf8(&folded_text).expect("folded utf8"),
		"    fn beta(&self) {\n        second();\n        third();\n    }\n",
	);
}

#[test]
fn materialize_fold_boundary_prefers_function_inside_trait_impl_over_parent_impl() {
	let (registry, root) = build_document(
		"impl UastProjection for UastRegistry {\n    fn query_viewport(\n        &self,\n        root: NodeId,\n        target_line: DocLine,\n        line_count: u32,\n    ) -> Vec<RenderToken> {\n        let mut current_global_byte = DocByte::ZERO;\n        current_global_byte = current_global_byte.saturating_add(line_count as u64);\n        Vec::new()\n    }\n\n    fn find_node_at_line_col(\n        &self,\n        root: NodeId,\n        target_line: DocLine,\n        target_col: DocDisplayCol,\n    ) -> NodeCursorTarget {\n        self.find_node_at_line_col_internal(root, target_line, target_col, true, None)\n    }\n}\n",
	);

	let fold = materialize_fold_boundary_at_cursor(
		&registry,
		root,
		DocLine::new(8),
		DocDisplayCol::new(8),
	)
	.expect("trait impl method fold should materialize");
	let folded_leaf = registry
		.get_first_child(fold)
		.expect("synthetic fold should have one child");
	let folded_text = registry
		.read_node_bytes_sync(folded_leaf)
		.expect("folded text should be readable");

	assert_eq!(
		std::str::from_utf8(&folded_text).expect("folded utf8"),
		"    fn query_viewport(\n        &self,\n        root: NodeId,\n        target_line: DocLine,\n        line_count: u32,\n    ) -> Vec<RenderToken> {\n        let mut current_global_byte = DocByte::ZERO;\n        current_global_byte = current_global_byte.saturating_add(line_count as u64);\n        Vec::new()\n    }\n",
	);
}

#[test]
fn materialize_fold_boundary_on_function_signature_indent_still_picks_function() {
	let (registry, root) = build_document(
		"impl UastRegistry {\n    fn alpha(&self) {\n        first();\n    }\n\n    fn beta(&self) {\n        second();\n        third();\n    }\n\n    fn gamma(&self) {\n        fourth();\n    }\n}\n",
	);

	let fold =
		materialize_fold_boundary_at_cursor(&registry, root, DocLine::new(5), DocDisplayCol::ZERO)
			.expect("method fold should materialize");
	let folded_leaf = registry
		.get_first_child(fold)
		.expect("synthetic fold should have one child");
	let folded_text = registry
		.read_node_bytes_sync(folded_leaf)
		.expect("folded text should be readable");

	assert_eq!(
		std::str::from_utf8(&folded_text).expect("folded utf8"),
		"    fn beta(&self) {\n        second();\n        third();\n    }\n",
	);
}

#[test]
fn materialize_fold_boundary_on_trait_impl_signature_indent_still_picks_function() {
	let (registry, root) = build_document(
		"impl UastProjection for UastRegistry {\n    fn query_viewport(\n        &self,\n        root: NodeId,\n        target_line: DocLine,\n        line_count: u32,\n    ) -> Vec<RenderToken> {\n        let mut current_global_byte = DocByte::ZERO;\n        current_global_byte = current_global_byte.saturating_add(line_count as u64);\n        Vec::new()\n    }\n\n    fn find_node_at_line_col(\n        &self,\n        root: NodeId,\n        target_line: DocLine,\n        target_col: DocDisplayCol,\n    ) -> NodeCursorTarget {\n        self.find_node_at_line_col_internal(root, target_line, target_col, true, None)\n    }\n}\n",
	);

	let fold =
		materialize_fold_boundary_at_cursor(&registry, root, DocLine::new(1), DocDisplayCol::ZERO)
			.expect("trait impl method fold should materialize from signature line");
	let folded_leaf = registry
		.get_first_child(fold)
		.expect("synthetic fold should have one child");
	let folded_text = registry
		.read_node_bytes_sync(folded_leaf)
		.expect("folded text should be readable");

	assert_eq!(
		std::str::from_utf8(&folded_text).expect("folded utf8"),
		"    fn query_viewport(\n        &self,\n        root: NodeId,\n        target_line: DocLine,\n        line_count: u32,\n    ) -> Vec<RenderToken> {\n        let mut current_global_byte = DocByte::ZERO;\n        current_global_byte = current_global_byte.saturating_add(line_count as u64);\n        Vec::new()\n    }\n",
	);
}

#[test]
fn materialize_fold_boundary_uses_real_projection_trait_impl_methods() {
	let text = include_str!("../../uast/projection.rs");
	let (registry, root) = build_document(text);
	let samples = [
		(
			doc_line_of_after(
				text,
				"\tfn query_viewport<F: FoldPolicy + ?Sized>(",
				"self.query_viewport_internal(",
			),
			DocDisplayCol::ZERO,
			"\tfn query_viewport<F: FoldPolicy + ?Sized>(",
		),
		(
			doc_line_of(
				text,
				"if let Some(child) = (*self.edges[index].get())",
			),
			DocDisplayCol::new(4),
			"\tfn get_next_node_in_walk(",
		),
		(
			doc_line_of_after(
				text,
				"impl UastProjection for UastRegistry",
				"self.find_node_at_line_col_internal(",
			),
			DocDisplayCol::new(4),
			"\tfn find_node_at_line_col<F: FoldPolicy + ?Sized>(",
		),
		(
			doc_line_of_after(
				text,
				"\tfn read_loaded_slice(",
				"try_reserve_exact(requested)",
			),
			DocDisplayCol::new(4),
			"\tfn read_loaded_slice(",
		),
		(
			doc_line_of_after(
				text,
				"\tfn doc_byte_for_node_offset(",
				"absolute = absolute.saturating_add(sibling_len);",
			),
			DocDisplayCol::new(4),
			"\tfn doc_byte_for_node_offset(",
		),
	];

	for (line, col, expected_start) in samples {
		let fold = materialize_fold_boundary_at_cursor(&registry, root, line, col)
			.expect("projection trait impl method fold should materialize");
		let folded_leaf = registry
			.get_first_child(fold)
			.expect("synthetic fold should have one child");
		let folded_text = registry
			.read_node_bytes_sync(folded_leaf)
			.expect("folded text should be readable");
		let folded_text = std::str::from_utf8(&folded_text).expect("folded utf8");

		assert!(
			folded_text.starts_with(expected_start),
			"expected fold to start with {expected_start:?}, got:\n{folded_text}"
		);
		assert!(
			!folded_text.starts_with("impl UastProjection for UastRegistry"),
			"method fold should not collapse the entire impl block"
		);
	}
}

#[test]
fn materialize_fold_boundary_in_projection_trait_impl_survives_prior_sibling_fold() {
	let text = include_str!("../../uast/projection.rs");
	let (registry, root) = build_document(text);

	let helper_fold = materialize_fold_boundary_at_cursor(
		&registry,
		root,
		doc_line_of(
			text,
			"if let Some(sib) = (*self.edges[curr.index()].get()).next_sibling {",
		),
		DocDisplayCol::new(4),
	)
	.expect("helper method fold should materialize");
	let mut folds = FoldSet::default();
	folds.insert(helper_fold);

	let trait_fold = materialize_fold_boundary_at_cursor(
		&registry,
		root,
		doc_line_of_after(
			text,
			"\tfn query_viewport<F: FoldPolicy + ?Sized>(",
			"self.query_viewport_internal(",
		),
		DocDisplayCol::ZERO,
	)
	.expect("trait impl method fold should still materialize");
	let folded_leaf = registry
		.get_first_child(trait_fold)
		.expect("synthetic fold should have one child");
	let folded_text = registry
		.read_node_bytes_sync(folded_leaf)
		.expect("folded text should be readable");
	let folded_text = std::str::from_utf8(&folded_text).expect("folded utf8");

	assert!(folded_text.starts_with("\tfn query_viewport<F: FoldPolicy + ?Sized>("));
	assert!(!folded_text.starts_with("impl UastProjection for UastRegistry"));
}

#[test]
fn resolve_fold_boundary_prefers_narrower_projection_method_over_existing_impl_fold() {
	let text = include_str!("../../uast/projection.rs");
	let (registry, root) = build_document(text);
	let impl_fold = materialize_fold_boundary_at_cursor(
		&registry,
		root,
		doc_line_of(text, "impl UastProjection for UastRegistry {"),
		DocDisplayCol::ZERO,
	)
	.expect("impl fold should materialize");
	let impl_leaf = registry
		.get_first_child(impl_fold)
		.expect("impl fold should have one child");
	let impl_text = registry
		.read_node_bytes_sync(impl_leaf)
		.expect("impl fold text should be readable");
	assert!(
		std::str::from_utf8(&impl_text)
			.expect("impl fold utf8")
			.starts_with("impl UastProjection for UastRegistry {")
	);

	let cursor_line = doc_line_of_after(
		text,
		"\tfn query_viewport<F: FoldPolicy + ?Sized>(",
		"self.query_viewport_internal(",
	);
	let cursor = registry.find_node_at_line_col(
		root,
		&crate::uast::NoFolds,
		cursor_line,
		DocDisplayCol::ZERO,
	);
	let folds = FoldSet::default();
	let target = resolve_fold_boundary_at_cursor(
		&registry,
		root,
		&folds,
		cursor.node_id,
		cursor_line,
		DocDisplayCol::ZERO,
		true,
	)
	.expect("narrower query_viewport fold should be selected");

	assert_ne!(target, impl_fold);
	let folded_leaf = registry
		.get_first_child(target)
		.expect("function fold should have one child");
	let folded_text = registry
		.read_node_bytes_sync(folded_leaf)
		.expect("function fold text should be readable");
	assert!(
		std::str::from_utf8(&folded_text)
			.expect("function fold utf8")
			.starts_with("\tfn query_viewport<F: FoldPolicy + ?Sized>(")
	);
}

#[test]
fn materialize_fold_boundary_inside_multiline_macro_in_method_prefers_method() {
	let (registry, root) = build_document(
		"impl UastRegistry {\n    fn alloc_test_node(&self) -> NodeId {\n        let id_val = self.next_id.fetch_add(1, Ordering::Relaxed);\n        assert!(\n            id_val <= self.capacity,\n            \"UastRegistry capacity exceeded during split\"\n        );\n        let node = NodeId::from_index(id_val as usize - 1);\n        node\n    }\n\n    fn reserve_chunk(&self, size: u32) -> Option<RegistryChunk<'_>> {\n        let start_id = self.next_id.fetch_add(size, Ordering::Relaxed);\n        if start_id + size > self.capacity + 1 {\n            return None;\n        }\n        Some(RegistryChunk::new(self, start_id, size))\n    }\n}\n",
	);

	let fold = materialize_fold_boundary_at_cursor(
		&registry,
		root,
		DocLine::new(4),
		DocDisplayCol::new(12),
	)
	.expect("method fold should materialize");
	let folded_leaf = registry
		.get_first_child(fold)
		.expect("synthetic fold should have one child");
	let folded_text = registry
		.read_node_bytes_sync(folded_leaf)
		.expect("folded text should be readable");

	assert_eq!(
		std::str::from_utf8(&folded_text).expect("folded utf8"),
		"    fn alloc_test_node(&self) -> NodeId {\n        let id_val = self.next_id.fetch_add(1, Ordering::Relaxed);\n        assert!(\n            id_val <= self.capacity,\n            \"UastRegistry capacity exceeded during split\"\n        );\n        let node = NodeId::from_index(id_val as usize - 1);\n        node\n    }\n",
	);
}

#[test]
fn materialize_fold_boundary_inside_if_in_method_prefers_method() {
	let (registry, root) = build_document(
		"impl UastRegistry {\n    fn alloc_test_node(&self) -> NodeId {\n        let id_val = self.next_id.fetch_add(1, Ordering::Relaxed);\n        assert!(\n            id_val <= self.capacity,\n            \"UastRegistry capacity exceeded during split\"\n        );\n        let node = NodeId::from_index(id_val as usize - 1);\n        node\n    }\n\n    fn reserve_chunk(&self, size: u32) -> Option<RegistryChunk<'_>> {\n        let start_id = self.next_id.fetch_add(size, Ordering::Relaxed);\n        if start_id + size > self.capacity + 1 {\n            return None;\n        }\n        Some(RegistryChunk::new(self, start_id, size))\n    }\n}\n",
	);

	let fold = materialize_fold_boundary_at_cursor(
		&registry,
		root,
		DocLine::new(13),
		DocDisplayCol::new(8),
	)
	.expect("method fold should materialize");
	let folded_leaf = registry
		.get_first_child(fold)
		.expect("synthetic fold should have one child");
	let folded_text = registry
		.read_node_bytes_sync(folded_leaf)
		.expect("folded text should be readable");

	assert_eq!(
		std::str::from_utf8(&folded_text).expect("folded utf8"),
		"    fn reserve_chunk(&self, size: u32) -> Option<RegistryChunk<'_>> {\n        let start_id = self.next_id.fetch_add(size, Ordering::Relaxed);\n        if start_id + size > self.capacity + 1 {\n            return None;\n        }\n        Some(RegistryChunk::new(self, start_id, size))\n    }\n",
	);
}

#[test]
fn materialize_fold_boundary_on_method_closing_brace_still_picks_function() {
	let (registry, root) = build_document(
		"impl UastRegistry {\n    fn alpha(&self) {\n        first();\n    }\n\n    fn beta(&self) {\n        second();\n        third();\n    }\n\n    fn gamma(&self) {\n        fourth();\n    }\n}\n",
	);

	let fold =
		materialize_fold_boundary_at_cursor(&registry, root, DocLine::new(8), DocDisplayCol::ZERO)
			.expect("method closing brace should still target beta");
	let folded_leaf = registry
		.get_first_child(fold)
		.expect("synthetic fold should have one child");
	let folded_text = registry
		.read_node_bytes_sync(folded_leaf)
		.expect("folded text should be readable");

	assert_eq!(
		std::str::from_utf8(&folded_text).expect("folded utf8"),
		"    fn beta(&self) {\n        second();\n        third();\n    }\n",
	);
}

#[test]
fn materialize_fold_boundary_on_blank_separator_after_method_prefers_previous_function() {
	let (registry, root) = build_document(
		"impl UastRegistry {\n    fn alpha(&self) {\n        first();\n    }\n\n    fn beta(&self) {\n        second();\n        third();\n    }\n\n    fn gamma(&self) {\n        fourth();\n    }\n}\n",
	);

	let fold =
		materialize_fold_boundary_at_cursor(&registry, root, DocLine::new(9), DocDisplayCol::ZERO)
			.expect("blank separator should still target the previous method");
	let folded_leaf = registry
		.get_first_child(fold)
		.expect("synthetic fold should have one child");
	let folded_text = registry
		.read_node_bytes_sync(folded_leaf)
		.expect("folded text should be readable");

	assert_eq!(
		std::str::from_utf8(&folded_text).expect("folded utf8"),
		"    fn beta(&self) {\n        second();\n        third();\n    }\n",
	);
}

#[test]
fn materialize_fold_boundary_includes_outer_attributes_consistently() {
	let (registry, root) = build_document(
		"#[test]\nfn smoke() {\n    alpha();\n    beta();\n}\n\n#[derive(\n    Debug,\n    Clone,\n)]\nstruct Widget {\n    field: u32,\n}\n",
	);

	let fn_fold = materialize_fold_boundary_at_cursor(
		&registry,
		root,
		DocLine::new(2),
		DocDisplayCol::new(4),
	)
	.expect("function fold should materialize");
	let fn_leaf = registry
		.get_first_child(fn_fold)
		.expect("synthetic function fold should have one child");
	let fn_text = registry
		.read_node_bytes_sync(fn_leaf)
		.expect("function fold text should be readable");
	assert_eq!(
		std::str::from_utf8(&fn_text).expect("folded utf8"),
		"#[test]\nfn smoke() {\n    alpha();\n    beta();\n}\n",
	);

	let derive_fold = materialize_fold_boundary_at_cursor(
		&registry,
		root,
		DocLine::new(10),
		DocDisplayCol::new(4),
	)
	.expect("struct fold should materialize");
	let derive_leaf = registry
		.get_first_child(derive_fold)
		.expect("synthetic derive fold should have one child");
	let derive_text = registry
		.read_node_bytes_sync(derive_leaf)
		.expect("derive fold text should be readable");
	assert_eq!(
		std::str::from_utf8(&derive_text).expect("folded utf8"),
		"#[derive(\n    Debug,\n    Clone,\n)]\nstruct Widget {\n    field: u32,\n}\n",
	);
}

#[test]
fn synthetic_fold_round_trips_edit_undo_redo_inside_hidden_range() {
	let original =
		"fn main() {\n    if true {\n        alpha();\n        beta();\n    }\n    gamma();\n}\n";
	let mutated = "fn main() {\n    if true {\n        alpha();\n        delta();\n        beta();\n    }\n    gamma();\n}\n";
	let (registry, root) = build_document(original);
	let mut root_id = Some(root);
	let fold = materialize_fold_boundary_at_cursor(
		&registry,
		root,
		DocLine::new(2),
		DocDisplayCol::new(8),
	)
	.expect("nested block fold should materialize");
	let mut folds = FoldSet::default();
	folds.insert(fold);

	let mut ledger = UndoLedger::new();
	let mut semantic_highlights = Vec::new();
	let mut diagnostics = Vec::new();
	let insert_offset =
		byte_offset_from_line_col(original.as_bytes(), DocLine::new(3), DocDisplayCol::ZERO);

	apply_deltas_to_document(
		&registry,
		&mut root_id,
		&mut folds,
		&mut ledger,
		&mut semantic_highlights,
		&mut diagnostics,
		vec![TextDelta {
			global_byte_offset: insert_offset,
			deleted_bytes: Vec::new(),
			inserted_bytes: "        delta();\n".as_bytes().to_vec(),
			state_before: StateId::ZERO,
			state_after: StateId::ZERO,
		}],
	)
	.expect("edit inside folded range should apply");

	let root = root_id.expect("mutated root");
	let bytes = registry
		.read_loaded_slice(
			root,
			DocByte::ZERO,
			DocByte::new(registry.get_total_bytes(root)),
		)
		.expect("collect mutated bytes");
	assert_eq!(String::from_utf8(bytes).expect("utf8"), mutated);
	assert!(!folds.contains(fold));
	let cursors = crate::engine::session::CursorSnapshot::single(DocByte::ZERO);
	ledger.finish_cursor_transaction(crate::core::StateId::ZERO, cursors.clone(), cursors);

	let crate::engine::undo::HistoryReplay::Text {
		deltas: undo_group,
		cursors: _,
	} = ledger
		.preview(crate::engine::undo::HistoryDirection::Undo)
		.expect("undo should exist")
	else {
		panic!("synthetic fold undo must replay text deltas");
	};
	apply_deltas_to_document_internal(
		&registry,
		&mut root_id,
		&mut folds,
		&mut ledger,
		&mut semantic_highlights,
		&mut diagnostics,
		undo_group,
		false,
	)
	.expect("undo should apply");
	ledger.commit(crate::engine::undo::HistoryDirection::Undo);

	let root = root_id.expect("restored root");
	let restored = registry
		.read_loaded_slice(
			root,
			DocByte::ZERO,
			DocByte::new(registry.get_total_bytes(root)),
		)
		.expect("collect restored bytes");
	assert_eq!(String::from_utf8(restored).expect("utf8"), original);

	let crate::engine::undo::HistoryReplay::Text {
		deltas: redo_group,
		cursors: _,
	} = ledger
		.preview(crate::engine::undo::HistoryDirection::Redo)
		.expect("redo should exist")
	else {
		panic!("synthetic fold redo must replay text deltas");
	};
	apply_deltas_to_document_internal(
		&registry,
		&mut root_id,
		&mut folds,
		&mut ledger,
		&mut semantic_highlights,
		&mut diagnostics,
		redo_group,
		false,
	)
	.expect("redo should apply");
	ledger.commit(crate::engine::undo::HistoryDirection::Redo);

	let root = root_id.expect("redone root");
	let redone = registry
		.read_loaded_slice(
			root,
			DocByte::ZERO,
			DocByte::new(registry.get_total_bytes(root)),
		)
		.expect("collect redone bytes");
	assert_eq!(String::from_utf8(redone).expect("utf8"), mutated);
}

#[test]
fn nested_synthetic_parent_fold_keeps_function_tail_attached() {
	let (registry, root) = build_document(
		"fn main() {\n    alpha();\n    if true {\n        beta();\n        gamma();\n    }\n    delta();\n    epsilon();\n}\n\nfn outside() {\n    zeta();\n}\n",
	);
	let inner_fold = materialize_fold_boundary_at_cursor(
		&registry,
		root,
		DocLine::new(3),
		DocDisplayCol::new(8),
	)
	.expect("inner if fold should materialize");
	let mut folds = FoldSet::default();
	folds.insert(inner_fold);

	let outer_fold =
		materialize_fold_boundary_at_cursor(&registry, root, DocLine::ZERO, DocDisplayCol::ZERO)
			.expect("outer function fold should materialize");
	folds.insert(outer_fold);

	let tokens = registry.query_viewport(root, &folds, DocLine::ZERO, 20);
	assert!(
		tokens
			.iter()
			.any(|token| token.is_folded && token.node_id == outer_fold)
	);
	assert!(!tokens.iter().any(|token| {
		String::from_utf8_lossy(&token.text).contains("delta()")
			|| String::from_utf8_lossy(&token.text).contains("epsilon()")
	}));
	assert!(tokens.iter().any(|token| {
		!token.is_folded && String::from_utf8_lossy(&token.text).contains("fn outside()")
	}));

	folds.remove(outer_fold);
	let reopened = registry.query_viewport(root, &folds, DocLine::ZERO, 20);
	assert!(
		reopened
			.iter()
			.any(|token| token.is_folded && token.node_id == inner_fold)
	);
	assert!(reopened.iter().any(|token| {
		!token.is_folded && String::from_utf8_lossy(&token.text).contains("delta();")
	}));
	assert!(reopened.iter().any(|token| {
		!token.is_folded && String::from_utf8_lossy(&token.text).contains("epsilon();")
	}));
}

#[test]
fn sum_tree_cache_rebuilds_after_synthetic_fold_materialization() {
	let (registry, root) = build_document(
		"fn main() {\n    alpha();\n    beta();\n}\n\nfn outside() {\n    gamma();\n}\n",
	);
	let mut cache = None;
	let _initial_tree = refresh_sum_tree(&mut cache, &registry, root, StateId::ZERO);
	let initial_topology_revision = cache.as_ref().expect("cached tree").topology_revision;

	let fold =
		materialize_fold_boundary_at_cursor(&registry, root, DocLine::ZERO, DocDisplayCol::ZERO)
			.expect("function fold should materialize");
	let mut folds = FoldSet::default();
	folds.insert(fold);

	let _rebuilt_tree = refresh_sum_tree(&mut cache, &registry, root, StateId::ZERO);
	let rebuilt_topology_revision = cache
		.as_ref()
		.expect("rebuilt cached tree")
		.topology_revision;
	assert!(rebuilt_topology_revision > initial_topology_revision);

	let tokens = registry.query_viewport(root, &folds, DocLine::ZERO, 20);
	assert!(
		tokens
			.iter()
			.any(|token| token.is_folded && token.node_id == fold)
	);
}
