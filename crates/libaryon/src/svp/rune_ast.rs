//! Phase 8 — Rune Structural Graft.
//!
//! Parses Rune source with the official `rune` crate's `Parser` and maps
//! the resulting AST into Baryon's `UastRegistry` via the same relational
//! isomorphism used for Python (`python_ast.rs`):
//!
//! * Scopes / blocks (File, ItemFn, ItemImpl, ItemMod, ItemStruct,
//!   ItemEnum, Block) → `SemanticKind::RelationalTable`
//! * Single statements / declarations (ItemUse, ItemConst, ItemMacroCall,
//!   Local, Expr, Stmt) → `SemanticKind::RelationalRow`
//! * Trivia (whitespace, comments, braces, gaps the parser drops) →
//!   `SemanticKind::Token`
//!
//! **Gap-filling invariant**: Rune's parser drops whitespace and comments
//! during `Peeker::next`, so walking the AST alone cannot reconstruct the
//! full byte range. This module therefore maintains an absolute offset
//! cursor and emits `Trivia` leaves wherever a parsed node starts past the
//! current cursor. As a result, `build_green_tree(...).text_len()` is
//! guaranteed to equal `source_len` for any input the parser accepts.
//!
//! **Parser-error resilience**: if Rune's parser returns a hard error (live
//! typing, syntax mid-flight, etc.) we fall back to a `File` branch wrapping
//! a single `Trivia` leaf covering the whole source, so the engine never
//! panics and byte length is still preserved.

use crate::uast::kind::{ProviderKindId, SemanticKind};
#[cfg(test)]
use crate::ecs::{NodeId, UastRegistry};
#[cfg(test)]
use crate::uast::metrics::SpanMetrics;

use rune::SourceId;
use rune::ast::{self, Spanned};

/// Structural kinds for the Rune green tree. Deliberately narrow — just
/// enough to distinguish tables from rows from trivia.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum RuneSyntaxKind {
	// Table-shaped (scopes / blocks)
	File,
	ItemFn,
	ItemStruct,
	ItemEnum,
	ItemImpl,
	ItemMod,
	Block,
	ExprBlock,

	// Row-shaped (single statements / declarations)
	ItemUse,
	ItemConst,
	ItemMacroCall,
	Local,
	Expr,
	Stmt,

	/// Whitespace, comments, braces, and other trivia the parser drops.
	Trivia,
}

impl RuneSyntaxKind {
	pub fn provider_kind(self) -> ProviderKindId {
		ProviderKindId::new(self as u16 + 1)
	}

	pub fn name(id: ProviderKindId) -> Option<String> {
		const NAMES: &[&str] = &[
			"File",
			"ItemFn",
			"ItemStruct",
			"ItemEnum",
			"ItemImpl",
			"ItemMod",
			"Block",
			"ExprBlock",
			"ItemUse",
			"ItemConst",
			"ItemMacroCall",
			"Local",
			"Expr",
			"Stmt",
			"Trivia",
		];
		let index = id.get().checked_sub(1)? as usize;
		NAMES.get(index).map(|name| (*name).to_owned())
	}
}

/// Light syntax-tree node. Stores only kind and byte length.
/// Text content lives on `UastRegistry` leaves via `virtual_data`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GreenNode {
	pub kind: RuneSyntaxKind,
	pub children: Vec<GreenNode>,
	leaf_len: u32,
}

impl GreenNode {
	pub fn leaf(kind: RuneSyntaxKind, len: u32) -> Self {
		Self {
			kind,
			children: Vec::new(),
			leaf_len: len,
		}
	}

	pub fn branch(kind: RuneSyntaxKind, children: Vec<GreenNode>) -> Self {
		Self {
			kind,
			children,
			leaf_len: 0,
		}
	}

	pub fn text_len(&self) -> u32 {
		if self.children.is_empty() {
			self.leaf_len
		} else {
			self.children.iter().map(|c| c.text_len()).sum()
		}
	}
}

// ============================================================
// Step 1: Parse
// ============================================================

/// Parse the given UTF-8 source into a Rune [`ast::File`]. Returns `None`
/// on any parser error (syntax mid-flight, unsupported constructs, etc.);
/// callers should fall back to an all-trivia tree in that case.
pub fn try_parse_rune_file(source: &str) -> Option<ast::File> {
	let mut parser = rune::parse::Parser::new(source, SourceId::EMPTY, true);
	parser.parse_all::<ast::File>().ok()
}

// ============================================================
// Step 2: Relational mapper with trivia gap-filling
// ============================================================

#[inline]
fn span_range(spanned: &impl Spanned) -> (u32, u32) {
	let s = spanned.span();
	(s.start.0, s.end.0)
}

/// Top-level green-tree builder. Always returns a node of kind `File`
/// whose `text_len()` equals `source_len`.
pub fn build_green_tree(source: &str, source_len: u32) -> GreenNode {
	match try_parse_rune_file(source) {
		Some(file) => build_from_file(&file, source_len),
		// Parser bailed — wrap the whole document in a single trivia leaf
		// so byte length is preserved and the engine stays alive.
		None => GreenNode::branch(
			RuneSyntaxKind::File,
			vec![GreenNode::leaf(RuneSyntaxKind::Trivia, source_len)],
		),
	}
}

fn build_from_file(file: &ast::File, source_len: u32) -> GreenNode {
	let mut children = Vec::new();
	let mut offset: u32 = 0;

	// Shebang + outer attributes: collapse into leading trivia — we only
	// need them to advance `offset` cleanly. The parser's `File` span
	// coverage starts at the first item, so anything before that is gap.
	//
	// For each item: emit a trivia gap if needed, then the item node, then
	// absorb the optional trailing semicolon into the same subtree.
	for (item, semi) in &file.items {
		let (item_start, mut item_end) = span_range(item);

		if item_start > offset {
			children.push(GreenNode::leaf(RuneSyntaxKind::Trivia, item_start - offset));
		}

		if let Some(s) = semi {
			let (_, semi_end) = span_range(s);
			if semi_end > item_end {
				item_end = semi_end;
			}
		}

		children.push(build_item_node(item, item_start, item_end));
		offset = item_end;
	}

	// Trailing trivia after the last item (or the whole file if empty).
	if offset < source_len {
		children.push(GreenNode::leaf(RuneSyntaxKind::Trivia, source_len - offset));
	}

	GreenNode::branch(RuneSyntaxKind::File, children)
}

/// Build the green node for a top-level or nested `ast::Item`, covering the
/// byte range `[item_start, item_end)`.
fn build_item_node(item: &ast::Item, item_start: u32, item_end: u32) -> GreenNode {
	match item {
		ast::Item::Fn(f) => build_item_fn(f, item_start, item_end),
		ast::Item::Impl(i) => build_item_impl(i, item_start, item_end),
		ast::Item::Struct(_) => {
			wrap_table_as_trivia(RuneSyntaxKind::ItemStruct, item_end - item_start)
		}
		ast::Item::Enum(_) => wrap_table_as_trivia(RuneSyntaxKind::ItemEnum, item_end - item_start),
		ast::Item::Mod(m) => build_item_mod(m, item_start, item_end),
		ast::Item::Use(_) => GreenNode::leaf(RuneSyntaxKind::ItemUse, item_end - item_start),
		ast::Item::Const(_) => GreenNode::leaf(RuneSyntaxKind::ItemConst, item_end - item_start),
		ast::Item::MacroCall(_) => {
			GreenNode::leaf(RuneSyntaxKind::ItemMacroCall, item_end - item_start)
		}
		// rune::ast::Item is #[non_exhaustive]; future variants land here.
		_ => GreenNode::leaf(RuneSyntaxKind::Stmt, item_end - item_start),
	}
}

/// Build a Table-kind branch as a thin wrapper around a single Trivia leaf.
/// Used for scopes we classify as tables but don't yet descend into.
fn wrap_table_as_trivia(kind: RuneSyntaxKind, len: u32) -> GreenNode {
	if len == 0 {
		return GreenNode::branch(kind, Vec::new());
	}
	GreenNode::branch(kind, vec![GreenNode::leaf(RuneSyntaxKind::Trivia, len)])
}

fn build_item_fn(f: &ast::ItemFn, item_start: u32, item_end: u32) -> GreenNode {
	let (body_start, body_end) = span_range(&f.body);
	let mut children = Vec::new();

	// Header: attributes, visibility, `fn`, name, args, return type.
	if body_start > item_start {
		children.push(GreenNode::leaf(
			RuneSyntaxKind::Trivia,
			body_start - item_start,
		));
	}

	// The body block (which is itself a Table).
	children.push(build_block(&f.body, body_start, body_end));

	// Anything after the closing brace up to item_end (e.g. a trailing
	// semicolon absorbed by the caller).
	if item_end > body_end {
		children.push(GreenNode::leaf(RuneSyntaxKind::Trivia, item_end - body_end));
	}

	GreenNode::branch(RuneSyntaxKind::ItemFn, children)
}

fn build_item_impl(i: &ast::ItemImpl, item_start: u32, item_end: u32) -> GreenNode {
	let mut children = Vec::new();
	// `impl Path {` header + attributes. Cursor advances through nested fns.
	let mut offset = item_start;

	for nested in &i.functions {
		let (fn_start, fn_end) = span_range(nested);
		if fn_start > offset {
			children.push(GreenNode::leaf(RuneSyntaxKind::Trivia, fn_start - offset));
		}
		children.push(build_item_fn(nested, fn_start, fn_end));
		offset = fn_end;
	}

	// Trailing `}` (and attributes) after the last function, up to item_end.
	if item_end > offset {
		children.push(GreenNode::leaf(RuneSyntaxKind::Trivia, item_end - offset));
	}

	GreenNode::branch(RuneSyntaxKind::ItemImpl, children)
}

fn build_item_mod(m: &ast::ItemMod, item_start: u32, item_end: u32) -> GreenNode {
	// For Phase 8 we do not descend into an inline module's nested `File`
	// — classification as a Table is enough. Wrap as a thin Trivia branch.
	let _ = m;
	wrap_table_as_trivia(RuneSyntaxKind::ItemMod, item_end - item_start)
}

fn build_block(block: &ast::Block, block_start: u32, block_end: u32) -> GreenNode {
	let mut children = Vec::new();
	let mut offset = block_start;

	for stmt in &block.statements {
		let (stmt_start, stmt_end) = span_range(stmt);
		if stmt_start > offset {
			children.push(GreenNode::leaf(RuneSyntaxKind::Trivia, stmt_start - offset));
		}
		children.push(build_stmt_node(stmt, stmt_start, stmt_end));
		offset = stmt_end;
	}

	if block_end > offset {
		children.push(GreenNode::leaf(RuneSyntaxKind::Trivia, block_end - offset));
	}

	GreenNode::branch(RuneSyntaxKind::Block, children)
}

fn build_stmt_node(stmt: &ast::Stmt, stmt_start: u32, stmt_end: u32) -> GreenNode {
	match stmt {
		ast::Stmt::Local(_) => GreenNode::leaf(RuneSyntaxKind::Local, stmt_end - stmt_start),
		ast::Stmt::Item(item, semi) => {
			// Honour the item's own span extent, then tack on the optional
			// semicolon if it extended the stmt_end.
			let (item_s, mut item_e) = span_range(item);
			if let Some(s) = semi {
				let (_, se) = span_range(s);
				if se > item_e {
					item_e = se;
				}
			}
			// Guard against edge cases where the derived Stmt span is
			// wider than the item span (shouldn't happen, but stay safe).
			let inner_start = item_s.max(stmt_start);
			let inner_end = item_e.min(stmt_end);
			let item_node = build_item_node(item, inner_start, inner_end);

			let mut children = Vec::new();
			if inner_start > stmt_start {
				children.push(GreenNode::leaf(
					RuneSyntaxKind::Trivia,
					inner_start - stmt_start,
				));
			}
			children.push(item_node);
			if stmt_end > inner_end {
				children.push(GreenNode::leaf(
					RuneSyntaxKind::Trivia,
					stmt_end - inner_end,
				));
			}
			// If the item was a Table we keep its Table nature; wrap in a
			// Stmt branch only if we actually had leading/trailing trivia,
			// otherwise return the item node directly.
			if children.len() == 1 {
				children.into_iter().next().unwrap()
			} else {
				GreenNode::branch(RuneSyntaxKind::Stmt, children)
			}
		}
		ast::Stmt::Expr(_) | ast::Stmt::Semi(_) => {
			GreenNode::leaf(RuneSyntaxKind::Expr, stmt_end - stmt_start)
		}
		// rune::ast::Stmt is #[non_exhaustive]; future variants land here.
		_ => GreenNode::leaf(RuneSyntaxKind::Stmt, stmt_end - stmt_start),
	}
}

// ============================================================
// Step 3: UastRegistry splice
// ============================================================

fn to_semantic_kind(kind: RuneSyntaxKind) -> SemanticKind {
	match kind {
		RuneSyntaxKind::File
		| RuneSyntaxKind::ItemFn
		| RuneSyntaxKind::ItemStruct
		| RuneSyntaxKind::ItemEnum
		| RuneSyntaxKind::ItemImpl
		| RuneSyntaxKind::ItemMod
		| RuneSyntaxKind::Block
		| RuneSyntaxKind::ExprBlock => SemanticKind::RelationalTable,

		RuneSyntaxKind::ItemUse
		| RuneSyntaxKind::ItemConst
		| RuneSyntaxKind::ItemMacroCall
		| RuneSyntaxKind::Local
		| RuneSyntaxKind::Expr
		| RuneSyntaxKind::Stmt => SemanticKind::RelationalRow,

		RuneSyntaxKind::Trivia => SemanticKind::Token,
	}
}

/// Convert language-specific GreenNode to the trait-level GreenNode.
pub fn to_trait_green_node(node: &GreenNode) -> crate::engine::language::GreenNode {
	let kind = to_semantic_kind(node.kind);
	if node.children.is_empty() {
		crate::engine::language::GreenNode {
			kind,
			provider_kind: node.kind.provider_kind(),
			byte_len: node.text_len(),
			children: Vec::new(),
		}
	} else {
		let children = node.children.iter().map(to_trait_green_node).collect();
		crate::engine::language::GreenNode {
			kind,
			provider_kind: node.kind.provider_kind(),
			byte_len: node.text_len(),
			children,
		}
	}
}

#[cfg(test)]
fn count_newlines_in_slice(text: &[u8], offset: u32, len: u32) -> u32 {
	let start = offset as usize;
	let end = (offset + len).min(text.len() as u32) as usize;
	if start >= text.len() || start >= end {
		return 0;
	}
	memchr::memchr_iter(b'\n', &text[start..end]).count() as u32
}

#[cfg(test)]
fn graft_recursive(
	registry: &UastRegistry,
	node: &GreenNode,
	parent: Option<NodeId>,
	prev_sibling: Option<NodeId>,
	source: &[u8],
	base_offset: u32,
) -> NodeId {
	let sem_kind = to_semantic_kind(node.kind);
	let byte_len = node.text_len();
	let newlines = if node.children.is_empty() {
		count_newlines_in_slice(source, base_offset, byte_len)
	} else {
		0
	};

	let id = registry.alloc_test_node();
	let idx = id.index();

	unsafe {
		*registry.kinds[idx].get() = sem_kind;
		*registry.spans[idx].get() = None;
		*registry.metrics[idx].get() = SpanMetrics {
			byte_length: u64::from(byte_len),
			newlines: u64::from(newlines),
		};
		let edges = &mut *registry.edges[idx].get();
		edges.parent = parent;
		edges.first_child = None;
		edges.next_sibling = None;
		*registry.child_tails[idx].get() = None;
	}

	if let Some(prev) = prev_sibling {
		unsafe {
			(*registry.edges[prev.index()].get()).next_sibling = Some(id);
		}
	}

	let mut child_offset = base_offset;
	let mut prev_child: Option<NodeId> = None;
	let mut first_child: Option<NodeId> = None;
	let mut total_child_newlines = 0u64;

	for child in &node.children {
		let child_id = graft_recursive(registry, child, Some(id), prev_child, source, child_offset);

		if first_child.is_none() {
			first_child = Some(child_id);
		}

		total_child_newlines += unsafe { (*registry.metrics[child_id.index()].get()).newlines };

		child_offset += child.text_len();
		prev_child = Some(child_id);
	}

	if let Some(fc) = first_child {
		unsafe {
			(*registry.edges[idx].get()).first_child = Some(fc);
			*registry.child_tails[idx].get() = prev_child;
		}
	}

	if !node.children.is_empty() {
		unsafe {
			(*registry.metrics[idx].get()).newlines = total_child_newlines;
		}
	}

	id
}

#[cfg(test)]
fn distribute_virtual_data(registry: &UastRegistry, node_id: NodeId, source: &[u8], offset: u32) {
	let idx = node_id.index();
	let has_children = unsafe { (*registry.edges[idx].get()).first_child.is_some() };

	if has_children {
		let mut child_offset = offset;
		let mut cursor = unsafe { (*registry.edges[idx].get()).first_child };
		while let Some(child_id) = cursor {
			let child_len =
				u32::try_from(unsafe { (*registry.metrics[child_id.index()].get()).byte_length })
					.expect("structural child length fits local offset");
			distribute_virtual_data(registry, child_id, source, child_offset);
			child_offset += child_len;
			cursor = unsafe { (*registry.edges[child_id.index()].get()).next_sibling };
		}
	} else {
		let byte_len = unsafe { (*registry.metrics[idx].get()).byte_length as usize };
		let start = offset as usize;
		let end = (start + byte_len).min(source.len());
		if start < source.len() {
			registry.replace_authoritative_data(node_id, source[start..end].to_vec());
		}
	}
}

/// Apply a parsed Rune green tree to the `UastRegistry`, replacing
/// `target_node_id` with the grafted structural root.
///
/// Mirrors `python_ast::apply_python_semantic_update`: the new root takes
/// the target's exact position in the LCRS tree, virtual data is pushed to
/// leaves, and the old target is fully disconnected.
#[cfg(test)]
pub fn apply_rune_semantic_update(
	registry: &UastRegistry,
	target_node_id: NodeId,
	parsed_tree: &GreenNode,
	source: &[u8],
) -> NodeId {
	let target_idx = target_node_id.index();

	let (parent, next_sibling, prev_sibling) = unsafe {
		let edges = &*registry.edges[target_idx].get();
		let parent = edges.parent;
		let next_sib = edges.next_sibling;

		let prev = parent.and_then(|p| {
			let mut cursor = (*registry.edges[p.index()].get()).first_child;
			let mut prev = None;
			while let Some(c) = cursor {
				if c == target_node_id {
					return prev;
				}
				prev = Some(c);
				cursor = (*registry.edges[c.index()].get()).next_sibling;
			}
			None
		});

		(parent, next_sib, prev)
	};

	let new_root = graft_recursive(registry, parsed_tree, parent, prev_sibling, source, 0);

	distribute_virtual_data(registry, new_root, source, 0);

	unsafe {
		(*registry.edges[new_root.index()].get()).next_sibling = next_sibling;

		if let Some(prev) = prev_sibling {
			(*registry.edges[prev.index()].get()).next_sibling = Some(new_root);
		} else if let Some(p) = parent {
			(*registry.edges[p.index()].get()).first_child = Some(new_root);
		}

		if let Some(p) = parent {
			let tail = &mut *registry.child_tails[p.index()].get();
			if *tail == Some(target_node_id) {
				*tail = Some(new_root);
			}
		}

		let old_edges = &mut *registry.edges[target_idx].get();
		old_edges.parent = None;
		old_edges.first_child = None;
		old_edges.next_sibling = None;
	}

	new_root
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
	use super::*;

	fn build(source: &str) -> GreenNode {
		build_green_tree(source, source.len() as u32)
	}

	// ---- byte-length invariants ----

	#[test]
	fn length_matches_empty_source() {
		let src = "";
		assert_eq!(build(src).text_len(), 0);
	}

	#[test]
	fn length_matches_single_fn_main() {
		let src = "fn main() {}\n";
		assert_eq!(build(src).text_len(), src.len() as u32);
	}

	#[test]
	fn length_matches_fn_with_let_and_comments() {
		let src = "// header\nfn foo() {\n    let x = 1;\n    // inside\n    let y = 2;\n}\n";
		assert_eq!(build(src).text_len(), src.len() as u32);
	}

	#[test]
	fn length_matches_use_and_struct() {
		let src = "use std::fmt;\n\nstruct Point { x, y }\n";
		assert_eq!(build(src).text_len(), src.len() as u32);
	}

	#[test]
	fn length_matches_impl_with_methods() {
		let src = "impl Foo {\n    fn a() {}\n    fn b() { 42 }\n}\n";
		assert_eq!(build(src).text_len(), src.len() as u32);
	}

	#[test]
	fn length_matches_module() {
		let src = "mod nested {\n    fn inside() {}\n}\n";
		assert_eq!(build(src).text_len(), src.len() as u32);
	}

	#[test]
	fn length_matches_trailing_whitespace_only() {
		let src = "\n\n\n";
		assert_eq!(build(src).text_len(), src.len() as u32);
	}

	#[test]
	fn length_preserved_on_parser_error() {
		// Unterminated brace — the parser will bail, the fallback must
		// still produce a tree of the exact source length.
		let src = "fn broken( { let x =";
		let tree = build(src);
		assert_eq!(tree.text_len(), src.len() as u32);
		assert_eq!(tree.kind, RuneSyntaxKind::File);
	}

	#[test]
	fn length_preserved_on_empty_parser_error() {
		// Garbage bytes — definitely not valid Rune.
		let src = "????????";
		let tree = build(src);
		assert_eq!(tree.text_len(), src.len() as u32);
	}

	// ---- structural classification ----

	#[test]
	fn fn_main_produces_itemfn_branch_wrapping_block() {
		let src = "fn main() {}\n";
		let tree = build(src);
		assert_eq!(tree.kind, RuneSyntaxKind::File);
		let item_fn = tree
			.children
			.iter()
			.find(|c| c.kind == RuneSyntaxKind::ItemFn)
			.expect("ItemFn must appear at file top level");
		assert!(
			item_fn
				.children
				.iter()
				.any(|c| c.kind == RuneSyntaxKind::Block),
			"ItemFn must contain a Block child"
		);
	}

	#[test]
	fn block_with_let_contains_local_row() {
		let src = "fn f() { let x = 1; }\n";
		let tree = build(src);
		let item_fn = tree
			.children
			.iter()
			.find(|c| c.kind == RuneSyntaxKind::ItemFn)
			.unwrap();
		let block = item_fn
			.children
			.iter()
			.find(|c| c.kind == RuneSyntaxKind::Block)
			.expect("ItemFn has Block");
		assert!(
			block
				.children
				.iter()
				.any(|c| c.kind == RuneSyntaxKind::Local),
			"Block must contain a Local statement"
		);
	}

	// ---- UastRegistry graft integration tests ----

	fn kind_of(registry: &UastRegistry, id: NodeId) -> SemanticKind {
		unsafe { *registry.kinds[id.index()].get() }
	}

	fn first_child(registry: &UastRegistry, id: NodeId) -> Option<NodeId> {
		unsafe { (*registry.edges[id.index()].get()).first_child }
	}

	fn next_sibling(registry: &UastRegistry, id: NodeId) -> Option<NodeId> {
		unsafe { (*registry.edges[id.index()].get()).next_sibling }
	}

	fn collect_children(registry: &UastRegistry, id: NodeId) -> Vec<NodeId> {
		let mut result = Vec::new();
		let mut cursor = first_child(registry, id);
		while let Some(c) = cursor {
			result.push(c);
			cursor = next_sibling(registry, c);
		}
		result
	}

	fn setup_registry(source_len: u32) -> (UastRegistry, NodeId, NodeId) {
		let registry = UastRegistry::new(256);
		let root_id = registry.alloc_test_node();
		let target_id = registry.alloc_test_node();
		unsafe {
			*registry.kinds[root_id.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[root_id.index()].get() = SpanMetrics {
				byte_length: u64::from(source_len),
				newlines: 0,
			};
			*registry.kinds[target_id.index()].get() = SemanticKind::Token;
			*registry.metrics[target_id.index()].get() = SpanMetrics {
				byte_length: u64::from(source_len),
				newlines: 0,
			};
			(*registry.edges[target_id.index()].get()).parent = Some(root_id);
			(*registry.edges[root_id.index()].get()).first_child = Some(target_id);
			*registry.child_tails[root_id.index()].get() = Some(target_id);
		}
		(registry, root_id, target_id)
	}

	#[test]
	fn graft_fn_main_maps_to_table_containing_row_block() {
		let source = b"fn main() { let x = 1; }\n";
		let green = build_green_tree(std::str::from_utf8(source).unwrap(), source.len() as u32);
		let (registry, root_id, target_id) = setup_registry(source.len() as u32);

		apply_rune_semantic_update(&registry, target_id, &green, source);

		assert!(
			first_child(&registry, target_id).is_none(),
			"old target must be disconnected"
		);

		let new_file = first_child(&registry, root_id).expect("root has new child");
		assert!(
			matches!(kind_of(&registry, new_file), SemanticKind::RelationalTable),
			"grafted File must be RelationalTable"
		);

		// File -> ItemFn (Table)
		let file_children = collect_children(&registry, new_file);
		let item_fn = file_children
			.iter()
			.find(|&&c| matches!(kind_of(&registry, c), SemanticKind::RelationalTable))
			.expect("File must contain a RelationalTable (ItemFn)");

		// ItemFn -> Block (Table)
		let fn_children = collect_children(&registry, *item_fn);
		let block = fn_children
			.iter()
			.find(|&&c| matches!(kind_of(&registry, c), SemanticKind::RelationalTable))
			.expect("ItemFn must contain a RelationalTable (Block)");

		// Block -> at least one RelationalRow (the `let` statement)
		let block_children = collect_children(&registry, *block);
		assert!(
			block_children
				.iter()
				.any(|&c| matches!(kind_of(&registry, c), SemanticKind::RelationalRow)),
			"Block must contain at least one RelationalRow"
		);

		// Byte length is preserved through the graft.
		let new_len = unsafe { (*registry.metrics[new_file.index()].get()).byte_length };
		assert_eq!(new_len, source.len() as u64);
	}

	#[test]
	fn graft_empty_fn_main_has_block_under_table_root() {
		let source = b"fn main() {}\n";
		let green = build_green_tree(std::str::from_utf8(source).unwrap(), source.len() as u32);
		let (registry, root_id, target_id) = setup_registry(source.len() as u32);

		apply_rune_semantic_update(&registry, target_id, &green, source);

		let new_file = first_child(&registry, root_id).unwrap();
		assert!(matches!(
			kind_of(&registry, new_file),
			SemanticKind::RelationalTable
		));

		let file_children = collect_children(&registry, new_file);
		// At least one Table child (the ItemFn) must appear.
		let tables: Vec<_> = file_children
			.iter()
			.filter(|&&c| matches!(kind_of(&registry, c), SemanticKind::RelationalTable))
			.copied()
			.collect();
		assert!(!tables.is_empty(), "File must contain an ItemFn Table");

		// The ItemFn's Block (Table) must be present even when empty.
		let fn_children = collect_children(&registry, tables[0]);
		assert!(
			fn_children
				.iter()
				.any(|&c| matches!(kind_of(&registry, c), SemanticKind::RelationalTable)),
			"ItemFn must contain a Block Table even when the body is empty"
		);
	}

	#[test]
	fn graft_use_statement_produces_relational_row() {
		let source = b"use std::fmt;\n";
		let green = build_green_tree(std::str::from_utf8(source).unwrap(), source.len() as u32);
		let (registry, root_id, target_id) = setup_registry(source.len() as u32);

		apply_rune_semantic_update(&registry, target_id, &green, source);

		let new_file = first_child(&registry, root_id).unwrap();
		let file_children = collect_children(&registry, new_file);
		assert!(
			file_children
				.iter()
				.any(|&c| matches!(kind_of(&registry, c), SemanticKind::RelationalRow)),
			"use declaration must land as RelationalRow under File"
		);

		let len = unsafe { (*registry.metrics[new_file.index()].get()).byte_length };
		assert_eq!(len, source.len() as u64);
	}

	#[test]
	fn graft_parser_error_falls_back_to_trivia_preserving_length() {
		let source = b"fn broken( { let x =";
		let green = build_green_tree(std::str::from_utf8(source).unwrap(), source.len() as u32);
		let (registry, root_id, target_id) = setup_registry(source.len() as u32);

		apply_rune_semantic_update(&registry, target_id, &green, source);

		let new_file = first_child(&registry, root_id).unwrap();
		let len = unsafe { (*registry.metrics[new_file.index()].get()).byte_length };
		assert_eq!(
			len,
			source.len() as u64,
			"parser-error fallback must still preserve byte length"
		);
		assert!(matches!(
			kind_of(&registry, new_file),
			SemanticKind::RelationalTable
		));
	}

	#[test]
	fn graft_preserves_virtual_data_on_leaves() {
		let source = b"fn main() {}\n";
		let green = build_green_tree(std::str::from_utf8(source).unwrap(), source.len() as u32);
		let registry = UastRegistry::new(64);
		let target_id = registry.alloc_test_node();
		unsafe {
			*registry.kinds[target_id.index()].get() = SemanticKind::Token;
			*registry.metrics[target_id.index()].get() = SpanMetrics {
				byte_length: source.len() as u64,
				newlines: 0,
			};
		}

		apply_rune_semantic_update(&registry, target_id, &green, source);

		let mut found_data = false;
		for i in 0..registry.next_id.load(std::sync::atomic::Ordering::Relaxed) as usize {
			let vd = unsafe { &*registry.virtual_data[i].get() };
			if let Some(data) = vd
				&& !data.is_empty()
			{
				found_data = true;
			}
		}
		assert!(
			found_data,
			"at least one leaf must have virtual_data populated"
		);
	}

	/// Spatial-index synchronization: every `\n` in the source must be
	/// accounted for by exactly one leaf newline metric after the graft,
	/// and the new root's aggregated newlines must match the same total.
	#[test]
	fn graft_newlines_match_source_newline_count() {
		let source = b"\n\n    // test\n\nfn foo() {\n\tlet x = 1;\n}\n";
		let green = build_green_tree(std::str::from_utf8(source).unwrap(), source.len() as u32);

		let registry = UastRegistry::new(256);
		let root_id = registry.alloc_test_node();
		let target_id = registry.alloc_test_node();
		unsafe {
			*registry.kinds[root_id.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[root_id.index()].get() = SpanMetrics {
				byte_length: source.len() as u64,
				newlines: 0,
			};
			*registry.kinds[target_id.index()].get() = SemanticKind::Token;
			*registry.metrics[target_id.index()].get() = SpanMetrics {
				byte_length: source.len() as u64,
				newlines: 0,
			};
			(*registry.edges[target_id.index()].get()).parent = Some(root_id);
			(*registry.edges[root_id.index()].get()).first_child = Some(target_id);
			*registry.child_tails[root_id.index()].get() = Some(target_id);
		}

		let new_root = apply_rune_semantic_update(&registry, target_id, &green, source);

		let expected = memchr::memchr_iter(b'\n', source).count() as u64;

		fn walk_leaves(registry: &UastRegistry, node: NodeId, sum: &mut u64) {
			let idx = node.index();
			let fc = unsafe { (*registry.edges[idx].get()).first_child };
			if let Some(first) = fc {
				let mut cursor = Some(first);
				while let Some(c) = cursor {
					walk_leaves(registry, c, sum);
					cursor = unsafe { (*registry.edges[c.index()].get()).next_sibling };
				}
			} else {
				*sum += unsafe { (*registry.metrics[idx].get()).newlines };
			}
		}

		let mut leaf_sum = 0u64;
		walk_leaves(&registry, new_root, &mut leaf_sum);
		assert_eq!(
			leaf_sum, expected,
			"sum of leaf newlines must equal {} (\\n count in source)",
			expected
		);

		let aggregated = unsafe { (*registry.metrics[new_root.index()].get()).newlines };
		assert_eq!(
			aggregated, expected,
			"new_root aggregated newlines must equal {} (\\n count in source)",
			expected
		);
	}
}
