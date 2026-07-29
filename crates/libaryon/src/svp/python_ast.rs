use crate::uast::kind::{ProviderKindId, SemanticKind};
#[cfg(test)]
use crate::ecs::{NodeId, UastRegistry};
#[cfg(test)]
use crate::uast::metrics::SpanMetrics;
use ruff_python_ast::{Mod, Stmt};
use ruff_python_parser::Parsed;
use ruff_text_size::Ranged;

/// Syntax kind for Python GreenNodes. Covers top-level structural categories.
/// Deliberately minimal — we only need enough to distinguish structure from trivia.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum PySyntaxKind {
	Module,
	FunctionDef,
	ClassDef,
	Import,
	Assign,
	Return,
	If,
	For,
	While,
	With,
	Try,
	Match,
	Raise,
	Assert,
	Expr,
	Pass,
	Break,
	Continue,
	Delete,
	Global,
	Nonlocal,
	TypeAlias,
	/// Whitespace, comments, and other trivia not covered by AST nodes.
	Trivia,
	/// Catch-all for statement types we don't specifically categorize.
	Statement,
}

impl PySyntaxKind {
	pub fn provider_kind(self) -> ProviderKindId {
		ProviderKindId::new(self as u16 + 1)
	}

	pub fn name(id: ProviderKindId) -> Option<String> {
		const NAMES: &[&str] = &[
			"Module",
			"FunctionDef",
			"ClassDef",
			"Import",
			"Assign",
			"Return",
			"If",
			"For",
			"While",
			"With",
			"Try",
			"Match",
			"Raise",
			"Assert",
			"Expr",
			"Pass",
			"Break",
			"Continue",
			"Delete",
			"Global",
			"Nonlocal",
			"TypeAlias",
			"Trivia",
			"Statement",
		];
		let index = id.get().checked_sub(1)? as usize;
		NAMES.get(index).map(|name| (*name).to_owned())
	}
}

/// A lightweight syntax tree node. Stores only kind and byte length.
/// Text content is NOT stored here — it remains in the UastRegistry's leaf chunks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GreenNode {
	pub kind: PySyntaxKind,
	pub children: Vec<GreenNode>,
	/// Byte length for leaf nodes (no children). Zero for branch nodes
	/// (their length is the sum of children).
	leaf_len: u32,
}

impl GreenNode {
	/// Create a leaf node with a fixed byte length.
	pub fn leaf(kind: PySyntaxKind, len: u32) -> Self {
		Self {
			kind,
			children: Vec::new(),
			leaf_len: len,
		}
	}

	/// Create a branch node whose length is derived from its children.
	pub fn branch(kind: PySyntaxKind, children: Vec<GreenNode>) -> Self {
		Self {
			kind,
			children,
			leaf_len: 0,
		}
	}

	/// Total byte length of this node and all descendants.
	pub fn text_len(&self) -> u32 {
		if self.children.is_empty() {
			self.leaf_len
		} else {
			self.children.iter().map(|c| c.text_len()).sum()
		}
	}
}

fn stmt_kind(stmt: &Stmt) -> PySyntaxKind {
	match stmt {
		Stmt::FunctionDef(_) => PySyntaxKind::FunctionDef,
		Stmt::ClassDef(_) => PySyntaxKind::ClassDef,
		Stmt::Import(_) | Stmt::ImportFrom(_) => PySyntaxKind::Import,
		Stmt::Assign(_) | Stmt::AugAssign(_) | Stmt::AnnAssign(_) => PySyntaxKind::Assign,
		Stmt::Return(_) => PySyntaxKind::Return,
		Stmt::If(_) => PySyntaxKind::If,
		Stmt::For(_) => PySyntaxKind::For,
		Stmt::While(_) => PySyntaxKind::While,
		Stmt::With(_) => PySyntaxKind::With,
		Stmt::Try(_) => PySyntaxKind::Try,
		Stmt::Match(_) => PySyntaxKind::Match,
		Stmt::Raise(_) => PySyntaxKind::Raise,
		Stmt::Assert(_) => PySyntaxKind::Assert,
		Stmt::Expr(_) => PySyntaxKind::Expr,
		Stmt::Pass(_) => PySyntaxKind::Pass,
		Stmt::Break(_) => PySyntaxKind::Break,
		Stmt::Continue(_) => PySyntaxKind::Continue,
		Stmt::Delete(_) => PySyntaxKind::Delete,
		Stmt::Global(_) => PySyntaxKind::Global,
		Stmt::Nonlocal(_) => PySyntaxKind::Nonlocal,
		Stmt::TypeAlias(_) => PySyntaxKind::TypeAlias,
		_ => PySyntaxKind::Statement,
	}
}

/// Walk a list of statements, emitting GreenNodes with trivia gap-filling.
/// `offset` tracks the current position in the byte stream.
/// Returns the children and the updated offset.
fn visit_body(body: &[Stmt], mut offset: u32) -> (Vec<GreenNode>, u32) {
	let mut children = Vec::new();

	for stmt in body {
		let range = stmt.range();
		let stmt_start: u32 = range.start().into();
		let stmt_end: u32 = range.end().into();

		// Gap before this statement = trivia
		if stmt_start > offset {
			children.push(GreenNode::leaf(PySyntaxKind::Trivia, stmt_start - offset));
		}

		// For compound statements with bodies, recurse into them.
		// The statement node wraps both the header trivia and inner body.
		let stmt_len = stmt_end.saturating_sub(stmt_start);
		let inner_children = visit_stmt_children(stmt, stmt_start);

		if inner_children.is_empty() {
			// Simple statement — emit as leaf
			children.push(GreenNode::leaf(stmt_kind(stmt), stmt_len));
		} else {
			children.push(GreenNode::branch(stmt_kind(stmt), inner_children));
		}

		offset = stmt_end;
	}

	(children, offset)
}

/// For compound statements (function def, class def, if, for, etc.),
/// recurse into their body statements. Returns child GreenNodes that
/// cover the statement's full byte range.
fn visit_stmt_children(stmt: &Stmt, stmt_start: u32) -> Vec<GreenNode> {
	let range = stmt.range();
	let stmt_end: u32 = range.end().into();

	let bodies: Vec<&[Stmt]> = match stmt {
		Stmt::FunctionDef(f) => vec![&f.body],
		Stmt::ClassDef(c) => vec![&c.body],
		Stmt::If(i) => {
			let mut b = vec![i.body.as_slice()];
			for clause in &i.elif_else_clauses {
				b.push(&clause.body);
			}
			b
		}
		Stmt::For(f) => {
			let mut b = vec![f.body.as_slice()];
			if !f.orelse.is_empty() {
				b.push(&f.orelse);
			}
			b
		}
		Stmt::While(w) => {
			let mut b = vec![w.body.as_slice()];
			if !w.orelse.is_empty() {
				b.push(&w.orelse);
			}
			b
		}
		Stmt::With(w) => vec![&w.body],
		Stmt::Try(t) => {
			let mut b = vec![t.body.as_slice()];
			for handler in &t.handlers {
				match handler {
					ruff_python_ast::ExceptHandler::ExceptHandler(h) => {
						b.push(&h.body);
					}
				}
			}
			if !t.orelse.is_empty() {
				b.push(&t.orelse);
			}
			if !t.finalbody.is_empty() {
				b.push(&t.finalbody);
			}
			b
		}
		Stmt::Match(m) => {
			let mut b = Vec::new();
			for case in &m.cases {
				b.push(case.body.as_slice());
			}
			b
		}
		_ => return Vec::new(),
	};

	if bodies.is_empty() {
		return Vec::new();
	}

	let mut children = Vec::new();
	let mut offset = stmt_start;

	for body in bodies {
		let (body_children, new_offset) = visit_body(body, offset);
		children.extend(body_children);
		offset = new_offset;
	}

	// Trailing trivia within the compound statement
	if offset < stmt_end {
		children.push(GreenNode::leaf(PySyntaxKind::Trivia, stmt_end - offset));
	}

	children
}

/// Build a GreenNode tree from a parsed Python module.
///
/// The resulting tree's `text_len()` is guaranteed to equal `source_len`
/// (the byte length of the source text that was parsed).
pub fn build_green_tree(parsed: &Parsed<Mod>, source_len: u32) -> GreenNode {
	let module = match parsed.syntax() {
		Mod::Module(m) => m,
		Mod::Expression(_) => {
			return GreenNode::leaf(PySyntaxKind::Module, source_len);
		}
	};

	let (mut children, offset) = visit_body(&module.body, 0);

	// Trailing trivia after the last statement
	if offset < source_len {
		children.push(GreenNode::leaf(PySyntaxKind::Trivia, source_len - offset));
	}

	GreenNode::branch(PySyntaxKind::Module, children)
}

// ============================================================
// Step 1: Relational Isomorphism Mapper
// ============================================================

/// Map a PySyntaxKind to the unified relational SemanticKind.
///
/// - RelationalTable: Scopes/blocks containing multiple statements
/// - RelationalRow: Single logical statements
/// - Token: Trivia leaves and terminal nodes
fn to_semantic_kind(kind: PySyntaxKind) -> SemanticKind {
	match kind {
		// Scopes / blocks → Table
		PySyntaxKind::Module
		| PySyntaxKind::FunctionDef
		| PySyntaxKind::ClassDef
		| PySyntaxKind::If
		| PySyntaxKind::For
		| PySyntaxKind::While
		| PySyntaxKind::With
		| PySyntaxKind::Try
		| PySyntaxKind::Match => SemanticKind::RelationalTable,

		// Single statements → Row
		PySyntaxKind::Import
		| PySyntaxKind::Assign
		| PySyntaxKind::Return
		| PySyntaxKind::Raise
		| PySyntaxKind::Assert
		| PySyntaxKind::Expr
		| PySyntaxKind::Pass
		| PySyntaxKind::Break
		| PySyntaxKind::Continue
		| PySyntaxKind::Delete
		| PySyntaxKind::Global
		| PySyntaxKind::Nonlocal
		| PySyntaxKind::TypeAlias
		| PySyntaxKind::Statement => SemanticKind::RelationalRow,

		// Trivia → Token
		PySyntaxKind::Trivia => SemanticKind::Token,
	}
}

/// Convert language-specific GreenNode to the trait-level GreenNode
/// used by the LanguageProvider::parse_structural_topology interface.
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

// ============================================================
// Step 2: UastRegistry Graft
// ============================================================

/// Count the total number of nodes in a GreenNode tree (for chunk reservation).
#[allow(dead_code)]
fn count_nodes(node: &GreenNode) -> u32 {
	1 + node.children.iter().map(count_nodes).sum::<u32>()
}

/// Count newlines in text for a given byte range.
#[cfg(test)]
fn count_newlines_in_slice(text: &[u8], offset: u32, len: u32) -> u32 {
	let start = offset as usize;
	let end = (offset + len).min(text.len() as u32) as usize;
	if start >= text.len() || start >= end {
		return 0;
	}
	memchr::memchr_iter(b'\n', &text[start..end]).count() as u32
}

/// Recursively graft a GreenNode tree into the UastRegistry.
/// Returns the NodeId of the grafted root.
///
/// `source` is the full source text — used to count newlines per span.
/// `base_offset` is the byte offset of this node within the source.
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
		0 // Branch nodes: newlines aggregated from children
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

	// Link to previous sibling
	if let Some(prev) = prev_sibling {
		unsafe {
			(*registry.edges[prev.index()].get()).next_sibling = Some(id);
		}
	}

	// Recurse into children
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

	// Set first_child and child_tail on the parent
	if let Some(fc) = first_child {
		unsafe {
			(*registry.edges[idx].get()).first_child = Some(fc);
			*registry.child_tails[idx].get() = prev_child;
		}
	}

	// For branch nodes, aggregate newlines from children
	if !node.children.is_empty() {
		unsafe {
			(*registry.metrics[idx].get()).newlines = total_child_newlines;
		}
	}

	id
}

// ============================================================
// Step 3: Splicing & Verification
// ============================================================

/// Apply a parsed Python GreenNode tree to the UastRegistry, replacing
/// the target node with the new structural tree.
///
/// - Translates GreenNode → relational SemanticKind
/// - Inserts nodes into the registry with proper LCRS topology
/// - Splices the new root into the target's position in the tree
/// - Stores the source text as virtual_data on leaf (Token) nodes
#[cfg(test)]
pub fn apply_python_semantic_update(
	registry: &UastRegistry,
	target_node_id: NodeId,
	parsed_tree: &GreenNode,
	source: &[u8],
) -> NodeId {
	let target_idx = target_node_id.index();

	// Capture the target's position in the tree before overwriting
	let (parent, next_sibling, prev_sibling) = unsafe {
		let edges = &*registry.edges[target_idx].get();
		let parent = edges.parent;
		let next_sib = edges.next_sibling;

		// Walk parent's children to find previous sibling
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

	// Graft the new tree into the registry
	let new_root = graft_recursive(registry, parsed_tree, parent, prev_sibling, source, 0);

	// Store source text as virtual_data on leaf nodes
	distribute_virtual_data(registry, new_root, source, 0);

	// Splice: link new_root into the target's position
	unsafe {
		// Point new root's next_sibling to target's old next_sibling
		(*registry.edges[new_root.index()].get()).next_sibling = next_sibling;

		if let Some(prev) = prev_sibling {
			// Previous sibling now points to new_root
			(*registry.edges[prev.index()].get()).next_sibling = Some(new_root);
		} else if let Some(p) = parent {
			// New root is the first child of parent
			(*registry.edges[p.index()].get()).first_child = Some(new_root);
		}

		// Update parent's child_tail if target was the last child
		if let Some(p) = parent {
			let tail = &mut *registry.child_tails[p.index()].get();
			if *tail == Some(target_node_id) {
				*tail = Some(new_root);
			}
		}

		// Disconnect the old target
		let old_edges = &mut *registry.edges[target_idx].get();
		old_edges.parent = None;
		old_edges.first_child = None;
		old_edges.next_sibling = None;
	}

	new_root
}

/// Distribute the source text across leaf (Token) nodes as virtual_data.
/// Each leaf gets the slice of source corresponding to its byte range.
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
		// Leaf node — store its slice of the source
		let byte_len = unsafe { (*registry.metrics[idx].get()).byte_length as usize };
		let start = offset as usize;
		let end = (start + byte_len).min(source.len());
		if start < source.len() {
			registry.replace_authoritative_data(node_id, source[start..end].to_vec());
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ruff_python_parser::{Mode, parse_unchecked};

	fn parse_and_build(source: &str) -> GreenNode {
		let parsed = parse_unchecked(source, Mode::Module);
		build_green_tree(&parsed, source.len() as u32)
	}

	#[test]
	fn length_matches_source_with_comments_and_whitespace() {
		let source = "def foo():\n    # comment\n    pass\n";
		let tree = parse_and_build(source);
		assert_eq!(
			tree.text_len(),
			source.len() as u32,
			"GreenNode text_len must match source byte length"
		);
	}

	#[test]
	fn length_matches_empty_source() {
		let source = "";
		let tree = parse_and_build(source);
		assert_eq!(tree.text_len(), 0);
	}

	#[test]
	fn length_matches_trailing_whitespace_only() {
		let source = "\n\n\n";
		let tree = parse_and_build(source);
		assert_eq!(tree.text_len(), source.len() as u32);
	}

	#[test]
	fn length_matches_multiple_functions() {
		let source = "def foo():\n    pass\n\ndef bar():\n    return 1\n";
		let tree = parse_and_build(source);
		assert_eq!(tree.text_len(), source.len() as u32);
	}

	#[test]
	fn length_matches_class_with_methods() {
		let source = "class Foo:\n    def __init__(self):\n        self.x = 1\n\n    def bar(self):\n        pass\n";
		let tree = parse_and_build(source);
		assert_eq!(tree.text_len(), source.len() as u32);
	}

	#[test]
	fn length_matches_if_elif_else() {
		let source = "if True:\n    pass\nelif False:\n    pass\nelse:\n    pass\n";
		let tree = parse_and_build(source);
		assert_eq!(tree.text_len(), source.len() as u32);
	}

	#[test]
	fn length_matches_try_except_finally() {
		let source = "try:\n    pass\nexcept ValueError:\n    pass\nfinally:\n    pass\n";
		let tree = parse_and_build(source);
		assert_eq!(tree.text_len(), source.len() as u32);
	}

	#[test]
	fn length_matches_imports_and_comments() {
		let source = "# header comment\nimport os\nimport sys  # inline comment\n\n# another comment\ndef main():\n    pass\n";
		let tree = parse_and_build(source);
		assert_eq!(tree.text_len(), source.len() as u32);
	}

	#[test]
	fn function_def_is_branch_with_children() {
		let source = "def foo():\n    pass\n";
		let tree = parse_and_build(source);
		// Module should contain a FunctionDef branch
		assert_eq!(tree.kind, PySyntaxKind::Module);
		assert!(
			tree.children
				.iter()
				.any(|c| c.kind == PySyntaxKind::FunctionDef)
		);
		let func = tree
			.children
			.iter()
			.find(|c| c.kind == PySyntaxKind::FunctionDef)
			.unwrap();
		assert!(
			!func.children.is_empty(),
			"FunctionDef should be a branch with children"
		);
	}

	#[test]
	fn leading_trivia_is_captured() {
		let source = "# comment\ndef foo():\n    pass\n";
		let tree = parse_and_build(source);
		assert_eq!(tree.children[0].kind, PySyntaxKind::Trivia);
		assert_eq!(tree.children[0].text_len(), 10); // "# comment\n"
	}

	#[test]
	fn length_matches_for_loop_with_else() {
		let source = "for x in range(10):\n    pass\nelse:\n    pass\n";
		let tree = parse_and_build(source);
		assert_eq!(tree.text_len(), source.len() as u32);
	}

	#[test]
	fn length_matches_match_statement() {
		let source = "match x:\n    case 1:\n        pass\n    case _:\n        pass\n";
		let tree = parse_and_build(source);
		assert_eq!(tree.text_len(), source.len() as u32);
	}

	// ============================================================
	// Integration tests: UastRegistry graft
	// ============================================================

	/// Helper: get SemanticKind of a node.
	fn kind_of(registry: &UastRegistry, id: NodeId) -> SemanticKind {
		unsafe { *registry.kinds[id.index()].get() }
	}

	/// Helper: get first child of a node.
	fn first_child(registry: &UastRegistry, id: NodeId) -> Option<NodeId> {
		unsafe { (*registry.edges[id.index()].get()).first_child }
	}

	/// Helper: get next sibling of a node.
	fn next_sibling(registry: &UastRegistry, id: NodeId) -> Option<NodeId> {
		unsafe { (*registry.edges[id.index()].get()).next_sibling }
	}

	/// Helper: collect children of a node.
	fn collect_children(registry: &UastRegistry, id: NodeId) -> Vec<NodeId> {
		let mut result = Vec::new();
		let mut cursor = first_child(registry, id);
		while let Some(c) = cursor {
			result.push(c);
			cursor = next_sibling(registry, c);
		}
		result
	}

	#[test]
	fn graft_def_foo_pass_into_registry() {
		let source = b"def foo():\n    pass\n";
		let parsed = parse_unchecked(std::str::from_utf8(source).unwrap(), Mode::Module);
		let green = build_green_tree(&parsed, source.len() as u32);

		// Create a minimal registry with a single unparsed text node
		let registry = UastRegistry::new(256);
		let root_id = registry.alloc_test_node();
		let target_id = registry.alloc_test_node();

		// Set up root -> target as a simple parent-child
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

		// Apply the graft
		apply_python_semantic_update(&registry, target_id, &green, source);

		// The old target should be disconnected
		assert!(
			first_child(&registry, target_id).is_none(),
			"old target must be disconnected"
		);

		// root's first child should now be the new Module (RelationalTable)
		let new_module =
			first_child(&registry, root_id).expect("root must have a child after graft");
		assert!(
			matches!(
				kind_of(&registry, new_module),
				SemanticKind::RelationalTable
			),
			"new root must be RelationalTable (Module)"
		);

		// Module's children: the FunctionDef (RelationalTable) + possible trailing trivia
		let module_children = collect_children(&registry, new_module);
		let func_node = module_children
			.iter()
			.find(|&&c| matches!(kind_of(&registry, c), SemanticKind::RelationalTable))
			.expect("Module must contain a RelationalTable (FunctionDef)");

		// FunctionDef's children: trivia header + pass (RelationalRow) + possible trailing trivia
		let func_children = collect_children(&registry, *func_node);
		assert!(
			func_children
				.iter()
				.any(|&c| matches!(kind_of(&registry, c), SemanticKind::RelationalRow)),
			"FunctionDef must contain a RelationalRow (pass statement)"
		);

		// Verify total byte length is preserved
		let module_bytes = unsafe { (*registry.metrics[new_module.index()].get()).byte_length };
		assert_eq!(
			module_bytes,
			source.len() as u64,
			"grafted module byte_length must match source"
		);
	}

	#[test]
	fn graft_preserves_virtual_data_on_leaves() {
		let source = b"pass\n";
		let parsed = parse_unchecked(std::str::from_utf8(source).unwrap(), Mode::Module);
		let green = build_green_tree(&parsed, source.len() as u32);

		let registry = UastRegistry::new(64);
		let target_id = registry.alloc_test_node();
		unsafe {
			*registry.kinds[target_id.index()].get() = SemanticKind::Token;
			*registry.metrics[target_id.index()].get() = SpanMetrics {
				byte_length: source.len() as u64,
				newlines: 0,
			};
		}

		apply_python_semantic_update(&registry, target_id, &green, source);

		// Find the leaf nodes and verify they have virtual_data
		// The Module should have a Row child ("pass\n")
		// Walk all nodes to find leaves with virtual_data
		let mut found_data = false;
		for i in 0..registry.allocated_node_count() as usize {
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
	/// accounted for by exactly one Trivia/leaf newline metric after the
	/// graft. Sums newlines from leaves only (branch nodes carry an
	/// aggregate that would otherwise double-count).
	#[test]
	fn graft_newlines_match_source_newline_count() {
		let source = b"\n\n    # test\n\ndef foo():\n    pass";
		let parsed = parse_unchecked(std::str::from_utf8(source).unwrap(), Mode::Module);
		let green = build_green_tree(&parsed, source.len() as u32);

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

		let new_root = apply_python_semantic_update(&registry, target_id, &green, source);

		let expected = memchr::memchr_iter(b'\n', source).count() as u64;

		// Sum across leaves only.
		let mut leaf_sum = 0u64;
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
		walk_leaves(&registry, new_root, &mut leaf_sum);

		assert_eq!(
			leaf_sum, expected,
			"sum of leaf newlines must equal {} (\\n count in source)",
			expected
		);

		// Aggregated newlines on the new root must also match.
		let aggregated = unsafe { (*registry.metrics[new_root.index()].get()).newlines };
		assert_eq!(
			aggregated, expected,
			"new_root aggregated newlines must equal {} (\\n count in source)",
			expected
		);
	}
}
