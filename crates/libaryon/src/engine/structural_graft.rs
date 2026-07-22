//! Structural Graft — translates a LanguageProvider's GreenNode tree
//! into the UastRegistry's LCRS topology.
//!
//! The provider returns a position-independent `GreenNode` tree on the
//! background thread. The engine's main thread then calls `apply_graft`
//! to splice that structure into the live LCRS tree, replacing the flat
//! token chain with a deep structural representation.

use crate::ecs::id::NodeId;
use crate::ecs::registry::UastRegistry;
use crate::engine::language::GreenNode;
use crate::uast::kind::SemanticKind;
use crate::uast::metrics::SpanMetrics;
use crate::uast::topology::TARGET_TREE_FANOUT;

/// Why a graft was refused. A graft that does not preserve the document's bytes
/// exactly is a content mutation wearing a re-parse's clothes, and must never be
/// applied: carets, spans, and the save path all treat the leaves as truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraftError {
	/// The payload does not match the bytes the document currently holds.
	SourceLengthMismatch { document: u64, source: usize },
	/// The topology does not span the payload it was parsed from.
	TopologyLengthMismatch { topology: u32, source: usize },
	/// An interior node's children do not exactly cover its declared extent.
	TopologyCoverageMismatch { declared: u32, children: u64 },
}

impl std::fmt::Display for GraftError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::SourceLengthMismatch { document, source } => write!(
				f,
				"graft source is {source} bytes but the document holds {document}"
			),
			Self::TopologyLengthMismatch { topology, source } => write!(
				f,
				"graft topology spans {topology} bytes but its source is {source}"
			),
			Self::TopologyCoverageMismatch { declared, children } => write!(
				f,
				"graft node declares {declared} bytes but its children cover {children}"
			),
		}
	}
}

/// Validate a graft without mutating anything.
///
/// A graft replaces the leaves that every caret, span, and save reads from, so it
/// is only legal when it reproduces the document's bytes exactly. Checking this
/// before the old tree is detached means a rejected graft leaves the document
/// untouched rather than half-replaced — the same ordering as the backing-generation
/// reservation in `f6ec891`.
pub fn validate_graft(
	registry: &UastRegistry,
	root: NodeId,
	green: &GreenNode,
	source: &[u8],
) -> Result<(), GraftError> {
	let document = registry.get_total_bytes(root);
	if document != source.len() as u64 {
		return Err(GraftError::SourceLengthMismatch {
			document,
			source: source.len(),
		});
	}
	if green.byte_len as usize != source.len() {
		return Err(GraftError::TopologyLengthMismatch {
			topology: green.byte_len,
			source: source.len(),
		});
	}
	validate_coverage(green)
}

/// Every interior node's children must exactly cover its declared extent, or the
/// leaf offsets the graft distributes `virtual_data` at are meaningless.
fn validate_coverage(green: &GreenNode) -> Result<(), GraftError> {
	if green.children.is_empty() {
		return Ok(());
	}
	let children: u64 = green.children.iter().map(|c| c.byte_len as u64).sum();
	if children != green.byte_len as u64 {
		return Err(GraftError::TopologyCoverageMismatch {
			declared: green.byte_len,
			children,
		});
	}
	for child in &green.children {
		validate_coverage(child)?;
	}
	Ok(())
}

/// Graft a `GreenNode` tree into the registry, replacing all existing
/// children of `root`.
///
/// The graft must be byte-identical to the document it replaces; this is
/// enforced by [`validate_graft`] before anything is mutated. On `Err` the
/// registry is untouched.
///
/// Returns the NodeId of the grafted module node (first child of root).
pub fn apply_graft(
	registry: &UastRegistry,
	root: NodeId,
	green: &GreenNode,
	source: &[u8],
) -> Result<NodeId, GraftError> {
	// Reject before detaching anything. Past this point the graft cannot fail.
	validate_graft(registry, root, green, source)?;

	// The immutable source generation is being replaced by a semantic tree.
	// Detach old scan leaves before publishing the graft so late completions
	// cannot propagate stale newline deltas into the new root.
	registry.cancel_metric_scan(root);
	let mut old_child = registry.get_first_child(root);
	while let Some(node) = old_child {
		let next = registry.get_next_sibling(node);
		unsafe {
			(*registry.edges[node.index()].get()).parent = None;
		}
		old_child = next;
	}

	// 1. Recursively build the new LCRS subtree
	let new_module = graft_recursive(registry, green, Some(root), None, source, 0);

	// 2. Disconnect old children and install the new subtree
	unsafe {
		(*registry.edges[root.index()].get()).first_child = Some(new_module);
		*registry.child_tails[root.index()].get() = Some(new_module);
		// The root is the authoritative source for document-wide metrics. A
		// graft can win the race with the initial background newline scan, so
		// preserve the count computed from the new source-backed leaf tree.
		let module_metrics = *registry.metrics[new_module.index()].get();
		let root_metrics = &mut *registry.metrics[root.index()].get();
		debug_assert_eq!(root_metrics.byte_length, module_metrics.byte_length);
		root_metrics.newlines = module_metrics.newlines;
	}
	registry.remove_root_child_line_index(root);

	Ok(new_module)
}

/// Recursively allocate nodes in the registry for a GreenNode subtree.
///
/// `parent` — the parent NodeId in the LCRS tree.
/// `prev_sibling` — the previous sibling (for linking next_sibling).
/// `source` — the full source bytes for virtual_data distribution.
/// `base_offset` — byte offset of this node within the source.
fn graft_recursive(
	registry: &UastRegistry,
	node: &GreenNode,
	parent: Option<NodeId>,
	prev_sibling: Option<NodeId>,
	source: &[u8],
	base_offset: u32,
) -> NodeId {
	let byte_len = node.byte_len;
	let is_leaf = node.children.is_empty();
	let newlines = if is_leaf {
		count_newlines(source, base_offset, byte_len)
	} else {
		0 // Aggregated from children below
	};

	let id = allocate_graft_node(
		registry,
		node.kind,
		byte_len,
		u64::from(newlines),
		parent,
		prev_sibling,
	);
	let idx = id.index();

	if !is_leaf {
		let group_depth = grouping_depth(node.children.len());
		let children = graft_child_level(
			registry,
			&node.children,
			id,
			source,
			base_offset,
			group_depth,
		);
		unsafe {
			(*registry.edges[idx].get()).first_child = children.first;
			*registry.child_tails[idx].get() = children.last;
			(*registry.metrics[idx].get()).newlines = children.newlines;
		}
	}

	// Distribute virtual_data on leaf nodes
	if is_leaf {
		let start = base_offset as usize;
		let end = (start + byte_len as usize).min(source.len());
		if start < source.len() {
			registry.hot_swap_virtual_data(id, source[start..end].to_vec());
		}
	}

	id
}

#[derive(Default)]
struct GraftedChildLevel {
	first: Option<NodeId>,
	last: Option<NodeId>,
	byte_length: u64,
	newlines: u64,
}

fn allocate_graft_node(
	registry: &UastRegistry,
	kind: SemanticKind,
	byte_len: u32,
	newlines: u64,
	parent: Option<NodeId>,
	prev_sibling: Option<NodeId>,
) -> NodeId {
	let id = registry.alloc_node_internal();
	let idx = id.index();
	unsafe {
		*registry.kinds[idx].get() = kind;
		*registry.spans[idx].get() = None;
		*registry.metrics[idx].get() = SpanMetrics {
			byte_length: u64::from(byte_len),
			newlines,
		};
		let edges = &mut *registry.edges[idx].get();
		edges.parent = parent;
		edges.first_child = None;
		edges.next_sibling = None;
		*registry.child_tails[idx].get() = None;
		if let Some(prev) = prev_sibling {
			(*registry.edges[prev.index()].get()).next_sibling = Some(id);
		}
	}
	id
}

fn grouping_depth(child_count: usize) -> usize {
	let mut depth = 0usize;
	let mut bounded_capacity = TARGET_TREE_FANOUT;
	while child_count > bounded_capacity {
		depth += 1;
		let next = bounded_capacity.saturating_mul(TARGET_TREE_FANOUT);
		if next == bounded_capacity {
			break;
		}
		bounded_capacity = next;
	}
	depth
}

#[cfg(test)]
pub(crate) fn graft_allocation_count(node: &GreenNode) -> usize {
	let semantic_nodes = 1usize.saturating_add(
		node.children
			.iter()
			.map(graft_allocation_count)
			.sum::<usize>(),
	);
	let mut level_width = node.children.len();
	let mut groups = 0usize;
	while level_width > TARGET_TREE_FANOUT {
		level_width = level_width.div_ceil(TARGET_TREE_FANOUT);
		groups = groups.saturating_add(level_width);
	}
	semantic_nodes.saturating_add(groups)
}

fn child_span_at_group_depth(depth: usize) -> usize {
	(0..depth).fold(1usize, |span, _| span.saturating_mul(TARGET_TREE_FANOUT))
}

fn graft_child_level(
	registry: &UastRegistry,
	children: &[GreenNode],
	parent: NodeId,
	source: &[u8],
	base_offset: u32,
	group_depth: usize,
) -> GraftedChildLevel {
	let mut result = GraftedChildLevel::default();
	let mut child_offset = base_offset;

	if group_depth == 0 {
		for child in children {
			let child_id = graft_recursive(
				registry,
				child,
				Some(parent),
				result.last,
				source,
				child_offset,
			);
			result.first.get_or_insert(child_id);
			result.last = Some(child_id);
			result.byte_length += u64::from(child.byte_len);
			result.newlines += unsafe { (*registry.metrics[child_id.index()].get()).newlines };
			child_offset = child_offset.saturating_add(child.byte_len);
		}
		return result;
	}

	let child_span = child_span_at_group_depth(group_depth);
	for grouped_children in children.chunks(child_span) {
		let byte_length = grouped_children
			.iter()
			.map(|child| u64::from(child.byte_len))
			.sum::<u64>();
		let byte_len = u32::try_from(byte_length).expect("graft group byte length fits u32");
		let group = allocate_graft_node(
			registry,
			SemanticKind::Group,
			byte_len,
			0,
			Some(parent),
			result.last,
		);
		let grouped = graft_child_level(
			registry,
			grouped_children,
			group,
			source,
			child_offset,
			group_depth - 1,
		);
		debug_assert_eq!(grouped.byte_length, byte_length);
		unsafe {
			(*registry.edges[group.index()].get()).first_child = grouped.first;
			*registry.child_tails[group.index()].get() = grouped.last;
			(*registry.metrics[group.index()].get()).newlines = grouped.newlines;
		}

		result.first.get_or_insert(group);
		result.last = Some(group);
		result.byte_length += byte_length;
		result.newlines += grouped.newlines;
		child_offset = child_offset.saturating_add(byte_len);
	}

	result
}

fn count_newlines(text: &[u8], offset: u32, len: u32) -> u32 {
	let start = offset as usize;
	let end = (offset + len).min(text.len() as u32) as usize;
	if start >= text.len() || start >= end {
		return 0;
	}
	memchr::memchr_iter(b'\n', &text[start..end]).count() as u32
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::core::DocByte;
	use crate::svp::pointer::SvpPointer;
	use crate::uast::UastProjection;
	use crate::uast::kind::SemanticKind;

	/// "José" and "Zürich" in latin-1. Not valid UTF-8; an entirely ordinary CSV.
	const LATIN1_CSV: &[u8] = b"name,city\nJos\xe9,Z\xfcrich\n";

	fn build_flat_document(raw: &[u8]) -> (UastRegistry, NodeId) {
		let newlines = raw.iter().filter(|&&b| b == b'\n').count() as u64;
		let registry = UastRegistry::new(256);
		let root = registry.alloc_node_internal();
		let leaf = registry.alloc_node_internal();
		unsafe {
			*registry.kinds[root.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[root.index()].get() = SpanMetrics {
				byte_length: raw.len() as u64,
				newlines,
			};
			*registry.kinds[leaf.index()].get() = SemanticKind::Token;
			*registry.metrics[leaf.index()].get() = SpanMetrics {
				byte_length: raw.len() as u64,
				newlines,
			};
			(*registry.edges[root.index()].get()).first_child = Some(leaf);
			*registry.child_tails[root.index()].get() = Some(leaf);
		}
		registry.hot_swap_virtual_data(leaf, raw.to_vec());
		(registry, root)
	}

	fn read_document(registry: &UastRegistry, root: NodeId) -> Vec<u8> {
		let total = registry.get_total_bytes(root);
		registry
			.read_loaded_slice(root, DocByte::ZERO, DocByte::new(total))
			.expect("read document")
	}

	/// A semantic graft must never alter the document's bytes.
	///
	/// The reactor parses structural topology from the document's *own* bytes, so a
	/// document that is not valid UTF-8 still gets topology and comes back unchanged.
	/// Previously the engine sent a `String::from_utf8_lossy` of the live bytes and the
	/// reactor returned *that* String's bytes, so a latin-1 CSV was silently rewritten
	/// to U+FFFD — and, because the root's metrics still described the original length,
	/// truncated as well.
	#[test]
	fn semantic_graft_preserves_non_utf8_document_bytes() {
		use crate::engine::language::LanguageProvider;
		use crate::svp::csv_provider::CsvProvider;

		// The fixture is deliberately invalid UTF-8; that is the whole point.
		#[allow(invalid_from_utf8)]
		let is_non_utf8 = std::str::from_utf8(LATIN1_CSV).is_err();
		assert!(is_non_utf8, "fixture must be non-UTF-8");
		let (registry, root) = build_flat_document(LATIN1_CSV);

		// The production reactor path, byte-oriented throughout (svp/semantic.rs).
		let provider = CsvProvider;
		let green = provider
			.parse_structural_topology(LATIN1_CSV)
			.expect("csv provides a topology");
		let source_bytes = LATIN1_CSV.to_vec();

		apply_graft(&registry, root, &green, &source_bytes).expect("graft is byte-identical");

		let after = read_document(&registry, root);
		assert_eq!(
			after,
			LATIN1_CSV,
			"semantic graft rewrote the document's bytes:\n  before: {:?}\n   after: {:?}",
			String::from_utf8_lossy(LATIN1_CSV),
			String::from_utf8_lossy(&after),
		);
	}

	/// The lossy payload that used to reach `apply_graft` is now refused, and refused
	/// *before* the existing tree is detached.
	#[test]
	fn graft_rejects_a_payload_that_does_not_match_the_document() {
		use crate::engine::language::LanguageProvider;
		use crate::svp::csv_provider::CsvProvider;

		let (registry, root) = build_flat_document(LATIN1_CSV);

		// Exactly what the old pipeline produced: topology and bytes from a lossy String.
		let lossy = String::from_utf8_lossy(LATIN1_CSV).into_owned();
		let green = CsvProvider
			.parse_structural_topology(lossy.as_bytes())
			.expect("csv provides a topology");

		let error = apply_graft(&registry, root, &green, lossy.as_bytes())
			.expect_err("a graft that changes the document's bytes must be refused");
		assert_eq!(
			error,
			GraftError::SourceLengthMismatch {
				document: LATIN1_CSV.len() as u64,
				source: lossy.len(),
			}
		);

		assert_eq!(
			read_document(&registry, root),
			LATIN1_CSV,
			"a refused graft must leave the document untouched"
		);
	}

	/// A topology whose children do not cover their parent's extent is refused: the leaf
	/// offsets it would distribute `virtual_data` at are meaningless.
	#[test]
	fn graft_rejects_topology_that_does_not_cover_its_parent() {
		let source = b"hello world\n";
		let (registry, root) = build_flat_document(source);

		let green = GreenNode {
			kind: SemanticKind::RelationalTable,
			byte_len: source.len() as u32,
			children: vec![GreenNode {
				kind: SemanticKind::Token,
				byte_len: 4, // short of the 12 its parent declares
				children: Vec::new(),
			}],
		};

		let error = apply_graft(&registry, root, &green, source)
			.expect_err("incomplete coverage must be refused");
		assert_eq!(
			error,
			GraftError::TopologyCoverageMismatch {
				declared: source.len() as u32,
				children: 4,
			}
		);
		assert_eq!(read_document(&registry, root), source);
	}

	fn kind_of(registry: &UastRegistry, id: NodeId) -> SemanticKind {
		unsafe { *registry.kinds[id.index()].get() }
	}

	fn collect_children(registry: &UastRegistry, id: NodeId) -> Vec<NodeId> {
		let mut result = Vec::new();
		let mut cursor = registry.get_first_child(id);
		while let Some(c) = cursor {
			result.push(c);
			cursor = registry.get_next_sibling(c);
		}
		result
	}

	fn assert_bounded_fan_out(registry: &UastRegistry, root: NodeId) -> usize {
		let mut groups = 0usize;
		let mut stack = vec![root];
		while let Some(node) = stack.pop() {
			let children = collect_children(registry, node);
			assert!(
				children.len() <= TARGET_TREE_FANOUT,
				"node {node:?} has {} direct children",
				children.len()
			);
			for child in children {
				if kind_of(registry, child) == SemanticKind::Group {
					groups += 1;
				}
				stack.push(child);
			}
		}
		groups
	}

	#[test]
	fn graft_inserts_enough_group_levels_to_bound_every_sibling_chain() {
		const ROWS: usize = TARGET_TREE_FANOUT * TARGET_TREE_FANOUT + 1;
		let source = vec![b'x'; ROWS];
		let green = GreenNode {
			kind: SemanticKind::RelationalTable,
			byte_len: ROWS as u32,
			children: (0..ROWS)
				.map(|_| GreenNode {
					kind: SemanticKind::RelationalRow,
					byte_len: 1,
					children: Vec::new(),
				})
				.collect(),
		};
		let registry = UastRegistry::new((ROWS * 2 + 16) as u32);
		let root = registry.alloc_node_internal();
		unsafe {
			*registry.kinds[root.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[root.index()].get() = SpanMetrics {
				byte_length: ROWS as u64,
				newlines: 0,
			};
		}

		let module =
			apply_graft(&registry, root, &green, &source).expect("test graft is byte-identical");

		assert!(
			assert_bounded_fan_out(&registry, module) > TARGET_TREE_FANOUT,
			"a 64²+1 fan-out needs two synthetic group levels"
		);
		assert_eq!(registry.get_total_bytes(module), ROWS as u64);
		assert_eq!(registry.get_total_newlines(module), 0);
		assert_eq!(
			registry
				.read_loaded_slice(root, DocByte::ZERO, DocByte::new(ROWS as u64))
				.expect("grouped graft bytes"),
			source
		);
	}

	#[test]
	fn apply_graft_creates_deep_tree_from_green_node() {
		let source = b"def foo():\n    pass\n";
		let green = GreenNode {
			kind: SemanticKind::RelationalTable, // Module
			byte_len: source.len() as u32,
			children: vec![GreenNode {
				kind: SemanticKind::RelationalTable, // FunctionDef
				byte_len: source.len() as u32,
				children: vec![
					GreenNode {
						kind: SemanticKind::Token, // header trivia
						byte_len: 11,
						children: Vec::new(),
					},
					GreenNode {
						kind: SemanticKind::RelationalRow, // pass statement, incl. trailing "\n"
						byte_len: 9,
						children: Vec::new(),
					},
				],
			}],
		};

		let registry = UastRegistry::new(64);
		let root = registry.alloc_node_internal();
		let old_token = registry.alloc_node_internal();
		unsafe {
			*registry.kinds[root.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[root.index()].get() = SpanMetrics {
				byte_length: source.len() as u64,
				newlines: 1,
			};
			*registry.kinds[old_token.index()].get() = SemanticKind::Token;
			*registry.metrics[old_token.index()].get() = SpanMetrics {
				byte_length: source.len() as u64,
				newlines: 1,
			};
			(*registry.edges[old_token.index()].get()).parent = Some(root);
			(*registry.edges[root.index()].get()).first_child = Some(old_token);
			*registry.child_tails[root.index()].get() = Some(old_token);
		}

		apply_graft(&registry, root, &green, source).expect("test graft is byte-identical");

		// Root's child should now be the module (RelationalTable)
		let children = collect_children(&registry, root);
		assert_eq!(children.len(), 1);
		let module = children[0];
		assert!(matches!(
			kind_of(&registry, module),
			SemanticKind::RelationalTable
		));

		// Module's child should be the FunctionDef (RelationalTable)
		let mod_children = collect_children(&registry, module);
		assert_eq!(mod_children.len(), 1);
		let func = mod_children[0];
		assert!(matches!(
			kind_of(&registry, func),
			SemanticKind::RelationalTable
		));

		// FunctionDef children: Token + RelationalRow
		let func_children = collect_children(&registry, func);
		assert_eq!(func_children.len(), 2);
		assert!(matches!(
			kind_of(&registry, func_children[0]),
			SemanticKind::Token
		));
		assert!(matches!(
			kind_of(&registry, func_children[1]),
			SemanticKind::RelationalRow
		));
	}

	#[test]
	fn grafted_tree_newlines_match_source() {
		let source = b"line1\nline2\nline3\n";
		let green = GreenNode {
			kind: SemanticKind::RelationalTable,
			byte_len: source.len() as u32,
			children: vec![
				GreenNode {
					kind: SemanticKind::Token,
					byte_len: 6, // "line1\n"
					children: Vec::new(),
				},
				GreenNode {
					kind: SemanticKind::Token,
					byte_len: 6, // "line2\n"
					children: Vec::new(),
				},
				GreenNode {
					kind: SemanticKind::Token,
					byte_len: 6, // "line3\n"
					children: Vec::new(),
				},
			],
		};

		let registry = UastRegistry::new(64);
		let root = registry.alloc_node_internal();
		unsafe {
			*registry.kinds[root.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[root.index()].get() = SpanMetrics {
				byte_length: source.len() as u64,
				newlines: 3,
			};
		}

		let module =
			apply_graft(&registry, root, &green, source).expect("test graft is byte-identical");

		// Module should have 3 newlines total
		let module_newlines = unsafe { (*registry.metrics[module.index()].get()).newlines };
		assert_eq!(module_newlines, 3);
	}

	#[test]
	fn graft_completes_root_metrics_when_initial_scan_is_still_pending() {
		let source = b"line1\nline2\nline3\n";
		let green = GreenNode {
			kind: SemanticKind::RelationalTable,
			byte_len: source.len() as u32,
			children: vec![GreenNode {
				kind: SemanticKind::Token,
				byte_len: source.len() as u32,
				children: Vec::new(),
			}],
		};

		let registry = UastRegistry::new(8);
		let mut chunk = registry.reserve_chunk(2).expect("registry chunk");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: source.len() as u64,
				newlines: 0,
			},
		);
		let cold_leaf = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: 0,
				byte_length: source.len() as u32,
				device_id: 1,
				head_trim: 0,
			}),
			SpanMetrics {
				byte_length: source.len() as u64,
				newlines: 0,
			},
		);
		chunk.append_local_child(root, cold_leaf);
		registry.begin_metric_scan(root, cold_leaf, cold_leaf);

		assert_eq!(registry.exact_line_count(root), None);
		let module =
			apply_graft(&registry, root, &green, source).expect("test graft is byte-identical");

		assert_eq!(registry.get_total_newlines(module), 3);
		assert_eq!(registry.get_total_newlines(root), 3);
		assert_eq!(registry.exact_line_count(root), Some(4));
		assert_eq!(registry.stats().pending_scan_blocks, 0);
	}

	#[test]
	fn grafted_tree_supports_spatial_query() {
		let source = b"aaaa\nbbbbbbbbbb\n";
		let green = GreenNode {
			kind: SemanticKind::RelationalTable,
			byte_len: source.len() as u32,
			children: vec![
				GreenNode {
					kind: SemanticKind::RelationalRow,
					byte_len: 5,
					children: Vec::new(),
				},
				GreenNode {
					kind: SemanticKind::RelationalTable, // function
					byte_len: 11,
					children: vec![GreenNode {
						kind: SemanticKind::Token,
						byte_len: 11,
						children: Vec::new(),
					}],
				},
			],
		};

		let registry = UastRegistry::new(64);
		let root = registry.alloc_node_internal();
		unsafe {
			*registry.kinds[root.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[root.index()].get() = SpanMetrics {
				byte_length: source.len() as u64,
				newlines: 2,
			};
		}

		apply_graft(&registry, root, &green, source).expect("test graft is byte-identical");

		// Query at offset 7 (inside "function") should find the Token leaf
		let target = registry.find_node_at_doc_byte_raw(root, DocByte::new(7));
		assert_eq!(target.node_start_byte, DocByte::new(5));
		assert_eq!(target.node_end_byte, DocByte::new(16));
	}

	#[test]
	fn grafted_tree_sum_tree_has_correct_line_count() {
		use crate::engine::index::SumTree;

		// Source with 3 functions separated by blank lines, total 10 newlines
		let source = b"fn a() {}\n\nfn b() {\n    x\n}\n\nfn c() {\n    y\n}\n\n";
		let expected_newlines = source.iter().filter(|&&b| b == b'\n').count() as u64;

		let green = crate::engine::language::GreenNode {
			kind: SemanticKind::RelationalTable,
			byte_len: source.len() as u32,
			children: vec![
				// fn a() {}\n
				crate::engine::language::GreenNode {
					kind: SemanticKind::RelationalTable,
					byte_len: 10,
					children: vec![crate::engine::language::GreenNode {
						kind: SemanticKind::Token,
						byte_len: 10,
						children: Vec::new(),
					}],
				},
				// \n
				crate::engine::language::GreenNode {
					kind: SemanticKind::Token,
					byte_len: 1,
					children: Vec::new(),
				},
				// fn b() {\n    x\n}\n
				crate::engine::language::GreenNode {
					kind: SemanticKind::RelationalTable,
					byte_len: 17,
					children: vec![crate::engine::language::GreenNode {
						kind: SemanticKind::Token,
						byte_len: 17,
						children: Vec::new(),
					}],
				},
				// \n
				crate::engine::language::GreenNode {
					kind: SemanticKind::Token,
					byte_len: 1,
					children: Vec::new(),
				},
				// fn c() {\n    y\n}\n
				crate::engine::language::GreenNode {
					kind: SemanticKind::RelationalTable,
					byte_len: 17,
					children: vec![crate::engine::language::GreenNode {
						kind: SemanticKind::Token,
						byte_len: 17,
						children: Vec::new(),
					}],
				},
				// \n
				crate::engine::language::GreenNode {
					kind: SemanticKind::Token,
					byte_len: 1,
					children: Vec::new(),
				},
			],
		};

		let registry = UastRegistry::new(128);
		let root = registry.alloc_node_internal();
		unsafe {
			*registry.kinds[root.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[root.index()].get() = SpanMetrics {
				byte_length: source.len() as u64,
				newlines: expected_newlines,
			};
		}

		apply_graft(&registry, root, &green, source).expect("test graft is byte-identical");

		let sum_tree = SumTree::build_from_registry(&registry, root);
		let doc_line_count = sum_tree.doc_line_count();
		// doc_line_count includes a final-line entry, so for N newlines we
		// get N+1 entries (one per line, including the last line after final \n)
		assert!(
			doc_line_count.get() >= expected_newlines,
			"sum tree doc line count ({doc_line_count}) must be >= expected newlines ({expected_newlines})"
		);
	}

	#[test]
	fn graft_real_rust_file_newline_consistency() {
		use crate::svp::rust_provider::build_rust_green_tree;

		let source = std::fs::read("src/engine/structural_graft.rs")
			.expect("should read structural_graft.rs");
		let text = std::str::from_utf8(&source).unwrap();
		let expected_newlines = source.iter().filter(|&&b| b == b'\n').count() as u64;

		let green = build_rust_green_tree(text);
		assert_eq!(green.byte_len as usize, source.len());

		let registry = UastRegistry::new(
			graft_allocation_count(&green)
				.saturating_add(1)
				.try_into()
				.expect("test graft fits u32 registry capacity"),
		);
		let root = registry.alloc_node_internal();
		unsafe {
			*registry.kinds[root.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[root.index()].get() = SpanMetrics {
				byte_length: source.len() as u64,
				newlines: expected_newlines,
			};
		}

		let module =
			apply_graft(&registry, root, &green, &source).expect("test graft is byte-identical");
		let module_metrics = unsafe { *registry.metrics[module.index()].get() };

		assert_eq!(
			module_metrics.byte_length as usize,
			source.len(),
			"module byte_length mismatch"
		);
		assert_eq!(
			module_metrics.newlines, expected_newlines,
			"module newlines ({}) != expected ({})",
			module_metrics.newlines, expected_newlines
		);
	}
}
