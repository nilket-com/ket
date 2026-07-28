use super::core::MAX_SEMANTIC_BYTES;
use super::document::io::read_document_snapshot;
use super::event_bus::EngineEventQueue;
use super::language::{LanguageId, LanguageRegistry};
use super::session::{
	BufferStore, CommonViewState, LayoutNode, SemanticState, TextEditorMut, with_text_buffer_mut,
};
use super::structural_graft::apply_graft;
use crate::core::{BufferId, DocByte, RequestId};
use crate::ecs::UastRegistry;
use crate::svp::semantic::{SemanticReactor, SemanticRequest, SemanticResponse};
use std::path::Path;
use std::sync::Arc;

pub(crate) struct SemanticPipelineParams<'a> {
	pub(crate) registry: &'a Arc<UastRegistry>,
	pub(crate) reactor: &'a SemanticReactor,
	pub(crate) lang_registry: &'a Arc<LanguageRegistry>,
	pub(crate) next_request_id: &'a mut RequestId,
	pub(crate) buffer_id: BufferId,
	pub(crate) root_id: crate::ecs::NodeId,
	pub(crate) file_path: Option<&'a str>,
	pub(crate) backing_path: Option<&'a str>,
	pub(crate) active_language_id: Option<LanguageId>,
	pub(crate) state_id: crate::core::StateId,
	pub(crate) semantic: &'a mut SemanticState,
	pub(crate) has_virtual_tokens: bool,
}

pub(crate) fn invalidate_minimap_cache(view: &mut CommonViewState) {
	view.reset_minimap_cache();
}

fn clear_for_unavailable_inputs(semantic: &mut SemanticState) {
	semantic.highlights.clear();
	semantic.diagnostics.clear();
	semantic.inline_annotations.clear();
	semantic.last_semantic_state = None;
	semantic.last_semantic_len = usize::MAX;
	semantic.last_semantic_path = None;
	semantic.pending_semantic_request_id = None;
}

fn allocate_semantic_request_id(next_request_id: &mut RequestId) -> RequestId {
	let request_id = *next_request_id;
	*next_request_id = (*next_request_id).next();
	request_id
}

fn apply_semantic_response(
	registry: &Arc<UastRegistry>,
	buffer_id: BufferId,
	doctab: &mut TextEditorMut<'_>,
	response: SemanticResponse,
) -> bool {
	let SemanticResponse {
		buffer_id: response_buffer_id,
		state_id,
		request_id,
		highlights,
		diagnostics,
		inline_annotations,
		structural_topology,
	} = response;
	let document = &mut doctab.document;

	if response_buffer_id != buffer_id
		|| Some(request_id) != document.semantic.pending_semantic_request_id
		|| state_id != document.ledger.current_state_id
	{
		return false;
	}

	document.semantic.highlights = highlights;
	document.semantic.diagnostics = diagnostics;
	document.semantic.inline_annotations = inline_annotations;
	document.semantic.view_revision = document.semantic.view_revision.wrapping_add(1);
	document.semantic.pending_semantic_request_id = None;
	invalidate_minimap_cache(&mut doctab.view.common);

	if let Some((green, source_bytes)) = structural_topology {
		let rid = document.root_id;
		let fold_snapshot = doctab.view.folds.snapshot(registry, rid);
		document.bump_topology_rev();

		// A graft that would not reproduce the document's bytes is refused, and the
		// document keeps the tree it has. The highlights above still stand: they are
		// offsets into unchanged bytes.
		if let Err(error) = apply_graft(registry, rid, &green, &source_bytes) {
			debug_assert!(false, "semantic graft rejected: {error}");
			return false;
		}
		doctab
			.view
			.folds
			.restore_exact(registry, rid, fold_snapshot);

		document.semantic.applied_structural_state_id = Some(state_id);
		document.semantic.cached_sum_tree = None;
		invalidate_minimap_cache(&mut doctab.view.common);
		return true;
	}
	false
}

pub(crate) fn dispatch_semantic_responses_in_layout(
	registry: &Arc<UastRegistry>,
	reactor: &SemanticReactor,
	buffers: &mut BufferStore,
	layout: &mut LayoutNode,
	events: &mut EngineEventQueue,
) -> bool {
	let mut grafted = false;
	while let Some(response) = reactor.try_recv() {
		let buffer_id = response.buffer_id;
		let view_id = layout.view_id_for_buffer(buffer_id);
		if let Some(view_id) = view_id
			&& with_text_buffer_mut(registry, buffers, layout, events, view_id, |doctab| {
				apply_semantic_response(registry, buffer_id, doctab, response)
			}) == Some(true)
		{
			grafted = true;
		}
	}
	grafted
}

pub(crate) fn drive_semantic_pipeline(params: SemanticPipelineParams<'_>) {
	let SemanticPipelineParams {
		registry,
		reactor,
		lang_registry,
		next_request_id,
		buffer_id,
		root_id,
		file_path,
		backing_path,
		active_language_id,
		state_id,
		semantic,
		has_virtual_tokens,
	} = params;

	if let Some(path) = file_path {
		let active_provider = active_language_id.and_then(|id| lang_registry.get_provider(id));
		if active_provider.is_some() {
			if has_virtual_tokens {
				{
					let rid = root_id;
					let live_len_wide = registry.get_total_bytes(rid);
					let live_len = usize::try_from(live_len_wide).unwrap_or(usize::MAX);
					let path_changed = semantic.last_semantic_path.as_deref() != Some(path);
					if path_changed
						|| semantic.last_semantic_state != Some(state_id)
						|| live_len != semantic.last_semantic_len
					{
						if live_len_wide == 0 || live_len_wide >= MAX_SEMANTIC_BYTES {
							semantic.highlights.clear();
							semantic.diagnostics.clear();
							semantic.inline_annotations.clear();
							semantic.last_semantic_state = Some(state_id);
							semantic.last_semantic_len = live_len;
							semantic.last_semantic_path = Some(path.to_string());
							semantic.pending_semantic_request_id = None;
						} else if let Ok(live_bytes) =
							read_document_snapshot(registry, rid, backing_path.map(Path::new))
						{
							let request_id = allocate_semantic_request_id(next_request_id);
							reactor.send(SemanticRequest {
								buffer_id,
								content: live_bytes,
								global_offset: DocByte::ZERO,
								file_path: path.to_string(),
								language_id: active_language_id.expect("active provider"),
								state_id,
								request_id,
							});
							semantic.pending_semantic_request_id = Some(request_id);
							semantic.last_semantic_state = Some(state_id);
							semantic.last_semantic_len = live_len;
							semantic.last_semantic_path = Some(path.to_string());
						}
					}
				}
			} else {
				semantic.highlights.clear();
				semantic.diagnostics.clear();
				semantic.inline_annotations.clear();
				semantic.last_semantic_state = Some(state_id);
				semantic.last_semantic_len = 0;
				semantic.last_semantic_path = Some(path.to_string());
				semantic.pending_semantic_request_id = None;
			}
		} else {
			clear_for_unavailable_inputs(semantic);
		}
	} else {
		clear_for_unavailable_inputs(semantic);
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::engine::document::io::read_loaded_document;
	use crate::engine::session::TestTextEditor;
	use crate::uast::kind::SemanticKind;
	use crate::uast::metrics::SpanMetrics;

	fn append_virtual_document(
		registry: &UastRegistry,
		text: &str,
	) -> (crate::ecs::NodeId, crate::ecs::NodeId) {
		let newlines = text.bytes().filter(|&byte| byte == b'\n').count() as u64;
		let mut chunk = registry.reserve_chunk(2).expect("OOM");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: text.len() as u64,
				newlines,
			},
		);
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: text.len() as u64,
				newlines,
			},
		);
		chunk.append_local_child(root, leaf);
		unsafe {
			*registry.virtual_data[leaf.index()].get() = Some(text.as_bytes().to_vec());
		}
		(root, leaf)
	}

	fn flat_green_node(text: &str) -> super::super::language::GreenNode {
		super::super::language::GreenNode {
			kind: SemanticKind::RelationalTable,
			provider_kind: crate::uast::ProviderKindId::NONE,
			byte_len: text.len() as u32,
			children: vec![super::super::language::GreenNode {
				kind: SemanticKind::Token,
				provider_kind: crate::uast::ProviderKindId::NONE,
				byte_len: text.len() as u32,
				children: Vec::new(),
			}],
		}
	}

	fn foldable_green_node() -> super::super::language::GreenNode {
		use super::super::language::GreenNode;

		GreenNode {
			kind: SemanticKind::RelationalTable,
			provider_kind: crate::uast::ProviderKindId::NONE,
			byte_len: 17,
			children: vec![
				GreenNode {
					kind: SemanticKind::Token,
					provider_kind: crate::uast::ProviderKindId::NONE,
					byte_len: 5,
					children: Vec::new(),
				},
				GreenNode {
					kind: SemanticKind::RelationalTable,
					provider_kind: crate::uast::ProviderKindId::NONE,
					byte_len: 8,
					children: vec![GreenNode {
						kind: SemanticKind::Token,
						provider_kind: crate::uast::ProviderKindId::NONE,
						byte_len: 8,
						children: Vec::new(),
					}],
				},
				GreenNode {
					kind: SemanticKind::Token,
					provider_kind: crate::uast::ProviderKindId::NONE,
					byte_len: 4,
					children: Vec::new(),
				},
			],
		}
	}

	fn test_settings() -> super::super::session::SettingsPersistent {
		super::super::session::SettingsPersistent {
			theme_name: "onedark".to_string(),
			minimap_enabled: false,
			wrap_enabled: false,
			line_numbers_enabled: true,
			relative_line_numbers_enabled: false,
			visible_whitespace: true,
			reload_enabled: true,
			save_durability: crate::engine::settings::SaveDurability::default(),
			ui_colors: crate::engine::UiColorOverrides::default(),
			language_themes: Default::default(),
		}
	}

	fn test_editor(
		registry: &UastRegistry,
		root: crate::ecs::NodeId,
		leaf: crate::ecs::NodeId,
		path: &str,
	) -> TestTextEditor {
		let mut fixture = TestTextEditor::new(registry, &test_settings(), 80, 24);
		{
			let editor = fixture.editor_mut();
			editor.document.root_id = root;
			editor.document.file_path = Some(path.to_string());
			editor.cursor.primary_mut().reset_to_start(
				leaf,
				editor.document.topology_rev,
				editor.document.ledger.current_state_id,
			);
		}
		fixture
	}

	fn dispatch_test_response(
		registry: &Arc<UastRegistry>,
		doctabs: &mut [TestTextEditor],
		response: SemanticResponse,
	) {
		let buffer_id = response.buffer_id;
		if let Some(fixture) = doctabs
			.iter_mut()
			.find(|fixture| fixture.buffer.id == buffer_id)
		{
			let mut editor = fixture.editor_mut();
			apply_semantic_response(registry, buffer_id, &mut editor, response);
		}
	}

	#[test]
	fn dispatch_semantic_response_routes_by_buffer_id_when_versions_match() {
		let registry = Arc::new(UastRegistry::new(16));
		let (root_a, leaf_a) = append_virtual_document(&registry, "alpha_body\n");
		let (root_b, leaf_b) = append_virtual_document(&registry, "bravo_body\n");
		let mut doctab_a = test_editor(&registry, root_a, leaf_a, "src/main.rs");
		let mut doctab_b = test_editor(&registry, root_b, leaf_b, "src/engine/core.rs");
		let buffer_b = BufferId::new(2);
		doctab_b.buffer.id = buffer_b;
		doctab_b.view.buffer_id = buffer_b;
		doctab_a
			.editor_mut()
			.document
			.semantic
			.pending_semantic_request_id = Some(RequestId::new(101));
		doctab_b
			.editor_mut()
			.document
			.semantic
			.pending_semantic_request_id = Some(RequestId::new(101));
		let state_a = doctab_a.editor_mut().document.ledger.current_state_id;
		assert_eq!(
			state_a,
			doctab_b.editor_mut().document.ledger.current_state_id
		);
		let mut doctabs = vec![doctab_a, doctab_b];

		dispatch_test_response(
			&registry,
			&mut doctabs,
			SemanticResponse {
				buffer_id: buffer_b,
				state_id: state_a,
				request_id: RequestId::new(101),
				highlights: Vec::new(),
				diagnostics: Vec::new(),
				inline_annotations: Vec::new(),
				structural_topology: Some((
					flat_green_node("amber_body\n"),
					b"amber_body\n".to_vec(),
				)),
			},
		);

		assert_eq!(
			doctabs[0]
				.editor_mut()
				.document
				.semantic
				.pending_semantic_request_id,
			Some(RequestId::new(101))
		);
		assert_eq!(
			doctabs[0].editor_mut().document.topology_rev,
			crate::core::TopologyRev::ZERO
		);
		assert_eq!(
			doctabs[1].editor_mut().document.topology_rev,
			crate::core::TopologyRev::new(1)
		);
		assert_eq!(
			doctabs[1]
				.editor_mut()
				.document
				.semantic
				.pending_semantic_request_id,
			None
		);
		assert_eq!(
			String::from_utf8(read_loaded_document(&registry, root_a).expect("read doctab A"))
				.expect("utf8 doctab A"),
			"alpha_body\n"
		);
		assert_eq!(
			String::from_utf8(read_loaded_document(&registry, root_b).expect("read doctab B"))
				.expect("utf8 doctab B"),
			"amber_body\n"
		);
	}

	#[test]
	fn dispatch_semantic_response_ignores_mismatched_state_id() {
		let registry = Arc::new(UastRegistry::new(8));
		let (root, leaf) = append_virtual_document(&registry, "fn stale() {}\n");
		let mut doctab = test_editor(&registry, root, leaf, "src/stale.rs");
		doctab
			.editor_mut()
			.document
			.semantic
			.pending_semantic_request_id = Some(RequestId::new(7));
		let original_state = doctab.editor_mut().document.ledger.current_state_id;
		let mut doctabs = vec![doctab];

		dispatch_test_response(
			&registry,
			&mut doctabs,
			SemanticResponse {
				buffer_id: BufferId::new(1),
				state_id: original_state.next(),
				request_id: RequestId::new(7),
				highlights: Vec::new(),
				diagnostics: Vec::new(),
				inline_annotations: Vec::new(),
				structural_topology: Some((
					flat_green_node("fn fresh() {}\n"),
					b"fn fresh() {}\n".to_vec(),
				)),
			},
		);

		assert_eq!(
			doctabs[0]
				.editor_mut()
				.document
				.semantic
				.pending_semantic_request_id,
			Some(RequestId::new(7))
		);
		assert_eq!(
			doctabs[0].editor_mut().document.topology_rev,
			crate::core::TopologyRev::ZERO
		);
		assert_eq!(
			String::from_utf8(read_loaded_document(&registry, root).expect("read doctab"))
				.expect("utf8 doctab"),
			"fn stale() {}\n"
		);
	}

	#[test]
	fn semantic_graft_leaves_byte_anchors_untouched() {
		use crate::engine::session::CursorSnapshot;

		let registry = Arc::new(UastRegistry::new(16));
		let text = "alpha\nbravo\n";
		let (root, leaf) = append_virtual_document(&registry, text);
		let mut doctab = test_editor(&registry, root, leaf, "src/main.rs");
		doctab
			.editor_mut()
			.document
			.semantic
			.pending_semantic_request_id = Some(RequestId::new(303));
		let (cursors_before, state) = {
			let editor = doctab.editor_mut();
			editor.cursor.restore_snapshot(CursorSnapshot::from_bytes(
				DocByte::new(2),
				vec![DocByte::new(8)],
			));
			(
				editor.cursor.snapshot(),
				editor.document.ledger.current_state_id,
			)
		};

		{
			let mut editor = doctab.editor_mut();
			assert!(apply_semantic_response(
				&registry,
				BufferId::new(1),
				&mut editor,
				SemanticResponse {
					buffer_id: BufferId::new(1),
					state_id: state,
					request_id: RequestId::new(303),
					highlights: Vec::new(),
					diagnostics: Vec::new(),
					inline_annotations: Vec::new(),
					structural_topology: Some((flat_green_node(text), text.as_bytes().to_vec())),
				},
			));
			assert_eq!(
				editor.document.semantic.applied_structural_state_id,
				Some(state)
			);
			assert_eq!(editor.cursor.snapshot(), cursors_before);
		}
	}

	#[test]
	fn semantic_graft_restores_view_fold_to_unique_exact_boundary() {
		use crate::engine::test_support::build_foldable_document;

		let (registry, root, old_fold) = build_foldable_document();
		let registry = Arc::new(registry);
		let leaf = registry.get_first_child(root).expect("foldable leaf");
		let mut doctab = test_editor(&registry, root, leaf, "table.csv");
		doctab
			.editor_mut()
			.document
			.semantic
			.pending_semantic_request_id = Some(RequestId::new(404));
		let state = {
			let editor = doctab.editor_mut();
			editor.view.folds.insert(old_fold);
			editor.document.ledger.current_state_id
		};

		{
			let mut editor = doctab.editor_mut();
			assert!(apply_semantic_response(
				&registry,
				BufferId::new(1),
				&mut editor,
				SemanticResponse {
					buffer_id: BufferId::new(1),
					state_id: state,
					request_id: RequestId::new(404),
					highlights: Vec::new(),
					diagnostics: Vec::new(),
					inline_annotations: Vec::new(),
					structural_topology: Some((
						foldable_green_node(),
						b"head\none\ntwo\ntail".to_vec(),
					)),
				},
			));
		}

		let restored = doctab.editor_mut().view.folds.iter().collect::<Vec<_>>();
		assert_eq!(restored.len(), 1);
		assert_ne!(restored[0], old_fold);
	}
}
