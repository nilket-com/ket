use std::sync::Arc;
use std::sync::mpsc;
use std::thread;

use crate::core::{BufferId, ByteLen, DocByte, RequestId, StateId};
use crate::engine::MAX_SEMANTIC_BYTES;
use crate::engine::language::{
	AnalysisDepth, DefinitionResult, GreenNode, InlineAnnotation, LanguageId, LanguageRegistry,
	normalize_definition_targets,
};
use crate::engine::navigation::{GotoDefinitionRequest, NavigationResolved};
use crate::engine::request::{InternalEvent, InternalSender};
use crate::svp::diagnostic::DiagnosticSpan;
use crate::svp::rust_highlight::HighlightSpan;

#[derive(Debug)]
pub struct SemanticRequest {
	pub buffer_id: BufferId,
	/// The document's authoritative bytes. Not a `String`: a document is not
	/// required to be valid UTF-8, and a lossy conversion here would be grafted
	/// back into the document as truth.
	pub content: Vec<u8>,
	pub global_offset: DocByte,
	pub file_path: String,
	pub language_id: LanguageId,
	pub state_id: StateId,
	pub request_id: RequestId,
	pub depth: AnalysisDepth,
}

#[derive(Debug)]
pub struct SemanticResponse {
	pub buffer_id: BufferId,
	pub state_id: StateId,
	pub request_id: RequestId,
	pub highlights: Vec<HighlightSpan>,
	pub diagnostics: Vec<DiagnosticSpan>,
	pub inline_annotations: Vec<InlineAnnotation>,
	/// Structural AST topology for deep graft, paired with the source bytes
	/// needed to distribute virtual_data on leaf nodes.
	pub structural_topology: Option<(GreenNode, Vec<u8>)>,
}

#[derive(Debug)]
enum ReactorRequest {
	Analyze(SemanticRequest),
	GotoDefinition(GotoDefinitionRequest),
}

pub struct SemanticReactor {
	tx_in: Option<mpsc::Sender<ReactorRequest>>,
	rx_out: mpsc::Receiver<SemanticResponse>,
	worker: Option<thread::JoinHandle<()>>,
}

impl SemanticReactor {
	pub(crate) fn new(tx_internal: InternalSender, registry: Arc<LanguageRegistry>) -> Self {
		let (tx_in, rx_worker) = mpsc::channel::<ReactorRequest>();
		let (tx_worker, rx_out) = mpsc::channel::<SemanticResponse>();

		let handle = thread::Builder::new()
			.name("semantic-reactor".into())
			.spawn(move || {
				Self::run_event_loop(rx_worker, tx_worker, tx_internal, registry);
			})
			.expect("failed to spawn semantic reactor thread");

		Self {
			tx_in: Some(tx_in),
			rx_out,
			worker: Some(handle),
		}
	}

	/// Push new file content to the reactor. Non-blocking.
	pub fn send(&self, request: SemanticRequest) {
		if let Some(tx_in) = &self.tx_in {
			let _ = tx_in.send(ReactorRequest::Analyze(request));
		}
	}

	pub(crate) fn send_goto_definition(&self, request: GotoDefinitionRequest) {
		if let Some(tx_in) = &self.tx_in {
			let _ = tx_in.send(ReactorRequest::GotoDefinition(request));
		}
	}

	/// Try to receive semantic highlights. Non-blocking.
	pub fn try_recv(&self) -> Option<SemanticResponse> {
		self.rx_out.try_recv().ok()
	}

	fn run_event_loop(
		rx: mpsc::Receiver<ReactorRequest>,
		tx: mpsc::Sender<SemanticResponse>,
		tx_internal: InternalSender,
		registry: Arc<LanguageRegistry>,
	) {
		while let Ok(work) = rx.recv() {
			let request = match work {
				ReactorRequest::Analyze(request) => request,
				ReactorRequest::GotoDefinition(request) => {
					let result = if request.content.len() as u64 >= MAX_SEMANTIC_BYTES {
						crate::engine::language::DefinitionResult::Failed(
							"Go to definition is unavailable for documents at or above 8 MiB"
								.to_string(),
						)
					} else {
						match String::from_utf8(request.content) {
							Ok(source) => registry.get_provider(request.language_id).map_or(
								crate::engine::language::DefinitionResult::Unsupported,
								|provider| {
									provider.goto_definition(
										Arc::new(source),
										&request.file_path,
										request.stamp.cursor,
									)
								},
							),
							Err(_) => crate::engine::language::DefinitionResult::Failed(
								"Go to definition requires valid UTF-8".to_string(),
							),
						}
					};
					let result = match result {
						DefinitionResult::Targets(targets) => {
							DefinitionResult::Targets(normalize_definition_targets(targets))
						}
						other => other,
					};
					let _ = tx_internal.send(InternalEvent::NavigationResolved(Box::new(
						NavigationResolved {
							stamp: request.stamp,
							result,
						},
					)));
					continue;
				}
			};
			let provider = registry.get_provider(request.language_id);

			let (highlights, diagnostics, inline_annotations, structural_topology) = match provider
			{
				Some(p) => {
					let source_bytes = request.content;

					// Structural topology is byte-oriented and must be parsed from the
					// document's own bytes, so the graft payload is byte-identical to what
					// the document already holds.
					let topology = p
						.parse_structural_topology(&source_bytes)
						.map(|green| (green, source_bytes.clone()));

					// Deep semantic analysis is string-oriented. A document that is not
					// valid UTF-8 forgoes it rather than being rewritten to suit it; it
					// still gets topology and lexical highlighting.
					let (highlights, diagnostics, inline_annotations) = match request.depth {
						AnalysisDepth::TopologyOnly => (Vec::new(), Vec::new(), Vec::new()),
						AnalysisDepth::Full => match std::str::from_utf8(&source_bytes) {
							Ok(text) => {
								let mut output = p.semantic_reactor(
									Arc::new(text.to_owned()),
									request.global_offset,
									&request.file_path,
								);
								// Only the parent describes injections. Calling this method on
								// injected providers would turn a syntax highlighter into an
								// unbounded recursive embedding system.
								for injection in p.injections(text) {
									let Some(fragment) = text.get(injection.range.clone()) else {
										continue;
									};
									let Some(embedded) = registry
										.resolve_supported_language_provider(
											&injection.language_id,
										)
									else {
										continue;
									};
									let embedded_offset = request.global_offset
										+ ByteLen::new(injection.range.start as u64);
									// A provider's semantic pass enriches its lexical baseline; it
									// does not necessarily repeat keywords and punctuation. Run both
									// here so a fenced block gets the same complete coloring as a
									// top-level document.
									output.highlights.extend(embedded.lexical_baseline(
										fragment.as_bytes(),
										&[(0, embedded_offset)],
									));
									let embedded_output = embedded.semantic_reactor(
										Arc::new(fragment.to_owned()),
										embedded_offset,
										&format!(
											"embedded.{}",
											embedded.extensions().first().copied().unwrap_or("txt")
										),
									);
									// Appending gives the embedded span precedence where a parent
									// Markdown span overlaps it; the frontend already resolves
									// overlapping categories from the end of this list.
									output.highlights.extend(embedded_output.highlights);
									output.diagnostics.extend(embedded_output.diagnostics);
									output
										.inline_annotations
										.extend(embedded_output.inline_annotations);
								}
								(
									output.highlights,
									output.diagnostics,
									output.inline_annotations,
								)
							}
							Err(_) => (Vec::new(), Vec::new(), Vec::new()),
						},
					};

					(highlights, diagnostics, inline_annotations, topology)
				}
				None => (Vec::new(), Vec::new(), Vec::new(), None),
			};

			let _ = tx.send(SemanticResponse {
				buffer_id: request.buffer_id,
				state_id: request.state_id,
				request_id: request.request_id,
				highlights,
				diagnostics,
				inline_annotations,
				structural_topology,
			});
			let _ = tx_internal.send(InternalEvent::Refresh);
		}
	}
}

impl Drop for SemanticReactor {
	fn drop(&mut self) {
		self.tx_in.take();
		if let Some(worker) = self.worker.take() {
			let _ = worker.join();
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ecs::NodeId;
	use crate::engine::language::{LanguageProvider, SemanticOutput};
	use crate::engine::navigation::{GotoDefinitionRequest, NavigationStamp};
	use crate::engine::request::{EngineRequest, InternalEvent};
	use crate::uast::{ProviderKindId, SemanticKind};
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::time::{Duration, Instant};

	struct SlowTestProvider;

	impl LanguageProvider for SlowTestProvider {
		fn language_id(&self) -> &'static str {
			"slowtest"
		}

		fn label(&self) -> &'static str {
			"SlowTest"
		}

		fn extensions(&self) -> &'static [&'static str] {
			&["slow"]
		}

		fn lexical_baseline(
			&self,
			_viewport_bytes: &[u8],
			_offset_map: &[(usize, DocByte)],
		) -> Vec<HighlightSpan> {
			Vec::new()
		}

		fn semantic_reactor(
			&self,
			_full_source: Arc<String>,
			_global_offset: DocByte,
			_file_path: &str,
		) -> SemanticOutput {
			std::thread::sleep(Duration::from_millis(40));
			SemanticOutput::default()
		}
	}

	struct CountingTestProvider {
		deep_calls: Arc<AtomicUsize>,
		injection_calls: Arc<AtomicUsize>,
	}

	impl LanguageProvider for CountingTestProvider {
		fn language_id(&self) -> &'static str {
			"counting"
		}

		fn label(&self) -> &'static str {
			"Counting"
		}

		fn extensions(&self) -> &'static [&'static str] {
			&["counting"]
		}

		fn lexical_baseline(
			&self,
			_viewport_bytes: &[u8],
			_offset_map: &[(usize, DocByte)],
		) -> Vec<HighlightSpan> {
			Vec::new()
		}

		fn semantic_reactor(
			&self,
			_full_source: Arc<String>,
			_global_offset: DocByte,
			_file_path: &str,
		) -> SemanticOutput {
			self.deep_calls.fetch_add(1, Ordering::Relaxed);
			SemanticOutput::default()
		}

		fn injections(&self, _source: &str) -> Vec<crate::engine::language::LanguageInjection> {
			self.injection_calls.fetch_add(1, Ordering::Relaxed);
			Vec::new()
		}

		fn parse_structural_topology(&self, source: &[u8]) -> Option<GreenNode> {
			Some(GreenNode {
				kind: SemanticKind::RelationalTable,
				provider_kind: ProviderKindId::NONE,
				byte_len: source.len() as u32,
				children: Vec::new(),
			})
		}
	}

	fn recv_semantic_response(reactor: &SemanticReactor) -> SemanticResponse {
		let deadline = Instant::now() + Duration::from_secs(2);
		loop {
			if let Some(response) = reactor.try_recv() {
				return response;
			}
			assert!(Instant::now() < deadline, "semantic response timed out");
			std::thread::sleep(Duration::from_millis(5));
		}
	}

	#[test]
	fn topology_only_returns_structure_without_calling_deep_or_injected_semantics() {
		let deep_calls = Arc::new(AtomicUsize::new(0));
		let injection_calls = Arc::new(AtomicUsize::new(0));
		let (_commands, _input, tx_internal, _requests) = crate::engine::request::channel();
		let mut registry = LanguageRegistry::new();
		registry.register(CountingTestProvider {
			deep_calls: deep_calls.clone(),
			injection_calls: injection_calls.clone(),
		});
		let reactor = SemanticReactor::new(tx_internal, Arc::new(registry));
		let content = b"structure only".to_vec();

		reactor.send(SemanticRequest {
			buffer_id: BufferId::new(1),
			content: content.clone(),
			global_offset: DocByte::ZERO,
			file_path: "snapshot.counting".to_string(),
			language_id: LanguageId::from_registered("counting"),
			state_id: StateId::new(1),
			request_id: RequestId::new(1),
			depth: AnalysisDepth::TopologyOnly,
		});
		let response = recv_semantic_response(&reactor);

		let (green, source_bytes) = response
			.structural_topology
			.expect("topology-only work must still return a provider tree");
		assert_eq!(green.byte_len, content.len() as u32);
		assert_eq!(source_bytes, content);
		assert!(response.highlights.is_empty());
		assert!(response.diagnostics.is_empty());
		assert!(response.inline_annotations.is_empty());
		assert_eq!(deep_calls.load(Ordering::Relaxed), 0);
		assert_eq!(injection_calls.load(Ordering::Relaxed), 0);
	}

	#[test]
	fn reactor_emits_a_response_for_each_enqueued_request() {
		let (_commands, _input, tx_internal, _requests) = crate::engine::request::channel();
		let mut registry = LanguageRegistry::new();
		registry.register(SlowTestProvider);
		let reactor = SemanticReactor::new(tx_internal, Arc::new(registry));

		reactor.send(SemanticRequest {
			buffer_id: BufferId::new(1),
			content: b"first".to_vec(),
			global_offset: DocByte::ZERO,
			file_path: "first.slow".to_string(),
			language_id: LanguageId::from_registered("slowtest"),
			state_id: StateId::new(1),
			request_id: RequestId::new(1),
			depth: AnalysisDepth::Full,
		});
		reactor.send(SemanticRequest {
			buffer_id: BufferId::new(2),
			content: b"second".to_vec(),
			global_offset: DocByte::ZERO,
			file_path: "second.slow".to_string(),
			language_id: LanguageId::from_registered("slowtest"),
			state_id: StateId::new(2),
			request_id: RequestId::new(2),
			depth: AnalysisDepth::Full,
		});

		let deadline = Instant::now() + Duration::from_secs(2);
		let mut response_ids = Vec::new();
		while Instant::now() < deadline && response_ids.len() < 2 {
			if let Some(response) = reactor.try_recv() {
				response_ids.push((response.buffer_id, response.request_id));
			} else {
				std::thread::sleep(Duration::from_millis(10));
			}
		}

		assert_eq!(
			response_ids,
			vec![
				(BufferId::new(1), RequestId::new(1)),
				(BufferId::new(2), RequestId::new(2)),
			]
		);
	}

	#[test]
	fn navigation_uses_the_internal_event_path_and_leaves_semantic_output_empty() {
		let (_commands, _input, tx_internal, requests) = crate::engine::request::channel();
		let mut registry = LanguageRegistry::new();
		registry.register(SlowTestProvider);
		let reactor = SemanticReactor::new(tx_internal, Arc::new(registry));
		let stamp = NavigationStamp {
			request_id: RequestId::new(9),
			buffer_id: BufferId::new(3),
			root: NodeId::from_index(7),
			state_id: StateId::new(4),
			view_id: crate::core::ViewId::new(5),
			cursor: DocByte::new(2),
		};

		reactor.send_goto_definition(GotoDefinitionRequest {
			stamp,
			content: b"hello".to_vec(),
			file_path: "test.slow".to_string(),
			language_id: LanguageId::from_registered("slowtest"),
		});

		let event = requests
			.recv_timeout(Duration::from_secs(2))
			.expect("navigation event");
		let EngineRequest::Internal(InternalEvent::NavigationResolved(resolved)) = event else {
			panic!("navigation must not use the semantic response path");
		};
		assert_eq!(resolved.stamp, stamp);
		assert_eq!(
			resolved.result,
			crate::engine::language::DefinitionResult::Unsupported
		);
		assert!(reactor.try_recv().is_none());
	}

	#[test]
	fn navigation_rejects_invalid_utf8_before_calling_a_provider() {
		let (_commands, _input, tx_internal, requests) = crate::engine::request::channel();
		let mut registry = LanguageRegistry::new();
		registry.register(SlowTestProvider);
		let reactor = SemanticReactor::new(tx_internal, Arc::new(registry));
		let stamp = NavigationStamp {
			request_id: RequestId::new(1),
			buffer_id: BufferId::new(1),
			root: NodeId::from_index(0),
			state_id: StateId::new(1),
			view_id: crate::core::ViewId::new(1),
			cursor: DocByte::ZERO,
		};
		reactor.send_goto_definition(GotoDefinitionRequest {
			stamp,
			content: vec![0xff],
			file_path: "test.slow".to_string(),
			language_id: LanguageId::from_registered("slowtest"),
		});
		let event = requests
			.recv_timeout(Duration::from_secs(2))
			.expect("navigation event");
		let EngineRequest::Internal(InternalEvent::NavigationResolved(resolved)) = event else {
			panic!("expected navigation result");
		};
		assert!(matches!(
			resolved.result,
			crate::engine::language::DefinitionResult::Failed(ref message)
				if message.contains("UTF-8")
		));
	}

	#[test]
	fn navigation_rejects_documents_at_the_semantic_size_limit() {
		let (_commands, _input, tx_internal, requests) = crate::engine::request::channel();
		let mut registry = LanguageRegistry::new();
		registry.register(SlowTestProvider);
		let reactor = SemanticReactor::new(tx_internal, Arc::new(registry));
		let stamp = NavigationStamp {
			request_id: RequestId::new(1),
			buffer_id: BufferId::new(1),
			root: NodeId::from_index(0),
			state_id: StateId::new(1),
			view_id: crate::core::ViewId::new(1),
			cursor: DocByte::ZERO,
		};
		reactor.send_goto_definition(GotoDefinitionRequest {
			stamp,
			content: vec![b'x'; MAX_SEMANTIC_BYTES as usize],
			file_path: "test.slow".to_string(),
			language_id: LanguageId::from_registered("slowtest"),
		});
		let event = requests
			.recv_timeout(Duration::from_secs(2))
			.expect("navigation event");
		let EngineRequest::Internal(InternalEvent::NavigationResolved(resolved)) = event else {
			panic!("expected navigation result");
		};
		assert!(matches!(
			resolved.result,
			DefinitionResult::Failed(ref message) if message.contains("8 MiB")
		));
	}

	#[test]
	fn reactor_merges_fenced_sql_highlights_in_parent_coordinates() {
		use crate::svp::markdown_provider::MarkdownProvider;
		use crate::svp::rust_highlight::TokenCategory;
		use crate::svp::sql_provider::SqlProvider;

		let source = "# Query\n\n```sql {#trips}\nselect id from trips;\n```\n";
		let sql_start = source.find("select").expect("SQL body") as u64;
		let (_commands, _input, tx_internal, _requests) = crate::engine::request::channel();
		let mut registry = LanguageRegistry::new();
		registry.register(MarkdownProvider);
		registry.register(SqlProvider);
		let reactor = SemanticReactor::new(tx_internal, Arc::new(registry));
		reactor.send(SemanticRequest {
			buffer_id: BufferId::new(1),
			content: source.as_bytes().to_vec(),
			global_offset: DocByte::new(500),
			file_path: "notebook.md".to_owned(),
			language_id: LanguageId::from_registered("markdown"),
			state_id: StateId::new(1),
			request_id: RequestId::new(1),
			depth: AnalysisDepth::Full,
		});

		let deadline = Instant::now() + Duration::from_secs(2);
		let response = loop {
			if let Some(response) = reactor.try_recv() {
				break response;
			}
			assert!(Instant::now() < deadline, "semantic response timed out");
			std::thread::sleep(Duration::from_millis(5));
		};
		assert!(response.highlights.iter().any(|span| {
			span.start == DocByte::new(500 + sql_start) && span.category == TokenCategory::Keyword
		}));
	}

	/// The Markdown pass classifies a fence body as code; the language inside it
	/// must still win. Existence of the embedded span is not enough — it has to
	/// resolve on top of the enclosing one, which is what span order buys.
	#[test]
	fn embedded_spans_outrank_the_enclosing_markdown_code_block() {
		use crate::svp::markdown_provider::MarkdownProvider;
		use crate::svp::rust_highlight::{HighlightSpanIndex, TokenCategory};
		use crate::svp::sql_provider::SqlProvider;

		let source = "# Query\n\n```sql\nselect id from trips;\n```\n";
		let sql_start = source.find("select").expect("SQL body") as u64;
		let (_commands, _input, tx_internal, _requests) = crate::engine::request::channel();
		let mut registry = LanguageRegistry::new();
		registry.register(MarkdownProvider);
		registry.register(SqlProvider);
		let reactor = SemanticReactor::new(tx_internal, Arc::new(registry));
		reactor.send(SemanticRequest {
			buffer_id: BufferId::new(1),
			content: source.as_bytes().to_vec(),
			global_offset: DocByte::ZERO,
			file_path: "notebook.md".to_owned(),
			language_id: LanguageId::from_registered("markdown"),
			state_id: StateId::new(1),
			request_id: RequestId::new(1),
			depth: AnalysisDepth::Full,
		});

		let deadline = Instant::now() + Duration::from_secs(2);
		let response = loop {
			if let Some(response) = reactor.try_recv() {
				break response;
			}
			assert!(Instant::now() < deadline, "semantic response timed out");
			std::thread::sleep(Duration::from_millis(5));
		};

		let index = HighlightSpanIndex::new(response.highlights);
		assert_eq!(
			index.category_at(DocByte::new(sql_start)),
			TokenCategory::Keyword,
			"SQL decides the color inside its own fence"
		);
		assert_eq!(
			index.category_at(DocByte::new(2)),
			TokenCategory::Type,
			"and the Markdown around it is unaffected"
		);
	}

	/// The reactor must return topology whose source bytes are exactly what it was
	/// sent, even when those bytes are not valid UTF-8. It previously received a
	/// `String` and handed back that string's (lossy) bytes.
	#[test]
	fn reactor_returns_non_utf8_source_bytes_unchanged() {
		use crate::svp::csv_provider::CsvProvider;

		// "José,Zürich" in latin-1 — not valid UTF-8.
		let raw = b"name,city\nJos\xe9,Z\xfcrich\n".to_vec();

		let (_commands, _input, tx_internal, _requests) = crate::engine::request::channel();
		let mut registry = LanguageRegistry::new();
		registry.register(CsvProvider);
		let reactor = SemanticReactor::new(tx_internal, Arc::new(registry));

		reactor.send(SemanticRequest {
			buffer_id: BufferId::new(1),
			content: raw.clone(),
			global_offset: DocByte::ZERO,
			file_path: "data.csv".to_string(),
			language_id: LanguageId::from_registered("csv"),
			state_id: StateId::new(1),
			request_id: RequestId::new(1),
			depth: AnalysisDepth::Full,
		});

		let deadline = Instant::now() + Duration::from_secs(2);
		let response = loop {
			if let Some(response) = reactor.try_recv() {
				break response;
			}
			assert!(Instant::now() < deadline, "reactor did not respond");
			std::thread::sleep(Duration::from_millis(10));
		};

		let (_green, source_bytes) = response
			.structural_topology
			.expect("csv yields structural topology");
		assert_eq!(
			source_bytes,
			raw,
			"reactor altered the document's bytes:\n  sent: {:?}\n  got:  {:?}",
			String::from_utf8_lossy(&raw),
			String::from_utf8_lossy(&source_bytes),
		);
	}
}
