use std::sync::Arc;
use std::sync::mpsc;
use std::thread;

use crate::core::{BufferId, ByteLen, DocByte, RequestId, StateId};
use crate::engine::language::{GreenNode, InlineAnnotation, LanguageId, LanguageRegistry};
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

pub struct SemanticReactor {
	tx_in: Option<mpsc::Sender<SemanticRequest>>,
	rx_out: mpsc::Receiver<SemanticResponse>,
	worker: Option<thread::JoinHandle<()>>,
}

impl SemanticReactor {
	pub(crate) fn new(tx_internal: InternalSender, registry: Arc<LanguageRegistry>) -> Self {
		let (tx_in, rx_worker) = mpsc::channel::<SemanticRequest>();
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
			let _ = tx_in.send(request);
		}
	}

	/// Try to receive semantic highlights. Non-blocking.
	pub fn try_recv(&self) -> Option<SemanticResponse> {
		self.rx_out.try_recv().ok()
	}

	fn run_event_loop(
		rx: mpsc::Receiver<SemanticRequest>,
		tx: mpsc::Sender<SemanticResponse>,
		tx_internal: InternalSender,
		registry: Arc<LanguageRegistry>,
	) {
		while let Ok(request) = rx.recv() {
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
					let (highlights, diagnostics, inline_annotations) =
						match std::str::from_utf8(&source_bytes) {
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
	use crate::engine::language::{LanguageProvider, SemanticOutput};
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
		});
		reactor.send(SemanticRequest {
			buffer_id: BufferId::new(2),
			content: b"second".to_vec(),
			global_offset: DocByte::ZERO,
			file_path: "second.slow".to_string(),
			language_id: LanguageId::from_registered("slowtest"),
			state_id: StateId::new(2),
			request_id: RequestId::new(2),
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
