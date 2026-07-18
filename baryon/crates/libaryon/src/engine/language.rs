use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::core::DocByte;
use crate::svp::diagnostic::DiagnosticSpan;
use crate::svp::rust_highlight::HighlightSpan;
use crate::uast::kind::SemanticKind;

// ============================================================
// SemanticOutput — Reactor result bundle
// ============================================================

/// Combined output from a semantic reactor pass: highlights and diagnostics.
#[derive(Debug, Default)]
pub struct SemanticOutput {
	pub highlights: Vec<HighlightSpan>,
	pub diagnostics: Vec<DiagnosticSpan>,
}

// ============================================================
// GreenNode — Structural AST Infrastructure
// ============================================================

/// Position-independent intermediate representation for structural AST mapping.
///
/// Devoid of string data by design: the tree captures only the relational
/// topology and the exact byte extent of each node. This makes it safe to
/// cache, diff, and transmit across threads without lifetime entanglement.
///
/// The semantic reactor produces a GreenNode tree on the background thread;
/// the core engine then applies a structural graft to splice it into the
/// live LCRS tree, replacing the flat token chain with deep structure.
#[derive(Debug, Clone)]
pub struct GreenNode {
	/// The relational mapping (e.g., RelationalTable, RelationalRow, Token).
	pub kind: SemanticKind,

	/// The EXACT byte length of this node, including all children.
	pub byte_len: u32,

	/// The nested structure of the AST. Empty if this is a leaf node.
	pub children: Vec<GreenNode>,
}

// ============================================================
// LanguageProvider Trait
// ============================================================

/// The central contract for all Baryon language plugins, driving the
/// Semantic Viewport Pipeline (SVP).
///
/// Implementors are stateless data transformation pipelines. They are
/// completely forbidden from mutating the engine's central memory (the
/// `UastRegistry`). They receive immutable slices or `Arc` snapshots and
/// return owned highlight data.
pub trait LanguageProvider: Send + Sync {
	/// A unique identifier for the language (e.g., "rust", "python").
	fn language_id(&self) -> &'static str;

	/// Human-readable label for the status bar (e.g., "Rust", "Python").
	fn label(&self) -> &'static str;

	/// File extensions this provider handles (e.g., &["rs"] or &["py", "pyi"]).
	fn extensions(&self) -> &'static [&'static str];

	// -----------------------------------------------------------------
	// PHASE 1: THE LEXICAL BASELINE (Synchronous Hot-Path)
	// -----------------------------------------------------------------
	// Instant, token-based highlighting running directly on the viewport.
	// Executes synchronously on the main thread during the render loop.
	// Must be error-resilient: smoothly step over invalid syntax and
	// synthetic code-folding placeholders (e.g., ❯❯❯).

	fn lexical_baseline(
		&self,
		viewport_bytes: &[u8],
		offset_map: &[(usize, DocByte)],
	) -> Vec<HighlightSpan>;

	// -----------------------------------------------------------------
	// PHASE 2: THE SEMANTIC REACTOR (Asynchronous/Background)
	// -----------------------------------------------------------------
	// Deep scope binding and type resolution.
	// Takes a read-only Arc snapshot of the full document, runs the heavy
	// AST walker, and yields absolute HighlightSpans.

	fn semantic_reactor(
		&self,
		full_source: Arc<String>,
		global_offset: DocByte,
		file_path: &str,
	) -> SemanticOutput;

	// -----------------------------------------------------------------
	// PHASE 3: STRUCTURAL MAPPING
	// -----------------------------------------------------------------
	// Produces a position-independent GreenNode tree for the full source.
	// The core engine applies the structural graft on the main thread.

	fn parse_structural_topology(&self, _source: &[u8]) -> Option<GreenNode> {
		None
	}
}

// ============================================================
// LanguageRegistry
// ============================================================

/// User-facing metadata for a registered document language.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LanguageInfo {
	pub id: &'static str,
	pub label: &'static str,
	pub extensions: &'static [&'static str],
}

/// Centralized routing mechanism for the core engine.
///
/// Maps language identifiers and file extensions to their `LanguageProvider`
/// implementations. Adding a new language requires only registering a new
/// provider — zero changes to core engine files.
pub struct LanguageRegistry {
	providers: HashMap<String, Box<dyn LanguageProvider>>,
	extension_map: HashMap<String, String>,
	internal_provider_ids: HashSet<String>,
}

impl Default for LanguageRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl LanguageRegistry {
	pub fn new() -> Self {
		Self {
			providers: HashMap::new(),
			extension_map: HashMap::new(),
			internal_provider_ids: HashSet::new(),
		}
	}

	pub fn register(&mut self, provider: impl LanguageProvider + 'static) {
		self.register_provider(provider, false);
	}

	/// Register a provider used by an engine-owned virtual document rather than
	/// by user documents. Internal providers remain available by id to the
	/// renderer, but are omitted from completion and language documentation.
	pub(crate) fn register_internal(&mut self, provider: impl LanguageProvider + 'static) {
		self.register_provider(provider, true);
	}

	fn register_provider(&mut self, provider: impl LanguageProvider + 'static, internal: bool) {
		let id = provider.language_id().to_string();
		for ext in provider.extensions() {
			self.extension_map.insert(ext.to_string(), id.clone());
		}
		if internal {
			self.internal_provider_ids.insert(id.clone());
		} else {
			self.internal_provider_ids.remove(&id);
		}
		self.providers.insert(id, Box::new(provider));
	}

	pub fn get_provider(&self, language_id: &str) -> Option<&dyn LanguageProvider> {
		self.providers.get(language_id).map(|p| p.as_ref())
	}

	pub fn provider_for_extension(&self, ext: &str) -> Option<&dyn LanguageProvider> {
		self.extension_map
			.get(ext)
			.and_then(|id| self.get_provider(id))
	}

	pub fn resolve_language_provider(
		&self,
		name_or_extension: &str,
	) -> Option<&dyn LanguageProvider> {
		self.get_provider(name_or_extension)
			.or_else(|| self.provider_for_extension(name_or_extension))
	}

	pub fn resolve_language_id(&self, name_or_extension: &str) -> Option<&'static str> {
		self.resolve_language_provider(name_or_extension)
			.map(|provider| provider.language_id())
	}

	/// Resolve a language exposed to user documents, excluding providers used
	/// only for engine-owned virtual documents.
	pub fn resolve_supported_language_provider(
		&self,
		name_or_extension: &str,
	) -> Option<&dyn LanguageProvider> {
		self.resolve_language_provider(name_or_extension)
			.filter(|provider| !self.internal_provider_ids.contains(provider.language_id()))
	}

	pub fn resolve_supported_language_id(&self, name_or_extension: &str) -> Option<&'static str> {
		self.resolve_supported_language_provider(name_or_extension)
			.map(|provider| provider.language_id())
	}

	pub fn completion_candidates(&self) -> Vec<String> {
		let mut candidates = self
			.providers
			.keys()
			.filter(|id| !self.internal_provider_ids.contains(*id))
			.cloned()
			.collect::<Vec<_>>();
		candidates.extend(
			self.extension_map
				.iter()
				.filter(|(_, id)| !self.internal_provider_ids.contains(*id))
				.map(|(extension, _)| extension.clone()),
		);
		candidates.sort();
		candidates.dedup();
		candidates
	}

	/// Sorted metadata for languages users can select with `:language` and use
	/// as canonical keys in per-language settings.
	pub fn supported_languages(&self) -> Vec<LanguageInfo> {
		let mut languages = self
			.providers
			.iter()
			.filter(|(id, _)| !self.internal_provider_ids.contains(*id))
			.map(|(_, provider)| LanguageInfo {
				id: provider.language_id(),
				label: provider.label(),
				extensions: provider.extensions(),
			})
			.collect::<Vec<_>>();
		languages.sort_by_key(|language| language.id);
		languages
	}

	pub fn language_ids(&self) -> impl Iterator<Item = &str> {
		self.providers.keys().map(|s| s.as_str())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	struct DummyProvider;

	impl LanguageProvider for DummyProvider {
		fn language_id(&self) -> &'static str {
			"dummy"
		}
		fn label(&self) -> &'static str {
			"Dummy"
		}
		fn extensions(&self) -> &'static [&'static str] {
			&["dum", "dmy"]
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
			SemanticOutput::default()
		}
	}

	#[test]
	fn registry_lookup_by_id() {
		let mut reg = LanguageRegistry::new();
		reg.register(DummyProvider);
		assert!(reg.get_provider("dummy").is_some());
		assert!(reg.get_provider("nonexistent").is_none());
	}

	#[test]
	fn registry_lookup_by_extension() {
		let mut reg = LanguageRegistry::new();
		reg.register(DummyProvider);
		let p = reg.provider_for_extension("dum").unwrap();
		assert_eq!(p.language_id(), "dummy");
		assert_eq!(p.label(), "Dummy");
		assert!(reg.provider_for_extension("xyz").is_none());
	}

	#[test]
	fn multiple_extensions_resolve_to_same_provider() {
		let mut reg = LanguageRegistry::new();
		reg.register(DummyProvider);
		let a = reg.provider_for_extension("dum").unwrap().language_id();
		let b = reg.provider_for_extension("dmy").unwrap().language_id();
		assert_eq!(a, b);
	}

	#[test]
	fn resolve_language_id_accepts_provider_id_or_extension() {
		let mut reg = LanguageRegistry::new();
		reg.register(DummyProvider);
		assert_eq!(reg.resolve_language_id("dummy"), Some("dummy"));
		assert_eq!(reg.resolve_language_id("dum"), Some("dummy"));
		assert_eq!(reg.resolve_language_id("dmy"), Some("dummy"));
		assert_eq!(reg.resolve_language_id("xyz"), None);
	}

	#[test]
	fn completion_candidates_include_provider_ids_and_extensions() {
		let mut reg = LanguageRegistry::new();
		reg.register(DummyProvider);
		assert_eq!(reg.completion_candidates(), vec!["dmy", "dum", "dummy"]);
	}

	#[test]
	fn internal_providers_are_hidden_from_user_facing_catalogs() {
		let mut reg = LanguageRegistry::new();
		reg.register_internal(DummyProvider);
		assert!(reg.get_provider("dummy").is_some());
		assert!(reg.resolve_supported_language_id("dummy").is_none());
		assert!(reg.resolve_supported_language_id("dum").is_none());
		assert!(reg.completion_candidates().is_empty());
		assert!(reg.supported_languages().is_empty());
	}

	#[test]
	fn supported_languages_include_provider_metadata() {
		let mut reg = LanguageRegistry::new();
		reg.register(DummyProvider);
		assert_eq!(
			reg.supported_languages(),
			vec![LanguageInfo {
				id: "dummy",
				label: "Dummy",
				extensions: &["dum", "dmy"],
			}]
		);
	}

	#[test]
	fn structural_topology_defaults_to_none() {
		let p = DummyProvider;
		assert!(p.parse_structural_topology(b"anything").is_none());
	}

	#[test]
	fn plaintext_files_get_no_provider() {
		let mut reg = LanguageRegistry::new();
		reg.register(DummyProvider);
		assert!(reg.provider_for_extension("txt").is_none());
		assert!(reg.provider_for_extension("").is_none());
	}
}
