use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct SymbolId {
	pub path: String,
	pub start: u32,
	pub end: u32,
	pub provider_kind: String,
}

#[derive(Debug, Clone)]
pub struct Symbol {
	pub id: SymbolId,
	pub name: String,
	pub name_offset: u32,
	pub file_index: usize,
	pub name_tokens: Vec<String>,
	pub span_identifiers: Vec<String>,
	pub literal_tokens: Vec<String>,
	pub context_tokens: Vec<String>,
	pub call_site_count: usize,
	pub callees: Vec<usize>,
	pub callers: Vec<usize>,
}

#[derive(Debug, Clone)]
pub struct SourceFile {
	pub relative_path: String,
	pub canonical_path: PathBuf,
	pub text: String,
}

#[derive(Debug, Clone)]
pub struct Corpus {
	pub root: PathBuf,
	pub files: Vec<SourceFile>,
	pub symbols: Vec<Symbol>,
	pub resolution: ResolutionStats,
	pub resolution_backend: Option<String>,
	pub resolution_error: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct ResolutionStats {
	pub call_sites: usize,
	pub resolved_inside_corpus: usize,
	pub unresolved_or_external: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureDepth {
	Name,
	Span,
	Callees,
	Callers,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct FeatureCell {
	pub depth: FeatureDepth,
	pub exclude_own_name: bool,
	pub include_literals: bool,
}

impl FeatureCell {
	pub fn label(self) -> String {
		format!(
			"{:?}/own-{}/literals-{}",
			self.depth,
			if self.exclude_own_name {
				"excluded"
			} else {
				"included"
			},
			if self.include_literals { "on" } else { "off" }
		)
		.to_lowercase()
	}
}

#[derive(Debug, Clone)]
pub struct FeatureDocument {
	pub symbol_index: usize,
	pub terms: BTreeMap<String, f64>,
}

#[derive(Debug, Clone)]
pub struct FeatureTokenStream {
	pub symbol_index: usize,
	pub tokens: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MethodConfig {
	pub extractor_version: u32,
	pub depths: Vec<FeatureDepthConfig>,
	pub own_name_exclusion: Vec<bool>,
	pub literal_channel: Vec<bool>,
	pub weights: FeatureWeights,
	pub bm25: Bm25Config,
	pub rank_sif: RankSifConfig,
	pub embedder: EmbedderConfig,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureDepthConfig {
	Name,
	Span,
	Callees,
	Callers,
}

impl From<FeatureDepthConfig> for FeatureDepth {
	fn from(value: FeatureDepthConfig) -> Self {
		match value {
			FeatureDepthConfig::Name => Self::Name,
			FeatureDepthConfig::Span => Self::Span,
			FeatureDepthConfig::Callees => Self::Callees,
			FeatureDepthConfig::Callers => Self::Callers,
		}
	}
}

#[derive(Debug, Clone, Deserialize)]
pub struct FeatureWeights {
	pub name: f64,
	pub span_identifier: f64,
	pub context: f64,
	pub literal: f64,
	pub callee: f64,
	pub caller: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Bm25Config {
	pub k1: f64,
	pub b: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RankSifConfig {
	pub a: f64,
	pub power_iterations: usize,
	pub frequency_ranked: bool,
	#[serde(default)]
	pub salience_k: Vec<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmbedderConfig {
	pub model_id: String,
	pub revision: String,
	pub license: String,
	pub max_sequence: usize,
	pub threads: usize,
	pub batch_size: usize,
	pub max_batch_size: usize,
	pub renderings: Vec<EmbedderRendering>,
	pub reference_tolerance: f64,
	pub artifacts: EmbedderArtifacts,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmbedderArtifacts {
	pub model_sha256: String,
	pub tokenizer_sha256: String,
	pub config_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbedderRendering {
	ChannelOrder,
	SalienceOrder,
	ChunkedMax,
}

impl EmbedderRendering {
	pub fn label(self) -> &'static str {
		match self {
			Self::ChannelOrder => "channel-order",
			Self::SalienceOrder => "salience-order",
			Self::ChunkedMax => "chunked-max",
		}
	}
}

#[derive(Debug, Clone, Deserialize)]
pub struct Pilot {
	pub metadata: PilotMetadata,
	pub probe: Vec<Probe>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PilotMetadata {
	pub contaminated: bool,
	pub purpose: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Probe {
	Canonical {
		id: String,
		query: String,
		expected: ExpectedSymbol,
	},
	Paraphrase {
		id: String,
		query: String,
		expected: ExpectedSymbol,
	},
	Distinction {
		id: String,
		query: String,
		preferred: ExpectedSymbol,
		over: ExpectedSymbol,
	},
	Negative {
		id: String,
		query: String,
	},
}

impl Probe {
	pub fn id(&self) -> &str {
		match self {
			Self::Canonical { id, .. }
			| Self::Paraphrase { id, .. }
			| Self::Distinction { id, .. }
			| Self::Negative { id, .. } => id,
		}
	}

	pub fn query(&self) -> &str {
		match self {
			Self::Canonical { query, .. }
			| Self::Paraphrase { query, .. }
			| Self::Distinction { query, .. }
			| Self::Negative { query, .. } => query,
		}
	}

	pub fn positive_expected(&self) -> Option<&ExpectedSymbol> {
		match self {
			Self::Canonical { expected, .. } | Self::Paraphrase { expected, .. } => Some(expected),
			Self::Distinction { .. } | Self::Negative { .. } => None,
		}
	}
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExpectedSymbol {
	pub path: String,
	pub name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RankedSymbol {
	pub id: SymbolId,
	pub name: String,
	pub score: f64,
}

pub fn add_weighted_terms(
	output: &mut BTreeMap<String, f64>,
	terms: impl IntoIterator<Item = String>,
	weight: f64,
) {
	if weight <= 0.0 {
		return;
	}
	for term in terms {
		*output.entry(term).or_default() += weight;
	}
}

pub fn split_identifier(identifier: &str) -> Vec<String> {
	let identifier = identifier.strip_prefix("r#").unwrap_or(identifier);
	let chars = identifier.chars().collect::<Vec<_>>();
	let mut words = Vec::new();
	let mut current = String::new();
	for (index, ch) in chars.iter().copied().enumerate() {
		if !ch.is_alphanumeric() {
			if !current.is_empty() {
				words.push(current.to_lowercase());
				current.clear();
			}
			continue;
		}
		let previous = index
			.checked_sub(1)
			.and_then(|previous| chars.get(previous));
		let next_is_lower = chars.get(index + 1).is_some_and(|next| next.is_lowercase());
		let camel_boundary = !current.is_empty()
			&& ch.is_uppercase()
			&& previous.is_some_and(|previous| {
				previous.is_lowercase()
					|| previous.is_numeric()
					|| previous.is_uppercase() && next_is_lower
			});
		if camel_boundary {
			words.push(current.to_lowercase());
			current.clear();
		}
		current.push(ch);
	}
	if !current.is_empty() {
		words.push(current.to_lowercase());
	}
	words
}

pub fn feature_documents(
	corpus: &Corpus,
	cell: FeatureCell,
	weights: &FeatureWeights,
) -> Vec<FeatureDocument> {
	corpus
		.symbols
		.iter()
		.enumerate()
		.map(|(symbol_index, symbol)| {
			let mut terms = BTreeMap::new();
			if !cell.exclude_own_name {
				add_weighted_terms(&mut terms, symbol.name_tokens.clone(), weights.name);
			}
			if cell.depth >= FeatureDepth::Span {
				add_weighted_terms(
					&mut terms,
					symbol
						.span_identifiers
						.iter()
						.filter(|identifier| !cell.exclude_own_name || *identifier != &symbol.name)
						.flat_map(|identifier| split_identifier(identifier)),
					weights.span_identifier,
				);
				add_weighted_terms(&mut terms, symbol.context_tokens.clone(), weights.context);
				if cell.include_literals {
					add_weighted_terms(&mut terms, symbol.literal_tokens.clone(), weights.literal);
				}
			}
			if cell.depth >= FeatureDepth::Callees {
				for callee in &symbol.callees {
					if cell.exclude_own_name && corpus.symbols[*callee].name == symbol.name {
						continue;
					}
					add_weighted_terms(
						&mut terms,
						corpus.symbols[*callee].name_tokens.clone(),
						weights.callee,
					);
				}
			}
			if cell.depth >= FeatureDepth::Callers {
				for caller in &symbol.callers {
					if cell.exclude_own_name && corpus.symbols[*caller].name == symbol.name {
						continue;
					}
					add_weighted_terms(
						&mut terms,
						corpus.symbols[*caller].name_tokens.clone(),
						weights.caller,
					);
				}
			}
			FeatureDocument {
				symbol_index,
				terms,
			}
		})
		.collect()
}

pub fn feature_token_streams(corpus: &Corpus, cell: FeatureCell) -> Vec<FeatureTokenStream> {
	corpus
		.symbols
		.iter()
		.enumerate()
		.map(|(symbol_index, symbol)| {
			let mut tokens = Vec::new();
			if !cell.exclude_own_name {
				tokens.extend(symbol.name_tokens.clone());
			}
			if cell.depth >= FeatureDepth::Span {
				tokens.extend(
					symbol
						.span_identifiers
						.iter()
						.filter(|identifier| !cell.exclude_own_name || *identifier != &symbol.name)
						.flat_map(|identifier| split_identifier(identifier)),
				);
				tokens.extend(symbol.context_tokens.clone());
				if cell.include_literals {
					tokens.extend(symbol.literal_tokens.clone());
				}
			}
			if cell.depth >= FeatureDepth::Callees {
				for callee in &symbol.callees {
					if cell.exclude_own_name && corpus.symbols[*callee].name == symbol.name {
						continue;
					}
					tokens.extend(corpus.symbols[*callee].name_tokens.clone());
				}
			}
			if cell.depth >= FeatureDepth::Callers {
				for caller in &symbol.callers {
					if cell.exclude_own_name && corpus.symbols[*caller].name == symbol.name {
						continue;
					}
					tokens.extend(corpus.symbols[*caller].name_tokens.clone());
				}
			}
			FeatureTokenStream {
				symbol_index,
				tokens,
			}
		})
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn own_name_and_literal_channels_toggle_independently() {
		let symbol = Symbol {
			id: SymbolId {
				path: "src/test.rs".to_string(),
				start: 0,
				end: 1,
				provider_kind: "FN".to_string(),
			},
			name: "render_frame".to_string(),
			name_offset: 0,
			file_index: 0,
			name_tokens: split_identifier("render_frame"),
			span_identifiers: vec!["render_frame".to_string(), "dispatch_jobs".to_string()],
			literal_tokens: vec!["watchdog".to_string()],
			context_tokens: Vec::new(),
			call_site_count: 0,
			callees: Vec::new(),
			callers: Vec::new(),
		};
		let corpus = Corpus {
			root: PathBuf::new(),
			files: Vec::new(),
			symbols: vec![symbol],
			resolution: ResolutionStats::default(),
			resolution_backend: None,
			resolution_error: None,
		};
		let weights = FeatureWeights {
			name: 1.0,
			span_identifier: 1.0,
			context: 1.0,
			literal: 1.0,
			callee: 1.0,
			caller: 1.0,
		};
		let without_literals = feature_documents(
			&corpus,
			FeatureCell {
				depth: FeatureDepth::Span,
				exclude_own_name: true,
				include_literals: false,
			},
			&weights,
		);
		assert!(!without_literals[0].terms.contains_key("render"));
		assert!(without_literals[0].terms.contains_key("dispatch"));
		assert!(!without_literals[0].terms.contains_key("watchdog"));

		let with_literals = feature_documents(
			&corpus,
			FeatureCell {
				depth: FeatureDepth::Span,
				exclude_own_name: true,
				include_literals: true,
			},
			&weights,
		);
		assert!(with_literals[0].terms.contains_key("watchdog"));

		let streams = feature_token_streams(
			&corpus,
			FeatureCell {
				depth: FeatureDepth::Span,
				exclude_own_name: true,
				include_literals: true,
			},
		);
		assert_eq!(streams[0].tokens, vec!["dispatch", "jobs", "watchdog"]);
	}
}
