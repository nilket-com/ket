use crate::model::{Bm25Config, Corpus, FeatureDocument, RankedSymbol, split_identifier};
use std::collections::BTreeMap;

pub struct Bm25Index<'a> {
	corpus: &'a Corpus,
	documents: &'a [FeatureDocument],
	config: &'a Bm25Config,
	document_frequency: BTreeMap<String, usize>,
	average_length: f64,
}

impl<'a> Bm25Index<'a> {
	pub fn new(
		corpus: &'a Corpus,
		documents: &'a [FeatureDocument],
		config: &'a Bm25Config,
	) -> Self {
		let mut document_frequency = BTreeMap::new();
		let mut total_length = 0.0;
		for document in documents {
			for term in document.terms.keys() {
				*document_frequency.entry(term.clone()).or_default() += 1;
			}
			total_length += document.terms.values().sum::<f64>();
		}
		let average_length = if documents.is_empty() {
			0.0
		} else {
			total_length / documents.len() as f64
		};
		Self {
			corpus,
			documents,
			config,
			document_frequency,
			average_length,
		}
	}

	pub fn rank(&self, query: &str) -> Vec<RankedSymbol> {
		let query_terms = query
			.split(|character: char| !character.is_alphanumeric() && character != '_')
			.flat_map(split_identifier)
			.collect::<Vec<_>>();
		let count = self.documents.len() as f64;
		let mut ranked = Vec::new();
		for document in self.documents {
			let length = document.terms.values().sum::<f64>();
			let mut score = 0.0;
			for query_term in &query_terms {
				let Some(tf) = document.terms.get(query_term).copied() else {
					continue;
				};
				let df = *self.document_frequency.get(query_term).unwrap_or(&0) as f64;
				let idf = (1.0 + (count - df + 0.5) / (df + 0.5)).ln();
				let normalization = if self.average_length > 0.0 {
					1.0 - self.config.b + self.config.b * length / self.average_length
				} else {
					1.0
				};
				score +=
					idf * (tf * (self.config.k1 + 1.0)) / (tf + self.config.k1 * normalization);
			}
			if score > 0.0 {
				let symbol = &self.corpus.symbols[document.symbol_index];
				ranked.push(RankedSymbol {
					id: symbol.id.clone(),
					name: symbol.name.clone(),
					score,
				});
			}
		}
		ranked.sort_by(|left, right| {
			right
				.score
				.total_cmp(&left.score)
				.then_with(|| left.id.cmp(&right.id))
		});
		ranked
	}
}

pub fn logical_index_bytes(documents: &[FeatureDocument]) -> usize {
	documents
		.iter()
		.map(|document| {
			std::mem::size_of::<usize>()
				+ document
					.terms
					.keys()
					.map(|term| term.len() + std::mem::size_of::<f64>())
					.sum::<usize>()
		})
		.sum()
}

pub fn select_salient_terms(
	documents: &[FeatureDocument],
	config: &Bm25Config,
	limit: usize,
) -> Vec<FeatureDocument> {
	assert!(limit > 0, "salience limit must be nonzero");
	let count = documents.len() as f64;
	let mut document_frequency = BTreeMap::new();
	let mut total_length = 0.0;
	for document in documents {
		for term in document.terms.keys() {
			*document_frequency.entry(term.as_str()).or_insert(0usize) += 1;
		}
		total_length += document.terms.values().sum::<f64>();
	}
	let average_length = if documents.is_empty() {
		0.0
	} else {
		total_length / documents.len() as f64
	};
	documents
		.iter()
		.map(|document| {
			let length = document.terms.values().sum::<f64>();
			let normalization = if average_length > 0.0 {
				1.0 - config.b + config.b * length / average_length
			} else {
				1.0
			};
			let mut scored = document
				.terms
				.iter()
				.map(|(term, tf)| {
					let df = *document_frequency.get(term.as_str()).unwrap_or(&0) as f64;
					let idf = (1.0 + (count - df + 0.5) / (df + 0.5)).ln();
					let score = idf * (tf * (config.k1 + 1.0)) / (tf + config.k1 * normalization);
					(term, score)
				})
				.collect::<Vec<_>>();
			scored.sort_by(|(left_term, left_score), (right_term, right_score)| {
				right_score
					.total_cmp(left_score)
					.then_with(|| left_term.cmp(right_term))
			});
			let terms = scored
				.into_iter()
				.take(limit)
				.map(|(term, _)| (term.clone(), document.terms[term]))
				.collect();
			FeatureDocument {
				symbol_index: document.symbol_index,
				terms,
			}
		})
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::model::{
		Corpus, FeatureCell, FeatureDepth, FeatureWeights, ResolutionStats, SourceFile, Symbol,
		SymbolId, feature_documents,
	};
	use std::path::PathBuf;

	#[test]
	fn bm25_prefers_the_document_with_the_behavioral_query_term() {
		let symbols = ["dispatch_semantic_responses", "mutate_authoritative_data"]
			.into_iter()
			.enumerate()
			.map(|(index, name)| Symbol {
				id: SymbolId {
					path: "src/test.rs".into(),
					start: index as u32,
					end: index as u32 + 1,
					provider_kind: "FN".into(),
				},
				name: name.into(),
				name_offset: 0,
				file_index: 0,
				name_tokens: split_identifier(name),
				span_identifiers: Vec::new(),
				literal_tokens: Vec::new(),
				context_tokens: Vec::new(),
				call_site_count: 0,
				callees: Vec::new(),
				callers: Vec::new(),
			})
			.collect();
		let corpus = Corpus {
			root: PathBuf::new(),
			files: vec![SourceFile {
				relative_path: "src/test.rs".into(),
				canonical_path: PathBuf::new(),
				text: String::new(),
			}],
			symbols,
			resolution: ResolutionStats::default(),
			resolution_backend: None,
			resolution_error: None,
		};
		let documents = feature_documents(
			&corpus,
			FeatureCell {
				depth: FeatureDepth::Name,
				exclude_own_name: false,
				include_literals: false,
			},
			&FeatureWeights {
				name: 1.0,
				span_identifier: 1.0,
				context: 1.0,
				literal: 1.0,
				callee: 1.0,
				caller: 1.0,
			},
		);
		let config = Bm25Config { k1: 1.2, b: 0.75 };
		let ranked = Bm25Index::new(&corpus, &documents, &config).rank("dispatch");
		assert_eq!(ranked[0].name, "dispatch_semantic_responses");
	}

	#[test]
	fn salience_prefers_a_rare_term_over_a_repeated_corpus_term() {
		let documents = vec![
			FeatureDocument {
				symbol_index: 0,
				terms: BTreeMap::from([("common".to_string(), 10.0), ("rare".to_string(), 1.0)]),
			},
			FeatureDocument {
				symbol_index: 1,
				terms: BTreeMap::from([("common".to_string(), 10.0)]),
			},
		];
		let selected = select_salient_terms(&documents, &Bm25Config { k1: 1.2, b: 0.75 }, 1);
		assert_eq!(
			selected[0]
				.terms
				.keys()
				.map(String::as_str)
				.collect::<Vec<_>>(),
			vec!["rare"]
		);
	}
}
