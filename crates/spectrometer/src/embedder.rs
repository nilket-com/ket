use crate::bm25::salient_term_order;
use crate::model::{
	Bm25Config, Corpus, EmbedderConfig, EmbedderRendering, FeatureCell, FeatureWeights,
	RankedSymbol, feature_documents, feature_token_streams,
};
use crate::vectors::sha256_file;
use candle_core::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config as BertConfig};
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokenizers::{Encoding, PaddingDirection, Tokenizer, TruncationDirection, TruncationParams};

const REFERENCE_FIXTURE: &str = include_str!("../reference/minilm_cosines.json");

#[derive(Debug, Clone)]
pub struct EmbedderMetadata {
	pub model_id: String,
	pub revision: String,
	pub license: String,
	pub model_path: PathBuf,
	pub model_sha256: String,
	pub tokenizer_sha256: String,
	pub config_sha256: String,
	pub dimension: usize,
	pub max_sequence: usize,
	pub threads: usize,
	pub batch_size: usize,
	pub max_batch_size: usize,
	pub load_micros: u128,
	pub reference_max_abs_error: f64,
}

pub struct EmbedderSession {
	model: BertModel,
	tokenizer: Tokenizer,
	config: EmbedderConfig,
	queries: BTreeMap<String, Vec<f32>>,
	embedding_cache: RefCell<BTreeMap<Vec<u32>, Vec<f32>>>,
	metadata: EmbedderMetadata,
}

pub struct EmbedderIndex<'a> {
	corpus: &'a Corpus,
	symbol_indices: Vec<usize>,
	document_vectors: Vec<Vec<Vec<f32>>>,
	pub embedding_micros: u128,
	pub incremental_embedding_micros: u128,
	pub logical_index_bytes: usize,
}

#[derive(Debug, Clone)]
struct RenderedDocument {
	symbol_index: usize,
	text: String,
}

#[derive(Debug, Deserialize)]
struct ReferenceFixture {
	model: String,
	revision: String,
	artifacts: BTreeMap<String, String>,
	sentences: Vec<String>,
	cosine_matrix: Vec<Vec<f64>>,
	tolerance: f64,
}

impl EmbedderSession {
	pub fn load(
		directory: &Path,
		config: &EmbedderConfig,
		queries: impl IntoIterator<Item = String>,
	) -> Result<Self, String> {
		validate_config(config)?;
		let model_path = directory.join("model.safetensors");
		let tokenizer_path = directory.join("tokenizer.json");
		let config_path = directory.join("config.json");
		let model_sha256 = verify_artifact(
			&model_path,
			"model.safetensors",
			&config.artifacts.model_sha256,
		)?;
		let tokenizer_sha256 = verify_artifact(
			&tokenizer_path,
			"tokenizer.json",
			&config.artifacts.tokenizer_sha256,
		)?;
		let config_sha256 =
			verify_artifact(&config_path, "config.json", &config.artifacts.config_sha256)?;

		let started = Instant::now();
		let serialized_config = std::fs::read_to_string(&config_path)
			.map_err(|error| format!("unable to read {}: {error}", config_path.display()))?;
		let bert_config: BertConfig = serde_json::from_str(&serialized_config)
			.map_err(|error| format!("invalid MiniLM config.json: {error}"))?;
		if config.max_sequence > bert_config.max_position_embeddings {
			return Err(format!(
				"embedder max_sequence={} exceeds model position limit {}",
				config.max_sequence, bert_config.max_position_embeddings
			));
		}
		let tokenizer = Tokenizer::from_file(&tokenizer_path)
			.map_err(|error| format!("invalid MiniLM tokenizer.json: {error}"))?;
		let device = Device::Cpu;
		// The artifact was checksum-verified immediately above and remains alive
		// for the session. Candle's mmap loader is unsafe because callers own
		// those two obligations.
		let variables = unsafe {
			VarBuilder::from_mmaped_safetensors(
				std::slice::from_ref(&model_path),
				DType::F32,
				&device,
			)
		}
		.map_err(|error| format!("unable to mmap MiniLM weights: {error}"))?;
		let model = BertModel::load(variables, &bert_config)
			.map_err(|error| format!("MiniLM tensor layout is incompatible: {error}"))?;
		let mut session = Self {
			model,
			tokenizer,
			config: config.clone(),
			queries: BTreeMap::new(),
			embedding_cache: RefCell::new(BTreeMap::new()),
			metadata: EmbedderMetadata {
				model_id: config.model_id.clone(),
				revision: config.revision.clone(),
				license: config.license.clone(),
				model_path,
				model_sha256,
				tokenizer_sha256,
				config_sha256,
				dimension: bert_config.hidden_size,
				max_sequence: config.max_sequence,
				threads: config.threads,
				batch_size: config.batch_size,
				max_batch_size: config.max_batch_size,
				load_micros: started.elapsed().as_micros(),
				reference_max_abs_error: 0.0,
			},
		};
		let reference_error = session.verify_reference()?;
		session.metadata.reference_max_abs_error = reference_error;
		let unique_queries = queries.into_iter().collect::<BTreeSet<_>>();
		let query_texts = unique_queries.iter().cloned().collect::<Vec<_>>();
		let encodings = query_texts
			.iter()
			.map(|query| session.encode_truncated(query))
			.collect::<Result<Vec<_>, _>>()?;
		let vectors = session.embed_encodings(&encodings)?;
		session.queries = unique_queries.into_iter().zip(vectors).collect();
		Ok(session)
	}

	pub fn metadata(&self) -> &EmbedderMetadata {
		&self.metadata
	}

	pub fn cache_entries(&self) -> usize {
		self.embedding_cache.borrow().len()
	}

	pub fn build_index<'a>(
		&self,
		corpus: &'a Corpus,
		cell: FeatureCell,
		rendering: EmbedderRendering,
		bm25: &Bm25Config,
		weights: &FeatureWeights,
	) -> Result<EmbedderIndex<'a>, String> {
		let documents = render_documents(corpus, cell, rendering, bm25, weights);
		let started = Instant::now();
		let mut flat_encodings = Vec::new();
		let mut groups = Vec::with_capacity(documents.len());
		for document in &documents {
			let encodings = match rendering {
				EmbedderRendering::ChunkedMax => self.encode_chunks(&document.text)?,
				EmbedderRendering::ChannelOrder | EmbedderRendering::SalienceOrder => {
					vec![self.encode_truncated(&document.text)?]
				}
			};
			let start = flat_encodings.len();
			flat_encodings.extend(encodings);
			groups.push(start..flat_encodings.len());
		}
		let flat_vectors = self.embed_encodings(&flat_encodings)?;
		let embedding_micros = started.elapsed().as_micros();
		let document_vectors = groups
			.into_iter()
			.map(|range| flat_vectors[range].to_vec())
			.collect::<Vec<_>>();
		let logical_index_bytes = flat_vectors
			.iter()
			.map(|vector| vector.len() * std::mem::size_of::<f32>())
			.sum();

		let representative = documents
			.iter()
			.find(|document| corpus.symbols[document.symbol_index].name == "render_frame")
			.or_else(|| documents.first());
		let incremental_embedding_micros = if let Some(document) = representative {
			let started = Instant::now();
			let encodings = match rendering {
				EmbedderRendering::ChunkedMax => self.encode_chunks(&document.text)?,
				EmbedderRendering::ChannelOrder | EmbedderRendering::SalienceOrder => {
					vec![self.encode_truncated(&document.text)?]
				}
			};
			// This models an edited symbol, whose token sequence is not yet in the
			// run-wide cache. Measuring `embed_encodings` here would report a cache
			// lookup because the full index populated these exact entries above.
			let _ = self.embed_uncached(&encodings)?;
			started.elapsed().as_micros()
		} else {
			0
		};

		Ok(EmbedderIndex {
			corpus,
			symbol_indices: documents
				.iter()
				.map(|document| document.symbol_index)
				.collect(),
			document_vectors,
			embedding_micros,
			incremental_embedding_micros,
			logical_index_bytes,
		})
	}

	pub fn rank(
		&self,
		index: &EmbedderIndex<'_>,
		query: &str,
	) -> Result<Vec<RankedSymbol>, String> {
		let query = self
			.queries
			.get(query)
			.ok_or_else(|| format!("query was not pre-embedded: {query}"))?;
		Ok(index.rank(query))
	}

	fn encode_truncated(&self, text: &str) -> Result<Encoding, String> {
		let mut tokenizer = self.tokenizer.clone();
		tokenizer
			.with_truncation(Some(TruncationParams {
				max_length: self.config.max_sequence,
				..Default::default()
			}))
			.map_err(|error| format!("unable to configure tokenizer truncation: {error}"))?;
		let encoding = tokenizer
			.encode(text, true)
			.map_err(|error| format!("unable to tokenize embedder input: {error}"))?;
		if encoding.len() > self.config.max_sequence {
			return Err("tokenizer exceeded the frozen sequence cap".to_string());
		}
		Ok(encoding)
	}

	fn encode_chunks(&self, text: &str) -> Result<Vec<Encoding>, String> {
		let mut encoding = self
			.tokenizer
			.encode(text, false)
			.map_err(|error| format!("unable to tokenize chunked embedder input: {error}"))?;
		let added = self
			.tokenizer
			.encode("", true)
			.map_err(|error| format!("unable to inspect tokenizer special tokens: {error}"))?
			.len();
		let payload = self
			.config
			.max_sequence
			.checked_sub(added)
			.ok_or_else(|| "sequence cap cannot hold tokenizer special tokens".to_string())?;
		if payload == 0 {
			return Err("sequence cap leaves no chunk payload".to_string());
		}
		encoding.truncate(payload, 0, TruncationDirection::Right);
		let mut chunks = Vec::with_capacity(1 + encoding.get_overflowing().len());
		chunks.push(encoding.clone());
		chunks.extend(encoding.get_overflowing().iter().cloned());
		chunks
			.into_iter()
			.map(|chunk| {
				let chunk = self
					.tokenizer
					.post_process(chunk, None, true)
					.map_err(|error| format!("unable to add chunk special tokens: {error}"))?;
				if chunk.len() > self.config.max_sequence {
					return Err("chunk exceeded the frozen sequence cap".to_string());
				}
				Ok(chunk)
			})
			.collect()
	}

	fn embed_encodings(&self, encodings: &[Encoding]) -> Result<Vec<Vec<f32>>, String> {
		let missing = {
			let cache = self.embedding_cache.borrow();
			let mut seen = BTreeSet::new();
			encodings
				.iter()
				.filter_map(|encoding| {
					let key = encoding.get_ids().to_vec();
					(!cache.contains_key(&key) && seen.insert(key.clone()))
						.then_some((key, encoding.clone()))
				})
				.collect::<Vec<_>>()
		};
		if !missing.is_empty() {
			let vectors = self.embed_uncached(
				&missing
					.iter()
					.map(|(_, encoding)| encoding.clone())
					.collect::<Vec<_>>(),
			)?;
			let mut cache = self.embedding_cache.borrow_mut();
			for ((key, _), vector) in missing.into_iter().zip(vectors) {
				cache.insert(key, vector);
			}
		}
		let cache = self.embedding_cache.borrow();
		encodings
			.iter()
			.map(|encoding| {
				cache
					.get(encoding.get_ids())
					.cloned()
					.ok_or_else(|| "embedder cache omitted a requested sequence".to_string())
			})
			.collect()
	}

	fn embed_uncached(&self, encodings: &[Encoding]) -> Result<Vec<Vec<f32>>, String> {
		let mut order = (0..encodings.len()).collect::<Vec<_>>();
		order.sort_by_key(|index| (encodings[*index].len(), *index));
		let mut output = vec![Vec::new(); encodings.len()];
		let attention_budget = self
			.config
			.batch_size
			.saturating_mul(self.config.max_sequence)
			.saturating_mul(self.config.max_sequence);
		let mut batch_start = 0;
		while batch_start < order.len() {
			let mut batch_end = batch_start + 1;
			while batch_end < order.len() && batch_end - batch_start < self.config.max_batch_size {
				let prospective_length = encodings[order[batch_end]].len().max(1);
				let prospective_size = batch_end + 1 - batch_start;
				if prospective_size
					.saturating_mul(prospective_length)
					.saturating_mul(prospective_length)
					> attention_budget
				{
					break;
				}
				batch_end += 1;
			}
			let batch_indices = &order[batch_start..batch_end];
			let max_length = batch_indices
				.iter()
				.map(|index| encodings[*index].len())
				.max()
				.unwrap_or(0)
				.max(1);
			let mut token_ids = Vec::with_capacity(batch_indices.len());
			let mut attention_masks = Vec::with_capacity(batch_indices.len());
			for index in batch_indices {
				let mut encoding = encodings[*index].clone();
				encoding.pad(max_length, 0, 0, "[PAD]", PaddingDirection::Right);
				token_ids.push(
					Tensor::new(encoding.get_ids(), &self.model.device)
						.map_err(candle_error("build token tensor"))?,
				);
				attention_masks.push(
					Tensor::new(encoding.get_attention_mask(), &self.model.device)
						.map_err(candle_error("build attention tensor"))?,
				);
			}
			let token_ids =
				Tensor::stack(&token_ids, 0).map_err(candle_error("stack token tensors"))?;
			let attention_mask = Tensor::stack(&attention_masks, 0)
				.map_err(candle_error("stack attention tensors"))?;
			let token_type_ids = token_ids
				.zeros_like()
				.map_err(candle_error("build token-type tensor"))?;
			let hidden = self
				.model
				.forward(&token_ids, &token_type_ids, Some(&attention_mask))
				.map_err(candle_error("run MiniLM encoder"))?;
			let pooling_mask = attention_mask
				.to_dtype(DType::F32)
				.and_then(|mask| mask.unsqueeze(2))
				.map_err(candle_error("build pooling mask"))?;
			let sum_mask = pooling_mask
				.sum(1)
				.map_err(candle_error("sum pooling mask"))?;
			let pooled = hidden
				.broadcast_mul(&pooling_mask)
				.and_then(|weighted| weighted.sum(1))
				.and_then(|summed| summed.broadcast_div(&sum_mask))
				.map_err(candle_error("mean-pool MiniLM output"))?;
			let normalized = pooled
				.sqr()
				.and_then(|squared| squared.sum_keepdim(1))
				.and_then(|norm| norm.sqrt())
				.and_then(|norm| pooled.broadcast_div(&norm))
				.map_err(candle_error("normalize MiniLM output"))?;
			let vectors = normalized
				.to_vec2::<f32>()
				.map_err(candle_error("read MiniLM output"))?;
			for (index, vector) in batch_indices.iter().zip(vectors) {
				output[*index] = vector;
			}
			batch_start = batch_end;
		}
		Ok(output)
	}

	fn verify_reference(&self) -> Result<f64, String> {
		let reference: ReferenceFixture = serde_json::from_str(REFERENCE_FIXTURE)
			.map_err(|error| format!("invalid MiniLM reference fixture: {error}"))?;
		if reference.model != self.config.model_id || reference.revision != self.config.revision {
			return Err("MiniLM reference fixture names a different model revision".to_string());
		}
		let expected_artifacts = [
			("config.json", &self.metadata.config_sha256),
			("tokenizer.json", &self.metadata.tokenizer_sha256),
			("model.safetensors", &self.metadata.model_sha256),
		];
		for (name, actual) in expected_artifacts {
			if reference.artifacts.get(name) != Some(actual) {
				return Err(format!(
					"MiniLM reference fixture has stale {name} identity"
				));
			}
		}
		let encodings = reference
			.sentences
			.iter()
			.map(|sentence| self.encode_truncated(sentence))
			.collect::<Result<Vec<_>, _>>()?;
		let vectors = self.embed_encodings(&encodings)?;
		if vectors.len() != reference.cosine_matrix.len() {
			return Err("MiniLM reference fixture matrix has the wrong size".to_string());
		}
		let mut max_error = 0.0f64;
		for (row, left) in vectors.iter().enumerate() {
			for (column, right) in vectors.iter().enumerate() {
				let actual = f64::from(dot(left, right));
				let expected = reference.cosine_matrix[row][column];
				max_error = max_error.max((actual - expected).abs());
			}
		}
		let tolerance = self.config.reference_tolerance.min(reference.tolerance);
		if max_error > tolerance {
			return Err(format!(
				"MiniLM backend differs from sentence-transformers reference by {max_error:e}, tolerance {tolerance:e}"
			));
		}
		Ok(max_error)
	}
}

impl EmbedderIndex<'_> {
	fn rank(&self, query: &[f32]) -> Vec<RankedSymbol> {
		let mut ranked = self
			.document_vectors
			.iter()
			.enumerate()
			.map(|(document_index, vectors)| {
				let symbol = &self.corpus.symbols[self.symbol_indices[document_index]];
				let score = vectors
					.iter()
					.map(|vector| dot(vector, query))
					.max_by(f32::total_cmp)
					.unwrap_or(0.0);
				RankedSymbol {
					id: symbol.id.clone(),
					name: symbol.name.clone(),
					score: f64::from(score),
				}
			})
			.collect::<Vec<_>>();
		ranked.sort_by(|left, right| {
			right
				.score
				.total_cmp(&left.score)
				.then_with(|| left.id.cmp(&right.id))
		});
		ranked
	}
}

fn validate_config(config: &EmbedderConfig) -> Result<(), String> {
	if config.max_sequence < 3 {
		return Err("embedder max_sequence must hold content plus special tokens".to_string());
	}
	if config.threads == 0 {
		return Err("embedder thread count must be nonzero".to_string());
	}
	if config.batch_size == 0 {
		return Err("embedder batch size must be nonzero".to_string());
	}
	if config.max_batch_size < config.batch_size {
		return Err("embedder max_batch_size must cover its full-sequence batch".to_string());
	}
	if config.renderings.is_empty() {
		return Err("embedder requires at least one rendering arm".to_string());
	}
	Ok(())
}

fn verify_artifact(path: &Path, name: &str, expected: &str) -> Result<String, String> {
	let actual = sha256_file(path)?;
	if !actual.eq_ignore_ascii_case(expected) {
		return Err(format!(
			"embedder {name} checksum mismatch: expected {}, found {actual}",
			expected.to_lowercase()
		));
	}
	Ok(actual)
}

fn render_documents(
	corpus: &Corpus,
	cell: FeatureCell,
	rendering: EmbedderRendering,
	bm25: &Bm25Config,
	weights: &FeatureWeights,
) -> Vec<RenderedDocument> {
	match rendering {
		EmbedderRendering::ChannelOrder | EmbedderRendering::ChunkedMax => {
			feature_token_streams(corpus, cell)
				.into_iter()
				.map(|stream| RenderedDocument {
					symbol_index: stream.symbol_index,
					text: stream.tokens.join(" "),
				})
				.collect()
		}
		EmbedderRendering::SalienceOrder => {
			let documents = feature_documents(corpus, cell, weights);
			let orders = salient_term_order(&documents, bm25);
			documents
				.into_iter()
				.zip(orders)
				.map(|(document, terms)| RenderedDocument {
					symbol_index: document.symbol_index,
					text: terms.join(" "),
				})
				.collect()
		}
	}
}

fn dot(left: &[f32], right: &[f32]) -> f32 {
	left.iter()
		.zip(right)
		.map(|(left, right)| left * right)
		.sum()
}

fn candle_error(context: &'static str) -> impl FnOnce(candle_core::Error) -> String {
	move |error| format!("{context}: {error}")
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::model::MethodConfig;

	#[test]
	fn reference_fixture_is_symmetric_and_pinned() {
		let reference: ReferenceFixture = serde_json::from_str(REFERENCE_FIXTURE).unwrap();
		assert_eq!(reference.sentences.len(), 4);
		assert_eq!(reference.cosine_matrix.len(), 4);
		assert_eq!(
			reference.revision,
			"1110a243fdf4706b3f48f1d95db1a4f5529b4d41"
		);
		for row in 0..4 {
			assert!((reference.cosine_matrix[row][row] - 1.0).abs() < 1.0e-6);
			for column in 0..4 {
				assert_eq!(
					reference.cosine_matrix[row][column],
					reference.cosine_matrix[column][row]
				);
			}
		}
	}

	#[test]
	fn supplied_artifact_checksum_mismatch_fails_closed() {
		let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("candidate.toml");
		let error = verify_artifact(&path, "candidate.toml", "00").unwrap_err();
		assert!(error.contains("checksum mismatch"));
	}

	#[test]
	#[ignore = "requires SPECTROMETER_EMBEDDER_DIR with pinned external artifacts"]
	fn pinned_backend_matches_sentence_transformers_and_chunks_without_loss() {
		let directory = std::env::var_os("SPECTROMETER_EMBEDDER_DIR")
			.map(PathBuf::from)
			.expect("SPECTROMETER_EMBEDDER_DIR");
		let config: MethodConfig =
			toml::from_slice(include_bytes!("../candidate.toml")).expect("candidate config");
		let session =
			EmbedderSession::load(&directory, &config.embedder, ["supervisor".to_string()])
				.expect("pinned MiniLM backend");
		assert!(session.metadata().reference_max_abs_error <= config.embedder.reference_tolerance);
		let text = std::iter::repeat_n("orchestrate", 2_000)
			.collect::<Vec<_>>()
			.join(" ");
		let chunks = session.encode_chunks(&text).expect("chunked encoding");
		assert!(chunks.len() > 1);
		assert!(
			chunks
				.iter()
				.all(|chunk| chunk.len() <= config.embedder.max_sequence)
		);
		let encoding = session.encode_truncated("supervisor").unwrap();
		let before = session.embedding_cache.borrow().len();
		let first = session
			.embed_encodings(std::slice::from_ref(&encoding))
			.unwrap();
		let after_first = session.embedding_cache.borrow().len();
		let second = session
			.embed_encodings(std::slice::from_ref(&encoding))
			.unwrap();
		assert_eq!(first, second);
		assert_eq!(after_first, session.embedding_cache.borrow().len());
		assert!(after_first >= before);
	}
}
