use crate::model::{Corpus, FeatureDocument, RankSifConfig, RankedSymbol, split_identifier};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
struct WordVector {
	values: Vec<f32>,
	rank: usize,
}

#[derive(Debug, Clone)]
pub struct StaticVectors {
	pub path: PathBuf,
	pub checksum: String,
	pub dimension: usize,
	pub total_rows: usize,
	pub wanted_tokens: usize,
	harmonic: f64,
	words: BTreeMap<String, WordVector>,
}

impl StaticVectors {
	pub fn retained_tokens(&self) -> usize {
		self.words.len()
	}
}

#[derive(Debug, Clone, Copy)]
pub struct Coverage {
	pub present: usize,
	pub requested: usize,
}

impl Coverage {
	pub fn ratio(self) -> f64 {
		if self.requested == 0 {
			1.0
		} else {
			self.present as f64 / self.requested as f64
		}
	}
}

pub fn load_static_vectors(
	path: &Path,
	expected_sha256: &str,
	frequency_ranked: bool,
	wanted: &BTreeSet<String>,
) -> Result<StaticVectors, String> {
	if !frequency_ranked {
		return Err("rank-SIF requires a frequency-ranked vector file".to_string());
	}
	let checksum = sha256_file(path)?;
	if !checksum.eq_ignore_ascii_case(expected_sha256) {
		return Err(format!(
			"vector checksum mismatch: expected {}, found {checksum}",
			expected_sha256.to_lowercase()
		));
	}
	let file = File::open(path)
		.map_err(|error| format!("unable to open vector file {}: {error}", path.display()))?;
	let mut words = BTreeMap::new();
	let mut dimension = None;
	let mut total_rows = 0usize;
	for (line_index, line) in BufReader::new(file).lines().enumerate() {
		let line = line.map_err(|error| format!("unable to read vector row: {error}"))?;
		let mut fields = line.split_whitespace();
		let Some(word) = fields.next() else {
			continue;
		};
		let values = fields.collect::<Vec<_>>();
		if line_index == 0
			&& values.len() == 1
			&& word.parse::<usize>().is_ok()
			&& values[0].parse::<usize>().is_ok()
		{
			dimension = values[0].parse::<usize>().ok();
			continue;
		}
		let row_dimension = dimension.get_or_insert(values.len());
		if values.len() != *row_dimension {
			return Err(format!(
				"vector row {} has dimension {}, expected {}",
				line_index + 1,
				values.len(),
				row_dimension
			));
		}
		total_rows += 1;
		let normalized_word = word.to_lowercase();
		if !wanted.contains(&normalized_word) {
			continue;
		}
		let parsed = values
			.into_iter()
			.map(|value| {
				value
					.parse::<f32>()
					.map_err(|_| format!("vector row {} contains an invalid float", line_index + 1))
			})
			.collect::<Result<Vec<_>, _>>()?;
		words.entry(normalized_word).or_insert(WordVector {
			values: parsed,
			rank: total_rows,
		});
	}
	let dimension = dimension.ok_or_else(|| "vector file contains no vectors".to_string())?;
	Ok(StaticVectors {
		path: path.to_path_buf(),
		checksum,
		dimension,
		total_rows,
		wanted_tokens: wanted.len(),
		harmonic: harmonic_number(total_rows.max(1)),
		words,
	})
}

pub fn sha256_file(path: &Path) -> Result<String, String> {
	let mut file = File::open(path)
		.map_err(|error| format!("unable to open {} for hashing: {error}", path.display()))?;
	let mut hasher = Sha256::new();
	let mut buffer = [0u8; 64 * 1024];
	loop {
		let read = file
			.read(&mut buffer)
			.map_err(|error| format!("unable to hash {}: {error}", path.display()))?;
		if read == 0 {
			break;
		}
		hasher.update(&buffer[..read]);
	}
	Ok(format!("{:x}", hasher.finalize()))
}

pub struct RankSifIndex<'a> {
	corpus: &'a Corpus,
	vectors: &'a StaticVectors,
	symbol_indices: Vec<usize>,
	document_vectors: Vec<Option<Vec<f64>>>,
	principal_component: Option<Vec<f64>>,
	config: &'a RankSifConfig,
}

impl<'a> RankSifIndex<'a> {
	pub fn new(
		corpus: &'a Corpus,
		documents: &[FeatureDocument],
		vectors: &'a StaticVectors,
		config: &'a RankSifConfig,
	) -> Self {
		let mut document_vectors = documents
			.iter()
			.map(|document| compose(&document.terms, vectors, config))
			.collect::<Vec<_>>();
		let principal_component = top_component(
			&document_vectors,
			vectors.dimension,
			config.power_iterations,
		);
		if let Some(component) = principal_component.as_deref() {
			for vector in document_vectors.iter_mut().flatten() {
				subtract_projection(vector, component);
				normalize(vector);
			}
		}
		Self {
			corpus,
			vectors,
			symbol_indices: documents
				.iter()
				.map(|document| document.symbol_index)
				.collect(),
			document_vectors,
			principal_component,
			config,
		}
	}

	pub fn rank(&self, query: &str) -> (Vec<RankedSymbol>, Coverage) {
		let terms = query
			.split(|character: char| !character.is_alphanumeric() && character != '_')
			.flat_map(split_identifier)
			.fold(BTreeMap::new(), |mut terms, term| {
				*terms.entry(term).or_insert(0.0) += 1.0;
				terms
			});
		let requested = terms.len();
		let present = terms
			.keys()
			.filter(|term| self.vectors.words.contains_key(*term))
			.count();
		let Some(mut query_vector) = compose(&terms, self.vectors, self.config) else {
			return (Vec::new(), Coverage { present, requested });
		};
		if let Some(component) = self.principal_component.as_deref() {
			subtract_projection(&mut query_vector, component);
			normalize(&mut query_vector);
		}
		let mut ranked = self
			.document_vectors
			.iter()
			.enumerate()
			.filter_map(|(document_index, vector)| {
				let vector = vector.as_ref()?;
				let symbol = &self.corpus.symbols[self.symbol_indices[document_index]];
				Some(RankedSymbol {
					id: symbol.id.clone(),
					name: symbol.name.clone(),
					score: dot(vector, &query_vector),
				})
			})
			.collect::<Vec<_>>();
		ranked.sort_by(|left, right| {
			right
				.score
				.total_cmp(&left.score)
				.then_with(|| left.id.cmp(&right.id))
		});
		(ranked, Coverage { present, requested })
	}
}

fn compose(
	terms: &BTreeMap<String, f64>,
	vectors: &StaticVectors,
	config: &RankSifConfig,
) -> Option<Vec<f64>> {
	let mut output = vec![0.0; vectors.dimension];
	let mut total_weight = 0.0;
	for (term, term_weight) in terms {
		let Some(vector) = vectors.words.get(term) else {
			continue;
		};
		let probability = 1.0 / (vector.rank as f64 * vectors.harmonic);
		let sif = config.a / (config.a + probability);
		let weight = term_weight * sif;
		for (output, value) in output.iter_mut().zip(&vector.values) {
			*output += weight * f64::from(*value);
		}
		total_weight += weight;
	}
	if total_weight == 0.0 {
		return None;
	}
	for value in &mut output {
		*value /= total_weight;
	}
	normalize(&mut output);
	Some(output)
}

fn harmonic_number(count: usize) -> f64 {
	(1..=count).map(|rank| 1.0 / rank as f64).sum()
}

fn top_component(
	vectors: &[Option<Vec<f64>>],
	dimension: usize,
	iterations: usize,
) -> Option<Vec<f64>> {
	if dimension == 0 || !vectors.iter().any(Option::is_some) {
		return None;
	}
	let mut component = vec![1.0 / (dimension as f64).sqrt(); dimension];
	for _ in 0..iterations {
		let mut next = vec![0.0; dimension];
		for vector in vectors.iter().flatten() {
			let projection = dot(vector, &component);
			for (next, value) in next.iter_mut().zip(vector) {
				*next += projection * value;
			}
		}
		if !normalize(&mut next) {
			return None;
		}
		component = next;
	}
	Some(component)
}

fn subtract_projection(vector: &mut [f64], component: &[f64]) {
	let projection = dot(vector, component);
	for (value, direction) in vector.iter_mut().zip(component) {
		*value -= projection * direction;
	}
}

fn dot(left: &[f64], right: &[f64]) -> f64 {
	left.iter()
		.zip(right)
		.map(|(left, right)| left * right)
		.sum()
}

fn normalize(vector: &mut [f64]) -> bool {
	let norm = dot(vector, vector).sqrt();
	if norm <= f64::EPSILON {
		return false;
	}
	for value in vector {
		*value /= norm;
	}
	true
}

pub fn wanted_tokens(
	documents: impl IntoIterator<Item = FeatureDocument>,
	queries: impl IntoIterator<Item = String>,
) -> BTreeSet<String> {
	let mut wanted = BTreeSet::new();
	for document in documents {
		wanted.extend(document.terms.into_keys());
	}
	for query in queries {
		wanted.extend(
			query
				.split(|character: char| !character.is_alphanumeric() && character != '_')
				.flat_map(split_identifier),
		);
	}
	wanted
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::io::Write;

	#[test]
	fn loader_checks_sha_and_retains_only_wanted_words() {
		let path = std::env::temp_dir().join(format!(
			"spectrometer-vectors-{}-{}.vec",
			std::process::id(),
			line!()
		));
		let bytes = b"3 2\nsupervisor 1.0 0.0\nauditor 0.0 1.0\nunused 0.5 0.5\n";
		File::create(&path).unwrap().write_all(bytes).unwrap();
		let checksum = {
			let mut hash = Sha256::new();
			hash.update(bytes);
			format!("{:x}", hash.finalize())
		};
		let wanted = ["supervisor".to_string()].into_iter().collect();
		let vectors = load_static_vectors(&path, &checksum, true, &wanted).expect("valid vectors");
		assert_eq!(vectors.dimension, 2);
		assert_eq!(vectors.total_rows, 3);
		assert!(vectors.words.contains_key("supervisor"));
		assert!(!vectors.words.contains_key("auditor"));
		assert!(load_static_vectors(&path, &"0".repeat(64), true, &wanted).is_err());
		std::fs::remove_file(path).unwrap();
	}
}
