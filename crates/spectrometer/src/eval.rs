use crate::bm25::{Bm25Index, logical_index_bytes, select_salient_terms};
use crate::fuzzy;
use crate::model::{
	Corpus, ExpectedSymbol, FeatureCell, FeatureDepth, MethodConfig, Pilot, Probe, RankedSymbol,
	ResolutionStats, feature_documents,
};
use crate::vectors::{Coverage, RankSifIndex, StaticVectors};
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::BTreeSet;
use std::time::Instant;

#[derive(Debug, Serialize)]
pub struct Report {
	pub metadata: RunMetadata,
	pub corpus: CorpusReport,
	pub cells: Vec<CellReport>,
}

#[derive(Debug, Serialize)]
pub struct RunMetadata {
	pub corpus_revision: String,
	pub corpus_rust_dirty: bool,
	pub extractor_version: u32,
	pub config_sha256: String,
	pub vector_checksum: Option<String>,
	pub vector_path: Option<String>,
	pub vector_dimension: Option<usize>,
	pub vector_rows: Option<usize>,
	pub vector_wanted_tokens: Option<usize>,
	pub vector_retained_tokens: Option<usize>,
	pub vector_storage: Option<String>,
	pub backend: String,
	pub cross_platform_score_tolerance: f64,
	pub comment_policy: String,
	pub embedder: String,
}

#[derive(Debug, Serialize)]
pub struct CorpusReport {
	pub files: usize,
	pub symbols: usize,
	pub resolution: ResolutionStats,
	pub resolution_backend: Option<String>,
	pub resolution_error: Option<String>,
	pub build_micros: u128,
	pub incremental_reindex_micros: u128,
	pub peak_rss_kib: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct CellReport {
	pub retrieval: String,
	pub features: String,
	pub status: String,
	pub metrics: Option<Metrics>,
	pub candidate_threshold: Option<CandidateThreshold>,
	pub average_query_micros: Option<f64>,
	pub logical_index_bytes: Option<usize>,
	pub mean_query_vector_coverage: Option<f64>,
	pub determinism: String,
	pub outcomes: Vec<QueryOutcome>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct CandidateThreshold {
	pub minimum_score: f64,
	pub minimum_margin: f64,
	pub tuning_only: bool,
}

#[derive(Debug, Serialize)]
pub struct Metrics {
	pub positives: usize,
	pub hit_at_1: f64,
	pub hit_at_3: f64,
	pub mean_reciprocal_rank: f64,
	pub distinction_total: usize,
	pub distinction_probes: usize,
	pub distinction_win_rate: Option<f64>,
	pub negatives: usize,
	pub false_accept_rate: f64,
}

#[derive(Debug, Serialize)]
pub struct QueryOutcome {
	pub id: String,
	pub kind: String,
	pub query: String,
	pub expected_rank: Option<usize>,
	pub preferred_rank: Option<usize>,
	pub over_rank: Option<usize>,
	pub accepted: bool,
	pub top: Vec<RankedSymbol>,
	pub vector_coverage: Option<f64>,
}

struct RawOutcome {
	id: String,
	kind: String,
	query: String,
	expected_rank: Option<usize>,
	preferred_rank: Option<usize>,
	over_rank: Option<usize>,
	distinction_win: Option<bool>,
	ranked: Vec<RankedSymbol>,
	coverage: Option<Coverage>,
}

pub struct EvaluationInputs<'a> {
	pub corpus: &'a Corpus,
	pub pilot: &'a Pilot,
	pub config: &'a MethodConfig,
	pub vectors: Option<&'a StaticVectors>,
	pub corpus_revision: String,
	pub corpus_rust_dirty: bool,
	pub config_sha256: String,
	pub build_micros: u128,
	pub incremental_reindex_micros: u128,
}

pub fn feature_cells(config: &MethodConfig) -> Vec<FeatureCell> {
	let mut cells = BTreeSet::new();
	for depth in config.depths.iter().copied().map(FeatureDepth::from) {
		for exclude_own_name in &config.own_name_exclusion {
			for include_literals in &config.literal_channel {
				cells.insert(FeatureCell {
					depth,
					exclude_own_name: *exclude_own_name,
					include_literals: *include_literals,
				});
			}
		}
	}
	cells.into_iter().collect()
}

pub fn evaluate(inputs: EvaluationInputs<'_>) -> Result<Report, String> {
	let mut cells = Vec::new();
	cells.push(evaluate_ranker(
		"fz",
		"raw-source-floor",
		inputs.pilot,
		None,
		|| Ok(fuzzy_rankings(inputs.pilot, inputs.corpus)),
	)?);

	for cell in feature_cells(inputs.config) {
		if cell.depth >= FeatureDepth::Callees && inputs.corpus.resolution_backend.is_none() {
			let reason = inputs
				.corpus
				.resolution_error
				.as_deref()
				.unwrap_or("resolved call-edge extraction disabled");
			cells.push(absent_cell("bm25", &cell.label(), reason));
			for salience in salience_values(inputs.config) {
				cells.push(absent_cell(
					"rank_sif",
					&rank_sif_label(cell, salience),
					reason,
				));
			}
			cells.push(absent_cell("embedder", &cell.label(), reason));
			continue;
		}
		let documents = feature_documents(inputs.corpus, cell, &inputs.config.weights);
		let logical_bytes = logical_index_bytes(&documents);
		let bm25 = Bm25Index::new(inputs.corpus, &documents, &inputs.config.bm25);
		cells.push(evaluate_ranker(
			"bm25",
			&cell.label(),
			inputs.pilot,
			Some(logical_bytes),
			|| {
				Ok(inputs
					.pilot
					.probe
					.iter()
					.map(|probe| (bm25.rank(probe.query()), None))
					.collect())
			},
		)?);

		for salience in salience_values(inputs.config) {
			let label = rank_sif_label(cell, salience);
			if let Some(vectors) = inputs.vectors {
				let vector_documents = salience.map_or_else(
					|| documents.clone(),
					|limit| select_salient_terms(&documents, &inputs.config.bm25, limit),
				);
				let vector_logical_bytes = logical_index_bytes(&vector_documents);
				let index = RankSifIndex::new(
					inputs.corpus,
					&vector_documents,
					vectors,
					&inputs.config.rank_sif,
				);
				cells.push(evaluate_ranker(
					"rank_sif",
					&label,
					inputs.pilot,
					Some(vector_logical_bytes),
					|| {
						Ok(inputs
							.pilot
							.probe
							.iter()
							.map(|probe| {
								let (ranking, coverage) = index.rank(probe.query());
								(ranking, Some(coverage))
							})
							.collect())
					},
				)?);
			} else {
				cells.push(absent_cell("rank_sif", &label, "no vector file supplied"));
			}
		}
		cells.push(absent_cell(
			"embedder",
			&cell.label(),
			"backend deliberately reserved for a later harness-only pair",
		));
	}

	Ok(Report {
		metadata: RunMetadata {
			corpus_revision: inputs.corpus_revision,
			corpus_rust_dirty: inputs.corpus_rust_dirty,
			extractor_version: inputs.config.extractor_version,
			config_sha256: inputs.config_sha256,
			vector_checksum: inputs.vectors.map(|vectors| vectors.checksum.clone()),
			vector_path: inputs
				.vectors
				.map(|vectors| vectors.path.display().to_string()),
			vector_dimension: inputs.vectors.map(|vectors| vectors.dimension),
			vector_rows: inputs.vectors.map(|vectors| vectors.total_rows),
			vector_wanted_tokens: inputs.vectors.map(|vectors| vectors.wanted_tokens),
			vector_retained_tokens: inputs.vectors.map(StaticVectors::retained_tokens),
			vector_storage: inputs.vectors.map(|_| "f32 retained rows".to_string()),
			backend: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
			cross_platform_score_tolerance: 1.0e-9,
			comment_policy:
				"RA-token identifiers; COMMENT and explicit #[doc] excluded; literals separate"
					.to_string(),
			embedder: "absent by contract".to_string(),
		},
		corpus: CorpusReport {
			files: inputs.corpus.files.len(),
			symbols: inputs.corpus.symbols.len(),
			resolution: inputs.corpus.resolution,
			resolution_backend: inputs.corpus.resolution_backend.clone(),
			resolution_error: inputs.corpus.resolution_error.clone(),
			build_micros: inputs.build_micros,
			incremental_reindex_micros: inputs.incremental_reindex_micros,
			peak_rss_kib: peak_rss_kib(),
		},
		cells,
	})
}

fn salience_values(config: &MethodConfig) -> Vec<Option<usize>> {
	let limits = config
		.rank_sif
		.salience_k
		.iter()
		.copied()
		.filter(|limit| *limit > 0)
		.collect::<BTreeSet<_>>();
	let mut values = vec![None];
	values.extend(limits.into_iter().map(Some));
	values
}

fn rank_sif_label(cell: FeatureCell, salience: Option<usize>) -> String {
	format!(
		"{}/salience-{}",
		cell.label(),
		salience.map_or_else(|| "all".to_string(), |limit| limit.to_string())
	)
}

fn fuzzy_rankings(pilot: &Pilot, corpus: &Corpus) -> Vec<(Vec<RankedSymbol>, Option<Coverage>)> {
	pilot
		.probe
		.iter()
		.map(|probe| (fuzzy::rank(probe.query(), corpus), None))
		.collect()
}

fn evaluate_ranker(
	retrieval: &str,
	features: &str,
	pilot: &Pilot,
	logical_index_bytes: Option<usize>,
	run: impl Fn() -> Result<Vec<(Vec<RankedSymbol>, Option<Coverage>)>, String>,
) -> Result<CellReport, String> {
	let started = Instant::now();
	let first = run()?;
	let elapsed = started.elapsed();
	let second = run()?;
	let first_identities = ranking_identities(&first);
	let second_identities = ranking_identities(&second);
	if first_identities != second_identities {
		return Err(format!(
			"determinism failure in {retrieval}/{features}: ranked symbol identities changed"
		));
	}
	let raw = build_raw_outcomes(pilot, first);
	let threshold = calibrate_candidate_threshold(&raw);
	let metrics = calculate_metrics(&raw, threshold);
	let mean_coverage = {
		let coverage = raw
			.iter()
			.filter_map(|outcome| outcome.coverage)
			.map(Coverage::ratio)
			.collect::<Vec<_>>();
		(!coverage.is_empty()).then(|| coverage.iter().sum::<f64>() / coverage.len() as f64)
	};
	let outcomes = raw
		.into_iter()
		.map(|outcome| QueryOutcome {
			id: outcome.id,
			kind: outcome.kind,
			query: outcome.query,
			expected_rank: outcome.expected_rank,
			preferred_rank: outcome.preferred_rank,
			over_rank: outcome.over_rank,
			accepted: accepted(&outcome.ranked, threshold),
			top: outcome.ranked.into_iter().take(5).collect(),
			vector_coverage: outcome.coverage.map(Coverage::ratio),
		})
		.collect();
	Ok(CellReport {
		retrieval: retrieval.to_string(),
		features: features.to_string(),
		status: "available".to_string(),
		metrics: Some(metrics),
		candidate_threshold: Some(threshold),
		average_query_micros: Some(elapsed.as_micros() as f64 / pilot.probe.len().max(1) as f64),
		logical_index_bytes,
		mean_query_vector_coverage: mean_coverage,
		determinism: "identical ranked identities across two consecutive evaluations".to_string(),
		outcomes,
	})
}

fn ranking_identities(
	rankings: &[(Vec<RankedSymbol>, Option<Coverage>)],
) -> Vec<Vec<crate::model::SymbolId>> {
	rankings
		.iter()
		.map(|(ranking, _)| ranking.iter().map(|symbol| symbol.id.clone()).collect())
		.collect()
}

fn build_raw_outcomes(
	pilot: &Pilot,
	rankings: Vec<(Vec<RankedSymbol>, Option<Coverage>)>,
) -> Vec<RawOutcome> {
	pilot
		.probe
		.iter()
		.zip(rankings)
		.map(|(probe, (ranked, coverage))| {
			let expected_rank = probe
				.positive_expected()
				.and_then(|expected| rank_of(&ranked, expected));
			let (kind, preferred_rank, over_rank, distinction_win) = match probe {
				Probe::Canonical { .. } => ("canonical", None, None, None),
				Probe::Paraphrase { .. } => ("paraphrase", None, None, None),
				Probe::Negative { .. } => ("negative", None, None, None),
				Probe::Distinction {
					preferred, over, ..
				} => {
					let preferred_rank = rank_of(&ranked, preferred);
					let over_rank = rank_of(&ranked, over);
					let supported = preferred_rank.is_some_and(|rank| rank < 3)
						|| over_rank.is_some_and(|rank| rank < 3);
					(
						"distinction",
						preferred_rank,
						over_rank,
						supported.then_some(match (preferred_rank, over_rank) {
							(Some(preferred), Some(over)) => preferred < over,
							(Some(_), None) => true,
							(None, _) => false,
						}),
					)
				}
			};
			RawOutcome {
				id: probe.id().to_string(),
				kind: kind.to_string(),
				query: probe.query().to_string(),
				expected_rank,
				preferred_rank,
				over_rank,
				distinction_win,
				ranked,
				coverage,
			}
		})
		.collect()
}

fn rank_of(ranked: &[RankedSymbol], expected: &ExpectedSymbol) -> Option<usize> {
	ranked
		.iter()
		.position(|symbol| symbol.id.path == expected.path && symbol.name == expected.name)
}

fn calibrate_candidate_threshold(outcomes: &[RawOutcome]) -> CandidateThreshold {
	let mut scores = vec![0.0];
	let mut margins = vec![0.0];
	for outcome in outcomes {
		if let Some(first) = outcome.ranked.first() {
			scores.push(first.score);
			scores.push(next_greater_threshold(first.score));
			let margin = first.score
				- outcome
					.ranked
					.get(1)
					.map(|second| second.score)
					.unwrap_or(0.0);
			margins.push(margin);
			margins.push(next_greater_threshold(margin));
		}
	}
	scores.sort_by(f64::total_cmp);
	scores.dedup_by(|left, right| left.total_cmp(right).is_eq());
	margins.sort_by(f64::total_cmp);
	margins.dedup_by(|left, right| left.total_cmp(right).is_eq());

	let mut best = CandidateThreshold {
		minimum_score: 0.0,
		minimum_margin: 0.0,
		tuning_only: true,
	};
	let mut best_key = (false, 0usize, Reverse(usize::MAX));
	for minimum_score in scores {
		for minimum_margin in margins.iter().copied() {
			let threshold = CandidateThreshold {
				minimum_score,
				minimum_margin,
				tuning_only: true,
			};
			let false_accepts = outcomes
				.iter()
				.filter(|outcome| outcome.kind == "negative")
				.filter(|outcome| accepted(&outcome.ranked, threshold))
				.count();
			let correct_accepts = outcomes
				.iter()
				.filter(|outcome| matches!(outcome.kind.as_str(), "canonical" | "paraphrase"))
				.filter(|outcome| outcome.expected_rank == Some(0))
				.filter(|outcome| accepted(&outcome.ranked, threshold))
				.count();
			let incorrect_accepts = outcomes
				.iter()
				.filter(|outcome| matches!(outcome.kind.as_str(), "canonical" | "paraphrase"))
				.filter(|outcome| accepted(&outcome.ranked, threshold))
				.filter(|outcome| outcome.expected_rank != Some(0))
				.count();
			let key = (
				false_accepts == 0,
				correct_accepts,
				Reverse(incorrect_accepts),
			);
			if key > best_key {
				best_key = key;
				best = threshold;
			}
		}
	}
	best
}

fn next_greater_threshold(value: f64) -> f64 {
	if value.is_finite() && value >= 0.0 {
		f64::from_bits(value.to_bits().saturating_add(1))
	} else {
		value
	}
}

fn accepted(ranked: &[RankedSymbol], threshold: CandidateThreshold) -> bool {
	let Some(first) = ranked.first() else {
		return false;
	};
	let margin = first.score - ranked.get(1).map(|second| second.score).unwrap_or(0.0);
	first.score >= threshold.minimum_score && margin >= threshold.minimum_margin
}

fn calculate_metrics(outcomes: &[RawOutcome], threshold: CandidateThreshold) -> Metrics {
	let positives = outcomes
		.iter()
		.filter(|outcome| matches!(outcome.kind.as_str(), "canonical" | "paraphrase"))
		.collect::<Vec<_>>();
	let distinctions = outcomes
		.iter()
		.filter_map(|outcome| outcome.distinction_win)
		.collect::<Vec<_>>();
	let distinction_total = outcomes
		.iter()
		.filter(|outcome| outcome.kind == "distinction")
		.count();
	let negatives = outcomes
		.iter()
		.filter(|outcome| outcome.kind == "negative")
		.collect::<Vec<_>>();
	let mean_reciprocal_rank = if positives.is_empty() {
		0.0
	} else {
		let value = positives
			.iter()
			.filter_map(|outcome| outcome.expected_rank)
			.map(|rank| 1.0 / (rank + 1) as f64)
			.sum::<f64>()
			/ positives.len() as f64;
		if value == 0.0 { 0.0 } else { value }
	};
	Metrics {
		positives: positives.len(),
		hit_at_1: ratio(
			positives
				.iter()
				.filter(|outcome| outcome.expected_rank == Some(0))
				.count(),
			positives.len(),
		),
		hit_at_3: ratio(
			positives
				.iter()
				.filter(|outcome| outcome.expected_rank.is_some_and(|rank| rank < 3))
				.count(),
			positives.len(),
		),
		mean_reciprocal_rank,
		distinction_total,
		distinction_probes: distinctions.len(),
		distinction_win_rate: (!distinctions.is_empty()).then(|| {
			ratio(
				distinctions.iter().filter(|won| **won).count(),
				distinctions.len(),
			)
		}),
		negatives: negatives.len(),
		false_accept_rate: ratio(
			negatives
				.iter()
				.filter(|outcome| accepted(&outcome.ranked, threshold))
				.count(),
			negatives.len(),
		),
	}
}

fn ratio(numerator: usize, denominator: usize) -> f64 {
	if denominator == 0 {
		0.0
	} else {
		numerator as f64 / denominator as f64
	}
}

fn absent_cell(retrieval: &str, features: &str, reason: &str) -> CellReport {
	CellReport {
		retrieval: retrieval.to_string(),
		features: features.to_string(),
		status: format!("absent: {reason}"),
		metrics: None,
		candidate_threshold: None,
		average_query_micros: None,
		logical_index_bytes: None,
		mean_query_vector_coverage: None,
		determinism: "not applicable".to_string(),
		outcomes: Vec::new(),
	}
}

fn peak_rss_kib() -> Option<u64> {
	let status = std::fs::read_to_string("/proc/self/status").ok()?;
	status.lines().find_map(|line| {
		let value = line.strip_prefix("VmHWM:")?.trim();
		value.split_whitespace().next()?.parse().ok()
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::model::{
		Bm25Config, FeatureDepthConfig, FeatureWeights, PilotMetadata, RankSifConfig, SymbolId,
	};
	use std::path::PathBuf;

	fn ranked(scores: &[f64]) -> Vec<RankedSymbol> {
		scores
			.iter()
			.enumerate()
			.map(|(index, score)| RankedSymbol {
				id: SymbolId {
					path: format!("src/{index}.rs"),
					start: 0,
					end: 1,
					provider_kind: "FN".to_string(),
				},
				name: format!("symbol_{index}"),
				score: *score,
			})
			.collect()
	}

	fn outcome(kind: &str, expected_rank: Option<usize>, scores: &[f64]) -> RawOutcome {
		RawOutcome {
			id: kind.to_string(),
			kind: kind.to_string(),
			query: kind.to_string(),
			expected_rank,
			preferred_rank: None,
			over_rank: None,
			distinction_win: None,
			ranked: ranked(scores),
			coverage: None,
		}
	}

	#[test]
	fn threshold_can_move_just_above_an_observed_false_hit() {
		let outcomes = vec![
			outcome("canonical", Some(5), &[0.9, 0.8]),
			outcome("negative", None, &[0.8, 0.7]),
		];
		let threshold = calibrate_candidate_threshold(&outcomes);
		assert!(!accepted(&outcomes[0].ranked, threshold));
		assert!(!accepted(&outcomes[1].ranked, threshold));
	}

	#[test]
	fn threshold_keeps_a_correct_separated_hit_while_rejecting_a_negative() {
		let outcomes = vec![
			outcome("canonical", Some(0), &[0.9, 0.5]),
			outcome("negative", None, &[0.8, 0.7]),
		];
		let threshold = calibrate_candidate_threshold(&outcomes);
		assert!(accepted(&outcomes[0].ranked, threshold));
		assert!(!accepted(&outcomes[1].ranked, threshold));
	}

	#[test]
	fn distinction_is_undefined_without_a_target_in_the_top_three() {
		let expected = |index: usize| ExpectedSymbol {
			path: format!("src/{index}.rs"),
			name: format!("symbol_{index}"),
		};
		let pilot = Pilot {
			metadata: PilotMetadata {
				contaminated: true,
				purpose: "fixture".to_string(),
			},
			probe: vec![Probe::Distinction {
				id: "pair".to_string(),
				query: "query".to_string(),
				preferred: expected(3),
				over: expected(4),
			}],
		};
		let unsupported =
			build_raw_outcomes(&pilot, vec![(ranked(&[5.0, 4.0, 3.0, 2.0, 1.0]), None)]);
		assert_eq!(unsupported[0].preferred_rank, Some(3));
		assert_eq!(unsupported[0].over_rank, Some(4));
		assert_eq!(unsupported[0].distinction_win, None);
		let metrics = calculate_metrics(
			&unsupported,
			CandidateThreshold {
				minimum_score: 0.0,
				minimum_margin: 0.0,
				tuning_only: true,
			},
		);
		assert_eq!(metrics.distinction_total, 1);
		assert_eq!(metrics.distinction_probes, 0);
		assert_eq!(metrics.distinction_win_rate, None);
	}

	#[test]
	fn resolved_feature_cells_are_absent_when_the_backend_is_unavailable() {
		let corpus = Corpus {
			root: PathBuf::new(),
			files: Vec::new(),
			symbols: Vec::new(),
			resolution: ResolutionStats::default(),
			resolution_backend: None,
			resolution_error: Some("backend unavailable".to_string()),
		};
		let pilot = Pilot {
			metadata: PilotMetadata {
				contaminated: false,
				purpose: "holdout-compatible fixture".to_string(),
			},
			probe: Vec::new(),
		};
		let config = MethodConfig {
			extractor_version: 1,
			depths: vec![FeatureDepthConfig::Callees],
			own_name_exclusion: vec![true],
			literal_channel: vec![false],
			weights: FeatureWeights {
				name: 1.0,
				span_identifier: 1.0,
				context: 1.0,
				literal: 1.0,
				callee: 1.0,
				caller: 1.0,
			},
			bm25: Bm25Config { k1: 1.2, b: 0.75 },
			rank_sif: RankSifConfig {
				a: 0.001,
				power_iterations: 1,
				frequency_ranked: true,
				salience_k: vec![16, 32],
			},
		};
		let report = evaluate(EvaluationInputs {
			corpus: &corpus,
			pilot: &pilot,
			config: &config,
			vectors: None,
			corpus_revision: "fixture".to_string(),
			corpus_rust_dirty: false,
			config_sha256: "fixture".to_string(),
			build_micros: 0,
			incremental_reindex_micros: 0,
		})
		.unwrap();
		assert_eq!(report.cells.len(), 6);
		assert!(
			report
				.cells
				.iter()
				.filter(|cell| cell.retrieval != "fz")
				.all(|cell| cell.status.contains("backend unavailable"))
		);
	}
}
