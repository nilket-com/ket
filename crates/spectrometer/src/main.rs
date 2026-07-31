mod bm25;
mod corpus;
mod embedder;
mod eval;
mod fuzzy;
mod lsp;
mod model;
mod vectors;

use crate::embedder::EmbedderSession;
use crate::eval::{EvaluationInputs, evaluate, feature_cells};
use crate::model::{ExpectedSymbol, MethodConfig, Pilot, Probe, feature_documents};
use crate::vectors::{load_static_vectors, wanted_tokens};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

struct Options {
	root: PathBuf,
	config: Option<PathBuf>,
	pilot: Option<PathBuf>,
	vectors: Option<PathBuf>,
	vector_sha256: Option<String>,
	embedder_dir: Option<PathBuf>,
	embedder_headline_only: bool,
	report: Option<PathBuf>,
	resolve: bool,
	rust_analyzer: PathBuf,
	json_stdout: bool,
}

fn main() {
	if let Err(error) = run() {
		eprintln!("spectrometer: {error}");
		std::process::exit(2);
	}
}

fn run() -> Result<(), String> {
	let options = parse_options()?;
	let root = options
		.root
		.canonicalize()
		.map_err(|error| format!("unable to canonicalize root: {error}"))?;
	let config_path = options
		.config
		.unwrap_or_else(|| root.join("crates/spectrometer/candidate.toml"));
	let pilot_path = options
		.pilot
		.unwrap_or_else(|| root.join("crates/spectrometer/pilot.toml"));
	let config_bytes = std::fs::read(&config_path)
		.map_err(|error| format!("unable to read {}: {error}", config_path.display()))?;
	let config: MethodConfig = toml::from_slice(&config_bytes)
		.map_err(|error| format!("invalid candidate config: {error}"))?;
	let pilot_bytes = std::fs::read(&pilot_path)
		.map_err(|error| format!("unable to read {}: {error}", pilot_path.display()))?;
	let pilot: Pilot =
		toml::from_slice(&pilot_bytes).map_err(|error| format!("invalid pilot corpus: {error}"))?;
	if options.embedder_dir.is_some() {
		rayon::ThreadPoolBuilder::new()
			.num_threads(config.embedder.threads)
			.build_global()
			.map_err(|error| format!("unable to fix embedder worker count: {error}"))?;
	}

	let build_started = Instant::now();
	let resolver = options.resolve.then_some(options.rust_analyzer.as_path());
	let corpus = corpus::load_corpus(&root, resolver)?;
	let build_micros = build_started.elapsed().as_micros();
	validate_expected_symbols(&corpus, &pilot)?;
	let incremental_reindex_micros = measure_incremental_reindex(&corpus)?;

	let static_vectors = match (options.vectors, options.vector_sha256) {
		(Some(path), Some(checksum)) => {
			let all_documents = feature_cells(&config)
				.into_iter()
				.flat_map(|cell| feature_documents(&corpus, cell, &config.weights))
				.collect::<Vec<_>>();
			let wanted = wanted_tokens(
				all_documents,
				pilot.probe.iter().map(|probe| probe.query().to_string()),
			);
			Some(load_static_vectors(
				&path,
				&checksum,
				config.rank_sif.frequency_ranked,
				&wanted,
			)?)
		}
		(None, None) => None,
		(Some(_), None) => return Err("--vectors requires --vectors-sha256".to_string()),
		(None, Some(_)) => return Err("--vectors-sha256 requires --vectors".to_string()),
	};
	let embedder = options
		.embedder_dir
		.as_deref()
		.map(|directory| {
			EmbedderSession::load(
				directory,
				&config.embedder,
				pilot.probe.iter().map(|probe| probe.query().to_string()),
			)
		})
		.transpose()?;

	let report = evaluate(EvaluationInputs {
		corpus: &corpus,
		pilot: &pilot,
		config: &config,
		vectors: static_vectors.as_ref(),
		embedder: embedder.as_ref(),
		embedder_headline_only: options.embedder_headline_only,
		corpus_revision: git_revision(&root)?,
		corpus_rust_dirty: git_tracked_rust_dirty(&root)?,
		config_sha256: sha256_bytes(&config_bytes),
		build_micros,
		incremental_reindex_micros,
	})?;
	let json = serde_json::to_string_pretty(&report)
		.map_err(|error| format!("unable to serialize report: {error}"))?;
	if let Some(report_path) = options.report {
		std::fs::write(&report_path, &json)
			.map_err(|error| format!("unable to write {}: {error}", report_path.display()))?;
	}
	if options.json_stdout {
		println!("{json}");
	} else {
		print_summary(&report, &pilot);
	}
	Ok(())
}

fn parse_options() -> Result<Options, String> {
	let mut root = PathBuf::from(".");
	let mut config = None;
	let mut pilot = None;
	let mut vectors = None;
	let mut vector_sha256 = None;
	let mut embedder_dir = None;
	let mut embedder_headline_only = false;
	let mut report = None;
	let mut resolve = true;
	let mut rust_analyzer = PathBuf::from("rust-analyzer");
	let mut json_stdout = false;
	let mut arguments = std::env::args().skip(1);
	while let Some(argument) = arguments.next() {
		match argument.as_str() {
			"--root" => root = PathBuf::from(next_value(&mut arguments, "--root")?),
			"--config" => config = Some(PathBuf::from(next_value(&mut arguments, "--config")?)),
			"--pilot" => pilot = Some(PathBuf::from(next_value(&mut arguments, "--pilot")?)),
			"--vectors" => {
				vectors = Some(PathBuf::from(next_value(&mut arguments, "--vectors")?));
			}
			"--vectors-sha256" => {
				vector_sha256 = Some(next_value(&mut arguments, "--vectors-sha256")?);
			}
			"--embedder-dir" => {
				embedder_dir = Some(PathBuf::from(next_value(&mut arguments, "--embedder-dir")?));
			}
			"--embedder-headline-only" => embedder_headline_only = true,
			"--report" => report = Some(PathBuf::from(next_value(&mut arguments, "--report")?)),
			"--no-resolve" => resolve = false,
			"--rust-analyzer" => {
				rust_analyzer = PathBuf::from(next_value(&mut arguments, "--rust-analyzer")?);
			}
			"--json" => json_stdout = true,
			"--help" | "-h" => {
				print_help();
				std::process::exit(0);
			}
			other => return Err(format!("unknown argument: {other}")),
		}
	}
	Ok(Options {
		root,
		config,
		pilot,
		vectors,
		vector_sha256,
		embedder_dir,
		embedder_headline_only,
		report,
		resolve,
		rust_analyzer,
		json_stdout,
	})
}

fn next_value(
	arguments: &mut impl Iterator<Item = String>,
	option: &str,
) -> Result<String, String> {
	arguments
		.next()
		.ok_or_else(|| format!("{option} requires a value"))
}

fn print_help() {
	println!(
		"Usage: spectrometer [--root PATH] [--config PATH] [--pilot PATH]\n\
		 [--vectors PATH --vectors-sha256 HEX] [--embedder-dir PATH]\n\
		 [--embedder-headline-only]\n\
		 [--report PATH] [--no-resolve]\n\
		 [--rust-analyzer PATH] [--json]\n\n\
		 No model is downloaded. Missing vector/embedder paths report absent columns."
	);
}

fn validate_expected_symbols(corpus: &model::Corpus, pilot: &Pilot) -> Result<(), String> {
	for probe in &pilot.probe {
		let expected = match probe {
			Probe::Canonical { expected, .. } | Probe::Paraphrase { expected, .. } => {
				vec![expected]
			}
			Probe::Distinction {
				preferred, over, ..
			} => vec![preferred, over],
			Probe::Negative { .. } => Vec::new(),
		};
		for expected in expected {
			let matches = corpus
				.symbols
				.iter()
				.filter(|symbol| expected_matches(expected, symbol))
				.count();
			if matches != 1 {
				return Err(format!(
					"pilot target {}::{} resolved to {matches} symbols, expected exactly one",
					expected.path, expected.name
				));
			}
		}
	}
	Ok(())
}

fn expected_matches(expected: &ExpectedSymbol, symbol: &model::Symbol) -> bool {
	expected.path == symbol.id.path && expected.name == symbol.name
}

fn measure_incremental_reindex(corpus: &model::Corpus) -> Result<u128, String> {
	let representative = corpus
		.files
		.iter()
		.find(|file| file.relative_path == "crates/libaryon/src/engine/render.rs")
		.or_else(|| corpus.files.first())
		.ok_or_else(|| "corpus contains no Rust files".to_string())?;
	let started = Instant::now();
	let _ = corpus::extract_file_symbols(&representative.relative_path, &representative.text);
	Ok(started.elapsed().as_micros())
}

fn git_revision(root: &Path) -> Result<String, String> {
	let output = Command::new("git")
		.arg("-C")
		.arg(root)
		.args(["rev-parse", "HEAD"])
		.output()
		.map_err(|error| format!("unable to read corpus revision: {error}"))?;
	if !output.status.success() {
		return Err("git rev-parse HEAD failed".to_string());
	}
	Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn git_tracked_rust_dirty(root: &Path) -> Result<bool, String> {
	let status = Command::new("git")
		.arg("-C")
		.arg(root)
		.args([
			"diff",
			"--quiet",
			"HEAD",
			"--",
			"*.rs",
			":(exclude)crates/spectrometer/**",
		])
		.status()
		.map_err(|error| format!("unable to inspect corpus worktree: {error}"))?;
	match status.code() {
		Some(0) => Ok(false),
		Some(1) => Ok(true),
		_ => Err("git diff --quiet failed while inspecting the corpus".to_string()),
	}
}

fn sha256_bytes(bytes: &[u8]) -> String {
	let mut hasher = Sha256::new();
	hasher.update(bytes);
	format!("{:x}", hasher.finalize())
}

fn print_summary(report: &eval::Report, pilot: &Pilot) {
	println!(
		"spectrometer · revision={}{} · files={} · functions={} · build={:.1}ms",
		report.metadata.corpus_revision,
		if report.metadata.corpus_rust_dirty {
			"+dirty"
		} else {
			""
		},
		report.corpus.files,
		report.corpus.symbols,
		report.corpus.build_micros as f64 / 1_000.0
	);
	println!(
		"pilot: contaminated={} · {}",
		pilot.metadata.contaminated, pilot.metadata.purpose
	);
	println!(
		"resolution: calls={} internal={} unresolved/external={} backend={}",
		report.corpus.resolution.call_sites,
		report.corpus.resolution.resolved_inside_corpus,
		report.corpus.resolution.unresolved_or_external,
		report
			.corpus
			.resolution_backend
			.as_deref()
			.unwrap_or("absent")
	);
	if let Some(error) = &report.corpus.resolution_error {
		println!("resolution unavailable: {error}");
	}
	for cell in report.cells.iter().filter(|cell| {
		let strict_feature = [
			"span/own-excluded/literals-off",
			"callees/own-excluded/literals-off",
			"callers/own-excluded/literals-off",
		]
		.into_iter()
		.any(|feature| {
			cell.features == feature
				|| cell
					.features
					.strip_prefix(feature)
					.is_some_and(|suffix| suffix.starts_with('/'))
		});
		cell.retrieval == "fz"
			|| matches!(cell.retrieval.as_str(), "bm25" | "rank_sif" | "embedder") && strict_feature
	}) {
		if let Some(metrics) = &cell.metrics {
			let distinction = metrics
				.distinction_win_rate
				.map(|rate| format!("{rate:.2}"))
				.unwrap_or_else(|| "n/a".to_string());
			println!(
				"{:9} {:52} h@1={:.2} h@3={:.2} mrr={:.2} distinction={} far={:.2}",
				cell.retrieval,
				cell.features,
				metrics.hit_at_1,
				metrics.hit_at_3,
				metrics.mean_reciprocal_rank,
				distinction,
				metrics.false_accept_rate,
			);
		}
	}
	if report.metadata.vector_checksum.is_none() {
		println!("rank_sif: absent (supply a checksum-verified text vector file)");
	}
	if let Some(embedder) = &report.metadata.embedder_model {
		println!(
			"embedder: {}@{} · {}d · seq={} · threads={} · cached={} · load={:.1}ms · reference-error={:.3e}",
			embedder.model_id,
			embedder.revision,
			embedder.dimension,
			embedder.max_sequence,
			embedder.threads,
			embedder.cached_sequences,
			embedder.load_micros as f64 / 1_000.0,
			embedder.reference_max_abs_error,
		);
	} else {
		println!("embedder: absent (supply the checksum-pinned --embedder-dir)");
	}
}
