//! Evaluate ADR 079 local-idle soak observations using the shared soak
//! artifact names. Capture is deliberately separate: the owner of an
//! isolated `nilket local` process writes config/samples, then invokes this
//! bounded evaluator before publishing the evidence directory.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write as _};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use wendelon::idle_soak::{LocalIdleSoakConfig, LocalIdleSoakSample, evaluate_local_idle_soak};

fn usage() -> &'static str {
	"usage: cargo run -p wendelon --example local_idle_soak -- --evidence-dir <directory>\n\
the directory must contain soak_config.json and samples.jsonl; summary.json is replaced"
}

fn parse_args() -> Result<PathBuf, String> {
	let mut args = std::env::args().skip(1);
	let Some(flag) = args.next() else {
		return Err(usage().to_owned());
	};
	if flag != "--evidence-dir" {
		return Err(format!("unknown argument {flag:?}\n{}", usage()));
	}
	let directory = args
		.next()
		.map(PathBuf::from)
		.ok_or_else(|| format!("--evidence-dir requires a directory\n{}", usage()))?;
	if let Some(extra) = args.next() {
		return Err(format!("unexpected argument {extra:?}\n{}", usage()));
	}
	Ok(directory)
}

fn read_samples(path: &Path) -> Result<Vec<LocalIdleSoakSample>, String> {
	let file = File::open(path).map_err(|error| format!("open {}: {error}", path.display()))?;
	BufReader::new(file)
		.lines()
		.enumerate()
		.filter_map(|(index, line)| match line {
			Ok(line) if line.trim().is_empty() => None,
			Ok(line) => {
				Some(serde_json::from_str(&line).map_err(|error| {
					format!("parse {} line {}: {error}", path.display(), index + 1)
				}))
			}
			Err(error) => Some(Err(format!(
				"read {} line {}: {error}",
				path.display(),
				index + 1
			))),
		})
		.collect()
}

fn run(directory: &Path) -> Result<bool, String> {
	let config_path = directory.join("soak_config.json");
	let samples_path = directory.join("samples.jsonl");
	let summary_path = directory.join("summary.json");
	let config: LocalIdleSoakConfig = serde_json::from_slice(
		&fs::read(&config_path)
			.map_err(|error| format!("read {}: {error}", config_path.display()))?,
	)
	.map_err(|error| format!("parse {}: {error}", config_path.display()))?;
	let samples = read_samples(&samples_path)?;
	let summary = evaluate_local_idle_soak(&config, &samples)?;
	let mut output = serde_json::to_vec_pretty(&summary)
		.map_err(|error| format!("serialize summary: {error}"))?;
	output.push(b'\n');
	let temporary = directory.join("summary.json.tmp");
	let mut file = File::create(&temporary)
		.map_err(|error| format!("create {}: {error}", temporary.display()))?;
	file.write_all(&output)
		.and_then(|()| file.sync_all())
		.map_err(|error| format!("write {}: {error}", temporary.display()))?;
	fs::rename(&temporary, &summary_path)
		.map_err(|error| format!("publish {}: {error}", summary_path.display()))?;
	println!(
		"local-idle soak {}: {}",
		if summary.passed { "passed" } else { "failed" },
		summary_path.display()
	);
	for failure in &summary.failures {
		eprintln!("local-idle soak: {failure}");
	}
	Ok(summary.passed)
}

fn main() -> ExitCode {
	match parse_args().and_then(|directory| run(&directory)) {
		Ok(true) => ExitCode::SUCCESS,
		Ok(false) => ExitCode::from(1),
		Err(error) => {
			eprintln!("local-idle soak: {error}");
			ExitCode::from(2)
		}
	}
}
