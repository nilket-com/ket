//! ADR 581 build wrapper. `build` constructs identity and immutable staged executables; `run`
//! executes only those staged bytes. Sealing is deliberately a separate explicit subcommand.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};

use serde::Deserialize;
use toron_tpch::evidence::{
	BUILD_MANIFEST_ENV, BuildManifest, BuildRecipe, RunEvidenceManifest, SealOptions,
	seal_evidence_manifest, sha256_bytes, sha256_file, stage_executable, verify_staged_executable,
};

const DEFAULT_ARCHIVE_LIMIT_BYTES: u64 = 64 * 1024 * 1024 * 1024;

fn main() -> ExitCode {
	let args = std::env::args_os().skip(1).collect::<Vec<_>>();
	match args.first().and_then(|arg| arg.to_str()) {
		Some("build") => match build(&args[1..]) {
			Ok(path) => {
				println!("{}", path.display());
				ExitCode::SUCCESS
			}
			Err(error) => fail(&error),
		},
		Some("run") => match run_staged(&args[1..]) {
			Ok(code) => code,
			Err(error) => fail(&error),
		},
		Some("seal-evidence") => match seal(&args[1..]) {
			Ok(manifest) => {
				println!(
					"{}",
					serde_json::to_string(&manifest).expect("evidence manifest serializes")
				);
				ExitCode::SUCCESS
			}
			Err(error) => fail(&error),
		},
		_ => usage(),
	}
}

fn usage() -> ExitCode {
	eprintln!(
		"usage: toron-bench build [--workspace PATH] [--profile PROFILE] [--features A,B] \
		 [--target TRIPLE] [--toron-bin PATH] [--duckdb-bin PATH] [--staging-root PATH] \
		 [--manifest PATH]\n       \
		 toron-bench run --manifest PATH -- <toron-tpch arguments>\n       \
		 toron-bench seal-evidence --manifest PATH [--sealed PATH] [--repo PATH] \
		 [--archive-root PATH] [--remote NAME] [--tag NAME] [--max-archive-bytes N]"
	);
	ExitCode::from(2)
}

fn fail(message: &str) -> ExitCode {
	eprintln!("{message}");
	ExitCode::from(2)
}

#[derive(Debug)]
struct BuildOptions {
	workspace: PathBuf,
	profile: String,
	features: Vec<String>,
	target: Option<String>,
	toron_bin: Option<PathBuf>,
	duckdb_bin: Option<PathBuf>,
	staging_root: Option<PathBuf>,
	manifest_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceSnapshot {
	tree_id: String,
	untracked_build_inputs: Vec<String>,
}

fn build(args: &[OsString]) -> Result<PathBuf, String> {
	let options = parse_build_options(args)?;
	let workspace = options.workspace.canonicalize().map_err(|error| {
		format!(
			"canonicalize workspace {}: {error}",
			options.workspace.display()
		)
	})?;
	let rustc_version = command_stdout(Command::new("rustc").arg("-vV"), "rustc -vV")?;
	let target = match options.target.clone() {
		Some(target) => target,
		None => rustc_version
			.lines()
			.find_map(|line| line.strip_prefix("host: "))
			.ok_or_else(|| "rustc -vV did not report a host triple".to_string())?
			.to_string(),
	};
	let metadata = cargo_metadata(&workspace, &options.features, Some(&target))?;
	let repo = git_stdout(&workspace, &["rev-parse", "--show-toplevel"])?;
	let repo = PathBuf::from(repo.trim());
	let source_roots = local_dependency_roots(&metadata, &repo, &workspace)?;
	let cargo_output_roots = cargo_output_roots(&metadata, &repo, &source_roots);
	let source_before = source_snapshot(&repo, &source_roots, &cargo_output_roots)?;
	let commit_hint = git_stdout(&repo, &["rev-parse", "HEAD"])?
		.trim()
		.to_string();
	let cargo_lock = workspace.join("Cargo.lock");
	let cargo_lock_sha256 = sha256_file(&cargo_lock)?;

	let rustflags_sha256 = effective_rustflags_digest(&workspace, &target)?;
	let mut cargo = Command::new("cargo");
	cargo
		.current_dir(&workspace)
		.arg("build")
		.args(["-p", "toron-tpch", "--bin", "toron-tpch", "--profile"])
		.arg(&options.profile)
		.arg("--target")
		.arg(&target);
	if !options.features.is_empty() {
		cargo.arg("--features").arg(options.features.join(","));
	}
	let status = cargo
		.status()
		.map_err(|error| format!("run cargo build: {error}"))?;
	if !status.success() {
		return Err(format!("cargo build failed with {status}"));
	}
	let source_after = source_snapshot(&repo, &source_roots, &cargo_output_roots)?;
	if source_after != source_before {
		return Err(format!(
			"source inputs changed while cargo was building: before_tree={} after_tree={} \
			 before_untracked={:?} after_untracked={:?}",
			source_before.tree_id,
			source_after.tree_id,
			source_before.untracked_build_inputs,
			source_after.untracked_build_inputs,
		));
	}

	let recipe = BuildRecipe::new(
		options.profile.clone(),
		options.features.clone(),
		target.clone(),
		rustc_version,
		rustflags_sha256,
		cargo_lock_sha256,
	)?;
	let profile_dir = if options.profile == "dev" {
		"debug"
	} else {
		&options.profile
	};
	let mut harness_binary = PathBuf::from(&metadata.target_directory);
	harness_binary.push(&target);
	harness_binary.push(profile_dir);
	harness_binary.push(executable_name("toron-tpch", &target));
	if !harness_binary.is_file() {
		return Err(format!(
			"cargo succeeded but harness binary is absent at {}",
			harness_binary.display()
		));
	}
	let toron_binary = match options.toron_bin {
		Some(path) if path.is_absolute() => path,
		Some(path) => workspace.join(path),
		None => harness_binary.clone(),
	};
	let duckdb_binary = options.duckdb_bin.map(|path| {
		if path.is_absolute() {
			path
		} else {
			workspace.join(path)
		}
	});
	let staging_root = absolute_from(
		&workspace,
		options
			.staging_root
			.unwrap_or_else(|| PathBuf::from("target/tpch/evidence/staged")),
	)?;
	let harness = stage_executable(&harness_binary, &staging_root)?;
	let same_executable = harness_binary.canonicalize().ok() == toron_binary.canonicalize().ok();
	let toron = if same_executable {
		harness.clone()
	} else {
		stage_executable(&toron_binary, &staging_root)?
	};
	let duckdb = duckdb_binary
		.as_deref()
		.map(|binary| stage_executable(binary, &staging_root))
		.transpose()?;
	let manifest = BuildManifest::new(
		source_before.tree_id,
		commit_hint,
		recipe,
		harness,
		toron,
		duckdb,
		source_before.untracked_build_inputs,
	)?;
	let manifest_path = match options.manifest_path {
		Some(path) => absolute_from(&workspace, path)?,
		None => {
			let manifest_sha256 = sha256_bytes(&manifest.canonical_bytes()?);
			workspace.join("target/tpch/evidence/build").join(format!(
				"{}-{}-{}.manifest",
				manifest.source_tree_id,
				manifest.recipe_digest,
				&manifest_sha256[..12],
			))
		}
	};
	write_or_verify_manifest(&manifest, &manifest_path)?;
	Ok(manifest_path)
}

fn run_staged(args: &[OsString]) -> Result<ExitCode, String> {
	let separator = args
		.iter()
		.position(|arg| arg == OsStr::new("--"))
		.ok_or_else(|| "toron-bench run requires -- before harness arguments".to_string())?;
	let before = &args[..separator];
	if before.len() != 2 || before[0] != OsStr::new("--manifest") {
		return Err("toron-bench run requires exactly --manifest PATH before --".to_string());
	}
	let manifest_path = PathBuf::from(&before[1])
		.canonicalize()
		.map_err(|error| format!("canonicalize build manifest: {error}"))?;
	let manifest = BuildManifest::read(&manifest_path)?;
	let harness_root = manifest
		.harness
		.staged_path
		.parent()
		.ok_or_else(|| "staged harness has no parent".to_string())?;
	let toron_root = manifest
		.toron
		.staged_path
		.parent()
		.ok_or_else(|| "staged Toron has no parent".to_string())?;
	verify_staged_executable(&manifest.harness, harness_root)?;
	if manifest.toron != manifest.harness {
		verify_staged_executable(&manifest.toron, toron_root)?;
	}
	if let Some(duckdb) = &manifest.duckdb {
		let duckdb_root = duckdb
			.staged_path
			.parent()
			.ok_or_else(|| "staged DuckDB has no parent".to_string())?;
		verify_staged_executable(duckdb, duckdb_root)?;
	}
	let mut command = Command::new(&manifest.harness.staged_path);
	command
		.args(&args[separator + 1..])
		.env(BUILD_MANIFEST_ENV, &manifest_path)
		.env("TORON_TPCH_TORON_BIN", &manifest.toron.staged_path);
	if let Some(duckdb) = &manifest.duckdb {
		command.env("TORON_TPCH_DUCKDB_BIN", &duckdb.staged_path);
	}
	let status = command
		.status()
		.map_err(|error| format!("run staged harness: {error}"))?;
	Ok(ExitCode::from(status.code().unwrap_or(1) as u8))
}

fn seal(args: &[OsString]) -> Result<RunEvidenceManifest, String> {
	let values = parse_named_values(
		args,
		&[
			"--manifest",
			"--sealed",
			"--repo",
			"--archive-root",
			"--remote",
			"--tag",
			"--max-archive-bytes",
		],
	)?;
	let manifest_path = required_path(&values, "--manifest")?
		.canonicalize()
		.map_err(|error| format!("canonicalize evidence manifest: {error}"))?;
	let unsealed = RunEvidenceManifest::read(&manifest_path)?;
	let current = std::env::current_dir().map_err(|error| format!("current directory: {error}"))?;
	let repo = match values.get("--repo") {
		Some(path) => PathBuf::from(path),
		None => PathBuf::from(git_stdout(&current, &["rev-parse", "--show-toplevel"])?.trim()),
	};
	let repo = repo
		.canonicalize()
		.map_err(|error| format!("canonicalize evidence repo: {error}"))?;
	let sealed_path = values
		.get("--sealed")
		.map(PathBuf::from)
		.unwrap_or_else(|| PathBuf::from(format!("{}.sealed", manifest_path.display())));
	let archive_root = values
		.get("--archive-root")
		.map(PathBuf::from)
		.unwrap_or_else(|| repo.join("target/tpch/evidence/archive"));
	let remote = utf8_value(&values, "--remote")?.unwrap_or_else(|| "origin".to_string());
	let tag = utf8_value(&values, "--tag")?.unwrap_or_else(|| {
		format!(
			"evidence/{}-{}",
			&unsealed.build.source_tree_id[..12],
			&unsealed.artifact_sha256[..12]
		)
	});
	let max_archive_bytes = utf8_value(&values, "--max-archive-bytes")?
		.map(|value| {
			value
				.parse::<u64>()
				.map_err(|_| "--max-archive-bytes must be a u64".to_string())
		})
		.transpose()?
		.unwrap_or(DEFAULT_ARCHIVE_LIMIT_BYTES);
	seal_evidence_manifest(
		&manifest_path,
		&sealed_path,
		&SealOptions {
			repo,
			archive_root,
			remote,
			tag,
			max_archive_bytes,
		},
	)
}

fn parse_named_values(
	args: &[OsString],
	allowed: &[&str],
) -> Result<BTreeMap<String, OsString>, String> {
	let mut values = BTreeMap::new();
	let mut index = 0;
	while index < args.len() {
		let flag = args[index]
			.to_str()
			.ok_or_else(|| "command flag is not UTF-8".to_string())?;
		if !allowed.contains(&flag) {
			return Err(format!("unknown flag {flag}"));
		}
		let value = args
			.get(index + 1)
			.ok_or_else(|| format!("{flag} requires a value"))?
			.clone();
		if values.insert(flag.to_string(), value).is_some() {
			return Err(format!("duplicate flag {flag}"));
		}
		index += 2;
	}
	Ok(values)
}

fn required_path(values: &BTreeMap<String, OsString>, flag: &str) -> Result<PathBuf, String> {
	values
		.get(flag)
		.map(PathBuf::from)
		.ok_or_else(|| format!("{flag} is required"))
}

fn utf8_value(values: &BTreeMap<String, OsString>, flag: &str) -> Result<Option<String>, String> {
	values
		.get(flag)
		.map(|value| {
			value
				.clone()
				.into_string()
				.map_err(|_| format!("{flag} value is not UTF-8"))
		})
		.transpose()
}

fn parse_build_options(args: &[OsString]) -> Result<BuildOptions, String> {
	let mut values = BTreeMap::<String, OsString>::new();
	let mut index = 0;
	while index < args.len() {
		let flag = args[index]
			.to_str()
			.ok_or_else(|| "build flag is not UTF-8".to_string())?;
		if !matches!(
			flag,
			"--workspace"
				| "--profile"
				| "--features"
				| "--target" | "--toron-bin"
				| "--duckdb-bin"
				| "--staging-root"
				| "--manifest"
		) {
			return Err(format!("unknown toron-bench build flag {flag}"));
		}
		let value = args
			.get(index + 1)
			.ok_or_else(|| format!("{flag} requires a value"))?
			.clone();
		if values.insert(flag.to_string(), value).is_some() {
			return Err(format!("duplicate toron-bench build flag {flag}"));
		}
		index += 2;
	}
	let workspace = values
		.remove("--workspace")
		.map(PathBuf::from)
		.unwrap_or(std::env::current_dir().map_err(|error| format!("current directory: {error}"))?);
	let profile = values
		.remove("--profile")
		.and_then(|value| value.into_string().ok())
		.unwrap_or_else(|| "release".to_string());
	let mut features = values
		.remove("--features")
		.and_then(|value| value.into_string().ok())
		.map(|value| {
			value
				.split(',')
				.filter(|feature| !feature.is_empty())
				.map(ToOwned::to_owned)
				.collect::<Vec<_>>()
		})
		.unwrap_or_default();
	features.sort();
	features.dedup();
	Ok(BuildOptions {
		workspace,
		profile,
		features,
		target: optional_utf8(values.remove("--target"), "--target")?,
		toron_bin: values.remove("--toron-bin").map(PathBuf::from),
		duckdb_bin: values.remove("--duckdb-bin").map(PathBuf::from),
		staging_root: values.remove("--staging-root").map(PathBuf::from),
		manifest_path: values.remove("--manifest").map(PathBuf::from),
	})
}

fn optional_utf8(value: Option<OsString>, flag: &str) -> Result<Option<String>, String> {
	value
		.map(|value| {
			value
				.into_string()
				.map_err(|_| format!("{flag} value is not UTF-8"))
		})
		.transpose()
}

#[derive(Debug, Deserialize)]
struct CargoMetadata {
	packages: Vec<MetadataPackage>,
	resolve: Option<MetadataResolve>,
	target_directory: String,
}

#[derive(Debug, Deserialize)]
struct MetadataPackage {
	id: String,
	name: String,
	manifest_path: String,
}

#[derive(Debug, Deserialize)]
struct MetadataResolve {
	nodes: Vec<MetadataNode>,
}

#[derive(Debug, Deserialize)]
struct MetadataNode {
	id: String,
	deps: Vec<MetadataNodeDependency>,
}

#[derive(Debug, Deserialize)]
struct MetadataNodeDependency {
	pkg: String,
	dep_kinds: Vec<MetadataDependencyKind>,
}

#[derive(Debug, Deserialize)]
struct MetadataDependencyKind {
	kind: Option<String>,
}

fn cargo_metadata(
	workspace: &Path,
	features: &[String],
	target: Option<&str>,
) -> Result<CargoMetadata, String> {
	let mut command = Command::new("cargo");
	command
		.current_dir(workspace)
		.args(["metadata", "--format-version", "1", "--manifest-path"])
		.arg(workspace.join("crates/toron-tpch/Cargo.toml"));
	if !features.is_empty() {
		command.arg("--features").arg(features.join(","));
	}
	if let Some(target) = target {
		command.arg("--filter-platform").arg(target);
	}
	let output = command
		.output()
		.map_err(|error| format!("cargo metadata: {error}"))?;
	if !output.status.success() {
		return Err(format!(
			"cargo metadata failed: {}",
			String::from_utf8_lossy(&output.stderr)
		));
	}
	serde_json::from_slice(&output.stdout)
		.map_err(|error| format!("decode cargo metadata: {error}"))
}

fn local_dependency_roots(
	metadata: &CargoMetadata,
	repo: &Path,
	workspace: &Path,
) -> Result<Vec<PathBuf>, String> {
	let packages = metadata
		.packages
		.iter()
		.map(|package| (package.id.as_str(), package))
		.collect::<BTreeMap<_, _>>();
	let nodes = metadata
		.resolve
		.as_ref()
		.ok_or_else(|| "cargo metadata omitted the resolve graph".to_string())?
		.nodes
		.iter()
		.map(|node| (node.id.as_str(), node))
		.collect::<BTreeMap<_, _>>();
	let mut pending = metadata
		.packages
		.iter()
		.filter(|package| package.name == "toron-tpch")
		.map(|package| package.id.clone())
		.collect::<Vec<_>>();
	if pending.len() != 1 {
		return Err("cargo metadata did not identify exactly one toron-tpch package".to_string());
	}
	let mut visited = BTreeSet::new();
	while let Some(id) = pending.pop() {
		if !visited.insert(id.clone()) {
			continue;
		}
		if let Some(node) = nodes.get(id.as_str()) {
			pending.extend(
				node.deps
					.iter()
					.filter(|dependency| {
						dependency
							.dep_kinds
							.iter()
							.any(|kind| kind.kind.as_deref() != Some("dev"))
					})
					.map(|dependency| dependency.pkg.clone()),
			);
		}
	}
	let mut roots = Vec::new();
	for id in visited {
		let Some(package) = packages.get(id.as_str()) else {
			continue;
		};
		let manifest = PathBuf::from(&package.manifest_path);
		if manifest.starts_with(repo) {
			roots.push(
				manifest
					.parent()
					.expect("Cargo.toml has a parent")
					.strip_prefix(repo)
					.expect("checked prefix")
					.to_path_buf(),
			);
		}
	}
	for input in [
		workspace.join("Cargo.toml"),
		workspace.join("Cargo.lock"),
		workspace.join(".cargo"),
	] {
		if input.exists() && input.starts_with(repo) {
			roots.push(
				input
					.strip_prefix(repo)
					.expect("checked prefix")
					.to_path_buf(),
			);
		}
	}
	roots.sort();
	roots.dedup();
	Ok(roots)
}

fn cargo_output_roots(
	metadata: &CargoMetadata,
	repo: &Path,
	source_roots: &[PathBuf],
) -> Vec<PathBuf> {
	let mut outputs = source_roots
		.iter()
		.map(|root| root.join("target"))
		.collect::<Vec<_>>();
	let target_directory = Path::new(&metadata.target_directory);
	if let Ok(relative) = target_directory.strip_prefix(repo) {
		outputs.push(relative.to_path_buf());
	}
	outputs.sort();
	outputs.dedup();
	outputs
}

fn source_snapshot(
	repo: &Path,
	roots: &[PathBuf],
	cargo_output_roots: &[PathBuf],
) -> Result<SourceSnapshot, String> {
	Ok(SourceSnapshot {
		tree_id: source_tree_id(repo, roots)?,
		untracked_build_inputs: untracked_inputs(repo, roots, cargo_output_roots)?,
	})
}

fn untracked_inputs(
	repo: &Path,
	roots: &[PathBuf],
	excluded_roots: &[PathBuf],
) -> Result<Vec<String>, String> {
	let mut command = Command::new("git");
	command
		.current_dir(repo)
		.args(["ls-files", "-z", "--others", "--"]);
	command.args(roots);
	for root in excluded_roots {
		let root = root
			.to_str()
			.ok_or_else(|| "Cargo output path is not UTF-8".to_string())?;
		command.arg(format!(":(exclude){root}"));
		command.arg(format!(":(exclude){root}/**"));
	}
	let output = command
		.output()
		.map_err(|error| format!("git ls-files: {error}"))?;
	if !output.status.success() {
		return Err(format!(
			"git ls-files failed: {}",
			String::from_utf8_lossy(&output.stderr)
		));
	}
	let mut paths = output
		.stdout
		.split(|byte| *byte == 0)
		.filter(|path| !path.is_empty())
		.map(|path| {
			String::from_utf8(path.to_vec()).map_err(|_| "untracked path is not UTF-8".to_string())
		})
		.collect::<Result<Vec<_>, _>>()?;
	paths.sort();
	paths.dedup();
	Ok(paths)
}

fn source_tree_id(repo: &Path, roots: &[PathBuf]) -> Result<String, String> {
	// Read the tracked-path universe through the caller's real index before switching to the
	// temporary one. This includes newly added tracked files, which are absent from HEAD and
	// therefore cannot be recovered by `git add -u` against a HEAD-seeded index.
	let mut tracked = Command::new("git");
	tracked
		.current_dir(repo)
		.args(["ls-files", "-z", "--cached", "--"])
		.args(roots);
	let tracked = tracked
		.output()
		.map_err(|error| format!("enumerate tracked inputs: {error}"))?;
	if !tracked.status.success() {
		return Err(format!(
			"enumerate tracked inputs failed: {}",
			String::from_utf8_lossy(&tracked.stderr)
		));
	}

	let temporary = TempDirectory::new("toron-evidence-index")?;
	let index = temporary.path().join("index");
	git_with_index(repo, &index, &["read-tree", "HEAD"])?;
	let mut remove = Command::new("git");
	remove
		.current_dir(repo)
		.env("GIT_INDEX_FILE", &index)
		.args(["rm", "-r", "-f", "--cached", "--ignore-unmatch", "--"])
		.args(roots);
	let remove = remove
		.output()
		.map_err(|error| format!("clear tracked snapshot roots: {error}"))?;
	if !remove.status.success() {
		return Err(format!(
			"clear tracked snapshot roots failed: {}",
			String::from_utf8_lossy(&remove.stderr)
		));
	}
	let mut command = Command::new("git");
	command
		.current_dir(repo)
		.env("GIT_INDEX_FILE", &index)
		.args(["update-index", "--add", "--remove", "-z", "--stdin"])
		.stdin(Stdio::piped())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped());
	let mut child = command
		.spawn()
		.map_err(|error| format!("snapshot tracked inputs: {error}"))?;
	child
		.stdin
		.take()
		.expect("piped git stdin")
		.write_all(&tracked.stdout)
		.map_err(|error| format!("send tracked paths to snapshot index: {error}"))?;
	let output = child
		.wait_with_output()
		.map_err(|error| format!("snapshot tracked inputs: {error}"))?;
	if !output.status.success() {
		return Err(format!(
			"snapshot tracked inputs failed: {}",
			String::from_utf8_lossy(&output.stderr)
		));
	}
	Ok(git_with_index(repo, &index, &["write-tree"])?
		.trim()
		.to_string())
}

fn git_with_index(repo: &Path, index: &Path, args: &[&str]) -> Result<String, String> {
	let output = Command::new("git")
		.current_dir(repo)
		.env("GIT_INDEX_FILE", index)
		.args(args)
		.output()
		.map_err(|error| format!("git {}: {error}", args.join(" ")))?;
	if !output.status.success() {
		return Err(format!(
			"git {} failed: {}",
			args.join(" "),
			String::from_utf8_lossy(&output.stderr)
		));
	}
	String::from_utf8(output.stdout).map_err(|_| "git output is not UTF-8".to_string())
}

fn git_stdout(repo: &Path, args: &[&str]) -> Result<String, String> {
	let output = Command::new("git")
		.current_dir(repo)
		.args(args)
		.output()
		.map_err(|error| format!("git {}: {error}", args.join(" ")))?;
	if !output.status.success() {
		return Err(format!(
			"git {} failed: {}",
			args.join(" "),
			String::from_utf8_lossy(&output.stderr)
		));
	}
	String::from_utf8(output.stdout).map_err(|_| "git output is not UTF-8".to_string())
}

fn command_stdout(command: &mut Command, label: &str) -> Result<String, String> {
	let output = command
		.output()
		.map_err(|error| format!("{label}: {error}"))?;
	if !output.status.success() {
		return Err(format!(
			"{label} failed: {}",
			String::from_utf8_lossy(&output.stderr)
		));
	}
	String::from_utf8(output.stdout).map_err(|_| format!("{label} output is not UTF-8"))
}

/// Canonicalize every input Cargo uses to construct effective rustflags. This deliberately does
/// not inspect the set of rustc processes that happened to run: that set depends on incremental
/// cache state and is not part of a build recipe.
fn effective_rustflags_digest(workspace: &Path, target: &str) -> Result<String, String> {
	let mut inputs = Vec::<(String, Vec<u8>)>::new();
	for key in [
		"RUSTFLAGS".to_string(),
		"CARGO_ENCODED_RUSTFLAGS".to_string(),
		"CARGO_BUILD_RUSTFLAGS".to_string(),
		format!(
			"CARGO_TARGET_{}_RUSTFLAGS",
			target.to_ascii_uppercase().replace(['-', '.'], "_")
		),
	] {
		let value = std::env::var_os(&key)
			.map(|value| value.to_string_lossy().as_bytes().to_vec())
			.unwrap_or_default();
		inputs.push((format!("env:{key}"), value));
	}
	let mut ancestors = workspace.ancestors().collect::<Vec<_>>();
	ancestors.reverse();
	for ancestor in ancestors {
		for name in ["config", "config.toml"] {
			let path = ancestor.join(".cargo").join(name);
			if path.is_file() {
				inputs.push((
					format!("config:{}", path.display()),
					std::fs::read(&path).map_err(|error| {
						format!("read Cargo config {}: {error}", path.display())
					})?,
				));
			}
		}
	}
	let cargo_home = std::env::var_os("CARGO_HOME").or_else(|| {
		std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cargo").into_os_string())
	});
	if let Some(cargo_home) = cargo_home {
		for name in ["config", "config.toml"] {
			let path = PathBuf::from(&cargo_home).join(name);
			if path.is_file() {
				inputs.push((
					format!("cargo-home-config:{name}"),
					std::fs::read(&path).map_err(|error| {
						format!("read Cargo config {}: {error}", path.display())
					})?,
				));
			}
		}
	}
	inputs.sort_by(|left, right| left.0.cmp(&right.0));
	let mut canonical = Vec::new();
	for (label, value) in inputs {
		canonical.extend_from_slice(&(label.len() as u32).to_be_bytes());
		canonical.extend_from_slice(label.as_bytes());
		canonical.extend_from_slice(&(value.len() as u64).to_be_bytes());
		canonical.extend_from_slice(&value);
	}
	Ok(sha256_bytes(&canonical))
}

fn write_or_verify_manifest(manifest: &BuildManifest, path: &Path) -> Result<(), String> {
	if path.exists() {
		let existing = BuildManifest::read(path)?;
		if &existing == manifest {
			return Ok(());
		}
		return Err(format!(
			"refuse to overwrite different build manifest {}",
			path.display()
		));
	}
	manifest.write_new(path)
}

fn absolute_from(base: &Path, path: PathBuf) -> Result<PathBuf, String> {
	let path = if path.is_absolute() {
		path
	} else {
		base.join(path)
	};
	if let Some(parent) = path.parent() {
		std::fs::create_dir_all(parent)
			.map_err(|error| format!("create {}: {error}", parent.display()))?;
	}
	Ok(path)
}

fn executable_name(name: &str, target: &str) -> OsString {
	if target.split('-').any(|component| component == "windows") {
		format!("{name}.exe").into()
	} else {
		name.into()
	}
}

struct TempDirectory(PathBuf);

impl TempDirectory {
	fn new(label: &str) -> Result<Self, String> {
		for attempt in 0..100u32 {
			let path =
				std::env::temp_dir().join(format!("{label}-{}-{attempt}", std::process::id()));
			match std::fs::create_dir(&path) {
				Ok(()) => return Ok(Self(path)),
				Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
				Err(error) => return Err(format!("create temporary directory: {error}")),
			}
		}
		Err("could not allocate a temporary directory".to_string())
	}

	fn path(&self) -> &Path {
		&self.0
	}
}

impl Drop for TempDirectory {
	fn drop(&mut self) {
		let _ = std::fs::remove_dir_all(&self.0);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn git(repo: &Path, args: &[&str]) {
		let status = Command::new("git")
			.current_dir(repo)
			.args(args)
			.env("GIT_AUTHOR_NAME", "ADR 581")
			.env("GIT_AUTHOR_EMAIL", "adr581@example.invalid")
			.env("GIT_COMMITTER_NAME", "ADR 581")
			.env("GIT_COMMITTER_EMAIL", "adr581@example.invalid")
			.status()
			.unwrap();
		assert!(status.success(), "git {args:?}");
	}

	#[test]
	fn build_features_are_sorted_and_deduplicated() {
		let options = parse_build_options(&[
			"--features".into(),
			"z,a,z".into(),
			"--profile".into(),
			"release".into(),
			"--duckdb-bin".into(),
			"/opt/duckdb".into(),
		])
		.unwrap();
		assert_eq!(options.features, ["a", "z"]);
		assert_eq!(options.duckdb_bin, Some(PathBuf::from("/opt/duckdb")));
	}

	#[test]
	fn executable_suffix_follows_the_build_target_not_the_wrapper_host() {
		assert_eq!(
			executable_name("toron-tpch", "x86_64-pc-windows-msvc"),
			OsString::from("toron-tpch.exe")
		);
		assert_eq!(
			executable_name("toron-tpch", "x86_64-unknown-linux-gnu"),
			OsString::from("toron-tpch")
		);
	}

	#[test]
	fn build_input_closure_includes_normal_and_build_dependencies_but_not_dev_only_ones() {
		let repo = Path::new("/repo");
		let workspace = repo.join("nilket");
		let package = |id: &str, name: &str| MetadataPackage {
			id: id.into(),
			name: name.into(),
			manifest_path: workspace
				.join("crates")
				.join(name)
				.join("Cargo.toml")
				.to_string_lossy()
				.into_owned(),
		};
		let dependency = |pkg: &str, kind: Option<&str>| MetadataNodeDependency {
			pkg: pkg.into(),
			dep_kinds: vec![MetadataDependencyKind {
				kind: kind.map(str::to_string),
			}],
		};
		let metadata = CargoMetadata {
			packages: vec![
				package("root", "toron-tpch"),
				package("normal", "toron"),
				package("build", "build-helper"),
				package("dev", "test-helper"),
			],
			resolve: Some(MetadataResolve {
				nodes: vec![
					MetadataNode {
						id: "root".into(),
						deps: vec![
							dependency("normal", None),
							dependency("build", Some("build")),
							dependency("dev", Some("dev")),
						],
					},
					MetadataNode {
						id: "normal".into(),
						deps: Vec::new(),
					},
					MetadataNode {
						id: "build".into(),
						deps: Vec::new(),
					},
					MetadataNode {
						id: "dev".into(),
						deps: Vec::new(),
					},
				],
			}),
			target_directory: workspace.join("target").to_string_lossy().into_owned(),
		};

		let roots = local_dependency_roots(&metadata, repo, &workspace).unwrap();
		assert_eq!(
			roots,
			[
				PathBuf::from("nilket/crates/build-helper"),
				PathBuf::from("nilket/crates/toron"),
				PathBuf::from("nilket/crates/toron-tpch"),
			]
		);
		let outputs = cargo_output_roots(&metadata, repo, &roots);
		assert!(outputs.contains(&PathBuf::from("nilket/target")));
		assert!(outputs.contains(&PathBuf::from("nilket/crates/toron/target")));
	}

	#[test]
	fn run_requires_a_manifest_and_argument_separator() {
		assert!(run_staged(&[]).unwrap_err().contains("requires --"));
		assert!(
			run_staged(&["--".into()])
				.unwrap_err()
				.contains("--manifest")
		);
	}

	#[test]
	fn tracked_snapshot_sees_unstaged_edits_without_moving_head_or_the_real_index() {
		let temporary = TempDirectory::new("toron-evidence-git").unwrap();
		let repo = temporary.path();
		git(repo, &["init", "-q"]);
		std::fs::create_dir(repo.join("source")).unwrap();
		std::fs::write(repo.join("source/lib.rs"), "one\n").unwrap();
		git(repo, &["add", "source/lib.rs"]);
		git(repo, &["commit", "-qm", "base"]);
		let roots = vec![PathBuf::from("source")];
		let head_before = git_stdout(repo, &["rev-parse", "HEAD"]).unwrap();
		let index_before = git_stdout(repo, &["write-tree"]).unwrap();
		let tree_before = source_tree_id(repo, &roots).unwrap();
		let executable_before = stage_executable(&repo.join("source/lib.rs"), &repo.join("stage"))
			.unwrap()
			.sha256;
		std::fs::write(repo.join("source/lib.rs"), "two\n").unwrap();
		let tree_after = source_tree_id(repo, &roots).unwrap();
		let executable_after = stage_executable(&repo.join("source/lib.rs"), &repo.join("stage"))
			.unwrap()
			.sha256;
		assert_ne!(
			tree_before, tree_after,
			"the unstaged source edit is in the snapshot"
		);
		assert_ne!(
			executable_before, executable_after,
			"changed executable bytes receive a new content identity"
		);
		assert_eq!(
			git_stdout(repo, &["rev-parse", "HEAD"]).unwrap(),
			head_before
		);
		assert_eq!(git_stdout(repo, &["write-tree"]).unwrap(), index_before);
	}

	#[test]
	fn only_untracked_files_inside_the_declared_closure_are_build_inputs() {
		let temporary = TempDirectory::new("toron-evidence-untracked").unwrap();
		let repo = temporary.path();
		git(repo, &["init", "-q"]);
		std::fs::create_dir(repo.join("source")).unwrap();
		std::fs::create_dir(repo.join("notes")).unwrap();
		std::fs::write(repo.join("source/tracked.rs"), "tracked\n").unwrap();
		std::fs::write(
			repo.join(".gitignore"),
			"source/ignored.rs\nsource/target/\n",
		)
		.unwrap();
		git(repo, &["add", "source/tracked.rs"]);
		git(repo, &["add", ".gitignore"]);
		git(repo, &["commit", "-qm", "base"]);
		std::fs::write(repo.join("source/generated.rs"), "input\n").unwrap();
		std::fs::write(
			repo.join("source/ignored.rs"),
			"ignored but still an input\n",
		)
		.unwrap();
		std::fs::write(repo.join("notes/run.log"), "not an input\n").unwrap();
		std::fs::create_dir(repo.join("source/target")).unwrap();
		std::fs::write(repo.join("source/target/output.bin"), "Cargo output\n").unwrap();
		assert_eq!(
			untracked_inputs(
				repo,
				&[PathBuf::from("source")],
				&[PathBuf::from("source/target")],
			)
			.unwrap(),
			["source/generated.rs", "source/ignored.rs"]
		);
	}

	#[test]
	fn a_source_change_between_the_two_build_snapshots_is_detected() {
		let temporary = TempDirectory::new("toron-evidence-build-window").unwrap();
		let repo = temporary.path();
		git(repo, &["init", "-q"]);
		std::fs::create_dir(repo.join("source")).unwrap();
		std::fs::write(repo.join("source/lib.rs"), "before\n").unwrap();
		git(repo, &["add", "source/lib.rs"]);
		git(repo, &["commit", "-qm", "base"]);
		let roots = [PathBuf::from("source")];
		let before = source_snapshot(repo, &roots, &[]).unwrap();

		std::fs::write(repo.join("source/lib.rs"), "during build\n").unwrap();
		let after_tracked_change = source_snapshot(repo, &roots, &[]).unwrap();
		assert_ne!(before, after_tracked_change);

		std::fs::write(repo.join("source/generated.rs"), "during build\n").unwrap();
		let after_untracked_change = source_snapshot(repo, &roots, &[]).unwrap();
		assert_ne!(after_tracked_change, after_untracked_change);
	}

	#[test]
	fn tracked_snapshot_includes_a_newly_added_file_without_moving_the_real_index() {
		let temporary = TempDirectory::new("toron-evidence-added-file").unwrap();
		let repo = temporary.path();
		git(repo, &["init", "-q"]);
		std::fs::create_dir(repo.join("source")).unwrap();
		std::fs::write(repo.join("source/lib.rs"), "base\n").unwrap();
		git(repo, &["add", "source/lib.rs"]);
		git(repo, &["commit", "-qm", "base"]);
		std::fs::write(repo.join("source/new.rs"), "new tracked input\n").unwrap();
		git(repo, &["add", "source/new.rs"]);
		let index_before = git_stdout(repo, &["write-tree"]).unwrap();

		let snapshot = source_tree_id(repo, &[PathBuf::from("source")]).unwrap();
		let names = git_stdout(repo, &["ls-tree", "-r", "--name-only", &snapshot]).unwrap();
		assert!(names.lines().any(|name| name == "source/new.rs"));
		assert_eq!(git_stdout(repo, &["write-tree"]).unwrap(), index_before);
		assert!(
			untracked_inputs(repo, &[PathBuf::from("source")], &[])
				.unwrap()
				.is_empty()
		);
	}

	#[test]
	fn rustflag_identity_is_stable_across_cache_state_and_changes_with_config() {
		let temporary = TempDirectory::new("toron-evidence-rustflags").unwrap();
		let workspace = temporary.path();
		std::fs::create_dir(workspace.join(".cargo")).unwrap();
		let config = workspace.join(".cargo/config.toml");
		std::fs::write(&config, "[build]\nrustflags = ['-Cdebuginfo=0']\n").unwrap();
		let first = effective_rustflags_digest(workspace, "test-target").unwrap();
		let second = effective_rustflags_digest(workspace, "test-target").unwrap();
		assert_eq!(first, second);
		std::fs::write(&config, "[build]\nrustflags = ['-Cdebuginfo=1']\n").unwrap();
		assert_ne!(
			first,
			effective_rustflags_digest(workspace, "test-target").unwrap()
		);
	}
}
