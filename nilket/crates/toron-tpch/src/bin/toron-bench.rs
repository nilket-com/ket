//! ADR 581 build wrapper. `build` constructs identity and immutable staged executables; `run`
//! executes only those staged bytes. Sealing is deliberately a separate explicit subcommand.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use serde::Deserialize;
use toron_tpch::evidence::{
	BUILD_MANIFEST_ENV, BuildManifest, BuildRecipe, sha256_bytes, sha256_file, stage_executable,
	verify_staged_executable,
};

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
		_ => usage(),
	}
}

fn usage() -> ExitCode {
	eprintln!(
		"usage: toron-bench build [--workspace PATH] [--profile PROFILE] [--features A,B] \
		 [--target TRIPLE] [--toron-bin PATH] [--staging-root PATH] [--manifest PATH]\n       \
		 toron-bench run --manifest PATH -- <toron-tpch arguments>"
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
	staging_root: Option<PathBuf>,
	manifest_path: Option<PathBuf>,
}

fn build(args: &[OsString]) -> Result<PathBuf, String> {
	let options = parse_build_options(args)?;
	let workspace = options.workspace.canonicalize().map_err(|error| {
		format!(
			"canonicalize workspace {}: {error}",
			options.workspace.display()
		)
	})?;
	let metadata = cargo_metadata(&workspace)?;
	let repo = git_stdout(&workspace, &["rev-parse", "--show-toplevel"])?;
	let repo = PathBuf::from(repo.trim());
	let source_roots = local_dependency_roots(&metadata, &repo)?;
	let untracked_build_inputs = untracked_inputs(&repo, &source_roots)?;
	let source_tree_id = source_tree_id(&repo, &source_roots)?;
	let commit_hint = git_stdout(&repo, &["rev-parse", "HEAD"])?
		.trim()
		.to_string();
	let rustc_version = command_stdout(Command::new("rustc").arg("-vV"), "rustc -vV")?;
	let target = match options.target.clone() {
		Some(target) => target,
		None => rustc_version
			.lines()
			.find_map(|line| line.strip_prefix("host: "))
			.ok_or_else(|| "rustc -vV did not report a host triple".to_string())?
			.to_string(),
	};
	let cargo_lock = workspace.join("Cargo.lock");
	let cargo_lock_sha256 = sha256_file(&cargo_lock)?;

	let rustflags_sha256 = effective_rustflags_digest(&workspace, &target)?;
	let mut cargo = Command::new("cargo");
	cargo
		.current_dir(&workspace)
		.arg("build")
		.args(["-p", "toron-tpch", "--bin", "toron-tpch", "--profile"])
		.arg(&options.profile);
	if !options.features.is_empty() {
		cargo.arg("--features").arg(options.features.join(","));
	}
	if let Some(target) = &options.target {
		cargo.arg("--target").arg(target);
	}
	let status = cargo
		.status()
		.map_err(|error| format!("run cargo build: {error}"))?;
	if !status.success() {
		return Err(format!("cargo build failed with {status}"));
	}

	let recipe = BuildRecipe::new(
		options.profile.clone(),
		options.features.clone(),
		target,
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
	if let Some(target) = &options.target {
		harness_binary.push(target);
	}
	harness_binary.push(profile_dir);
	harness_binary.push(executable_name("toron-tpch"));
	if !harness_binary.is_file() {
		return Err(format!(
			"cargo succeeded but harness binary is absent at {}",
			harness_binary.display()
		));
	}
	let toron_binary = options.toron_bin.unwrap_or_else(|| harness_binary.clone());
	let staging_root = absolute_from(
		&workspace,
		options
			.staging_root
			.unwrap_or_else(|| PathBuf::from("target/tpch/evidence/staged")),
	)?;
	let harness = stage_executable(&harness_binary, &staging_root)?;
	let toron = stage_executable(&toron_binary, &staging_root)?;
	let manifest = BuildManifest::new(
		source_tree_id,
		commit_hint,
		recipe,
		harness,
		toron,
		untracked_build_inputs,
	)?;
	let manifest_path = match options.manifest_path {
		Some(path) => absolute_from(&workspace, path)?,
		None => workspace.join("target/tpch/evidence/build").join(format!(
			"{}-{}.manifest",
			manifest.source_tree_id, manifest.recipe_digest
		)),
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
	verify_staged_executable(&manifest.toron, toron_root)?;
	let status = Command::new(&manifest.harness.staged_path)
		.args(&args[separator + 1..])
		.env(BUILD_MANIFEST_ENV, &manifest_path)
		.env("TORON_TPCH_TORON_BIN", &manifest.toron.staged_path)
		.status()
		.map_err(|error| format!("run staged harness: {error}"))?;
	Ok(ExitCode::from(status.code().unwrap_or(1) as u8))
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
	dependencies: Vec<String>,
}

fn cargo_metadata(workspace: &Path) -> Result<CargoMetadata, String> {
	let output = Command::new("cargo")
		.current_dir(workspace)
		.args(["metadata", "--format-version", "1"])
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

fn local_dependency_roots(metadata: &CargoMetadata, repo: &Path) -> Result<Vec<PathBuf>, String> {
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
			pending.extend(node.dependencies.iter().cloned());
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
	let workspace = PathBuf::from(&metadata.target_directory)
		.parent()
		.ok_or_else(|| "cargo target directory has no workspace parent".to_string())?
		.to_path_buf();
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

fn untracked_inputs(repo: &Path, roots: &[PathBuf]) -> Result<Vec<String>, String> {
	let mut command = Command::new("git");
	command
		.current_dir(repo)
		.args(["ls-files", "-z", "--others", "--exclude-standard", "--"]);
	command.args(roots);
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
	let temporary = TempDirectory::new("toron-evidence-index")?;
	let index = temporary.path().join("index");
	git_with_index(repo, &index, &["read-tree", "HEAD"])?;
	let mut command = Command::new("git");
	command
		.current_dir(repo)
		.env("GIT_INDEX_FILE", &index)
		.args(["add", "-u", "--"])
		.args(roots);
	let output = command
		.output()
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

fn executable_name(name: &str) -> OsString {
	if cfg!(windows) {
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
		])
		.unwrap();
		assert_eq!(options.features, ["a", "z"]);
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
		std::fs::write(repo.join("source/lib.rs"), "two\n").unwrap();
		let tree_after = source_tree_id(repo, &roots).unwrap();
		assert_ne!(
			tree_before, tree_after,
			"the unstaged source edit is in the snapshot"
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
		git(repo, &["add", "source/tracked.rs"]);
		git(repo, &["commit", "-qm", "base"]);
		std::fs::write(repo.join("source/generated.rs"), "input\n").unwrap();
		std::fs::write(repo.join("notes/run.log"), "not an input\n").unwrap();
		assert_eq!(
			untracked_inputs(repo, &[PathBuf::from("source")]).unwrap(),
			["source/generated.rs"]
		);
	}
}
