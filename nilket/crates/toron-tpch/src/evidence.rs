//! ADR 581: content-addressed identity for performance evidence.
//!
//! Identity is constructed before timing by `toron-bench`. The timed process only reads and
//! reports the supplied manifest; it never hashes itself or asks git what it was built from.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;
use sha2::{Digest, Sha256};

pub const BUILD_MANIFEST_ENV: &str = "TORON_TPCH_BUILD_MANIFEST";
pub const BUILD_MANIFEST_VERSION: u8 = 2;
pub const RECIPE_VERSION: u8 = 1;
pub const EVIDENCE_MANIFEST_VERSION: u8 = 2;
pub const SHA256_HEX_LEN: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BuildRecipe {
	pub profile: String,
	pub features: Vec<String>,
	pub target: String,
	pub rustc_version: String,
	pub rustflags_sha256: String,
	pub cargo_lock_sha256: String,
}

impl BuildRecipe {
	pub fn new(
		profile: impl Into<String>,
		features: impl IntoIterator<Item = String>,
		target: impl Into<String>,
		rustc_version: impl Into<String>,
		rustflags_sha256: impl Into<String>,
		cargo_lock_sha256: impl Into<String>,
	) -> Result<Self, String> {
		let mut features = features.into_iter().collect::<Vec<_>>();
		features.sort();
		features.dedup();
		let recipe = Self {
			profile: profile.into(),
			features,
			target: target.into(),
			rustc_version: rustc_version.into(),
			rustflags_sha256: rustflags_sha256.into(),
			cargo_lock_sha256: cargo_lock_sha256.into(),
		};
		require_sha256("rustflags", &recipe.rustflags_sha256)?;
		require_sha256("Cargo.lock", &recipe.cargo_lock_sha256)?;
		Ok(recipe)
	}

	pub fn canonical_bytes(&self) -> Vec<u8> {
		let mut bytes = vec![RECIPE_VERSION];
		put_string(&mut bytes, &self.profile);
		put_strings(&mut bytes, &self.features);
		put_string(&mut bytes, &self.target);
		put_string(&mut bytes, &self.rustc_version);
		put_string(&mut bytes, &self.rustflags_sha256);
		put_string(&mut bytes, &self.cargo_lock_sha256);
		bytes
	}

	pub fn digest(&self) -> String {
		sha256_bytes(&self.canonical_bytes())
	}

	fn decode(cursor: &mut Cursor<'_>) -> Result<Self, String> {
		let version = cursor.byte()?;
		if version != RECIPE_VERSION {
			return Err(format!("unknown evidence recipe version {version}"));
		}
		let recipe = Self {
			profile: cursor.string()?,
			features: cursor.strings()?,
			target: cursor.string()?,
			rustc_version: cursor.string()?,
			rustflags_sha256: cursor.string()?,
			cargo_lock_sha256: cursor.string()?,
		};
		if !is_sorted_unique(&recipe.features) {
			return Err(
				"evidence recipe features are not canonically sorted and unique".to_string(),
			);
		}
		require_sha256("rustflags", &recipe.rustflags_sha256)?;
		require_sha256("Cargo.lock", &recipe.cargo_lock_sha256)?;
		Ok(recipe)
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutableIdentity {
	pub sha256: String,
	pub staged_path: PathBuf,
}

impl ExecutableIdentity {
	fn encode(&self, bytes: &mut Vec<u8>) -> Result<(), String> {
		require_sha256("executable", &self.sha256)?;
		put_string(bytes, &self.sha256);
		put_path(bytes, &self.staged_path)?;
		Ok(())
	}

	fn decode(cursor: &mut Cursor<'_>) -> Result<Self, String> {
		let identity = Self {
			sha256: cursor.string()?,
			staged_path: PathBuf::from(cursor.string()?),
		};
		require_sha256("executable", &identity.sha256)?;
		Ok(identity)
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BuildManifest {
	pub source_tree_id: String,
	pub commit_hint: String,
	pub recipe: BuildRecipe,
	pub recipe_digest: String,
	pub harness: ExecutableIdentity,
	pub toron: ExecutableIdentity,
	pub duckdb: Option<ExecutableIdentity>,
	pub untracked_build_inputs: Vec<String>,
}

impl BuildManifest {
	pub fn new(
		source_tree_id: impl Into<String>,
		commit_hint: impl Into<String>,
		recipe: BuildRecipe,
		harness: ExecutableIdentity,
		toron: ExecutableIdentity,
		duckdb: Option<ExecutableIdentity>,
		mut untracked_build_inputs: Vec<String>,
	) -> Result<Self, String> {
		untracked_build_inputs.sort();
		untracked_build_inputs.dedup();
		let manifest = Self {
			source_tree_id: source_tree_id.into(),
			commit_hint: commit_hint.into(),
			recipe_digest: recipe.digest(),
			recipe,
			harness,
			toron,
			duckdb,
			untracked_build_inputs,
		};
		manifest.validate()?;
		Ok(manifest)
	}

	pub fn validate(&self) -> Result<(), String> {
		if self.source_tree_id.len() != 40
			|| !self
				.source_tree_id
				.bytes()
				.all(|byte| byte.is_ascii_hexdigit())
		{
			return Err("evidence source tree id is not a full git object id".to_string());
		}
		require_sha256("recipe", &self.recipe_digest)?;
		if self.recipe_digest != self.recipe.digest() {
			return Err("evidence recipe digest does not match the canonical recipe".to_string());
		}
		require_sha256("harness", &self.harness.sha256)?;
		require_sha256("toron", &self.toron.sha256)?;
		if let Some(duckdb) = &self.duckdb {
			require_sha256("duckdb", &duckdb.sha256)?;
		}
		if !is_sorted_unique(&self.recipe.features) {
			return Err("evidence recipe features are not sorted and unique".to_string());
		}
		if !is_sorted_unique(&self.untracked_build_inputs) {
			return Err("untracked build inputs are not sorted and unique".to_string());
		}
		Ok(())
	}

	pub fn canonical_bytes(&self) -> Result<Vec<u8>, String> {
		self.validate()?;
		let mut bytes = vec![BUILD_MANIFEST_VERSION];
		put_string(&mut bytes, &self.source_tree_id);
		put_string(&mut bytes, &self.commit_hint);
		let recipe = self.recipe.canonical_bytes();
		put_bytes(&mut bytes, &recipe);
		put_string(&mut bytes, &self.recipe_digest);
		self.harness.encode(&mut bytes)?;
		self.toron.encode(&mut bytes)?;
		put_optional_executable(&mut bytes, self.duckdb.as_ref())?;
		put_strings(&mut bytes, &self.untracked_build_inputs);
		Ok(bytes)
	}

	pub fn decode(bytes: &[u8]) -> Result<Self, String> {
		let mut cursor = Cursor::new(bytes);
		let version = cursor.byte()?;
		if !matches!(version, 1 | BUILD_MANIFEST_VERSION) {
			return Err(format!("unknown build manifest version {version}"));
		}
		let source_tree_id = cursor.string()?;
		let commit_hint = cursor.string()?;
		let recipe_bytes = cursor.bytes()?;
		let mut recipe_cursor = Cursor::new(recipe_bytes);
		let recipe = BuildRecipe::decode(&mut recipe_cursor)?;
		recipe_cursor.finish("recipe")?;
		let recipe_digest = cursor.string()?;
		let harness = ExecutableIdentity::decode(&mut cursor)?;
		let toron = ExecutableIdentity::decode(&mut cursor)?;
		let duckdb = if version >= 2 {
			decode_optional_executable(&mut cursor)?
		} else {
			None
		};
		let untracked_build_inputs = cursor.strings()?;
		cursor.finish("build manifest")?;
		let manifest = Self {
			source_tree_id,
			commit_hint,
			recipe,
			recipe_digest,
			harness,
			toron,
			duckdb,
			untracked_build_inputs,
		};
		manifest.validate()?;
		Ok(manifest)
	}

	pub fn read(path: &Path) -> Result<Self, String> {
		let bytes = std::fs::read(path)
			.map_err(|error| format!("read build manifest {}: {error}", path.display()))?;
		Self::decode(&bytes)
	}

	pub fn write_new(&self, path: &Path) -> Result<(), String> {
		write_new_synced(path, &self.canonical_bytes()?)
	}

	pub fn authority_issues(&self) -> Vec<String> {
		self.untracked_build_inputs
			.iter()
			.map(|path| format!("untracked_build_input:{path}"))
			.collect()
	}

	pub fn executable_for_path(&self, path: &Path) -> Option<&ExecutableIdentity> {
		let canonical = path.canonicalize().ok()?;
		[Some(&self.harness), Some(&self.toron), self.duckdb.as_ref()]
			.into_iter()
			.flatten()
			.find(|identity| identity.staged_path.canonicalize().ok().as_ref() == Some(&canonical))
	}

	pub fn json_value_for_executable(&self, executable: &ExecutableIdentity) -> serde_json::Value {
		serde_json::json!({
			"manifest_version": BUILD_MANIFEST_VERSION,
			"recipe_version": RECIPE_VERSION,
			"executable_sha256": executable.sha256,
			"harness_sha256": self.harness.sha256,
			"toron_sha256": self.toron.sha256,
			"duckdb_sha256": self.duckdb.as_ref().map(|identity| &identity.sha256),
			"recipe_sha256": self.recipe_digest,
			"source_tree_id": self.source_tree_id,
			"commit_hint": self.commit_hint,
			"untracked_build_inputs": self.untracked_build_inputs,
			"authority_issues": self.authority_issues(),
		})
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeIdentity {
	pub manifest: BuildManifest,
	pub executable: ExecutableIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SealState {
	Unsealed,
	Sealed,
	Unretained,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunEvidenceManifest {
	pub build: BuildManifest,
	pub suite: String,
	pub artifact_path: PathBuf,
	pub artifact_sha256: String,
	pub candidate_authority: bool,
	pub seal_state: SealState,
	pub final_authority: bool,
	pub archived_harness: Option<PathBuf>,
	pub archived_toron: Option<PathBuf>,
	pub archived_duckdb: Option<PathBuf>,
	pub source_tree_tag: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SealOptions {
	pub repo: PathBuf,
	pub archive_root: PathBuf,
	pub remote: String,
	pub tag: String,
	pub max_archive_bytes: u64,
}

impl RunEvidenceManifest {
	pub fn unsealed(
		build: BuildManifest,
		suite: impl Into<String>,
		artifact_path: &Path,
		candidate_authority: bool,
	) -> Result<Self, String> {
		let artifact_path = artifact_path.canonicalize().map_err(|error| {
			format!("canonicalize artifact {}: {error}", artifact_path.display())
		})?;
		let artifact_sha256 = sha256_file(&artifact_path)?;
		let manifest = Self {
			build,
			suite: suite.into(),
			artifact_path,
			artifact_sha256,
			candidate_authority,
			seal_state: SealState::Unsealed,
			final_authority: false,
			archived_harness: None,
			archived_toron: None,
			archived_duckdb: None,
			source_tree_tag: None,
		};
		manifest.validate()?;
		manifest.validate_artifact_identity()?;
		Ok(manifest)
	}

	pub fn validate_artifact_identity(&self) -> Result<(), String> {
		let file = File::open(&self.artifact_path).map_err(|error| {
			format!(
				"open evidence artifact {}: {error}",
				self.artifact_path.display()
			)
		})?;
		let mut lines = std::io::BufRead::lines(std::io::BufReader::new(file));
		let header = lines
			.next()
			.ok_or_else(|| "evidence artifact is empty".to_string())?
			.map_err(|error| format!("read evidence artifact header: {error}"))?;
		let header: serde_json::Value = serde_json::from_str(&header)
			.map_err(|error| format!("decode evidence artifact header: {error}"))?;
		let expected = self.build.json_value_for_executable(&self.build.harness);
		if header.get("evidence_identity") != Some(&expected) {
			return Err("artifact header does not carry its build-manifest identity".to_string());
		}
		Ok(())
	}

	pub fn validate(&self) -> Result<(), String> {
		self.build.validate()?;
		require_sha256("artifact", &self.artifact_sha256)?;
		match self.seal_state {
			SealState::Unsealed => {
				if self.final_authority
					|| self.archived_harness.is_some()
					|| self.archived_toron.is_some()
					|| self.archived_duckdb.is_some()
					|| self.source_tree_tag.is_some()
				{
					return Err("an unsealed evidence manifest carries sealed claims".to_string());
				}
			}
			SealState::Sealed => {
				if self.archived_harness.is_none()
					|| self.archived_toron.is_none()
					|| (self.build.duckdb.is_some() != self.archived_duckdb.is_some())
					|| self.source_tree_tag.is_none()
				{
					return Err(
						"a sealed evidence manifest lacks retained executables or tree tag"
							.to_string(),
					);
				}
				if self.final_authority
					&& (!self.candidate_authority || !self.build.authority_issues().is_empty())
				{
					return Err("sealed authority contradicts the run identity gates".to_string());
				}
			}
			SealState::Unretained => {
				if self.final_authority {
					return Err("an unretained seal cannot be authoritative".to_string());
				}
			}
		}
		Ok(())
	}

	pub fn canonical_bytes(&self) -> Result<Vec<u8>, String> {
		self.validate()?;
		let mut bytes = vec![EVIDENCE_MANIFEST_VERSION];
		put_bytes(&mut bytes, &self.build.canonical_bytes()?);
		put_string(&mut bytes, &self.suite);
		put_path(&mut bytes, &self.artifact_path)?;
		put_string(&mut bytes, &self.artifact_sha256);
		bytes.push(u8::from(self.candidate_authority));
		bytes.push(match self.seal_state {
			SealState::Unsealed => 0,
			SealState::Sealed => 1,
			SealState::Unretained => 2,
		});
		bytes.push(u8::from(self.final_authority));
		put_optional_path(&mut bytes, self.archived_harness.as_deref())?;
		put_optional_path(&mut bytes, self.archived_toron.as_deref())?;
		put_optional_path(&mut bytes, self.archived_duckdb.as_deref())?;
		put_optional_string(&mut bytes, self.source_tree_tag.as_deref());
		Ok(bytes)
	}

	pub fn decode(bytes: &[u8]) -> Result<Self, String> {
		let mut cursor = Cursor::new(bytes);
		let version = cursor.byte()?;
		if !matches!(version, 1 | EVIDENCE_MANIFEST_VERSION) {
			return Err(format!("unknown evidence manifest version {version}"));
		}
		let build = BuildManifest::decode(cursor.bytes()?)?;
		let suite = cursor.string()?;
		let artifact_path = PathBuf::from(cursor.string()?);
		let artifact_sha256 = cursor.string()?;
		let candidate_authority = cursor.boolean()?;
		let seal_state = match cursor.byte()? {
			0 => SealState::Unsealed,
			1 => SealState::Sealed,
			2 => SealState::Unretained,
			other => return Err(format!("unknown evidence seal state {other}")),
		};
		let final_authority = cursor.boolean()?;
		let archived_harness = cursor.optional_string()?.map(PathBuf::from);
		let archived_toron = cursor.optional_string()?.map(PathBuf::from);
		let archived_duckdb = if version >= 2 {
			cursor.optional_string()?.map(PathBuf::from)
		} else {
			None
		};
		let source_tree_tag = cursor.optional_string()?;
		cursor.finish("evidence manifest")?;
		let manifest = Self {
			build,
			suite,
			artifact_path,
			artifact_sha256,
			candidate_authority,
			seal_state,
			final_authority,
			archived_harness,
			archived_toron,
			archived_duckdb,
			source_tree_tag,
		};
		manifest.validate()?;
		Ok(manifest)
	}

	pub fn read(path: &Path) -> Result<Self, String> {
		let bytes = std::fs::read(path)
			.map_err(|error| format!("read evidence manifest {}: {error}", path.display()))?;
		Self::decode(&bytes)
	}

	pub fn write_new(&self, path: &Path) -> Result<(), String> {
		write_new_synced(path, &self.canonical_bytes()?)
	}

	pub fn digest(&self) -> Result<String, String> {
		Ok(sha256_bytes(&self.canonical_bytes()?))
	}
}

/// Archive the exact staged bytes, anchor their source tree, and publish a new sealed manifest.
/// This is the only ADR 581 operation that mutates git.
pub fn seal_evidence_manifest(
	unsealed_path: &Path,
	sealed_path: &Path,
	options: &SealOptions,
) -> Result<RunEvidenceManifest, String> {
	let mut manifest = RunEvidenceManifest::read(unsealed_path)?;
	if manifest.seal_state != SealState::Unsealed {
		return Err("seal-evidence requires an unsealed manifest".to_string());
	}
	if sha256_file(&manifest.artifact_path)? != manifest.artifact_sha256 {
		return Err("evidence artifact changed after the timed run".to_string());
	}
	manifest.validate_artifact_identity()?;
	let retention = (|| {
		let harness_root = manifest
			.build
			.harness
			.staged_path
			.parent()
			.ok_or_else(|| "staged harness has no parent".to_string())?;
		let toron_root = manifest
			.build
			.toron
			.staged_path
			.parent()
			.ok_or_else(|| "staged Toron has no parent".to_string())?;
		verify_staged_executable(&manifest.build.harness, harness_root)?;
		if manifest.build.toron != manifest.build.harness {
			verify_staged_executable(&manifest.build.toron, toron_root)?;
		}
		if let Some(duckdb) = &manifest.build.duckdb {
			let duckdb_root = duckdb
				.staged_path
				.parent()
				.ok_or_else(|| "staged DuckDB has no parent".to_string())?;
			verify_staged_executable(duckdb, duckdb_root)?;
		}
		std::fs::create_dir_all(&options.archive_root).map_err(|error| {
			format!(
				"create evidence archive {}: {error}",
				options.archive_root.display()
			)
		})?;
		let identities = [
			Some(&manifest.build.harness),
			Some(&manifest.build.toron),
			manifest.build.duckdb.as_ref(),
		]
		.into_iter()
		.flatten()
		.collect::<Vec<_>>();
		let needed = archive_bytes_needed(&options.archive_root, identities.iter().copied())?;
		let occupied = archive_bytes(&options.archive_root)?;
		if occupied.checked_add(needed).is_none() || occupied + needed > options.max_archive_bytes {
			return Err(format!(
				"evidence archive limit {} bytes would be exceeded",
				options.max_archive_bytes
			));
		}
		let archived_harness = archive_executable(&manifest.build.harness, &options.archive_root)?;
		let archived_toron = if manifest.build.toron == manifest.build.harness {
			archived_harness.clone()
		} else {
			archive_executable(&manifest.build.toron, &options.archive_root)?
		};
		let archived_duckdb = manifest
			.build
			.duckdb
			.as_ref()
			.map(|duckdb| archive_executable(duckdb, &options.archive_root))
			.transpose()?;
		if sha256_file(&archived_harness)? != manifest.build.harness.sha256
			|| (archived_toron != archived_harness
				&& sha256_file(&archived_toron)? != manifest.build.toron.sha256)
			|| match (&archived_duckdb, &manifest.build.duckdb) {
				(Some(archive), Some(duckdb)) => sha256_file(archive)? != duckdb.sha256,
				(None, None) => false,
				_ => true,
			} {
			return Err(
				"archived executable hash does not match the staged executable".to_string(),
			);
		}
		Ok((archived_harness, archived_toron, archived_duckdb))
	})();
	let (archived_harness, archived_toron, archived_duckdb) = match retention {
		Ok(archives) => archives,
		Err(error) => return record_unretained(manifest, sealed_path, &error),
	};
	manifest.seal_state = SealState::Sealed;
	manifest.final_authority =
		manifest.candidate_authority && manifest.build.authority_issues().is_empty();
	manifest.archived_harness = Some(archived_harness);
	manifest.archived_toron = Some(archived_toron);
	manifest.archived_duckdb = archived_duckdb;
	manifest.source_tree_tag = Some(options.tag.clone());
	manifest.validate()?;
	let sealed_manifest_digest = manifest.digest()?;
	ensure_tree_tag(
		&options.repo,
		&options.remote,
		&options.tag,
		&manifest.build.source_tree_id,
		&sealed_manifest_digest,
	)?;
	manifest.write_new(sealed_path)?;
	Ok(manifest)
}

fn record_unretained(
	mut manifest: RunEvidenceManifest,
	sealed_path: &Path,
	reason: &str,
) -> Result<RunEvidenceManifest, String> {
	manifest.seal_state = SealState::Unretained;
	manifest.final_authority = false;
	let unretained = PathBuf::from(format!("{}.unretained", sealed_path.display()));
	manifest.write_new(&unretained)?;
	Err(format!(
		"{reason}; unretained seal written to {}",
		unretained.display()
	))
}

fn archive_executable(
	identity: &ExecutableIdentity,
	archive_root: &Path,
) -> Result<PathBuf, String> {
	let destination = archive_root.join(&identity.sha256);
	if destination.exists() {
		if sha256_file(&destination)? != identity.sha256 {
			return Err(format!(
				"evidence archive collision at {}",
				destination.display()
			));
		}
	} else {
		let temporary = archive_root.join(format!(
			".{}.{}.partial",
			identity.sha256,
			std::process::id()
		));
		std::fs::copy(&identity.staged_path, &temporary).map_err(|error| {
			format!(
				"archive staged executable {}: {error}",
				identity.staged_path.display()
			)
		})?;
		make_read_only(&temporary)?;
		std::fs::rename(&temporary, &destination).map_err(|error| {
			format!(
				"publish archived executable {}: {error}",
				destination.display()
			)
		})?;
	}
	make_read_only(&destination)?;
	destination
		.canonicalize()
		.map_err(|error| format!("canonicalize archive {}: {error}", destination.display()))
}

fn archive_bytes_needed<'a>(
	archive_root: &Path,
	identities: impl IntoIterator<Item = &'a ExecutableIdentity>,
) -> Result<u64, String> {
	let mut unique = std::collections::BTreeMap::new();
	for identity in identities {
		unique.entry(&identity.sha256).or_insert(identity);
	}
	let mut bytes = 0u64;
	for identity in unique.values() {
		if !archive_root.join(&identity.sha256).exists() {
			bytes = bytes
				.checked_add(
					std::fs::metadata(&identity.staged_path)
						.map_err(|error| format!("stat staged executable: {error}"))?
						.len(),
				)
				.ok_or_else(|| "evidence archive byte count overflow".to_string())?;
		}
	}
	Ok(bytes)
}

fn archive_bytes(root: &Path) -> Result<u64, String> {
	let mut bytes = 0u64;
	for entry in std::fs::read_dir(root)
		.map_err(|error| format!("read evidence archive {}: {error}", root.display()))?
	{
		let entry = entry.map_err(|error| format!("read evidence archive entry: {error}"))?;
		if entry
			.file_type()
			.map_err(|error| format!("read evidence archive file type: {error}"))?
			.is_file()
		{
			bytes = bytes
				.checked_add(
					entry
						.metadata()
						.map_err(|error| format!("stat evidence archive entry: {error}"))?
						.len(),
				)
				.ok_or_else(|| "evidence archive byte count overflow".to_string())?;
		}
	}
	Ok(bytes)
}

fn ensure_tree_tag(
	repo: &Path,
	remote: &str,
	tag: &str,
	tree_id: &str,
	manifest_digest: &str,
) -> Result<(), String> {
	let object_type = git_output(repo, &["cat-file", "-t", tree_id])?;
	if object_type.trim() != "tree" {
		return Err(format!(
			"evidence source object {tree_id} is not a git tree"
		));
	}
	let reference = format!("refs/tags/{tag}");
	let existing = std::process::Command::new("git")
		.current_dir(repo)
		.args(["rev-parse", "--verify", "--quiet", &reference])
		.output()
		.map_err(|error| format!("inspect evidence tag {tag}: {error}"))?;
	if existing.status.success() {
		let target = git_output(repo, &["rev-parse", &format!("{reference}^{{tree}}")])?;
		if target.trim() != tree_id {
			return Err(format!("evidence tag {tag} already names a different tree"));
		}
	} else {
		git_output(
			repo,
			&[
				"tag",
				"-a",
				tag,
				tree_id,
				"-m",
				&format!("sealed evidence manifest sha256:{manifest_digest}"),
			],
		)?;
	}
	git_output(repo, &["push", remote, &reference])?;
	Ok(())
}

fn git_output(repo: &Path, args: &[&str]) -> Result<String, String> {
	let output = std::process::Command::new("git")
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

pub fn evidence_manifest_path(artifact_path: &Path) -> PathBuf {
	PathBuf::from(format!("{}.evidence", artifact_path.display()))
}

pub fn write_runtime_evidence_manifest(
	artifact_path: &Path,
	suite: &str,
	candidate_authority: bool,
) -> Result<Option<PathBuf>, String> {
	let Some(identity) = runtime_identity()? else {
		return Ok(None);
	};
	let manifest = RunEvidenceManifest::unsealed(
		identity.manifest,
		suite,
		artifact_path,
		candidate_authority,
	)?;
	let path = evidence_manifest_path(artifact_path);
	manifest.write_new(&path)?;
	Ok(Some(path))
}

impl RuntimeIdentity {
	pub fn load_from(manifest_path: &Path, current_executable: &Path) -> Result<Self, String> {
		let manifest = BuildManifest::read(manifest_path)?;
		let executable = manifest
			.executable_for_path(current_executable)
			.cloned()
			.ok_or_else(|| {
				format!(
					"running executable {} is not one of the staged manifest paths",
					current_executable.display()
				)
			})?;
		Ok(Self {
			manifest,
			executable,
		})
	}

	pub fn json_value(&self) -> serde_json::Value {
		self.manifest.json_value_for_executable(&self.executable)
	}
}

pub fn runtime_identity() -> Result<Option<RuntimeIdentity>, String> {
	let Some(path) = std::env::var_os(BUILD_MANIFEST_ENV) else {
		return Ok(None);
	};
	let current =
		std::env::current_exe().map_err(|error| format!("current executable: {error}"))?;
	RuntimeIdentity::load_from(Path::new(&path), &current).map(Some)
}

pub fn runtime_identity_json() -> serde_json::Value {
	match runtime_identity() {
		Ok(Some(identity)) => identity.json_value(),
		Ok(None) => serde_json::Value::Null,
		Err(error) => serde_json::json!({ "invalid": error }),
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IdentityGateDecision {
	pub would_admit: bool,
	pub reasons: Vec<String>,
}

/// Evaluate the content identity supplied to this process. Cut 1 reported this verdict in shadow;
/// after cut 2 it is the build-identity input to candidate authority.
pub fn runtime_identity_gate() -> IdentityGateDecision {
	match runtime_identity() {
		Ok(Some(identity)) => {
			let reasons = identity.manifest.authority_issues();
			IdentityGateDecision {
				would_admit: reasons.is_empty(),
				reasons,
			}
		}
		Ok(None) => IdentityGateDecision {
			would_admit: false,
			reasons: vec!["content_identity_manifest_absent".to_string()],
		},
		Err(error) => IdentityGateDecision {
			would_admit: false,
			reasons: vec![format!("content_identity_invalid:{error}")],
		},
	}
}

/// Compare identities already carried by two receipt rows. The gate never hashes a file or reads
/// git: receipt construction is the only place identity enters the timed process.
pub fn compare_row_identities(
	candidate: &serde_json::Value,
	control: &serde_json::Value,
) -> IdentityGateDecision {
	let mut reasons = Vec::new();
	let candidate = candidate.get("evidence_identity");
	let control = control.get("evidence_identity");
	let (Some(candidate), Some(control)) = (candidate, control) else {
		return IdentityGateDecision {
			would_admit: false,
			reasons: vec!["content_identity_missing_from_arm".to_string()],
		};
	};
	for field in [
		"executable_sha256",
		"recipe_sha256",
		"source_tree_id",
		"harness_sha256",
		"toron_sha256",
	] {
		match (candidate.get(field), control.get(field)) {
			(Some(left), Some(right)) if left == right && left.is_string() => {}
			(Some(_), Some(_)) => reasons.push(format!("content_identity_arms_disagree:{field}")),
			_ => reasons.push(format!("content_identity_field_missing:{field}")),
		}
	}
	for (arm, identity) in [("candidate", candidate), ("control", control)] {
		match identity
			.get("authority_issues")
			.and_then(|value| value.as_array())
		{
			Some(issues) if issues.is_empty() => {}
			Some(issues) => {
				for issue in issues {
					reasons.push(format!(
						"content_identity_{arm}:{}",
						issue.as_str().unwrap_or("malformed_authority_issue")
					));
				}
			}
			None => reasons.push(format!("content_identity_{arm}:authority_issues_missing")),
		}
	}
	IdentityGateDecision {
		would_admit: reasons.is_empty(),
		reasons,
	}
}

pub fn runtime_commit_hint() -> String {
	runtime_identity()
		.ok()
		.flatten()
		.map(|identity| identity.manifest.commit_hint)
		.unwrap_or_else(|| "unknown".to_string())
}

pub fn sha256_bytes(bytes: &[u8]) -> String {
	let digest = Sha256::digest(bytes);
	format!("{digest:x}")
}

pub fn sha256_file(path: &Path) -> Result<String, String> {
	let mut file = File::open(path).map_err(|error| format!("open {}: {error}", path.display()))?;
	let mut digest = Sha256::new();
	let mut buffer = [0u8; 128 * 1024];
	loop {
		let read = file
			.read(&mut buffer)
			.map_err(|error| format!("read {}: {error}", path.display()))?;
		if read == 0 {
			break;
		}
		digest.update(&buffer[..read]);
	}
	Ok(format!("{:x}", digest.finalize()))
}

pub fn stage_executable(source: &Path, staging_root: &Path) -> Result<ExecutableIdentity, String> {
	let sha256 = sha256_file(source)?;
	std::fs::create_dir_all(staging_root)
		.map_err(|error| format!("create staging root {}: {error}", staging_root.display()))?;
	let staged_path = staging_root.join(&sha256);
	if staged_path.exists() {
		if sha256_file(&staged_path)? != sha256 {
			return Err(format!(
				"content-addressed staging collision at {}",
				staged_path.display()
			));
		}
	} else {
		let temporary = staging_root.join(format!(".{sha256}.{}.partial", std::process::id()));
		std::fs::copy(source, &temporary).map_err(|error| {
			format!(
				"stage executable {} to {}: {error}",
				source.display(),
				temporary.display()
			)
		})?;
		make_read_only(&temporary)?;
		std::fs::rename(&temporary, &staged_path).map_err(|error| {
			format!(
				"publish staged executable {}: {error}",
				staged_path.display()
			)
		})?;
	}
	make_read_only(&staged_path)?;
	let staged_path = staged_path
		.canonicalize()
		.map_err(|error| format!("canonicalize {}: {error}", staged_path.display()))?;
	Ok(ExecutableIdentity {
		sha256,
		staged_path,
	})
}

pub fn verify_staged_executable(
	identity: &ExecutableIdentity,
	staging_root: &Path,
) -> Result<(), String> {
	let staged = identity
		.staged_path
		.canonicalize()
		.map_err(|error| format!("canonicalize {}: {error}", identity.staged_path.display()))?;
	let root = staging_root
		.canonicalize()
		.map_err(|error| format!("canonicalize {}: {error}", staging_root.display()))?;
	if !staged.starts_with(&root) {
		return Err(format!(
			"staged executable {} is outside staging root {}",
			staged.display(),
			root.display()
		));
	}
	if staged.file_name().and_then(|name| name.to_str()) != Some(&identity.sha256) {
		return Err("staged executable filename is not its SHA-256".to_string());
	}
	if sha256_file(&staged)? != identity.sha256 {
		return Err("staged executable bytes do not match its SHA-256".to_string());
	}
	if !std::fs::metadata(&staged)
		.map_err(|error| format!("stat {}: {error}", staged.display()))?
		.permissions()
		.readonly()
	{
		return Err("staged executable is writable".to_string());
	}
	Ok(())
}

fn make_read_only(path: &Path) -> Result<(), String> {
	let mut permissions = std::fs::metadata(path)
		.map_err(|error| format!("stat {}: {error}", path.display()))?
		.permissions();
	permissions.set_readonly(true);
	std::fs::set_permissions(path, permissions)
		.map_err(|error| format!("make {} read-only: {error}", path.display()))
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<(), String> {
	if let Some(parent) = path.parent() {
		std::fs::create_dir_all(parent)
			.map_err(|error| format!("create {}: {error}", parent.display()))?;
	}
	let mut file = OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(path)
		.map_err(|error| format!("create {}: {error}", path.display()))?;
	file.write_all(bytes)
		.map_err(|error| format!("write {}: {error}", path.display()))?;
	file.sync_all()
		.map_err(|error| format!("sync {}: {error}", path.display()))
}

fn require_sha256(label: &str, value: &str) -> Result<(), String> {
	if value.len() == SHA256_HEX_LEN && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
		Ok(())
	} else {
		Err(format!("{label} digest is not a SHA-256"))
	}
}

fn is_sorted_unique(values: &[String]) -> bool {
	values.windows(2).all(|pair| pair[0] < pair[1])
}

fn put_u32(bytes: &mut Vec<u8>, value: usize) {
	bytes.extend_from_slice(&(value as u32).to_be_bytes());
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
	put_u32(bytes, value.len());
	bytes.extend_from_slice(value);
}

fn put_string(bytes: &mut Vec<u8>, value: &str) {
	put_bytes(bytes, value.as_bytes());
}

fn put_path(bytes: &mut Vec<u8>, path: &Path) -> Result<(), String> {
	let value = path
		.to_str()
		.ok_or_else(|| format!("evidence path is not UTF-8: {}", path.display()))?;
	put_string(bytes, value);
	Ok(())
}

fn put_strings(bytes: &mut Vec<u8>, values: &[String]) {
	put_u32(bytes, values.len());
	for value in values {
		put_string(bytes, value);
	}
}

fn put_optional_string(bytes: &mut Vec<u8>, value: Option<&str>) {
	match value {
		Some(value) => {
			bytes.push(1);
			put_string(bytes, value);
		}
		None => bytes.push(0),
	}
}

fn put_optional_path(bytes: &mut Vec<u8>, value: Option<&Path>) -> Result<(), String> {
	match value {
		Some(value) => {
			bytes.push(1);
			put_path(bytes, value)?;
		}
		None => bytes.push(0),
	}
	Ok(())
}

fn put_optional_executable(
	bytes: &mut Vec<u8>,
	value: Option<&ExecutableIdentity>,
) -> Result<(), String> {
	match value {
		Some(value) => {
			bytes.push(1);
			value.encode(bytes)?;
		}
		None => bytes.push(0),
	}
	Ok(())
}

fn decode_optional_executable(
	cursor: &mut Cursor<'_>,
) -> Result<Option<ExecutableIdentity>, String> {
	match cursor.byte()? {
		0 => Ok(None),
		1 => ExecutableIdentity::decode(cursor).map(Some),
		other => Err(format!("invalid optional executable tag {other}")),
	}
}

struct Cursor<'a> {
	bytes: &'a [u8],
	position: usize,
}

impl<'a> Cursor<'a> {
	fn new(bytes: &'a [u8]) -> Self {
		Self { bytes, position: 0 }
	}

	fn byte(&mut self) -> Result<u8, String> {
		let byte = *self
			.bytes
			.get(self.position)
			.ok_or_else(|| "truncated evidence schema".to_string())?;
		self.position += 1;
		Ok(byte)
	}

	fn bytes(&mut self) -> Result<&'a [u8], String> {
		let end = self
			.position
			.checked_add(4)
			.ok_or_else(|| "evidence length overflow".to_string())?;
		let length_bytes: [u8; 4] = self
			.bytes
			.get(self.position..end)
			.ok_or_else(|| "truncated evidence length".to_string())?
			.try_into()
			.expect("four-byte slice");
		self.position = end;
		let length = u32::from_be_bytes(length_bytes) as usize;
		let end = self
			.position
			.checked_add(length)
			.ok_or_else(|| "evidence field length overflow".to_string())?;
		let value = self
			.bytes
			.get(self.position..end)
			.ok_or_else(|| "truncated evidence field".to_string())?;
		self.position = end;
		Ok(value)
	}

	fn string(&mut self) -> Result<String, String> {
		String::from_utf8(self.bytes()?.to_vec())
			.map_err(|_| "evidence field is not valid UTF-8".to_string())
	}

	fn strings(&mut self) -> Result<Vec<String>, String> {
		let count = self.u32()? as usize;
		if count > 65_536 {
			return Err("evidence list exceeds schema bound".to_string());
		}
		(0..count).map(|_| self.string()).collect()
	}

	fn u32(&mut self) -> Result<u32, String> {
		let end = self
			.position
			.checked_add(4)
			.ok_or_else(|| "evidence length overflow".to_string())?;
		let bytes: [u8; 4] = self
			.bytes
			.get(self.position..end)
			.ok_or_else(|| "truncated evidence count".to_string())?
			.try_into()
			.expect("four-byte slice");
		self.position = end;
		Ok(u32::from_be_bytes(bytes))
	}

	fn boolean(&mut self) -> Result<bool, String> {
		match self.byte()? {
			0 => Ok(false),
			1 => Ok(true),
			other => Err(format!("invalid evidence boolean {other}")),
		}
	}

	fn optional_string(&mut self) -> Result<Option<String>, String> {
		match self.byte()? {
			0 => Ok(None),
			1 => self.string().map(Some),
			other => Err(format!("invalid evidence option tag {other}")),
		}
	}

	fn finish(&self, label: &str) -> Result<(), String> {
		if self.position == self.bytes.len() {
			Ok(())
		} else {
			Err(format!("{label} has trailing bytes"))
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::process::Command;

	fn hash(byte: u8) -> String {
		format!("{byte:02x}").repeat(32)
	}

	fn fixture() -> BuildManifest {
		BuildManifest::new(
			"1".repeat(40),
			"0123456789ab",
			BuildRecipe::new(
				"release",
				vec!["z".into(), "a".into(), "a".into()],
				"x86_64-unknown-linux-gnu",
				"rustc 1.0\nhost: x86_64-unknown-linux-gnu",
				hash(2),
				hash(3),
			)
			.unwrap(),
			ExecutableIdentity {
				sha256: hash(4),
				staged_path: PathBuf::from("/stage/harness"),
			},
			ExecutableIdentity {
				sha256: hash(5),
				staged_path: PathBuf::from("/stage/toron"),
			},
			None,
			vec!["z.rs".into(), "a.rs".into(), "a.rs".into()],
		)
		.unwrap()
	}

	fn v1_build_bytes(build: &BuildManifest) -> Vec<u8> {
		assert!(build.duckdb.is_none());
		let mut bytes = vec![1];
		put_string(&mut bytes, &build.source_tree_id);
		put_string(&mut bytes, &build.commit_hint);
		put_bytes(&mut bytes, &build.recipe.canonical_bytes());
		put_string(&mut bytes, &build.recipe_digest);
		build.harness.encode(&mut bytes).unwrap();
		build.toron.encode(&mut bytes).unwrap();
		put_strings(&mut bytes, &build.untracked_build_inputs);
		bytes
	}

	fn v1_run_bytes(run: &RunEvidenceManifest) -> Vec<u8> {
		assert!(run.archived_duckdb.is_none());
		let mut bytes = vec![1];
		put_bytes(&mut bytes, &v1_build_bytes(&run.build));
		put_string(&mut bytes, &run.suite);
		put_path(&mut bytes, &run.artifact_path).unwrap();
		put_string(&mut bytes, &run.artifact_sha256);
		bytes.push(u8::from(run.candidate_authority));
		bytes.push(match run.seal_state {
			SealState::Unsealed => 0,
			SealState::Sealed => 1,
			SealState::Unretained => 2,
		});
		bytes.push(u8::from(run.final_authority));
		put_optional_path(&mut bytes, run.archived_harness.as_deref()).unwrap();
		put_optional_path(&mut bytes, run.archived_toron.as_deref()).unwrap();
		put_optional_string(&mut bytes, run.source_tree_tag.as_deref());
		bytes
	}

	fn git(repo: &Path, args: &[&str]) -> String {
		let output = Command::new("git")
			.current_dir(repo)
			.args(args)
			.env("GIT_AUTHOR_NAME", "ADR 581")
			.env("GIT_AUTHOR_EMAIL", "adr581@example.invalid")
			.env("GIT_COMMITTER_NAME", "ADR 581")
			.env("GIT_COMMITTER_EMAIL", "adr581@example.invalid")
			.output()
			.unwrap();
		assert!(
			output.status.success(),
			"git {args:?}: {}",
			String::from_utf8_lossy(&output.stderr)
		);
		String::from_utf8(output.stdout).unwrap().trim().to_string()
	}

	fn seal_fixture(root: &Path) -> (PathBuf, PathBuf, SealOptions, String) {
		let repo = root.join("repo");
		let remote = root.join("remote.git");
		std::fs::create_dir_all(&repo).unwrap();
		git(&repo, &["init", "-q"]);
		std::fs::write(repo.join("source.rs"), "fn source() {}\n").unwrap();
		git(&repo, &["add", "source.rs"]);
		git(&repo, &["commit", "-qm", "source"]);
		git(root, &["init", "--bare", "-q", remote.to_str().unwrap()]);
		git(
			&repo,
			&["remote", "add", "origin", remote.to_str().unwrap()],
		);
		let tree = git(&repo, &["rev-parse", "HEAD^{tree}"]);

		let binary = root.join("binary");
		std::fs::write(&binary, b"exact executable bytes").unwrap();
		let staged = stage_executable(&binary, &root.join("staged")).unwrap();
		let duckdb_binary = root.join("duckdb");
		std::fs::write(&duckdb_binary, b"exact DuckDB executable bytes").unwrap();
		let staged_duckdb = stage_executable(&duckdb_binary, &root.join("staged")).unwrap();
		let recipe =
			BuildRecipe::new("release", Vec::new(), "target", "rustc", hash(1), hash(2)).unwrap();
		let build = BuildManifest::new(
			tree.clone(),
			git(&repo, &["rev-parse", "HEAD"]),
			recipe,
			staged.clone(),
			staged,
			Some(staged_duckdb),
			Vec::new(),
		)
		.unwrap();
		let artifact = root.join("run.jsonl");
		let header = serde_json::json!({
			"evidence_identity": build.json_value_for_executable(&build.harness),
		});
		std::fs::write(&artifact, format!("{header}\n")).unwrap();
		let unsealed = RunEvidenceManifest::unsealed(build, "suite", &artifact, true).unwrap();
		let unsealed_path = root.join("run.evidence");
		unsealed.write_new(&unsealed_path).unwrap();
		let sealed_path = root.join("run.evidence.sealed");
		let options = SealOptions {
			repo,
			archive_root: root.join("archive"),
			remote: "origin".to_string(),
			tag: "evidence/test".to_string(),
			max_archive_bytes: 1024,
		};
		(unsealed_path, sealed_path, options, tree)
	}

	#[test]
	fn recipe_and_manifest_are_canonical_and_round_trip() {
		let manifest = fixture();
		assert_eq!(manifest.recipe.features, ["a", "z"]);
		assert_eq!(manifest.untracked_build_inputs, ["a.rs", "z.rs"]);
		let bytes = manifest.canonical_bytes().unwrap();
		assert_eq!(BuildManifest::decode(&bytes).unwrap(), manifest);
	}

	#[test]
	fn v1_schema_bytes_still_decode_and_are_pinned_independently_of_v2() {
		let build = fixture();
		assert_eq!(
			sha256_bytes(&build.recipe.canonical_bytes()),
			"c6d22f3f89d05c2fffa55531ba16354dc6e66e80ee41dd81122e256c7c38f306"
		);
		assert_eq!(
			sha256_bytes(&v1_build_bytes(&build)),
			"98fd75574e02d9ccfd72fe84b84481a4be64009b54842ef0d079a63ffb81f70a"
		);
		assert_eq!(
			BuildManifest::decode(&v1_build_bytes(&build)).unwrap(),
			build
		);
		let run = RunEvidenceManifest {
			build,
			suite: "suite".to_string(),
			artifact_path: PathBuf::from("/artifact"),
			artifact_sha256: hash(6),
			candidate_authority: false,
			seal_state: SealState::Unsealed,
			final_authority: false,
			archived_harness: None,
			archived_toron: None,
			archived_duckdb: None,
			source_tree_tag: None,
		};
		assert_eq!(
			sha256_bytes(&v1_run_bytes(&run)),
			"bb6e5d1c63ec160e8c09282edfa5b01b4408bc5ee103fce707c11a736c56695d",
			"a schema change requires a version bump and migration, not a fixture edit"
		);
		assert_eq!(
			RunEvidenceManifest::decode(&v1_run_bytes(&run)).unwrap(),
			run
		);
	}

	#[test]
	fn unknown_recipe_or_manifest_version_is_refused() {
		let manifest = fixture();
		let mut bytes = manifest.canonical_bytes().unwrap();
		bytes[0] = BUILD_MANIFEST_VERSION + 1;
		assert!(
			BuildManifest::decode(&bytes)
				.unwrap_err()
				.contains("unknown build")
		);

		let mut bytes = manifest.canonical_bytes().unwrap();
		let recipe_version_offset = 1 + 4 + 40 + 4 + manifest.commit_hint.len() + 4;
		bytes[recipe_version_offset] = RECIPE_VERSION + 1;
		assert!(
			BuildManifest::decode(&bytes)
				.unwrap_err()
				.contains("unknown evidence recipe")
		);
	}

	#[test]
	fn feature_order_does_not_change_recipe_but_a_feature_does() {
		let base = BuildRecipe::new(
			"release",
			vec!["b".into(), "a".into()],
			"host",
			"rustc",
			hash(1),
			hash(2),
		)
		.unwrap();
		let reordered = BuildRecipe::new(
			"release",
			vec!["a".into(), "b".into()],
			"host",
			"rustc",
			hash(1),
			hash(2),
		)
		.unwrap();
		let changed = BuildRecipe::new(
			"release",
			vec!["a".into()],
			"host",
			"rustc",
			hash(1),
			hash(2),
		)
		.unwrap();
		assert_eq!(base.digest(), reordered.digest());
		assert_ne!(base.digest(), changed.digest());
	}

	#[test]
	fn decoder_refuses_a_noncanonical_feature_order_instead_of_normalizing_it() {
		let mut bytes = vec![RECIPE_VERSION];
		put_string(&mut bytes, "release");
		put_strings(&mut bytes, &["z".to_string(), "a".to_string()]);
		put_string(&mut bytes, "target");
		put_string(&mut bytes, "rustc");
		put_string(&mut bytes, &hash(1));
		put_string(&mut bytes, &hash(2));
		let mut cursor = Cursor::new(&bytes);
		assert!(
			BuildRecipe::decode(&mut cursor)
				.unwrap_err()
				.contains("canonically sorted")
		);
	}

	#[test]
	fn row_identity_gate_distinguishes_recipe_and_untracked_input() {
		let manifest = fixture();
		let row = serde_json::json!({
			"evidence_identity": manifest.json_value_for_executable(&manifest.harness),
		});
		let decision = compare_row_identities(&row, &row);
		assert!(
			!decision.would_admit,
			"the fixture names untracked build inputs"
		);
		assert!(
			decision
				.reasons
				.iter()
				.any(|reason| reason.contains("a.rs"))
		);

		let mut clean = manifest.clone();
		clean.untracked_build_inputs.clear();
		let clean_row = serde_json::json!({
			"evidence_identity": clean.json_value_for_executable(&clean.harness),
		});
		let mut different_recipe = clean_row.clone();
		different_recipe["evidence_identity"]["recipe_sha256"] = serde_json::json!(hash(9));
		let decision = compare_row_identities(&clean_row, &different_recipe);
		assert!(!decision.would_admit);
		assert!(
			decision
				.reasons
				.iter()
				.any(|reason| reason.ends_with("recipe_sha256"))
		);

		let mut navigated = clean_row.clone();
		navigated["evidence_identity"]["commit_hint"] =
			serde_json::json!("different-dirty-navigation-hint");
		let decision = compare_row_identities(&clean_row, &navigated);
		assert!(
			decision.would_admit,
			"commit hints are not authority inputs"
		);
		assert!(
			decision
				.reasons
				.iter()
				.all(|reason| !reason.contains("dirty")),
			"dirty is not an ADR 581 gate reason"
		);
	}

	#[test]
	fn staged_executable_is_named_by_its_hash_and_read_only() {
		let root =
			std::env::temp_dir().join(format!("toron-evidence-stage-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&root);
		std::fs::create_dir_all(&root).unwrap();
		let source = root.join("source");
		std::fs::write(&source, b"binary bytes").unwrap();
		let stage = root.join("staged");
		let identity = stage_executable(&source, &stage).unwrap();
		assert_eq!(
			identity.staged_path.file_name().unwrap(),
			identity.sha256.as_str()
		);
		assert!(
			std::fs::metadata(&identity.staged_path)
				.unwrap()
				.permissions()
				.readonly()
		);
		verify_staged_executable(&identity, &stage).unwrap();
		assert!(
			OpenOptions::new()
				.write(true)
				.open(&identity.staged_path)
				.is_err()
		);
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn unsealed_manifest_round_trips_and_unknown_version_is_refused() {
		let root =
			std::env::temp_dir().join(format!("toron-evidence-artifact-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&root);
		std::fs::create_dir_all(&root).unwrap();
		let artifact = root.join("run.jsonl");
		let build = fixture();
		let header = serde_json::json!({
			"evidence_identity": build.json_value_for_executable(&build.harness),
		});
		std::fs::write(&artifact, format!("{header}\n")).unwrap();
		let manifest = RunEvidenceManifest::unsealed(build, "suite", &artifact, false).unwrap();
		let bytes = manifest.canonical_bytes().unwrap();
		assert_eq!(RunEvidenceManifest::decode(&bytes).unwrap(), manifest);
		let mut future = bytes;
		future[0] = EVIDENCE_MANIFEST_VERSION + 1;
		assert!(
			RunEvidenceManifest::decode(&future)
				.unwrap_err()
				.contains("unknown evidence manifest")
		);
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn seal_archives_the_staged_bytes_and_tags_the_tree() {
		let root = std::env::temp_dir().join(format!("toron-evidence-seal-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&root);
		std::fs::create_dir_all(&root).unwrap();
		let (unsealed, sealed_path, options, tree) = seal_fixture(&root);
		let sealed = seal_evidence_manifest(&unsealed, &sealed_path, &options).unwrap();
		assert_eq!(sealed.seal_state, SealState::Sealed);
		assert!(sealed.final_authority);
		for (archive, expected) in [
			(
				sealed.archived_harness.as_ref().unwrap(),
				&sealed.build.harness.sha256,
			),
			(
				sealed.archived_toron.as_ref().unwrap(),
				&sealed.build.toron.sha256,
			),
			(
				sealed.archived_duckdb.as_ref().unwrap(),
				&sealed.build.duckdb.as_ref().unwrap().sha256,
			),
		] {
			assert_eq!(sha256_file(archive).unwrap(), *expected);
			assert!(OpenOptions::new().write(true).open(archive).is_err());
		}
		assert_eq!(RunEvidenceManifest::read(&sealed_path).unwrap(), sealed);
		let remote_tree = git(
			&root,
			&[
				"--git-dir",
				options
					.repo
					.parent()
					.unwrap()
					.join("remote.git")
					.to_str()
					.unwrap(),
				"rev-parse",
				"refs/tags/evidence/test^{tree}",
			],
		);
		assert_eq!(remote_tree, tree);
		let remote_tag = git(
			&root,
			&[
				"--git-dir",
				options
					.repo
					.parent()
					.unwrap()
					.join("remote.git")
					.to_str()
					.unwrap(),
				"cat-file",
				"-p",
				"refs/tags/evidence/test",
			],
		);
		assert!(
			remote_tag.contains(&format!("sha256:{}", sealed.digest().unwrap())),
			"the tag names the sealed manifest, not its unsealed predecessor"
		);
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn seal_without_archive_capacity_is_recorded_as_unretained() {
		let root =
			std::env::temp_dir().join(format!("toron-evidence-unretained-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&root);
		std::fs::create_dir_all(&root).unwrap();
		let (unsealed, sealed_path, mut options, _) = seal_fixture(&root);
		options.max_archive_bytes = 0;
		let error = seal_evidence_manifest(&unsealed, &sealed_path, &options).unwrap_err();
		assert!(error.contains("unretained"));
		let unretained = RunEvidenceManifest::read(&PathBuf::from(format!(
			"{}.unretained",
			sealed_path.display()
		)))
		.unwrap();
		assert_eq!(unretained.seal_state, SealState::Unretained);
		assert!(!unretained.final_authority);
		assert!(!sealed_path.exists());
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn seal_rehashes_the_artifact_instead_of_trusting_the_manifest() {
		let root = std::env::temp_dir().join(format!(
			"toron-evidence-mutated-artifact-{}",
			std::process::id()
		));
		let _ = std::fs::remove_dir_all(&root);
		std::fs::create_dir_all(&root).unwrap();
		let (unsealed, sealed_path, options, _) = seal_fixture(&root);
		let manifest = RunEvidenceManifest::read(&unsealed).unwrap();
		std::fs::OpenOptions::new()
			.append(true)
			.open(&manifest.artifact_path)
			.unwrap()
			.write_all(b"{}\n")
			.unwrap();
		let error = seal_evidence_manifest(&unsealed, &sealed_path, &options).unwrap_err();
		assert!(error.contains("changed after the timed run"));
		assert!(!sealed_path.exists());
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn a_missing_staged_binary_produces_an_unretained_record() {
		let root = std::env::temp_dir().join(format!(
			"toron-evidence-missing-binary-{}",
			std::process::id()
		));
		let _ = std::fs::remove_dir_all(&root);
		std::fs::create_dir_all(&root).unwrap();
		let (unsealed, sealed_path, options, _) = seal_fixture(&root);
		let staged = RunEvidenceManifest::read(&unsealed)
			.unwrap()
			.build
			.harness
			.staged_path;
		std::fs::remove_file(staged).unwrap();
		let error = seal_evidence_manifest(&unsealed, &sealed_path, &options).unwrap_err();
		assert!(error.contains("unretained seal written"));
		let unretained_path = PathBuf::from(format!("{}.unretained", sealed_path.display()));
		assert_eq!(
			RunEvidenceManifest::read(&unretained_path)
				.unwrap()
				.seal_state,
			SealState::Unretained
		);
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn authority_gates_have_no_compile_stamp_or_dirty_escape_hatch() {
		assert!(
			!Path::new(env!("CARGO_MANIFEST_DIR"))
				.join("build.rs")
				.exists(),
			"ADR 581 retires compile-time git inference"
		);
		let main = include_str!("main.rs");
		for (start, end) in [
			(
				"fn shape_preserving_spill_index_gate_record",
				"fn run_route_distinct_gate",
			),
			("fn route_distinct_gate_record", "fn paired_ablation_arm"),
		] {
			let gate = main
				.split(start)
				.nth(1)
				.and_then(|tail| tail.split(end).next())
				.expect("authority gate source");
			assert!(gate.contains("compare_row_identities"));
			assert!(!gate.contains("build_commit"));
			assert!(!gate.contains("dirty"));
		}
	}
}
