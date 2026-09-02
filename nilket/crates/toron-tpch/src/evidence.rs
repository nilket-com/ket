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
pub const BUILD_MANIFEST_VERSION: u8 = 1;
pub const RECIPE_VERSION: u8 = 1;
pub const EVIDENCE_MANIFEST_VERSION: u8 = 1;
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
		Self::new(
			cursor.string()?,
			cursor.strings()?,
			cursor.string()?,
			cursor.string()?,
			cursor.string()?,
			cursor.string()?,
		)
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
	pub untracked_build_inputs: Vec<String>,
}

impl BuildManifest {
	pub fn new(
		source_tree_id: impl Into<String>,
		commit_hint: impl Into<String>,
		recipe: BuildRecipe,
		harness: ExecutableIdentity,
		toron: ExecutableIdentity,
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
		put_strings(&mut bytes, &self.untracked_build_inputs);
		Ok(bytes)
	}

	pub fn decode(bytes: &[u8]) -> Result<Self, String> {
		let mut cursor = Cursor::new(bytes);
		let version = cursor.byte()?;
		if version != BUILD_MANIFEST_VERSION {
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
		let untracked_build_inputs = cursor.strings()?;
		cursor.finish("build manifest")?;
		let manifest = Self {
			source_tree_id,
			commit_hint,
			recipe,
			recipe_digest,
			harness,
			toron,
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
		[&self.harness, &self.toron]
			.into_iter()
			.find(|identity| identity.staged_path.canonicalize().ok().as_ref() == Some(&canonical))
	}

	pub fn json_value_for_executable(&self, executable: &ExecutableIdentity) -> serde_json::Value {
		serde_json::json!({
			"manifest_version": BUILD_MANIFEST_VERSION,
			"recipe_version": RECIPE_VERSION,
			"executable_sha256": executable.sha256,
			"harness_sha256": self.harness.sha256,
			"toron_sha256": self.toron.sha256,
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
	pub source_tree_tag: Option<String>,
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
			source_tree_tag: None,
		};
		manifest.validate()?;
		Ok(manifest)
	}

	pub fn validate(&self) -> Result<(), String> {
		self.build.validate()?;
		require_sha256("artifact", &self.artifact_sha256)?;
		match self.seal_state {
			SealState::Unsealed => {
				if self.final_authority
					|| self.archived_harness.is_some()
					|| self.archived_toron.is_some()
					|| self.source_tree_tag.is_some()
				{
					return Err("an unsealed evidence manifest carries sealed claims".to_string());
				}
			}
			SealState::Sealed => {
				if self.archived_harness.is_none()
					|| self.archived_toron.is_none()
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
		put_optional_string(&mut bytes, self.source_tree_tag.as_deref());
		Ok(bytes)
	}

	pub fn decode(bytes: &[u8]) -> Result<Self, String> {
		let mut cursor = Cursor::new(bytes);
		let version = cursor.byte()?;
		if version != EVIDENCE_MANIFEST_VERSION {
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

/// Evaluate the content identity supplied to this process. This is shadow-only in ADR 581 cut 1:
/// callers report the result beside the legacy verdict but must not use it to grant authority.
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
			vec!["z.rs".into(), "a.rs".into(), "a.rs".into()],
		)
		.unwrap()
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
		std::fs::write(&artifact, b"{}\n").unwrap();
		let manifest = RunEvidenceManifest::unsealed(fixture(), "suite", &artifact, false).unwrap();
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
}
