//! Versioned, read-only discovery for Nilket's local workspace.
//!
//! The local supervisor owns publication and lifecycle. This crate owns the
//! identity schema and observes it without acquiring the ownership lock
//! (PRODUCT-0005).

#![deny(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read};
use std::net::SocketAddr;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub const FORMAT_VERSION: u32 = 1;
pub const SQL_ENDPOINT: &str = "127.0.0.1:7742";
pub const WENDELON_ENDPOINT: &str = "127.0.0.1:7001";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfileMarker {
	pub format_version: u32,
	pub profile_uuid: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeManifest {
	pub version: u32,
	pub profile_uuid: Uuid,
	pub session_nonce: Uuid,
	pub pid: u32,
	pub process_start_token: String,
	pub started_unix_ms: u128,
	pub wendelon_endpoint: String,
	pub sql_endpoint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfilePaths {
	pub root: PathBuf,
	pub marker: PathBuf,
	pub config_dir: PathBuf,
	pub wendelon_config: PathBuf,
	pub toron_config: PathBuf,
	pub pki_dir: PathBuf,
	pub wendelon_data: PathBuf,
	pub logs_dir: PathBuf,
	pub lock: PathBuf,
	pub runtime: PathBuf,
}

impl ProfilePaths {
	#[must_use]
	pub fn new(root: PathBuf) -> Self {
		Self {
			marker: root.join("profile.toml"),
			config_dir: root.join("config"),
			wendelon_config: root.join("config/wendelon.toml"),
			toron_config: root.join("config/toron.toml"),
			pki_dir: root.join("pki"),
			wendelon_data: root.join("data/wendelon"),
			logs_dir: root.join("logs"),
			lock: root.join("local.lock"),
			runtime: root.join("runtime.json"),
			root,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Artifact {
	ProfileRoot,
	ProfileMarker,
	RuntimeManifest,
	OwnershipLock,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionedArtifact {
	ProfileMarker,
	RuntimeManifest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Evidence {
	ProfileRoot,
	ProfileMarker,
	RuntimeManifest,
	OwnershipLock,
	OwnerProcess,
	StableIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StaleCause {
	LockFree,
	OwnerMismatch { lock_owner_pid: u32 },
	OwnerGone,
	PidRecycled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceIdentity {
	pub profile_root: PathBuf,
	pub profile_uuid: Uuid,
	pub session_nonce: Uuid,
	pub owner_pid: u32,
	pub started_unix_ms: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservationContext {
	Root {
		profile_root: PathBuf,
	},
	Profile {
		profile_root: PathBuf,
		profile_uuid: Uuid,
	},
	Runtime(WorkspaceIdentity),
}

impl ObservationContext {
	#[must_use]
	pub fn profile_root(&self) -> &Path {
		match self {
			Self::Root { profile_root } | Self::Profile { profile_root, .. } => profile_root,
			Self::Runtime(identity) => &identity.profile_root,
		}
	}
}

impl fmt::Display for ObservationContext {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Root { profile_root } => write!(formatter, "{}", profile_root.display()),
			Self::Profile {
				profile_root,
				profile_uuid,
			} => write!(
				formatter,
				"{} profile {profile_uuid}",
				profile_root.display()
			),
			Self::Runtime(identity) => write!(
				formatter,
				"{} profile {} session {} pid {}",
				identity.profile_root.display(),
				identity.profile_uuid,
				identity.session_nonce,
				identity.owner_pid
			),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyWorkspace {
	pub sql_endpoint: SocketAddr,
	pub identity: WorkspaceIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalWorkspace {
	Absent {
		profile_root: PathBuf,
	},
	Starting {
		profile_root: PathBuf,
		owner_pid: u32,
	},
	Ready(ReadyWorkspace),
	Unhealthy {
		identity: WorkspaceIdentity,
		cause: String,
	},
	Stale {
		identity: WorkspaceIdentity,
		cause: StaleCause,
	},
	Corrupt {
		context: ObservationContext,
		artifact: Artifact,
		cause: String,
	},
	Incompatible {
		context: ObservationContext,
		artifact: VersionedArtifact,
		found: u32,
		supported: u32,
	},
	Unverifiable {
		context: ObservationContext,
		evidence: Evidence,
		cause: String,
	},
}

impl LocalWorkspace {
	#[must_use]
	pub fn profile_root(&self) -> &Path {
		match self {
			Self::Absent { profile_root } | Self::Starting { profile_root, .. } => profile_root,
			Self::Ready(ready) => &ready.identity.profile_root,
			Self::Unhealthy { identity, .. } | Self::Stale { identity, .. } => {
				&identity.profile_root
			}
			Self::Corrupt { context, .. }
			| Self::Incompatible { context, .. }
			| Self::Unverifiable { context, .. } => context.profile_root(),
		}
	}
}

impl fmt::Display for LocalWorkspace {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Absent { profile_root } => {
				write!(
					formatter,
					"local workspace is not running ({})",
					profile_root.display()
				)
			}
			Self::Starting {
				profile_root,
				owner_pid,
			} => write!(
				formatter,
				"local workspace is starting under pid {owner_pid} ({})",
				profile_root.display()
			),
			Self::Ready(ready) => write!(
				formatter,
				"local workspace ready at {} (session {})",
				ready.sql_endpoint, ready.identity.session_nonce
			),
			Self::Unhealthy { identity, cause } => write!(
				formatter,
				"local workspace session {} owned by pid {} is unhealthy: {cause}",
				identity.session_nonce, identity.owner_pid
			),
			Self::Stale { identity, cause } => write!(
				formatter,
				"local workspace session {} is stale: {cause:?}",
				identity.session_nonce
			),
			Self::Corrupt {
				context,
				artifact,
				cause,
			} => write!(
				formatter,
				"local workspace {context} has corrupt {artifact:?}: {cause}"
			),
			Self::Incompatible {
				context,
				artifact,
				found,
				supported,
			} => write!(
				formatter,
				"local workspace {context} has incompatible {artifact:?} version {found}; supported {supported}"
			),
			Self::Unverifiable {
				context,
				evidence,
				cause,
			} => write!(
				formatter,
				"local workspace {context} could not verify {evidence:?}: {cause}"
			),
		}
	}
}

#[derive(Debug, Deserialize)]
struct ProfileVersionHeader {
	format_version: u32,
}

#[derive(Debug, Deserialize)]
struct RuntimeVersionHeader {
	version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum IdentityObservation {
	Verified {
		identity: WorkspaceIdentity,
		sql_endpoint: SocketAddr,
		lock_owner_pid: u32,
	},
	Settled(LocalWorkspace),
}

pub fn resolve_profile_root(explicit: Option<&Path>) -> Result<PathBuf, String> {
	if let Some(path) = explicit {
		if path.as_os_str().is_empty() {
			return Err("--data-dir must not be empty".to_owned());
		}
		return absolute_lexical(path);
	}
	if let Some(xdg) = std::env::var_os("XDG_DATA_HOME").filter(|value| !value.is_empty()) {
		return absolute_lexical(&PathBuf::from(xdg).join("nilket/local"));
	}
	let home = std::env::var_os("HOME")
		.filter(|value| !value.is_empty())
		.ok_or_else(|| {
			"neither XDG_DATA_HOME nor HOME is usable; pass --data-dir PATH".to_owned()
		})?;
	absolute_lexical(&PathBuf::from(home).join(".local/share/nilket/local"))
}

fn absolute_lexical(path: &Path) -> Result<PathBuf, String> {
	if path.is_absolute() {
		Ok(path.to_path_buf())
	} else {
		std::env::current_dir()
			.map(|current| current.join(path))
			.map_err(|error| format!("resolve current directory: {error}"))
	}
}

#[must_use]
pub async fn discover(profile_root: Option<&Path>) -> LocalWorkspace {
	let root = match resolve_profile_root(profile_root) {
		Ok(root) => root,
		Err(cause) => {
			return LocalWorkspace::Unverifiable {
				context: ObservationContext::Root {
					profile_root: profile_root
						.unwrap_or(Path::new("<unresolved>"))
						.to_path_buf(),
				},
				evidence: Evidence::ProfileRoot,
				cause,
			};
		}
	};
	discover_async_root(root, false).await
}

async fn discover_async_root(root: PathBuf, retried: bool) -> LocalWorkspace {
	let first = observe_identity(&root);
	let IdentityObservation::Verified {
		identity,
		sql_endpoint,
		lock_owner_pid,
	} = first
	else {
		return match first {
			IdentityObservation::Settled(state) => state,
			_ => unreachable!(),
		};
	};
	let health = match toron_client::ToronClient::loopback(sql_endpoint.to_string()) {
		Ok(client) => client.health().await.map_err(|error| error.to_string()),
		Err(error) => Err(error.to_string()),
	};
	match confirm_identity(&root, &identity, sql_endpoint, lock_owner_pid) {
		Confirmation::Stable => health_state(identity, sql_endpoint, health),
		Confirmation::Settled(state) => state,
		Confirmation::Changed if !retried => discover_async_root_boxed(root).await,
		Confirmation::Changed => LocalWorkspace::Unverifiable {
			context: ObservationContext::Runtime(identity),
			evidence: Evidence::StableIdentity,
			cause: "workspace identity changed twice during discovery".to_owned(),
		},
	}
}

fn discover_async_root_boxed(
	root: PathBuf,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = LocalWorkspace> + Send>> {
	Box::pin(discover_async_root(root, true))
}

#[must_use]
pub fn blocking_discover(profile_root: Option<&Path>) -> LocalWorkspace {
	let root = match resolve_profile_root(profile_root) {
		Ok(root) => root,
		Err(cause) => {
			return LocalWorkspace::Unverifiable {
				context: ObservationContext::Root {
					profile_root: profile_root
						.unwrap_or(Path::new("<unresolved>"))
						.to_path_buf(),
				},
				evidence: Evidence::ProfileRoot,
				cause,
			};
		}
	};
	discover_blocking_root(root, false)
}

fn discover_blocking_root(root: PathBuf, retried: bool) -> LocalWorkspace {
	let first = observe_identity(&root);
	let IdentityObservation::Verified {
		identity,
		sql_endpoint,
		lock_owner_pid,
	} = first
	else {
		return match first {
			IdentityObservation::Settled(state) => state,
			_ => unreachable!(),
		};
	};
	let health = toron_client::blocking_health_loopback(&sql_endpoint.to_string())
		.map_err(|error| error.to_string());
	match confirm_identity(&root, &identity, sql_endpoint, lock_owner_pid) {
		Confirmation::Stable => health_state(identity, sql_endpoint, health),
		Confirmation::Settled(state) => state,
		Confirmation::Changed if !retried => discover_blocking_root(root, true),
		Confirmation::Changed => LocalWorkspace::Unverifiable {
			context: ObservationContext::Runtime(identity),
			evidence: Evidence::StableIdentity,
			cause: "workspace identity changed twice during discovery".to_owned(),
		},
	}
}

fn health_state(
	identity: WorkspaceIdentity,
	sql_endpoint: SocketAddr,
	health: Result<(), String>,
) -> LocalWorkspace {
	match health {
		Ok(()) => LocalWorkspace::Ready(ReadyWorkspace {
			sql_endpoint,
			identity,
		}),
		Err(cause) => LocalWorkspace::Unhealthy { identity, cause },
	}
}

enum Confirmation {
	Stable,
	Changed,
	Settled(LocalWorkspace),
}

fn confirm_identity(
	root: &Path,
	identity: &WorkspaceIdentity,
	sql_endpoint: SocketAddr,
	lock_owner_pid: u32,
) -> Confirmation {
	match observe_identity(root) {
		IdentityObservation::Verified {
			identity: observed,
			sql_endpoint: observed_endpoint,
			lock_owner_pid: observed_owner,
		} if observed == *identity
			&& observed_endpoint == sql_endpoint
			&& observed_owner == lock_owner_pid =>
		{
			Confirmation::Stable
		}
		IdentityObservation::Verified { .. } => Confirmation::Changed,
		IdentityObservation::Settled(state) => Confirmation::Settled(state),
	}
}

fn observe_identity(root: &Path) -> IdentityObservation {
	let root_context = || ObservationContext::Root {
		profile_root: root.to_path_buf(),
	};
	let root_metadata = match fs::symlink_metadata(root) {
		Ok(metadata) => metadata,
		Err(error) if error.kind() == io::ErrorKind::NotFound => {
			return IdentityObservation::Settled(LocalWorkspace::Absent {
				profile_root: root.to_path_buf(),
			});
		}
		Err(error) => return unverifiable(root_context(), Evidence::ProfileRoot, error),
	};
	if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
		return corrupt(
			root_context(),
			Artifact::ProfileRoot,
			"profile root must be a real directory",
		);
	}
	let paths = ProfilePaths::new(root.to_path_buf());
	for (path, artifact) in [
		(&paths.marker, Artifact::ProfileMarker),
		(&paths.runtime, Artifact::RuntimeManifest),
		(&paths.lock, Artifact::OwnershipLock),
	] {
		match fs::symlink_metadata(path) {
			Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
				return corrupt(root_context(), artifact, "artifact must be a regular file");
			}
			Ok(_) => {}
			Err(error) if error.kind() == io::ErrorKind::NotFound => {}
			Err(error) => return unverifiable(root_context(), evidence_for(artifact), error),
		}
	}
	let lock_owner = match observe_lock_owner(&paths.lock) {
		Ok(owner) => owner,
		Err(error) => return unverifiable(root_context(), Evidence::OwnershipLock, error),
	};
	if !paths.marker.exists() {
		if let Some(owner_pid) = lock_owner {
			return IdentityObservation::Settled(LocalWorkspace::Starting {
				profile_root: root.to_path_buf(),
				owner_pid,
			});
		}
		return match root_has_only_optional_lock(&paths) {
			Ok(true) => IdentityObservation::Settled(LocalWorkspace::Absent {
				profile_root: root.to_path_buf(),
			}),
			Ok(false) => corrupt(
				root_context(),
				Artifact::ProfileRoot,
				"profile content exists without profile.toml",
			),
			Err(error) => unverifiable(root_context(), Evidence::ProfileRoot, error),
		};
	}
	let marker_bytes = match read_regular(&paths.marker) {
		Ok(bytes) => bytes,
		Err(error) => return unverifiable(root_context(), Evidence::ProfileMarker, error),
	};
	let marker_text = match std::str::from_utf8(&marker_bytes) {
		Ok(text) => text,
		Err(error) => return corrupt(root_context(), Artifact::ProfileMarker, error),
	};
	let marker_header: ProfileVersionHeader = match toml::from_str(marker_text) {
		Ok(header) => header,
		Err(error) => return corrupt(root_context(), Artifact::ProfileMarker, error),
	};
	if marker_header.format_version != FORMAT_VERSION {
		return incompatible(
			root_context(),
			VersionedArtifact::ProfileMarker,
			marker_header.format_version,
		);
	}
	let marker: ProfileMarker = match toml::from_str(marker_text) {
		Ok(marker) => marker,
		Err(error) => return corrupt(root_context(), Artifact::ProfileMarker, error),
	};
	let profile_context = || ObservationContext::Profile {
		profile_root: root.to_path_buf(),
		profile_uuid: marker.profile_uuid,
	};
	if !paths.runtime.exists() {
		return IdentityObservation::Settled(match lock_owner {
			Some(owner_pid) => LocalWorkspace::Starting {
				profile_root: root.to_path_buf(),
				owner_pid,
			},
			None => LocalWorkspace::Absent {
				profile_root: root.to_path_buf(),
			},
		});
	}
	let runtime_bytes = match read_regular(&paths.runtime) {
		Ok(bytes) => bytes,
		Err(error) => return unverifiable(profile_context(), Evidence::RuntimeManifest, error),
	};
	let runtime_header: RuntimeVersionHeader = match serde_json::from_slice(&runtime_bytes) {
		Ok(header) => header,
		Err(error) => return corrupt(profile_context(), Artifact::RuntimeManifest, error),
	};
	if runtime_header.version != FORMAT_VERSION {
		return incompatible(
			profile_context(),
			VersionedArtifact::RuntimeManifest,
			runtime_header.version,
		);
	}
	let runtime: RuntimeManifest = match serde_json::from_slice(&runtime_bytes) {
		Ok(runtime) => runtime,
		Err(error) => return corrupt(profile_context(), Artifact::RuntimeManifest, error),
	};
	if runtime.profile_uuid != marker.profile_uuid {
		return corrupt(
			profile_context(),
			Artifact::RuntimeManifest,
			"profile UUID does not match marker",
		);
	}
	let identity = WorkspaceIdentity {
		profile_root: root.to_path_buf(),
		profile_uuid: runtime.profile_uuid,
		session_nonce: runtime.session_nonce,
		owner_pid: runtime.pid,
		started_unix_ms: runtime.started_unix_ms,
	};
	let runtime_context = || ObservationContext::Runtime(identity.clone());
	let Some(lock_owner_pid) = lock_owner else {
		return IdentityObservation::Settled(LocalWorkspace::Stale {
			identity,
			cause: StaleCause::LockFree,
		});
	};
	if lock_owner_pid != runtime.pid {
		return IdentityObservation::Settled(LocalWorkspace::Stale {
			identity,
			cause: StaleCause::OwnerMismatch { lock_owner_pid },
		});
	}
	let token = match classified_process_start_token(runtime.pid) {
		Ok(token) => token,
		Err(ProcessTokenError::Gone) => {
			return IdentityObservation::Settled(LocalWorkspace::Stale {
				identity,
				cause: StaleCause::OwnerGone,
			});
		}
		Err(ProcessTokenError::Unreadable(cause)) => {
			return unverifiable(runtime_context(), Evidence::OwnerProcess, cause);
		}
	};
	if token != runtime.process_start_token {
		return IdentityObservation::Settled(LocalWorkspace::Stale {
			identity,
			cause: StaleCause::PidRecycled,
		});
	}
	let sql_endpoint = match runtime.sql_endpoint.parse::<SocketAddr>() {
		Ok(endpoint) if endpoint.ip().is_loopback() => endpoint,
		Ok(_) => {
			return corrupt(
				runtime_context(),
				Artifact::RuntimeManifest,
				"SQL endpoint is not loopback",
			);
		}
		Err(error) => return corrupt(runtime_context(), Artifact::RuntimeManifest, error),
	};
	IdentityObservation::Verified {
		identity,
		sql_endpoint,
		lock_owner_pid,
	}
}

fn root_has_only_optional_lock(paths: &ProfilePaths) -> Result<bool, io::Error> {
	for entry in fs::read_dir(&paths.root)? {
		if entry?.file_name() != "local.lock" {
			return Ok(false);
		}
	}
	Ok(true)
}

fn read_regular(path: &Path) -> Result<Vec<u8>, io::Error> {
	let before = fs::symlink_metadata(path)?;
	if before.file_type().is_symlink() || !before.is_file() {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			"artifact is not a regular file",
		));
	}
	let mut file = File::open(path)?;
	let opened = file.metadata()?;
	if !opened.is_file() || opened.dev() != before.dev() || opened.ino() != before.ino() {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			"artifact identity changed while opening",
		));
	}
	let mut bytes = Vec::new();
	file.read_to_end(&mut bytes)?;
	Ok(bytes)
}

fn observe_lock_owner(lock_path: &Path) -> Result<Option<u32>, io::Error> {
	let metadata = match fs::symlink_metadata(lock_path) {
		Ok(metadata) => metadata,
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
		Err(error) => return Err(error),
	};
	let device_major = u64::from(libc::major(metadata.dev()));
	let device_minor = u64::from(libc::minor(metadata.dev()));
	let inode = metadata.ino();
	let locks = fs::read_to_string("/proc/locks")?;
	for line in locks.lines() {
		let fields: Vec<_> = line.split_ascii_whitespace().collect();
		let offset = usize::from(fields.get(1) == Some(&"->"));
		if fields.len() < 8 + offset {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				format!("unrecognized /proc/locks line: {line}"),
			));
		}
		if fields[1 + offset] != "FLOCK" || fields[3 + offset] != "WRITE" {
			continue;
		}
		let mut identity = fields[5 + offset].split(':');
		let major = parse_hex(identity.next(), "lock device major")?;
		let minor = parse_hex(identity.next(), "lock device minor")?;
		let lock_inode = identity
			.next()
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing lock inode"))?
			.parse::<u64>()
			.map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
		if identity.next().is_some() {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"unexpected lock identity field",
			));
		}
		if major == device_major && minor == device_minor && lock_inode == inode {
			let owner = fields[4 + offset]
				.parse::<u32>()
				.map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
			return Ok(Some(owner));
		}
	}
	Ok(None)
}

fn parse_hex(value: Option<&str>, label: &str) -> Result<u64, io::Error> {
	u64::from_str_radix(
		value.ok_or_else(|| {
			io::Error::new(io::ErrorKind::InvalidData, format!("missing {label}"))
		})?,
		16,
	)
	.map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[derive(Debug, thiserror::Error)]
enum ProcessTokenError {
	#[error("process is absent")]
	Gone,
	#[error("{0}")]
	Unreadable(String),
}

fn classified_process_start_token(pid: u32) -> Result<String, ProcessTokenError> {
	let stat = fs::read_to_string(format!("/proc/{pid}/stat")).map_err(|error| {
		if error.kind() == io::ErrorKind::NotFound {
			ProcessTokenError::Gone
		} else {
			ProcessTokenError::Unreadable(format!("read process start token: {error}"))
		}
	})?;
	let after_name = stat
		.rsplit_once(')')
		.map(|(_, fields)| fields)
		.ok_or_else(|| ProcessTokenError::Unreadable("malformed /proc process stat".to_owned()))?;
	after_name
		.split_ascii_whitespace()
		.nth(19)
		.map(str::to_owned)
		.ok_or_else(|| {
			ProcessTokenError::Unreadable("missing /proc process start token".to_owned())
		})
}

pub fn process_start_token(pid: u32) -> Result<String, String> {
	classified_process_start_token(pid).map_err(|error| match error {
		ProcessTokenError::Gone => format!("process {pid} is absent"),
		ProcessTokenError::Unreadable(cause) => cause,
	})
}

fn corrupt(
	context: ObservationContext,
	artifact: Artifact,
	cause: impl fmt::Display,
) -> IdentityObservation {
	IdentityObservation::Settled(LocalWorkspace::Corrupt {
		context,
		artifact,
		cause: cause.to_string(),
	})
}

fn incompatible(
	context: ObservationContext,
	artifact: VersionedArtifact,
	found: u32,
) -> IdentityObservation {
	IdentityObservation::Settled(LocalWorkspace::Incompatible {
		context,
		artifact,
		found,
		supported: FORMAT_VERSION,
	})
}

fn unverifiable(
	context: ObservationContext,
	evidence: Evidence,
	cause: impl fmt::Display,
) -> IdentityObservation {
	IdentityObservation::Settled(LocalWorkspace::Unverifiable {
		context,
		evidence,
		cause: cause.to_string(),
	})
}

fn evidence_for(artifact: Artifact) -> Evidence {
	match artifact {
		Artifact::ProfileRoot => Evidence::ProfileRoot,
		Artifact::ProfileMarker => Evidence::ProfileMarker,
		Artifact::RuntimeManifest => Evidence::RuntimeManifest,
		Artifact::OwnershipLock => Evidence::OwnershipLock,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::os::unix::fs::PermissionsExt;
	use std::time::{SystemTime, UNIX_EPOCH};

	fn fresh_root(label: &str) -> PathBuf {
		let nonce = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_nanos();
		std::env::temp_dir().join(format!(
			"nilket-local-{label}-{}-{nonce}",
			std::process::id()
		))
	}

	fn write_marker(paths: &ProfilePaths, version: u32) -> ProfileMarker {
		fs::create_dir_all(&paths.root).unwrap();
		let marker = ProfileMarker {
			format_version: version,
			profile_uuid: Uuid::nil(),
		};
		fs::write(&paths.marker, toml::to_string(&marker).unwrap()).unwrap();
		marker
	}

	#[test]
	fn absence_corruption_and_version_skew_are_distinct() {
		let root = fresh_root("states");
		assert!(matches!(
			blocking_discover(Some(&root)),
			LocalWorkspace::Absent { .. }
		));
		let paths = ProfilePaths::new(root.clone());
		write_marker(&paths, FORMAT_VERSION + 1);
		assert!(matches!(
			blocking_discover(Some(&root)),
			LocalWorkspace::Incompatible {
				artifact: VersionedArtifact::ProfileMarker,
				..
			}
		));
		fs::write(&paths.marker, b"not = [toml").unwrap();
		assert!(matches!(
			blocking_discover(Some(&root)),
			LocalWorkspace::Corrupt {
				artifact: Artifact::ProfileMarker,
				..
			}
		));
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn unsupported_runtime_version_precedes_body_decode() {
		let root = fresh_root("runtime-skew");
		let paths = ProfilePaths::new(root.clone());
		write_marker(&paths, FORMAT_VERSION);
		fs::write(&paths.runtime, br#"{"version":2,"future":"shape"}"#).unwrap();
		let state = blocking_discover(Some(&root));
		assert!(matches!(
			&state,
			LocalWorkspace::Incompatible {
				artifact: VersionedArtifact::RuntimeManifest,
				..
			}
		));
		assert!(state.to_string().contains(&Uuid::nil().to_string()));
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn symlinked_identity_artifact_is_corrupt_and_not_followed() {
		let root = fresh_root("symlink");
		let paths = ProfilePaths::new(root.clone());
		fs::create_dir_all(&root).unwrap();
		let target = root.join("target");
		fs::write(&target, b"format_version = 1").unwrap();
		std::os::unix::fs::symlink(&target, &paths.marker).unwrap();
		assert!(matches!(
			blocking_discover(Some(&root)),
			LocalWorkspace::Corrupt {
				artifact: Artifact::ProfileMarker,
				..
			}
		));
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn marker_read_failure_is_unverifiable_when_permissions_enforce_it() {
		let root = fresh_root("unreadable");
		let paths = ProfilePaths::new(root.clone());
		write_marker(&paths, FORMAT_VERSION);
		fs::set_permissions(&paths.marker, fs::Permissions::from_mode(0o0)).unwrap();
		let state = blocking_discover(Some(&root));
		if !nix_like_root() {
			assert!(matches!(
				state,
				LocalWorkspace::Unverifiable {
					evidence: Evidence::ProfileMarker,
					..
				}
			));
		}
		fs::set_permissions(&paths.marker, fs::Permissions::from_mode(0o600)).unwrap();
		fs::remove_dir_all(root).unwrap();
	}

	fn nix_like_root() -> bool {
		fs::read_to_string("/proc/self/status")
			.ok()
			.is_some_and(|status| status.lines().any(|line| line == "Uid:\t0\t0\t0\t0"))
	}

	#[test]
	fn product_0005_discovery_source_has_no_lifecycle_authority() {
		fn forbidden(source: &str) -> bool {
			[
				"try_lock",
				"lock_exclusive",
				"Command::new",
				"fs::remove",
				"fs::write",
				".write(true)",
				"libc::flock",
				"libc::kill",
			]
			.iter()
			.any(|needle| source.contains(needle))
		}
		let production = include_str!("lib.rs")
			.split("#[cfg(test)]")
			.next()
			.expect("production discovery source");
		assert!(
			!forbidden(production),
			"PRODUCT-0005 discovery must not acquire ownership or mutate lifecycle state"
		);
		assert!(
			forbidden("Command::new(\"nilket\").spawn();"),
			"positive control: PRODUCT-0005 lifecycle assay must fire"
		);
		assert!(production.contains("libc::major"));
		assert!(production.contains("libc::minor"));
		assert_eq!(production.matches("libc::").count(), 2);
	}

	#[test]
	fn product_0005_schema_and_dependency_authority_are_guarded() {
		let production = include_str!("lib.rs")
			.split("#[cfg(test)]")
			.next()
			.expect("production discovery source");
		let supervisor = include_str!("../../nilket/src/local.rs")
			.split("#[cfg(test)]")
			.next()
			.expect("production supervisor source");
		assert_eq!(production.matches("pub struct ProfileMarker").count(), 1);
		assert_eq!(production.matches("pub struct RuntimeManifest").count(), 1);
		assert!(!supervisor.contains("struct ProfileMarker"));
		assert!(!supervisor.contains("struct RuntimeManifest"));

		let manifest = include_str!("../Cargo.toml");
		for forbidden in ["fs2 =", "nix =", "signal-hook =", "wendelon =", "toron ="] {
			assert!(
				!manifest.contains(forbidden),
				"PRODUCT-0005 public discovery acquired forbidden dependency {forbidden}"
			);
		}
		assert!(
			"fs2 = \"synthetic\"".contains("fs2 ="),
			"positive control: PRODUCT-0005 dependency assay must fire"
		);
	}

	#[test]
	#[ignore = "requires a running nilket local profile"]
	fn live_default_profile_discovers_verified_identity() {
		let state = blocking_discover(None);
		assert!(matches!(state, LocalWorkspace::Ready(_)), "{state}");
	}
}
