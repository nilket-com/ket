//! A fresh single-node ParentDecisions local profile: its state is established
//! read-only BEFORE anything is generated, and its one formation intent is issued
//! through a durable, ordered transition of the profile's own marker.
//!
//! The formation stage lives in the required profile marker, bound to its UUID:
//! - `initializing`: written (durably, create-new) when the profile is created;
//!   the intent may be provisioned and the daemon is never started;
//! - `issued`: published by an atomic rewrite of the marker (same UUID) after
//!   the config, the directories and the intent are durable.
//!
//! Absence is never evidence: an `issued` profile whose intent and store are
//! gone refuses, whatever else is missing, and never forms again. A damaged
//! marker refuses at parsing. Only an explicit `nilket local --reset` returns an
//! `issued` profile to `initializing`, after discarding its store.

use super::*;

/// The marker being republished; only this module creates it.
pub(super) const MARKER_TMP: &str = ".profile.toml.tmp";

/// What preparing a ParentDecisions profile will do, decided before any write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Plan {
	/// `initializing`: complete the provisioning. Render the config only if it
	/// is absent while neither the intent nor a store exists (no daemon has ever
	/// run: it starts only after `issued`), then provision the intent if absent,
	/// make every prerequisite durable and publish `issued`.
	Provision { render_config: bool },
	/// `issued`, its intent not yet consumed: make the prerequisites the daemon
	/// will rely on durable again (a previous run may have stopped before its
	/// final syncs), then start.
	ReadyUnconsumed,
	/// `issued`, and its store exists: a daemon started after a durable
	/// `issued`; nothing to provision or sync.
	ReadyFormed,
}

/// The profile's formation stage, from its marker; `None` for a profile that
/// records no ParentDecisions authority.
pub(super) fn stage(marker: &ProfileMarker) -> Option<FormationStage> {
	match marker.decision_authority {
		Some(ProfileDecisionAuthority::ParentDecisions { formation }) => Some(formation),
		None => None,
	}
}

/// The profile's state, read only, and the one plan it allows. Every refusal
/// here precedes every write of the preparation: no key, PKI, config, directory
/// or intent is created for a profile that is refused.
pub(super) fn plan(paths: &ProfilePaths, formation: FormationStage) -> Result<Plan, String> {
	let data_root = paths.root.join("data");
	for dir in [&paths.config_dir, &data_root, &paths.wendelon_data] {
		real_dir_present(dir)?;
	}
	let config = regular_present(&paths.wendelon_config, "Wendelon config")?;
	let intent = regular_present(&intent_path(paths), "formation intent")?;
	regular_present(&paths.root.join(MARKER_TMP), "profile marker")?;
	let store = store_present(&paths.wendelon_data)?;
	match formation {
		FormationStage::Initializing if store => Err(format!(
			"{} holds a Wendelon store, but this ParentDecisions profile never completed its provisioning; nothing was changed",
			paths.wendelon_data.display()
		)),
		FormationStage::Initializing if !config && intent => Err(format!(
			"{} is missing from a ParentDecisions profile whose intent is already provisioned; its decision key is never regenerated; restore it, or discard the profile's store with `nilket local --reset`",
			paths.wendelon_config.display()
		)),
		FormationStage::Initializing => Ok(Plan::Provision {
			render_config: !config,
		}),
		FormationStage::Issued if !config => Err(format!(
			"{} is missing from a ParentDecisions profile whose intent was issued; its decision key is never regenerated; restore it, or discard the store with `nilket local --reset`",
			paths.wendelon_config.display()
		)),
		FormationStage::Issued if store => Ok(Plan::ReadyFormed),
		FormationStage::Issued if intent => Ok(Plan::ReadyUnconsumed),
		FormationStage::Issued => Err(format!(
			"the store this ParentDecisions profile formed is missing from {}: its one formation intent was issued, and a profile never forms again; restore the store, or discard it with `nilket local --reset`",
			paths.wendelon_data.display()
		)),
	}
}

/// Carries out `plan` after the configs are in place and validated.
pub(super) fn execute(
	paths: &ProfilePaths,
	marker: &ProfileMarker,
	plan: Plan,
) -> Result<(), String> {
	match plan {
		Plan::ReadyFormed => Ok(()),
		Plan::ReadyUnconsumed => sync_prerequisites(paths),
		Plan::Provision { .. } => {
			let intent = intent_path(paths);
			if !regular_present(&intent, "formation intent")? {
				// Durable: Wendelon writes it create-new and syncs it and its
				// directory.
				wendelon::node::provision_formation_intent(&paths.wendelon_data).map_err(
					|error| {
						format!(
							"provision the formation intent in {}: {error}",
							paths.wendelon_data.display()
						)
					},
				)?;
			}
			// What a previous, interrupted run left may never have been synced:
			// everything `issued` vouches for is made durable first.
			sync_prerequisites(paths)?;
			publish_marker(
				paths,
				&ProfileMarker {
					decision_authority: Some(ProfileDecisionAuthority::ParentDecisions {
						formation: FormationStage::Issued,
					}),
					..marker.clone()
				},
			)
		}
	}
}

/// Rewrites the profile marker atomically (same UUID): a create-new temporary
/// file, synced, renamed over the marker, and the root directory synced. A
/// leftover temporary file of an interrupted rewrite is this module's own and is
/// replaced.
pub(super) fn publish_marker(paths: &ProfilePaths, marker: &ProfileMarker) -> Result<(), String> {
	let tmp = paths.root.join(MARKER_TMP);
	if regular_present(&tmp, "profile marker")? {
		fs::remove_file(&tmp).map_err(|error| format!("remove {}: {error}", tmp.display()))?;
		sync_dir(&paths.root)?;
	}
	let bytes = toml::to_string_pretty(marker)
		.map_err(|error| format!("serialize profile marker: {error}"))?;
	write_durable_new(&tmp, bytes.as_bytes(), 0o600)?;
	fs::rename(&tmp, &paths.marker)
		.map_err(|error| format!("publish {}: {error}", paths.marker.display()))?;
	sync_dir(&paths.root)
}

/// Syncs every file and directory entry an `issued` profile's first start
/// relies on: the config (its key) and its directory, the intent and the store
/// directory with its parents, and the marker's directory.
fn sync_prerequisites(paths: &ProfilePaths) -> Result<(), String> {
	let data_root = paths.root.join("data");
	sync_file(&paths.wendelon_config)?;
	sync_file(&intent_path(paths))?;
	for dir in [
		&paths.config_dir,
		&paths.wendelon_data,
		&data_root,
		&paths.root,
	] {
		sync_dir(dir)?;
	}
	Ok(())
}

/// Creates the ParentDecisions profile's own directories durably (each new
/// directory entry is synced into its parent); an existing one must be a real
/// directory, never a symlink to another.
pub(super) fn ensure_owned_directories(paths: &ProfilePaths) -> Result<(), String> {
	let data_root = paths.root.join("data");
	for (dir, parent) in [
		(&paths.config_dir, &paths.root),
		(&data_root, &paths.root),
		(&paths.wendelon_data, &data_root),
	] {
		if !real_dir_present(dir)? {
			fs::DirBuilder::new()
				.mode(0o700)
				.create(dir)
				.map_err(|error| format!("create {}: {error}", dir.display()))?;
			sync_dir(parent)?;
		}
	}
	Ok(())
}

/// The managed config must select ParentDecisions, form a new cluster
/// (bootstrap) and keep its store exactly in the profile's own data directory,
/// where the intent is provisioned.
pub(super) fn validate_config(
	paths: &ProfilePaths,
	config: &wendelon::node::NodeBootstrapConfig,
) -> Result<(), String> {
	let refuse = |what: &str| {
		Err(format!(
			"retained Wendelon config {} of ParentDecisions profile {} {what}; a profile keeps its authority and its own store; repair the config",
			paths.wendelon_config.display(),
			paths.root.display()
		))
	};
	if !config.selects_parent_decisions() {
		return refuse("does not select ParentDecisions");
	}
	if !matches!(
		config.cluster,
		wendelon::node::ClusterBootstrapConfig::Bootstrap
	) {
		return refuse("does not form a new cluster (cluster.mode must be bootstrap)");
	}
	let owned = fs::canonicalize(&paths.wendelon_data)
		.map_err(|error| format!("resolve {}: {error}", paths.wendelon_data.display()))?;
	match fs::canonicalize(&config.data_dir) {
		Ok(configured) if configured == owned => Ok(()),
		_ => refuse("keeps its store outside the profile's own data directory"),
	}
}

/// The durable write the ParentDecisions artifacts use: create-new (never
/// clobber), contents synced, then the directory entry synced into its parent.
pub(super) fn write_durable_new(path: &Path, bytes: &[u8], mode: u32) -> Result<(), String> {
	let mut file = OpenOptions::new()
		.write(true)
		.create_new(true)
		.mode(mode)
		.open(path)
		.map_err(|error| format!("create {}: {error}", path.display()))?;
	file.write_all(bytes)
		.and_then(|()| file.sync_all())
		.map_err(|error| format!("write {}: {error}", path.display()))?;
	sync_dir(path.parent().unwrap_or(Path::new(".")))
}

fn sync_file(path: &Path) -> Result<(), String> {
	File::open(path)
		.and_then(|handle| handle.sync_all())
		.map_err(|error| format!("sync {}: {error}", path.display()))
}

fn sync_dir(dir: &Path) -> Result<(), String> {
	File::open(dir)
		.and_then(|handle| handle.sync_all())
		.map_err(|error| format!("sync directory {}: {error}", dir.display()))
}

fn intent_path(paths: &ProfilePaths) -> PathBuf {
	paths.wendelon_data.join(wendelon::node::FORMATION_INTENT)
}

/// Whether `dir` exists as a real directory; a symlink or a non-directory
/// refuses, and an inspection error is an error, never absence.
pub(super) fn real_dir_present(dir: &Path) -> Result<bool, String> {
	match fs::symlink_metadata(dir) {
		Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => Err(format!(
			"{} must be a real directory of the profile, not a symlink or a file",
			dir.display()
		)),
		Ok(_) => Ok(true),
		Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
		Err(error) => Err(format!("inspect {}: {error}", dir.display())),
	}
}

/// Whether `path` exists as a regular file; anything else refuses, and an
/// inspection error is an error, never absence.
pub(super) fn regular_present(path: &Path, label: &str) -> Result<bool, String> {
	match fs::symlink_metadata(path) {
		Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
			Err(format!("{label} {} must be a regular file", path.display()))
		}
		Ok(_) => Ok(true),
		Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
		Err(error) => Err(format!("inspect {label} {}: {error}", path.display())),
	}
}

/// Whether the Wendelon data directory holds a store, or the start of one:
/// anything but its ownership `LOCK` and an unconsumed intent. An absent
/// directory holds none; an unreadable one is an error, never "empty".
fn store_present(data: &Path) -> Result<bool, String> {
	if !real_dir_present(data)? {
		return Ok(false);
	}
	for entry in fs::read_dir(data).map_err(|error| format!("read {}: {error}", data.display()))? {
		let name = entry
			.map_err(|error| format!("read {} entry: {error}", data.display()))?
			.file_name();
		if name != "LOCK" && name != wendelon::node::FORMATION_INTENT {
			return Ok(true);
		}
	}
	Ok(false)
}
