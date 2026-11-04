use crate::consensus::raft_storage::{ControlRingBackupImage, SmState};
use crate::hash::blake3;
use crate::schema::SlabId;
use crate::storage::manager::{StorageBackupImage, TieredSlabPointer};
use openraft::LogId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Error, ErrorKind};
use std::time::{SystemTime, UNIX_EPOCH};

pub const CLUSTER_BACKUP_FORMAT_VERSION: u16 = 2;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterBackupConsistentCut {
	pub last_applied_log_id: Option<LogId<u32>>,
	pub current_epoch: u64,
	pub last_e_safe_index: u64,
	pub voter_ids: Vec<u32>,
	pub control_state_hash: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterNodeBackupManifest {
	pub node_id: u32,
	/// The store this node's local ids and pointers belong to (ADR 0697).
	pub source: crate::consensus::OwnerRef,
	pub active_slab_id: SlabId,
	pub next_slab_id: SlabId,
	pub local_copy_slab_ids: Vec<SlabId>,
	pub local_copy_slab_bytes: u64,
	pub tiered_references: Vec<TieredSlabPointer>,
	pub omitted_rehydrated_slabs: Vec<SlabId>,
	pub storage_image_hash: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterNodeBackupImage {
	pub node_id: u32,
	pub storage: StorageBackupImage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterBackupManifest {
	pub format_version: u16,
	pub created_at_unix_seconds: u64,
	pub consistent_cut: ClusterBackupConsistentCut,
	pub control_image_hash: [u8; 32],
	pub nodes: Vec<ClusterNodeBackupManifest>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterBackupImage {
	pub manifest: ClusterBackupManifest,
	pub control: ControlRingBackupImage,
	pub nodes: Vec<ClusterNodeBackupImage>,
}

impl ClusterBackupImage {
	pub fn assemble(
		control: ControlRingBackupImage,
		state: &SmState,
		mut nodes: Vec<ClusterNodeBackupImage>,
	) -> std::io::Result<Self> {
		if nodes.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"cluster backup requires at least one node storage artifact",
			));
		}
		nodes.sort_by_key(|node| node.node_id);
		let mut seen = BTreeSet::new();
		for node in &nodes {
			if !seen.insert(node.node_id) {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					format!("duplicate node storage artifact for node {}", node.node_id),
				));
			}
		}

		let control_image_hash = artifact_hash(&control)?;
		let node_manifests = nodes
			.iter()
			.map(|node| {
				storage_manifest_for_node(
					node.node_id,
					&node.storage,
					artifact_hash(&node.storage)?,
				)
			})
			.collect::<std::io::Result<Vec<_>>>()?;

		Ok(Self {
			manifest: ClusterBackupManifest {
				format_version: CLUSTER_BACKUP_FORMAT_VERSION,
				created_at_unix_seconds: unix_now_seconds(),
				consistent_cut: consistent_cut_for_state(state, &control),
				control_image_hash,
				nodes: node_manifests,
			},
			control,
			nodes,
		})
	}

	pub fn validate_integrity(&self) -> std::io::Result<()> {
		if self.manifest.format_version != CLUSTER_BACKUP_FORMAT_VERSION {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"unsupported cluster backup manifest format version {}",
					self.manifest.format_version
				),
			));
		}
		self.control.validate_format()?;
		for node in &self.nodes {
			node.storage.validate_format()?;
		}
		if self.manifest.control_image_hash != artifact_hash(&self.control)? {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"cluster backup control image hash mismatch",
			));
		}
		if self.manifest.consistent_cut.last_applied_log_id
			!= self.control.manifest.last_applied_log_id
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				"cluster backup cut last_applied_log_id does not match the control image",
			));
		}
		if self.manifest.consistent_cut.control_state_hash != self.control.manifest.state_hash {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"cluster backup cut control_state_hash does not match the control image",
			));
		}
		let mut control_voters = self.control.manifest.voter_ids.clone();
		control_voters.sort_unstable();
		if self.manifest.consistent_cut.voter_ids != control_voters {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"cluster backup cut voter_ids do not match the control image",
			));
		}

		let mut manifest_nodes = BTreeMap::new();
		for node in &self.manifest.nodes {
			if manifest_nodes.insert(node.node_id, node).is_some() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!("duplicate node {} in cluster backup manifest", node.node_id),
				));
			}
		}
		if manifest_nodes.len() != self.nodes.len() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"cluster backup manifest node count does not match embedded storage images",
			));
		}
		for node in &self.nodes {
			let recomputed_storage_hash = artifact_hash(&node.storage)?;
			let Some(recorded) = manifest_nodes.get(&node.node_id) else {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!("missing node {} from cluster backup manifest", node.node_id),
				));
			};
			if recorded.storage_image_hash != recomputed_storage_hash {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"cluster backup storage image hash mismatch for node {}",
						node.node_id
					),
				));
			}
			if node.storage.manifest.source.node() != node.node_id {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"cluster backup storage for node {} names source node {}",
						node.node_id,
						node.storage.manifest.source.node()
					),
				));
			}
			let expected =
				storage_manifest_for_node(node.node_id, &node.storage, recomputed_storage_hash)?;
			if **recorded != expected {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"cluster backup node manifest mismatch for node {}",
						node.node_id
					),
				));
			}
		}
		Ok(())
	}
}

/// Where a backup node's storage is being restored to (ADR 0697).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RestoreTarget {
	/// The same node id, as a fresh store incarnation.
	SameNode,
	/// A replacement node with a different id, as a fresh store incarnation.
	Replacement { target_node: u32 },
}

/// ADR 0697: a restore this release cannot perform safely, refused before any
/// restore effect. Restoring a registered node to a fresh incarnation and
/// re-admitting cold data need an arbitrated owner transition (DC-162).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UnsupportedRestore {
	/// The source store has tiered or omitted (cold) data that a fresh
	/// incarnation would not own.
	ColdReferences {
		source: crate::consensus::OwnerRef,
		tiered: usize,
		omitted: usize,
	},
	/// The target node is already bound to a store incarnation on the ring; a
	/// fresh store cannot take it over without an owner transition.
	IncarnationTransition { node: u32 },
}

impl std::fmt::Display for UnsupportedRestore {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::ColdReferences {
				source,
				tiered,
				omitted,
			} => write!(
				f,
				"UnsupportedRestore::ColdReferences: source {source:?} has {tiered} tiered and {omitted} omitted slab references; restoring cold data to a fresh store is not supported (DC-162)"
			),
			Self::IncarnationTransition { node } => write!(
				f,
				"UnsupportedRestore::IncarnationTransition: node {node} is bound to a store incarnation on the ring; a fresh store cannot take it over (DC-162)"
			),
		}
	}
}

impl std::error::Error for UnsupportedRestore {}

/// A backup artifact declares a manifest format this build does not read
/// (ADR 0697). There is no migration path; the artifact is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnsupportedBackupFormat {
	pub artifact: &'static str,
	pub found: u16,
	pub required: u16,
}

impl std::fmt::Display for UnsupportedBackupFormat {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"UnsupportedBackupFormat: {} backup manifest declares format version {} (this build reads only {})",
			self.artifact, self.found, self.required
		)
	}
}

impl std::error::Error for UnsupportedBackupFormat {}

fn validate_backup_format(
	artifact: &'static str,
	found: u16,
	required: u16,
) -> std::io::Result<()> {
	if found == required {
		return Ok(());
	}
	Err(Error::new(
		ErrorKind::InvalidData,
		UnsupportedBackupFormat {
			artifact,
			found,
			required,
		},
	))
}

impl ControlRingBackupImage {
	/// This artifact's own manifest format, never inferred from a successful
	/// decode or from the current bodies it encloses.
	pub(crate) fn validate_format(&self) -> std::io::Result<()> {
		validate_backup_format(
			"control",
			self.manifest.format_version,
			crate::consensus::raft_storage::CONTROL_RING_BACKUP_FORMAT_VERSION,
		)
	}
}

impl StorageBackupImage {
	/// This artifact's own manifest format, never inferred from a successful
	/// decode.
	pub(crate) fn validate_format(&self) -> std::io::Result<()> {
		validate_backup_format(
			"storage",
			self.manifest.format_version,
			crate::storage::manager::STORAGE_BACKUP_FORMAT_VERSION,
		)
	}
}

/// The image's storage does not agree with the control state its own bytes
/// replay to (ADR 0697 restore preflight); the image is refused as invalid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackupSourceMismatch {
	/// The storage manifest's source names another node.
	SourceNode {
		node: u32,
		source: crate::consensus::OwnerRef,
	},
	/// The source is registered but is not its node's current incarnation.
	NotCurrentIncarnation { source: crate::consensus::OwnerRef },
	/// The storage references differ from the source's committed pointers.
	ReferencesDiffer { source: crate::consensus::OwnerRef },
	/// The control state holds pointers for a node that never registered.
	UnregisteredPointers { node: u32 },
}

impl std::fmt::Display for BackupSourceMismatch {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::SourceNode { node, source } => write!(
				f,
				"BackupSourceMismatch::SourceNode: storage for node {node} names source {source:?}"
			),
			Self::NotCurrentIncarnation { source } => write!(
				f,
				"BackupSourceMismatch::NotCurrentIncarnation: backup source {source:?} is not its node's current incarnation in the backup's control state"
			),
			Self::ReferencesDiffer { source } => write!(
				f,
				"BackupSourceMismatch::ReferencesDiffer: backup storage references for {source:?} do not match its committed pointers"
			),
			Self::UnregisteredPointers { node } => write!(
				f,
				"BackupSourceMismatch::UnregisteredPointers: backup control state holds pointers for unregistered node {node}"
			),
		}
	}
}

impl std::error::Error for BackupSourceMismatch {}

impl ClusterBackupImage {
	/// ADR 0697 restore preflight, decided from the control state these exact
	/// backup bytes replay to, before any restore marker, artifact or
	/// publication. It binds the node's storage to its source store, checks
	/// the source's complete canonical pointer set against the image, and
	/// refuses cold references and registered-node incarnation transitions.
	pub(crate) fn validate_restore_eligibility(
		&self,
		source_node: u32,
		target: RestoreTarget,
	) -> std::io::Result<()> {
		let node = self
			.nodes
			.iter()
			.find(|node| node.node_id == source_node)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::NotFound,
					format!("cluster backup has no storage for node {source_node}"),
				)
			})?;
		let source = node.storage.manifest.source;
		if source.node() != source_node {
			return Err(Error::new(
				ErrorKind::InvalidData,
				BackupSourceMismatch::SourceNode {
					node: source_node,
					source,
				},
			));
		}
		let state = self.control.replayed_state()?;
		let registry = state.incarnations.get(&source.node());
		let registered = registry
			.is_some_and(|registry| registry.current.is_some() || !registry.retired.is_empty());
		let mut canonical: Vec<TieredSlabPointer> = state
			.tiered_slabs
			.values()
			.filter(|tier| tier.slab.owner() == source)
			.map(TieredSlabPointer::from_ring)
			.collect();
		canonical.sort_by_key(|pointer| pointer.slab_id);
		let mut referenced = node.storage.manifest.tiered_slabs.clone();
		referenced.sort_by_key(|pointer| pointer.slab_id);
		if registered {
			if registry.and_then(|registry| registry.current) != Some(source.incarnation()) {
				return Err(Error::new(
					ErrorKind::InvalidData,
					BackupSourceMismatch::NotCurrentIncarnation { source },
				));
			}
			if canonical != referenced {
				return Err(Error::new(
					ErrorKind::InvalidData,
					BackupSourceMismatch::ReferencesDiffer { source },
				));
			}
		} else if state
			.tiered_slabs
			.keys()
			.any(|slab| slab.owner().node() == source.node())
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				BackupSourceMismatch::UnregisteredPointers {
					node: source.node(),
				},
			));
		}
		let omitted = node.storage.manifest.omitted_rehydrated_slabs.len();
		if !referenced.is_empty() || !canonical.is_empty() || omitted > 0 {
			return Err(Error::new(
				ErrorKind::Unsupported,
				UnsupportedRestore::ColdReferences {
					source,
					tiered: referenced.len().max(canonical.len()),
					omitted,
				},
			));
		}
		let target_node = match target {
			RestoreTarget::SameNode => source.node(),
			RestoreTarget::Replacement { target_node } => target_node,
		};
		if state
			.incarnations
			.get(&target_node)
			.is_some_and(|registry| registry.current.is_some())
		{
			return Err(Error::new(
				ErrorKind::Unsupported,
				UnsupportedRestore::IncarnationTransition { node: target_node },
			));
		}
		Ok(())
	}
}

fn current_epoch_for_state(state: &SmState) -> u64 {
	state
		.epoch_history
		.keys()
		.next_back()
		.copied()
		.or_else(|| state.e_safe_watermarks.keys().copied().max())
		.unwrap_or(0)
}

fn consistent_cut_for_state(
	state: &SmState,
	control: &ControlRingBackupImage,
) -> ClusterBackupConsistentCut {
	let mut voter_ids: Vec<u32> = state.last_membership.voter_ids().collect();
	voter_ids.sort_unstable();
	ClusterBackupConsistentCut {
		last_applied_log_id: state.last_applied_log_id,
		current_epoch: current_epoch_for_state(state),
		last_e_safe_index: state.last_e_safe_index,
		voter_ids,
		control_state_hash: control.manifest.state_hash,
	}
}

fn storage_manifest_for_node(
	node_id: u32,
	storage: &StorageBackupImage,
	storage_image_hash: [u8; 32],
) -> std::io::Result<ClusterNodeBackupManifest> {
	let mut local_copy_slab_ids = storage
		.local_slabs
		.iter()
		.map(|slab| slab.slab_id)
		.collect::<Vec<_>>();
	local_copy_slab_ids.sort_unstable();
	Ok(ClusterNodeBackupManifest {
		node_id,
		source: storage.manifest.source,
		active_slab_id: storage.manifest.active_slab_id,
		next_slab_id: storage.manifest.next_slab_id,
		local_copy_slab_ids,
		local_copy_slab_bytes: storage.manifest.local_slab_bytes,
		tiered_references: storage.manifest.tiered_slabs.clone(),
		omitted_rehydrated_slabs: storage.manifest.omitted_rehydrated_slabs.clone(),
		storage_image_hash,
	})
}

fn artifact_hash<T: Serialize>(artifact: &T) -> std::io::Result<[u8; 32]> {
	let bytes = postcard::to_stdvec(artifact)
		.map_err(|err| Error::other(format!("encode backup artifact for hashing: {err}")))?;
	Ok(blake3::digest(&bytes))
}

fn unix_now_seconds() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_secs()
}
