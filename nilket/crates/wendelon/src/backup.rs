use crate::consensus::raft_storage::{ControlRingBackupImage, SmState};
use crate::hash::blake3;
use crate::schema::SlabId;
use crate::storage::manager::{StorageBackupImage, TieredSlabPointer};
use openraft::LogId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Error, ErrorKind};
use std::time::{SystemTime, UNIX_EPOCH};

pub const CLUSTER_BACKUP_FORMAT_VERSION: u16 = 1;

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
