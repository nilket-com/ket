//! Data-directory ownership plumbing for the data server, kept out of the
//! dispatch root: binding the server's redb stores to the node's owner and the
//! owner-admitted writes the server performs directly.

use std::path::PathBuf;

use super::WendelonDataServer;
use crate::storage::counter::DurableCounterWitnessStore;
use crate::storage::ownership::{DataDirectoryOwner, WritePermit};

pub(super) type Owner = DataDirectoryOwner;

/// The server's owner (if the slab is owned) and a permit spanning its
/// construction, during which it opens and initializes its stores.
pub(super) fn admit(
	slab: &crate::storage::manager::SlabManager,
) -> std::io::Result<(Option<Owner>, Option<WritePermit>)> {
	let owner = slab.directory_owner();
	let permit = DataDirectoryOwner::optional_permit(owner.as_ref())?;
	Ok((owner, permit))
}

impl WendelonDataServer {
	pub(crate) fn with_directory_owner(mut self, owner: DataDirectoryOwner) -> Self {
		self._directory_owner = Some(owner);
		self
	}
}

/// Bind every redb-backed store to the node's owner so each is closed (and its
/// database dropped) inside the owner's close, before the lock is released.
/// Runs inside the constructor's initialization permit, before the server is
/// returned to any caller.
pub(super) fn bind(server: &WendelonDataServer, owner: &Option<Owner>) -> std::io::Result<()> {
	let Some(owner) = owner.as_ref() else {
		return Ok(());
	};
	server.pending_finalizations.bind_owner(owner.clone())?;
	server.mutation_outcomes.bind_owner(owner.clone())?;
	server.drop_reclaim_progress.bind_owner(owner.clone())
}

/// Open a tenant's counter witness store, owner-admitted and contained in the
/// owned directory when the server is owned.
pub(super) fn witness_store(
	witness_dir: PathBuf,
	owner: Option<&DataDirectoryOwner>,
) -> std::io::Result<DurableCounterWitnessStore> {
	match owner {
		Some(owner) => {
			owner.ensure_contains(&witness_dir)?;
			DurableCounterWitnessStore::open_with_owner(witness_dir, owner.clone())
		}
		None => DurableCounterWitnessStore::open(witness_dir),
	}
}

/// Write the local-persistence distrust marker under a write permit.
pub(super) async fn write_distrust_marker(
	owner: Option<&DataDirectoryOwner>,
	path: PathBuf,
	detail: String,
) -> std::io::Result<()> {
	let permit = DataDirectoryOwner::optional_permit(owner)?;
	tokio::task::spawn_blocking(move || {
		let _permit = permit;
		std::fs::write(path, detail)
	})
	.await
	.map_err(|err| {
		std::io::Error::other(format!(
			"local persistence distrust marker task failed: {err}"
		))
	})?
}
