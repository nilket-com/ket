//! The durable slab id allocator bound. Every id is covered by a committed
//! high-water before it is used, so no reopen ever issues it again.

use super::*;

/// How far ahead of the next id the durable high-water is committed.
pub(super) const SLAB_ID_LEASE: u64 = 64;

pub(super) fn slab_ids_exhausted() -> Error {
	Error::other("slab id space exhausted")
}

impl TieredSlabPointer {
	/// The local pointer for a ring pointer this store owns. Callers have
	/// classified the owner (`classify_ring_slab` / `own_tier_view`).
	pub(crate) fn from_ring(tier: &crate::consensus::TieredSlab) -> Self {
		Self {
			slab_id: tier.slab.local_id(),
			l2_uri: tier.l2_uri.clone(),
			payload_hash: tier.payload_hash,
			is_encrypted: tier.is_encrypted,
			tenant_id: tier.tenant_id,
			tenant_kek_id: tier.tenant_kek_id,
			stats_format_version: tier.stats_format_version,
			stats_block_len: tier.stats_block_len,
			stats_block_hash128: tier.stats_block_hash128,
			tombstone_metadata: tier.tombstone_metadata,
			supersession_metadata: tier.supersession_metadata,
		}
	}
}

/// How a ring-named slab relates to this store (ADR 0697 Addendum A1),
/// judged against the owner node's registry as of the event's apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Ownership {
	/// Another node's store: no local effect.
	Foreign,
	/// This store's own slab, under current authority.
	Own(SlabId),
	/// A retired earlier incarnation of this node: its effects are ignored.
	RetiredOther,
	/// This store has itself been superseded: local authority fences.
	LocalSuperseded,
	/// An own-node identity the registry does not explain: fences.
	Unexplained,
	/// Evidence this store's newer authority already supersedes: no effect,
	/// and it never mints a fence.
	Obsolete,
}

/// The evidence an owner classification reads, and so what makes it obsolete.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerEvidence {
	/// The committed state as read now.
	Current,
	/// A delivered log event, at its applied index.
	Applied(u64),
	/// A snapshot install callback, for its comparison epoch.
	SnapshotInstall(u64),
}

/// A complete committed tier view, narrowed to this store.
pub(crate) enum OwnTierView {
	/// This store's own pointers, as local ids.
	Authoritative(Vec<TieredSlabPointer>),
	/// Not authoritative for this store (superseded or unexplained identity,
	/// already fenced): absence in it is never retirement.
	NotAuthoritative,
	/// A newer comparison owns this decision.
	Obsolete,
}

impl SlabManager {
	/// Bind this store to the control-ring node it serves. A manager serves
	/// one node for its whole life.
	pub fn bind_control_node(&self, node: u32) -> std::io::Result<()> {
		let bound = *self.control_node.get_or_init(|| node);
		if bound != node {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!("store already serves control node {bound}, not {node}"),
			));
		}
		Ok(())
	}

	/// This store's owner reference, for every slab command it proposes.
	pub(crate) fn owner_ref(&self) -> std::io::Result<crate::consensus::OwnerRef> {
		// Production binds every store at node bootstrap, before any slab
		// command is built. Unit tests that never join a ring name their
		// store as node 1 until a test binds it explicitly.
		#[cfg(test)]
		let node = self.control_node.get().copied().unwrap_or(1);
		#[cfg(not(test))]
		let node = self.control_node.get().copied().ok_or_else(|| {
			Error::new(
				ErrorKind::WouldBlock,
				"store is not bound to a control node; it cannot name its slabs on the ring",
			)
		})?;
		let index = self.persistent_index.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::WouldBlock,
				"store has no durable identity; it cannot name its slabs on the ring",
			)
		})?;
		Ok(crate::consensus::OwnerRef::new(
			node,
			index.store_incarnation(),
		))
	}

	/// Classify a ring-named slab against this store, using the owner node's
	/// registry carried with the event. A superseded or unexplained own
	/// identity fences local authority permanently for this process; a later
	/// event classified Own never undoes that, and obsolete evidence never
	/// mints it.
	pub(crate) fn classify_ring_slab(
		&self,
		slab: crate::consensus::SlabRef,
		registry: &crate::consensus::IncarnationRegistry,
		evidence: OwnerEvidence,
	) -> Ownership {
		match self.classify_ring_owner(slab.owner(), registry, evidence) {
			Ownership::Own(_) => Ownership::Own(slab.local_id()),
			other => other,
		}
	}

	/// Classify a ring owner (a whole single-owner command) against this
	/// store. `Own` carries no slab id here.
	pub(crate) fn classify_ring_owner(
		&self,
		owner: crate::consensus::OwnerRef,
		registry: &crate::consensus::IncarnationRegistry,
		evidence: OwnerEvidence,
	) -> Ownership {
		let local = match self.owner_ref() {
			Ok(local) => local,
			Err(_) => {
				return self.fence_owner_authority(
					"UnexplainedOwnIdentity(StoreNotBound)",
					Ownership::Unexplained,
					evidence,
				);
			}
		};
		if owner.node() != local.node() {
			return Ownership::Foreign;
		}
		match self.classify_own_node(registry, evidence) {
			Ownership::Own(_) if owner.incarnation() == local.incarnation() => {
				Ownership::Own(SlabId::ZERO)
			}
			Ownership::Own(_) if registry.retired.contains(&owner.incarnation()) => {
				Ownership::RetiredOther
			}
			Ownership::Own(_) => self.fence_owner_authority(
				"UnexplainedOwnIdentity",
				Ownership::Unexplained,
				evidence,
			),
			other => other,
		}
	}

	/// Classify this store against its own node's registry and fence when it
	/// is superseded or unexplained. Called for registry-only changes too.
	pub(crate) fn classify_own_node(
		&self,
		registry: &crate::consensus::IncarnationRegistry,
		evidence: OwnerEvidence,
	) -> Ownership {
		if self.owner_evidence_is_superseded(evidence) {
			return Ownership::Obsolete;
		}
		match self.own_node_verdict(registry) {
			Ok(ownership) => ownership,
			Err((reason, classified)) => self.fence_owner_authority(reason, classified, evidence),
		}
	}

	/// What `registry` says of this store, with no effect: `Err` names the
	/// fence it calls for.
	fn own_node_verdict(
		&self,
		registry: &crate::consensus::IncarnationRegistry,
	) -> Result<Ownership, (&'static str, Ownership)> {
		let Ok(local) = self.owner_ref() else {
			return Err((
				"UnexplainedOwnIdentity(StoreNotBound)",
				Ownership::Unexplained,
			));
		};
		if registry.retired.contains(&local.incarnation()) {
			return Err(("OwnerAuthoritySuperseded", Ownership::LocalSuperseded));
		}
		if registry.current == Some(local.incarnation()) {
			// A sticky fence stays: an Own event after supersession was seen
			// earlier changes nothing.
			if self.owner_authority_fence.get().is_some() {
				return Ok(Ownership::LocalSuperseded);
			}
			return Ok(Ownership::Own(SlabId::ZERO));
		}
		if registry.current.is_none() && registry.retired.is_empty() {
			// Not yet registered: nothing of this node is on the ring.
			return Ok(Ownership::Own(SlabId::ZERO));
		}
		Err(("UnexplainedOwnIdentity", Ownership::Unexplained))
	}

	/// Consume a begun comparison's registries before its index covers any
	/// delivery. The caller holds the epoch gate for write, which already
	/// orders this against every other mint.
	pub(super) fn classify_incoming_registries(
		&self,
		registries: &std::collections::BTreeMap<u32, crate::consensus::IncarnationRegistry>,
	) {
		let registry = self
			.owner_ref()
			.ok()
			.and_then(|local| registries.get(&local.node()).cloned())
			.unwrap_or_default();
		if let Err((reason, _)) = self.own_node_verdict(&registry) {
			self.mint_owner_fence(reason);
		}
	}

	/// Narrow the current committed tier view to this store's own pointers,
	/// as local ids. `None` means the view is not authoritative for this
	/// store (superseded or unexplained identity, already fenced): callers
	/// must not read absence from it as retirement.
	pub(crate) fn own_tier_view<'a>(
		&self,
		tiers: impl IntoIterator<Item = &'a crate::consensus::TieredSlab>,
		registries: &std::collections::BTreeMap<u32, crate::consensus::IncarnationRegistry>,
	) -> Option<Vec<TieredSlabPointer>> {
		match self.own_tier_view_from(tiers, registries, OwnerEvidence::Current) {
			OwnTierView::Authoritative(own) => Some(own),
			OwnTierView::NotAuthoritative | OwnTierView::Obsolete => None,
		}
	}

	/// Narrow a complete tier view read from `evidence`.
	pub(crate) fn own_tier_view_from<'a>(
		&self,
		tiers: impl IntoIterator<Item = &'a crate::consensus::TieredSlab>,
		registries: &std::collections::BTreeMap<u32, crate::consensus::IncarnationRegistry>,
		evidence: OwnerEvidence,
	) -> OwnTierView {
		let refused = |classified: Ownership| match classified {
			Ownership::Obsolete => OwnTierView::Obsolete,
			_ => OwnTierView::NotAuthoritative,
		};
		let local = match self.owner_ref() {
			Ok(local) => local,
			Err(_) => {
				return refused(self.fence_owner_authority(
					"UnexplainedOwnIdentity(StoreNotBound)",
					Ownership::Unexplained,
					evidence,
				));
			}
		};
		let registry = registries.get(&local.node()).cloned().unwrap_or_default();
		let own_current = match self.classify_own_node(&registry, evidence) {
			Ownership::Own(_) => registry.current == Some(local.incarnation()),
			other => return refused(other),
		};
		let mut own = Vec::new();
		for tier in tiers {
			let owner = tier.slab.owner();
			if owner.node() != local.node() || registry.retired.contains(&owner.incarnation()) {
				continue;
			}
			if owner != local || !own_current {
				return refused(self.fence_owner_authority(
					"UnexplainedOwnIdentity",
					Ownership::Unexplained,
					evidence,
				));
			}
			own.push(TieredSlabPointer::from_ring(tier));
		}
		OwnTierView::Authoritative(own)
	}

	/// Whether a newer authority already supersedes `evidence`, by the same
	/// orderings the manager's apply paths use.
	fn owner_evidence_is_superseded(&self, evidence: OwnerEvidence) -> bool {
		match evidence {
			OwnerEvidence::Current => false,
			OwnerEvidence::Applied(index) => self.observer_is_superseded(Some(index)),
			OwnerEvidence::SnapshotInstall(epoch) => self.capture_snapshot_epoch() != epoch,
		}
	}

	/// Mint the sticky fence, unless `evidence` is obsolete, and answer how the
	/// evidence classifies. The epoch gate orders this against a comparison
	/// starting. A delivery at or below the index that comparison read is
	/// covered by its registry and mints nothing; a newer one is evidence that
	/// comparison has not seen, so it fences at once, and the comparison
	/// completing cannot undo that.
	fn fence_owner_authority(
		&self,
		reason: &'static str,
		classified: Ownership,
		evidence: OwnerEvidence,
	) -> Ownership {
		let _epoch_gate = self
			.snapshot_epoch_gate
			.read()
			.unwrap_or_else(|e| e.into_inner());
		let covered = matches!(
			evidence,
			OwnerEvidence::Applied(index) if self.comparison_covers_index(index)
		);
		if covered || self.owner_evidence_is_superseded(evidence) {
			return Ownership::Obsolete;
		}
		self.mint_owner_fence(reason);
		classified
	}

	fn mint_owner_fence(&self, reason: &'static str) {
		if self.owner_authority_fence.set(reason).is_ok() {
			self.index_authority_unavailable
				.store(true, std::sync::atomic::Ordering::Release);
			tracing::error!(
				reason,
				"store is not its node's current incarnation on the control ring; local authority fenced"
			);
		}
	}

	/// The store's incarnation, for tests that follow it across reopens.
	#[cfg(test)]
	pub(crate) fn store_incarnation_for_test(
		&self,
	) -> Option<crate::storage::index::StoreIncarnation> {
		self.persistent_index
			.as_ref()
			.map(|index| index.store_incarnation())
	}

	/// Commit the durable high-water past `last` before any id up to it is
	/// used, and return `last + 1`. The bound is raised a lease ahead so most
	/// allocations need no redb commit. A failed commit issues nothing.
	pub(super) async fn cover_slab_ids_through(
		&self,
		state: &mut ManagerState,
		last: SlabId,
	) -> std::io::Result<SlabId> {
		let end = last
			.get()
			.checked_add(1)
			.map(SlabId::new)
			.ok_or_else(slab_ids_exhausted)?;
		if end <= state.durable_slab_id_bound {
			return Ok(end);
		}
		let Some(index) = &self.persistent_index else {
			return Err(Error::other(
				"slab ids cannot be issued without the durable storage index",
			));
		};
		let bound = end
			.get()
			.checked_add(SLAB_ID_LEASE)
			.ok_or_else(slab_ids_exhausted)?;
		index.persist_slab_id_high_water(bound).await?;
		state.durable_slab_id_bound = SlabId::new(bound);
		Ok(end)
	}
}

/// Every slab id a file name in a store directory carries: slab files, their
/// temporary and quarantine variants, and staged maintenance replacements.
fn slab_ids_named_by_file(name: &str) -> Vec<SlabId> {
	let hex16 = |text: &str| {
		let digits = text.get(..16)?;
		if !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
			return None;
		}
		u64::from_str_radix(digits, 16).ok().map(SlabId::new)
	};
	let mut ids = Vec::new();
	if let Some(body) = name.strip_prefix("maintenance-staged-") {
		ids.extend(hex16(body));
		ids.extend(body.get(17..).and_then(hex16));
	}
	let mut rest = name;
	while let Some(at) = rest.find("slab_") {
		rest = &rest[at + "slab_".len()..];
		ids.extend(hex16(rest));
	}
	ids
}

/// The exclusive bound on every slab id this store's own evidence names,
/// read without changing any file or row. Open validates the durable
/// high-water against it before any recovery step may rename, unlink or
/// rewrite that evidence.
pub(super) async fn slab_id_evidence_floor(
	file_ops: &dyn AsyncFileOps,
	dir: &Path,
	snapshot: Option<&crate::storage::index::PersistentIndexSnapshot>,
) -> std::io::Result<SlabId> {
	let mut highest: Option<SlabId> = None;
	for entry in file_ops.read_dir(dir).await? {
		if !entry.is_file {
			continue;
		}
		if let Some(name) = entry.file_name.to_str() {
			highest = highest.max(slab_ids_named_by_file(name).into_iter().max());
		}
	}
	if let Some(snapshot) = snapshot {
		let named = snapshot
			.record_rows
			.iter()
			.chain(snapshot.tiered_records.iter())
			.map(|row| row.location.slab_id)
			.chain(snapshot.lsm_runs.iter().map(|run| run.slab_id))
			.chain(
				snapshot
					.secondary_index_segments
					.iter()
					.map(|row| row.key.source_slab_id),
			)
			.chain(snapshot.tiered_slabs.iter().map(|pointer| pointer.slab_id))
			.chain(snapshot.retired_tiered_slabs.iter().copied())
			.chain(snapshot.lsm_install_intents.iter().flat_map(|intent| {
				intent
					.inputs
					.iter()
					.map(|input| input.slab_id)
					.chain(intent.commit.slabs_to_add.iter().map(|run| run.slab_id))
					.chain(intent.local_output_files.iter().map(|file| file.slab_id))
					.chain(
						intent
							.commit
							.sidecar_swaps
							.iter()
							.map(|swap| swap.output_slab_id),
					)
			}))
			.max();
		highest = highest.max(named);
	}
	match highest {
		None => Ok(SlabId::ZERO),
		Some(id) => id
			.get()
			.checked_add(1)
			.map(SlabId::new)
			.ok_or_else(slab_ids_exhausted),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn every_id_bearing_store_file_name_is_counted() {
		let named = |name: &str| slab_ids_named_by_file(name);
		let id = |n| SlabId::new(n);
		assert_eq!(named("slab_000000000000002a.wnd"), vec![id(0x2a)]);
		assert_eq!(
			named("slab_000000000000002a.wnd.retention.tmp"),
			vec![id(0x2a)]
		);
		assert_eq!(
			named("slab_000000000000002a.wnd.rehydrate.tmp"),
			vec![id(0x2a)]
		);
		assert_eq!(
			named("maintenance-staged-0000000000000001-0000000000000063.wnd"),
			vec![id(1), id(0x63)]
		);
		assert_eq!(
			named("lsm-quarantine-00000000000000ff-slab_0000000000000007.wnd"),
			vec![id(7)]
		);
		assert_eq!(
			named("compaction-temp-00000000000000ff-slab_0000000000000009.wnd"),
			vec![id(9)]
		);
		assert_eq!(named("slab_ffffffffffffffff.wnd"), vec![id(u64::MAX)]);
		assert!(named("storage_index.redb").is_empty());
		assert!(named("slab_xyz.wnd").is_empty());
	}
}
