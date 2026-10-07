use super::store::{LocalStore, Prospective};
use super::*;
/// Only local capacity and exact bytes are bound here. No Reserve/lookup RPC is issued.
impl LocalStore {
	pub(super) async fn prospective(
		self: &Arc<Self>,
		entry: ParentEntry,
		data: UnverifiedData2,
	) -> StoreResult<Prospective> {
		let store = self.clone();
		tokio::spawn(async move {
			let _buffer = store
				.io
				.buffers
				.clone()
				.try_acquire_many_owned(96 * 1024 * 1024)
				.map_err(|_| StoreError::Limit)?;
			store.id.owner(&entry.owner)?;
			entry.credit.matches(&data)?;
			let (claim, _) = &data.bundle.claims[0];
			require(
				claim.manifest.identity == entry.owner.identity
					&& claim.digest()? == entry.owner.claim,
			)?;
			let key = prospective_key(&entry.owner)?;
			let space = store.io.adapter.space(&store.root).await?;
			let mut s = store.publisher.lock().await;
			if s.fenced {
				return Err(StoreError::Fenced);
			}
			if let Some(old) = s.prospective.get(&key) {
				require(old == &entry)?;
				return Ok(Prospective {
					entry,
					store: store.clone(),
				});
			}
			if s.prospective.len() >= 8
				|| s.inventory.parents.len() + s.prospective.len() >= PARENTS
			{
				return Err(StoreError::Limit);
			}
			require(
				!s.inventory
					.parents
					.contains_key(&sha(&entry.owner.bytes()?)),
			)?;
			let pending = s.prospective.values().map(|p| p.credit.charge).sum::<u64>();
			let total =
				s.inventory.charged + s.debt_bytes + HEADROOM + pending + entry.credit.charge;
			if total > QUOTA
				|| space.bytes < total.saturating_sub(store.io.materialized()) + FREE_FLOOR
				|| space.inodes < 3 * (s.prospective.len() as u64 + 1)
			{
				return Err(StoreError::Limit);
			}
			require(
				s.inventory.objects.binary_search(&entry.data).is_err()
					&& s.inventory.objects.binary_search(&entry.index).is_err()
					&& s.prospective.values().all(|p| {
						p.data != entry.data
							&& p.index != entry.data
							&& p.data != entry.index
							&& p.index != entry.index
					}),
			)?;
			s.prospective.insert(key, entry.clone());
			drop(s);
			Ok(Prospective { entry, store })
		})
		.await
		.map_err(|_| StoreError::Unavailable)?
	}
	pub(super) async fn resolve_prospective(
		self: &Arc<Self>,
		known: JournalIdentity2,
		reservation: Log2,
		data: UnverifiedData2,
	) -> StoreResult<ParentEntry> {
		let store = self.clone();
		tokio::spawn(async move {
			let key = prospective_key(&known)?;
			let mut entry = store
				.publisher
				.lock()
				.await
				.prospective
				.get(&key)
				.ok_or(StoreError::Unavailable)?
				.clone();
			entry.owner.reservation = reservation;
			entry.owner.bytes()?;
			entry.credit.matches(&data)?;
			let (claim, actual) = &data.bundle.claims[0];
			require(
				*actual == reservation
					&& claim.manifest.identity == entry.owner.identity
					&& claim.digest()? == entry.owner.claim,
			)?;
			store
				.append_allocation(Allocation::Parent(entry.clone()))
				.await?;
			// Release only after the complete logged declaration has replaced prospective credit.
			store.publisher.lock().await.prospective.remove(&key);
			Ok(entry)
		})
		.await
		.map_err(|_| StoreError::Unavailable)?
	}
}
pub(super) fn prospective_key(owner: &JournalIdentity2) -> StoreResult<Digest2> {
	let mut raw = owner.bytes()?;
	raw.truncate(137);
	Ok(sha(&raw))
}
