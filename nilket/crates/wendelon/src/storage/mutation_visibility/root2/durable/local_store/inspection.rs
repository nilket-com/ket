use super::*;
use super::{scan::scan_chain, store::LocalStore};
#[derive(Debug, PartialEq, Eq)]
pub(super) enum PhysicalBytes {
	Missing,
	Structural,
	Unavailable,
}
pub(super) struct PhysicalParent {
	pub(super) owner: JournalIdentity2,
	pub(super) data: PhysicalBytes,
	pub(super) index: PhysicalBytes,
	pub(super) records: u64,
	pub(super) endpoint: u64,
	pub(super) damaged_suffix: Option<(u64, u64)>,
	pub(super) fenced: bool,
}
impl LocalStore {
	pub(super) async fn physical_parent(
		self: &Arc<Self>,
		owner: JournalIdentity2,
	) -> StoreResult<PhysicalParent> {
		let store = self.clone();
		tokio::spawn(async move {
			let _stream = store
				.recovery
				.clone()
				.try_acquire_owned()
				.map_err(|_| StoreError::Limit)?;
			let _buffers = store
				.io
				.buffers
				.clone()
				.try_acquire_many_owned(160 * 1024 * 1024)
				.map_err(|_| StoreError::Limit)?;
			let key = owner.bytes()?;
			let digest = sha(&key);
			let entry = store
				.publisher
				.lock()
				.await
				.inventory
				.parents
				.get(&digest)
				.ok_or(StoreError::Unavailable)?
				.clone();
			require(entry.owner == owner)?;
			if !store.owners.lock().await.insert(key.clone()) {
				return Err(StoreError::Conflict);
			}
			let result = async {
				let mut work = 0;
				let raw = optional_file(
					&store.io,
					&store.root.join("data").join(hex(&entry.data)),
					entry.credit.data as usize,
					&mut work,
				)
				.await?;
				let parsed = raw.as_ref().map(|raw| -> StoreResult<UnverifiedData2> {
					require(raw.len() == entry.credit.data as usize)?;
					let inner =
						object_inner(raw, store.id, 1, entry.data, entry.credit.data as usize)?;
					let data = UnverifiedData2::decode(inner)?;
					entry.credit.matches(&data)?;
					require(
						data.bundle.claims[0].0.digest()? == owner.claim
							&& data.bundle.claims[0].1 == owner.reservation
							&& data.bundle.claims[0].0.manifest.identity == owner.identity,
					)?;
					Ok(data)
				});
				let data = match &parsed {
					None => PhysicalBytes::Missing,
					Some(Ok(_)) => PhysicalBytes::Structural,
					_ => PhysicalBytes::Unavailable,
				};
				let raw_index = optional_file(
					&store.io,
					&store.root.join("index").join(hex(&entry.index)),
					entry.credit.index as usize,
					&mut work,
				)
				.await?;
				let index = match (&raw_index, &parsed, &raw) {
					(None, _, _) => PhysicalBytes::Missing,
					(Some(index), Some(Ok(data)), Some(raw)) => {
						if super::store::build_index(store.id, &entry, data, raw)
							.is_ok_and(|candidate| candidate == *index)
						{
							PhysicalBytes::Structural
						} else {
							PhysicalBytes::Unavailable
						}
					}
					_ => PhysicalBytes::Unavailable,
				};
				let chain = match scan_chain(
					&store.io,
					&store.root.join("journals").join(owner.locator()?),
					store.id,
					&entry,
					&mut work,
				)
				.await
				{
					Ok(s) => s,
					Err(StoreError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
						super::scan::ChainState::default()
					}
					Err(e) => return Err(e),
				};
				Ok(PhysicalParent {
					owner,
					data,
					index,
					records: chain.count,
					endpoint: chain.endpoint,
					damaged_suffix: chain.damaged,
					fenced: store.observed(digest, super::store::FENCED).await,
				})
			}
			.await;
			store.owners.lock().await.remove(&key);
			result
		})
		.await
		.map_err(|_| StoreError::Unavailable)?
	}
}
async fn optional_file(
	io: &Io,
	path: &Path,
	cap: usize,
	work: &mut u64,
) -> StoreResult<Option<Vec<u8>>> {
	match io.bounded_file(path, cap, work).await {
		Ok(raw) => Ok(Some(raw)),
		Err(StoreError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
		Err(e) => Err(e),
	}
}
