use super::super::loader::{LoaderCapability2, load_content2};
use super::store::{LocalStore, build_index};
use super::*;
use crate::storage::mutation_visibility::ProofKeyProvider;
pub(super) struct RepairResult {
	pub(super) candidate: Vec<u8>,
	pub(super) durable: bool,
	_buffer: OwnedSemaphorePermit,
}
impl LocalStore {
	pub(super) async fn repair_index<P: ProofKeyProvider + Send + Sync + 'static>(
		self: &Arc<Self>,
		owner: JournalIdentity2,
		cap: LoaderCapability2,
		provider: Arc<P>,
		evidence: [u8; 16],
		pending: [u8; 16],
	) -> StoreResult<RepairResult> {
		self.parent_operation(owner, move |store, entry| {
			Box::pin(async move {
				let buffer = store
					.io
					.buffers
					.clone()
					.try_acquire_many_owned(120 * 1024 * 1024)
					.map_err(|_| StoreError::Limit)?;
				let mut work = 0;
				let path = store.root.join("data").join(hex(&entry.data));
				let raw = store
					.io
					.bounded_file(&path, entry.credit.data as usize, &mut work)
					.await?;
				require(raw.len() == entry.credit.data as usize)?;
				let inner =
					object_inner(&raw, store.id, 1, entry.data, entry.credit.data as usize)?;
				let data = UnverifiedData2::decode(inner)?;
				entry.credit.matches(&data)?;
				require(
					data.bundle.claims[0].0.manifest.identity == entry.owner.identity
						&& data.bundle.claims[0].1 == entry.owner.reservation,
				)?;
				load_content2(data.clone(), &cap, provider.as_ref())?;
				let candidate = build_index(store.id, &entry, &data, &raw)?;
				let data_handle = store.io.open(&path, false, true).await?;
				store.io.sync(&data_handle).await?;
				store.io.directory_sync(&store.root.join("data")).await?;
				drop(data_handle);
				let index_path = store.root.join("index").join(hex(&entry.index));
				let old = match store
					.io
					.bounded_file(&index_path, entry.credit.index as usize, &mut work)
					.await
				{
					Ok(old) => Some(old),
					Err(StoreError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => None,
					Err(e) => return Err(e),
				};
				if old.as_ref() == Some(&candidate) {
					let index_handle = store.io.open(&index_path, false, true).await?;
					store.io.sync(&index_handle).await?;
					store.io.directory_sync(&store.root.join("index")).await?;
					store
						.observe(sha(&entry.owner.bytes()?), super::store::CONTENT_VERIFIED)
						.await?;
					return Ok(RepairResult {
						candidate,
						durable: true,
						_buffer: buffer,
					});
				}
				id_name(&hex(&evidence))?;
				id_name(&hex(&pending))?;
				let extra = candidate.len() as u64 + old.as_ref().map_or(0, |b| b.len() as u64);
				let files = 1 + usize::from(old.is_some());
				{
					let mut state = store.publisher.lock().await;
					if state.debt_files + files > TEMP_FILES
						|| state.inventory.charged + state.debt_bytes + HEADROOM + extra > QUOTA
					{
						return Ok(RepairResult {
							candidate,
							durable: false,
							_buffer: buffer,
						});
					}
					// Keep reservations even on uncertain completion; reopen inspects actual debt.
					state.debt_files += files;
					state.debt_bytes += extra;
				}
				let old_bytes = old.as_ref().map_or(0, |b| b.len() as u64);
				if let Some(old) = old {
					store
						.io
						.immutable(&store.root.join("quarantine").join(hex(&evidence)), &old)
						.await?;
				}
				let from = store.root.join("index").join(format!(
					"{}.pending.{}",
					hex(&entry.index),
					hex(&pending)
				));
				let h = store.io.open(&from, true, true).await?;
				store.io.write_sync(&h, 0, &candidate).await?;
				// Remove replacement bytes before uncertain rename; underestimation is conservative.
				store.io.forget_materialized(old_bytes);
				store.io.files.rename(&from, &index_path).await?;
				store.io.directory_sync(&store.root.join("index")).await?;
				let mut state = store.publisher.lock().await;
				state.debt_files -= 1;
				state.debt_bytes -= candidate.len() as u64;
				drop(state);
				store
					.observe(sha(&entry.owner.bytes()?), super::store::CONTENT_VERIFIED)
					.await?;
				Ok(RepairResult {
					candidate,
					durable: true,
					_buffer: buffer,
				})
			})
		})
		.await
	}
	pub(super) async fn unsupported_transform(&self) -> StoreResult<()> {
		Err(StoreError::Unavailable)
	}
}
