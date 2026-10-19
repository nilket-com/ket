use super::*;
use super::{
	recovery::{RecoveryReport, recover},
	scan::{ChainState, scan_chain},
};
use crate::storage::mutation_visibility::ProofKeyProvider;
use crate::storage::mutation_visibility::root2::durable::loader::{
	LoaderCapability2, load_content2,
};
pub(super) struct Publisher {
	pub(super) inventory: Inventory,
	pub(super) fenced: bool,
	pub(super) debt_bytes: u64,
	pub(super) debt_files: usize,
	pub(super) prospective: BTreeMap<Digest2, ParentEntry>,
}
pub(super) struct LocalStore {
	pub(super) root: PathBuf,
	pub(super) id: ContainerId,
	pub(super) io: Io,
	pub(super) _lock: LockedFile,
	pub(super) publisher: Mutex<Publisher>,
	pub(super) inflight: Arc<Semaphore>,
	pub(super) owners: Mutex<BTreeSet<Vec<u8>>>,
	pub(super) recovery: Arc<Semaphore>,
}
/// A prospective reservation exists before authority resolution. Drop retains credit:
/// resolving or abandoning uncertain authority work requires explicit inspection.
pub(super) struct Prospective {
	pub(super) entry: ParentEntry,
	pub(super) store: Arc<LocalStore>,
}
const PREPARED_DURABLE: u8 = 1;
pub(super) const CONTENT_VERIFIED: u8 = 2;
pub(super) const HARD_FENCED: u8 = 4;
pub(super) const INDEX_FENCED: u8 = 8;
pub(super) const FENCED: u8 = HARD_FENCED | INDEX_FENCED;
enum ParentAccess {
	Append,
	VerifiedIndexRepair,
}
impl LocalStore {
	pub(super) async fn observed(&self, key: Digest2, flag: u8) -> bool {
		self.publisher
			.lock()
			.await
			.inventory
			.parents
			.get(&key)
			.is_some_and(|p| p.observations & flag != 0)
	}
	pub(super) async fn observe(&self, key: Digest2, flag: u8) -> StoreResult<()> {
		let mut state = self.publisher.lock().await;
		let p = state
			.inventory
			.parents
			.get_mut(&key)
			.ok_or(StoreError::Unavailable)?;
		p.observations |= flag;
		Ok(())
	}

	pub(super) async fn create(
		root: PathBuf,
		id: ContainerId,
		files: Arc<dyn AsyncFileOps>,
		adapter: Arc<dyn LocalAdapter>,
	) -> StoreResult<Arc<Self>> {
		tokio::spawn(async move {
			id.write(&mut Vec::new())?;
			let io = Io::new(files, adapter);
			match io.list(&root, 0).await {
				Ok(entries) => require(entries.is_empty())?,
				Err(StoreError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
					io.adapter.mkdir(&root).await?
				}
				Err(e) => return Err(e.into()),
			}
			io.directory_sync(root.parent().ok_or(StoreError::Unavailable)?)
				.await?;
			for dir in DIRS {
				io.adapter.mkdir(&root.join(dir)).await?;
				io.directory_sync(&root).await?;
			}
			let lock = io.open(&root.join("LOCK"), true, true).await?;
			io.adapter.lock(&lock.handle).await?;
			let lock = LockedFile::new(lock, io.adapter.clone());
			io.sync(lock.file()).await?;
			io.directory_sync(&root).await?;
			let inventory = Inventory::default();
			let empty = inventory.checkpoint(id)?;
			io.immutable(&root.join("CATALOG"), &empty).await?;
			io.immutable(
				&root.join("allocations/0000000000000001"),
				&segment_header(id, 1, 1, [0; 32])?,
			)
			.await?;
			io.immutable(&root.join("INIT"), &init(id, sha(&empty))?)
				.await?;
			Ok(Arc::new(Self {
				root,
				id,
				io,
				_lock: lock,
				publisher: Mutex::new(Publisher {
					inventory,
					fenced: false,
					debt_bytes: 0,
					debt_files: 0,
					prospective: BTreeMap::new(),
				}),
				inflight: Arc::new(Semaphore::new(8)),
				owners: Mutex::new(BTreeSet::new()),
				recovery: Arc::new(Semaphore::new(1)),
			}))
		})
		.await
		.map_err(|_| StoreError::Unavailable)?
	}
	pub(super) async fn open_existing(
		root: PathBuf,
		id: ContainerId,
		files: Arc<dyn AsyncFileOps>,
		adapter: Arc<dyn LocalAdapter>,
	) -> StoreResult<(Arc<Self>, RecoveryReport)> {
		tokio::spawn(async move {
			let io = Io::new(files, adapter);
			let lock = io.open(&root.join("LOCK"), false, true).await?;
			io.adapter.lock(&lock.handle).await?;
			let lock = LockedFile::new(lock, io.adapter.clone());
			let _buffer = io
				.buffers
				.clone()
				.try_acquire_many_owned(160 * 1024 * 1024)
				.map_err(|_| StoreError::Limit)?;
			let report = recover(&io, &root, id).await?;
			io.set_materialized(report.materialized);
			// A live reopen does not prove earlier name/sync results. Establish shared
			// metadata durability before exposing a writable instance; create nothing.
			for name in ["INIT", "CATALOG"] {
				let h = io.open(&root.join(name), false, true).await?;
				io.sync(&h).await?;
			}
			let segments = io.list(&root.join("allocations"), SEGMENTS).await?;
			for entry in segments {
				let h = io
					.open(&root.join("allocations").join(entry.name), false, true)
					.await?;
				io.sync(&h).await?;
			}
			io.directory_sync(&root.join("allocations")).await?;
			io.directory_sync(&root).await?;
			let mut report = report;
			for key in &report.unavailable_owners {
				if let Some(p) = report.inventory.parents.get_mut(key) {
					p.observations |= HARD_FENCED;
				}
			}
			let summary = report.report();
			let store = Arc::new(Self {
				root,
				id,
				io,
				_lock: lock,
				publisher: Mutex::new(Publisher {
					inventory: report.inventory,
					fenced: report.tail.is_some(),
					debt_bytes: report.debt_bytes,
					debt_files: report.debt_files,
					prospective: BTreeMap::new(),
				}),
				inflight: Arc::new(Semaphore::new(8)),
				owners: Mutex::new(BTreeSet::new()),
				recovery: Arc::new(Semaphore::new(1)),
			});
			Ok((store, summary))
		})
		.await
		.map_err(|_| StoreError::Unavailable)?
	}
	pub(super) async fn append_allocation(&self, a: Allocation) -> StoreResult<()> {
		let mut state = self.publisher.lock().await;
		if state.fenced {
			return Err(StoreError::Fenced);
		}
		// Validate in bounded logarithmic map operations before IO; no catalog serialization.
		let seq = state.inventory.sequence + 1;
		let raw = a.encode(self.id, seq, state.inventory.digest)?;
		let charge = a.charge();
		let space = self.io.adapter.space(&self.root).await?;
		let pending = state
			.prospective
			.values()
			.map(|p| p.credit.charge)
			.sum::<u64>();
		let replacing = match &a {
			Allocation::Parent(p) => state
				.prospective
				.get(&super::prospective::prospective_key(&p.owner)?)
				.map_or(0, |p| p.credit.charge),
			_ => 0,
		};
		let promised = state.inventory.charged + state.debt_bytes + HEADROOM + pending - replacing;
		if promised.checked_add(charge).is_none_or(|v| v > QUOTA)
			|| space.bytes < (charge + promised).saturating_sub(self.io.materialized()) + FREE_FLOOR
			|| space.inodes < 4
		{
			return Err(StoreError::Limit);
		}
		match &a {
			Allocation::Parent(p) => {
				if state.inventory.parents.len() >= PARENTS {
					return Err(StoreError::Limit);
				}
				require(
					!state
						.inventory
						.parents
						.contains_key(&sha(&p.owner.bytes()?))
						&& state.inventory.objects.binary_search(&p.data).is_err()
						&& state.inventory.objects.binary_search(&p.index).is_err(),
				)?;
			}
			Allocation::Grant(g) => {
				if state.inventory.grants.len() >= GRANTS {
					return Err(StoreError::Limit);
				}
				require(
					!state.inventory.grants.contains_key(&g.digest)
						&& state.inventory.objects.binary_search(&g.object).is_err(),
				)?;
			}
		}
		// Mark uncertain *before* submission. Caller cancellation never drops this owned task.
		state.fenced = true;
		let segment = (seq - 1) / SEGMENT_RECORDS + 1;
		let record = (seq - 1) % SEGMENT_RECORDS;
		let path = self
			.root
			.join("allocations")
			.join(format!("{segment:016x}"));
		let h = self
			.io
			.open(&path, segment > 1 && record == 0, true)
			.await?;
		if segment > 1 && record == 0 {
			let mut b = segment_header(self.id, segment, seq, state.inventory.digest)?;
			b.extend(&raw);
			self.io.write_sync(&h, 0, &b).await?;
			self.io
				.directory_sync(&self.root.join("allocations"))
				.await?;
		} else {
			require(self.io.adapter.length(&h.handle).await? == 144 + record * 512)?;
			self.io.write_sync(&h, 144 + record * 512, &raw).await?;
		}
		state.inventory.add(a, seq, sha(&raw))?;
		state.fenced = false;
		Ok(())
	}
	#[cfg(test)]
	pub(super) async fn allocate(self: &Arc<Self>, entry: ParentEntry) -> StoreResult<()> {
		let store = self.clone();
		tokio::spawn(async move { store.append_allocation(Allocation::Parent(entry)).await })
			.await
			.map_err(|_| StoreError::Unavailable)?
	}
	pub(super) async fn archive(
		self: &Arc<Self>,
		archive: UnverifiedGrantArchive2,
		obj: [u8; 16],
	) -> StoreResult<()> {
		let store = self.clone();
		tokio::spawn(async move {
			require(
				archive.generation == store.id.generation && archive.cluster == store.id.cluster,
			)?;
			let inner = archive.encode()?;
			let raw = object(store.id, 3, obj, &inner)?;
			let entry = GrantEntry {
				digest: sha(&inner),
				object: obj,
				bytes: raw.len() as u32,
				charge: raw.len() as u64 + 512,
			};
			store.append_allocation(Allocation::Grant(entry)).await?;
			let result=store.io.immutable(&store.root.join("grants").join(hex(&obj)),&raw).await;
            if matches!(&result,Err(StoreError::Io(e)) if matches!(e.raw_os_error(),Some(libc::ENOSPC|libc::EDQUOT))){store.publisher.lock().await.fenced=true;}result
		})
		.await
		.map_err(|_| StoreError::Unavailable)?
	}
	pub(super) async fn checkpoint(self: &Arc<Self>, pending: [u8; 16]) -> StoreResult<()> {
		let store = self.clone();
		tokio::spawn(async move {
			id_name(&hex(&pending))?;
            let _buffer = store
				.io
				.buffers
				.clone()
				.try_acquire_many_owned(48 * 1024 * 1024)
				.map_err(|_| StoreError::Limit)?;
			let _stream = store
				.recovery
				.clone()
				.try_acquire_owned()
				.map_err(|_| StoreError::Limit)?;
			let snapshot = {
				let state = store.publisher.lock().await;
				if state.fenced {
					return Err(StoreError::Fenced);
				}
				state.inventory.clone()
			};
			let raw = snapshot.checkpoint(store.id)?;
			drop(snapshot);
			{
				let mut s = store.publisher.lock().await;
				if s.debt_files >= TEMP_FILES || s.inventory.charged + HEADROOM + s.debt_bytes + raw.len() as u64 > QUOTA {
					return Err(StoreError::Limit);
				}
				s.debt_files += 1;
                s.debt_bytes += raw.len() as u64;
			}
			let name = format!("CATALOG.pending.{}", hex(&pending));
			id_name(&hex(&pending))?;
			let from = store.root.join(name);
            let result=async {
                let h=store.io.open(&from,true,true).await?;store.io.write_sync(&h,0,&raw).await?;
                let old=store.io.open(&store.root.join("CATALOG"),false,false).await?;
                let old_bytes=store.io.adapter.length(&old.handle).await?;
                // Removing the old catalog before uncertain rename underestimates
                // materialization on a before-error, preserving conservative growth credit.
                store.io.forget_materialized(old_bytes);
                store.io.files.rename(&from,&store.root.join("CATALOG")).await?;store.io.directory_sync(&store.root).await?;Ok(())
            }.await;
            if matches!(&result,Err(StoreError::Io(e)) if matches!(e.raw_os_error(),Some(libc::ENOSPC|libc::EDQUOT))){store.publisher.lock().await.fenced=true;}
            if result.is_ok(){let mut s=store.publisher.lock().await;s.debt_files-=1;s.debt_bytes-=raw.len() as u64;}result
		})
		.await
		.map_err(|_| StoreError::Unavailable)?
	}
	pub(super) async fn parent_operation<T: Send + 'static>(
		self: &Arc<Self>,
		owner: JournalIdentity2,
		job: impl FnOnce(
			Arc<Self>,
			ParentEntry,
		)
			-> std::pin::Pin<Box<dyn std::future::Future<Output = StoreResult<T>> + Send>>
		+ Send
		+ 'static,
	) -> StoreResult<T> {
		tokio::spawn(self.clone().run_parent(owner, ParentAccess::Append, job))
			.await
			.map_err(|_| StoreError::Unavailable)?
	}
	/// Exclusively owned index repair may inspect an index-only recovery fence.
	/// Data/journal/ownership/uncertain-I/O fences still refuse before verification.
	pub(super) async fn index_repair_operation<T: Send + 'static>(
		self: &Arc<Self>,
		owner: JournalIdentity2,
		job: impl FnOnce(
			Arc<Self>,
			ParentEntry,
		)
			-> std::pin::Pin<Box<dyn std::future::Future<Output = StoreResult<T>> + Send>>
		+ Send
		+ 'static,
	) -> StoreResult<T> {
		tokio::spawn(
			self.clone()
				.run_parent(owner, ParentAccess::VerifiedIndexRepair, job),
		)
		.await
		.map_err(|_| StoreError::Unavailable)?
	}
	async fn run_parent<T: Send + 'static>(
		self: Arc<Self>,
		owner: JournalIdentity2,
		access: ParentAccess,
		job: impl FnOnce(
			Arc<Self>,
			ParentEntry,
		)
			-> std::pin::Pin<Box<dyn std::future::Future<Output = StoreResult<T>> + Send>>
		+ Send
		+ 'static,
	) -> StoreResult<T> {
		let _buffers = self
			.io
			.buffers
			.clone()
			.try_acquire_many_owned(16 * 1024 * 1024)
			.map_err(|_| StoreError::Limit)?;
		let _permit = self
			.inflight
			.clone()
			.try_acquire_owned()
			.map_err(|_| StoreError::Limit)?;
		let key = owner.bytes()?;
		{
			let mut owners = self.owners.lock().await;
			if !owners.insert(key.clone()) {
				return Err(StoreError::Conflict);
			}
		}
		// Ownership precedes the authoritative fence read. A failed predecessor
		// publishes its fence before releasing ownership under this same ordering.
		let result = async {
			let entry = self
				.publisher
				.lock()
				.await
				.inventory
				.parents
				.get(&sha(&key))
				.ok_or(StoreError::Unavailable)?
				.clone();
			require(entry.owner == owner)?;
			let forbidden = match access {
				ParentAccess::Append => FENCED,
				ParentAccess::VerifiedIndexRepair => HARD_FENCED,
			};
			if entry.observations & forbidden != 0 {
				// An admission refusal never escalates an index-only fence.
				return Err(StoreError::Fenced);
			}
			let result = job(self.clone(), entry).await;
			if matches!(&result, Err(StoreError::Io(_) | StoreError::Fenced)) {
				let _ = self.observe(sha(&key), HARD_FENCED).await;
			}
			if matches!(&result,Err(StoreError::Io(e)) if matches!(e.raw_os_error(),Some(libc::ENOSPC|libc::EDQUOT))) {
				self.publisher.lock().await.fenced = true;
			}
			result
		}
		.await;
		self.owners.lock().await.remove(&key);
		result
	}
	pub(super) async fn prepared(
		self: &Arc<Self>,
		owner: JournalIdentity2,
		claim: ClaimProjection2,
	) -> StoreResult<()> {
		self.parent_operation(owner, move |store, entry| {
			Box::pin(async move {
				let record = UnverifiedJournal2 {
					owner: entry.owner.clone(),
					sequence: 1,
					previous: [0; 32],
					step: UnverifiedJournalStep2::Prepared(claim),
				};
				ChainState::default().validate(&entry, &record)?;
				let mut raw = journal_header(store.id, &entry.owner)?;
				raw.extend(record.encode()?);
				let path = store.root.join("journals").join(entry.owner.locator()?);
				let mut work = 0;
				match scan_chain(&store.io, &path, store.id, &entry, &mut work).await {
					Ok(chain) => {
						require(chain.damaged.is_none())?;
						let h = store.io.open(&path, false, true).await?;
						require(
							chain
								.identical(&store.io, &h, &record.step, &mut work)
								.await?,
						)?;
						store.io.sync(&h).await?;
						store
							.io
							.directory_sync(&store.root.join("journals"))
							.await?;
					}
					Err(StoreError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
						store.io.immutable(&path, &raw).await?
					}
					Err(e) => return Err(e),
				}
				store
					.observe(sha(&entry.owner.bytes()?), PREPARED_DURABLE)
					.await?;
				Ok(())
			})
		})
		.await
	}
	pub(super) async fn append(
		self: &Arc<Self>,
		owner: JournalIdentity2,
		step: UnverifiedJournalStep2,
	) -> StoreResult<()> {
		self.parent_operation(owner, move |store, entry| {
			Box::pin(async move {
				let path = store.root.join("journals").join(entry.owner.locator()?);
				let mut work = 0;
				let chain = scan_chain(&store.io, &path, store.id, &entry, &mut work).await?;
				if chain.damaged.is_some() {
					return Err(StoreError::Fenced);
				}
				let h = store.io.open(&path, false, true).await?;
				if chain.identical(&store.io, &h, &step, &mut work).await? {
					store.io.sync(&h).await?;
					store
						.io
						.directory_sync(&store.root.join("journals"))
						.await?;
					store
						.observe(sha(&entry.owner.bytes()?), PREPARED_DURABLE)
						.await?;
					return Ok(());
				}
				if !store
					.observed(sha(&entry.owner.bytes()?), PREPARED_DURABLE)
					.await
				{
					store.io.sync(&h).await?;
					store
						.io
						.directory_sync(&store.root.join("journals"))
						.await?;
					store
						.observe(sha(&entry.owner.bytes()?), PREPARED_DURABLE)
						.await?;
				}
				if matches!(
					step,
					UnverifiedJournalStep2::LocalChildDurable(_)
						| UnverifiedJournalStep2::ProjectionComplete { .. }
				) {
					let _prerequisite_buffers = store
						.io
						.buffers
						.clone()
						.try_acquire_many_owned(64 * 1024 * 1024)
						.map_err(|_| StoreError::Limit)?;
					require(
						store
							.observed(sha(&entry.owner.bytes()?), CONTENT_VERIFIED)
							.await,
					)?;
					let data = store
						.io
						.bounded_file(
							&store.root.join("data").join(hex(&entry.data)),
							entry.credit.data as usize,
							&mut work,
						)
						.await?;
					let parsed = UnverifiedData2::decode(object_inner(
						&data,
						store.id,
						1,
						entry.data,
						entry.credit.data as usize,
					)?)?;
					let expected = build_index(store.id, &entry, &parsed, &data)?;
					let index = store
						.io
						.bounded_file(
							&store.root.join("index").join(hex(&entry.index)),
							entry.credit.index as usize,
							&mut work,
						)
						.await?;
					require(index == expected)?;
				}

				let record = UnverifiedJournal2 {
					owner: entry.owner.clone(),
					sequence: chain.count + 1,
					previous: chain.previous,
					step,
				};
				chain.validate(&entry, &record)?;
				let raw = record.encode()?;
				if chain.endpoint + raw.len() as u64 > entry.credit.journal as u64 {
					return Err(StoreError::Limit);
				}
				store.io.write_sync(&h, chain.endpoint, &raw).await
			})
		})
		.await
	}
	pub(super) async fn persist_content<P: ProofKeyProvider + Send + Sync + 'static>(
		self: &Arc<Self>,
		owner: JournalIdentity2,
		data: UnverifiedData2,
		cap: LoaderCapability2,
		provider: Arc<P>,
	) -> StoreResult<()> {
		self.parent_operation(owner, move |store, entry| {
			Box::pin(async move {
				let _buffer = store
					.io
					.buffers
					.clone()
					.try_acquire_many_owned(120 * 1024 * 1024)
					.map_err(|_| StoreError::Limit)?;
				entry.credit.matches(&data)?;
				require(
					data.bundle.claims[0].0.manifest.identity == entry.owner.identity
						&& data.bundle.claims[0].1 == entry.owner.reservation,
				)?;
				let mut work = 0;
				let path = store.root.join("journals").join(entry.owner.locator()?);
				let state = scan_chain(&store.io, &path, store.id, &entry, &mut work).await?;
				require(state.damaged.is_none() && state.count > 0)?;
				if !store
					.observed(sha(&entry.owner.bytes()?), PREPARED_DURABLE)
					.await
				{
					let h = store.io.open(&path, false, true).await?;
					store.io.sync(&h).await?;
					store
						.io
						.directory_sync(&store.root.join("journals"))
						.await?;
					store
						.observe(sha(&entry.owner.bytes()?), PREPARED_DURABLE)
						.await?;
				}
				let inner = data.encode()?;
				let raw = object(store.id, 1, entry.data, &inner)?;
				require(raw.len() == entry.credit.data as usize)?;
				// Verification precedes the staged index. No local proof key/capability factory.
				load_content2(data.clone(), &cap, provider.as_ref())?;
				let index = build_index(store.id, &entry, &data, &raw)?;
				store
					.io
					.immutable(&store.root.join("data").join(hex(&entry.data)), &raw)
					.await?;
				store
					.io
					.immutable(&store.root.join("index").join(hex(&entry.index)), &index)
					.await?;
				store
					.observe(sha(&entry.owner.bytes()?), CONTENT_VERIFIED)
					.await?;
				Ok(())
			})
		})
		.await
	}
}
pub(super) fn build_index(
	id: ContainerId,
	p: &ParentEntry,
	data: &UnverifiedData2,
	raw: &[u8],
) -> StoreResult<Vec<u8>> {
	let c = &data.bundle.claims[0].0;
	let mut records = Vec::new();
	for (physical, r) in data.bundle.provenance.references().iter().enumerate() {
		require(r.parent == 0)?;
		let child = &c.manifest.children[r.child as usize];
		let v = &child.versions[r.row as usize];
		let stamp = c
			.stamps
			.iter()
			.find(|s| s.child == r.child && s.row == r.row)
			.ok_or(StoreError::Unavailable)?;
		let index = UnverifiedIndex2 {
			identity: p.owner.identity,
			child: r.child,
			row: r.row,
			hlc: stamp.hlc,
			claim: p.owner.claim,
			reservation: p.owner.reservation,
			binding: child_binding2(c, id.cluster, p.owner.reservation, r.child)?,
			object: p.data,
			object_digest: sha(raw),
			offset: 85,
			length: (raw.len() - 117) as u32,
			physical_row: physical as u32,
			tombstone: v.tombstone,
			partition: child.partition,
		};
		index.check_original_binding(&data.bundle)?;
		records.push((index.key()?, index.encode()?));
	}
	records.sort_by_key(|r| r.0);
	require(
		records.len() == p.credit.versions as usize && records.windows(2).all(|r| r[0].0 < r[1].0),
	)?;
	let inner = records.into_iter().flat_map(|(_, b)| b).collect::<Vec<_>>();
	let index = object(id, 2, p.index, &inner)?;
	require(index.len() == p.credit.index as usize)?;
	Ok(index)
}
