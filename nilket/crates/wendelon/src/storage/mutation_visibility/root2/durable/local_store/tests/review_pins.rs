use super::super::store::FENCED;
use super::*;
fn gate() -> Arc<Gate> {
	Arc::new(Gate {
		entered: tokio::sync::Notify::new(),
		release: tokio::sync::Notify::new(),
	})
}
async fn prepared_sync_cut() -> usize {
	let temp = Temp::new();
	let (model, store, p) = admitted(&temp).await;
	let start = model.trace().len();
	store
		.prepared(p.owner, inputs().1.bundle.claims[0].0.clone())
		.await
		.unwrap();
	let mut calls = 0;
	for e in &model.trace()[start..] {
		if let Event::Submitted(op, _) = e {
			calls += 1;
			if *op == Op::FileSync {
				return calls;
			}
		}
	}
	panic!("missing Prepared barrier")
}
#[tokio::test]
async fn concurrent_parent_failure_and_retry_cannot_bypass_fence() {
	let cut = prepared_sync_cut().await;
	for after in [false, true] {
		for overlap in [false, true] {
			let temp = Temp::new();
			let (model, store, p) = admitted(&temp).await;
			let g = gate();
			model.fault(model.count() + cut, Fault::GateError(g.clone(), after));
			let first = {
				let s = store.clone();
				let owner = p.owner.clone();
				tokio::spawn(async move {
					s.prepared(owner, inputs().1.bundle.claims[0].0.clone())
						.await
				})
			};
			g.entered.notified().await;
			if overlap {
				let start = model.count();
				assert!(matches!(
					store
						.prepared(p.owner.clone(), inputs().1.bundle.claims[0].0.clone())
						.await,
					Err(StoreError::Conflict)
				));
				assert_eq!(model.count(), start);
			}
			g.release.notify_one();
			assert!(matches!(first.await.unwrap(), Err(StoreError::Io(_))));
			let start = model.count();
			assert!(matches!(
				store
					.prepared(p.owner.clone(), inputs().1.bundle.claims[0].0.clone())
					.await,
				Err(StoreError::Fenced)
			));
			assert_eq!(model.count(), start);
			assert!(store.observed(sha(&p.owner.bytes().unwrap()), FENCED).await);
		}
	}
}
#[tokio::test]
async fn pre_io_limit_and_validation_refusal_do_not_fence_parent() {
	let temp = Temp::new();
	let (model, store, p) = admitted(&temp).await;
	for limit in [false, true] {
		let start = model.count();
		assert!(
			store
				.parent_operation(p.owner.clone(), move |_, _| Box::pin(async move {
					if limit {
						Err::<(), _>(StoreError::Limit)
					} else {
						require(false)
					}
				}))
				.await
				.is_err()
		);
		assert_eq!(model.count(), start);
		assert!(!store.observed(sha(&p.owner.bytes().unwrap()), FENCED).await);
	}
	store
		.prepared(p.owner, inputs().1.bundle.claims[0].0.clone())
		.await
		.unwrap();
}
#[tokio::test]
async fn transfer_permits_are_held_across_all_chunks_under_contention() {
	let temp = Temp::new();
	let (model, store, _) = initialized(&temp).await;
	let path = temp.root.join("quarantine").join(hex(&[0x59; 16]));
	let h = Arc::new(store.io.open(&path, true, true).await.unwrap());
	let bytes = vec![0x59; 3 * IO_CHUNK + 17];
	let g = gate();
	model.fault(model.count() + 2, Fault::Gate(g.clone()));
	let writer = {
		let s = store.clone();
		let h = h.clone();
		let b = bytes.clone();
		tokio::spawn(async move { s.io.write(&h, 0, &b).await })
	};
	g.entered.notified().await;
	let buffers = store
		.io
		.buffers
		.clone()
		.try_acquire_many_owned(store.io.buffers.available_permits() as u32)
		.unwrap();
	let handles = store
		.io
		.handles
		.clone()
		.try_acquire_many_owned(store.io.handles.available_permits() as u32)
		.unwrap();
	g.release.notify_one();
	writer.await.unwrap().unwrap();
	drop(buffers);
	drop(handles);
	assert_eq!(std::fs::read(&path).unwrap(), bytes);
	let g = gate();
	model.fault(model.count() + 1, Fault::Gate(g.clone()));
	let reader = {
		let s = store.clone();
		let h = h.clone();
		let n = bytes.len();
		tokio::spawn(async move { s.io.read(&h, 0, n, &mut 0).await })
	};
	g.entered.notified().await;
	let buffers = store
		.io
		.buffers
		.clone()
		.try_acquire_many_owned(store.io.buffers.available_permits() as u32)
		.unwrap();
	let handles = store
		.io
		.handles
		.clone()
		.try_acquire_many_owned(store.io.handles.available_permits() as u32)
		.unwrap();
	g.release.notify_one();
	assert_eq!(reader.await.unwrap().unwrap(), bytes);
	drop(buffers);
	drop(handles);
	let handles = store
		.io
		.handles
		.clone()
		.try_acquire_many_owned(store.io.handles.available_permits() as u32)
		.unwrap();
	let start = model.count();
	assert!(matches!(
		store.io.write(&h, 0, b"refused").await,
		Err(StoreError::Limit)
	));
	assert_eq!(model.count(), start);
	assert_eq!(std::fs::read(path).unwrap(), bytes);
	drop(handles);
}
#[tokio::test]
async fn final_new_segment_early_name_preserves_empty_partial_header_and_header_only() {
	for length in [0usize, 1, 73, 143, 144, 215] {
		let temp = Temp::new();
		let (model, store, next) = super::isolation::first_segment_fixture(&temp).await;
		let previous = store.publisher.lock().await.inventory.digest;
		let mut raw = segment_header(store.id, 2, 4097, previous).unwrap();
		raw.extend(
			Allocation::Parent(next.clone())
				.encode(store.id, 4097, previous)
				.unwrap(),
		);
		let path = temp.root.join("allocations/0000000000000002");
		let h = model.open_regular(&path, true, true).await.unwrap();
		if length > 0 {
			model
				.write_plain_at(&h, 0, raw[..length].to_vec())
				.await
				.unwrap();
		}
		drop(h);
		drop(store);
		model.allow_early_names().unwrap();
		model.crash(true, Some(length)).unwrap();
		let (store, report) =
			LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
				.await
				.unwrap();
		assert_eq!(report.inventory.sequence, 4096);
		assert!(report.tail.is_some());
		assert!(matches!(
			store.allocate(next).await,
			Err(StoreError::Fenced)
		));
		assert!(
			std::fs::read_dir(temp.root.join("journals"))
				.unwrap()
				.next()
				.is_none()
		);
		drop(store);
		// The same incomplete segment becomes a middle discontinuity when a later name exists.
		std::fs::write(temp.root.join("allocations/0000000000000003"), &raw).unwrap();
		assert!(
			LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
				.await
				.is_err()
		);
	}
}
#[tokio::test]
async fn materialized_bytes_reduce_future_growth_and_checkpoint_pending_is_charged() {
	let temp = Temp::new();
	let (model, store, p) = admitted(&temp).await;
	let (_, data, cap, key, _) = inputs();
	store
		.prepared(p.owner.clone(), data.bundle.claims[0].0.clone())
		.await
		.unwrap();
	store
		.persist_content(p.owner.clone(), data.clone(), cap, key)
		.await
		.unwrap();
	let mut next = p.clone();
	next.owner.identity.sequence += 1;
	next.data = [0x65; 16];
	next.index = [0x66; 16];
	let (charged, debt) = {
		let s = store.publisher.lock().await;
		(s.inventory.charged, s.debt_bytes)
	};
	let required = (charged + debt + HEADROOM + next.credit.charge)
		.saturating_sub(store.io.materialized())
		+ FREE_FLOOR;
	model.space_bytes(required - 1);
	assert!(matches!(
		store.allocate(next.clone()).await,
		Err(StoreError::Limit)
	));
	model.space_bytes(required);
	store.allocate(next).await.unwrap();
	let size = store
		.publisher
		.lock()
		.await
		.inventory
		.checkpoint(store.id)
		.unwrap()
		.len() as u64;
	model.fault(model.count() + 1, Fault::Before);
	assert!(store.checkpoint([0x67; 16]).await.is_err());
	let s = store.publisher.lock().await;
	assert_eq!(s.debt_files, 1);
	assert_eq!(s.debt_bytes, size);
	drop(s);
	drop(store);
	let (store, report) =
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.unwrap();
	// Before-open failure leaves no pending filename; recovery charges only actual retained files.
	assert_eq!(report.debt_bytes, 0);
	assert!(store.io.materialized() > 0);
}
#[tokio::test]
async fn recovery_charges_pending_catalog_and_fences_bad_content_objects() {
	let temp = Temp::new();
	let (model, store, p) = admitted(&temp).await;
	let (_, data, cap, key, _) = inputs();
	store
		.prepared(p.owner.clone(), data.bundle.claims[0].0.clone())
		.await
		.unwrap();
	store
		.persist_content(p.owner.clone(), data, cap, key)
		.await
		.unwrap();
	let pending = temp
		.root
		.join(format!("CATALOG.pending.{}", hex(&[0x68; 16])));
	std::fs::write(&pending, vec![0x68; 137]).unwrap();
	let path = temp.root.join("index").join(hex(&p.index));
	let mut bytes = std::fs::read(&path).unwrap();
	bytes[0] ^= 1;
	std::fs::write(path, bytes).unwrap();
	drop(store);
	let (store, report) =
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.unwrap();
	assert_eq!(report.debt_bytes, 137);
	assert_eq!(report.debt_files, 1);
	assert_eq!(report.bad_objects, 1);
	assert!(store.observed(sha(&p.owner.bytes().unwrap()), FENCED).await);
	assert!(matches!(
		store
			.prepared(p.owner, inputs().1.bundle.claims[0].0.clone())
			.await,
		Err(StoreError::Fenced)
	));
}
#[tokio::test]
async fn barrier_traces_require_file_then_name_then_dependent_write() {
	use std::os::unix::fs::MetadataExt;
	let temp = Temp::new();
	let (model, store, p) = initialized(&temp).await;
	let start = model.trace().len();
	store.allocate(p.clone()).await.unwrap();
	let (_, data, cap, key, _) = inputs();
	store
		.prepared(p.owner.clone(), data.bundle.claims[0].0.clone())
		.await
		.unwrap();
	store
		.persist_content(p.owner.clone(), data, cap, key)
		.await
		.unwrap();
	store.checkpoint([0x79; 16]).await.unwrap();
	model.observed("all acknowledgements");
	let trace = model.trace();
	let trace = &trace[start..];
	let inode = |path: PathBuf| std::fs::metadata(path).unwrap().ino().to_string();
	let file_sync = |id: &str| {
		trace
			.iter()
			.position(|e| matches!(e,Event::Completed(Op::FileSync,s) if s==id))
			.unwrap()
	};
	let write = |id: &str| {
		trace
			.iter()
			.position(
				|e| matches!(e,Event::Submitted(Op::Write,s) if s.starts_with(&format!("{id}:"))),
			)
			.unwrap()
	};
	let dir_sync = |dir: &str| {
		trace.iter().position(|e|matches!(e,Event::Completed(Op::DirSync,s) if s==&(if dir.is_empty(){temp.root.clone()}else{temp.root.join(dir)}).display().to_string())).unwrap()
	};
	let allocation = inode(temp.root.join("allocations/0000000000000001"));
	let journal = inode(temp.root.join("journals").join(p.owner.locator().unwrap()));
	let data = inode(temp.root.join("data").join(hex(&p.data)));
	let index = inode(temp.root.join("index").join(hex(&p.index)));
	assert!(write(&allocation) < file_sync(&allocation));
	assert!(file_sync(&allocation) < write(&journal));
	assert!(write(&journal) < file_sync(&journal));
	assert!(file_sync(&journal) < dir_sync("journals"));
	assert!(dir_sync("journals") < write(&data));
	assert!(write(&data) < file_sync(&data));
	assert!(file_sync(&data) < dir_sync("data"));
	assert!(dir_sync("data") < write(&index));
	assert!(write(&index) < file_sync(&index));
	assert!(file_sync(&index) < dir_sync("index"));
	let catalog = inode(temp.root.join("CATALOG"));
	let rename = trace
		.iter()
		.position(|e| matches!(e, Event::Submitted(Op::Rename, _)))
		.unwrap();
	assert!(write(&catalog) < file_sync(&catalog));
	assert!(file_sync(&catalog) < rename);
	assert!(rename < dir_sync(""));
	assert!(
		dir_sync("")
			< trace
				.iter()
				.position(|e| matches!(e, Event::Observed(_)))
				.unwrap()
	);
}
async fn early_phase_setup(temp: &Temp, phase: u8) -> (Arc<Model>, Arc<LocalStore>, ParentEntry) {
	let (model, store, p) = admitted(temp).await;
	if phase == 1 {
		store
			.prepared(p.owner.clone(), inputs().1.bundle.claims[0].0.clone())
			.await
			.unwrap();
	}
	(model, store, p)
}
async fn early_phase_run(store: &Arc<LocalStore>, p: &ParentEntry, phase: u8) -> StoreResult<()> {
	let (_, data, cap, key, _) = inputs();
	match phase {
		0 => {
			store
				.prepared(p.owner.clone(), data.bundle.claims[0].0.clone())
				.await
		}
		1 => store.persist_content(p.owner.clone(), data, cap, key).await,
		_ => store.checkpoint([0x7a; 16]).await,
	}
}
#[tokio::test]
async fn early_names_at_prepared_content_and_checkpoint_cuts_preserve_known_inventory() {
	for phase in [0, 1, 2] {
		let temp = Temp::new();
		let (model, store, p) = early_phase_setup(&temp, phase).await;
		let before = model.count();
		early_phase_run(&store, &p, phase).await.unwrap();
		let calls = model.count() - before;
		for cut in 1..=calls {
			for after in [false, true] {
				for partial in [None, Some(17)] {
					let temp = Temp::new();
					let (model, store, p) = early_phase_setup(&temp, phase).await;
					model.fault(
						model.count() + cut,
						if after { Fault::After } else { Fault::Before },
					);
					let acknowledged = early_phase_run(&store, &p, phase).await.is_ok();
					drop(store);
					model.allow_early_names().unwrap();
					model.crash(true, partial).unwrap();
					let (store, report) = LocalStore::open_existing(
						temp.root.clone(),
						inputs().0,
						model.clone(),
						model.clone(),
					)
					.await
					.unwrap();
					assert_eq!(report.inventory.sequence, 1);
					if acknowledged && phase == 1 {
						assert_eq!(report.bad_objects, 0);
					}
					if report.bad_objects > 0 {
						assert!(store.observed(sha(&p.owner.bytes().unwrap()), FENCED).await);
					}
					if phase == 2 {
						let catalog = std::fs::read(temp.root.join("CATALOG")).unwrap();
						let (n, _) = Inventory::check_checkpoint(&catalog, store.id).unwrap();
						assert!(n == 0 || n == 1);
					}
					assert_eq!(
						std::fs::read(temp.root.join("allocations/0000000000000001"))
							.unwrap()
							.len(),
						656
					);
				}
			}
		}
	}
}
#[tokio::test]
async fn ownership_wait_rereads_published_fence_before_any_job() {
	for already_fenced in [false, true] {
		let temp = Temp::new();
		let (model, store, p) = admitted(&temp).await;
		let owners = store.owners.lock().await;
		let mut publisher = store.publisher.lock().await;
		let key = sha(&p.owner.bytes().unwrap());
		if already_fenced {
			publisher
				.inventory
				.parents
				.get_mut(&key)
				.unwrap()
				.observations |= FENCED;
		}
		let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
		let before = model.count();
		let candidate = {
			let s = store.clone();
			let o = p.owner.clone();
			let ran = ran.clone();
			tokio::spawn(async move {
				s.parent_operation(o, move |_, _| {
					Box::pin(async move {
						ran.store(true, std::sync::atomic::Ordering::Relaxed);
						Ok(())
					})
				})
				.await
			})
		};
		// This current-thread executor runs the owned candidate until its first
		// held mutex blocks it. The inflight permit proves the candidate entered.
		for _ in 0..8 {
			if store.inflight.available_permits() == 7 {
				break;
			}
			tokio::task::yield_now().await;
		}
		assert_eq!(store.inflight.available_permits(), 7);
		drop(publisher);
		// Mutex FIFO ensures an old fence-before-ownership implementation has
		// already cached the unfenced entry before this fence is published.
		let mut publisher = store.publisher.lock().await;
		publisher
			.inventory
			.parents
			.get_mut(&key)
			.unwrap()
			.observations |= FENCED;
		drop(publisher);
		drop(owners);
		assert!(matches!(candidate.await.unwrap(), Err(StoreError::Fenced)));
		assert!(!ran.load(std::sync::atomic::Ordering::Relaxed));
		assert_eq!(model.count(), before);
	}
}
#[tokio::test]
async fn rotation_file_and_name_barriers_precede_prepared_dependent_write() {
	let temp = Temp::new();
	let (model, store, mut next) = super::isolation::first_segment_fixture(&temp).await;
	let (data, _, _) = super::super::super::loader::tests::raw_variant(false, Some(4097));
	let (claim, reservation) = &data.bundle.claims[0];
	next.owner.identity = claim.manifest.identity;
	next.owner.claim = claim.digest().unwrap();
	next.owner.reservation = *reservation;
	next.credit = Credit::freeze(
		&data,
		128,
		1,
		NodeRef2 {
			node: claim.original.lease.holder,
			incarnation: claim.original.incarnation,
		},
	)
	.unwrap();
	let start = model.trace().len();
	store.allocate(next.clone()).await.unwrap();
	store
		.prepared(next.owner.clone(), claim.clone())
		.await
		.unwrap();
	let trace = model.trace();
	let trace = &trace[start..];
	let file = trace
		.iter()
		.position(|e| matches!(e, Event::Completed(Op::FileSync, _)))
		.unwrap();
	let name=trace.iter().position(|e|matches!(e,Event::Completed(Op::DirSync,s) if s==&temp.root.join("allocations").display().to_string())).unwrap();
	let dependent = trace
		.iter()
		.position(|e| matches!(e,Event::Submitted(Op::Open,s) if s.contains("/journals/")))
		.unwrap();
	assert!(file < name);
	assert!(name < dependent);
	assert_eq!(writes(trace)[0], (0, 656));
}

fn physical_size(root: &Path) -> u64 {
	std::fs::read_dir(root)
		.unwrap()
		.map(|e| {
			let e = e.unwrap();
			let m = e.metadata().unwrap();
			if m.is_dir() {
				physical_size(&e.path())
			} else {
				m.len()
			}
		})
		.sum()
}
#[tokio::test]
async fn index_replacement_removes_old_materialization_before_uncertain_rename() {
	for fault in [None, Some(Fault::Before), Some(Fault::After)] {
		let temp = Temp::new();
		let (model, store, p) = admitted(&temp).await;
		let (_, data, cap, key, _) = inputs();
		store
			.prepared(p.owner.clone(), data.bundle.claims[0].0.clone())
			.await
			.unwrap();
		store
			.persist_content(p.owner.clone(), data, cap, key)
			.await
			.unwrap();
		let index = temp.root.join("index").join(hex(&p.index));
		let mut bytes = std::fs::read(&index).unwrap();
		bytes[0] ^= 1;
		std::fs::write(&index, bytes).unwrap();
		assert_eq!(store.io.materialized(), physical_size(&temp.root));
		if let Some(f) = fault.clone() {
			// Independently calibrate the rename cut on another physical store.
			let baseline = Temp::new();
			let (m, s, q) = admitted(&baseline).await;
			let (_, d, c, k, _) = inputs();
			s.prepared(q.owner.clone(), d.bundle.claims[0].0.clone())
				.await
				.unwrap();
			s.persist_content(q.owner.clone(), d, c, k).await.unwrap();
			let path = baseline.root.join("index").join(hex(&q.index));
			let mut b = std::fs::read(&path).unwrap();
			b[0] ^= 1;
			std::fs::write(path, b).unwrap();
			let start = m.trace().len();
			let (_, _, c, k, _) = inputs();
			s.repair_index(q.owner, c, k, [0x6b; 16], [0x6c; 16])
				.await
				.unwrap();
			let mut calls = 0;
			let mut rename = None;
			for e in &m.trace()[start..] {
				if let Event::Submitted(op, _) = e {
					calls += 1;
					if *op == Op::Rename {
						rename = Some(calls);
						break;
					}
				}
			}
			model.fault(model.count() + rename.unwrap(), f);
		}
		let (_, _, cap, key, _) = inputs();
		let repaired = store
			.repair_index(p.owner.clone(), cap, key, [0x6b; 16], [0x6c; 16])
			.await;
		if fault.is_none() {
			assert!(repaired.unwrap().durable);
			assert_eq!(store.io.materialized(), physical_size(&temp.root));
		} else {
			assert!(repaired.is_err());
			assert!(store.io.materialized() <= physical_size(&temp.root));
		}
	}
}
