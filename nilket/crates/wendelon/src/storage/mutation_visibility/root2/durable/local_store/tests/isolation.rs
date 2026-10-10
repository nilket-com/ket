use super::*;
fn second() -> (UnverifiedData2, ParentEntry) {
	let (data, _, _) =
		crate::storage::mutation_visibility::root2::durable::loader::tests::raw_variant(
			false,
			Some(908),
		);
	let (id, _, _, _, mut entry) = inputs();
	let (c, r) = &data.bundle.claims[0];
	entry.owner.identity = c.manifest.identity;
	entry.owner.claim = c.digest().unwrap();
	entry.owner.reservation = *r;
	entry.data = [0x43; 16];
	entry.index = [0x44; 16];
	entry.credit = Credit::freeze(
		&data,
		128,
		1,
		NodeRef2 {
			node: c.original.lease.holder,
			incarnation: c.original.incarnation,
		},
	)
	.unwrap();
	assert!(entry.owner.cluster == id.cluster);
	(data, entry)
}
#[tokio::test]
async fn damaged_chain_does_not_hide_independent_parent() {
	for damage in ["header", "tail", "middle", "owner", "claimed_length"] {
		let temp = Temp::new();
		let (model, store, a) = admitted(&temp).await;
		let (_, data, _, _, _) = inputs();
		let (data_b, b) = second();
		store.allocate(b.clone()).await.unwrap();
		store
			.prepared(a.owner.clone(), data.bundle.claims[0].0.clone())
			.await
			.unwrap();
		store
			.prepared(b.owner.clone(), data_b.bundle.claims[0].0.clone())
			.await
			.unwrap();
		let path = temp.root.join("journals").join(a.owner.locator().unwrap());
		let mut bytes = std::fs::read(&path).unwrap();
		match damage {
			"header" => bytes[120] ^= 1,
			"tail" => bytes.extend([0x71; 17]),
			"middle" => bytes[500] ^= 1,
			"owner" => bytes[90] ^= 1,
			_ => bytes[281..285].copy_from_slice(&u32::MAX.to_le_bytes()),
		}
		std::fs::write(&path, bytes).unwrap();
		drop(store);
		let (store, report) =
			LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
				.await
				.unwrap();
		assert_eq!(report.inventory.sequence, 2);
		let mut work = 0;
		let good = scan_chain(
			&store.io,
			&temp.root.join("journals").join(b.owner.locator().unwrap()),
			store.id,
			&b,
			&mut work,
		)
		.await
		.unwrap();
		assert!(good.damaged.is_none());
		assert_eq!(good.count, 1);
		assert_eq!(good.claim.unwrap().manifest.identity.sequence, 908);
		let bad = scan_chain(&store.io, &path, store.id, &a, &mut work)
			.await
			.unwrap();
		assert!(bad.damaged.is_some());
	}
}
#[tokio::test]
async fn duplicate_owner_across_scan_batches_invalidates_all_copies_in_either_order() {
	for first_b in [false, true] {
		let temp = Temp::new();
		let (model, store, a) = admitted(&temp).await;
		let (_, data, _, _, _) = inputs();
		let (data_b, b) = second();
		store.allocate(b.clone()).await.unwrap();
		store
			.prepared(a.owner.clone(), data.bundle.claims[0].0.clone())
			.await
			.unwrap();
		store
			.prepared(b.owner.clone(), data_b.bundle.claims[0].0.clone())
			.await
			.unwrap();
		let (from, to) = if first_b { (&b, &a) } else { (&a, &b) };
		std::fs::copy(
			temp.root
				.join("journals")
				.join(from.owner.locator().unwrap()),
			temp.root.join("journals").join(to.owner.locator().unwrap()),
		)
		.unwrap();
		drop(store);
		let (store, report) =
			LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
				.await
				.unwrap();
		assert_eq!(report.inventory.sequence, 2);
		assert!(
			store
				.observed(
					sha(&a.owner.bytes().unwrap()),
					crate::storage::mutation_visibility::root2::durable::local_store::store::FENCED
				)
				.await
		);
		assert!(
			store
				.observed(
					sha(&b.owner.bytes().unwrap()),
					crate::storage::mutation_visibility::root2::durable::local_store::store::FENCED
				)
				.await
		);
	}
}
#[tokio::test]
async fn checkpoint_prefix_remains_valid_while_later_allocation_appends_continue() {
	let temp = Temp::new();
	let (model, store, _) = admitted(&temp).await;
	let gate = Arc::new(Gate {
		entered: tokio::sync::Notify::new(),
		release: tokio::sync::Notify::new(),
	});
	model.fault(model.count() + 1, Fault::Gate(gate.clone()));
	let maintenance = {
		let store = store.clone();
		tokio::spawn(async move { store.checkpoint([0x69; 16]).await })
	};
	gate.entered.notified().await;
	let (_, b) = second();
	store.allocate(b).await.unwrap();
	gate.release.notify_one();
	maintenance.await.unwrap().unwrap();
	drop(store);
	model.crash(false, None).unwrap();
	let (store, report) =
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.unwrap();
	assert_eq!(report.inventory.sequence, 2);
	let cp = std::fs::read(temp.root.join("CATALOG")).unwrap();
	assert_eq!(Inventory::check_checkpoint(&cp, store.id).unwrap().0, 1);
}
#[tokio::test]
async fn allocation_new_segment_name_is_durable_before_dependents() {
	let temp = Temp::new();
	let (model, store, entry) = initialized(&temp).await;
	// Independent synthetic retained physical allocations (no authority result) fill one segment.
	for n in 1..=4096u64 {
		let mut p = entry.clone();
		p.owner.identity.sequence = n;
		p.data[..8].copy_from_slice(&n.to_le_bytes());
		p.data[15] = 0x71;
		p.index[..8].copy_from_slice(&n.to_le_bytes());
		p.index[15] = 0x72;
		store.allocate(p).await.unwrap();
	}
	let mut next = entry.clone();
	next.owner.identity.sequence = 4097;
	next.data = [0x73; 16];
	next.index = [0x74; 16];
	let before = model.trace().len();
	store.allocate(next).await.unwrap();
	let trace = model.trace();
	assert_eq!(writes(&trace[before..]), vec![(0, 656)]);
	assert_eq!(
		trace[before..]
			.iter()
			.filter(|e| matches!(e, Event::Submitted(Op::FileSync, _)))
			.count(),
		1
	);
	assert_eq!(
		trace[before..]
			.iter()
			.filter(|e| matches!(e, Event::Submitted(Op::DirSync, _)))
			.count(),
		1
	);
	drop(store);
	model.crash(false, None).unwrap();
	let (_, report) =
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.unwrap();
	assert_eq!(report.inventory.sequence, 4097);
}
pub(super) async fn first_segment_fixture(
	temp: &Temp,
) -> (Arc<Model>, Arc<LocalStore>, ParentEntry) {
	use std::io::Write;
	let (model, store, entry) = initialized(temp).await;
	let mut inv = Inventory::default();
	let path = temp.root.join("allocations/0000000000000001");
	let mut file = std::fs::OpenOptions::new()
		.append(true)
		.open(&path)
		.unwrap();
	for n in 1..=4096u64 {
		let mut p = entry.clone();
		p.owner.identity.sequence = n;
		p.data[..8].copy_from_slice(&n.to_le_bytes());
		p.data[15] = 0x71;
		p.index[..8].copy_from_slice(&n.to_le_bytes());
		p.index[15] = 0x72;
		let a = Allocation::Parent(p);
		let raw = a.encode(store.id, n, inv.digest).unwrap();
		file.write_all(&raw).unwrap();
		inv.add(a, n, sha(&raw)).unwrap();
	}
	drop(file);
	let h = model.open_regular(&path, false, true).await.unwrap();
	model.fsync(&h).await.unwrap();
	drop(h);
	store.publisher.lock().await.inventory = inv;
	let mut next = entry;
	next.owner.identity.sequence = 4097;
	next.data = [0x73; 16];
	next.index = [0x74; 16];
	(model, store, next)
}
#[tokio::test]
async fn allocation_segment_creation_cuts_never_allow_dependent_work_before_name_barrier() {
	let baseline = Temp::new();
	let (model, store, next) = first_segment_fixture(&baseline).await;
	let before = model.count();
	store.allocate(next).await.unwrap();
	let calls = model.count() - before;
	for at in 1..=calls {
		for fault in [Fault::Before, Fault::After] {
			let temp = Temp::new();
			let (model, store, next) = first_segment_fixture(&temp).await;
			model.fault(model.count() + at, fault);
			let acknowledged = store.allocate(next).await.is_ok();
			drop(store);
			model.crash(false, None).unwrap();
			let (_, report) = LocalStore::open_existing(
				temp.root.clone(),
				inputs().0,
				model.clone(),
				model.clone(),
			)
			.await
			.unwrap();
			assert!(report.inventory.sequence == 4096 || report.inventory.sequence == 4097);
			if acknowledged {
				assert_eq!(report.inventory.sequence, 4097)
			}
			assert!(
				std::fs::read_dir(temp.root.join("journals"))
					.unwrap()
					.next()
					.is_none()
			);
		}
	}
}
#[tokio::test]
async fn allocation_final_partial_first_record_keeps_prior_segment_but_middle_gap_refuses() {
	use std::io::Write;
	let temp = Temp::new();
	let (model, store, next) = first_segment_fixture(&temp).await;
	let previous = store.publisher.lock().await.inventory.digest;
	let mut raw = segment_header(store.id, 2, 4097, previous).unwrap();
	let record = Allocation::Parent(next)
		.encode(store.id, 4097, previous)
		.unwrap();
	raw.extend(&record[..71]);
	let path = temp.root.join("allocations/0000000000000002");
	std::fs::File::create(&path)
		.unwrap()
		.write_all(&raw)
		.unwrap();
	drop(store);
	let (store, report) =
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.unwrap();
	assert_eq!(report.inventory.sequence, 4096);
	assert!(report.tail.is_some());
	drop(store);
	std::fs::write(temp.root.join("allocations/0000000000000003"), raw).unwrap();
	assert!(
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.is_err()
	);
}
