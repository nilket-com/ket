use super::*;
#[tokio::test]
async fn directory_sync_does_not_substitute_for_file_sync() {
	let temp = Temp::new();
	let model = Model::new(temp.root.clone());
	model.mkdir(&temp.root).await.unwrap();
	model.fsync_dir(&temp.parent).await.unwrap();
	let path = temp.root.join("file");
	let h = model.open_regular(&path, true, true).await.unwrap();
	model
		.write_plain_at(&h, 0, b"independent expected bytes".to_vec())
		.await
		.unwrap();
	model.fsync_dir(&temp.root).await.unwrap();
	drop(h);
	model.crash(false, None).unwrap();
	assert_eq!(std::fs::read(path).unwrap(), b"");
}
#[tokio::test]
async fn replacement_rename_preserves_file_identity_without_directory_data_sync() {
	let temp = Temp::new();
	let model = Model::new(temp.root.clone());
	model.mkdir(&temp.root).await.unwrap();
	model.fsync_dir(&temp.parent).await.unwrap();
	for (name, bytes) in [
		("old", b"old stable bytes".as_slice()),
		("pending", b"new stable bytes".as_slice()),
	] {
		let h = model
			.open_regular(&temp.root.join(name), true, true)
			.await
			.unwrap();
		model.write_plain_at(&h, 0, bytes.to_vec()).await.unwrap();
		model.fsync(&h).await.unwrap();
	}
	model.fsync_dir(&temp.root).await.unwrap();
	model
		.rename(&temp.root.join("pending"), &temp.root.join("old"))
		.await
		.unwrap();
	model.crash(false, None).unwrap();
	assert_eq!(
		std::fs::read(temp.root.join("old")).unwrap(),
		b"old stable bytes"
	);
	model
		.rename(&temp.root.join("pending"), &temp.root.join("old"))
		.await
		.unwrap();
	model.fsync_dir(&temp.root).await.unwrap();
	model.crash(false, None).unwrap();
	assert_eq!(
		std::fs::read(temp.root.join("old")).unwrap(),
		b"new stable bytes"
	);
}
#[tokio::test]
async fn prepared_without_bundle_retains_reserved_parent() {
	let temp = Temp::new();
	let (model, store, entry) = admitted(&temp).await;
	let (_, data, _, _, _) = inputs();
	store
		.prepared(entry.owner.clone(), data.bundle.claims[0].0.clone())
		.await
		.unwrap();
	model.observed("Prepared");
	drop(store);
	model.crash(false, None).unwrap();
	let (reopened, report) =
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.unwrap();
	assert_eq!(report.inventory.sequence, 1);
	assert_eq!(report.damaged_parents, 0);
	assert!(!temp.root.join("data").join(hex(&entry.data)).exists());
	let mut work = 0;
	let chain = scan_chain(
		&reopened.io,
		&temp
			.root
			.join("journals")
			.join(entry.owner.locator().unwrap()),
		reopened.id,
		&entry,
		&mut work,
	)
	.await
	.unwrap();
	assert_eq!(chain.count, 1);
	assert_eq!(
		chain.claim.unwrap().encode().unwrap(),
		data.bundle.claims[0].0.encode().unwrap()
	);
	assert_eq!(report.inventory.charged, entry.credit.charge);
}
#[tokio::test]
async fn initialization_requires_last_durable_marker() {
	let baseline = Temp::new();
	let (model, store, _) = initialized(&baseline).await;
	let calls = model.count();
	drop(store);
	for at in 1..=calls {
		for fault in [Fault::Before, Fault::After] {
			let temp = Temp::new();
			let model = Model::new(temp.root.clone());
			model.fault(at, fault);
			let result =
				LocalStore::create(temp.root.clone(), inputs().0, model.clone(), model.clone())
					.await;
			let success = result.is_ok();
			drop(result);
			model.crash(false, None).unwrap();
			let opened = LocalStore::open_existing(
				temp.root.clone(),
				inputs().0,
				model.clone(),
				model.clone(),
			)
			.await;
			if success {
				assert!(opened.is_ok(), "init cut {at}")
			} else if let Ok((_, report)) = opened {
				assert_eq!(report.inventory.sequence, 0);
				assert!(temp.root.join("INIT").exists());
			}
		}
	}
}
#[tokio::test]
async fn absent_marker_never_means_empty_and_foreign_generation_refuses_before_write() {
	let temp = Temp::new();
	let (model, store, _) = initialized(&temp).await;
	drop(store);
	std::fs::remove_file(temp.root.join("INIT")).unwrap();
	assert!(
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.is_err()
	);
	assert!(!temp.root.join("INIT").exists());
	let temp = Temp::new();
	let (model, store, _) = initialized(&temp).await;
	drop(store);
	let mut id = inputs().0;
	id.generation = [0x91; 16];
	let before = model.trace().len();
	assert!(
		LocalStore::open_existing(temp.root.clone(), id, model.clone(), model.clone())
			.await
			.is_err()
	);
	assert!(writes(&model.trace()[before..]).is_empty());
}
#[tokio::test]
async fn allocation_record_precedes_all_dependent_io_and_append_is_fixed_size() {
	let temp = Temp::new();
	let (model, store, entry) = initialized(&temp).await;
	let before = model.trace().len();
	store.allocate(entry.clone()).await.unwrap();
	let trace = model.trace();
	assert_eq!(writes(&trace[before..]), vec![(144, 512)]);
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
		0
	);
	assert!(
		trace[before..]
			.iter()
			.all(|e| !matches!(e,Event::Submitted(_,p) if p.contains("CATALOG")))
	);
	let (_, data, _, _, _) = inputs();
	store
		.prepared(entry.owner.clone(), data.bundle.claims[0].0.clone())
		.await
		.unwrap();
	assert_eq!(
		std::fs::metadata(temp.root.join("allocations/0000000000000001"))
			.unwrap()
			.len(),
		656
	);
}
#[tokio::test]
async fn allocation_torn_tail_preserves_prior_inventory_and_fences_admission() {
	let temp = Temp::new();
	let (model, store, entry) = admitted(&temp).await;
	drop(store);
	let path = temp.root.join("allocations/0000000000000001");
	use std::io::Write;
	std::fs::OpenOptions::new()
		.append(true)
		.open(&path)
		.unwrap()
		.write_all(&[0x53; 71])
		.unwrap();
	let (store, report) =
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.unwrap();
	assert_eq!(report.inventory.sequence, 1);
	assert!(report.tail.is_some());
	assert_eq!(report.inventory.charged, entry.credit.charge);
	assert!(matches!(
		store.allocate(entry).await,
		Err(StoreError::Fenced)
	));
	assert_eq!(std::fs::metadata(path).unwrap().len(), 727);
}
#[tokio::test]
async fn allocation_middle_defect_and_unexpected_tail_dependent_are_container_unavailable() {
	for unexpected in [false, true] {
		let temp = Temp::new();
		let (model, store, _) = admitted(&temp).await;
		drop(store);
		if unexpected {
			use std::io::Write;
			std::fs::OpenOptions::new()
				.append(true)
				.open(temp.root.join("allocations/0000000000000001"))
				.unwrap()
				.write_all(&[0x53; 71])
				.unwrap();
			std::fs::write(temp.root.join("data").join(hex(&[0x71; 16])), [0]).unwrap();
		} else {
			let path = temp.root.join("allocations/0000000000000001");
			let mut bytes = std::fs::read(&path).unwrap();
			bytes[300] ^= 1;
			std::fs::write(path, bytes).unwrap();
		}
		assert!(
			LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
				.await
				.is_err()
		);
	}
}
#[tokio::test]
async fn prepared_file_and_name_precede_data_and_index_barriers() {
	let temp = Temp::new();
	let (model, store, entry) = admitted(&temp).await;
	let (_, data, cap, key, _) = inputs();
	store
		.prepared(entry.owner.clone(), data.bundle.claims[0].0.clone())
		.await
		.unwrap();
	let before = model.trace().len();
	store
		.persist_content(entry.owner.clone(), data.clone(), cap, key)
		.await
		.unwrap();
	let trace = model.trace();
	assert_eq!(
		trace[before..]
			.iter()
			.filter(|e| matches!(e, Event::Submitted(Op::FileSync, _)))
			.count(),
		2
	);
	assert_eq!(
		trace[before..]
			.iter()
			.filter(|e| matches!(e, Event::Submitted(Op::DirSync, _)))
			.count(),
		2
	);
	let raw = std::fs::read(temp.root.join("data").join(hex(&entry.data))).unwrap();
	assert_eq!(raw.len(), entry.credit.data as usize);
	assert_eq!(
		object_inner(&raw, store.id, 1, entry.data, entry.credit.data as usize).unwrap(),
		data.encode().unwrap()
	);
	let index = std::fs::read(temp.root.join("index").join(hex(&entry.index))).unwrap();
	assert_eq!(index.len(), 420);
	let idx =
		UnverifiedIndex2::decode(object_inner(&index, store.id, 2, entry.index, 420).unwrap())
			.unwrap();
	assert_eq!(idx.offset, 85);
	assert_eq!(idx.object_digest, sha(&raw));
	assert_eq!(idx.hlc, data.bundle.claims[0].0.stamps[0].hlc);
	drop(store);
	model.crash(false, None).unwrap();
	assert!(
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.is_ok()
	);
}
