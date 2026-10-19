use super::*;
use crate::storage::mutation_visibility::root2::durable::local_store::store::{
	FENCED, HARD_FENCED, INDEX_FENCED,
};

async fn reopened(
	temp: &Temp,
	shape: &str,
) -> (
	Arc<Model>,
	Arc<LocalStore>,
	ParentEntry,
	Vec<u8>,
	Option<Vec<u8>>,
) {
	let (model, store, p, _) = progress::content(temp).await;
	let path = temp.root.join("index").join(hex(&p.index));
	let original = std::fs::read(&path).unwrap();
	let old = match shape {
		"valid" => Some(original.clone()),
		"missing" => None,
		"torn" => Some(original[..17].to_vec()),
		"empty" => Some(Vec::new()),
		_ => {
			let mut bytes = original.clone();
			bytes[0] ^= 1;
			Some(bytes)
		}
	};
	if let Some(bytes) = &old {
		std::fs::write(&path, bytes).unwrap();
		let h = model.open_regular(&path, false, true).await.unwrap();
		model.fsync(&h).await.unwrap();
		drop(h);
	} else {
		std::fs::remove_file(&path).unwrap();
	}
	model.fsync_dir(&temp.root.join("index")).await.unwrap();
	drop(store);
	let (store, report) =
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.unwrap();
	assert_eq!(
		report.bad_objects,
		usize::from(!matches!(shape, "valid" | "missing"))
	);
	(model, store, p, original, old)
}

#[tokio::test]
async fn reopened_invalid_torn_empty_index_can_be_verified_and_repaired_with_controls() {
	for shape in ["invalid", "torn", "empty", "valid", "missing"] {
		let temp = Temp::new();
		let (model, store, p, original, old) = reopened(&temp, shape).await;
		let key_id = sha(&p.owner.bytes().unwrap());
		let damaged = !matches!(shape, "valid" | "missing");
		assert_eq!(store.observed(key_id, INDEX_FENCED).await, damaged);
		assert!(!store.observed(key_id, HARD_FENCED).await);
		if damaged {
			let before = model.count();
			let (_, data, _, _, _) = inputs();
			assert!(matches!(
				store
					.prepared(p.owner.clone(), data.bundle.claims[0].0.clone())
					.await,
				Err(StoreError::Fenced)
			));
			assert_eq!(model.count(), before);
			assert!(!store.observed(key_id, HARD_FENCED).await);
		}
		let (_, data, cap, key, _) = inputs();
		let result = store
			.repair_index(p.owner.clone(), cap, key, [0x71; 16], [0x72; 16])
			.await
			.unwrap();
		assert!(result.durable);
		assert_eq!(result.candidate, original);
		assert_eq!(
			std::fs::read(temp.root.join("index").join(hex(&p.index))).unwrap(),
			original
		);
		assert!(!store.observed(key_id, FENCED).await);
		if damaged {
			assert_eq!(
				std::fs::read(temp.root.join("quarantine").join(hex(&[0x71; 16]))).unwrap(),
				old.unwrap()
			);
		} else {
			assert!(
				std::fs::read_dir(temp.root.join("quarantine"))
					.unwrap()
					.next()
					.is_none()
			);
		}
		store
			.prepared(p.owner.clone(), data.bundle.claims[0].0.clone())
			.await
			.unwrap();
		drop(result);
		drop(store);
		model.crash(false, None).unwrap();
		let (store, report) =
			LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
				.await
				.unwrap();
		assert_eq!(report.bad_objects, 0);
		assert!(!store.observed(key_id, FENCED).await);
	}
}

#[tokio::test]
async fn reopened_bad_or_missing_data_refuses_index_repair_without_replacement() {
	for data_missing in [false, true] {
		for shape in ["invalid", "missing", "valid"] {
			let temp = Temp::new();
			let (model, store, p, _, old) = reopened(&temp, shape).await;
			drop(store);
			let path = temp.root.join("data").join(hex(&p.data));
			if data_missing {
				std::fs::remove_file(path).unwrap();
			} else {
				std::fs::write(path, b"incomplete data").unwrap();
			}
			let (store, _) = LocalStore::open_existing(
				temp.root.clone(),
				inputs().0,
				model.clone(),
				model.clone(),
			)
			.await
			.unwrap();
			let before = model.count();
			let (_, _, cap, key, _) = inputs();
			assert!(
				store
					.repair_index(p.owner.clone(), cap, key, [0x71; 16], [0x72; 16])
					.await
					.is_err()
			);
			if !data_missing {
				assert_eq!(model.count(), before);
			}
			assert_eq!(
				std::fs::read(temp.root.join("index").join(hex(&p.index))).ok(),
				old
			);
			assert!(
				std::fs::read_dir(temp.root.join("quarantine"))
					.unwrap()
					.next()
					.is_none()
			);
			assert!(
				store
					.observed(sha(&p.owner.bytes().unwrap()), HARD_FENCED)
					.await
			);
		}
	}
}

#[tokio::test]
async fn reopened_journal_and_duplicate_fences_refuse_index_repair() {
	for damage in ["tail", "header", "duplicate"] {
		let temp = Temp::new();
		let (model, store, p, _, old) = reopened(&temp, "invalid").await;
		let path = temp.root.join("journals").join(p.owner.locator().unwrap());
		if damage == "duplicate" {
			let mut b = p.clone();
			b.owner.identity.sequence = 908;
			b.data = [0x43; 16];
			b.index = [0x44; 16];
			store.allocate(b.clone()).await.unwrap();
			std::fs::copy(
				&path,
				temp.root.join("journals").join(b.owner.locator().unwrap()),
			)
			.unwrap();
		} else {
			let mut bytes = std::fs::read(&path).unwrap();
			if damage == "tail" {
				bytes.extend([0x71; 17]);
			} else {
				bytes[120] ^= 1;
			}
			std::fs::write(path, bytes).unwrap();
		}
		drop(store);
		let (store, _) =
			LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
				.await
				.unwrap();
		let before = model.count();
		let (_, _, cap, key, _) = inputs();
		assert!(matches!(
			store
				.repair_index(p.owner.clone(), cap, key, [0x71; 16], [0x72; 16])
				.await,
			Err(StoreError::Fenced)
		));
		assert_eq!(model.count(), before);
		assert_eq!(
			std::fs::read(temp.root.join("index").join(hex(&p.index))).ok(),
			old
		);
		assert!(
			store
				.observed(sha(&p.owner.bytes().unwrap()), HARD_FENCED)
				.await
		);
		assert!(
			store
				.observed(sha(&p.owner.bytes().unwrap()), INDEX_FENCED)
				.await
		);
	}
}

async fn repair_calls() -> Vec<(usize, Op)> {
	let temp = Temp::new();
	let (model, store, p, _, _) = reopened(&temp, "invalid").await;
	let start = model.trace().len();
	let (_, _, cap, key, _) = inputs();
	store
		.repair_index(p.owner, cap, key, [0x71; 16], [0x72; 16])
		.await
		.unwrap();
	model.trace()[start..]
		.iter()
		.filter_map(|e| match e {
			Event::Submitted(op, _) => Some(op.clone()),
			_ => None,
		})
		.enumerate()
		.map(|(i, op)| (i + 1, op))
		.collect()
}

#[tokio::test]
async fn reopened_index_repair_each_failure_cut_retains_evidence_and_hard_fences_retry() {
	for (at, _) in repair_calls().await {
		for fault in [Fault::Before, Fault::After] {
			let temp = Temp::new();
			let (model, store, p, original, old) = reopened(&temp, "invalid").await;
			let old = old.unwrap();
			model.fault(model.count() + at, fault);
			let (_, _, cap, key, _) = inputs();
			let result = store
				.repair_index(p.owner.clone(), cap, key, [0x71; 16], [0x72; 16])
				.await;
			assert!(result.is_err(), "cut {at} must fail");
			assert!(
				store
					.observed(sha(&p.owner.bytes().unwrap()), HARD_FENCED)
					.await
			);
			let before = model.count();
			let (_, _, cap, key, _) = inputs();
			assert!(matches!(
				store
					.repair_index(p.owner.clone(), cap, key, [0x73; 16], [0x74; 16])
					.await,
				Err(StoreError::Fenced)
			));
			assert_eq!(model.count(), before);
			drop(store);
			model.crash(false, None).unwrap();
			let path = temp.root.join("index").join(hex(&p.index));
			let current = std::fs::read(path).unwrap();
			if current != old {
				assert_eq!(current, original);
				assert_eq!(
					std::fs::read(temp.root.join("quarantine").join(hex(&[0x71; 16]))).unwrap(),
					old
				);
			}
			let (_, report) = LocalStore::open_existing(
				temp.root.clone(),
				inputs().0,
				model.clone(),
				model.clone(),
			)
			.await
			.unwrap();
			assert_eq!(report.bad_objects, usize::from(current != original));
		}
	}
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_reopened_index_repair_keeps_owner_and_completion_through_barriers() {
	for (at, op) in repair_calls().await {
		if !matches!(op, Op::Write | Op::FileSync | Op::DirSync | Op::Rename) {
			continue;
		}
		for after in [false, true] {
			let temp = Temp::new();
			let (model, store, p, original, old) = reopened(&temp, "invalid").await;
			let gate = Arc::new(Gate {
				entered: tokio::sync::Notify::new(),
				release: tokio::sync::Notify::new(),
			});
			model.fault(
				model.count() + at,
				if after {
					Fault::GateAfter(gate.clone())
				} else {
					Fault::Gate(gate.clone())
				},
			);
			let s = store.clone();
			let owner = p.owner.clone();
			let (_, _, cap, key, _) = inputs();
			let caller = tokio::spawn(async move {
				s.repair_index(owner, cap, key, [0x71; 16], [0x72; 16])
					.await
			});
			gate.entered.notified().await;
			caller.abort();
			let _ = caller.await;
			assert!(
				store
					.owners
					.lock()
					.await
					.contains(&p.owner.bytes().unwrap())
			);
			let before = model.count();
			let (_, _, cap, key, _) = inputs();
			assert!(matches!(
				store
					.repair_index(p.owner.clone(), cap, key, [0x73; 16], [0x74; 16])
					.await,
				Err(StoreError::Conflict)
			));
			assert_eq!(model.count(), before);
			assert!(
				store
					.observed(sha(&p.owner.bytes().unwrap()), INDEX_FENCED)
					.await
			);
			gate.release.notify_one();
			// The detached repair finishes real syncs on blocking threads after the release; wait
			// on wall time, not a yield count, which a slow fsync outlasts.
			let drained = tokio::time::timeout(std::time::Duration::from_secs(30), async {
				while !store.owners.lock().await.is_empty() {
					tokio::time::sleep(std::time::Duration::from_millis(1)).await;
				}
			})
			.await;
			assert!(drained.is_ok(), "detached repair must release its owner");
			assert!(store.owners.lock().await.is_empty());
			assert!(!store.observed(sha(&p.owner.bytes().unwrap()), FENCED).await);
			assert_eq!(store.inflight.available_permits(), 8);
			assert_eq!(
				std::fs::read(temp.root.join("quarantine").join(hex(&[0x71; 16]))).unwrap(),
				old.unwrap()
			);
			drop(store);
			model.crash(false, None).unwrap();
			assert_eq!(
				std::fs::read(temp.root.join("index").join(hex(&p.index))).unwrap(),
				original
			);
		}
	}
}

#[tokio::test]
async fn reopened_index_repair_requires_matching_scoped_content_verification() {
	let temp = Temp::new();
	let (model, store, p, _, old) = reopened(&temp, "invalid").await;
	let (_, wrong_cap, wrong_key) =
		crate::storage::mutation_visibility::root2::durable::loader::tests::raw_variant(
			false,
			Some(908),
		);
	assert!(
		store
			.repair_index(
				p.owner.clone(),
				wrong_cap,
				Arc::new(wrong_key),
				[0x71; 16],
				[0x72; 16]
			)
			.await
			.is_err()
	);
	assert_eq!(
		std::fs::read(temp.root.join("index").join(hex(&p.index))).ok(),
		old
	);
	assert!(
		std::fs::read_dir(temp.root.join("quarantine"))
			.unwrap()
			.next()
			.is_none()
	);
	assert!(
		store
			.observed(sha(&p.owner.bytes().unwrap()), INDEX_FENCED)
			.await
	);
	assert!(
		!store
			.observed(sha(&p.owner.bytes().unwrap()), HARD_FENCED)
			.await
	);
	let (_, _, cap, key, _) = inputs();
	store
		.repair_index(p.owner.clone(), cap, key, [0x71; 16], [0x72; 16])
		.await
		.unwrap();
	assert!(!store.observed(sha(&p.owner.bytes().unwrap()), FENCED).await);
	assert!(model.count() > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn verified_index_completion_clears_only_index_reason_under_exclusive_ownership() {
	let temp = Temp::new();
	let (model, store, p, _, _) = reopened(&temp, "invalid").await;
	let gate = Arc::new(Gate {
		entered: tokio::sync::Notify::new(),
		release: tokio::sync::Notify::new(),
	});
	let at = repair_calls()
		.await
		.into_iter()
		.rev()
		.find(|(_, op)| *op == Op::DirSync)
		.unwrap()
		.0;
	model.fault(model.count() + at, Fault::GateAfter(gate.clone()));
	let s = store.clone();
	let owner = p.owner.clone();
	let (_, _, cap, key, _) = inputs();
	let job = tokio::spawn(async move {
		s.repair_index(owner, cap, key, [0x71; 16], [0x72; 16])
			.await
	});
	gate.entered.notified().await;
	// A separate diagnostic fence published during completion must survive repair.
	store
		.observe(sha(&p.owner.bytes().unwrap()), HARD_FENCED)
		.await
		.unwrap();
	gate.release.notify_one();
	assert!(job.await.unwrap().unwrap().durable);
	assert!(
		!store
			.observed(sha(&p.owner.bytes().unwrap()), INDEX_FENCED)
			.await
	);
	assert!(
		store
			.observed(sha(&p.owner.bytes().unwrap()), HARD_FENCED)
			.await
	);
	let before = model.count();
	let (_, _, cap, key, _) = inputs();
	assert!(matches!(
		store
			.repair_index(p.owner.clone(), cap, key, [0x73; 16], [0x74; 16])
			.await,
		Err(StoreError::Fenced)
	));
	assert_eq!(model.count(), before);
}

#[tokio::test]
async fn same_process_uncertain_journal_sync_keeps_verified_index_repair_fenced() {
	let temp = Temp::new();
	let (model, store, p, data) = progress::content(&temp).await;
	let start = model.trace().len();
	store
		.append(p.owner, progress::steps(&data, store.id, false).remove(0))
		.await
		.unwrap();
	let at = model.trace()[start..]
		.iter()
		.filter_map(|e| match e {
			Event::Submitted(op, _) => Some(op),
			_ => None,
		})
		.position(|op| *op == Op::FileSync)
		.unwrap()
		+ 1;
	for after in [false, true] {
		let temp = Temp::new();
		let (model, store, p, data) = progress::content(&temp).await;
		let old = std::fs::read(temp.root.join("index").join(hex(&p.index))).unwrap();
		model.fault(
			model.count() + at,
			if after { Fault::After } else { Fault::Before },
		);
		assert!(
			store
				.append(
					p.owner.clone(),
					progress::steps(&data, store.id, false).remove(0)
				)
				.await
				.is_err()
		);
		let before = model.count();
		let (_, _, cap, key, _) = inputs();
		assert!(matches!(
			store
				.repair_index(p.owner.clone(), cap, key, [0x71; 16], [0x72; 16])
				.await,
			Err(StoreError::Fenced)
		));
		assert_eq!(model.count(), before);
		assert_eq!(
			std::fs::read(temp.root.join("index").join(hex(&p.index))).unwrap(),
			old
		);
		assert!(
			std::fs::read_dir(temp.root.join("quarantine"))
				.unwrap()
				.next()
				.is_none()
		);
		assert!(
			store
				.observed(sha(&p.owner.bytes().unwrap()), HARD_FENCED)
				.await
		);
	}
}

#[tokio::test]
async fn reopened_index_repair_capacity_refusal_keeps_candidate_and_index_fence() {
	let temp = Temp::new();
	let (_, store, p, original, old) = reopened(&temp, "invalid").await;
	{
		let mut s = store.publisher.lock().await;
		s.debt_bytes = QUOTA - s.inventory.charged - HEADROOM;
	}
	let (_, _, cap, key, _) = inputs();
	let result = store
		.repair_index(p.owner.clone(), cap, key, [0x71; 16], [0x72; 16])
		.await
		.unwrap();
	assert!(!result.durable);
	assert_eq!(result.candidate, original);
	drop(result);
	assert_eq!(
		std::fs::read(temp.root.join("index").join(hex(&p.index))).ok(),
		old
	);
	assert!(
		std::fs::read_dir(temp.root.join("quarantine"))
			.unwrap()
			.next()
			.is_none()
	);
	assert!(
		store
			.observed(sha(&p.owner.bytes().unwrap()), INDEX_FENCED)
			.await
	);
	assert!(
		!store
			.observed(sha(&p.owner.bytes().unwrap()), HARD_FENCED)
			.await
	);
	store.publisher.lock().await.debt_bytes = 0;
	let (_, _, cap, key, _) = inputs();
	assert!(
		store
			.repair_index(p.owner.clone(), cap, key, [0x71; 16], [0x72; 16])
			.await
			.unwrap()
			.durable
	);
}
