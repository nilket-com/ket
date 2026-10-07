use super::*;
#[tokio::test]
async fn short_write_loop_is_exact_and_zero_write_refuses() {
	for fault in [Fault::Short(7), Fault::Interrupted, Fault::Zero] {
		let temp = Temp::new();
		let (model, store, entry) = initialized(&temp).await;
		// Space, open and handle length precede the exact allocation write.
		model.fault(model.count() + 4, fault.clone());
		let result = store.allocate(entry.clone()).await;
		assert_eq!(result.is_err(), matches!(fault, Fault::Zero));
		if result.is_ok() {
			assert_eq!(
				std::fs::metadata(temp.root.join("allocations/0000000000000001"))
					.unwrap()
					.len(),
				656
			)
		} else {
			assert!(matches!(
				store.allocate(entry).await,
				Err(StoreError::Fenced)
			));
		}
	}
}
#[tokio::test]
async fn partial_write_or_fsync_error_is_not_success() {
	for at in [4, 5] {
		for fault in [Fault::Before, Fault::After] {
			let temp = Temp::new();
			let (model, store, entry) = initialized(&temp).await;
			model.fault(model.count() + at, fault);
			assert!(store.allocate(entry.clone()).await.is_err());
			assert!(matches!(
				store.allocate(entry).await,
				Err(StoreError::Fenced)
			));
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
			assert!(report.inventory.sequence <= 1); // Complete but unobserved allocation remains pending.
		}
	}
}
#[tokio::test]
async fn read_error_is_unavailable_not_empty_and_size_bound_precedes_read() {
	let temp = Temp::new();
	let (model, store, entry) = admitted(&temp).await;
	drop(store);
	let before = model.count();
	model.fault(before + 4, Fault::Before);
	assert!(
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.is_err()
	);
	let io = Io::new(model.clone(), model.clone());
	let path = temp.root.join("allocations/0000000000000001");
	let start = model.trace().len();
	let mut work = 0;
	assert!(io.bounded_file(&path, 655, &mut work).await.is_err());
	assert!(
		!model.trace()[start..]
			.iter()
			.any(|e| matches!(e, Event::Submitted(Op::Read, _)))
	);
	assert_eq!(std::fs::metadata(path).unwrap().len(), 656);
	assert!(entry.credit.charge > 0);
}
#[tokio::test]
async fn bounded_enumerator_and_failed_entry_never_report_empty() {
	let temp = Temp::new();
	let (model, store, _) = initialized(&temp).await;
	assert!(model.list(&temp.root, 8).await.is_err());
	assert_eq!(model.list(&temp.root, 9).await.unwrap().len(), 9);
	model.fault(model.count() + 1, Fault::ListEntry(0));
	assert!(model.list(&temp.root, 9).await.is_err());
	drop(store);
	std::fs::remove_dir(temp.root.join("data")).unwrap();
	assert!(
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.is_err()
	);
	assert!(!temp.root.join("data").exists());
}
#[tokio::test]
async fn cancelled_wait_keeps_submitted_io_owned() {
	let temp = Temp::new();
	let (model, store, entry) = initialized(&temp).await;
	let gate = Arc::new(Gate {
		entered: tokio::sync::Notify::new(),
		release: tokio::sync::Notify::new(),
	});
	model.fault(model.count() + 4, Fault::Gate(gate.clone()));
	let caller = {
		let store = store.clone();
		let entry = entry.clone();
		tokio::spawn(async move { store.allocate(entry).await })
	};
	gate.entered.notified().await;
	caller.abort();
	gate.release.notify_one();
	// Causal publisher acquisition waits until the owned operation releases its mutation lock.
	let state = store.publisher.lock().await;
	assert_eq!(state.inventory.sequence, 1);
	assert!(!state.fenced);
	drop(state);
	drop(store);
	model.crash(false, None).unwrap();
	let (_, report) =
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.unwrap();
	assert_eq!(report.inventory.sequence, 1);
}
#[tokio::test]
async fn handle_and_buffer_limits_precede_acquisition() {
	let temp = Temp::new();
	let (model, store, _) = initialized(&temp).await;
	let mut permits = Vec::new();
	for _ in 0..63 {
		permits.push(store.io.handles.clone().try_acquire_owned().unwrap());
	}
	let count = model.count();
	assert!(
		store
			.io
			.open(&temp.root.join("INIT"), false, false)
			.await
			.is_err()
	);
	assert_eq!(model.count(), count);
	drop(permits);
	let _buffers = store
		.io
		.buffers
		.clone()
		.try_acquire_many_owned(BUFFER_BYTES)
		.unwrap();
	let mut work = 0;
	let h = store
		.io
		.open(&temp.root.join("INIT"), false, false)
		.await
		.unwrap();
	let count = model.count();
	assert!(store.io.read(&h, 0, 136, &mut work).await.is_err());
	assert_eq!(model.count(), count);
}
#[tokio::test]
async fn checkpoint_replacement_replays_matching_prefix_and_never_selects_pending() {
	let baseline = Temp::new();
	let (model, store, _) = admitted(&baseline).await;
	let start = model.count();
	store.checkpoint([0x77; 16]).await.unwrap();
	let calls = model.count() - start;
	for at in 1..=calls {
		for fault in [Fault::Before, Fault::After] {
			let temp = Temp::new();
			let (model, store, entry) = admitted(&temp).await;
			model.fault(model.count() + at, fault);
			let _ = store.checkpoint([0x77; 16]).await;
			drop(store);
			model.crash(false, None).unwrap();
			let (store, report) = LocalStore::open_existing(
				temp.root.clone(),
				inputs().0,
				model.clone(),
				model.clone(),
			)
			.await
			.unwrap();
			assert_eq!(report.inventory.sequence, 1);
			assert_eq!(report.inventory.charged, entry.credit.charge);
			let checkpoint = std::fs::read(temp.root.join("CATALOG")).unwrap();
			let (seq, _) = Inventory::check_checkpoint(&checkpoint, store.id).unwrap();
			assert!(seq == 0 || seq == 1);
			assert_eq!(
				std::fs::metadata(temp.root.join("allocations/0000000000000001"))
					.unwrap()
					.len(),
				656
			);
		}
	}
}
#[tokio::test]
async fn index_replacement_keeps_old_evidence_and_original_on_budget_failure() {
	for exhausted in [false, true] {
		let temp = Temp::new();
		let (model, store, entry, _) = progress::content(&temp).await;
		let path = temp.root.join("index").join(hex(&entry.index));
		let bad = b"independent corrupt index evidence";
		std::fs::write(&path, bad).unwrap();
		if exhausted {
			store.publisher.lock().await.debt_files = TEMP_FILES;
		}
		let (_, _, cap, key, _) = inputs();
		let result = store
			.repair_index(entry.owner.clone(), cap, key, [0x61; 16], [0x62; 16])
			.await
			.unwrap();
		assert_eq!(result.durable, !exhausted);
		assert_eq!(result.candidate.len(), 420);
		if exhausted {
			assert_eq!(std::fs::read(path).unwrap(), bad)
		} else {
			assert_eq!(
				std::fs::read(temp.root.join("quarantine").join(hex(&[0x61; 16]))).unwrap(),
				bad
			);
			assert_eq!(std::fs::read(&path).unwrap(), result.candidate);
			drop(result);
			drop(store);
			model.crash(false, None).unwrap();
			assert_eq!(
				std::fs::read(temp.root.join("quarantine").join(hex(&[0x61; 16]))).unwrap(),
				bad
			);
			assert_eq!(std::fs::read(path).unwrap().len(), 420);
		}
	}
}
#[tokio::test]
async fn index_cannot_supply_missing_or_corrupt_bundle_and_import_cannot_replace_source() {
	for missing in [false, true] {
		let temp = Temp::new();
		let (_, store, entry, _) = progress::content(&temp).await;
		let data = temp.root.join("data").join(hex(&entry.data));
		let index = temp.root.join("index").join(hex(&entry.index));
		let original = std::fs::read(&index).unwrap();
		assert!(store.unsupported_transform().await.is_err());
		assert_eq!(std::fs::read(&index).unwrap(), original);
		if missing {
			std::fs::remove_file(data).unwrap()
		} else {
			std::fs::write(data, [0x93; 41]).unwrap()
		}
		let (_, _, cap, key, _) = inputs();
		assert!(
			store
				.repair_index(entry.owner.clone(), cap, key, [0x61; 16], [0x62; 16])
				.await
				.is_err()
		);
		assert_eq!(std::fs::read(index).unwrap(), original);
	}
}
#[tokio::test]
async fn complete_unobserved_record_does_not_prove_reply_and_cancellation_keeps_completion_owned() {
	let temp = Temp::new();
	let (model, store, entry) = initialized(&temp).await;
	let gate = Arc::new(Gate {
		entered: tokio::sync::Notify::new(),
		release: tokio::sync::Notify::new(),
	});
	model.fault(model.count() + 5, Fault::GateAfter(gate.clone()));
	let caller = {
		let store = store.clone();
		let entry = entry.clone();
		tokio::spawn(async move { store.allocate(entry).await })
	};
	gate.entered.notified().await;
	caller.abort();
	gate.release.notify_one();
	let state = store.publisher.lock().await;
	assert_eq!(state.inventory.sequence, 1);
	drop(state);
	assert!(
		!model
			.trace()
			.iter()
			.any(|e| matches!(e, Event::Observed(_)))
	);
	drop(store);
	model.crash(false, None).unwrap();
	let (_, report) =
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.unwrap();
	assert_eq!(report.inventory.sequence, 1);
}
#[tokio::test]
async fn disk_full_blocks_admission_without_deleting_admitted_proof() {
	for code in [libc::ENOSPC, libc::EDQUOT] {
		for after in [false, true] {
			let temp = Temp::new();
			let (model, store, entry, _) = progress::content(&temp).await;
			let path = temp.root.join("data").join(hex(&entry.data));
			let digest = sha(&std::fs::read(&path).unwrap());
			let start = model.count();
			model.fault(
				start + 1,
				if after {
					Fault::CodeAfter(code)
				} else {
					Fault::Code(code)
				},
			);
			assert!(
				store
					.append(entry.owner.clone(), UnverifiedJournalStep2::WitnessRetired)
					.await
					.is_err()
			);
			assert!(store.publisher.lock().await.fenced);
			assert_eq!(
				store.publisher.lock().await.inventory.charged,
				entry.credit.charge
			);
			assert_eq!(sha(&std::fs::read(path).unwrap()), digest);
		}
	}
}
#[tokio::test]
async fn index_repair_each_cut_keeps_source_or_durably_preserved_old_evidence() {
	let baseline = Temp::new();
	let (model, store, entry, _) = progress::content(&baseline).await;
	let path = baseline.root.join("index").join(hex(&entry.index));
	std::fs::write(path, b"old invalid evidence").unwrap();
	let before = model.count();
	let (_, _, cap, key, _) = inputs();
	store
		.repair_index(entry.owner.clone(), cap, key, [0x61; 16], [0x62; 16])
		.await
		.unwrap();
	let calls = model.count() - before;
	for at in 1..=calls {
		for fault in [Fault::Before, Fault::After] {
			let temp = Temp::new();
			let (model, store, entry, _) = progress::content(&temp).await;
			let path = temp.root.join("index").join(hex(&entry.index));
			let old = b"old invalid evidence";
			std::fs::write(&path, old).unwrap();
			// Model the already durable damaged source independently of the replacement operation.
			let h = model.open_regular(&path, false, true).await.unwrap();
			model.fsync(&h).await.unwrap();
			drop(h);
			model.fault(model.count() + at, fault);
			let (_, _, cap, key, _) = inputs();
			let _ = store
				.repair_index(entry.owner.clone(), cap, key, [0x61; 16], [0x62; 16])
				.await;
			drop(store);
			model.crash(false, None).unwrap();
			let current = std::fs::read(&path).unwrap();
			if current != old {
				assert_eq!(
					std::fs::read(temp.root.join("quarantine").join(hex(&[0x61; 16]))).unwrap(),
					old
				);
				assert_eq!(current.len(), 420);
			}
		}
	}
}
#[tokio::test]
async fn physical_inventory_reports_missing_and_corrupt_bytes_without_trust() {
	use super::super::inspection::PhysicalBytes;
	let temp = Temp::new();
	let (_, store, entry, _) = progress::content(&temp).await;
	let physical = store.physical_parent(entry.owner.clone()).await.unwrap();
	assert!(physical.owner == entry.owner);
	assert_eq!(physical.data, PhysicalBytes::Structural);
	assert_eq!(physical.index, PhysicalBytes::Structural);
	let path = temp.root.join("data").join(hex(&entry.data));
	std::fs::write(&path, [0x73; 41]).unwrap();
	let physical = store.physical_parent(entry.owner.clone()).await.unwrap();
	assert_eq!(physical.data, PhysicalBytes::Unavailable);
	assert_eq!(physical.index, PhysicalBytes::Unavailable);
	std::fs::remove_file(path).unwrap();
	let physical = store.physical_parent(entry.owner).await.unwrap();
	assert_eq!(physical.data, PhysicalBytes::Missing);
	assert_eq!(physical.index, PhysicalBytes::Unavailable);
	assert_eq!(physical.records, 1);
}
#[tokio::test]
async fn checked_offsets_and_short_reads_refuse_or_complete_before_unbounded_io() {
	let temp = Temp::new();
	let (model, store, _) = initialized(&temp).await;
	let h = store
		.io
		.open(&temp.root.join("INIT"), false, true)
		.await
		.unwrap();
	let before = model.count();
	let mut work = 0;
	assert!(store.io.write(&h, u64::MAX, &[1, 2]).await.is_err());
	assert!(store.io.read(&h, u64::MAX, 2, &mut work).await.is_err());
	assert_eq!(model.count(), before);
	model.fault(model.count() + 1, Fault::Short(7));
	assert_eq!(
		store.io.read(&h, 0, 136, &mut work).await.unwrap().len(),
		136
	);
	model.fault(model.count() + 1, Fault::Zero);
	assert!(store.io.read(&h, 0, 136, &mut work).await.is_err());
	let mut work = READ_WORK;
	let before = model.count();
	assert!(store.io.read(&h, 0, 1, &mut work).await.is_err());
	assert_eq!(model.count(), before);
}
#[tokio::test]
async fn allowed_early_persistence_keeps_complete_unobserved_or_exact_torn_tail() {
	for partial in [None, Some(200usize), Some(400), Some(655)] {
		let temp = Temp::new();
		let (model, store, entry) = initialized(&temp).await;
		// A complete write returned, the subsequent file barrier failed before execution.
		model.fault(model.count() + 5, Fault::Before);
		assert!(store.allocate(entry.clone()).await.is_err());
		drop(store);
		model.allow_early_names().unwrap();
		model.crash(true, partial).unwrap();
		let opened =
			LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
				.await;
		// The header was already durable. Partial persistence of a new record is retained/fenced.
		let (store, report) = opened.unwrap();
		if partial.is_none() || partial.unwrap() >= 656 {
			assert_eq!(report.inventory.sequence, 1)
		} else {
			assert!(report.tail.is_some());
			assert!(store.publisher.lock().await.fenced);
		}
	}
}
#[tokio::test]
async fn data_index_and_first_names_all_cut_before_acknowledgement() {
	let baseline = Temp::new();
	let (model, store, entry) = admitted(&baseline).await;
	let (_, data, cap, key, _) = inputs();
	store
		.prepared(entry.owner.clone(), data.bundle.claims[0].0.clone())
		.await
		.unwrap();
	let start = model.count();
	store
		.persist_content(entry.owner.clone(), data, cap, key)
		.await
		.unwrap();
	let calls = model.count() - start;
	for at in 1..=calls {
		for fault in [Fault::Before, Fault::After] {
			let temp = Temp::new();
			let (model, store, entry) = admitted(&temp).await;
			let (_, data, cap, key, _) = inputs();
			store
				.prepared(entry.owner.clone(), data.bundle.claims[0].0.clone())
				.await
				.unwrap();
			model.fault(model.count() + at, fault);
			let acknowledged = store
				.persist_content(entry.owner.clone(), data, cap, key)
				.await
				.is_ok();
			drop(store);
			model.crash(false, None).unwrap();
			let (store, report) = LocalStore::open_existing(
				temp.root.clone(),
				inputs().0,
				model.clone(),
				model.clone(),
			)
			.await
			.unwrap();
			assert_eq!(report.inventory.sequence, 1);
			let physical = store.physical_parent(entry.owner).await.unwrap();
			if acknowledged {
				assert_eq!(
					physical.data,
					super::super::inspection::PhysicalBytes::Structural
				);
				assert_eq!(
					physical.index,
					super::super::inspection::PhysicalBytes::Structural
				);
			}
			assert_eq!(physical.records, 1); // None of these cuts generates LocalChildDurable.
		}
	}
}
#[tokio::test]
async fn disk_full_on_write_file_sync_and_directory_sync_retains_credit_and_closes_admission() {
	let baseline = Temp::new();
	let (model, store, entry) = admitted(&baseline).await;
	let (_, data, cap, key, _) = inputs();
	store
		.prepared(entry.owner.clone(), data.bundle.claims[0].0.clone())
		.await
		.unwrap();
	let before = model.trace().len();
	store
		.persist_content(entry.owner.clone(), data, cap, key)
		.await
		.unwrap();
	let cuts = model.trace()[before..]
		.iter()
		.filter_map(|e| {
			if let Event::Submitted(op, _) = e {
				Some(op.clone())
			} else {
				None
			}
		})
		.enumerate()
		.filter_map(|(i, op)| {
			if matches!(op, Op::Write | Op::FileSync | Op::DirSync) {
				Some(i + 1)
			} else {
				None
			}
		})
		.collect::<Vec<_>>();
	for cut in cuts {
		for code in [libc::ENOSPC, libc::EDQUOT] {
			for after in [false, true] {
				let temp = Temp::new();
				let (model, store, entry) = admitted(&temp).await;
				let (_, data, cap, key, _) = inputs();
				store
					.prepared(entry.owner.clone(), data.bundle.claims[0].0.clone())
					.await
					.unwrap();
				let path = temp
					.root
					.join("journals")
					.join(entry.owner.locator().unwrap());
				let digest = sha(&std::fs::read(&path).unwrap());
				model.fault(
					model.count() + cut,
					if after {
						Fault::CodeAfter(code)
					} else {
						Fault::Code(code)
					},
				);
				assert!(
					store
						.persist_content(entry.owner.clone(), data, cap, key)
						.await
						.is_err()
				);
				let state = store.publisher.lock().await;
				assert!(state.fenced);
				assert_eq!(state.inventory.charged, entry.credit.charge);
				drop(state);
				assert_eq!(sha(&std::fs::read(path).unwrap()), digest);
				assert!(store.allocate(entry).await.is_err());
			}
		}
	}
}
#[tokio::test]
async fn allowed_early_journal_prefix_body_or_checksum_tail_preserves_acknowledged_endpoint() {
	let baseline = Temp::new();
	let (model, store, entry, data) = progress::content(&baseline).await;
	let receipt = progress::steps(&data, store.id, false)[0].clone();
	let before = model.count();
	store.append(entry.owner, receipt).await.unwrap();
	let calls = model.count() - before;
	for suffix in [1usize, 16, 319, 639, 640] {
		let temp = Temp::new();
		let (model, store, entry, data) = progress::content(&temp).await;
		let path = temp
			.root
			.join("journals")
			.join(entry.owner.locator().unwrap());
		let acknowledged = std::fs::read(&path).unwrap();
		model.fault(model.count() + calls, Fault::Before);
		let receipt = progress::steps(&data, store.id, false)[0].clone();
		assert!(store.append(entry.owner.clone(), receipt).await.is_err());
		drop(store);
		model.allow_early_names().unwrap();
		model
			.crash(true, Some(acknowledged.len() + suffix))
			.unwrap();
		let bytes = std::fs::read(&path).unwrap();
		assert!(bytes.starts_with(&acknowledged));
		let (store, _) =
			LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
				.await
				.unwrap();
		let physical = store.physical_parent(entry.owner.clone()).await.unwrap();
		if suffix < 640 {
			assert_eq!(physical.records, 1);
			assert_eq!(physical.endpoint, acknowledged.len() as u64);
			assert_eq!(
				physical.damaged_suffix,
				Some((acknowledged.len() as u64, bytes.len() as u64))
			);
			assert!(physical.fenced)
		} else {
			assert_eq!(physical.records, 2);
			assert!(physical.damaged_suffix.is_none())
		}
	}
}
#[tokio::test]
async fn live_reopen_reestablishes_journal_name_before_new_progress() {
	let baseline = Temp::new();
	let (model, store, entry) = admitted(&baseline).await;
	let (_, data, _, _, _) = inputs();
	let before = model.count();
	store
		.prepared(entry.owner, data.bundle.claims[0].0.clone())
		.await
		.unwrap();
	let calls = model.count() - before;
	let temp = Temp::new();
	let (model, store, entry) = admitted(&temp).await;
	let (_, data, _, _, _) = inputs();
	model.fault(model.count() + calls, Fault::Before);
	assert!(
		store
			.prepared(entry.owner.clone(), data.bundle.claims[0].0.clone())
			.await
			.is_err()
	);
	drop(store);
	let (store, _) =
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.unwrap();
	let witness = progress::steps(&data, store.id, false)[entry.credit.slots as usize].clone();
	store.append(entry.owner.clone(), witness).await.unwrap();
	drop(store);
	model.crash(false, None).unwrap();
	let (store, _) =
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.unwrap();
	let physical = store.physical_parent(entry.owner).await.unwrap();
	assert_eq!(physical.records, 2);
	assert!(physical.damaged_suffix.is_none());
}
#[tokio::test]
async fn live_reopen_reestablishes_init_marker_before_new_admission() {
	let baseline = Temp::new();
	let (model, store, _) = initialized(&baseline).await;
	let calls = model.count();
	drop(store);
	let temp = Temp::new();
	let model = Model::new(temp.root.clone());
	model.fault(calls, Fault::Before);
	assert!(
		LocalStore::create(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.is_err()
	);
	assert!(temp.root.join("INIT").exists());
	let (store, _) =
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.unwrap();
	let entry = inputs().4;
	store.allocate(entry).await.unwrap();
	drop(store);
	model.crash(false, None).unwrap();
	let (_, report) =
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.unwrap();
	assert_eq!(report.inventory.sequence, 1);
}
