use super::*;
#[test]
fn declared_sizes_and_plan_determine_charge_and_hard_caps_are_limits() {
	let (_, data, _, _, entry) = inputs();
	let c = &entry.credit;
	assert_eq!(c.data, 117 + data.encode().unwrap().len() as u32);
	assert_eq!(c.index, 420);
	assert_eq!(
		c.claim,
		data.bundle.claims[0].0.encode().unwrap().len() as u32
	);
	assert_eq!(c.slots, 3);
	let h = 212 + 37 * c.children as u64 + 20 * c.slots as u64;
	let w = 78 + c.claim as u64 + 390 * c.slots as u64 + h;
	let r = c.claim as u64 + 241;
	let o = 214 + r + c.response as u64;
	let journal =
		269 + 254 + c.claim as u64 + 640 * c.slots as u64 + 254 + w + 274 + r + 254 + o + 302 + 250;
	assert_eq!(c.journal as u64, journal);
	assert_eq!(c.charge, 512 + c.data as u64 + c.index as u64 + journal);
	assert!((c.data as usize) < DATA_MAX);
	assert!((c.journal as usize) < JOURNAL_CAP);
	assert_eq!(Credit::journal_bound(766, 1, 3, 128, 1).unwrap(), 9222);
	assert_eq!(512 + 5516 + 420 + 9222, 15670);
	assert_eq!(HEADROOM, 2 * 16777216 + 9 * 144 + 136);
}
#[test]
fn local_metadata_exact_widths_reserved_fields_and_checkpoint_canonicality() {
	let (id, _, _, _, entry) = inputs();
	assert_eq!(entry.bytes().unwrap().len(), 276);
	let allocation = Allocation::Parent(entry.clone());
	let raw = allocation.encode(id, 1, [0; 32]).unwrap();
	assert_eq!(raw.len(), 512);
	assert!(Allocation::decode(&raw, id, 1, [0; 32]).unwrap() == allocation);
	assert_eq!(segment_header(id, 1, 1, [0; 32]).unwrap().len(), 144);
	assert_eq!(journal_header(id, &entry.owner).unwrap().len(), 269);
	let empty = Inventory::default().checkpoint(id).unwrap();
	assert_eq!(empty.len(), 144);
	assert_eq!(init(id, sha(&empty)).unwrap().len(), 136);
	let mut inv = Inventory::default();
	inv.add(allocation, 1, sha(&raw)).unwrap();
	let cp = inv.checkpoint(id).unwrap();
	assert_eq!(cp.len(), 420);
	assert_eq!(Inventory::check_checkpoint(&cp, id).unwrap().0, 1);
	for offset in [8, 10, 11, 12, 430, 479, 511] {
		let mut bad = raw.clone();
		bad[offset] ^= 1;
		assert!(Allocation::decode(&bad, id, 1, [0; 32]).is_err());
	}
	assert_eq!(144 + 276 * PARENTS + 60 * GRANTS, 9289872);
}
#[tokio::test]
async fn reserve_unknown_keeps_identity_and_capacity_until_exact_resolution() {
	let temp = Temp::new();
	let (_, store, entry) = initialized(&temp).await;
	let (_, data, _, _, _) = inputs();
	let prospective = store
		.prospective(entry.clone(), data.clone())
		.await
		.unwrap();
	drop(prospective);
	for _lookup in ["Absent", "Unavailable", "Reserved"] {
		let s = store.publisher.lock().await;
		assert_eq!(s.prospective.len(), 1);
		assert_eq!(s.inventory.sequence, 0);
		assert_eq!(
			s.prospective.values().next().unwrap().owner.identity,
			entry.owner.identity
		);
		assert_eq!(
			s.prospective.values().next().unwrap().credit.charge,
			entry.credit.charge
		);
	}
	let resolved = store
		.resolve_prospective(entry.owner.clone(), entry.owner.reservation, data)
		.await
		.unwrap();
	assert!(resolved == entry);
	let state = store.publisher.lock().await;
	assert!(state.prospective.is_empty());
	assert_eq!(state.inventory.charged, entry.credit.charge);
}
#[tokio::test]
async fn replica_cannot_grow_into_holder_sequence_and_out_of_plan_growth_refuses_before_write() {
	let temp = Temp::new();
	let (model, store, mut entry) = initialized(&temp).await;
	let (_, data, _, _, _) = inputs();
	let node = data.bundle.claims[0].0.plans[0].nodes[1];
	entry.credit = Credit::freeze(&data, 128, 2, node).unwrap();
	assert_eq!(entry.credit.slots, 1);
	assert_eq!(entry.credit.journal, 269 + 254 + entry.credit.claim + 640);
	store.allocate(entry.clone()).await.unwrap();
	store
		.prepared(entry.owner.clone(), data.bundle.claims[0].0.clone())
		.await
		.unwrap();
	let mut attestation = UnverifiedWitness2::decode(&fixture("witness_two_receipts")).unwrap();
	attestation.claim = data.bundle.claims[0].0.clone();
	let before = model.trace().len();
	assert!(
		store
			.append(
				entry.owner.clone(),
				UnverifiedJournalStep2::WitnessReady(attestation)
			)
			.await
			.is_err()
	);
	assert!(writes(&model.trace()[before..]).is_empty());
	assert_eq!(
		store.publisher.lock().await.inventory.charged,
		entry.credit.charge
	);
}
#[tokio::test]
async fn charged_pending_files_cannot_bypass_store_quota() {
	let temp = Temp::new();
	let (model, store, _) = admitted(&temp).await;
	store.publisher.lock().await.debt_files = TEMP_FILES;
	let before = model.trace().len();
	assert!(store.checkpoint([0x88; 16]).await.is_err());
	assert!(writes(&model.trace()[before..]).is_empty());
}
#[tokio::test]
async fn real_linux_exact_file_reopen_corrobates_selected_io_without_power_loss_claim() {
	let temp = Temp::new();
	let (id, data, cap, key, entry) = inputs();
	let files = Arc::new(BlockingFileOps);
	let adapter = Arc::new(LinuxAdapter);
	let store = LocalStore::create(temp.root.clone(), id, files.clone(), adapter.clone())
		.await
		.unwrap();
	store.allocate(entry.clone()).await.unwrap();
	store
		.prepared(entry.owner.clone(), data.bundle.claims[0].0.clone())
		.await
		.unwrap();
	store
		.persist_content(entry.owner.clone(), data, cap, key)
		.await
		.unwrap();
	drop(store);
	let (_, report) = LocalStore::open_existing(temp.root.clone(), id, files, adapter)
		.await
		.unwrap();
	assert_eq!(report.inventory.sequence, 1);
	assert_eq!(
		std::fs::metadata(temp.root.join("data").join(hex(&entry.data)))
			.unwrap()
			.len(),
		entry.credit.data as u64
	);
	assert_eq!(
		std::fs::metadata(temp.root.join("index").join(hex(&entry.index)))
			.unwrap()
			.len(),
		420
	);
}
#[tokio::test]
async fn allocation_append_bytes_do_not_grow_with_occupancy_and_parent_limit_counts_missing_entries()
 {
	use std::io::Write;
	for retained in [0u64, 17, 4095, 4096, 32767] {
		let temp = Temp::new();
		let (model, store, entry) = initialized(&temp).await;
		let mut inv = Inventory::default();
		let mut segment = None;
		for n in 1..=retained {
			let mut p = entry.clone();
			p.owner.identity.sequence = n;
			p.data[..8].copy_from_slice(&n.to_le_bytes());
			p.data[15] = 0x71;
			p.index[..8].copy_from_slice(&n.to_le_bytes());
			p.index[15] = 0x72;
			let number = (n - 1) / 4096 + 1;
			if (n - 1) % 4096 == 0 {
				let path = temp.root.join("allocations").join(format!("{number:016x}"));
				let mut file = std::fs::OpenOptions::new()
					.create(true)
					.truncate(true)
					.write(true)
					.open(path)
					.unwrap();
				file.write_all(&segment_header(store.id, number, n, inv.digest).unwrap())
					.unwrap();
				segment = Some(file);
			}
			let a = Allocation::Parent(p);
			let raw = a.encode(store.id, n, inv.digest).unwrap();
			segment.as_mut().unwrap().write_all(&raw).unwrap();
			inv.add(a, n, sha(&raw)).unwrap();
		}
		drop(segment);
		assert!(inv.summary_bytes() <= 16 * 1024 * 1024);
		// Publish the explicitly built fixture through the model's data and name barriers.
		for e in std::fs::read_dir(temp.root.join("allocations")).unwrap() {
			let h = model
				.open_regular(&e.unwrap().path(), false, true)
				.await
				.unwrap();
			model.fsync(&h).await.unwrap();
		}
		model
			.fsync_dir(&temp.root.join("allocations"))
			.await
			.unwrap();
		store.publisher.lock().await.inventory = inv;
		let mut next = entry.clone();
		next.owner.identity.sequence = retained + 1;
		next.data = [0x73; 16];
		next.index = [0x74; 16];
		let before = model.trace().len();
		store.allocate(next).await.unwrap();
		let trace = model.trace();
		let expected = if retained > 0 && retained % 4096 == 0 {
			vec![(0, 656)]
		} else {
			vec![(144 + (retained % 4096) * 512, 512)]
		};
		assert_eq!(writes(&trace[before..]), expected);
		assert!(
			trace[before..]
				.iter()
				.all(|e| !matches!(e,Event::Submitted(_,p) if p.contains("CATALOG")))
		);
		if retained == 32767 {
			let mut excess = entry.clone();
			excess.owner.identity.sequence = 32769;
			excess.data = [0x75; 16];
			excess.index = [0x76; 16];
			let before = model.trace().len();
			assert!(matches!(
				store.allocate(excess).await,
				Err(StoreError::Limit)
			));
			assert!(writes(&model.trace()[before..]).is_empty());
			assert_eq!(store.publisher.lock().await.inventory.parents.len(), 32768);
		}
		drop(store);
		model.crash(false, None).unwrap();
		let (_, report) =
			LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
				.await
				.unwrap();
		assert_eq!(report.inventory.sequence, retained + 1);
	}
}
#[test]
fn declared_response_bound_covers_supported_terminal_templates_and_full_planned_slots() {
	let (id, data, _, _, entry) = inputs();
	let c = &data.bundle.claims[0].0;
	for refused in [false, true] {
		for effect in [false, true] {
			let decision = Log2 {
				term: 1,
				node: 1,
				index: 6,
			};
			let terminal = if refused {
				Terminal2::Refused {
					first: decision,
					cause: 1,
					anchor: entry.owner.reservation,
				}
			} else {
				Terminal2::Accepted {
					first: decision,
					evidence: [0x71; 32],
				}
			};
			let result = ApplyResult2::Parent {
				record: ParentRecord2 {
					claim: c.clone(),
					reservation: entry.owner.reservation,
					held_bytes: c.held_bytes().unwrap(),
					terminal: Some(terminal),
				},
				new_effect: effect,
			}
			.encode(id.generation, id.cluster, decision);
			if refused && effect {
				assert!(result.is_err());
				continue;
			}
			let result = result.unwrap();
			assert_eq!(result.len(), c.encode().unwrap().len() + 241);
			let outcome = UnverifiedOutcome2 {
				generation: id.generation,
				cluster: id.cluster,
				identity: c.manifest.identity,
				claim: c.digest().unwrap(),
				decision,
				result,
				response: vec![0x7a; entry.credit.response as usize],
			};
			assert_eq!(
				outcome.encode().unwrap().len(),
				214 + c.encode().unwrap().len() + 241 + entry.credit.response as usize
			);
		}
	}
	let progress = progress::steps(&data, id, false);
	let witnessed = progress
		.iter()
		.find_map(|s| {
			if let UnverifiedJournalStep2::WitnessReady(w) = s {
				Some(w)
			} else {
				None
			}
		})
		.unwrap();
	assert_eq!(
		witnessed.attestation.encode().unwrap().len(),
		212 + 37 * entry.credit.children as usize + 20 * entry.credit.slots as usize
	);
	assert_eq!(
		witnessed.encode().unwrap().len(),
		78 + entry.credit.claim as usize
			+ 390 * entry.credit.slots as usize
			+ witnessed.attestation.encode().unwrap().len()
	);
}
#[test]
fn literal_metadata_goldens_match_independent_encoder() {
	let j: serde_json::Value = serde_json::from_str(include_str!("goldens.json")).unwrap();
	let golden = |name: &str| {
		let f = j["fixtures"]
			.as_array()
			.unwrap()
			.iter()
			.find(|f| f["name"] == name)
			.unwrap();
		let s = f["hex"].as_str().unwrap();
		(0..s.len())
			.step_by(2)
			.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
			.collect::<Vec<_>>()
	};
	let id = ContainerId {
		generation: [0x11; 16],
		cluster: [0x12; 16],
		store: [0x31; 16],
	};
	let empty = Inventory::default().checkpoint(id).unwrap();
	assert_eq!(empty, golden("empty_checkpoint"));
	assert_eq!(init(id, sha(&empty)).unwrap(), golden("init"));
	assert_eq!(
		segment_header(id, 1, 1, [0; 32]).unwrap(),
		golden("segment_header")
	);
	let owner = JournalIdentity2 {
		generation: id.generation,
		cluster: id.cluster,
		identity: MutationIdentity {
			generation: id.generation,
			tenant: 42,
			scope: crate::storage::mutation_visibility::MutationScope::Raw,
			table: [0; 16],
			schema_generation: 0,
			caller: [0x22; 16],
			sequence: 7,
		},
		claim: [0x33; 32],
		reservation: Log2 {
			term: 1,
			node: 4,
			index: 5,
		},
	};
	let p = ParentEntry {
		observations: 0,
		owner,
		data: [0x41; 16],
		index: [0x42; 16],
		credit: Credit {
			data: 5516,
			index: 420,
			journal: 9222,
			claim: 766,
			children: 1,
			versions: 1,
			slots: 3,
			response: 128,
			charge: 15670,
			plan: [0x55; 32],
			role: 1,
			local: NodeRef2 {
				node: 4,
				incarnation: [0x66; 16],
			},
		},
	};
	assert_eq!(
		journal_header(id, &p.owner).unwrap(),
		golden("journal_header")
	);
	let allocation = Allocation::Parent(p);
	let raw = allocation.encode(id, 1, [0; 32]).unwrap();
	assert_eq!(raw, golden("parent_allocation"));
	let mut inv = Inventory::default();
	inv.add(allocation, 1, sha(&raw)).unwrap();
	assert_eq!(inv.checkpoint(id).unwrap(), golden("parent_checkpoint"));
	let g = Allocation::Grant(GrantEntry {
		digest: [0x77; 32],
		object: [0x78; 16],
		bytes: 655,
		charge: 1167,
	});
	assert_eq!(
		g.encode(id, 1, [0; 32]).unwrap(),
		golden("grant_allocation")
	);
}
#[tokio::test]
async fn grant_archive_has_exact_credit_and_independent_allocation_barriers() {
	let temp = Temp::new();
	let (model, store, _) = initialized(&temp).await;
	let archive = UnverifiedGrantArchive2::decode(&fixture("grant_original_unverified")).unwrap();
	let before = model.trace().len();
	store.archive(archive.clone(), [0x78; 16]).await.unwrap();
	let trace = model.trace();
	assert_eq!(writes(&trace[before..]), vec![(144, 512), (0, 655)]);
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
		1
	);
	let raw = std::fs::read(temp.root.join("grants").join(hex(&[0x78; 16]))).unwrap();
	assert_eq!(raw.len(), 655);
	assert_eq!(
		object_inner(&raw, store.id, 3, [0x78; 16], 655).unwrap(),
		archive.encode().unwrap()
	);
	assert_eq!(store.publisher.lock().await.inventory.charged, 1167);
	drop(store);
	model.crash(false, None).unwrap();
	let (_, report) =
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.unwrap();
	assert_eq!(report.inventory.grants, 1);
	assert_eq!(report.inventory.charged, 1167);
}
#[test]
fn checked_profile_summary_and_charge_bounds_hold_at_full_parent_and_grant_capacity() {
	let parent_bytes = PARENTS * (std::mem::size_of::<(Digest2, ParentEntry)>() + 80);
	let grant_bytes = GRANTS * (std::mem::size_of::<(Digest2, GrantEntry)>() + 80);
	let object_bytes = (2 * PARENTS + GRANTS) * 16;
	assert!(parent_bytes + grant_bytes + object_bytes <= 16 * 1024 * 1024);
	assert!(Credit::journal_bound(u64::MAX, 1, 3, 128, 1).is_err());
	assert!(Credit::journal_bound(766, u64::MAX, 3, 128, 1).is_err());
	assert!(Credit::journal_bound(766, 1, u64::MAX, 128, 1).is_err());
	assert!(Credit::journal_bound(766, 1, 3, 65537, 1).is_err());
	assert_eq!(NAMESPACE, 16 + 3 * PARENTS + GRANTS + SEGMENTS + TEMP_FILES);
}
#[test]
fn impossible_role_slot_declarations_and_empty_response_bounds_refuse() {
	let (_, data, _, _, entry) = inputs();
	let mut credit = entry.credit.clone();
	credit.response = 0;
	assert!(credit.validate().is_err());
	assert!(Credit::freeze(&data, 0, 1, entry.credit.local).is_err());
	for role in [1, 2] {
		let mut credit = entry.credit.clone();
		credit.role = role;
		credit.slots = if role == 1 { 4 } else { 2 };
		credit.journal =
			Credit::journal_bound(credit.claim as u64, 1, credit.slots as u64, 128, role).unwrap()
				as u32;
		credit.charge = 512 + credit.data as u64 + credit.index as u64 + credit.journal as u64;
		assert!(credit.validate().is_err());
	}
}
#[tokio::test]
async fn allocated_metadata_cannot_supply_volatile_content_observations() {
	let temp = Temp::new();
	let (_, store, mut entry) = initialized(&temp).await;
	entry.observations = u8::MAX;
	store.allocate(entry.clone()).await.unwrap();
	let state = store.publisher.lock().await;
	let stored = state
		.inventory
		.parents
		.get(&sha(&entry.owner.bytes().unwrap()))
		.unwrap();
	assert_eq!(stored.observations, 0);
	assert_eq!(stored.bytes().unwrap(), entry.bytes().unwrap());
}
