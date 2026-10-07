use super::scan::scan_chain;
use super::*;
/// Bounded physical report; no recovered record becomes an acceptance/readiness token.
pub(super) struct Recovered {
	pub(super) inventory: Inventory,
	pub(super) materialized: u64,
	pub(super) tail: Option<(String, u64, u64)>,
	pub(super) debt_bytes: u64,
	pub(super) debt_files: usize,
	pub(super) damaged_parents: usize,
	pub(super) bad_objects: usize,
	pub(super) unavailable_owners: Vec<Digest2>,
}
pub(super) struct InventorySummary {
	pub(super) sequence: u64,
	pub(super) charged: u64,
	pub(super) parents: usize,
	pub(super) grants: usize,
}
pub(super) struct RecoveryReport {
	pub(super) inventory: InventorySummary,
	pub(super) tail: Option<(String, u64, u64)>,
	pub(super) debt_bytes: u64,
	pub(super) debt_files: usize,
	pub(super) damaged_parents: usize,
	pub(super) bad_objects: usize,
}
impl Recovered {
	pub(super) fn report(&self) -> RecoveryReport {
		RecoveryReport {
			inventory: InventorySummary {
				sequence: self.inventory.sequence,
				charged: self.inventory.charged,
				parents: self.inventory.parents.len(),
				grants: self.inventory.grants.len(),
			},
			tail: self.tail.clone(),
			debt_bytes: self.debt_bytes,
			debt_files: self.debt_files,
			damaged_parents: self.damaged_parents,
			bad_objects: self.bad_objects,
		}
	}
}
async fn root_names(io: &Io, root: &Path) -> StoreResult<Vec<String>> {
	let entries = io.list(root, 16 + TEMP_FILES).await?;
	let mut fixed = BTreeSet::new();
	let mut pending = Vec::new();
	for e in entries {
		if DIRS.contains(&e.name.as_str()) {
			require(e.directory && fixed.insert(e.name))?
		} else if ["INIT", "CATALOG", "LOCK"].contains(&e.name.as_str()) {
			require(!e.directory && fixed.insert(e.name))?
		} else {
			require(
				!e.directory
					&& e.name.starts_with("CATALOG.pending.")
					&& id_name(&e.name[16..]).is_ok(),
			)?;
			pending.push(e.name)
		}
	}
	require(fixed.len() == 9 && pending.len() <= TEMP_FILES)?;
	Ok(pending)
}
pub(super) async fn recover(io: &Io, root: &Path, id: ContainerId) -> StoreResult<Recovered> {
	let pending = root_names(io, root).await?;
	let mut work = 0;
	let empty = Inventory::default().checkpoint(id)?;
	let raw = io.bounded_file(&root.join("INIT"), 136, &mut work).await?;
	check_init(&raw, id, sha(&empty))?;
	let checkpoint = io
		.bounded_file(&root.join("CATALOG"), CHECKPOINT_CAP, &mut work)
		.await?;
	let (covered, digest) = Inventory::check_checkpoint(&checkpoint, id)?;
	if covered == 0 {
		require(checkpoint == empty)?
	}
	let mut inventory = Inventory::default();
	let mut materialized = 136 + checkpoint.len() as u64;
	let mut tail = None;
	let mut segments = io.list(&root.join("allocations"), SEGMENTS).await?;
	segments.sort_by(|a, b| a.name.cmp(&b.name));
	require(!segments.is_empty())?;
	for (i, segment) in segments.iter().enumerate() {
		let number = i as u64 + 1;
		let name = format!("{number:016x}");
		require(!segment.directory && segment.name == name)?;
		let h = io
			.open(&root.join("allocations").join(&name), false, false)
			.await?;
		let len = io.adapter.length(&h.handle).await?;
		if len > 144 + SEGMENT_RECORDS * 512 {
			return Err(StoreError::Limit);
		}
		materialized = materialized.checked_add(len).ok_or(StoreError::Limit)?;
		if len < 144 {
			require(number > 1 && i + 1 == segments.len())?;
			let partial = io.read(&h, 0, len as usize, &mut work).await?;
			let expected = segment_header(id, number, inventory.sequence + 1, inventory.digest)?;
			require(partial == expected[..partial.len()])?;
			tail = Some((name, 0, len));
			continue;
		}
		let header = io.read(&h, 0, 144, &mut work).await?;
		check_segment(
			&header,
			id,
			number,
			inventory.sequence + 1,
			inventory.digest,
		)?;
		let records = (len - 144) / 512;
		let rest = (len - 144) % 512;
		if i + 1 < segments.len() {
			require(records == SEGMENT_RECORDS && rest == 0)?
		}
		if number > 1 {
			if records == 0 && rest == 0 {
				require(i + 1 == segments.len())?;
				tail = Some((name.clone(), 144, 144));
			}
		}
		for i in 0..records {
			let raw = io.read(&h, 144 + i * 512, 512, &mut work).await?;
			let seq = inventory.sequence + 1;
			let a = Allocation::decode(&raw, id, seq, inventory.digest)?;
			inventory.add(a, seq, sha(&raw))?;
			if inventory.sequence == covered {
				require(inventory.digest == digest && inventory.checkpoint(id)? == checkpoint)?
			}
		}
		if rest > 0 {
			require(i + 1 == segments.len())?;
			tail = Some((name, 144 + records * 512, len));
		}
	}
	require(covered <= inventory.sequence)?;
	let mut debt_files = pending.len();
	let mut debt_bytes = 0u64;
	let mut names = 9 + pending.len() + segments.len();
	// Bounded directory vectors are transient and dropped one directory at a time.
	// Inventory itself stores fixed summaries, never journal bodies or decoded claims.
	let mut damaged_parents = 0;
	let mut bad_objects = 0;
	let mut unavailable_owners = Vec::new();
	for dir in ["data", "index", "journals", "grants", "quarantine"] {
		let limit = match dir {
			"data" | "journals" => PARENTS,
			"index" => PARENTS + TEMP_FILES,
			"grants" => GRANTS,
			_ => TEMP_FILES,
		};
		let entries = io.list(&root.join(dir), limit).await?;
		let mut owners = Vec::<Digest2>::new();
		let mut lookup = Vec::<(Digest2, Digest2)>::new();
		if ["data", "index", "journals"].contains(&dir) {
			for (key, p) in &inventory.parents {
				let name = match dir {
					"data" => hex(&p.data),
					"index" => hex(&p.index),
					_ => p.owner.locator()?,
				};
				let mut name_key = [0; 32];
				if dir == "journals" {
					name_key = hex_octets(&name)?
				} else {
					name_key[..16].copy_from_slice(&id_name(&name)?)
				}
				lookup.push((name_key, *key));
			}
		} else if dir == "grants" {
			for (key, g) in &inventory.grants {
				let mut name_key = [0; 32];
				name_key[..16].copy_from_slice(&g.object);
				lookup.push((name_key, *key));
			}
		}
		lookup.sort_unstable();
		for e in entries {
			require(!e.directory)?;
			names += 1;
			let path = root.join(dir).join(&e.name);
			if dir == "quarantine" {
				id_name(&e.name)?;
				debt_files += 1;
				let h = io.open(&path, false, false).await?;
				let n = io.adapter.length(&h.handle).await?;
				materialized = materialized.checked_add(n).ok_or(StoreError::Limit)?;
				require(n <= JOURNAL_CAP as u64)?;
				debt_bytes = debt_bytes.checked_add(n).ok_or(StoreError::Limit)?;
				continue;
			}
			if dir == "journals" {
				require(
					e.name.len() == 64
						&& e.name
							.bytes()
							.all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
				)?;
				// Full repeated owner comes from its bounded header, not an unbounded name search.
				let h = io.open(&path, false, false).await?;
				let n = io.adapter.length(&h.handle).await?;
				materialized = materialized.checked_add(n).ok_or(StoreError::Limit)?;
				if n > JOURNAL_CAP as u64 {
					return Err(StoreError::Limit);
				}
				// The full canonical filename must name a logged tuple, even with a damaged header.
				let name_key = hex_octets::<32>(&e.name)?;
				let pos = lookup
					.binary_search_by_key(&name_key, |p| p.0)
					.map_err(|_| StoreError::Unavailable)?;
				let expected = inventory
					.parents
					.get(&lookup[pos].1)
					.ok_or(StoreError::Unavailable)?;
				if n > expected.credit.journal as u64 {
					return Err(StoreError::Limit);
				}
				if n >= 269 {
					let raw = io.read(&h, 0, 269, &mut work).await?;
					if let Ok(mut r) = payload(&raw, b"VISJHD01", 269) {
						let parsed = (|| -> StoreResult<Digest2> {
							require(ContainerId::read(&mut r)? == id)?;
							r.take(16)?;
							let o = owner(&mut r)?;
							r.end()?;
							Ok(sha(&o.bytes()?))
						})();
						if let Ok(key) = parsed {
							owners.push(key)
						}
					}
				}
				drop(h);
				let chain = scan_chain(io, &path, id, expected, &mut work).await?;
				if chain.damaged.is_some() {
					damaged_parents += 1;
					unavailable_owners.push(sha(&expected.owner.bytes()?));
				}
				continue;
			}
			let (base, is_pending) = if dir == "index" {
				if let Some((base, suffix)) = e.name.split_once(".pending.") {
					id_name(suffix)?;
					(base, true)
				} else {
					(e.name.as_str(), false)
				}
			} else {
				(e.name.as_str(), false)
			};
			let object_id = id_name(base)?;
			let mut name_key = [0; 32];
			name_key[..16].copy_from_slice(&object_id);
			let pos = lookup
				.binary_search_by_key(&name_key, |p| p.0)
				.map_err(|_| StoreError::Unavailable)?;
			let key = lookup[pos].1;
			let expected = match dir {
				"data" => inventory.parents.get(&key).map(|p| p.credit.data as usize),
				"index" => inventory.parents.get(&key).map(|p| p.credit.index as usize),
				"grants" => inventory.grants.get(&key).map(|g| g.bytes as usize),
				_ => None,
			}
			.ok_or(StoreError::Unavailable)?;
			let h = io.open(&path, false, false).await?;
			let n = io.adapter.length(&h.handle).await?;
			materialized = materialized.checked_add(n).ok_or(StoreError::Limit)?;
			if n > expected as u64 {
				return Err(StoreError::Limit);
			}
			if is_pending {
				debt_files += 1;
				debt_bytes = debt_bytes.checked_add(n).ok_or(StoreError::Limit)?;
				continue;
			}
			// Per-parent object corruption stays local; shared metadata faults already refused.
			// Content trust and rebuild require a separate scoped loader call.
			if dir == "data" || dir == "index" {
				let raw = io.read(&h, 0, n as usize, &mut work).await?;
				let p = inventory.parents.get(&key).ok_or(StoreError::Unavailable)?;
				let parsed = (|| -> StoreResult<()> {
					require(raw.len() == expected)?;
					let kind = if dir == "data" { 1 } else { 2 };
					let inner = object_inner(&raw, id, kind, object_id, expected)?;
					if kind == 1 {
						let d = UnverifiedData2::decode(inner)?;
						p.credit.matches(&d)?;
						require(
							d.bundle.claims[0].0.manifest.identity == p.owner.identity
								&& d.bundle.claims[0].0.digest()? == p.owner.claim
								&& d.bundle.claims[0].1 == p.owner.reservation,
						)?;
					} else {
						let mut last = None;
						for bytes in inner.chunks_exact(303) {
							let index = UnverifiedIndex2::decode(bytes)?;
							let key = index.key()?;
							require(
								last.is_none_or(|old| old < key)
									&& index.identity == p.owner.identity
									&& index.claim == p.owner.claim
									&& index.reservation == p.owner.reservation
									&& index.object == p.data && index.offset == 85
									&& index.length == p.credit.data - 117,
							)?;
							last = Some(key);
						}
					}
					Ok(())
				})();
				if parsed.is_err() {
					bad_objects += 1;
					unavailable_owners.push(key)
				}
			}
			if dir == "grants" {
				let raw = io.read(&h, 0, n as usize, &mut work).await?;
				let inner = object_inner(&raw, id, 3, object_id, 655)?;
				let archive = UnverifiedGrantArchive2::decode(inner)?;
				let g = inventory.grants.get(&key).ok_or(StoreError::Unavailable)?;
				require(
					sha(inner) == g.digest
						&& archive.generation == id.generation
						&& archive.cluster == id.cluster,
				)?;
			}
		}
		// Global duplicate-owner accounting covers the complete stream, never independent batches.
		owners.sort_unstable();
		for pair in owners.windows(2) {
			if pair[0] == pair[1] {
				unavailable_owners.push(pair[0]);
			}
		}
	}
	for name in pending {
		let h = io.open(&root.join(name), false, false).await?;
		let n = io.adapter.length(&h.handle).await?;
		materialized = materialized.checked_add(n).ok_or(StoreError::Limit)?;
		require(n <= CHECKPOINT_CAP as u64)?;
		debt_bytes = debt_bytes.checked_add(n).ok_or(StoreError::Limit)?;
	}
	if debt_files > TEMP_FILES
		|| names > NAMESPACE
		|| inventory.charged + HEADROOM + debt_bytes > QUOTA
	{
		return Err(StoreError::Limit);
	}
	unavailable_owners.sort_unstable();
	unavailable_owners.dedup();
	if inventory.summary_bytes() + unavailable_owners.capacity() * 32 > 16 * 1024 * 1024 {
		return Err(StoreError::Limit);
	}
	Ok(Recovered {
		inventory,
		materialized,
		tail,
		debt_bytes,
		debt_files,
		damaged_parents,
		bad_objects,
		unavailable_owners,
	})
}
