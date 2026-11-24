//! Wendelon ADR 0128: what a verification call's frame set retains and charges, at the store.
//! - A kept frame's charge is the sum of its parts, each compared with a cost known apart from
//!   the charge: the stored frame's length, the decoded columns' data, the index entries'
//!   count, the backing's URI, a weak store pointer's emptied record.
//! - The allowance holds exactly the sum of the held frames' charges after every step: a kept
//!   frame, a hit, an index parsed on a hit, an eviction, a frame not kept, and the set's end.
//! - A frame held for one backing generation never serves a copy that resolves to another; a
//!   local backing that becomes tiered verifies each copy it serves from then on. A held frame
//!   keeps no store alive, and a frame whose index cannot be charged leaves on every outcome.

use super::super::lsm_driver::{
	BUFFER_OWNER_BYTES, FrameAllowance, FrameReuse, VERIFIER_FRAME_ENTRIES, WEAK_STORE_BYTES,
	batch_retained_bytes,
};
use super::*;

const LIMIT: u64 = 8 * 1024 * 1024;

/// `frames` multi-row frames of `rows` rows each in one sealed slab: the store, its slab, each
/// frame's stored bytes and each frame's row locations.
async fn frames_in_one_slab(
	tag: &str,
	frames: u64,
	rows: u64,
) -> (
	[crate::test_support::TestDir; 2],
	Arc<SlabManager>,
	Arc<dyn ObjectStore>,
	SlabId,
	Vec<Vec<u8>>,
	Vec<Vec<RecordLocation>>,
) {
	let (dirs, mgr, l2) = fresh_manager_with_l2(tag).await;
	let mut stored = Vec::new();
	let mut keys = Vec::new();
	for f in 0..frames {
		let listed: Vec<_> = (0..rows)
			.map(|n| {
				(
					id(0x7c00_0000 + f * 0x1_0000 + n),
					Locality::new(7),
					hlc(1_000 + f * 10_000 + n),
					n,
					false,
				)
			})
			.collect();
		let frame = encode_test_multirow_storage_frame_with_tombstones(&listed);
		let entries = storage_multirow_index_entries_from_frame(&frame)
			.unwrap()
			.expect("a multi-row frame");
		mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
			.await
			.unwrap();
		stored.push(frame);
		keys.push(listed);
	}
	let slab_id = force_seal_active(&mgr).await;
	let mgr = restart_tier_manager(mgr, &dirs, &l2).await;
	let locations = keys
		.iter()
		.map(|listed| {
			listed
				.iter()
				.map(|(key, _, at, _, _)| {
					let copies = mgr.version_locations(tenant(), *key, *at).unwrap();
					assert_eq!(copies.len(), 1, "premise: one copy each");
					copies[0]
				})
				.collect::<Vec<_>>()
		})
		.collect();
	(dirs, mgr, l2, slab_id, stored, locations)
}

/// Tiers `slab_id` to an object named `name`: the pointer applied.
async fn tier(
	mgr: &Arc<SlabManager>,
	l2: &Arc<dyn ObjectStore>,
	slab_id: SlabId,
	name: &str,
) -> TieredSlabPointer {
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
	let l2_uri = l2.put(name, &encrypted.bytes).await.unwrap();
	let pointer = TieredSlabPointer {
		slab_id,
		l2_uri,
		payload_hash: crate::hash::content::digest(&encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(encrypted.tenant_id),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: None,
		stats_block_len: None,
		stats_block_hash128: None,
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	mgr.apply_tier_slab(pointer.clone()).await.unwrap();
	pointer
}

/// The allowance holds exactly what the set's frames are charged, part by part.
fn held(
	set: &FrameReuse,
	allowance: &FrameAllowance,
) -> Vec<super::super::lsm_driver::FrameCharge> {
	let charges = set.held_charges_for_test();
	for (parts, charged) in &charges {
		assert_eq!(
			parts.total(),
			*charged,
			"a frame is charged its parts: {parts:?}"
		);
	}
	assert_eq!(
		allowance.used(),
		charges.iter().map(|(_, charged)| charged).sum::<u64>(),
		"the allowance holds the held frames' charges and nothing else"
	);
	charges.into_iter().map(|(parts, _)| parts).collect()
}

/// The data bytes a batch's columns hold, from their lengths (not their allocations).
fn column_data_bytes(batch: &RecordBatch) -> u64 {
	batch
		.columns()
		.iter()
		.map(|column| {
			column
				.to_data()
				.buffers()
				.iter()
				.map(|buffer| buffer.len() as u64)
				.sum::<u64>()
		})
		.sum()
}

/// A batch's retained bytes count each allocation once: columns that point into one shared
/// allocation are charged it once, columns with their own are charged each, and the schema's
/// names and metadata are charged at their lengths.
#[test]
fn a_batchs_retained_bytes_count_each_allocation_once() {
	use arrow::array::{ArrayRef, Int64Array};
	use arrow::buffer::{Buffer, ScalarBuffer};
	let schema = |names: [&str; 2], metadata: Option<(&str, &str)>| {
		let schema = Schema::new(vec![
			Field::new(names[0], DataType::Int64, false),
			Field::new(names[1], DataType::Int64, false),
		]);
		Arc::new(match metadata {
			Some((key, value)) => {
				schema.with_metadata([(key.to_owned(), value.to_owned())].into_iter().collect())
			}
			None => schema,
		})
	};
	let values: Vec<i64> = (0..2_000).collect();
	// Two columns, each its own allocation of 16,000 bytes.
	let own = RecordBatch::try_new(
		schema(["a", "b"], None),
		vec![
			Arc::new(Int64Array::from(values.clone())) as ArrayRef,
			Arc::new(Int64Array::from(values.clone())) as ArrayRef,
		],
	)
	.unwrap();
	// Two columns that are halves of ONE allocation of 16,000 bytes.
	let body = Buffer::from_vec(values.clone());
	let capacity = body.capacity() as u64;
	assert!(capacity >= 16_000);
	let half = |offset| {
		Arc::new(Int64Array::new(
			ScalarBuffer::new(body.clone(), offset, 1_000),
			None,
		)) as ArrayRef
	};
	let shared =
		RecordBatch::try_new(schema(["a", "b"], None), vec![half(0), half(1_000)]).unwrap();
	let (own_bytes, shared_bytes) = (batch_retained_bytes(&own), batch_retained_bytes(&shared));
	let summed = shared.get_array_memory_size() as u64;
	eprintln!(
		"ADR0128 batch bytes: own allocations {own_bytes}, one shared allocation {shared_bytes} (capacity {capacity}; summed per buffer {summed})"
	);
	assert!(
		summed >= 2 * capacity,
		"premise: the per-buffer sum counts it twice"
	);
	assert!(
		(capacity..capacity + 2_048).contains(&shared_bytes),
		"the shared allocation once, plus records: {shared_bytes}"
	);
	assert!(
		(2 * 16_000..2 * 16_000 + 2_048 + 128).contains(&own_bytes),
		"each own allocation: {own_bytes}"
	);
	let own_capacity = own.column(0).to_data().buffers()[0].capacity() as u64;
	assert_eq!(
		own_bytes - shared_bytes,
		2 * own_capacity - capacity + BUFFER_OWNER_BYTES as u64,
		"the two differ only by their allocations: two with an owner each against one"
	);
	// Names and metadata are charged.
	let name = "n".repeat(300);
	let named = RecordBatch::try_new(
		schema([&name, "b"], Some(("key", &"v".repeat(500)))),
		vec![half(0), half(1_000)],
	)
	.unwrap();
	let named_bytes = batch_retained_bytes(&named);
	assert!(
		named_bytes >= shared_bytes + 299 + 500 + 3,
		"a longer name and metadata are charged: {named_bytes} vs {shared_bytes}"
	);
}

/// A local frame kept by a charged set: its parts against known costs; hits change nothing;
/// the set's end returns everything.
#[tokio::test]
async fn a_kept_local_frames_charge_is_its_parts_against_known_costs() {
	let (_dirs, mgr, _l2, _slab, stored, locations) =
		frames_in_one_slab("verifier-frames-parts", 1, 512).await;
	let allowance = FrameAllowance::new(LIMIT);
	let mut set = FrameReuse::charged(VERIFIER_FRAME_ENTRIES, allowance.clone());
	assert!(
		held(&set, &allowance).is_empty(),
		"an empty set charges nothing"
	);
	let row = mgr
		.stored_row_reusing_frame(locations[0][0], &mut set)
		.await
		.unwrap();
	let parts = held(&set, &allowance);
	assert_eq!(parts.len(), 1);
	let parts = parts[0];
	let decoded = crate::storage::multirow::decode_batch(&stored[0]).unwrap();
	let data = column_data_bytes(&decoded);
	let summed = decoded.get_array_memory_size() as u64;
	eprintln!(
		"ADR0128 local frame parts {parts:?} total {}; stored frame {} B, column data {data} B, per-buffer sum {summed} B",
		parts.total(),
		stored[0].len()
	);
	assert!(parts.record > 0);
	assert!(
		(stored[0].len() as u64..2 * stored[0].len() as u64 + 4_096).contains(&parts.frame),
		"the stored frame, at capacity"
	);
	assert!(parts.batch >= data, "at least the columns' data");
	assert!(parts.batch <= summed, "never more than the per-buffer sum");
	assert_eq!(
		parts.batch,
		batch_retained_bytes(&decoded),
		"as an independent decode's"
	);
	assert_eq!(parts.index, 0, "a local, untiered frame parses no index");
	assert_eq!(
		parts.backing, WEAK_STORE_BYTES as u64,
		"a local store is not the frame's: only a weak pointer's emptied record"
	);
	// Every other copy is a hit: nothing read, nothing more charged.
	for location in &locations[0][1..] {
		mgr.stored_row_reusing_frame(*location, &mut set)
			.await
			.unwrap();
	}
	assert_eq!((set.stats.reads, set.stats.reused), (1, 511));
	assert_eq!(held(&set, &allowance), vec![parts]);
	// A caller's slice outlives the set without holding the allowance.
	drop(set);
	assert_eq!(allowance.used(), 0, "the set's end returns its charge");
	assert_eq!(row.num_rows(), 1);
}

/// A set of two entries over three frames, then an allowance with room for one: the least
/// recently used frame leaves, a frame without room is not kept, and the allowance is the sum
/// of the held charges after every step.
#[tokio::test]
async fn evictions_and_frames_not_kept_leave_the_allowance_at_the_held_sum() {
	let (_dirs, mgr, _l2, _slab, _stored, locations) =
		frames_in_one_slab("verifier-frames-evict", 3, 64).await;
	let allowance = FrameAllowance::new(LIMIT);
	let mut set = FrameReuse::charged(2, allowance.clone());
	for frame in 0..3 {
		mgr.stored_row_reusing_frame(locations[frame][0], &mut set)
			.await
			.unwrap();
		assert_eq!(held(&set, &allowance).len(), (frame + 1).min(2));
	}
	assert_eq!(
		(set.stats.reads, set.stats.evicted, set.stats.not_kept),
		(3, 1, 0)
	);
	// Frame 0 left; frames 1 and 2 serve from the set.
	mgr.stored_row_reusing_frame(locations[1][1], &mut set)
		.await
		.unwrap();
	mgr.stored_row_reusing_frame(locations[2][1], &mut set)
		.await
		.unwrap();
	assert_eq!((set.stats.reads, set.stats.reused), (3, 2));
	let one = held(&set, &allowance)[0].total();
	// Another call takes all but less than one frame's room: this set's next frame makes room
	// from its own frames only, and when that is not enough it is read and not kept.
	let mut other = FrameReuse::charged(VERIFIER_FRAME_ENTRIES, allowance.clone());
	allowance.set_limit_for_test(allowance.used() + one - 1);
	mgr.stored_row_reusing_frame(locations[0][2], &mut other)
		.await
		.unwrap();
	assert!(
		other.held_charges_for_test().is_empty(),
		"no room: not kept"
	);
	assert_eq!((other.stats.reads, other.stats.not_kept), (1, 1));
	assert_eq!(
		held(&set, &allowance).len(),
		2,
		"another call's frames are not evicted"
	);
	// A frame larger than the whole allowance evicts nothing.
	allowance.set_limit_for_test(one - 1);
	mgr.stored_row_reusing_frame(locations[0][3], &mut set)
		.await
		.unwrap();
	assert_eq!(set.held_charges_for_test().len(), 2, "its own frames stay");
	assert_eq!(set.stats.not_kept, 1);
	drop(other);
	drop(set);
	assert_eq!(allowance.used(), 0, "quiescent");
}

/// Gives a slab a tier pointer while its local file stays the backing: the object is never read.
async fn mark_tiered(mgr: &Arc<SlabManager>, slab_id: SlabId) {
	mgr.state.lock().await.tiered.insert(
		slab_id,
		TieredSlabPointer {
			slab_id,
			l2_uri: "verifier-frames-tiered/never-read.wnd".to_owned(),
			payload_hash: [7; 32],
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		},
	);
}

/// A kept frame whose index is first parsed on a hit, with no room for the index: the frame
/// leaves the set before the call returns, whether the copy is served or refused. The set is
/// inspected at once, still alive: nothing is held beyond what is charged.
#[tokio::test]
async fn a_hit_whose_index_cannot_be_charged_leaves_the_set_on_every_outcome() {
	for forged_copy in [false, true] {
		let (_dirs, mgr, _l2, slab_id, _stored, locations) =
			frames_in_one_slab("verifier-frames-growth", 1, 64).await;
		let allowance = FrameAllowance::new(LIMIT);
		let mut set = FrameReuse::charged(VERIFIER_FRAME_ENTRIES, allowance.clone());
		mgr.stored_row_reusing_frame(locations[0][0], &mut set)
			.await
			.unwrap();
		let before = held(&set, &allowance);
		assert_eq!((before.len(), before[0].index), (1, 0));
		mark_tiered(&mgr, slab_id).await;
		// Room for the frame as it is, and not one byte of index.
		allowance.set_limit_for_test(allowance.used());
		let mut copy = locations[0][1];
		if forged_copy {
			copy.hlc = hlc(999_999);
		}
		let ordinary = mgr.read_reusing(copy, None).await;
		let outcome = mgr.stored_row_reusing_frame(copy, &mut set).await;
		assert_eq!(outcome.is_err(), forged_copy);
		assert_eq!(
			outcome.map(|_| ()).map_err(|error| error.to_string()),
			ordinary.map(|_| ()).map_err(|error| error.to_string()),
			"the ordinary read's outcome"
		);
		let after = held(&set, &allowance);
		eprintln!(
			"ADR0128 index growth refused (forged copy {forged_copy}): before {before:?} after {after:?} used {} stats reads {} reused {} evicted {}",
			allowance.used(),
			set.stats.reads,
			set.stats.reused,
			set.stats.evicted
		);
		assert!(after.is_empty(), "the frame left: {after:?}");
		assert_eq!(allowance.used(), 0, "and its charge with it");
		assert_eq!(
			(set.stats.reads, set.stats.reused, set.stats.evicted),
			(1, u64::from(!forged_copy), 1),
			"served from the held frame, then released"
		);
		drop(set);
		assert_eq!(allowance.used(), 0);
	}
}

/// A frame kept from a local, untiered slab; the slab then gains a tier pointer while its
/// local file is still the backing (the state a read's `tiered` flag exists for). The next
/// copy at the same coordinates is a hit on the same backing, and it is verified against its
/// own version: the index is parsed then and charged. A copy naming another version than its
/// row's is refused from the held frame exactly as its ordinary read is refused.
#[tokio::test]
async fn a_local_backing_that_becomes_tiered_verifies_each_copy_it_serves() {
	let (_dirs, mgr, _l2, slab_id, _stored, locations) =
		frames_in_one_slab("verifier-frames-tiered", 1, 64).await;
	let allowance = FrameAllowance::new(LIMIT);
	let mut set = FrameReuse::charged(VERIFIER_FRAME_ENTRIES, allowance.clone());
	let first = mgr
		.stored_row_reusing_frame(locations[0][0], &mut set)
		.await
		.unwrap();
	let before = held(&set, &allowance)[0];
	assert_eq!(
		(set.stats.reads, set.stats.index_parses, before.index),
		(1, 0, 0)
	);
	// A forged copy is NOT refused while the slab is untiered: no copy is verified yet.
	let mut forged = locations[0][2];
	forged.hlc = hlc(999_999);
	assert!(
		mgr.stored_row_reusing_frame(forged, &mut set).await.is_ok(),
		"premise: an untiered local copy is not checked against its version"
	);
	mark_tiered(&mgr, slab_id).await;
	let second = mgr
		.stored_row_reusing_frame(locations[0][1], &mut set)
		.await
		.unwrap();
	let stats = set.stats;
	let after = held(&set, &allowance);
	eprintln!(
		"ADR0128 local backing newly tiered: reads {} reused {} index_parses {} before {before:?} after {after:?}",
		stats.reads, stats.reused, stats.index_parses
	);
	assert_ne!(first, second);
	assert_eq!(
		(stats.reads, stats.reused, stats.index_parses),
		(1, 2, 1),
		"the same backing: a hit, now verified against its version"
	);
	assert_eq!(after.len(), 1);
	assert!(
		after[0].index >= 64 * std::mem::size_of::<MultiRowIndexEntry>() as u64,
		"the parsed index is charged: {after:?}"
	);
	assert_eq!(
		after[0].total() - before.total(),
		after[0].index,
		"the growth is the index"
	);
	let ordinary = mgr.read_reusing(forged, None).await;
	assert!(ordinary.is_err(), "premise: the ordinary read refuses it");
	let refused = mgr.stored_row_reusing_frame(forged, &mut set).await;
	assert_eq!(
		refused.map(|_| ()).map_err(|error| error.to_string()),
		ordinary.map(|_| ()).map_err(|error| error.to_string()),
		"the same refusal"
	);
	assert_eq!(set.stats.reads, 1, "from the held frame");
	held(&set, &allowance);
	drop(set);
	assert_eq!(allowance.used(), 0);
}

/// A frame kept from a slab's local file; the slab is then tiered, so the same coordinates
/// resolve to another backing generation (the object). The same copy is read afresh (the held
/// frame serves nothing), returns the same row as before and as its ordinary read, and the new
/// frame serves its own copies, each verified. The new frame is charged its backing's URI,
/// which differs with the object's name. A copy naming another version is refused as its
/// ordinary read is.
#[tokio::test]
async fn a_changed_backing_generation_is_read_afresh_at_the_same_coordinates() {
	let mut backings = Vec::new();
	for name in ["g/o.wnd".to_owned(), format!("g/{}.wnd", "long".repeat(40))] {
		let (_dirs, mgr, l2, slab_id, _stored, locations) =
			frames_in_one_slab("verifier-frames-generation", 1, 64).await;
		let allowance = FrameAllowance::new(LIMIT);
		let mut set = FrameReuse::charged(VERIFIER_FRAME_ENTRIES, allowance.clone());
		let before_row = mgr
			.stored_row_reusing_frame(locations[0][0], &mut set)
			.await
			.unwrap();
		let before = held(&set, &allowance);
		assert_eq!((set.stats.reads, before.len()), (1, 1));
		assert_eq!(
			set.held_stores_alive_for_test(),
			vec![Some(true)],
			"premise: the local store is alive, owned by the node"
		);
		let pointer = tier(&mgr, &l2, slab_id, &name).await;
		// The set is still alive and still holds the local frame: the store it was read from
		// is gone with the node's last owner (its index, file and heap), not kept by the set.
		assert_eq!(
			(set.held_stores_alive_for_test(), held(&set, &allowance)),
			(vec![Some(false)], before.clone()),
			"the held frame keeps no store alive"
		);
		assert!(mgr.tier_pointer(slab_id).await.is_some(), "premise: tiered");
		let ordinary = mgr
			.stored_rows_at_version(tenant(), id(0x7c00_0000), locations[0][0].hlc, None)
			.await
			.unwrap();
		let again = mgr
			.stored_row_reusing_frame(locations[0][0], &mut set)
			.await
			.unwrap();
		assert_eq!(
			(set.stats.reads, set.stats.reused),
			(2, 0),
			"another generation: a fresh read, not the held frame"
		);
		assert_eq!(again, before_row);
		assert_eq!(again, ordinary[0], "the ordinary read's row");
		mgr.stored_row_reusing_frame(locations[0][1], &mut set)
			.await
			.unwrap();
		assert_eq!(
			(set.stats.reads, set.stats.reused, set.stats.index_parses),
			(2, 1, 1),
			"the new generation's frame serves its own copies, verified"
		);
		let mut forged = locations[0][2];
		forged.hlc = hlc(999_999);
		let refused_ordinary = mgr.read_reusing(forged, None).await;
		assert!(
			refused_ordinary.is_err(),
			"premise: the ordinary read refuses it"
		);
		assert_eq!(
			mgr.stored_row_reusing_frame(forged, &mut set)
				.await
				.map(|_| ())
				.map_err(|error| error.to_string()),
			refused_ordinary
				.map(|_| ())
				.map_err(|error| error.to_string()),
			"the same refusal"
		);
		let after = held(&set, &allowance);
		let fresh = after
			.iter()
			.find(|parts| !before.contains(parts))
			.expect("the new generation's frame is held");
		eprintln!(
			"ADR0128 changed generation ({} B uri): before {before:?} after {after:?}",
			pointer.l2_uri.len()
		);
		assert!(
			fresh.index > 0,
			"a tiered copy is verified: its index is held"
		);
		assert!(
			fresh.backing >= pointer.l2_uri.len() as u64,
			"the backing's URI is charged: {fresh:?}"
		);
		backings.push((pointer.l2_uri.len() as u64, fresh.backing));
		drop(set);
		assert_eq!(allowance.used(), 0);
	}
	assert_eq!(
		backings[1].1 - backings[0].1,
		backings[1].0 - backings[0].0,
		"the charge follows the URI's length: {backings:?}"
	);
}
