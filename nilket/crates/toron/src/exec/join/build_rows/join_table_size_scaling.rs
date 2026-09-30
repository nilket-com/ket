//! ADR 619: a test-only table-size benchmark for the in-memory join tables of the ADR 618 scaling
//! cell (TPC-H query nine).
//!
//! It calls the production constructors and lookups: `build_in_memory_join_rows_with_insert_mode`
//! with the production default `Windowed` insert; Int64 lookups through `get_int64_batch` in
//! production's `PROBE_BATCH_WINDOW` windows (the restricted table API: the probe's key-window
//! preparation from Arrow arrays is outside the timed loop); and Generic lookups through
//! `join_key_bytes` then `get`, as the production Generic probe does per row, allocation included.
//!
//! Before any probe, each table's shape is validated against the scaling cell's hash-cited receipts
//! at that size (representation, distinct keys, entry and bucket capacity, and accounted index
//! bytes); a mismatch writes a named refusal and no timing for that build.
//!
//! The hash state is random per build (`ahash::RandomState::new()`), which a test cannot fix, so
//! each size is built five times independently, and each build is probed five times. For Int64
//! tables, the untimed probe-slot count (how many buckets production's linear probe examines per
//! key) is recorded per build. For the Generic table (a `HashMap`), probe work is not observable
//! from here, which is a stated limitation.
//!
//! Present keys come from a bijection, so an absent key never equals a present key; hash and
//! bucket collisions between them remain possible, and are what the probe-slot count measures.
//!
//! The fixtures run in the ladder. The measurement is `#[ignore]`d and run explicitly on strix, with
//! `ADR619_BENCH_OUT` naming a JSON-lines output file that must not already exist.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, ArrayRef, Int64Array};

use super::{
	BuildInsertMode, JoinBuildRowsInner, PROBE_BATCH_WINDOW, PrimitiveBuildTable,
	build_in_memory_join_rows_with_insert_mode,
};
use crate::exec::join::{JoinBuildRows, JoinKey, JoinKeyPlan, join_key_bytes};

/// SplitMix64's finalizer: a bijection on `u64`, so distinct inputs give distinct keys.
fn mix(mut z: u64) -> u64 {
	z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
	z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
	z ^ (z >> 31)
}

/// Present keys are `mix(base + i)` for `i < n`; absent keys use inputs at or beyond `base + n`, so
/// they never equal a present key.
fn int64_key(base: u64, i: u64) -> i64 {
	mix(base.wrapping_add(i)) as i64
}

const KEY_BASE: u64 = 0x6a09_e667_f3bc_c908;
const SECOND_COLUMN: u64 = 1 << 40;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
	/// Replays 1 and 3 of the scaling cell: one Int64 key column, `int64_primitive`.
	Int64,
	/// A synthetic two-Int64-column `generic_hash` table with accounting parity to replay 2's
	/// receipts; not a verified match to the current bound key of replay 2.
	GenericTwoInt64,
}

/// A table shape as the scaling cell's receipts record it, one per size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Receipt {
	representation: &'static str,
	distinct_keys: u64,
	entry_capacity: u64,
	bucket_capacity: u64,
	index_accounted_bytes: u64,
}

const fn receipt(
	representation: &'static str,
	n: u64,
	entries: u64,
	buckets: u64,
	bytes: u64,
) -> Receipt {
	Receipt {
		representation,
		distinct_keys: n,
		entry_capacity: entries,
		bucket_capacity: buckets,
		index_accounted_bytes: bytes,
	}
}

/// The ADR 618 receipts at SF8, SF16 and SF32, from the hash-cited extraction `adr619_pre_extract.py`.
const REPLAY1: [Receipt; 3] = [
	receipt("int64_primitive", 87_125, 87_125, 262_144, 6_279_152),
	receipt("int64_primitive", 173_930, 173_930, 524_288, 12_542_944),
	receipt("int64_primitive", 348_036, 348_036, 1_048_576, 25_094_336),
];
const REPLAY2: [Receipt; 3] = [
	receipt("generic_hash", 6_400_000, 7_340_032, 7_340_032, 792_002_560),
	receipt(
		"generic_hash",
		12_800_000,
		14_680_064,
		14_680_064,
		1_584_005_120,
	),
	receipt(
		"generic_hash",
		25_600_000,
		29_360_128,
		29_360_128,
		3_168_010_240,
	),
];
const REPLAY3: [Receipt; 3] = [
	receipt(
		"int64_primitive",
		12_000_000,
		12_000_000,
		33_554_432,
		844_435_456,
	),
	receipt(
		"int64_primitive",
		24_000_000,
		24_000_000,
		67_108_864,
		1_688_870_912,
	),
	receipt(
		"int64_primitive",
		48_000_000,
		48_000_000,
		134_217_728,
		3_377_741_824,
	),
];

fn observed(table: &JoinBuildRows) -> Receipt {
	let shape = table.profile_shape();
	Receipt {
		representation: shape.representation,
		distinct_keys: shape.distinct_keys,
		entry_capacity: shape.entry_capacity,
		bucket_capacity: shape.bucket_capacity,
		index_accounted_bytes: shape.index_accounted_bytes,
	}
}

/// The table must reproduce its receipt exactly before any probe; the error names every field.
fn validate(table: &JoinBuildRows, expected: &Receipt) -> Result<(), String> {
	let actual = observed(table);
	if actual == *expected {
		Ok(())
	} else {
		Err(format!(
			"table shape {actual:?} differs from receipt {expected:?}"
		))
	}
}

fn key_arrays(shape: Shape, n: usize) -> Vec<ArrayRef> {
	let first: Vec<i64> = (0..n as u64).map(|i| int64_key(KEY_BASE, i)).collect();
	match shape {
		Shape::Int64 => vec![Arc::new(Int64Array::from(first)) as ArrayRef],
		Shape::GenericTwoInt64 => {
			let second: Vec<i64> = (0..n as u64)
				.map(|i| int64_key(KEY_BASE ^ SECOND_COLUMN, i))
				.collect();
			vec![
				Arc::new(Int64Array::from(first)) as ArrayRef,
				Arc::new(Int64Array::from(second)) as ArrayRef,
			]
		}
	}
}

fn build(shape: Shape, arrays: &[ArrayRef], n: usize) -> JoinBuildRows {
	let plan = match shape {
		Shape::Int64 => JoinKeyPlan::Int64,
		Shape::GenericTwoInt64 => JoinKeyPlan::Generic,
	};
	build_in_memory_join_rows_with_insert_mode(
		plan,
		arrays,
		n,
		u64::MAX,
		false,
		BuildInsertMode::Windowed,
	)
	.expect("build")
	.expect("an uncapped build is admitted")
}

/// Probe indices, pre-generated outside any timed loop from a seeded stream: `Some(i)` probes present
/// key `i` (uniform over the present set), `None` an absent key, at the given hit fraction.
fn probe_indices(
	n: u64,
	probes: usize,
	hit_numerator: u64,
	hit_denominator: u64,
	seed: u64,
) -> Vec<Option<u64>> {
	(0..probes as u64)
		.map(|j| {
			let r = mix(seed ^ j.wrapping_mul(0x9e37_79b9_7f4a_7c15));
			if r % hit_denominator < hit_numerator {
				Some(mix(r) % n)
			} else {
				None
			}
		})
		.collect()
}

fn int64_probe_keys(indices: &[Option<u64>], n: u64) -> Vec<Option<i64>> {
	indices
		.iter()
		.enumerate()
		.map(|(j, index)| {
			Some(match index {
				Some(i) => int64_key(KEY_BASE, *i),
				None => int64_key(KEY_BASE, n + j as u64),
			})
		})
		.collect()
}

fn generic_probe_arrays(indices: &[Option<u64>], n: u64) -> Vec<ArrayRef> {
	let pick = |base: u64| -> Vec<i64> {
		indices
			.iter()
			.enumerate()
			.map(|(j, index)| match index {
				Some(i) => int64_key(base, *i),
				None => int64_key(base, n + j as u64),
			})
			.collect()
	};
	vec![
		Arc::new(Int64Array::from(pick(KEY_BASE))) as ArrayRef,
		Arc::new(Int64Array::from(pick(KEY_BASE ^ SECOND_COLUMN))) as ArrayRef,
	]
}

struct LookupOutcome {
	hits: u64,
	misses: u64,
	checksum: u64,
	nanos: u128,
}

fn lookup_int64(table: &JoinBuildRows, keys: &[Option<i64>]) -> LookupOutcome {
	let mut out = Vec::with_capacity(PROBE_BATCH_WINDOW);
	let (mut hits, mut misses, mut checksum) = (0u64, 0u64, 0u64);
	let start = Instant::now();
	for window in keys.chunks(PROBE_BATCH_WINDOW) {
		out.clear();
		table.get_int64_batch(window, &mut out);
		for found in &out {
			match found {
				Some(rows) => {
					hits += 1;
					checksum = checksum.wrapping_add(u64::from(rows.rows().next().unwrap_or(0)));
				}
				None => misses += 1,
			}
		}
	}
	let nanos = start.elapsed().as_nanos();
	LookupOutcome {
		hits,
		misses,
		checksum: black_box(checksum),
		nanos,
	}
}

fn lookup_generic(table: &JoinBuildRows, probe_arrays: &[ArrayRef], rows: usize) -> LookupOutcome {
	let (mut hits, mut misses, mut checksum) = (0u64, 0u64, 0u64);
	let start = Instant::now();
	for row in 0..rows {
		match join_key_bytes(probe_arrays, row).expect("encode") {
			Some(bytes) => match table.get(&JoinKey::Generic(bytes)) {
				Some(found) => {
					hits += 1;
					checksum = checksum.wrapping_add(u64::from(found.rows().next().unwrap_or(0)));
				}
				None => misses += 1,
			},
			None => misses += 1,
		}
	}
	let nanos = start.elapsed().as_nanos();
	LookupOutcome {
		hits,
		misses,
		checksum: black_box(checksum),
		nanos,
	}
}

/// Row `i` holds present key `i`, so the expected checksum is the sum of the present indices probed.
fn expected(indices: &[Option<u64>]) -> (u64, u64, u64) {
	let hits = indices.iter().filter(|index| index.is_some()).count() as u64;
	let checksum = indices
		.iter()
		.flatten()
		.fold(0u64, |sum, index| sum.wrapping_add(*index));
	(hits, indices.len() as u64 - hits, checksum)
}

/// Untimed: how many buckets production's linear probe examines for each key, split into hits and
/// misses, walking the same bucket sequence as `find_bucket`.
fn probe_slots(table: &JoinBuildRows, keys: &[Option<i64>]) -> Option<ProbeSlots> {
	let JoinBuildRowsInner::Int64(table) = &table.inner else {
		return None;
	};
	let mut slots = ProbeSlots::default();
	for key in keys.iter().flatten() {
		let (examined, hit) = examine(table, *key);
		if hit {
			slots.hit_keys += 1;
			slots.hit_slots += examined;
			slots.hit_max = slots.hit_max.max(examined);
		} else {
			slots.miss_keys += 1;
			slots.miss_slots += examined;
			slots.miss_max = slots.miss_max.max(examined);
		}
	}
	Some(slots)
}

fn examine(table: &PrimitiveBuildTable<i64>, key: i64) -> (u64, bool) {
	let mask = table.buckets.len() - 1;
	let mut bucket = table.bucket_for_key(key);
	let mut examined = 0u64;
	loop {
		examined += 1;
		let entry_index = table.buckets[bucket];
		if entry_index == super::EMPTY_PRIMITIVE_BUCKET {
			return (examined, false);
		}
		if table.entries[entry_index].key == key {
			return (examined, true);
		}
		bucket = (bucket + 1) & mask;
	}
}

#[derive(Default, Debug)]
struct ProbeSlots {
	hit_keys: u64,
	hit_slots: u64,
	hit_max: u64,
	miss_keys: u64,
	miss_slots: u64,
	miss_max: u64,
}

#[test]
fn int64_table_matches_production_accounting_and_counts_hits_exactly() {
	let n = REPLAY1[0].distinct_keys;
	let table = build(
		Shape::Int64,
		&key_arrays(Shape::Int64, n as usize),
		n as usize,
	);
	validate(&table, &REPLAY1[0]).expect("the SF8 replay-1 receipt is reproduced exactly");
	let indices = probe_indices(n, 10_000, 2_610_723, 47_989_007, 7);
	let keys = int64_probe_keys(&indices, n);
	let outcome = lookup_int64(&table, &keys);
	assert_eq!(
		(outcome.hits, outcome.misses, outcome.checksum),
		expected(&indices)
	);
	assert!(outcome.hits > 0 && outcome.misses > outcome.hits);
	let slots = probe_slots(&table, &keys).expect("an Int64 table exposes probe slots");
	assert_eq!(
		(slots.hit_keys, slots.miss_keys),
		(outcome.hits, outcome.misses)
	);
	assert!(slots.hit_slots >= slots.hit_keys && slots.miss_slots >= slots.miss_keys);
}

#[test]
fn two_column_generic_table_counts_hits_exactly() {
	let n = 5_000u64;
	let table = build(
		Shape::GenericTwoInt64,
		&key_arrays(Shape::GenericTwoInt64, n as usize),
		n as usize,
	);
	assert_eq!(observed(&table).representation, "generic_hash");
	assert_eq!(observed(&table).distinct_keys, n);
	assert!(probe_slots(&table, &[Some(1)]).is_none());
	for (numerator, denominator) in [(1, 1), (1, 3)] {
		let indices = probe_indices(n, 4_000, numerator, denominator, 11);
		let outcome = lookup_generic(&table, &generic_probe_arrays(&indices, n), indices.len());
		assert_eq!(
			(outcome.hits, outcome.misses, outcome.checksum),
			expected(&indices)
		);
	}
}

#[test]
fn a_table_that_differs_from_its_receipt_is_refused() {
	let n = REPLAY1[0].distinct_keys;
	let table = build(
		Shape::Int64,
		&key_arrays(Shape::Int64, n as usize),
		n as usize,
	);
	for wrong in [
		Receipt {
			index_accounted_bytes: REPLAY1[0].index_accounted_bytes + 1,
			..REPLAY1[0]
		},
		Receipt {
			representation: "generic_hash",
			..REPLAY1[0]
		},
		REPLAY1[1],
	] {
		let error = validate(&table, &wrong).expect_err("a mismatched receipt is refused");
		assert!(error.contains("differs from receipt"), "{error}");
	}
}

/// The measurement. Run on strix, pinned to one verified P-core hardware thread, by its full name:
/// `ADR619_BENCH_OUT=<new file> <test-binary>
/// exec::join::build_rows::join_table_size_scaling::measure_join_table_size_scaling --ignored --exact
/// --test-threads 1`.
#[test]
#[ignore = "ADR 619 measurement; run explicitly on strix"]
fn measure_join_table_size_scaling() {
	use std::io::Write;
	let path = std::env::var("ADR619_BENCH_OUT").expect("ADR619_BENCH_OUT names the output file");
	let mut sink = std::fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(&path)
		.expect("a fresh output file");
	// (label, shape, receipts at SF8/16/32, probes per pass, hit fraction)
	let cases: [(&str, Shape, [Receipt; 3], usize, (u64, u64)); 4] = [
		(
			"replay1_observed_mixture",
			Shape::Int64,
			REPLAY1,
			16_000_000,
			(2_610_723, 47_989_007),
		),
		(
			"replay1_successful",
			Shape::Int64,
			REPLAY1,
			16_000_000,
			(1, 1),
		),
		(
			"replay3_successful",
			Shape::Int64,
			REPLAY3,
			8_000_000,
			(1, 1),
		),
		(
			"replay2_successful",
			Shape::GenericTwoInt64,
			REPLAY2,
			4_000_000,
			(1, 1),
		),
	];
	const BUILDS: usize = 5;
	const PASSES: usize = 5;
	for (case, shape, receipts, probes, (numerator, denominator)) in cases {
		for build_rep in 0..BUILDS {
			// The size order rotates with the build repetition.
			for k in 0..3 {
				let size = (k + build_rep) % 3;
				let expected_receipt = receipts[size];
				let n = expected_receipt.distinct_keys;
				let arrays = key_arrays(shape, n as usize);
				let indices = probe_indices(n, probes, numerator, denominator, 0xad06_19 ^ n);
				let expected_outcome = expected(&indices);
				let int64_keys = (shape == Shape::Int64).then(|| int64_probe_keys(&indices, n));
				let generic_arrays =
					(shape == Shape::GenericTwoInt64).then(|| generic_probe_arrays(&indices, n));
				let start = Instant::now();
				let table = build(shape, &arrays, n as usize);
				let build_nanos = start.elapsed().as_nanos() as u64;
				let actual = observed(&table);
				let refusal = validate(&table, &expected_receipt).err();
				// Probe-slot inspection only after the shape is accepted: a refused table is never probed.
				let slots = if refusal.is_none() {
					int64_keys
						.as_deref()
						.and_then(|keys| probe_slots(&table, keys))
				} else {
					None
				};
				writeln!(
					sink,
					"{}",
					serde_json::json!({
						"record": "build", "case": case, "size": size, "keys": n, "build_rep": build_rep,
						"build_nanos": build_nanos, "representation": actual.representation,
						"distinct_keys": actual.distinct_keys, "entry_capacity": actual.entry_capacity,
						"bucket_capacity": actual.bucket_capacity, "index_accounted_bytes": actual.index_accounted_bytes,
						"refusal": refusal,
						"probe_slots": slots.map(|s| serde_json::json!({
							"hit_keys": s.hit_keys, "hit_slots": s.hit_slots, "hit_max": s.hit_max,
							"miss_keys": s.miss_keys, "miss_slots": s.miss_slots, "miss_max": s.miss_max,
						})),
					})
				)
				.expect("write");
				if refusal.is_some() {
					continue;
				}
				let run = |table: &JoinBuildRows| match shape {
					Shape::Int64 => lookup_int64(table, int64_keys.as_deref().expect("keys")),
					Shape::GenericTwoInt64 => {
						lookup_generic(table, generic_arrays.as_deref().expect("arrays"), probes)
					}
				};
				// One untimed warm pass, then the measured passes over the same table.
				let warm = run(&table);
				assert_eq!(
					(warm.hits, warm.misses, warm.checksum),
					expected_outcome,
					"{case} n={n}"
				);
				for pass in 0..PASSES {
					let outcome = run(&table);
					assert_eq!(
						(outcome.hits, outcome.misses, outcome.checksum),
						expected_outcome,
						"{case} n={n}"
					);
					writeln!(
						sink,
						"{}",
						serde_json::json!({
							"record": "lookup", "case": case, "size": size, "keys": n, "build_rep": build_rep,
							"pass": pass, "probes": probes, "hits": outcome.hits, "misses": outcome.misses,
							"lookup_nanos": outcome.nanos as u64,
						})
					)
					.expect("write");
				}
			}
		}
	}
}

// ADR 620: controlled synthetic comparisons at the SF8 replay-2 shape. Prototype tables keyed by the
// production two-Int64 key encoding, stored owned (a heap `Vec<u8>` per entry) or inline (`[u8; 18]`).
// Every form hashes through `<[u8] as Hash>` and borrows as `[u8]`, so owned, inline and borrowed
// lookups share one hash path; each paired build clones one `RandomState`.

/// Two Int64 columns, each a tag byte and eight little-endian bytes.
const TWO_INT64_KEY_BYTES: usize = 18;

#[derive(PartialEq, Eq)]
struct OwnedKey(Vec<u8>);

#[derive(PartialEq, Eq)]
struct InlineKey([u8; TWO_INT64_KEY_BYTES]);

impl std::hash::Hash for OwnedKey {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		<[u8] as std::hash::Hash>::hash(self.0.as_slice(), state);
	}
}

impl std::hash::Hash for InlineKey {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		<[u8] as std::hash::Hash>::hash(self.0.as_slice(), state);
	}
}

impl std::borrow::Borrow<[u8]> for OwnedKey {
	fn borrow(&self) -> &[u8] {
		&self.0
	}
}

impl std::borrow::Borrow<[u8]> for InlineKey {
	fn borrow(&self) -> &[u8] {
		&self.0
	}
}

type OwnedTable = std::collections::HashMap<OwnedKey, u32, ahash::RandomState>;
type InlineTable = std::collections::HashMap<InlineKey, u32, ahash::RandomState>;

/// The production encoding into a reused buffer: the same per-column encoder `join_key_bytes` uses.
fn encode_into(arrays: &[ArrayRef], row: usize, scratch: &mut Vec<u8>) -> bool {
	scratch.clear();
	for array in arrays {
		if array.is_null(row) {
			return false;
		}
		crate::exec::encode_partition_key_value(array.as_ref(), row, scratch).expect("encode");
	}
	true
}

fn build_owned(arrays: &[ArrayRef], n: usize, state: &ahash::RandomState) -> OwnedTable {
	let mut table = OwnedTable::with_capacity_and_hasher(n, state.clone());
	for row in 0..n {
		let key = join_key_bytes(arrays, row)
			.expect("encode")
			.expect("non-null key");
		table.insert(OwnedKey(key), row as u32);
	}
	table
}

fn build_inline(arrays: &[ArrayRef], n: usize, state: &ahash::RandomState) -> InlineTable {
	let mut table = InlineTable::with_capacity_and_hasher(n, state.clone());
	let mut scratch = Vec::with_capacity(TWO_INT64_KEY_BYTES);
	for row in 0..n {
		assert!(encode_into(arrays, row, &mut scratch));
		let mut key = [0u8; TWO_INT64_KEY_BYTES];
		key.copy_from_slice(&scratch);
		table.insert(InlineKey(key), row as u32);
	}
	table
}

/// C3's two probe loops, over the same owned table and the same borrowed `&[u8]` lookup: a fresh
/// `Vec` per probe (`join_key_bytes`), or one reused scratch buffer.
fn probe_fresh(table: &OwnedTable, probes: &[ArrayRef], rows: usize) -> LookupOutcome {
	let (mut hits, mut misses, mut checksum) = (0u64, 0u64, 0u64);
	let start = Instant::now();
	for row in 0..rows {
		let key = join_key_bytes(probes, row)
			.expect("encode")
			.expect("non-null key");
		match table.get(key.as_slice()) {
			Some(found) => {
				hits += 1;
				checksum = checksum.wrapping_add(u64::from(*found));
			}
			None => misses += 1,
		}
	}
	let nanos = start.elapsed().as_nanos();
	LookupOutcome {
		hits,
		misses,
		checksum: black_box(checksum),
		nanos,
	}
}

fn probe_scratch<K: std::borrow::Borrow<[u8]> + std::hash::Hash + Eq>(
	table: &std::collections::HashMap<K, u32, ahash::RandomState>,
	probes: &[ArrayRef],
	rows: usize,
) -> LookupOutcome {
	let (mut hits, mut misses, mut checksum) = (0u64, 0u64, 0u64);
	let mut scratch = Vec::with_capacity(TWO_INT64_KEY_BYTES);
	let start = Instant::now();
	for row in 0..rows {
		assert!(encode_into(probes, row, &mut scratch));
		match table.get(scratch.as_slice()) {
			Some(found) => {
				hits += 1;
				checksum = checksum.wrapping_add(u64::from(*found));
			}
			None => misses += 1,
		}
	}
	let nanos = start.elapsed().as_nanos();
	LookupOutcome {
		hits,
		misses,
		checksum: black_box(checksum),
		nanos,
	}
}

fn timed_drop<T>(value: T) -> u64 {
	let start = Instant::now();
	drop(black_box(value));
	start.elapsed().as_nanos() as u64
}

#[test]
fn owned_inline_and_borrowed_key_forms_share_bytes_hash_and_lookups() {
	let n = 2_000u64;
	let arrays = key_arrays(Shape::GenericTwoInt64, n as usize);
	let state = ahash::RandomState::new();
	let mut scratch = Vec::new();
	for row in 0..n as usize {
		let owned = join_key_bytes(&arrays, row)
			.expect("encode")
			.expect("non-null key");
		assert!(encode_into(&arrays, row, &mut scratch));
		assert_eq!(owned.len(), TWO_INT64_KEY_BYTES);
		assert_eq!(owned, scratch);
		let mut inline = [0u8; TWO_INT64_KEY_BYTES];
		inline.copy_from_slice(&scratch);
		let borrowed: &[u8] = &scratch;
		let (h_owned, h_inline, h_borrowed) = (
			state.hash_one(&OwnedKey(owned.clone())),
			state.hash_one(&InlineKey(inline)),
			state.hash_one(borrowed),
		);
		assert_eq!((h_owned, h_inline), (h_borrowed, h_borrowed));
	}
	let owned = build_owned(&arrays, n as usize, &state);
	let inline = build_inline(&arrays, n as usize, &state);
	for (numerator, denominator) in [(1, 1), (1, 3)] {
		let indices = probe_indices(n, 3_000, numerator, denominator, 5);
		let probes = generic_probe_arrays(&indices, n);
		let want = expected(&indices);
		for outcome in [
			probe_fresh(&owned, &probes, indices.len()),
			probe_scratch(&owned, &probes, indices.len()),
			probe_scratch(&inline, &probes, indices.len()),
		] {
			assert_eq!((outcome.hits, outcome.misses, outcome.checksum), want);
		}
	}
}

/// ADR 620's measurement. Run on strix, pinned, by its full name:
/// `ADR620_BENCH_OUT=<new file> <test-binary>
/// exec::join::build_rows::join_table_size_scaling::measure_generic_key_ownership --ignored --exact
/// --test-threads 1`.
#[test]
#[ignore = "ADR 620 measurement; run explicitly on strix"]
fn measure_generic_key_ownership() {
	use std::io::Write;
	let path = std::env::var("ADR620_BENCH_OUT").expect("ADR620_BENCH_OUT names the output file");
	let mut sink = std::fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(&path)
		.expect("a fresh output file");
	let receipt = REPLAY2[0];
	let n = receipt.distinct_keys;
	let probes_n = 2_610_723usize;
	let arrays = key_arrays(Shape::GenericTwoInt64, n as usize);
	let indices = probe_indices(n, probes_n, 1, 1, 0x0620);
	let probes = generic_probe_arrays(&indices, n);
	let want = expected(&indices);
	const REPS: usize = 5;
	const PASSES: usize = 5;
	let emit = |sink: &mut std::fs::File, value: serde_json::Value| {
		writeln!(sink, "{value}").expect("write")
	};
	for rep in 0..REPS {
		let state = ahash::RandomState::new();
		// Builds, in an order rotated by the repetition.
		let mut owned = None;
		let mut inline = None;
		let mut production = None;
		for k in 0..3 {
			let start = Instant::now();
			let variant = match (k + rep) % 3 {
				0 => {
					owned = Some(build_owned(&arrays, n as usize, &state));
					"owned"
				}
				1 => {
					inline = Some(build_inline(&arrays, n as usize, &state));
					"inline"
				}
				_ => {
					production = Some(build(Shape::GenericTwoInt64, &arrays, n as usize));
					"production"
				}
			};
			let nanos = start.elapsed().as_nanos() as u64;
			emit(
				&mut sink,
				serde_json::json!({"record": "build", "variant": variant, "rep": rep, "keys": n, "nanos": nanos}),
			);
		}
		let (owned, inline, production) = (
			owned.expect("owned"),
			inline.expect("inline"),
			production.expect("production"),
		);
		// Production accounting parity is checked against the receipt; the prototypes (u32 values,
		// different key layouts) are reported by their own length and capacity, never against it.
		// All three tables coexist during the lookups, unlike a sole production table in a query.
		let refusal = validate(&production, &receipt).err();
		let actual = observed(&production);
		emit(
			&mut sink,
			serde_json::json!({
				"record": "shape", "rep": rep, "refusal": refusal,
				"production": {
					"representation": actual.representation, "distinct_keys": actual.distinct_keys,
					"entry_capacity": actual.entry_capacity, "bucket_capacity": actual.bucket_capacity,
					"index_accounted_bytes": actual.index_accounted_bytes,
				},
				"receipt": {
					"representation": receipt.representation, "distinct_keys": receipt.distinct_keys,
					"entry_capacity": receipt.entry_capacity, "bucket_capacity": receipt.bucket_capacity,
					"index_accounted_bytes": receipt.index_accounted_bytes,
				},
				"owned_len": owned.len(), "owned_capacity": owned.capacity(),
				"inline_len": inline.len(), "inline_capacity": inline.capacity(),
			}),
		);
		if refusal.is_some() {
			continue;
		}
		// Lookups: an untimed warm pass per variant, then rotated measured passes.
		let variants: [&str; 4] = [
			"fresh_owned",
			"scratch_owned",
			"scratch_inline",
			"production",
		];
		let run = |variant: &str| match variant {
			"fresh_owned" => probe_fresh(&owned, &probes, probes_n),
			"scratch_owned" => probe_scratch(&owned, &probes, probes_n),
			"scratch_inline" => probe_scratch(&inline, &probes, probes_n),
			_ => lookup_generic(&production, &probes, probes_n),
		};
		for variant in variants {
			let warm = run(variant);
			assert_eq!((warm.hits, warm.misses, warm.checksum), want, "{variant}");
		}
		for pass in 0..PASSES {
			for k in 0..variants.len() {
				let variant = variants[(k + pass + rep) % variants.len()];
				let outcome = run(variant);
				assert_eq!(
					(outcome.hits, outcome.misses, outcome.checksum),
					want,
					"{variant}"
				);
				emit(
					&mut sink,
					serde_json::json!({
						"record": "lookup", "variant": variant, "rep": rep, "pass": pass, "probes": probes_n,
						"hits": outcome.hits, "misses": outcome.misses, "nanos": outcome.nanos as u64,
						"checksum": outcome.checksum, "expected_hits": want.0, "expected_misses": want.1,
						"expected_checksum": want.2,
					}),
				);
			}
		}
		// Drops, in an order rotated by the repetition.
		let mut tables = (Some(owned), Some(inline), Some(production));
		for k in 0..3 {
			let (variant, nanos) = match (k + rep) % 3 {
				0 => ("owned", timed_drop(tables.0.take())),
				1 => ("inline", timed_drop(tables.1.take())),
				_ => ("production", timed_drop(tables.2.take())),
			};
			emit(
				&mut sink,
				serde_json::json!({"record": "drop", "variant": variant, "rep": rep, "keys": n, "nanos": nanos}),
			);
		}
	}
}
