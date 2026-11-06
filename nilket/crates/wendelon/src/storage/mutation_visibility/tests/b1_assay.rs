//! Isolated cfg(test) allocator assay; never part of the ordinary library.
use super::provenance_tests::{batch, framed, key, parent, schema};
use super::*;
use crate::storage::mutation_visibility::{adapters::*, provenance::*};
use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
struct Counter;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static REQUESTED: AtomicUsize = AtomicUsize::new(0);
struct AssayKey {
	inner: TestProofKey,
	max: AtomicUsize,
}
#[cfg(test)]
impl ProofKeyProvider for AssayKey {
	fn keyed_digest(&self, b: &ProofKeyBinding, p: &[u8]) -> Result<[u8; 32], ProofKeyUnavailable> {
		self.max.fetch_max(p.len(), Ordering::Relaxed);
		self.inner.keyed_digest(b, p)
	}
}
fn copy_probe(schema: &ProofSchema, b: &RecordBatch) {
	let key_bytes = schema.encode_key(b, 0).unwrap();
	let row_bytes = schema.encode_row(b, 0).unwrap();
	let p = AssayKey {
		inner: key(),
		max: AtomicUsize::new(0),
	};
	let binding = manifest(1).binding();
	let token = key_token(
		&p,
		&binding,
		&identity(1),
		CanonicalKeyBytes::new(&key_bytes).unwrap(),
	)
	.unwrap();
	let key_preimage = p.max.swap(0, Ordering::Relaxed);
	let context = RowCommitmentContext {
		identity: identity(1),
		admission: Admission {
			epoch: 1,
			holder: 1,
		},
		operation: MutationOperation::Insert,
		child_ordinal: 0,
		row_ordinal: 0,
		key_token: token,
		tombstone: false,
	};
	row_content_commitment(
		&p,
		&binding,
		&context,
		CanonicalContentBytes::new(&row_bytes).unwrap(),
	)
	.unwrap();
	let row_preimage = p.max.load(Ordering::Relaxed);
	println!(
		"B1_COPY key_len={} key_capacity={} row_len={} row_capacity={} key_preimage_copy_len={} row_preimage_copy_len={} preimage_capacity_unavailable=true",
		key_bytes.len(),
		key_bytes.capacity(),
		row_bytes.len(),
		row_bytes.capacity(),
		key_preimage,
		row_preimage
	);
}
// Const-initialized, and a plain `Cell<u64>` has no destructor: reading it from the allocator
// allocates nothing and registers no teardown. `try_with` still covers thread exit.
thread_local! {
	static THREAD_ALLOCS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}
/// Allocation calls made by this thread so far, unaffected by anything else in the process.
pub(crate) fn thread_allocs() -> u64 {
	THREAD_ALLOCS.with(std::cell::Cell::get)
}
fn allocated(n: usize) {
	let live = LIVE.fetch_add(n, Ordering::Relaxed) + n;
	PEAK.fetch_max(live, Ordering::Relaxed);
	ALLOCS.fetch_add(1, Ordering::Relaxed);
	REQUESTED.fetch_add(n, Ordering::Relaxed);
	let _ = THREAD_ALLOCS.try_with(|count| count.set(count.get() + 1));
}
unsafe impl GlobalAlloc for Counter {
	unsafe fn alloc(&self, l: Layout) -> *mut u8 {
		let p = unsafe { System.alloc(l) };
		if !p.is_null() {
			allocated(l.size())
		}
		p
	}
	unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
		LIVE.fetch_sub(l.size(), Ordering::Relaxed);
		unsafe { System.dealloc(p, l) }
	}
	unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
		let q = unsafe { System.realloc(p, l, n) };
		if !q.is_null() {
			LIVE.fetch_sub(l.size(), Ordering::Relaxed);
			allocated(n)
		}
		q
	}
}
#[global_allocator]
static ALLOCATOR: Counter = Counter;
/// Process-wide requested-allocation counters. R0 uses only in an isolated exact test.
pub(crate) fn r0_counters() -> [usize; 4] {
	[
		LIVE.load(Ordering::Relaxed),
		PEAK.load(Ordering::Relaxed),
		ALLOCS.load(Ordering::Relaxed),
		REQUESTED.load(Ordering::Relaxed),
	]
}
pub(crate) fn r0_reset_peak() {
	PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
}
fn measure(name: &str, f: impl FnOnce() -> (usize, usize, usize, usize)) {
	let base = LIVE.load(Ordering::Relaxed);
	PEAK.store(base, Ordering::Relaxed);
	let allocs = ALLOCS.load(Ordering::Relaxed);
	let (input, canonical, encoded, owned) = f();
	let retained = LIVE.load(Ordering::Relaxed) as isize - base as isize;
	let peak = PEAK.load(Ordering::Relaxed).saturating_sub(base);
	let count = ALLOCS.load(Ordering::Relaxed) - allocs;
	println!(
		"B1_ASSAY name={name} input_bytes={input} canonical_bytes={canonical} encoded_bytes={encoded} owned_metadata_bytes={owned} peak_requested_bytes={peak} allocations={count} after_drop_delta={retained}"
	);
	assert_eq!(retained, 0, "unexpected retained allocation in {name}");
}
fn refs(n: usize) -> Vec<ProvenanceReference> {
	(0..n)
		.map(|i| ProvenanceReference {
			physical_row: i as u32,
			parent: 0,
			child: 0,
			row: i as u32,
		})
		.collect()
}
fn one_parent_case(n: usize, children: usize) -> (usize, usize, usize, usize) {
	let b = batch((0..n as i64).collect());
	let input = b.get_array_memory_size();
	let s = schema(&b);
	copy_probe(&s, &b);
	let canonical = (0..n)
		.map(|i| s.encode_key(&b, i).unwrap().len() + s.encode_row(&b, i).unwrap().len())
		.sum();
	let p = parent(1, &b, children);
	let frame = framed(&b, crate::schema::RecordBodyCompressionPolicy::Uncompressed);
	let reference = (0..n)
		.map(|i| ProvenanceReference {
			physical_row: i as u32,
			parent: 0,
			child: (i % children) as u32,
			row: (i / children) as u32,
		})
		.collect();
	let provenance =
		UnverifiedFrameProvenance::new(&identity(1), &frame, vec![p], reference).unwrap();
	let bytes = provenance.encode().unwrap();
	let encoded = bytes.len();
	let decoded = UnverifiedFrameProvenance::decode(&bytes).unwrap();
	let trusted = TrustedFrameRows::from_table_frame(frame, s).unwrap();
	let verified = decoded.verify(&key(), &trusted).unwrap();
	let owned = verified.provenance().owned_metadata_bytes();
	if n > 1 {
		let survivor = b.slice(n - 1, 1);
		let frame = framed(
			&survivor,
			crate::schema::RecordBodyCompressionPolicy::Uncompressed,
		);
		let r = ProvenanceReference {
			physical_row: 0,
			parent: 0,
			child: ((n - 1) % children) as u32,
			row: ((n - 1) / children) as u32,
		};
		let p = UnverifiedFrameProvenance::new(
			&identity(1),
			&frame,
			verified.provenance().parents().to_vec(),
			vec![r],
		)
		.unwrap();
		p.verify(
			&key(),
			&TrustedFrameRows::from_table_frame(frame, schema(&survivor)).unwrap(),
		)
		.unwrap();
	}
	(input, canonical, encoded, owned)
}
fn independent_parents() -> (usize, usize, usize, usize) {
	let b = batch((0..512).collect());
	let s = schema(&b);
	let input = b.get_array_memory_size();
	let mut parents = (0..512)
		.map(|i| parent(i as u64, &b.slice(i, 1), 1))
		.collect::<Vec<_>>();
	parents.sort_by_key(|p| p.manifest.identity.encode().unwrap());
	let references = (0..512)
		.map(|i| ProvenanceReference {
			physical_row: i as u32,
			parent: parents
				.iter()
				.position(|p| p.manifest.identity.sequence == i as u64)
				.unwrap() as u16,
			child: 0,
			row: 0,
		})
		.collect();
	let frame = framed(&b, crate::schema::RecordBodyCompressionPolicy::Uncompressed);
	let p = UnverifiedFrameProvenance::new(&identity(1), &frame, parents, references).unwrap();
	let bytes = p.encode().unwrap();
	let encoded = bytes.len();
	let decoded = UnverifiedFrameProvenance::decode(&bytes).unwrap();
	let owned = decoded.owned_metadata_bytes();
	decoded
		.verify(
			&key(),
			&TrustedFrameRows::from_table_frame(frame, s.clone()).unwrap(),
		)
		.unwrap();
	let canonical = (0..512)
		.map(|i| s.encode_key(&b, i).unwrap().len() + s.encode_row(&b, i).unwrap().len())
		.sum();
	(input, canonical, encoded, owned)
}
fn largest_structural_envelope() -> (usize, usize, usize, usize) {
	let b = batch((0..512).collect());
	let mut parents = (0..512)
		.map(|i| ProvenanceParent {
			manifest: manifest(i),
			commitment: [0; 32],
		})
		.collect::<Vec<_>>();
	// Base = 512 minimum dictionaries, 512 refs, header and envelope. Extra version 69,
	// extra child 31. Find an exact representable cap without ignored padding.
	let base = 95 + 46 + 512 * (212 + 36) + 512 * 14;
	let remaining = MAX_PROVENANCE_BYTES - base;
	let (versions, children) = (0..=255)
		.find_map(|children| {
			let used = 31 * children;
			if used <= remaining && (remaining - used) % 69 == 0 {
				Some(((remaining - used) / 69, children))
			} else {
				None
			}
		})
		.unwrap();
	let mut extra = versions;
	for (i, p) in parents.iter_mut().enumerate() {
		let n = 1 + extra.min(511);
		extra -= n - 1;
		let child_count = if i == 0 { children + 1 } else { 1 };
		assert!(n >= child_count);
		let mut cs = Vec::new();
		let mut input = 0usize;
		for child in 0..child_count {
			let count = if child == 0 { n - child_count + 1 } else { 1 };
			let vs = (0..count)
				.map(|ordinal| {
					let mut token = [0; 32];
					token[..8].copy_from_slice(&(input as u64).to_le_bytes());
					input += 1;
					LogicalVersion {
						ordinal: ordinal as u32,
						key_token: token,
						content_commitment: [0; 32],
						tombstone: false,
					}
				})
				.collect();
			cs.push(ManifestChild {
				ordinal: child as u32,
				range_start: 0,
				range_end: u64::MAX,
				partition: None,
				versions: vs,
			})
		}
		p.manifest.children = cs;
	}
	assert_eq!(extra, 0);
	parents.sort_by_key(|p| p.manifest.identity.encode().unwrap());
	let frame = framed(&b, crate::schema::RecordBodyCompressionPolicy::Uncompressed);
	let r = (0..512)
		.map(|i| ProvenanceReference {
			physical_row: i,
			parent: i as u16,
			child: 0,
			row: 0,
		})
		.collect();
	let p = UnverifiedFrameProvenance::new(&identity(1), &frame, parents, r).unwrap();
	let bytes = p.encode().unwrap();
	assert_eq!(bytes.len(), MAX_PROVENANCE_BYTES);
	let decoded = UnverifiedFrameProvenance::decode(&bytes).unwrap();
	let owned = decoded.owned_metadata_bytes();
	let mut bad = bytes;
	bad.push(0);
	assert!(UnverifiedFrameProvenance::decode(&bad).is_err());
	(b.get_array_memory_size(), 0, MAX_PROVENANCE_BYTES, owned)
}
fn wide_row() -> (usize, usize, usize, usize) {
	let mut fields = vec![Field::new("id", DataType::Int64, false)];
	fields.extend((1..128).map(|i| Field::new(format!("f{i:03}"), DataType::Binary, false)));
	let schema = Arc::new(Schema::new(fields));
	let s = ProofSchema::new([2; 16], 3, schema.clone(), 0).unwrap();
	let empty = || {
		let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1]))];
		columns.extend((1..128).map(|_| Arc::new(BinaryArray::from(vec![&[][..]])) as ArrayRef));
		columns
	};
	let base = RecordBatch::try_new(schema.clone(), empty()).unwrap();
	let overhead = s.encode_row(&base, 0).unwrap().len();
	let mut columns = empty();
	let value = vec![0; MAX_CONTENT_INPUT - overhead];
	columns[127] = Arc::new(BinaryArray::from(vec![value.as_slice()]));
	let b = RecordBatch::try_new(schema.clone(), columns).unwrap();
	let encoded = s.encode_row(&b, 0).unwrap();
	copy_probe(&s, &b);
	assert_eq!(encoded.len(), MAX_CONTENT_INPUT);
	s.validate_row(&encoded).unwrap();
	let value = vec![0; MAX_CONTENT_INPUT - overhead + 1];
	let mut columns = empty();
	columns[127] = Arc::new(BinaryArray::from(vec![value.as_slice()]));
	assert!(
		s.encode_row(&RecordBatch::try_new(schema, columns).unwrap(), 0)
			.is_err()
	);
	(b.get_array_memory_size(), encoded.len(), 0, 0)
}
fn aggregate_boundary() -> (usize, usize, usize, usize) {
	let columns = |extra: usize| {
		let mut values = vec!["x".repeat(MAX_CONTENT_INPUT - 64); 16];
		values[15].push_str(&"x".repeat(extra));
		RecordBatch::try_new(
			batch(vec![1]).schema(),
			vec![
				Arc::new(Int64Array::from((0..16).collect::<Vec<_>>())),
				Arc::new(StringArray::from(values)),
			],
		)
		.unwrap()
	};
	let b = columns(0);
	let s = schema(&b);
	assert_eq!(
		s.encode_key(&b, 0).unwrap().len() + s.encode_row(&b, 0).unwrap().len(),
		MAX_CONTENT_INPUT
	);
	let frame = framed(&b, crate::schema::RecordBodyCompressionPolicy::Uncompressed);
	let p = parent(1, &b, 1);
	let good =
		UnverifiedFrameProvenance::new(&identity(1), &frame, vec![p.clone()], refs(16)).unwrap();
	good.verify(
		&key(),
		&TrustedFrameRows::from_table_frame(frame, s.clone()).unwrap(),
	)
	.unwrap();
	let bad = columns(1);
	let frame = framed(
		&bad,
		crate::schema::RecordBodyCompressionPolicy::Uncompressed,
	);
	let provenance =
		UnverifiedFrameProvenance::new(&identity(1), &frame, vec![p], refs(16)).unwrap();
	assert_eq!(
		provenance
			.verify(
				&key(),
				&TrustedFrameRows::from_table_frame(frame, s).unwrap()
			)
			.err(),
		Some(AdapterError::LimitExceeded)
	);
	(b.get_array_memory_size(), MAX_PROOF_FRAME_BYTES, 0, 0)
}
#[test]
#[ignore = "isolated debug/release requested-byte and copy assay"]
fn isolated_logical_adapter_provenance_copy_assay() {
	// Prime existing Arrow/storage process-global registrations BEFORE baselines.
	one_parent_case(2, 1);
	println!("B1_ASSAY warmup complete (ordinary IPC helpers; no runtime stores)");
	println!(
		"B1_SIZE schema={} parent={} reference={} unverified={} verified={} trusted={}",
		std::mem::size_of::<ProofSchema>(),
		std::mem::size_of::<ProvenanceParent>(),
		std::mem::size_of::<ProvenanceReference>(),
		std::mem::size_of::<UnverifiedFrameProvenance>(),
		std::mem::size_of::<ContentVerifiedFrameProvenance>(),
		std::mem::size_of::<TrustedFrameRows>()
	);
	measure("one-parent-one-row", || one_parent_case(1, 1));
	measure("512-parents-512-rows", independent_parents);
	measure("one-parent-512-rows-256-children-survivor", || {
		one_parent_case(512, 256)
	});
	measure("128-fields-1MiB-and-plus1", wide_row);
	measure(
		"max-structural-envelope-and-plus1",
		largest_structural_envelope,
	);
	measure("16MiB-aggregate-and-plus1", aggregate_boundary);
	let rss = std::fs::read_to_string("/proc/self/status")
		.ok()
		.and_then(|s| {
			s.lines()
				.find(|l| l.starts_with("VmRSS:"))
				.map(str::to_owned)
		});
	println!("B1_RSS context={rss:?}; allocator requested bytes are not RSS");
}
