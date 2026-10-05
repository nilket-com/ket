//! Isolated allocator assay: requested live heap bytes are distinct from RSS and encoded bytes.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use wendelon::storage::mutation_visibility::*;

struct CountingAllocator;
static LIVE: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for CountingAllocator {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		let p = unsafe { System.alloc(layout) };
		if !p.is_null() {
			LIVE.fetch_add(layout.size(), Ordering::Relaxed);
		}
		p
	}
	unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
		LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
		unsafe { System.dealloc(p, layout) };
	}
	unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
		let next = unsafe { System.realloc(p, layout, size) };
		if !next.is_null() {
			if size >= layout.size() {
				LIVE.fetch_add(size - layout.size(), Ordering::Relaxed);
			} else {
				LIVE.fetch_sub(layout.size() - size, Ordering::Relaxed);
			}
		}
		next
	}
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
fn resident_kib() -> Option<usize> {
	std::fs::read_to_string("/proc/self/status")
		.ok()?
		.lines()
		.find_map(|line| {
			line.strip_prefix("VmRSS:")?
				.split_whitespace()
				.next()?
				.parse()
				.ok()
		})
}
#[test]
fn authority_encoded_live_heap_and_process_resident_assay() {
	let baseline_rss = resident_kib();
	let baseline = LIVE.load(Ordering::Relaxed);
	let log = |index| DecisionLogId {
		term: 1,
		node: 1,
		index,
	};
	let authority = DecisionAuthority::new([1; 16], 1, log(1)).unwrap();
	for sequence in 0..MAX_DECISIONS as u64 {
		let manifest = ParentManifest {
			identity: MutationIdentity {
				generation: [1; 16],
				tenant: 1,
				scope: MutationScope::Table,
				table: [2; 16],
				schema_generation: 1,
				caller: [3; 16],
				sequence,
			},
			admission: Admission {
				epoch: 1,
				holder: 1,
			},
			key_id: [4; 16],
			key_version: 1,
			operation: MutationOperation::Insert,
			content_codec: 1,
			children: vec![ManifestChild {
				ordinal: 0,
				range_start: 1,
				range_end: 100,
				partition: Some(1),
				versions: vec![LogicalVersion {
					ordinal: 0,
					key_token: [5; 32],
					content_commitment: [6; 32],
					tombstone: false,
				}],
			}],
		};
		let event = AuthorityEvent::Decision {
			log: log(sequence + 2),
			manifest,
			commitment: [7; 32],
			terminal: TerminalDecision::Accepted,
		};
		assert!(matches!(
			authority.apply(&event).unwrap(),
			ApplyOutcome::Recorded(_)
		));
	}
	let live_heap = LIVE.load(Ordering::Relaxed).checked_sub(baseline).unwrap();
	let populated_rss = resident_kib();
	let usage = authority.usage().unwrap();
	let encoded = authority.encode_extension().unwrap();
	let encoded_len = encoded.len();
	assert_eq!(usage.decisions, 32768);
	assert_eq!(usage.stored_bytes, 8978432);
	assert_eq!(encoded_len, 8978570);
	drop(encoded);
	drop(authority);
	let after_drop = LIVE.load(Ordering::Relaxed);
	println!(
		"ASSAY records={} canonical_record_bytes={} extension_bytes={} live_heap_delta_bytes={} baseline_rss_kib={:?} populated_rss_kib={:?} live_before={} live_after_drop={}",
		usage.decisions,
		usage.stored_bytes,
		encoded_len,
		live_heap,
		baseline_rss,
		populated_rss,
		baseline,
		after_drop
	);
	// RSS is process context, not a per-entry or retained-heap assertion.
	assert!(live_heap > usage.stored_bytes);
	assert_eq!(after_drop, baseline);
}
