use crate::ecs::NodeId;
use std::collections::HashSet;
use std::hint::black_box;
use std::num::NonZeroU32;
use std::time::{Duration, Instant};

const QUERY_COUNT: usize = 1 << 20;
const ROUNDS: usize = 7;

fn node(value: u32) -> NodeId {
	NodeId(NonZeroU32::new(value).expect("benchmark node IDs are non-zero"))
}

fn median(mut samples: Vec<Duration>) -> Duration {
	samples.sort_unstable();
	samples[samples.len() / 2]
}

fn measure(mut contains: impl FnMut(NodeId) -> bool, queries: &[NodeId]) -> Duration {
	let mut samples = Vec::with_capacity(ROUNDS);
	for _ in 0..ROUNDS {
		let started = Instant::now();
		let mut hits = 0usize;
		for &query in queries {
			hits += usize::from(black_box(contains(black_box(query))));
		}
		black_box(hits);
		samples.push(started.elapsed());
	}
	median(samples)
}

/// Storage gate for `plans/buffer-store.md` §4.2.
///
/// Projection walks are overwhelmingly misses because a view normally has only
/// tens of closed folds among many visited nodes. Run this in release mode:
///
/// `cargo test -p libaryon --release fold_set_storage_gate -- --ignored --nocapture`
#[test]
#[ignore = "manual representation benchmark; run in release mode with --nocapture"]
fn fold_set_storage_gate() {
	let queries: Vec<_> = (0..QUERY_COUNT)
		.map(|index| node((index as u32).saturating_add(1)))
		.collect();

	for fold_count in [8usize, 32, 128] {
		// Sparse IDs ensure fewer than one percent of the sequential queries hit,
		// matching the projection walk's miss-heavy lookup distribution.
		let folded: Vec<_> = (0..fold_count)
			.map(|index| node(1 + (index as u32) * 8_191))
			.collect();
		let hash: HashSet<_> = folded.iter().copied().collect();
		let sorted = folded;

		let hash_elapsed = measure(|candidate| hash.contains(&candidate), &queries);
		let sorted_elapsed = measure(
			|candidate| {
				sorted
					.binary_search_by_key(&candidate.index(), |node| node.index())
					.is_ok()
			},
			&queries,
		);
		let hash_ns = hash_elapsed.as_nanos() as f64 / QUERY_COUNT as f64;
		let sorted_ns = sorted_elapsed.as_nanos() as f64 / QUERY_COUNT as f64;

		eprintln!(
			"folds={fold_count:>3}: hash={hash_ns:>7.3} ns/query, sorted={sorted_ns:>7.3} ns/query, sorted/hash={:.3}",
			sorted_ns / hash_ns,
		);
	}
}
