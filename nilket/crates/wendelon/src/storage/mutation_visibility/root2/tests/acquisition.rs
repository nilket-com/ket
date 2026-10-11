//! Stage S2a: bounded committed-prefix acquisition over the real in-memory log store, with a
//! fault-injecting reader and scripted sources. Pins coverage, read counts, the terminal capture
//! after the boundary read, byte thresholds, failure and cancellation isolation, and the
//! documented limits.
use crate::consensus::retention::{
	ControlRetention, HistoryCheck, RetentionRefusals, RetentionViolation,
};
use crate::consensus::{RetentionStatus, WendelonLogStore, WendelonRequest, WendelonTypeConfig};
use crate::storage::mutation_visibility::root2::acquisition::*;
use crate::storage::mutation_visibility::root2::prefix::PrefixError2;
use openraft::anyerror::AnyError;
use openraft::storage::{RaftLogReader, RaftLogStorageExt};
use openraft::{
	BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership, OptionalSend,
	StorageError, StorageIOError,
};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

type E2 = Entry<WendelonTypeConfig>;

fn lid(term: u64, node: u32, index: u64) -> LogId<u32> {
	LogId::new(CommittedLeaderId::new(term, node), index)
}
fn genesis() -> E2 {
	Entry {
		log_id: LogId::default(),
		payload: EntryPayload::Membership(Membership::from(BTreeMap::from([(
			1u32,
			BasicNode { addr: "n1".into() },
		)]))),
	}
}
fn normal(log_id: LogId<u32>, len: usize) -> E2 {
	Entry {
		log_id,
		payload: EntryPayload::Normal(WendelonRequest::SchemaUpdate(vec![log_id.index as u8; len])),
	}
}
/// Genesis, then (1,1,i) for i in 1..=last, each with a 3-byte payload.
fn entries(last: u64) -> Vec<E2> {
	let mut v = vec![genesis()];
	v.extend((1..=last).map(|i| normal(lid(1, 1, i), 3)));
	v
}
fn size(e: &E2) -> u64 {
	postcard::experimental::serialized_size(e).unwrap() as u64
}
async fn store(v: Vec<E2>) -> WendelonLogStore {
	let mut s = WendelonLogStore::in_memory();
	s.blocking_append(v).await.unwrap();
	s
}
fn full() -> RetentionStatus {
	RetentionStatus {
		configured: ControlRetention::Full,
		history: HistoryCheck::Fresh,
		violation: None,
		refusals: RetentionRefusals::default(),
	}
}
fn wide() -> AcquisitionCaps2 {
	AcquisitionCaps2 {
		max_entries: 2,
		max_batch_bytes: 1 << 20,
		max_total_bytes: 1 << 30,
	}
}

/// Current source values; tests and reader hooks mutate them between and during reads.
struct Src {
	applied: Result<Option<LogId<u32>>, SourceClosed2>,
	retention: RetentionStatus,
}
#[derive(Clone)]
struct Sources(Arc<Mutex<Src>>);
impl Sources {
	fn at(c: LogId<u32>) -> Self {
		Self(Arc::new(Mutex::new(Src {
			applied: Ok(Some(c)),
			retention: full(),
		})))
	}
	fn set_applied(&self, v: Result<Option<LogId<u32>>, SourceClosed2>) {
		self.0.lock().unwrap().applied = v;
	}
	fn latch(&self) {
		self.0.lock().unwrap().retention.violation = Some(RetentionViolation::Purge);
	}
}
impl AcquisitionSources2 for Sources {
	fn retention(&self) -> RetentionStatus {
		self.0.lock().unwrap().retention
	}
	fn applied(&self) -> Result<Option<LogId<u32>>, SourceClosed2> {
		self.0.lock().unwrap().applied
	}
}

type Hook = Box<dyn Fn(usize, Vec<E2>) -> Fault + Send + Sync>;
enum Fault {
	Return(Vec<E2>),
	Error,
	Pending,
}
/// Counts reads (1-based) and lets a hook replace, refuse or stall any read.
struct FaultReader {
	inner: WendelonLogStore,
	reads: Arc<AtomicUsize>,
	hook: Arc<Hook>,
}
impl FaultReader {
	fn new(
		inner: WendelonLogStore,
		hook: impl Fn(usize, Vec<E2>) -> Fault + Send + Sync + 'static,
	) -> Self {
		Self {
			inner,
			reads: Arc::new(AtomicUsize::new(0)),
			hook: Arc::new(Box::new(hook)),
		}
	}
	fn plain(inner: WendelonLogStore) -> Self {
		Self::new(inner, |_, v| Fault::Return(v))
	}
	fn reads(&self) -> usize {
		self.reads.load(Ordering::SeqCst)
	}
}
impl RaftLogReader<WendelonTypeConfig> for FaultReader {
	async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
		&mut self,
		range: RB,
	) -> Result<Vec<E2>, StorageError<u32>> {
		let n = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
		let v = self.inner.try_get_log_entries(range).await?;
		match (self.hook)(n, v) {
			Fault::Return(v) => Ok(v),
			Fault::Error => Err(StorageIOError::read_logs(AnyError::error("injected")).into()),
			Fault::Pending => std::future::pending().await,
		}
	}
}

/// A pure owned accumulator whose drops are counted.
#[derive(Debug)]
struct Acc {
	seen: Vec<u64>,
	drops: Arc<AtomicUsize>,
}
impl Drop for Acc {
	fn drop(&mut self) {
		self.drops.fetch_add(1, Ordering::SeqCst);
	}
}
fn acc() -> (Acc, Arc<AtomicUsize>) {
	let drops = Arc::new(AtomicUsize::new(0));
	(
		Acc {
			seen: Vec::new(),
			drops: drops.clone(),
		},
		drops,
	)
}
fn record(mut a: Acc, b: &[E2]) -> Result<Acc, FoldError2> {
	a.seen.extend(b.iter().map(|e| e.log_id.index));
	Ok(a)
}

async fn run(
	reader: &mut FaultReader,
	sources: &Sources,
	caps: AcquisitionCaps2,
) -> (
	Result<(Acc, PrefixValidated2), AcquisitionError2>,
	Arc<AtomicUsize>,
) {
	let (a, drops) = acc();
	(
		acquire_committed_prefix2(reader, sources, caps, a, record).await,
		drops,
	)
}

#[tokio::test]
async fn single_batch_from_genesis() {
	let v = entries(3);
	let bytes: u64 = v.iter().map(size).sum();
	let mut r = FaultReader::plain(store(v).await);
	let caps = AcquisitionCaps2 {
		max_entries: 10,
		..wide()
	};
	let (out, drops) = run(&mut r, &Sources::at(lid(1, 1, 3)), caps).await;
	let (a, report) = out.unwrap();
	assert_eq!(a.seen, [0, 1, 2, 3]);
	assert_eq!(report.through(), lid(1, 1, 3));
	assert_eq!(report.entries(), 4);
	assert_eq!(report.encoded_bytes(), bytes);
	assert_eq!(r.reads(), 2, "one batch read and one boundary read");
	assert_eq!(drops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn multi_batch_exact_coverage_and_read_count() {
	// c on a batch edge (index 5 ends the third batch of two) and inside a batch (index 6).
	for (c, batches) in [(5u64, 3usize), (6, 4)] {
		let mut r = FaultReader::plain(store(entries(8)).await);
		let (out, _) = run(&mut r, &Sources::at(lid(1, 1, c)), wide()).await;
		let (a, report) = out.unwrap();
		assert_eq!(a.seen, (0..=c).collect::<Vec<_>>(), "in order, each once");
		assert_eq!(report.entries(), c + 1);
		assert_eq!(r.reads(), 2 * batches, "exactly 2B reads");
	}
}

#[tokio::test]
async fn late_gap_truncation_and_extra_refuse() {
	let c = lid(1, 1, 6);
	// Read 7 is the last batch (6..=6); read 5 is the third (4..=5).
	let cases: Vec<(usize, fn(Vec<E2>) -> Vec<E2>)> = vec![
		(5, |mut v| {
			v.pop();
			v
		}),
		(5, |mut v| {
			v.remove(0);
			v
		}),
		(5, |mut v| {
			let extra = v[1].clone();
			v.push(extra);
			v
		}),
	];
	for (at, f) in cases {
		let mut r = FaultReader::new(store(entries(8)).await, move |n, v| {
			Fault::Return(if n == at { f(v) } else { v })
		});
		let (out, drops) = run(&mut r, &Sources::at(c), wide()).await;
		assert!(
			matches!(
				out,
				Err(AcquisitionError2::Coverage {
					from: 4,
					end: 5,
					..
				})
			),
			"{out:?}"
		);
		assert_eq!(
			drops.load(Ordering::SeqCst),
			1,
			"provisional output discarded"
		);
	}
	// Right count, wrong index: S1's contiguity check.
	let mut r = FaultReader::new(store(entries(8)).await, |n, v| {
		Fault::Return(if n == 5 {
			vec![v[0].clone(), normal(lid(1, 1, 7), 3)]
		} else {
			v
		})
	});
	let (out, _) = run(&mut r, &Sources::at(c), wide()).await;
	assert!(
		matches!(
			out,
			Err(AcquisitionError2::Prefix(PrefixError2::Gap { at: 5 }))
		),
		"{out:?}"
	);
}

#[tokio::test]
async fn boundary_identity_payload_and_availability() {
	let c = lid(1, 1, 6);
	let run_with = |hook: fn(usize, Vec<E2>) -> Fault| async move {
		let mut r = FaultReader::new(store(entries(8)).await, hook);
		run(&mut r, &Sources::at(c), wide()).await.0
	};
	// Read 4 is the second boundary re-read: a different term.
	let out = run_with(|n, v| {
		Fault::Return(if n == 4 {
			vec![normal(lid(2, 1, 6), 3)]
		} else {
			v
		})
	})
	.await;
	assert!(
		matches!(
			out,
			Err(AcquisitionError2::Prefix(PrefixError2::BoundaryMismatch))
		),
		"{out:?}"
	);
	// Read 8 is the last boundary re-read: same log id, changed payload.
	let out = run_with(|n, v| {
		Fault::Return(if n == 8 {
			vec![normal(lid(1, 1, 6), 4)]
		} else {
			v
		})
	})
	.await;
	assert!(
		matches!(
			out,
			Err(AcquisitionError2::Prefix(PrefixError2::BoundaryMismatch))
		),
		"{out:?}"
	);
	// Missing or doubled boundary.
	let out = run_with(|n, v| Fault::Return(if n == 2 { vec![] } else { v })).await;
	assert!(
		matches!(out, Err(AcquisitionError2::BoundaryUnavailable { got: 0 })),
		"{out:?}"
	);
	let out = run_with(|n, v| {
		Fault::Return(if n == 2 {
			vec![v[0].clone(), v[0].clone()]
		} else {
			v
		})
	})
	.await;
	assert!(
		matches!(out, Err(AcquisitionError2::BoundaryUnavailable { got: 2 })),
		"{out:?}"
	);
	// `c` beyond the retained log: the first boundary read fails closed.
	let mut r = FaultReader::plain(store(entries(3)).await);
	let caps = AcquisitionCaps2 {
		max_entries: 2,
		..wide()
	};
	let (out, _) = run(&mut r, &Sources::at(lid(1, 1, 9)), caps).await;
	assert!(
		matches!(out, Err(AcquisitionError2::BoundaryUnavailable { got: 0 })),
		"{out:?}"
	);
}

/// The terminal samples come after the last boundary read returns, so a change during that read
/// is seen; a change after the samples (here, during the last fold) is outside the acquisition.
#[tokio::test]
async fn terminal_capture_observes_changes_during_last_boundary_read() {
	let c = lid(1, 1, 6);
	type Change = fn(&Sources);
	let changes: [(Change, fn(&AcquisitionError2) -> bool); 4] = [
		(Sources::latch, |e| {
			matches!(e, AcquisitionError2::Prefix(PrefixError2::Latched))
		}),
		(
			|s| s.set_applied(Err(SourceClosed2)),
			|e| matches!(e, AcquisitionError2::SourceClosed),
		),
		(
			|s| s.set_applied(Ok(None)),
			|e| matches!(e, AcquisitionError2::Prefix(PrefixError2::Regressed)),
		),
		(
			|s| s.set_applied(Ok(Some(lid(1, 1, 5)))),
			|e| matches!(e, AcquisitionError2::Prefix(PrefixError2::Regressed)),
		),
	];
	for (change, expect) in changes {
		let sources = Sources::at(c);
		let during = sources.clone();
		// Read 8 is the last boundary read.
		let mut r = FaultReader::new(store(entries(8)).await, move |n, v| {
			if n == 8 {
				change(&during);
			}
			Fault::Return(v)
		});
		let (out, drops) = run(&mut r, &sources, wide()).await;
		let err = out.err().unwrap();
		assert!(expect(&err), "{err:?}");
		assert_eq!(drops.load(Ordering::SeqCst), 1);
		// The same change during the last fold, after the final samples, is not observed.
		let sources = Sources::at(c);
		let later = sources.clone();
		let mut r = FaultReader::plain(store(entries(8)).await);
		let (a, drops) = acc();
		let out = acquire_committed_prefix2(&mut r, &sources, wide(), a, move |a, b| {
			if b.last().is_some_and(|e| e.log_id.index == 6) {
				change(&later);
			}
			record(a, b)
		})
		.await;
		assert!(out.is_ok(), "validation cut: later changes belong to L");
		assert_eq!(drops.load(Ordering::SeqCst), 0);
	}
}

#[tokio::test]
async fn retention_and_committed_point_cases() {
	let c = lid(1, 1, 6);
	// A violation appearing only in the last batch's end status.
	let sources = Sources::at(c);
	let during = sources.clone();
	let mut r = FaultReader::new(store(entries(8)).await, move |n, v| {
		if n == 7 {
			during.latch();
		}
		Fault::Return(v)
	});
	let (out, _) = run(&mut r, &sources, wide()).await;
	assert!(matches!(
		out,
		Err(AcquisitionError2::Prefix(PrefixError2::Latched))
	));
	// A start status NotChecked refuses before any fold.
	let sources = Sources::at(c);
	sources.0.lock().unwrap().retention.history = HistoryCheck::NotChecked;
	let mut r = FaultReader::plain(store(entries(8)).await);
	let folds = Arc::new(AtomicUsize::new(0));
	let counted = folds.clone();
	let (a, _) = acc();
	let out = acquire_committed_prefix2(&mut r, &sources, wide(), a, move |a, b| {
		counted.fetch_add(1, Ordering::SeqCst);
		record(a, b)
	})
	.await;
	assert!(matches!(
		out,
		Err(AcquisitionError2::Prefix(PrefixError2::NotChecked))
	));
	assert_eq!(folds.load(Ordering::SeqCst), 0);
	// Same index, different id at a terminal sample: Regressed. Advancement: accepted.
	for (after, ok) in [(lid(2, 1, 6), false), (lid(1, 1, 8), true)] {
		let sources = Sources::at(c);
		let during = sources.clone();
		let mut r = FaultReader::new(store(entries(8)).await, move |n, v| {
			if n == 2 {
				during.set_applied(Ok(Some(after)));
			}
			Fault::Return(v)
		});
		let (out, _) = run(&mut r, &sources, wide()).await;
		if ok {
			assert_eq!(
				out.unwrap().1.through(),
				c,
				"the captured c, not the later sample"
			);
		} else {
			assert!(matches!(
				out,
				Err(AcquisitionError2::Prefix(PrefixError2::Regressed))
			));
		}
	}
}

#[tokio::test]
async fn applied_source_none_closed_and_max() {
	let sources = Sources::at(lid(1, 1, 3));
	sources.set_applied(Ok(None));
	let mut r = FaultReader::plain(store(entries(3)).await);
	assert!(matches!(
		run(&mut r, &sources, wide()).await.0,
		Err(AcquisitionError2::NoCommittedEntry)
	));
	sources.set_applied(Err(SourceClosed2));
	assert!(matches!(
		run(&mut r, &sources, wide()).await.0,
		Err(AcquisitionError2::SourceClosed)
	));
	assert_eq!(r.reads(), 0);
	// Closed mid-acquisition (during the first batch read).
	let sources = Sources::at(lid(1, 1, 6));
	let during = sources.clone();
	let mut r = FaultReader::new(store(entries(8)).await, move |n, v| {
		if n == 3 {
			during.set_applied(Err(SourceClosed2));
		}
		Fault::Return(v)
	});
	assert!(matches!(
		run(&mut r, &sources, wide()).await.0,
		Err(AcquisitionError2::SourceClosed)
	));
	// c at u64::MAX: the count is unrepresentable; refused at capture with zero reads.
	let mut r = FaultReader::plain(store(entries(3)).await);
	assert!(matches!(
		run(&mut r, &Sources::at(lid(1, 1, u64::MAX)), wide())
			.await
			.0,
		Err(AcquisitionError2::Overflow)
	));
	assert_eq!(r.reads(), 0);
}

#[tokio::test]
async fn store_and_fold_failures_discard_provisional_output() {
	let c = lid(1, 1, 6);
	for at in [1usize, 2, 5, 8] {
		let mut r = FaultReader::new(store(entries(8)).await, move |n, v| {
			if n == at {
				Fault::Error
			} else {
				Fault::Return(v)
			}
		});
		let (out, drops) = run(&mut r, &Sources::at(c), wide()).await;
		assert!(matches!(out, Err(AcquisitionError2::Store(_))), "read {at}");
		assert_eq!(drops.load(Ordering::SeqCst), 1);
	}
	// A fold refusal at the last batch, after earlier batches were folded.
	let mut r = FaultReader::plain(store(entries(8)).await);
	let (a, drops) = acc();
	let out = acquire_committed_prefix2(&mut r, &Sources::at(c), wide(), a, |a, b| {
		if b[0].log_id.index == 6 {
			Err(FoldError2)
		} else {
			record(a, b)
		}
	})
	.await;
	assert!(matches!(out, Err(AcquisitionError2::Fold(FoldError2))));
	assert_eq!(
		drops.load(Ordering::SeqCst),
		1,
		"the accumulator is dropped, not returned"
	);
}

#[tokio::test]
async fn caps_inclusive_edges_and_precedence() {
	let v = entries(3);
	let all: u64 = v.iter().map(size).sum();
	let boundary = size(&v[3]);
	let c = lid(1, 1, 3);
	let one_batch = |max_batch_bytes, max_total_bytes| AcquisitionCaps2 {
		max_entries: 10,
		max_batch_bytes,
		max_total_bytes,
	};
	// Each zero cap.
	for caps in [
		AcquisitionCaps2 {
			max_entries: 0,
			..wide()
		},
		AcquisitionCaps2 {
			max_batch_bytes: 0,
			..wide()
		},
		AcquisitionCaps2 {
			max_total_bytes: 0,
			..wide()
		},
	] {
		let mut r = FaultReader::plain(store(v.clone()).await);
		assert!(matches!(
			run(&mut r, &Sources::at(c), caps).await.0,
			Err(AcquisitionError2::CapsUnset)
		));
		assert_eq!(r.reads(), 0);
	}
	// Combined threshold: batch + boundary exactly at the cap succeeds; one byte less refuses.
	let mut r = FaultReader::plain(store(v.clone()).await);
	assert!(
		run(&mut r, &Sources::at(c), one_batch(all + boundary, all))
			.await
			.0
			.is_ok()
	);
	let mut r = FaultReader::plain(store(v.clone()).await);
	assert!(matches!(
		run(&mut r, &Sources::at(c), one_batch(all + boundary - 1, all))
			.await
			.0,
		Err(AcquisitionError2::BoundaryBytes { from: 0 })
	));
	// Batch exactly at the cap passes the batch-only threshold but fails the combined one.
	let mut r = FaultReader::plain(store(v.clone()).await);
	assert!(matches!(
		run(&mut r, &Sources::at(c), one_batch(all, all)).await.0,
		Err(AcquisitionError2::BoundaryBytes { from: 0 })
	));
	assert_eq!(r.reads(), 2);
	// Batch-only over-cap: refused before any boundary read.
	let mut r = FaultReader::plain(store(v.clone()).await);
	assert!(matches!(
		run(&mut r, &Sources::at(c), one_batch(all - 1, all))
			.await
			.0,
		Err(AcquisitionError2::BatchBytes { from: 0 })
	));
	assert_eq!(r.reads(), 1, "no boundary read for an over-cap batch");
	// An oversized first entry, alone.
	let mut big = entries(3);
	big[1] = normal(lid(1, 1, 1), 4096);
	let mut r = FaultReader::plain(store(big).await);
	let caps = AcquisitionCaps2 {
		max_entries: 1,
		max_batch_bytes: 1024,
		max_total_bytes: 1 << 20,
	};
	assert!(matches!(
		run(&mut r, &Sources::at(c), caps).await.0,
		Err(AcquisitionError2::BatchBytes { from: 1 })
	));
	// Total exactly at the cap succeeds with a sufficient batch cap; one byte less refuses.
	let mut r = FaultReader::plain(store(v.clone()).await);
	assert!(
		run(&mut r, &Sources::at(c), one_batch(1 << 20, all))
			.await
			.0
			.is_ok()
	);
	let mut r = FaultReader::plain(store(v.clone()).await);
	assert!(matches!(
		run(&mut r, &Sources::at(c), one_batch(1 << 20, all - 1))
			.await
			.0,
		Err(AcquisitionError2::TotalBytes { from: 0 })
	));
	// Coincidences resolve in sequence order.
	let c6 = lid(1, 1, 6);
	let short_and_wrong = |n: usize, v: Vec<E2>| {
		Fault::Return(if n == 1 {
			vec![normal(lid(1, 1, 9), 3)]
		} else {
			v
		})
	};
	let mut r = FaultReader::new(store(entries(8)).await, short_and_wrong);
	assert!(matches!(
		run(&mut r, &Sources::at(c6), wide()).await.0,
		Err(AcquisitionError2::Coverage {
			from: 0,
			end: 1,
			got: 1
		})
	));
	let over_and_wrong = |n: usize, v: Vec<E2>| {
		Fault::Return(if n == 1 {
			vec![v[0].clone(), normal(lid(1, 1, 9), 4096)]
		} else {
			v
		})
	};
	let mut r = FaultReader::new(store(entries(8)).await, over_and_wrong);
	let caps = AcquisitionCaps2 {
		max_batch_bytes: 1024,
		..wide()
	};
	assert!(matches!(
		run(&mut r, &Sources::at(c6), caps).await.0,
		Err(AcquisitionError2::BatchBytes { from: 0 })
	));
	let sources = Sources::at(c6);
	sources.latch();
	let mut r = FaultReader::new(store(entries(8)).await, |n, v| {
		Fault::Return(if n == 2 { vec![] } else { v })
	});
	assert!(matches!(
		run(&mut r, &sources, wide()).await.0,
		Err(AcquisitionError2::BoundaryUnavailable { got: 0 })
	));
}

#[test]
fn next_range_and_sum_edges() {
	assert!(matches!(
		next_range_for_test(u64::MAX, 1, u64::MAX),
		Ok((u64::MAX, 1))
	));
	assert!(matches!(next_range_for_test(0, u64::MAX, 6), Ok((6, 7))));
	assert!(matches!(
		next_range_for_test(u64::MAX - 1, 3, u64::MAX),
		Ok((u64::MAX, 2))
	));
	assert!(matches!(next_range_for_test(4, 2, 6), Ok((5, 2))));
	assert!(matches!(
		next_range_for_test(7, 2, 6),
		Err(AcquisitionError2::Overflow)
	));
	assert!(matches!(add_bytes_for_test(u64::MAX - 1, 1), Ok(u64::MAX)));
	assert!(matches!(
		add_bytes_for_test(u64::MAX, 1),
		Err(AcquisitionError2::Overflow)
	));
}

#[tokio::test]
async fn cancelled_acquisition_exposes_nothing() {
	// Read 3 is the second batch read.
	let mut r = FaultReader::new(store(entries(8)).await, |n, v| {
		if n == 3 {
			Fault::Pending
		} else {
			Fault::Return(v)
		}
	});
	let sources = Sources::at(lid(1, 1, 6));
	let (a, drops) = acc();
	let timed = tokio::time::timeout(
		std::time::Duration::from_millis(50),
		acquire_committed_prefix2(&mut r, &sources, wide(), a, record),
	)
	.await;
	assert!(timed.is_err(), "cancelled");
	assert_eq!(
		drops.load(Ordering::SeqCst),
		1,
		"the accumulator was dropped with the future"
	);
}

/// Documented limit: each index is read once, so an entry below the boundary that changes after
/// its read is not detected.
#[tokio::test]
async fn altered_after_read_is_not_detected() {
	let c = lid(1, 1, 6);
	let mut r = FaultReader::new(store(entries(8)).await, |n, v| {
		Fault::Return(if n == 1 {
			vec![v[0].clone(), normal(lid(1, 1, 1), 9)]
		} else {
			v
		})
	});
	let (out, _) = run(&mut r, &Sources::at(c), wide()).await;
	assert!(out.is_ok());
}
