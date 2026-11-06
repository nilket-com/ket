//! Driver bound controls with a fake writer and a fake node control: writes
//! that never complete must not hold the warm-up abort, the final request or
//! the join past their deadlines, and are reported incomplete, never acked.

use super::*;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::atomic::AtomicBool;

fn batches(n: usize) -> Vec<RecordBatch> {
	let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
	(0..n)
		.map(|i| {
			RecordBatch::try_new(
				schema.clone(),
				vec![Arc::new(Int64Array::from(vec![i as i64; 4]))],
			)
			.unwrap()
		})
		.collect()
}

fn params() -> Params {
	Params {
		interval: Duration::from_millis(20),
		max_in_flight: 2,
		warmup: Duration::from_millis(300),
		measured: Duration::from_millis(400),
		drain_timeout: Duration::from_millis(200),
		diagnostic_quiesce: None,
	}
}

/// Writes complete immediately until `stall` is set; then they never do.
struct FakeWriter {
	stall: Arc<AtomicBool>,
}

impl Writer for FakeWriter {
	fn write(&self, _batch: RecordBatch) -> WriteFuture {
		if self.stall.load(Ordering::SeqCst) {
			Box::pin(std::future::pending())
		} else {
			Box::pin(async { Ok(false) })
		}
	}
}

/// Acks every request at once; optionally stalls the writer on reset.
struct FakeControl {
	stall_on_reset: Option<Arc<AtomicBool>>,
	requests: std::sync::Mutex<Vec<(u64, String, u64)>>,
}

/// Answers the final request late, by no more than a block allows.
struct LateFinal(Duration);

#[async_trait::async_trait]
impl Control for LateFinal {
	async fn request(&self, seq: u64, kind: &str) -> Result<Value, String> {
		if kind == "final" {
			tokio::time::sleep(self.0).await;
		}
		let t = mono_ns();
		Ok(json!({"seq": seq, "kind": kind, "t_request_ns": t, "t_ack_seen_ns": mono_ns()}))
	}
}

/// H2: admission keeps running until the final is answered. A dataset sized for the declared
/// phases alone runs out under an allowed late final; one sized for the overrun does not.
#[tokio::test]
async fn a_late_final_exhausts_only_a_dataset_sized_without_its_allowance() {
	let p = params();
	let late = Duration::from_millis(200);
	let slots = |span: Duration| span.as_nanos().div_ceil(p.interval.as_nanos()) as usize;
	let declared = slots(p.warmup) + slots(p.measured);
	let allowed = slots(p.warmup) + slots(p.measured + late + Duration::from_millis(100));
	for (dataset, runs_out) in [(declared, true), (allowed, false)] {
		let stall = Arc::new(AtomicBool::new(false));
		let mut ingest = Ingest::new(Arc::new(FakeWriter { stall }), batches(dataset), &p);
		let mut phases = json!({});
		let _ = run_phases(&mut ingest, Arc::new(LateFinal(late)), &p, &mut phases).await;
		assert_eq!(
			ingest.exhausted_at_ns.is_some(),
			runs_out,
			"{dataset} batches, {declared} declared"
		);
	}
}

/// The frozen manifest has room for the overrun: its capacity is checked against the rows TPC-H
/// generates at its scale factor. The driver's own preflight checks the real count.
#[test]
fn the_frozen_manifest_fits_its_overrun() {
	let m: Manifest = serde_json::from_str(include_str!(
		"../../../../../../scripts/s7-measure/manifest.json"
	))
	.unwrap();
	assert!(
		m.most_batches_admitted() > m.batches_in(m.warmup_secs) + m.batches_in(m.measured_secs)
	);
	let rows = (6_001_215.0 * m.scale_factor) as u64;
	assert!(
		m.most_batches_admitted() <= rows / m.batch_rows as u64,
		"{} batches needed from about {rows} rows",
		m.most_batches_admitted()
	);
}

#[async_trait::async_trait]
impl Control for FakeControl {
	async fn request(&self, seq: u64, kind: &str) -> Result<Value, String> {
		if kind == "reset"
			&& let Some(stall) = &self.stall_on_reset
		{
			stall.store(true, Ordering::SeqCst);
		}
		let t = mono_ns();
		self.requests.lock().unwrap().push((seq, kind.into(), t));
		Ok(json!({"seq": seq, "kind": kind, "t_request_ns": t, "t_ack_seen_ns": mono_ns()}))
	}
}

#[tokio::test]
async fn a_write_that_never_completes_aborts_the_warmup_within_its_bounds() {
	let stall = Arc::new(AtomicBool::new(true));
	let p = params();
	let mut ingest = Ingest::new(Arc::new(FakeWriter { stall }), batches(100), &p);
	let control = Arc::new(FakeControl {
		stall_on_reset: None,
		requests: Default::default(),
	});
	let mut phases = json!({});
	let started = std::time::Instant::now();
	let outcome = run_phases(&mut ingest, control.clone(), &p, &mut phases).await;
	let elapsed = started.elapsed();
	let err = outcome.expect_err("a stuck write must abort the block");
	assert!(err.contains("warm-up drain"), "{err}");
	assert!(
		elapsed < p.warmup + p.drain_timeout + Duration::from_millis(500),
		"abort bound held: {elapsed:?}"
	);
	assert!(
		control.requests.lock().unwrap().is_empty(),
		"no reset after a failed drain"
	);
	let incomplete = ingest.abandon();
	assert_eq!(
		incomplete, p.max_in_flight,
		"both in-flight writes are incomplete"
	);
	assert_eq!(
		ingest.done.len(),
		p.max_in_flight,
		"admission stopped at the permit bound"
	);
	assert!(
		ingest.done.iter().all(|r| !r.ok && !r.completed),
		"dropped work is never acknowledged: {:?}",
		ingest.done
	);
}

#[tokio::test]
async fn writes_stalling_in_the_measured_phase_hold_neither_the_final_nor_the_join() {
	let stall = Arc::new(AtomicBool::new(false));
	let p = params();
	let mut ingest = Ingest::new(
		Arc::new(FakeWriter {
			stall: stall.clone(),
		}),
		batches(1000),
		&p,
	);
	let control = Arc::new(FakeControl {
		stall_on_reset: Some(stall),
		requests: Default::default(),
	});
	let mut phases = json!({});
	let started = std::time::Instant::now();
	let outcome = run_phases(&mut ingest, control.clone(), &p, &mut phases).await;
	let elapsed = started.elapsed();
	// Admission is blocked on permits for most of the measured phase; the
	// final request must still go out on time and the join must time out.
	let requests = control.requests.lock().unwrap().clone();
	assert_eq!(
		requests.iter().map(|r| r.1.as_str()).collect::<Vec<_>>(),
		["reset", "final"]
	);
	let final_at = requests[1].2;
	let deadline = phases["measured"]["t_deadline_ns"].as_u64().unwrap();
	assert!(final_at >= deadline, "the final is never early");
	assert!(
		final_at - deadline < 100_000_000,
		"the final is not held by blocked admission: {} ms late",
		(final_at - deadline) / 1_000_000
	);
	let err = outcome.expect_err("unacknowledged measured writes abort the join");
	assert!(err.starts_with("join:"), "{err}");
	assert!(
		elapsed < p.warmup + p.measured + 2 * p.drain_timeout + Duration::from_millis(700),
		"shutdown bound held: {elapsed:?}"
	);
	let incomplete = ingest.abandon();
	assert_eq!(incomplete, p.max_in_flight);
	let warm = ingest.done.iter().filter(|r| r.phase == "warmup");
	assert!(
		warm.clone().count() > 0 && warm.into_iter().all(|r| r.ok && r.completed),
		"completed warm-up records are preserved"
	);
	let measured: Vec<_> = ingest
		.done
		.iter()
		.filter(|r| r.phase == "measured")
		.collect();
	assert_eq!(measured.len(), p.max_in_flight);
	assert!(measured.iter().all(|r| !r.ok && !r.completed));
}

#[tokio::test]
async fn a_clean_block_lasts_its_declared_phases() {
	let p = params();
	let stall = Arc::new(AtomicBool::new(false));
	let mut ingest = Ingest::new(Arc::new(FakeWriter { stall }), batches(1000), &p);
	let control = Arc::new(FakeControl {
		stall_on_reset: None,
		requests: Default::default(),
	});
	let mut phases = json!({});
	run_phases(&mut ingest, control, &p, &mut phases)
		.await
		.unwrap();
	let w = &phases["warmup"];
	let span = w["t_admission_stopped_ns"].as_u64().unwrap() - w["t_start_ns"].as_u64().unwrap();
	assert!(
		span >= p.warmup.as_nanos() as u64,
		"warm-up lasts its declared duration"
	);
	assert_eq!(ingest.abandon(), 0);
	assert!(ingest.done.iter().all(|r| r.ok && r.completed));
}
