//! Resolved-add throughput of the real counter leader over real files: three
//! participants on local slabs, quorum two, one counter, a unique write key per
//! add, every durability barrier on the production file-ops path. It measures;
//! it asserts only that each add resolved, so it never replaces a correctness
//! test. Run it explicitly, on the filesystem under measurement (never tmpfs):
//!
//! `WENDELON_COUNTER_BENCH_DIR=<dir> nilket/scripts/cargo-cgroup test --release
//! -p wendelon --lib counter_add_throughput_measurement -- --ignored --nocapture`
//! (from the `nilket` workspace; never raw `cargo`)
//!
//! With `WENDELON_COUNTER_BENCH_COMPACT=1` the leader's applied-log compaction
//! runs back to back for the whole measured window (the adds' latency with
//! compaction running); unset, nothing else changes.
use super::*;
use std::time::Instant;

fn env_count(name: &str, default: usize) -> usize {
	std::env::var(name)
		.ok()
		.map(|value| value.parse().expect("a count"))
		.unwrap_or(default)
}

#[tokio::test]
#[ignore = "measurement: needs WENDELON_COUNTER_BENCH_DIR on the filesystem under test"]
async fn counter_add_throughput_measurement() {
	let root = PathBuf::from(
		std::env::var("WENDELON_COUNTER_BENCH_DIR").expect("WENDELON_COUNTER_BENCH_DIR"),
	);
	let warmup = env_count("WENDELON_COUNTER_BENCH_WARMUP", 20);
	let ops = env_count("WENDELON_COUNTER_BENCH_OPS", 300);
	let dir = root.join(format!("run-{}", uuid::Uuid::new_v4()));
	std::fs::create_dir_all(&dir).unwrap();
	let (ps, _paths) = participants(&dir, 3).await;
	let store = DurableCounterWitnessStore::open(dir.join("witnesses")).unwrap();
	let leader = LeaderHarness::new(ps, 2, true, store);
	let key = ckey(b"throughput");
	for n in 0..warmup {
		leader.fetch_add(wk(n as u64 + 1), &key, 1).await.unwrap();
	}
	let compact = std::env::var("WENDELON_COUNTER_BENCH_COMPACT").is_ok_and(|value| value == "1");
	let stop = Arc::new(AtomicBool::new(false));
	let compactor = compact.then(|| {
		let (leader, stop) = (leader.leader.clone(), stop.clone());
		tokio::spawn(async move {
			let mut compactions = 0u64;
			while !stop.load(Ordering::Relaxed) {
				leader.compact().await.expect("compaction");
				compactions += 1;
				tokio::task::yield_now().await;
			}
			compactions
		})
	});
	let mut latencies = Vec::with_capacity(ops);
	let started = Instant::now();
	for n in 0..ops {
		let add = Instant::now();
		let result = leader
			.fetch_add(wk((warmup + n) as u64 + 1), &key, 1)
			.await
			.unwrap();
		latencies.push(add.elapsed());
		assert!(matches!(result, CounterAddResult::Applied(_)));
	}
	let elapsed = started.elapsed();
	stop.store(true, Ordering::Relaxed);
	let compactions = match compactor {
		Some(task) => task.await.unwrap(),
		None => 0,
	};
	assert_eq!(leader.pending_witnesses(), 0, "every add resolved");
	assert_eq!(
		leader.read(&key).await.unwrap(),
		Some((warmup + ops) as u64)
	);
	latencies.sort();
	let micros = |at: usize| latencies[at.min(ops - 1)].as_secs_f64() * 1e6;
	let mean = latencies.iter().map(|d| d.as_secs_f64()).sum::<f64>() / ops as f64 * 1e6;
	println!(
		"COUNTER_BENCH {{\"compact\":{compact},\"compactions\":{compactions},\"ops\":{ops},\"warmup\":{warmup},\"elapsed_s\":{:.6},\"adds_per_s\":{:.3},\"lat_mean_us\":{mean:.1},\"lat_p50_us\":{:.1},\"lat_p99_us\":{:.1},\"lat_max_us\":{:.1}}}",
		elapsed.as_secs_f64(),
		ops as f64 / elapsed.as_secs_f64(),
		micros(ops / 2),
		micros(ops * 99 / 100),
		micros(ops - 1),
	);
	drop(leader);
	std::fs::remove_dir_all(&dir).unwrap();
}
