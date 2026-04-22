use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use uuid::Uuid;
use wendelon::storage::{
	AlignedBuf, AsyncFileOps, DIRECT_IO_ALIGN, IoUringFileOps, OpenFlags, spawn_io_thread_named,
};

const WAL_DEPTH: u32 = 64;
const SLAB_DEPTH: u32 = 64;
const WAL_ITERS: usize = 128;
const SLAB_WORKERS: usize = 4;
const WAL_FILE_BYTES: u64 = (DIRECT_IO_ALIGN as u64) * 256;
const SLAB_FILE_BYTES: u64 = (DIRECT_IO_ALIGN as u64) * 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BenchmarkMode {
	Shared,
	Split,
	Both,
}

impl BenchmarkMode {
	fn parse() -> Result<Self, Box<dyn std::error::Error>> {
		let mut args = std::env::args().skip(1);
		let Some(flag) = args.next() else {
			return Ok(Self::Both);
		};
		if flag != "--mode" {
			return Err(format!("unknown argument: {flag}").into());
		}
		let value = args
			.next()
			.ok_or("expected one of: shared, split, both after --mode")?;
		match value.as_str() {
			"shared" => Ok(Self::Shared),
			"split" => Ok(Self::Split),
			"both" => Ok(Self::Both),
			other => Err(format!("invalid --mode value: {other}").into()),
		}
	}
}

#[derive(Clone, Copy, Debug)]
struct ScenarioStats {
	avg_ns: u128,
	p50_ns: u128,
	p99_ns: u128,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let mode = BenchmarkMode::parse()?;
	let root = std::env::temp_dir().join(format!(
		"wendelon-bench-io-topology-{}-{}",
		std::process::id(),
		Uuid::new_v4()
	));
	std::fs::create_dir_all(&root)?;

	let shared = match mode {
		BenchmarkMode::Shared | BenchmarkMode::Both => {
			Some(run_scenario(&root, "shared", Topology::Shared).await?)
		}
		BenchmarkMode::Split => None,
	};
	let split = match mode {
		BenchmarkMode::Split | BenchmarkMode::Both => {
			Some(run_scenario(&root, "split", Topology::Split).await?)
		}
		BenchmarkMode::Shared => None,
	};

	if let Some(stats) = shared {
		println!(
			"io_topology mode=shared wal_depth={} slab_depth={} slab_workers={} wal_iters={} avg_wal_write_fsync_ns={} p50_wal_write_fsync_ns={} p99_wal_write_fsync_ns={}",
			WAL_DEPTH,
			SLAB_DEPTH,
			SLAB_WORKERS,
			WAL_ITERS,
			stats.avg_ns,
			stats.p50_ns,
			stats.p99_ns,
		);
	}
	if let Some(stats) = split {
		println!(
			"io_topology mode=split wal_depth={} slab_depth={} slab_workers={} wal_iters={} avg_wal_write_fsync_ns={} p50_wal_write_fsync_ns={} p99_wal_write_fsync_ns={}",
			WAL_DEPTH,
			SLAB_DEPTH,
			SLAB_WORKERS,
			WAL_ITERS,
			stats.avg_ns,
			stats.p50_ns,
			stats.p99_ns,
		);
	}
	if let (Some(shared), Some(split)) = (shared, split) {
		println!(
			"io_topology delta avg_ns={} p50_ns={} p99_ns={}",
			split.avg_ns as i128 - shared.avg_ns as i128,
			split.p50_ns as i128 - shared.p50_ns as i128,
			split.p99_ns as i128 - shared.p99_ns as i128,
		);
	}

	std::fs::remove_dir_all(&root)?;
	Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Topology {
	Shared,
	Split,
}

async fn run_scenario(
	root: &std::path::Path,
	tag: &str,
	topology: Topology,
) -> Result<ScenarioStats, Box<dyn std::error::Error>> {
	let wal_path = root.join(format!("wal-{tag}.bench"));
	let slab_path = root.join(format!("slab-{tag}.bench"));

	let (wal_ops, slab_ops): (Arc<dyn AsyncFileOps>, Arc<dyn AsyncFileOps>) = match topology {
		Topology::Shared => {
			let shared =
				spawn_io_thread_named(WAL_DEPTH.max(SLAB_DEPTH), "wendelon-bench-io-shared")?;
			let ops: Arc<dyn AsyncFileOps> = Arc::new(IoUringFileOps::new(shared));
			(ops.clone(), ops)
		}
		Topology::Split => {
			let wal_io = spawn_io_thread_named(WAL_DEPTH, "wendelon-bench-io-wal")?;
			let slab_io = spawn_io_thread_named(SLAB_DEPTH, "wendelon-bench-io-slab")?;
			(
				Arc::new(IoUringFileOps::new(wal_io)),
				Arc::new(IoUringFileOps::new(slab_io)),
			)
		}
	};

	let wal_file = wal_ops
		.open(
			&wal_path,
			OpenFlags {
				write: true,
				create: true,
				truncate: true,
				direct: true,
				..Default::default()
			},
		)
		.await?;
	let slab_file = slab_ops
		.open(
			&slab_path,
			OpenFlags {
				write: true,
				create: true,
				truncate: true,
				direct: true,
				..Default::default()
			},
		)
		.await?;

	let stop = Arc::new(AtomicBool::new(false));
	let mut slab_handles = Vec::with_capacity(SLAB_WORKERS);
	for worker_id in 0..SLAB_WORKERS {
		let slab_ops = slab_ops.clone();
		let slab_file = slab_file.clone();
		let stop = stop.clone();
		slab_handles.push(tokio::spawn(async move {
			let mut page = worker_id as u64;
			while !stop.load(Ordering::Relaxed) {
				let offset =
					(page % (SLAB_FILE_BYTES / DIRECT_IO_ALIGN as u64)) * DIRECT_IO_ALIGN as u64;
				let buf = patterned_page(worker_id as u8);
				let _ = slab_ops.write_at(&slab_file, offset, buf).await?;
				page = page.wrapping_add(SLAB_WORKERS as u64);
			}
			Ok::<(), std::io::Error>(())
		}));
	}

	let mut samples = Vec::with_capacity(WAL_ITERS);
	for iter in 0..WAL_ITERS {
		let offset =
			(iter as u64 % (WAL_FILE_BYTES / DIRECT_IO_ALIGN as u64)) * DIRECT_IO_ALIGN as u64;
		let buf = patterned_page(0xA5);
		let started = Instant::now();
		let _ = wal_ops.write_at(&wal_file, offset, buf).await?;
		wal_ops.fdatasync(&wal_file).await?;
		samples.push(started.elapsed());
	}

	stop.store(true, Ordering::Relaxed);
	for handle in slab_handles {
		handle.await??;
	}

	let avg_ns = samples.iter().map(Duration::as_nanos).sum::<u128>() / samples.len() as u128;
	let mut sorted = samples;
	sorted.sort_unstable();
	Ok(ScenarioStats {
		avg_ns,
		p50_ns: percentile(&sorted, 0.50).as_nanos(),
		p99_ns: percentile(&sorted, 0.99).as_nanos(),
	})
}

fn patterned_page(seed: u8) -> AlignedBuf {
	let mut buf = AlignedBuf::new(DIRECT_IO_ALIGN);
	for (idx, byte) in buf.as_mut_slice().iter_mut().enumerate() {
		*byte = seed.wrapping_add((idx as u8).wrapping_mul(17));
	}
	buf
}

fn percentile(samples: &[Duration], q: f64) -> Duration {
	let idx = ((samples.len().saturating_sub(1)) as f64 * q).round() as usize;
	samples[idx.min(samples.len().saturating_sub(1))]
}
