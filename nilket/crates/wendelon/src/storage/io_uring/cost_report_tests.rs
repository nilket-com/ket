//! DC-287 (Codex 01a207d0 R2): the ordinary ring operations' observed cost, reported for an
//! identical offline comparison between a base and a candidate tree. It uses only APIs both
//! trees have. Reported, not asserted beyond sanity: one host, one process, interleaved
//! repetitions; the numbers bound an overhead only to within this run's noise.

use super::*;
use crate::storage::{AsyncFileOps, FileHandle, IoUringFileOps};
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_ring_operation_cost_is_reported() {
	const N: usize = 2_000;
	const REPS: usize = 3;
	let handle = spawn_io_thread(8).unwrap();
	let ops = IoUringFileOps::new(handle);
	let dir = std::env::temp_dir().join(format!("dc287-ring-cost-{}", uuid::Uuid::new_v4()));
	std::fs::create_dir(&dir).unwrap();
	let file = FileHandle::new(
		std::fs::OpenOptions::new()
			.create_new(true)
			.read(true)
			.write(true)
			.open(dir.join("data"))
			.unwrap(),
	);
	let mut buf = AlignedBuf::new(DIRECT_IO_ALIGN);
	buf.as_mut_slice().fill(0x67);
	for rep in 0..REPS {
		let (mut write, mut read, mut sync) = (Duration::ZERO, Duration::ZERO, Duration::ZERO);
		for i in 0..N {
			let offset = (i % 64 * DIRECT_IO_ALIGN) as u64;
			let started = Instant::now();
			let (_, returned) = ops.write_at(&file, offset, buf).await.unwrap();
			write += started.elapsed();
			let started = Instant::now();
			let (_, returned) = ops.read_at(&file, offset, returned).await.unwrap();
			read += started.elapsed();
			buf = returned;
			if i % 8 == 0 {
				let started = Instant::now();
				ops.fdatasync(&file).await.unwrap();
				sync += started.elapsed();
			}
		}
		eprintln!(
			"DC287 cost rep {rep}: write {:?}/op, read {:?}/op, fdatasync {:?}/op ({N} writes and reads, {} syncs)",
			write / N as u32,
			read / N as u32,
			sync / (N / 8) as u32,
			N / 8
		);
		assert!(write > Duration::ZERO && read > Duration::ZERO && sync > Duration::ZERO);
	}
	std::fs::remove_dir_all(dir).unwrap();
}
