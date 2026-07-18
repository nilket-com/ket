use super::{
	CompletionMeta, RESOLVER_TRANSFER_SIZE, ResolverNotifiers, SvpRequestQueue, complete_failed,
	complete_read, request_len,
};
use crate::ecs::registry::UastRegistry;
use io_uring::{IoUring, opcode, types};
use std::collections::HashMap;
use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

pub(super) struct LinuxUringBackend {
	fds: Arc<Mutex<HashMap<u16, Arc<File>>>>,
	stop: Arc<AtomicBool>,
	worker: Option<thread::JoinHandle<()>>,
}

impl LinuxUringBackend {
	pub(super) fn spawn(
		registry: Arc<UastRegistry>,
		queue: Arc<SvpRequestQueue>,
		notifiers: ResolverNotifiers,
	) -> Self {
		let fds = Arc::new(Mutex::new(HashMap::new()));
		let fds_loop = fds.clone();
		let stop = Arc::new(AtomicBool::new(false));
		let stop_loop = stop.clone();

		let worker = thread::Builder::new()
			.name("svp-io-uring".into())
			.spawn(move || {
				Self::io_completion_loop(registry, queue, fds_loop, notifiers, stop_loop);
			})
			.expect("failed to spawn io_uring SVP worker");

		Self {
			fds,
			stop,
			worker: Some(worker),
		}
	}

	pub(super) fn register_device(&self, device_id: u16, path: &str) -> std::io::Result<()> {
		let file = Arc::new(File::open(path)?);
		self.fds.lock().unwrap().insert(device_id, file);
		Ok(())
	}

	pub(super) fn unregister_device(&self, device_id: u16) {
		self.fds.lock().unwrap().remove(&device_id);
	}

	#[cfg(test)]
	pub(super) fn registered_device_count(&self) -> usize {
		self.fds.lock().unwrap().len()
	}

	fn io_completion_loop(
		registry: Arc<UastRegistry>,
		queue: Arc<SvpRequestQueue>,
		fds: Arc<Mutex<HashMap<u16, Arc<File>>>>,
		notifiers: ResolverNotifiers,
		stop: Arc<AtomicBool>,
	) {
		let mut ring = IoUring::new(256).expect("Failed to init io_uring");

		const BATCH_SIZE: usize = 16;
		let mut buffers = vec![vec![0u8; RESOLVER_TRANSFER_SIZE]; BATCH_SIZE];
		let mut metas: Vec<Option<CompletionMeta>> = (0..BATCH_SIZE).map(|_| None).collect();
		let mut in_flight_files: Vec<Option<Arc<File>>> = (0..BATCH_SIZE).map(|_| None).collect();
		let mut free_buffers: Vec<usize> = (0..BATCH_SIZE).rev().collect();
		let mut in_flight = 0;

		loop {
			while !stop.load(Ordering::Acquire) && !free_buffers.is_empty() {
				let buffer_index = free_buffers.pop().expect("checked free buffer");
				if let Some(req) = queue.pop() {
					let file = {
						let guard = fds.lock().unwrap();
						guard.get(&req.pointer.device_id).cloned()
					};

					if let Some(file) = file {
						let request_len = match request_len(&req) {
							Ok(length) => length,
							Err(_) => {
								let meta = CompletionMeta::new(req, buffer_index);
								let hydration_buffer = meta.hydration_buffer;
								notifiers.notify(
									complete_failed(&registry, &queue, meta),
									hydration_buffer,
								);
								free_buffers.push(buffer_index);
								continue;
							}
						};
						if request_len == 0 {
							let meta = CompletionMeta::new(req, buffer_index);
							let hydration_buffer = meta.hydration_buffer;
							notifiers
								.notify(complete_failed(&registry, &queue, meta), hydration_buffer);
							free_buffers.push(buffer_index);
							continue;
						}

						let read_offset = req.pointer.lba * 512
							+ u64::from(req.pointer.head_trim)
							+ u64::from(req.extent_offset);
						let meta = CompletionMeta::new(req, buffer_index);
						let read_op = opcode::Read::new(
							types::Fd(file.as_raw_fd()),
							buffers[meta.buffer_index].as_mut_ptr(),
							request_len,
						)
						.offset(read_offset)
						.build()
						.user_data(buffer_index as u64);
						metas[buffer_index] = Some(meta);
						// Keep the exact file description used by this SQE alive through
						// completion. Removing its device id from the registration map may
						// otherwise close and reuse the raw fd before the kernel consumes it.
						in_flight_files[buffer_index] = Some(file);

						unsafe {
							ring.submission()
								.push(&read_op)
								.expect("Submission queue full");
						}
						in_flight += 1;
					} else {
						let meta = CompletionMeta::new(req, buffer_index);
						let hydration_buffer = meta.hydration_buffer;
						notifiers
							.notify(complete_failed(&registry, &queue, meta), hydration_buffer);
						free_buffers.push(buffer_index);
					}
				} else {
					free_buffers.push(buffer_index);
					break;
				}
			}

			if in_flight > 0 {
				ring.submit_and_wait(1).expect("Ring wait failed");

				let mut notifications = Vec::<Option<crate::core::BufferId>>::new();
				let cq = ring.completion();
				for cqe in cq {
					let result = cqe.result();
					let buffer_index = cqe.user_data() as usize;
					let meta = metas[buffer_index]
						.take()
						.expect("completion has in-flight metadata");
					let _file = in_flight_files[buffer_index]
						.take()
						.expect("completion retains its backing file");

					let hydration_buffer = meta.hydration_buffer;
					let needs_refresh = if result >= 0 && result as u32 == meta.request_length {
						let byte_count = (result as usize).min(RESOLVER_TRANSFER_SIZE);
						complete_read(
							&registry,
							&queue,
							meta,
							&buffers[buffer_index][..byte_count],
						)
					} else {
						complete_failed(&registry, &queue, meta)
					};
					if needs_refresh && !notifications.contains(&hydration_buffer) {
						notifications.push(hydration_buffer);
					}

					free_buffers.push(buffer_index);
					in_flight -= 1;
				}
				notifiers.notify_batch(&notifications);
			} else if stop.load(Ordering::Acquire) {
				break;
			} else {
				thread::sleep(std::time::Duration::from_millis(1));
			}
		}
	}
}

impl Drop for LinuxUringBackend {
	fn drop(&mut self) {
		self.stop.store(true, Ordering::Release);
		if let Some(worker) = self.worker.take() {
			let _ = worker.join();
		}
	}
}
