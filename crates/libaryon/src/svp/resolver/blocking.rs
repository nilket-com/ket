use super::{
	CompletionMeta, RESOLVER_TRANSFER_SIZE, ResolverNotifiers, SvpRequestQueue, complete_failed,
	complete_read, request_len,
};
use crate::ecs::registry::UastRegistry;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

pub(super) struct BlockingBackend {
	stop: Arc<AtomicBool>,
	worker: Option<thread::JoinHandle<()>>,
}

impl BlockingBackend {
	pub(super) fn spawn(
		registry: Arc<UastRegistry>,
		queue: Arc<SvpRequestQueue>,
		notifiers: ResolverNotifiers,
	) -> Self {
		let stop = Arc::new(AtomicBool::new(false));
		let stop_loop = stop.clone();

		let worker = thread::Builder::new()
			.name("svp-blocking".into())
			.spawn(move || {
				Self::read_loop(registry, queue, notifiers, stop_loop);
			})
			.expect("failed to spawn blocking SVP worker");

		Self {
			stop,
			worker: Some(worker),
		}
	}

	pub(super) fn register_device(&self, _device_id: u16, _path: &str) -> std::io::Result<()> {
		Ok(())
	}

	pub(super) fn unregister_device(&self, _device_id: u16) {}

	#[cfg(test)]
	pub(super) fn registered_device_count(&self) -> usize {
		0
	}

	fn read_loop(
		registry: Arc<UastRegistry>,
		queue: Arc<SvpRequestQueue>,
		notifiers: ResolverNotifiers,
		stop: Arc<AtomicBool>,
	) {
		let mut buffer = vec![0u8; RESOLVER_TRANSFER_SIZE];

		while !stop.load(Ordering::Acquire) {
			let Some(req) = queue.pop() else {
				thread::sleep(std::time::Duration::from_millis(1));
				continue;
			};

			let request_len = match request_len(&req) {
				Ok(length) => length,
				Err(_) => {
					let meta = CompletionMeta::new(req, 0);
					let hydration_buffer = meta.hydration_buffer;
					notifiers.notify(complete_failed(&registry, &queue, meta), hydration_buffer);
					continue;
				}
			};
			let request_len = request_len as usize;
			if request_len == 0 {
				let meta = CompletionMeta::new(req, 0);
				let hydration_buffer = meta.hydration_buffer;
				notifiers.notify(complete_failed(&registry, &queue, meta), hydration_buffer);
				continue;
			}

			let device_id = req.pointer.device_id;
			let offset = req.pointer.lba * 512
				+ u64::from(req.pointer.head_trim)
				+ u64::from(req.extent_offset);
			let meta = CompletionMeta::new(req, 0);
			let hydration_buffer = meta.hydration_buffer;
			let bytes_read = registry
				.read_backing_exact_at(device_id, offset, &mut buffer[..request_len])
				.map(|()| request_len);

			match bytes_read {
				Ok(byte_count) => {
					let needs_refresh =
						complete_read(&registry, &queue, meta, &buffer[..byte_count]);
					notifiers.notify(needs_refresh, hydration_buffer);
				}
				Err(_) => {
					notifiers.notify(complete_failed(&registry, &queue, meta), hydration_buffer);
				}
			}
		}
	}
}

impl Drop for BlockingBackend {
	fn drop(&mut self) {
		self.stop.store(true, Ordering::Release);
		if let Some(worker) = self.worker.take() {
			let _ = worker.join();
		}
	}
}
