use super::lifecycle::{CompiledUnit, SourceKey, SourceRequestId, compile_source, read_source};
use crate::engine::request::{InternalEvent, InternalSender};
use rune::Context;
use rune::alloc::limit;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

struct SourceCompileRequest {
	key: SourceKey,
	request: SourceRequestId,
	path: PathBuf,
	reservation: usize,
}

pub(crate) struct SourceCompileResult {
	pub(super) key: SourceKey,
	pub(super) request: SourceRequestId,
	pub(super) reservation: usize,
	pub(super) result: Result<CompiledUnit, String>,
}

impl fmt::Debug for SourceCompileResult {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter
			.debug_struct("SourceCompileResult")
			.field("key", &self.key)
			.field("request", &self.request)
			.field("reservation", &self.reservation)
			.field("succeeded", &self.result.is_ok())
			.finish()
	}
}

pub(crate) struct SourceCompiler {
	requests: Option<mpsc::SyncSender<SourceCompileRequest>>,
	acknowledge: mpsc::SyncSender<()>,
	stop: Arc<AtomicBool>,
	worker: Option<thread::JoinHandle<()>>,
}

impl SourceCompiler {
	pub(crate) fn new(context: Context, internal: InternalSender) -> Result<Self, String> {
		let (tx_request, rx_request) = mpsc::sync_channel::<SourceCompileRequest>(1);
		let (tx_acknowledge, rx_acknowledge) = mpsc::sync_channel::<()>(1);
		let stop = Arc::new(AtomicBool::new(false));
		let worker_stop = stop.clone();
		let worker = thread::Builder::new()
			.name("rune-source-compiler".to_string())
			.spawn(move || {
				while let Ok(request) = rx_request.recv() {
					if worker_stop.load(Ordering::Acquire) {
						break;
					}
					let result = match read_source(&request.path) {
						Ok(Some(source)) => limit::with(request.reservation, || {
							compile_source(&context, &request.path, &source)
						})
						.call(),
						Ok(None) => Err(format!(
							"Rune source {} does not exist",
							request.path.display()
						)),
						Err(error) => Err(error),
					};
					let mut event = InternalEvent::RuneCompile(Box::new(SourceCompileResult {
						key: request.key,
						request: request.request,
						reservation: request.reservation,
						result,
					}));
					loop {
						if worker_stop.load(Ordering::Acquire) {
							return;
						}
						match internal.try_send(event) {
							Ok(()) => break,
							Err(mpsc::TrySendError::Full(returned)) => {
								event = returned;
								thread::sleep(Duration::from_millis(2));
							}
							Err(mpsc::TrySendError::Disconnected(_)) => return,
						}
					}
					if rx_acknowledge.recv().is_err() {
						break;
					}
				}
			})
			.map_err(|error| format!("Could not start Rune compiler worker: {error}"))?;
		Ok(Self {
			requests: Some(tx_request),
			acknowledge: tx_acknowledge,
			stop,
			worker: Some(worker),
		})
	}

	pub(super) fn submit(
		&self,
		key: SourceKey,
		request: SourceRequestId,
		path: PathBuf,
		reservation: usize,
	) -> Result<(), String> {
		let Some(requests) = &self.requests else {
			return Err("Rune compiler worker is stopped".to_string());
		};
		requests
			.try_send(SourceCompileRequest {
				key,
				request,
				path,
				reservation,
			})
			.map_err(|error| match error {
				mpsc::TrySendError::Full(_) => "Rune compiler worker is busy".to_string(),
				mpsc::TrySendError::Disconnected(_) => {
					"Rune compiler worker stopped unexpectedly".to_string()
				}
			})
	}

	pub(super) fn acknowledge(&self) {
		let _ = self.acknowledge.try_send(());
	}
}

impl Drop for SourceCompiler {
	fn drop(&mut self) {
		self.stop.store(true, Ordering::Release);
		self.requests.take();
		let _ = self.acknowledge.try_send(());
		if self
			.worker
			.as_ref()
			.is_some_and(thread::JoinHandle::is_finished)
			&& let Some(worker) = self.worker.take()
		{
			let _ = worker.join();
		}
		// A native compiler call cannot be interrupted safely. Dropping a still
		// running JoinHandle detaches it; the stop flag makes it exit at the next
		// phase boundary instead of blocking engine shutdown indefinitely.
	}
}
