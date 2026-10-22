//! Ownership only across the post-publish cluster initialization/JOIN block.
//! Earlier startup and later node construction remain outside this guard.
use super::*;
use crate::storage::ownership::{StartupCleanupSlot, StartupCleanupStatus, StartupDrainBarrier};

pub(super) struct Tasks {
	pub ring_updater: JoinHandle<()>,
	pub drift_monitor: JoinHandle<()>,
	pub audit_chain_root_rollup: JoinHandle<()>,
	pub checkpoint_emitter: JoinHandle<()>,
	pub epoch_reconciler: JoinHandle<()>,
}
impl Tasks {
	fn abort(&self) {
		self.ring_updater.abort();
		self.drift_monitor.abort();
		self.audit_chain_root_rollup.abort();
		self.checkpoint_emitter.abort();
		self.epoch_reconciler.abort();
	}
	fn into_named(self) -> [(&'static str, JoinHandle<()>); 5] {
		[
			("ring updater", self.ring_updater),
			("drift monitor", self.drift_monitor),
			("audit chain root rollup", self.audit_chain_root_rollup),
			("checkpoint emitter", self.checkpoint_emitter),
			("epoch reconciler", self.epoch_reconciler),
		]
	}
}
pub(super) struct Resources {
	pub owner: DataDirectoryOwner,
	pub raft: WendelonRaft,
	pub lifecycle: ServerLifecycle,
	pub transports: Vec<NodeTransportServer>,
	pub tasks: Tasks,
	pub bound: Duration,
	pub runtime: tokio::runtime::Handle,
}
impl Resources {
	fn fence(&self) {
		self.lifecycle.begin_drain();
		for server in &self.transports {
			server.begin_drain();
		}
		self.owner.begin_close();
		self.tasks.abort();
	}
	async fn cleanup(
		self,
		barrier: Option<StartupDrainBarrier>,
		progress: Arc<std::sync::Mutex<&'static str>>,
	) -> std::io::Result<()> {
		*progress.lock().unwrap() = "Raft shutdown";
		let mut first_error = self
			.raft
			.shutdown()
			.await
			.err()
			.map(|error| Error::other(format!("startup Raft shutdown: {error}")));
		for (name, task) in self.tasks.into_named() {
			*progress.lock().unwrap() = name;
			if let Err(error) = await_background_task(task, name).await {
				first_error.get_or_insert(error);
			}
		}
		*progress.lock().unwrap() = "request quiescence";
		self.lifecycle.wait_for_quiescence().await;
		*progress.lock().unwrap() = "transport shutdown";
		for server in self.transports {
			if let Err(error) = server.shutdown().await {
				first_error.get_or_insert(error);
			}
		}
		// This is the only failure-path release: every potential writer above
		// has actually joined, including consuming transport shutdown futures.
		if let Some(barrier) = barrier {
			barrier.complete_cleanup();
		}
		// Storage incomplete dominates earlier shutdown errors and keeps LOCK.
		*progress.lock().unwrap() = "storage close";
		self.owner.close(self.bound).await?;
		match first_error {
			Some(error) => Err(error),
			None => Ok(()),
		}
	}
}
pub(super) struct Guard {
	resources: Option<Resources>,
	barrier: Option<StartupDrainBarrier>,
}
impl Guard {
	pub async fn arm(resources: Resources) -> std::io::Result<Self> {
		match resources.owner.reserve_startup_drain() {
			Ok(barrier) => Ok(Self {
				resources: Some(resources),
				barrier: Some(barrier),
			}),
			Err(original) => {
				// The accepted boundary was never entered. Still own and clean the
				// already-created bundle; never detach it with a bare `?` here.
				resources.fence();
				let owner = resources.owner.clone();
				let bound = resources.bound;
				let runtime = resources.runtime.clone();
				let progress = Arc::new(std::sync::Mutex::new("startup arm refusal"));
				let slot = owner.track_startup_cleanup(
					resources.cleanup(None, progress.clone()),
					bound,
					original.to_string(),
					progress,
					runtime,
				);
				Err(with_cleanup_context(
					original,
					cleanup_result(&owner, &slot).await,
				))
			}
		}
	}
	fn start_cleanup(
		&mut self,
		context: String,
	) -> Option<(DataDirectoryOwner, Arc<StartupCleanupSlot>)> {
		let resources = self.resources.take()?;
		resources.fence();
		let owner = resources.owner.clone();
		let bound = resources.bound;
		let barrier = self.barrier.take();
		let runtime = resources.runtime.clone();
		let progress = Arc::new(std::sync::Mutex::new("startup cleanup registration"));
		let slot = owner.track_startup_cleanup(
			resources.cleanup(barrier, progress.clone()),
			bound,
			context,
			progress,
			runtime,
		);
		Some((owner, slot))
	}
	pub async fn fail(mut self, original: Error) -> Error {
		tracing::debug!(error = %original, "cluster initialization/JOIN failed; cleaning startup resources");
		let Some((owner, slot)) = self.start_cleanup(original.to_string()) else {
			return original;
		};
		with_cleanup_context(original, cleanup_result(&owner, &slot).await)
	}
	pub fn finish_success(mut self) -> Resources {
		// No await or fallible construction between taking the real bundle and
		// releasing its reservation. The returned value owns all original handles.
		let resources = self.resources.take().expect("live startup guard");
		self.barrier
			.take()
			.expect("accepted startup barrier")
			.release_after_handoff();
		resources
	}
}
impl Drop for Guard {
	fn drop(&mut self) {
		let _ = self.start_cleanup("bootstrap caller cancelled".into());
	}
}
#[derive(Debug)]
struct StartupFailure {
	original: Error,
	cleanup: Error,
}
impl std::fmt::Display for StartupFailure {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}; startup cleanup: {}", self.original, self.cleanup)
	}
}
impl std::error::Error for StartupFailure {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		Some(&self.cleanup)
	}
}
fn with_cleanup_context(original: Error, cleanup: std::io::Result<()>) -> Error {
	match cleanup {
		Ok(()) => original,
		Err(cleanup) => Error::new(original.kind(), StartupFailure { original, cleanup }),
	}
}

async fn cleanup_result(
	owner: &DataDirectoryOwner,
	slot: &StartupCleanupSlot,
) -> std::io::Result<()> {
	match slot.wait().await {
		StartupCleanupStatus::Complete => {
			slot.join_finished().await;
			Ok(())
		}
		StartupCleanupStatus::PendingIncomplete(message) => Err(owner.startup_incomplete(message)),
		StartupCleanupStatus::Failed {
			kind,
			message,
			incomplete,
			outstanding,
		} => {
			slot.join_finished().await;
			if incomplete {
				Err(owner.startup_incomplete_from(kind, message, outstanding))
			} else {
				Err(Error::new(kind, message))
			}
		}
		StartupCleanupStatus::Running => unreachable!("wait returns a report"),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::atomic::{AtomicBool, Ordering};
	struct FailingWorker(AtomicBool);
	#[async_trait]
	impl crate::storage::ownership::StorageWorker for FailingWorker {
		async fn close_and_join(&self) -> std::io::Result<()> {
			if self.0.load(Ordering::SeqCst) {
				Err(Error::new(
					ErrorKind::PermissionDenied,
					"named startup worker close denied",
				))
			} else {
				Ok(())
			}
		}
	}
	#[tokio::test]
	async fn failed_cleanup_keeps_original_startup_error_kind_and_both_causes() {
		let dir = crate::test_support::TestDir::new("startup-error-kind");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			7112,
			dir.to_path_buf(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let panic_task = tokio::spawn(async {
			panic!("startup cleanup producer panic");
		});
		while !panic_task.is_finished() {
			tokio::task::yield_now().await;
		}
		let resources = Resources {
			owner: node._directory_owner.clone(),
			raft: node.raft.clone(),
			lifecycle: node.lifecycle.clone(),
			transports: Vec::new(),
			tasks: Tasks {
				ring_updater: panic_task,
				drift_monitor: tokio::spawn(std::future::pending()),
				audit_chain_root_rollup: tokio::spawn(std::future::pending()),
				checkpoint_emitter: tokio::spawn(std::future::pending()),
				epoch_reconciler: tokio::spawn(std::future::pending()),
			},
			bound: Duration::from_secs(5),
			runtime: tokio::runtime::Handle::current(),
		};
		let guard = Guard::arm(resources).await.unwrap();
		let error = guard
			.fail(Error::new(ErrorKind::InvalidInput, "original JOIN refusal"))
			.await;
		let kind = error.kind();
		let text = error.to_string();
		let reports = node._directory_owner.startup_cleanup_reports();
		node.shutdown().await.unwrap();
		assert_eq!(kind, ErrorKind::InvalidInput, "{text}");
		assert!(
			text.contains("original JOIN refusal") && text.contains("ring updater task failed"),
			"{text}"
		);
		assert!(reports.iter().any(|r| matches!(r, StartupCleanupStatus::Failed { message, .. } if message.contains("original JOIN refusal") && message.contains("ring updater task failed"))), "{reports:?}");
	}

	#[test]
	fn actual_cleanup_joins_blocked_and_panicked_tasks_preserving_storage_error() {
		const CHILD: &str = "NILKET_STARTUP_RESOURCE_CHILD";
		if std::env::var_os(CHILD).is_none() {
			let output = std::process::Command::new(std::env::current_exe().unwrap()).env(CHILD, "1")
				.arg("node::startup::tests::actual_cleanup_joins_blocked_and_panicked_tasks_preserving_storage_error").arg("--exact").arg("--nocapture").output().unwrap();
			assert!(
				output.status.success(),
				"resource control child failed: {}",
				String::from_utf8_lossy(&output.stderr)
			);
			return;
		}
		tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
			let dir = crate::test_support::TestDir::new("startup-real-cleanup");
			let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(7111, dir.to_path_buf(), "127.0.0.1:0".into())).await.unwrap();
			let owner = node._directory_owner.clone();
			let failing = Arc::new(FailingWorker(AtomicBool::new(true)));
			owner.register_worker(failing.clone()).unwrap();
			let panic_task = tokio::spawn(async { panic!("joined startup producer panic"); });
			tokio::time::timeout(Duration::from_secs(10), async { while !panic_task.is_finished() { tokio::task::yield_now().await; } }).await.unwrap();
			let (release, wait) = std::sync::mpsc::channel();
			let (started, entered) = std::sync::mpsc::channel();
			let blocked = tokio::task::spawn_blocking(move || { let _ = started.send(()); let _ = wait.recv_timeout(Duration::from_secs(10)); });
			entered.recv_timeout(Duration::from_secs(10)).unwrap();
			let resources = Resources { owner: owner.clone(), raft: node.raft.clone(), lifecycle: node.lifecycle.clone(), transports: Vec::new(),
				tasks: Tasks { ring_updater: panic_task, drift_monitor: blocked, audit_chain_root_rollup: tokio::spawn(std::future::pending()), checkpoint_emitter: tokio::spawn(std::future::pending()), epoch_reconciler: tokio::spawn(std::future::pending()) },
				bound: Duration::from_secs(5), runtime: tokio::runtime::Handle::current() };
			let barrier = owner.reserve_startup_drain().unwrap(); resources.fence();
			let progress = Arc::new(std::sync::Mutex::new("resource control"));
			let slot = owner.track_startup_cleanup(resources.cleanup(Some(barrier), progress.clone()), Duration::from_millis(20), "original JOIN error".into(), progress.clone(), tokio::runtime::Handle::current());
			tokio::time::timeout(Duration::from_secs(5), async { while *progress.lock().unwrap() != "drift monitor" { tokio::task::yield_now().await; } }).await.unwrap();
			let pending = matches!(slot.wait().await, StartupCleanupStatus::PendingIncomplete(_));
			let external_incomplete = owner.close(Duration::from_millis(20)).await.err().is_some_and(|error| error.get_ref().is_some_and(|e| e.is::<crate::storage::ownership::ShutdownIncomplete>()));
			let busy = DataDirectoryOwner::acquire_existing(&dir).is_err();
			let _ = release.send(());
			let terminal = tokio::time::timeout(Duration::from_secs(10), async { loop {
				let status = slot.wait().await; if !matches!(status, StartupCleanupStatus::PendingIncomplete(_) | StartupCleanupStatus::Running) { break status; }
				tokio::task::yield_now().await;
			}}).await.unwrap();
			let report_preserved = matches!(terminal, StartupCleanupStatus::Failed { kind: ErrorKind::PermissionDenied, ref message, incomplete: true, outstanding: Some(0) } if message.contains("named startup worker close denied"));
			let result = cleanup_result(&owner, &slot).await.unwrap_err();
			let result_preserved = result.kind() == ErrorKind::PermissionDenied && result.get_ref().is_some_and(|e| e.is::<crate::storage::ownership::ShutdownIncomplete>()) && std::error::Error::source(&result).is_some_and(|e| e.to_string().contains("named startup worker close denied"));
			failing.0.store(false, Ordering::SeqCst); owner.close(Duration::from_secs(5)).await.unwrap();
			node.shutdown().await.unwrap(); drop(owner);
			let reopened = DataDirectoryOwner::acquire_existing(&dir).unwrap(); reopened.close(Duration::from_secs(5)).await.unwrap();
			assert!(pending && external_incomplete && busy && report_preserved && result_preserved,
				"pending={pending} external_incomplete={external_incomplete} busy={busy} report={report_preserved} result={result_preserved}");
		});
	}
}
