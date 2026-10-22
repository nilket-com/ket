//! Provider-only startup ownership probes. No listener/socket or pre-publish claim.
use super::*;
use std::sync::{Mutex as StdMutex, Weak};
use tokio::sync::Notify;
use tokio::task::AbortHandle;

#[derive(Clone, Default)]
pub(crate) struct StartupProbe(
	Arc<StdMutex<Option<Observation>>>,
	Option<Arc<Notify>>,
	bool,
);
impl std::fmt::Debug for StartupProbe {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("StartupProbe")
	}
}
impl PartialEq for StartupProbe {
	fn eq(&self, other: &Self) -> bool {
		Arc::ptr_eq(&self.0, &other.0)
	}
}
impl Eq for StartupProbe {}
struct Observation {
	raft: WendelonRaft,
	slab: Weak<SlabManager>,
	control: Weak<WendelonControlServer>,
	service: Weak<dyn HrftService>,
	lifecycle: ServerLifecycle,
	tasks: Vec<(&'static str, AbortHandle)>,
}
impl StartupProbe {
	pub(super) fn record(
		&self,
		slab: &Arc<SlabManager>,
		control: &Arc<WendelonControlServer>,
		service: &Arc<dyn HrftService>,
		lifecycle: &ServerLifecycle,
		raft: &WendelonRaft,
		tasks: Vec<(&'static str, AbortHandle)>,
	) {
		if let Some(pause) = &self.1 {
			slab.directory_owner()
				.expect("owned startup fixture")
				.pause_startup_cleanup(pause.clone());
		}
		*self.0.lock().unwrap() = Some(Observation {
			raft: raft.clone(),
			slab: Arc::downgrade(slab),
			control: Arc::downgrade(control),
			service: Arc::downgrade(service),
			lifecycle: lifecycle.clone(),
			tasks,
		});
	}
	pub(super) fn after_handoff(&self) -> std::io::Result<()> {
		if self.2 {
			Err(Error::new(
				ErrorKind::InvalidData,
				"PD25 injected post-handoff constructor refusal",
			))
		} else {
			Ok(())
		}
	}
}
struct JoinProvider {
	parked: bool,
	entered: Notify,
}
#[async_trait]
impl TransportProvider for JoinProvider {
	async fn connect(
		&self,
		_target: NodeId,
		_address: &str,
	) -> std::io::Result<Arc<dyn crate::network::transport::TransportConnection>> {
		self.entered.notify_one();
		if self.parked {
			std::future::pending::<()>().await;
		}
		Err(Error::new(
			ErrorKind::ConnectionRefused,
			"startup ownership fixture seed unavailable",
		))
	}
}
async fn observe_failed_startup(cancel: bool, park_cleanup: bool) {
	let dir = crate::test_support::TestDir::new("startup-ownership-runtime");
	let mut cfg = NodeBootstrapConfig::dev(7101, dir.to_path_buf(), "127.0.0.1:0".into());
	cfg.cluster = ClusterBootstrapConfig::Join {
		seeds: vec![SeedNodeConfig {
			node_id: 7102,
			address: "127.0.0.1:1".into(),
		}],
	};
	let mut probe = StartupProbe::default();
	if park_cleanup {
		probe.1 = Some(Arc::new(Notify::new()));
	}
	cfg.startup_probe = Some(probe.clone());
	let provider = Arc::new(JoinProvider {
		parked: cancel,
		entered: Notify::new(),
	});
	let p = provider.clone();
	let mut bootstrap =
		tokio::spawn(async move { WendelonNode::bootstrap_with_provider(cfg, p).await });
	let mut diagnostics = Vec::new();
	let entered = tokio::time::timeout(Duration::from_secs(30), provider.entered.notified())
		.await
		.is_ok();
	if !entered {
		diagnostics.push("seed connect ingress timed out".to_string());
	}
	if cancel || !entered {
		bootstrap.abort();
	}
	let mut unexpected_node = None;
	match tokio::time::timeout(Duration::from_secs(30), &mut bootstrap).await {
		Ok(Err(error)) if cancel && entered && error.is_cancelled() => {}
		Ok(Ok(Err(error)))
			if !cancel && entered && error.kind() == ErrorKind::ConnectionRefused => {}
		Ok(Ok(Ok(node))) => {
			unexpected_node = Some(node);
			diagnostics.push("unexpected bootstrap success".to_string());
		}
		Ok(other) => {
			diagnostics.push(format!(
				"unexpected bootstrap result kind: {}",
				match other {
					Err(_) => "join error",
					Ok(Err(_)) => "bootstrap error",
					Ok(Ok(_)) => unreachable!(),
				}
			));
		}
		Err(_) => {
			diagnostics.push("bootstrap result timed out; abort requested".to_string());
			bootstrap.abort();
			// Keep the handle borrowed while awaiting cancellation; never detach
			// the still-running bootstrap by moving it into timeout().
			match tokio::time::timeout(Duration::from_secs(30), &mut bootstrap).await {
				Ok(Ok(Ok(node))) => unexpected_node = Some(node),
				Ok(_) => {}
				Err(_) => {
					// No safe in-process teardown can be inferred for a task that
					// ignores abort. Fail the disposable test process closed rather
					// than detach it and contaminate a following test.
					eprintln!("FATAL fixture bootstrap failed to join after abort");
					std::process::exit(crate::storage::ownership::UNCLEAN_SHUTDOWN_EXIT_CODE);
				}
			}
		}
	}
	if let Some(node) = unexpected_node {
		if let Err(error) = node.shutdown().await {
			diagnostics.push(format!("unexpected node shutdown failed: {error}"));
		}
	}
	if park_cleanup {
		// This control intentionally retains an owner only AFTER bootstrap ended.
		// The worker cannot poll its resource future until this test releases it.
		let observed_owner = probe
			.0
			.lock()
			.unwrap()
			.as_ref()
			.and_then(|o| o.slab.upgrade())
			.and_then(|slab| slab.directory_owner());
		if let Some(owner) = observed_owner {
			if owner.write_permit().is_ok() {
				diagnostics.push("cancellation did not synchronously fence writes".into());
			}
			if !probe
				.0
				.lock()
				.unwrap()
				.as_ref()
				.unwrap()
				.lifecycle
				.is_draining()
			{
				diagnostics.push("cancellation did not synchronously drain".into());
			}
			match owner.close(Duration::from_millis(20)).await {
				Err(error)
					if error.get_ref().is_some_and(|e| {
						e.is::<crate::storage::ownership::ShutdownIncomplete>()
					}) => {}
				other => diagnostics.push(format!(
					"external close bypassed parked startup cleanup: {other:?}"
				)),
			}
			match DataDirectoryOwner::acquire_existing(&dir) {
				Err(error)
					if error.get_ref().is_some_and(|e| {
						e.is::<crate::storage::ownership::DataDirectoryBusy>()
					}) => {}
				Ok(owner) => {
					let _ = owner.close(Duration::from_secs(10)).await;
					diagnostics.push("parked startup directory was unlocked".into());
				}
				Err(error) => diagnostics.push(format!("parked lock observation failed: {error}")),
			}
			if let Some(slot) = owner.latest_startup_cleanup() {
				let mut observer = Box::pin(slot.wait());
				use std::future::Future;
				if !observer
					.as_mut()
					.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
					.is_pending()
				{
					diagnostics.push("parked observer did not reach Pending".into());
				}
				drop(observer);
			} else {
				diagnostics.push("parked cleanup was not registered".into());
			}
		} else {
			diagnostics.push("parked startup lost storage ownership".into());
		}
		probe.1.as_ref().unwrap().notify_one();
	}
	// GREEN waits for the production-owned cleanup report, not fixture teardown.
	// Upgrades are transient and occur only after the bootstrap task has ended.
	let cleanup_slot = probe
		.0
		.lock()
		.unwrap()
		.as_ref()
		.and_then(|o| o.slab.upgrade())
		.and_then(|slab| slab.directory_owner())
		.and_then(|owner| owner.latest_startup_cleanup());
	if let Some(slot) = cleanup_slot {
		match tokio::time::timeout(Duration::from_secs(30), slot.wait()).await {
			Ok(crate::storage::ownership::StartupCleanupStatus::Complete) => {
				slot.join_finished().await
			}
			other => diagnostics.push(format!("production cleanup did not complete: {other:?}")),
		}
	}
	let observation = probe.0.lock().unwrap().take();
	let Some(observation) = observation else {
		panic!(
			"bootstrap ended without post-publish probe; no post-publish ownership claim; diagnostics={diagnostics:?}"
		);
	};
	// Passive observation only: allow already-requested aborts/cleanup to be
	// scheduled before classifying a persistent orphan. No fixture cleanup runs.
	let observed_at = tokio::time::Instant::now();
	let early_live: Vec<_> = observation
		.tasks
		.iter()
		.filter(|(_, task)| !task.is_finished())
		.map(|(name, _)| *name)
		.collect();
	let early_draining = observation.lifecycle.is_draining();
	tokio::time::sleep(Duration::from_secs(1)).await;
	eprintln!(
		"startup passive observation elapsed={:?} early_live={early_live:?} early_draining={early_draining}",
		observed_at.elapsed()
	);
	// No observation owns service, storage, or task future. Upgrade only after
	// bootstrap has ended; drop upgrades before checking directory reopen.
	let live: Vec<_> = observation
		.tasks
		.iter()
		.filter(|(_, task)| !task.is_finished())
		.map(|(name, _)| *name)
		.collect();
	let still_serving = !observation.lifecycle.is_draining();
	let service_retained = observation.service.upgrade().is_some();
	let write_admitted = observation
		.slab
		.upgrade()
		.and_then(|slab| slab.directory_owner())
		.is_some_and(|owner| owner.write_permit().is_ok());
	let busy_before_cleanup = match DataDirectoryOwner::acquire_existing(&dir) {
		Ok(owner) => {
			if let Err(error) = owner.close(Duration::from_secs(10)).await {
				diagnostics.push(format!("observation owner close: {error}"));
			}
			false
		}
		Err(error) => {
			let busy = error
				.get_ref()
				.is_some_and(|e| e.is::<crate::storage::ownership::DataDirectoryBusy>());
			if !busy {
				diagnostics.push(format!("directory ownership observation failed: {error}"));
			}
			busy
		}
	};
	eprintln!(
		"startup cancel={cancel}: live={live:?} serving={still_serving} service_retained={service_retained} write_admitted={write_admitted} directory_busy={busy_before_cleanup}"
	);
	// Explicit fixture teardown AFTER recording the oracle. This is not startup
	// cleanup and cannot make a failed startup result count as clean.
	observation.lifecycle.begin_drain();
	if let Some(control) = observation.control.upgrade() {
		match tokio::time::timeout(
			Duration::from_secs(10),
			control.shutdown_startup_fixture_raft(),
		)
		.await
		{
			Ok(Ok(())) => {}
			other => diagnostics.push(format!("fixture Raft shutdown failed: {other:?}")),
		}
	}

	let owner = observation
		.slab
		.upgrade()
		.and_then(|slab| slab.directory_owner());
	for (_, task) in &observation.tasks {
		task.abort();
	}
	let tasks_finished = tokio::time::timeout(Duration::from_secs(10), async {
		while observation
			.tasks
			.iter()
			.any(|(_, task)| !task.is_finished())
		{
			tokio::task::yield_now().await;
		}
	})
	.await
	.is_ok();
	if !tasks_finished {
		diagnostics.push("fixture tasks did not finish after abort".into());
	}
	if let Some(owner) = owner {
		if let Err(error) = owner.close(Duration::from_secs(10)).await {
			diagnostics.push(format!("fixture owner close failed: {error}"));
		}
	}
	drop(observation);
	match DataDirectoryOwner::acquire_existing(&dir) {
		Ok(owner) => {
			if let Err(error) = owner.close(Duration::from_secs(10)).await {
				diagnostics.push(format!("reopened owner close failed: {error}"));
			}
		}
		Err(error) => diagnostics.push(format!("fixture directory reopen failed: {error}")),
	}
	eprintln!("startup protocol/teardown diagnostics={diagnostics:?}");
	assert!(
		diagnostics.is_empty(),
		"startup fixture protocol/cleanup failed: {diagnostics:?}"
	);
	assert!(
		live.is_empty()
			&& !still_serving
			&& !service_retained
			&& !write_admitted
			&& !busy_before_cleanup,
		"startup left resources: live={live:?} serving={still_serving} write_admitted={write_admitted} directory_busy={busy_before_cleanup}"
	);
}
#[tokio::test]
async fn failed_join_startup_releases_owned_resources() {
	observe_failed_startup(false, false).await;
}
#[tokio::test]
async fn cancelled_join_startup_releases_owned_resources() {
	observe_failed_startup(true, false).await;
}

#[tokio::test]
async fn cancelled_startup_fences_before_worker_and_excludes_external_close() {
	observe_failed_startup(true, true).await;
}

struct PrefixProvider {
	publish_panic: bool,
	publish_error: bool,
}
#[async_trait]
impl TransportProvider for PrefixProvider {
	async fn connect(
		&self,
		_target: NodeId,
		_address: &str,
	) -> std::io::Result<Arc<dyn crate::network::transport::TransportConnection>> {
		Err(Error::new(
			ErrorKind::ConnectionRefused,
			"PD25 unexpected connect",
		))
	}
	fn publish_local(
		&self,
		_node: NodeId,
		_address: &str,
		_service: Arc<dyn HrftService>,
	) -> std::io::Result<()> {
		assert!(!self.publish_panic, "PD25 provider publish panic");
		if self.publish_error {
			Err(Error::new(
				ErrorKind::ConnectionRefused,
				"PD25 provider publish refusal",
			))
		} else {
			Ok(())
		}
	}
}

async fn pd25_prefix_case(publish_error: bool, publish_panic: bool, after_handoff: bool) {
	let dir = crate::test_support::TestDir::new("pd25-startup-prefix");
	let mut cfg = NodeBootstrapConfig::dev(7121, dir.to_path_buf(), "127.0.0.1:0".into());
	let mut probe = StartupProbe::default();
	probe.2 = after_handoff;
	cfg.startup_probe = Some(probe.clone());
	let task = tokio::spawn(WendelonNode::bootstrap_with_provider(
		cfg,
		Arc::new(PrefixProvider {
			publish_panic,
			publish_error,
		}),
	));
	match tokio::time::timeout(Duration::from_secs(30), task)
		.await
		.unwrap()
	{
		Err(error) if publish_panic && error.is_panic() => {}
		Ok(Err(error))
			if !publish_panic
				&& error.kind()
					== if after_handoff {
						ErrorKind::InvalidData
					} else {
						ErrorKind::ConnectionRefused
					} => {}
		Ok(Ok(node)) => {
			node.shutdown().await.unwrap();
			panic!("PD25 fault did not execute");
		}
		other => panic!(
			"PD25 unexpected bootstrap result: {}",
			match other {
				Err(e) => e.to_string(),
				Ok(Err(e)) => e.to_string(),
				_ => unreachable!(),
			}
		),
	}
	let observation = probe
		.0
		.lock()
		.unwrap()
		.take()
		.expect("prefix observation before provider callback");
	let live = observation
		.tasks
		.iter()
		.filter(|(_, handle)| !handle.is_finished())
		.map(|(name, _)| *name)
		.collect::<Vec<_>>();
	let ingress_open = !observation.lifecycle.is_draining();
	let service_retained = observation.service.upgrade().is_some();
	let owner = observation
		.slab
		.upgrade()
		.and_then(|slab| slab.directory_owner());
	let write_admitted = owner
		.as_ref()
		.is_some_and(|owner| owner.write_permit().is_ok());
	let busy = match DataDirectoryOwner::acquire_existing(&dir) {
		Ok(owner) => {
			owner.close(Duration::from_secs(10)).await.unwrap();
			false
		}
		Err(error) => {
			assert!(
				error
					.get_ref()
					.is_some_and(|e| e.is::<crate::storage::ownership::DataDirectoryBusy>())
			);
			true
		}
	};
	// This fixture-only Raft alias is deliberately held through observation.
	// It enables actual teardown after an earlier failure dropped ControlServer;
	// a correct bootstrap cleanup must close writers/LOCK even with aliases.
	observation.lifecycle.begin_drain();
	tokio::time::timeout(Duration::from_secs(10), observation.raft.shutdown())
		.await
		.unwrap()
		.unwrap();
	for (_, task) in &observation.tasks {
		task.abort();
	}
	tokio::time::timeout(Duration::from_secs(10), async {
		while observation
			.tasks
			.iter()
			.any(|(_, handle)| !handle.is_finished())
		{
			tokio::task::yield_now().await;
		}
	})
	.await
	.unwrap();
	if let Some(owner) = owner {
		owner.close(Duration::from_secs(10)).await.unwrap();
	}
	drop(observation);
	let reopened = DataDirectoryOwner::acquire_existing(&dir).unwrap();
	reopened.close(Duration::from_secs(10)).await.unwrap();
	assert!(
		live.is_empty() && !ingress_open && !service_retained && !write_admitted && !busy,
		"PD25 failed startup leaked resources: live={live:?} ingress_open={ingress_open} service_retained={service_retained} write_admitted={write_admitted} directory_busy={busy}"
	);
}
#[tokio::test]
async fn pd25_provider_publish_error_cleans_the_earlier_prefix() {
	pd25_prefix_case(true, false, false).await;
}
#[tokio::test]
async fn pd25_provider_publish_unwind_cleans_the_earlier_prefix() {
	pd25_prefix_case(false, true, false).await;
}
#[tokio::test]
async fn pd25_post_handoff_error_cleans_the_later_prefix() {
	pd25_prefix_case(false, false, true).await;
}
