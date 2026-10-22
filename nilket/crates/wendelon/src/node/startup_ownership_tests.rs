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
	Option<Duration>,
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
	owner: DataDirectoryOwner,
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
			owner: slab.directory_owner().expect("owned probe"),
			raft: raft.clone(),
			slab: Arc::downgrade(slab),
			control: Arc::downgrade(control),
			service: Arc::downgrade(service),
			lifecycle: lifecycle.clone(),
			tasks,
		});
	}
	pub(super) fn watchdog_deadline(&self) -> Option<Duration> {
		self.3
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
	fn withdraw_local(
		&self,
		_node_id: crate::schema::NodeId,
		_address: &str,
		_service: &std::sync::Arc<dyn crate::network::server::HrftService>,
	) -> std::io::Result<()> {
		// This provider does not retain publish_local registrations.
		Ok(())
	}

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
	registration: StdMutex<Option<Arc<dyn HrftService>>>,
	publish_panic: bool,
	publish_error: bool,
}
#[async_trait]
impl TransportProvider for PrefixProvider {
	fn withdraw_local(
		&self,
		_node_id: crate::schema::NodeId,
		_address: &str,
		service: &std::sync::Arc<dyn crate::network::server::HrftService>,
	) -> std::io::Result<()> {
		let mut registration = self
			.registration
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		if registration
			.as_ref()
			.is_some_and(|current| Arc::ptr_eq(current, service))
		{
			registration.take();
		}
		Ok(())
	}

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
		service: Arc<dyn HrftService>,
	) -> std::io::Result<()> {
		*self.registration.lock().unwrap() = Some(service);
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
			registration: StdMutex::new(None),
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
	// Unwind/cancellation starts tracked cleanup in Drop. Wait for its actual
	// certificate before the final oracle; the alias does no cleanup itself.
	let slot = observation
		.owner
		.latest_startup_cleanup()
		.expect("owned cleanup slot");
	assert!(matches!(
		tokio::time::timeout(Duration::from_secs(30), slot.wait())
			.await
			.unwrap(),
		crate::storage::ownership::StartupCleanupStatus::Complete
	));
	let live = observation
		.tasks
		.iter()
		.filter(|(_, handle)| !handle.is_finished())
		.map(|(name, _)| *name)
		.collect::<Vec<_>>();
	let raft_stopped = observation.raft.is_initialized().await.is_err();
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
		live.is_empty()
			&& raft_stopped
			&& !ingress_open
			&& !service_retained
			&& !write_admitted
			&& !busy,
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

#[path = "../../tests/support/tls_pki.rs"]
mod tls_pki;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FaultMode {
	Error,
	Cancel,
	Panic,
}
#[derive(Clone)]
pub(crate) struct StartupFault {
	site: &'static str,
	mode: FaultMode,
	snapshot: Arc<StdMutex<Option<FaultSnapshot>>>,
	entered: Arc<Notify>,
	last_site: Arc<StdMutex<Option<&'static str>>>,
	pub(super) bound_tcp: Arc<StdMutex<Vec<std::net::SocketAddr>>>,
}
impl PartialEq for StartupFault {
	fn eq(&self, other: &Self) -> bool {
		Arc::ptr_eq(&self.snapshot, &other.snapshot)
	}
}
impl Eq for StartupFault {}
struct FaultSnapshot {
	owner: DataDirectoryOwner,
	tasks: Vec<(&'static str, AbortHandle)>,
	raft: Option<WendelonRaft>,
	lifecycle: Option<ServerLifecycle>,
}
impl StartupFault {
	pub(super) async fn reached(
		&self,
		site: &'static str,
		owner: DataDirectoryOwner,
		tasks: Vec<(&'static str, AbortHandle)>,
		raft: Option<WendelonRaft>,
		lifecycle: Option<ServerLifecycle>,
	) -> std::io::Result<()> {
		*self.last_site.lock().unwrap() = Some(site);
		if self.site != site {
			return Ok(());
		}
		*self.snapshot.lock().unwrap() = Some(FaultSnapshot {
			owner,
			tasks,
			raft,
			lifecycle,
		});
		self.entered.notify_one();
		match self.mode {
			FaultMode::Error => Err(Error::new(
				ErrorKind::InvalidData,
				format!("PD25 fault at {site}"),
			)),
			FaultMode::Cancel => std::future::pending().await,
			FaultMode::Panic => panic!("PD25 constructor panic at {site}"),
		}
	}
}
const PREFIX_SITES: &[&str] = &[
	"pool",
	"io_workers",
	"set_checkpoint_emitter",
	"set_audit_chain_root_rollup",
	"adopt_factory",
	"adopt_raft",
	"set_ring_updater",
	"set_drift_monitor",
	"adopt_lifecycle",
	"will_publish",
	"adopt_transport",
	"set_epoch_reconciler",
	"set_default_tenant_seeder",
	"set_tenant_policy_reconciler",
	"set_eviction_watchdog",
	"set_lsm_compaction_control",
	"set_lsm_compaction",
	"set_lsm_gc",
	"set_secondary_index_lifecycle",
	"set_lease_manager",
	"set_pending_finalization_reconciler",
	"set_drop_reclaim_sweeper",
	"set_cdc_finalization_release_observer",
	"set_cdc_locality_heartbeat",
	"set_migration_sweeper",
	"set_tiering_sweeper",
	"set_restore_validation",
	"before_cluster",
	"epoch_reconcile",
	"tenant_seed",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pd25_every_adopted_prefix_err_cancel_panic_releases_real_tls_socket_and_reboots() {
	let pki_dir = crate::test_support::TestDir::new("pd25-matrix-pki");
	let pki = tls_pki::write_test_pki(&pki_dir);
	for &site in PREFIX_SITES {
		for mode in [FaultMode::Error, FaultMode::Cancel, FaultMode::Panic] {
			let dir = crate::test_support::TestDir::new("pd25-matrix-case");

			let fault = StartupFault {
				site,
				mode,
				snapshot: Arc::default(),
				entered: Arc::new(Notify::new()),
				last_site: Arc::default(),
				bound_tcp: Arc::default(),
			};
			let mut cfg = NodeBootstrapConfig::dev(1, dir.to_path_buf(), "127.0.0.1:0".into());
			cfg.transport_tls = Some(TransportTlsConfig {
				cert_path: pki.server_cert.clone(),
				key_path: pki.server_key.clone(),
				node_ca_path: pki.ca_cert.clone(),
				operator_ca_path: pki.ops_ca_cert.clone(),
				cluster_domain: tls_pki::CLUSTER_DOMAIN.into(),
			});
			let healthy = cfg.clone();
			cfg.startup_fault = Some(fault.clone());
			let mut task = tokio::spawn(WendelonNode::bootstrap(cfg));
			let mut early = None;
			let reached = tokio::select! {
				notice = tokio::time::timeout(Duration::from_secs(30), fault.entered.notified()) => notice.is_ok(),
				outcome = &mut task => {
					early = Some(outcome);
					fault.snapshot.lock().unwrap().is_some()
				}
			};
			if !reached {
				task.abort();
				let outcome = match early.take() {
					Some(outcome) => outcome,
					None => (&mut task).await,
				};
				let diagnostic = match outcome {
					Ok(Ok(node)) => {
						node.shutdown().await.unwrap();
						"unexpected healthy completion".into()
					}
					Ok(Err(error)) => format!(
						"early bootstrap error kind={:?} error={error:?}",
						error.kind()
					),
					Err(error) => format!("join={error:?}"),
				};
				panic!(
					"site not reached: {site} {mode:?}; last={:?}; {diagnostic}",
					*fault.last_site.lock().unwrap()
				);
			}
			if mode == FaultMode::Cancel && early.is_none() {
				task.abort();
			}
			let outcome = match early {
				Some(outcome) => outcome,
				None => tokio::time::timeout(Duration::from_secs(30), &mut task)
					.await
					.expect("bootstrap joined"),
			};
			match outcome {
				Ok(Err(error)) if mode == FaultMode::Error => {
					assert_eq!(error.kind(), ErrorKind::InvalidData, "{site}")
				}
				Err(error) if mode == FaultMode::Cancel => assert!(error.is_cancelled()),
				Err(error) if mode == FaultMode::Panic => assert!(error.is_panic()),
				Ok(Ok(node)) => {
					node.shutdown().await.unwrap();
					panic!("fault escaped: {site}");
				}
				_ => panic!("wrong fault outcome at {site} {mode:?}"),
			}
			let snapshot = fault.snapshot.lock().unwrap().take().unwrap();
			let slot = snapshot
				.owner
				.latest_startup_cleanup()
				.expect("tracked cleanup");
			assert!(matches!(
				tokio::time::timeout(Duration::from_secs(30), slot.wait())
					.await
					.unwrap(),
				crate::storage::ownership::StartupCleanupStatus::Complete
			));
			let live: Vec<_> = snapshot
				.tasks
				.iter()
				.filter(|(_, task)| !task.is_finished())
				.map(|(name, _)| *name)
				.collect();
			assert!(live.is_empty(), "{site} {mode:?}: {live:?}");
			assert!(
				snapshot.owner.write_permit().is_err(),
				"{site}: writes open"
			);
			if let Some(lifecycle) = snapshot.lifecycle {
				assert!(lifecycle.is_draining(), "{site}");
			}
			if let Some(raft) = snapshot.raft {
				assert!(
					raft.is_initialized().await.is_err(),
					"{site}: Raft still running before fixture teardown"
				);
			}
			let bound = fault.bound_tcp.lock().unwrap().clone();
			assert!(
				!bound.is_empty(),
				"actual TLS listener missing from fixture observation"
			);
			for address in bound {
				assert!(
					tokio::net::TcpStream::connect(address).await.is_err(),
					"{site}: TLS listener still accepts {address}"
				);
			}
			assert!(
				snapshot
					.owner
					.closed_signal()
					.load(std::sync::atomic::Ordering::Acquire),
				"{site}: owner not certified closed"
			);
			let reopened = WendelonNode::bootstrap(healthy)
				.await
				.unwrap_or_else(|e| panic!("{site} {mode:?} reboot: {e}"));
			if matches!(
				site,
				"epoch_reconcile" | "tenant_seed" | "set_pending_finalization_reconciler"
			) {
				assert!(
					reopened.recovered_after_unclean_shutdown(),
					"{site}: startup mutation failure skipped recovery"
				);
			}
			reopened.shutdown().await.unwrap();
			eprintln!(
				"PD25 MATRIX {site} {mode:?} PASS tasks={}",
				snapshot.tasks.len()
			);
		}
	}
}

impl std::fmt::Debug for StartupFault {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("StartupFault")
			.field("site", &self.site)
			.field("mode", &self.mode)
			.finish()
	}
}

struct BlockingWithdrawal {
	entered_file: PathBuf,
	panic_publish: bool,
	withdraw_failure: Option<String>,
}
#[async_trait]
impl TransportProvider for BlockingWithdrawal {
	async fn connect(
		&self,
		_: NodeId,
		_: &str,
	) -> std::io::Result<Arc<dyn crate::network::transport::TransportConnection>> {
		Err(Error::other("not used"))
	}
	fn publish_local(&self, _: NodeId, _: &str, _: Arc<dyn HrftService>) -> std::io::Result<()> {
		if self.panic_publish {
			panic!("PD25 watchdog publish unwind");
		}
		Err(Error::new(
			ErrorKind::InvalidData,
			"PD25 watchdog publish error",
		))
	}
	fn withdraw_local(&self, _: NodeId, _: &str, _: &Arc<dyn HrftService>) -> std::io::Result<()> {
		std::fs::write(&self.entered_file, b"actual withdrawal entered").unwrap();
		match self.withdraw_failure.as_deref() {
			Some("error") => {
				return Err(Error::new(
					ErrorKind::PermissionDenied,
					"withdrawal refused",
				));
			}
			Some("panic") => panic!("withdrawal callback panic"),
			_ => {}
		}
		loop {
			std::thread::park();
		}
	}
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pd25_forever_withdraw_child() {
	let Some(path) = std::env::var_os("WENDELON_PD25_FOREVER_CHILD") else {
		return;
	};
	let path = PathBuf::from(path);
	let mut cfg = NodeBootstrapConfig::dev(7131, path.clone(), "127.0.0.1:0".into());
	cfg.storage_shutdown_timeout = Duration::from_millis(20);
	let mut probe = StartupProbe::default();
	probe.3 = Some(Duration::from_secs(5));
	cfg.startup_probe = Some(probe.clone());
	let provider = Arc::new(BlockingWithdrawal {
		entered_file: path.join("withdraw-entered"),
		panic_publish: std::env::var_os("WENDELON_PD25_PANIC").is_some(),
		withdraw_failure: std::env::var("WENDELON_PD25_WITHDRAW_FAILURE").ok(),
	});
	let outcome = tokio::spawn(WendelonNode::bootstrap_with_provider(cfg, provider)).await;
	let observation = probe.0.lock().unwrap().take().unwrap();
	let closed_trusted = async {
		while observation
			.tasks
			.iter()
			.any(|(_, task)| !task.is_finished())
		{
			tokio::task::yield_now().await;
		}
		assert!(observation.lifecycle.is_draining());
		assert!(observation.owner.write_permit().is_err());
		while observation.raft.is_initialized().await.is_ok() {
			tokio::task::yield_now().await;
		}
		true
	}
	.await;
	if closed_trusted {
		std::fs::write(
			path.join("trusted-cleanup-complete"),
			b"done before withdrawal finished",
		)
		.unwrap();
	}
	if std::env::var_os("WENDELON_PD25_CLI").is_some() {
		let error = match outcome {
			Ok(Err(error)) => error,
			_ => panic!("expected real bootstrap cleanup error"),
		};
		assert_eq!(error.kind(), ErrorKind::InvalidData);
		let _exit = crate::cli::bootstrap_failure_exit(&error, Duration::from_secs(5));
		std::fs::write(
			path.join("cli-returned"),
			b"nested incomplete not recognized",
		)
		.unwrap();
	}
	// Keep the runtime and owned blocking callback alive. Only the independent
	// scope watchdog can complete this child; parent kills/reaps on regression.
	std::future::pending::<()>().await;
}
#[tokio::test]
async fn pd25_forever_withdraw_exits_bounded_and_requires_recovery() {
	for (panic_publish, cli, withdraw_failure) in [
		(false, false, "block"),
		(true, false, "block"),
		(false, true, "block"),
		(false, false, "error"),
		(false, false, "panic"),
	] {
		let dir = crate::test_support::TestDir::new("pd25-withdraw-watchdog");
		let mut command = std::process::Command::new(std::env::current_exe().unwrap());
		command
			.args([
				"--exact",
				"node::startup_ownership_tests::pd25_forever_withdraw_child",
				"--nocapture",
			])
			.env("WENDELON_PD25_FOREVER_CHILD", dir.as_os_str());
		if panic_publish {
			command.env("WENDELON_PD25_PANIC", "1");
		}
		if cli {
			command.env("WENDELON_PD25_CLI", "1");
		}
		command.env("WENDELON_PD25_WITHDRAW_FAILURE", withdraw_failure);
		let output = crate::test_support::run_test_child(&mut command, Duration::from_secs(15))
			.await
			.unwrap();
		assert_eq!(
			output.status.code(),
			Some(crate::storage::ownership::UNCLEAN_SHUTDOWN_EXIT_CODE),
			"{}",
			String::from_utf8_lossy(&output.stderr)
		);
		assert!(
			dir.join("withdraw-entered").exists(),
			"actual callback was not exercised"
		);
		assert!(
			dir.join("trusted-cleanup-complete").exists(),
			"foreign withdrawal blocked trusted cleanup"
		);
		assert!(
			!dir.join("cli-returned").exists(),
			"nested incomplete bootstrap marker missed"
		);
		assert!(!dir.join("CLEAN_SHUTDOWN").exists());
		let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		assert!(
			!owner.previous_shutdown_clean(),
			"must recover after hard exit"
		);
		owner.close(Duration::from_secs(10)).await.unwrap();
	}
}

struct SlowPartialPublish {
	registration: StdMutex<Option<Arc<dyn HrftService>>>,
	release: (StdMutex<bool>, std::sync::Condvar),
	entered: Notify,
	panic_publish: bool,
}
#[async_trait]
impl TransportProvider for SlowPartialPublish {
	async fn connect(
		&self,
		_: NodeId,
		_: &str,
	) -> std::io::Result<Arc<dyn crate::network::transport::TransportConnection>> {
		Err(Error::other("unused"))
	}
	fn publish_local(
		&self,
		_: NodeId,
		_: &str,
		service: Arc<dyn HrftService>,
	) -> std::io::Result<()> {
		*self.registration.lock().unwrap() = Some(service);
		assert!(
			!self.panic_publish,
			"panic after actual partial registration"
		);
		Err(Error::new(
			ErrorKind::ConnectionRefused,
			"error after actual partial registration",
		))
	}
	fn withdraw_local(
		&self,
		_: NodeId,
		_: &str,
		service: &Arc<dyn HrftService>,
	) -> std::io::Result<()> {
		self.entered.notify_one();
		let (lock, changed) = &self.release;
		let released = lock.lock().unwrap();
		let _released = changed.wait_while(released, |released| !*released).unwrap();
		let mut registration = self.registration.lock().unwrap();
		if registration
			.as_ref()
			.is_some_and(|current| Arc::ptr_eq(current, service))
		{
			registration.take();
		}
		Ok(())
	}
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pd25_slow_partial_publish_withdrawal_keeps_lock_but_not_trusted_workers() {
	for panic_publish in [false, true] {
		let dir = crate::test_support::TestDir::new("pd25-slow-partial");
		let mut cfg = NodeBootstrapConfig::dev(7133, dir.to_path_buf(), "127.0.0.1:0".into());
		cfg.storage_shutdown_timeout = Duration::from_millis(20);
		let probe = StartupProbe::default();
		cfg.startup_probe = Some(probe.clone());
		let provider = Arc::new(SlowPartialPublish {
			registration: StdMutex::new(None),
			release: (StdMutex::new(false), std::sync::Condvar::new()),
			entered: Notify::new(),
			panic_publish,
		});
		let mut task = tokio::spawn(WendelonNode::bootstrap_with_provider(cfg, provider.clone()));
		tokio::time::timeout(Duration::from_secs(10), provider.entered.notified())
			.await
			.unwrap();
		// Always release the foreign callback even when a regression fails an oracle.
		let outcome = tokio::time::timeout(Duration::from_secs(10), &mut task).await;
		let observation = probe.0.lock().unwrap().take().unwrap();
		let slot = observation.owner.latest_startup_cleanup().unwrap();
		let trusted = tokio::time::timeout(Duration::from_secs(5), async {
			while observation
				.tasks
				.iter()
				.any(|(_, task)| !task.is_finished())
			{
				tokio::task::yield_now().await;
			}
			while observation.raft.is_initialized().await.is_ok() {
				tokio::task::yield_now().await;
			}
			true
		})
		.await;
		let write_closed = observation.owner.write_permit().is_err();
		let still_registered = provider.registration.lock().unwrap().is_some();
		let busy = DataDirectoryOwner::acquire_existing(&dir).is_err();
		let close_incomplete = observation
			.owner
			.close(Duration::from_millis(20))
			.await
			.is_err();
		*provider.release.0.lock().unwrap() = true;
		provider.release.1.notify_all();
		tokio::time::timeout(Duration::from_secs(10), async {
			while !observation
				.owner
				.closed_signal()
				.load(std::sync::atomic::Ordering::Acquire)
			{
				tokio::task::yield_now().await;
			}
		})
		.await
		.unwrap();
		slot.join_finished().await;
		assert!(trusted.unwrap() && write_closed && still_registered && busy && close_incomplete);
		assert!(provider.registration.lock().unwrap().is_none());
		match outcome.unwrap() {
			Err(error) if panic_publish => assert!(error.is_panic()),
			Ok(Err(error)) if !panic_publish => {
				assert_eq!(error.kind(), ErrorKind::ConnectionRefused)
			}
			_ => panic!("wrong partial publish outcome"),
		}
		assert!(observation.lifecycle.is_draining());
		let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		owner.close(Duration::from_secs(10)).await.unwrap();
	}
}

#[tokio::test]
async fn pd25_tcp_reservation_does_not_own_the_quic_udp_port() {
	let dir = crate::test_support::TestDir::new("pd25-port-domain-control");
	let pki = tls_pki::write_test_pki(&dir.join("pki"));
	// Acquire an actual two-protocol reservation; only a typed conflict during
	// reservation acquisition may select another port. Bootstrap is attempted once.
	let (reservation, _udp_owner) = (0..32)
		.find_map(|_| {
			let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
			match std::net::TcpListener::bind(udp.local_addr().unwrap()) {
				Ok(tcp) => Some((tcp, udp)),
				Err(error) if error.kind() == ErrorKind::AddrInUse => None,
				Err(error) => panic!("reservation: {error}"),
			}
		})
		.expect("available paired protocol port");
	let address = reservation.local_addr().unwrap();
	let fault = StartupFault {
		site: "set_pending_finalization_reconciler",
		mode: FaultMode::Cancel,
		snapshot: Arc::default(),
		entered: Arc::new(Notify::new()),
		last_site: Arc::default(),
		bound_tcp: Arc::default(),
	};
	let mut cfg = NodeBootstrapConfig::dev(1, dir.to_path_buf(), address.to_string());
	cfg.transport_tls = Some(TransportTlsConfig {
		cert_path: pki.server_cert,
		key_path: pki.server_key,
		node_ca_path: pki.ca_cert,
		operator_ca_path: pki.ops_ca_cert,
		cluster_domain: tls_pki::CLUSTER_DOMAIN.into(),
	});
	cfg.startup_fault = Some(fault.clone());
	drop(reservation);
	let error = match WendelonNode::bootstrap(cfg).await {
		Err(error) => error,
		Ok(node) => {
			node.shutdown().await.unwrap();
			panic!("occupied UDP port was ignored");
		}
	};
	assert_eq!(error.kind(), ErrorKind::AddrInUse, "{error:?}");
	assert!(fault.snapshot.lock().unwrap().is_none());
	assert!(fault.last_site.lock().unwrap().is_none());
	let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
	owner.close(Duration::from_secs(10)).await.unwrap();
}

#[tokio::test]
async fn pd25_partial_restore_refusal_never_certifies_clean_and_reboot_validates_recovery() {
	let source_dir = crate::test_support::TestDir::new("pd25-restore-source");
	let source = WendelonNode::bootstrap(NodeBootstrapConfig::dev(
		1,
		source_dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let backup = source.export_cluster_backup().await.unwrap();
	source.shutdown().await.unwrap();
	for mode in [FaultMode::Error, FaultMode::Cancel, FaultMode::Panic] {
		let dir = crate::test_support::TestDir::new("pd25-restore-partial");
		let healthy = NodeBootstrapConfig::dev(1, dir.to_path_buf(), "127.0.0.1:0".into());
		let fault = StartupFault {
			site: "after_control_restore",
			mode,
			snapshot: Arc::default(),
			entered: Arc::new(Notify::new()),
			last_site: Arc::default(),
			bound_tcp: Arc::default(),
		};
		let mut cfg = healthy.clone();
		cfg.startup_fault = Some(fault.clone());
		let image = backup.clone();
		let mut task =
			tokio::spawn(
				async move { WendelonNode::restore_from_cluster_backup(&image, cfg).await },
			);
		tokio::time::timeout(Duration::from_secs(30), fault.entered.notified())
			.await
			.unwrap();
		if mode == FaultMode::Cancel {
			task.abort();
		}
		match tokio::time::timeout(Duration::from_secs(30), &mut task)
			.await
			.unwrap()
		{
			Ok(Err(error)) if mode == FaultMode::Error => {
				assert_eq!(error.kind(), ErrorKind::InvalidData)
			}
			Err(error) if mode == FaultMode::Cancel => assert!(error.is_cancelled()),
			Err(error) if mode == FaultMode::Panic => assert!(error.is_panic()),
			Ok(Ok(node)) => {
				node.shutdown().await.unwrap();
				panic!("partial restore fault escaped");
			}
			_ => panic!("wrong restore fault outcome"),
		}
		let snapshot = fault.snapshot.lock().unwrap().take().unwrap();
		assert!(
			snapshot.owner.previous_shutdown_clean(),
			"restore must start from a genuinely fresh clean owner"
		);
		let slot = snapshot.owner.latest_startup_cleanup().unwrap();
		assert!(matches!(
			tokio::time::timeout(Duration::from_secs(30), slot.wait())
				.await
				.unwrap(),
			crate::storage::ownership::StartupCleanupStatus::Complete
		));
		assert!(
			dir.join("control.wal").exists(),
			"restore must really mutate disk before fault"
		);
		assert!(
			!dir.join("CLEAN_SHUTDOWN").exists(),
			"stopped writers must not certify failed restore"
		);
		assert!(
			snapshot
				.owner
				.closed_signal()
				.load(std::sync::atomic::Ordering::Acquire)
		);
		let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		assert!(!owner.previous_shutdown_clean());
		owner.close(Duration::from_secs(10)).await.unwrap();
		let recovered = WendelonNode::bootstrap(healthy).await.unwrap();
		assert!(recovered.recovered_after_unclean_shutdown());
		assert_eq!(
			recovered.log_store.bootstrap_mode().await,
			DurableBootstrapMode::Recovery
		);
		recovered.shutdown().await.unwrap();
		assert!(
			dir.join("CLEAN_SHUTDOWN").exists(),
			"later validated boot may certify clean shutdown"
		);
	}
}

#[tokio::test]
async fn pd25_healthy_handoff_preserves_base_task_census_and_clean_shutdown() {
	let dir = crate::test_support::TestDir::new("pd25-healthy-census");
	let node = WendelonNode::bootstrap(NodeBootstrapConfig::dev(
		7141,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let owner = node._directory_owner.clone();
	let tasks = [
		("ring_updater", node.handles.ring_updater.abort_handle()),
		("drift_monitor", node.handles.drift_monitor.abort_handle()),
		(
			"eviction_watchdog",
			node.handles.eviction_watchdog.abort_handle(),
		),
		("lease_manager", node.handles.lease_manager.abort_handle()),
		(
			"pending_finalization_reconciler",
			node.handles.pending_finalization_reconciler.abort_handle(),
		),
		(
			"cdc_finalization_release_observer",
			node.handles
				.cdc_finalization_release_observer
				.abort_handle(),
		),
		(
			"cdc_locality_heartbeat",
			node.handles.cdc_locality_heartbeat.abort_handle(),
		),
		(
			"epoch_reconciler",
			node.handles.epoch_reconciler.abort_handle(),
		),
		(
			"tenant_policy_reconciler",
			node.handles.tenant_policy_reconciler.abort_handle(),
		),
		(
			"drop_reclaim_sweeper",
			node.handles.drop_reclaim_sweeper.abort_handle(),
		),
		(
			"migration_sweeper",
			node.handles.migration_sweeper.abort_handle(),
		),
		("lsm_compaction", node.handles.lsm_compaction.abort_handle()),
		("lsm_gc", node.handles.lsm_gc.abort_handle()),
		(
			"secondary_index_lifecycle",
			node.handles.secondary_index_lifecycle.abort_handle(),
		),
		(
			"audit_chain_root_rollup",
			node.handles.audit_chain_root_rollup.abort_handle(),
		),
		(
			"default_tenant_seeder",
			node.handles.default_tenant_seeder.abort_handle(),
		),
		(
			"checkpoint_emitter",
			node.handles.checkpoint_emitter.abort_handle(),
		),
	];
	assert_eq!(
		tasks.len(),
		17,
		"base has seventeen required tasks and two optional task slots"
	);
	assert!(node.handles.tiering_sweeper.is_none());
	assert!(node.handles.restore_validation.is_none());
	node.shutdown().await.unwrap();
	assert!(tasks.iter().all(|(_, task)| task.is_finished()));
	assert!(
		owner
			.closed_signal()
			.load(std::sync::atomic::Ordering::Acquire)
	);
	assert!(
		dir.join("CLEAN_SHUTDOWN").exists(),
		"healthy handoff must preserve clean-shutdown behavior"
	);
}
