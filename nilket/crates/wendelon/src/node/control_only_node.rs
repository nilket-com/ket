//! ADR 0104 (wendelon): a control-only member.
//!
//! A replacement's process holds only what keeps its Raft obligations: its data
//! directory owner, its log store, its state machine (with its checkpoint
//! observer and the revocation snapshot), a Raft object constructed with
//! elections disabled, and a Raft-only router. It constructs no data server,
//! control server, slab manager or decision proposals, so no data, signing,
//! lease, takeover, generation or reclaim duty can run here. It votes, catches
//! up and never leads. Its role is durable in its store's page and holds after
//! Done and across restarts, until a later explicitly authorized activation.

use super::*;
use crate::consensus::decisions::instances::RaftIdentity;

pub struct ControlOnlyNode {
	pub local_node: NodeId,
	pub identity: RaftIdentity,
	pub raft: WendelonRaft,
	pub state_machine: WendelonStateMachine,
	provider: Arc<dyn TransportProvider>,
	lifecycle: ServerLifecycle,
	servers: Vec<NodeTransportServer>,
	checkpoint_emitter: tokio::task::JoinHandle<()>,
	owner: DataDirectoryOwner,
	/// ADR 0105 (wendelon): the replacement's preparation and its task.
	preparation: Arc<super::replacement_preparation::Preparation>,
	preparing: tokio::task::JoinHandle<()>,
	shutdown_bound: std::time::Duration,
}

impl ControlOnlyNode {
	/// Starts from configuration over its configured TLS transport.
	pub async fn bootstrap(cfg: NodeBootstrapConfig) -> std::io::Result<Self> {
		let (revocation_tx, _) = watch::channel(Arc::new(RevocationSnapshot::default()));
		let Some((providers, listeners)) = configured_transport(&cfg, &revocation_tx).await? else {
			return Err(Error::new(
				ErrorKind::Unsupported,
				"a control-only member needs its configured transport_tls",
			));
		};
		let provider = providers
			.get(&TransportTier::QuicIoUring)
			.cloned()
			.ok_or_else(|| Error::other("the configured transport has no QUIC provider"))?;
		Self::start(cfg, provider, listeners, revocation_tx).await
	}

	/// Starts over a caller's transport (in-process clusters).
	pub async fn bootstrap_with_provider(
		cfg: NodeBootstrapConfig,
		provider: Arc<dyn TransportProvider>,
	) -> std::io::Result<Self> {
		let (revocation_tx, _) = watch::channel(Arc::new(RevocationSnapshot::default()));
		Self::start(cfg, provider, Vec::new(), revocation_tx).await
	}

	async fn start(
		cfg: NodeBootstrapConfig,
		provider: Arc<dyn TransportProvider>,
		listeners: Vec<BootstrapListener>,
		revocation_tx: watch::Sender<Arc<RevocationSnapshot>>,
	) -> std::io::Result<Self> {
		cfg.validate()?;
		if cfg.decision_mode != crate::consensus::decision_mode::DecisionMode::ParentDecisions {
			return Err(Error::new(
				ErrorKind::Unsupported,
				"only a ParentDecisions member runs control-only",
			));
		}
		if !instance_startup::starts_control_only(&cfg)? {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				"RaftRoleMismatch: this data directory holds no control-only identity and no replacement token",
			));
		}
		let owner = DataDirectoryOwner::acquire_or_create(&cfg.data_dir)?;
		let wal_io = spawn_io_thread_named_for_class(
			cfg.wal_io_depth,
			"wendelon-io-uring-wal",
			IoUringClass::Wal,
		)?
		.with_data_directory_owner(owner.clone());
		let (wal_path, vote_path, snapshot_path) = control_artifact_paths(&cfg.data_dir);
		let retention = RetentionGuard::new(cfg.control_retention);
		let log_store = WendelonLogStore::open_with_retention_and_owner(
			&wal_path,
			&vote_path,
			&snapshot_path,
			wal_io,
			retention.clone(),
			owner.clone(),
			cfg.decision_mode,
		)
		.await?;
		let pool = BufPool::new(256, 1024 * 1024);
		let identity =
			instance_startup::establish_identity(&cfg, &log_store, &provider, &pool, true).await?;
		let checkpoints = CheckpointCoordinator::new(log_store.clone());
		let state_machine = log_store
			.recovered_state_machine()
			.await?
			.with_checkpoint_observer(checkpoints.clone())
			.with_external_revocation_snapshot(revocation_tx.clone());
		let checkpoint_emitter = checkpoints.spawn();
		let network = WendelonRaftNetworkFactory::new(provider.clone(), pool.clone())
			.with_decision_mode(cfg.decision_mode)
			.with_instance_routing(crate::consensus::raft_network::InstanceRouting {
				local_instance: identity.instance,
				local_slot: cfg.node_id,
				state: state_machine.clone(),
			});
		// Elections are disabled before construction: this member never
		// campaigns, so it never leads, coordinates or holds a lineage.
		let mut config = (*control_raft_config(&cfg)?).clone();
		config.enable_elect = false;
		let raft = WendelonRaft::new(
			identity.instance,
			Arc::new(config),
			network,
			log_store.clone(),
			state_machine.clone(),
		)
		.await
		.map_err(|error| Error::other(format!("Raft::new: {error}")))?;
		let raft_server = Arc::new(
			WendelonRaftInternalServer::new(raft.clone(), pool.clone(), retention)
				.with_decision_mode(cfg.decision_mode),
		);
		// ADR 0105 (wendelon): a data-plane request that reaches this member
		// gets its family's typed refusal, never a dropped stream; nothing here
		// serves, stores or acknowledges data.
		let router = Arc::new(
			MsgRouterBuilder::default()
				.with_raft_internal(raft_server)
				.with_data_ring(Arc::new(
					crate::network::data_server::not_serving::NotServing::new(pool.clone()),
				))
				.build(),
		);
		let lifecycle = ServerLifecycle::new();
		let service: Arc<dyn HrftService> =
			Arc::new(LifecycleService::new(router, lifecycle.clone()));
		let local_node = NodeId {
			id: cfg.node_id,
			_reserved: 0,
		};
		provider.publish_local(local_node, &cfg.advertise_address, service.clone())?;
		let servers = listeners
			.into_iter()
			.map(|listener| {
				spawn_listener(
					listener,
					service.clone(),
					pool.clone(),
					lifecycle.clone(),
					cfg.transport_admission.clone(),
					revocation_tx.subscribe(),
				)
			})
			.collect();
		// ADR 0105 (wendelon): after Done, admit this store and bind its key.
		let (secret, _) = identity::resolve_decision_keys(&cfg)?;
		let key = ed25519_dalek::SigningKey::from_bytes(&secret)
			.verifying_key()
			.to_bytes();
		let preparation = Arc::new(super::replacement_preparation::Preparation::new(
			cfg.node_id,
			identity.instance,
			cfg.decision_mode,
			raft.clone(),
			state_machine.clone(),
			provider.clone(),
			pool.clone(),
			owner.clone(),
			cfg.data_dir.clone(),
			key,
		));
		let preparing = tokio::spawn(preparation.clone().run());
		tracing::info!(
			slot = cfg.node_id,
			instance = identity.instance,
			"started control-only: votes and catches up, runs no data duty"
		);
		Ok(Self {
			local_node,
			identity,
			raft,
			state_machine,
			provider,
			lifecycle,
			servers,
			checkpoint_emitter,
			owner,
			preparation,
			preparing,
			shutdown_bound: cfg.storage_shutdown_timeout,
		})
	}

	/// ADR 0105 (wendelon): this member's data incarnation J, from its own
	/// store index (see `Preparation::store_incarnation`).
	#[cfg(test)]
	pub(crate) async fn store_incarnation(
		&self,
		may_create: bool,
	) -> std::io::Result<crate::consensus::StoreIncarnation> {
		self.preparation.store_incarnation(may_create).await
	}

	/// Test-only: the preparation proposals this node sent.
	#[cfg(test)]
	pub(crate) fn preparation_proposals_for_test(&self) -> u64 {
		self.preparation.sent_for_test()
	}

	/// Whether preparation has finished (promoted and bound) or stopped.
	pub fn preparation_ended(&self) -> bool {
		self.preparing.is_finished()
	}

	/// Stops serving, shuts Raft down and closes the data directory.
	pub async fn shutdown(self) -> std::io::Result<()> {
		self.preparing.abort();
		let _ = self.preparing.await;
		self.preparation.release().await;
		self.lifecycle.begin_drain();
		for server in &self.servers {
			server.begin_drain();
		}
		let raft = self.raft.shutdown().await.map_err(Error::other);
		for server in self.servers {
			server.shutdown().await?;
		}
		self.checkpoint_emitter.abort();
		let _ = self.checkpoint_emitter.await;
		drop(self.provider);
		drop(self.state_machine);
		drop(self.preparation);
		raft?;
		self.owner.close(self.shutdown_bound).await
	}
}
