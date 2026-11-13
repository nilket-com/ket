//! ADR 0726: no member joins a cluster that has not established its root check.

use super::*;
use crate::node::startup_ownership_tests::StartupFault;

fn provider(
	id: u32,
	network: &Arc<SimulatedTransportNetwork>,
) -> Arc<dyn crate::network::transport::TransportProvider> {
	Arc::new(SimulatedTransportProvider::new(
		crate::schema::NodeId { id, _reserved: 0 },
		network.clone(),
		crate::network::pool::BufPool::new(64, 1024 * 1024),
		TransportTier::QuicIoUring,
	))
}

/// The forming member is held just before it establishes the root check: it
/// leads a one-member cluster and answers control requests. A joiner that has
/// the cluster's root is refused, retryably, for as long as that lasts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_is_refused_until_the_root_check_exists() {
	let network = Arc::new(SimulatedTransportNetwork::default());
	let (_forming_dir, mut forming_cfg) = member(NODES[0]);
	let fault = StartupFault::parked_at("root_check");
	forming_cfg.startup_fault = Some(fault.clone());
	let forming_data = forming_cfg.data_dir.clone();
	let forming = tokio::spawn(WendelonNode::bootstrap_with_provider(
		forming_cfg,
		provider(NODES[0], &network),
	));
	tokio::time::timeout(std::time::Duration::from_secs(30), fault.entered())
		.await
		.expect("the forming member reached the root check step");

	let (_joiner_dir, joiner_cfg) = member(NODES[1]);
	crate::crypto::test_support::enroll_root_artifact(&forming_data, &joiner_cfg.data_dir).unwrap();
	let error =
		match WendelonNode::bootstrap_with_provider(joiner_cfg, provider(NODES[1], &network)).await
		{
			Err(error) => error,
			Ok(_) => panic!("a member joined a cluster with no root check"),
		};
	assert!(
		error.to_string().contains(&format!(
			"reason_code={}",
			crate::network::control_server::CONTROL_REASON_FORMATION_INCOMPLETE
		)),
		"{error}"
	);
	assert_eq!(error.kind(), ErrorKind::NotConnected, "retryable: {error}");
	forming.abort();
	let _ = forming.await;
}
